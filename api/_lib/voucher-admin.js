import { randomBytes } from 'node:crypto';

export const VOUCHER_CAMPAIGN_TYPES = Object.freeze([
  'free_booking', 'discount_amount', 'discount_percent',
]);

export const VOUCHER_PRICING_TYPES = Object.freeze([
  'standard', 'morning_weekday', 'morning_weekday_advance',
  'late_night', 'special_promotion',
]);

const CAMPAIGN_ID_RE = /^[a-z0-9][a-z0-9-]{2,63}$/;
const CODE_RE = /^[A-Z0-9][A-Z0-9_-]{2,63}$/;
const KEYWORD_RE = /^[A-Z0-9][A-Z0-9_-]{1,31}$/;
const PREFIX_RE = /^[A-Z0-9_-]{0,16}$/;
const TIME_RE = /^(?:[01]\d|2[0-3]):[0-5]\d$|^24:00$/;
const RANDOM_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

const clean = (value, max = 200) => String(value ?? '').trim().slice(0, max);
const upper = (value, max) => clean(value, max).toUpperCase();

function minuteOfDay(time) {
  const [h, m] = String(time).split(':').map(Number);
  return h * 60 + m;
}

function parseOptionalInstant(value, label) {
  const text = clean(value, 64);
  if (!text) return { ok: true, value: null };
  const ms = Date.parse(text);
  return Number.isFinite(ms)
    ? { ok: true, value: ms }
    : { ok: false, error: `${label} is invalid` };
}

export function normalizeCampaignInput(input = {}) {
  const campaignId = clean(input.campaignId, 64).toLowerCase();
  const name = clean(input.name, 100);
  const keyword = upper(input.keyword, 32);
  const codePrefix = upper(input.codePrefix, 16);
  const voucherType = clean(input.voucherType, 32);

  if (!CAMPAIGN_ID_RE.test(campaignId)) {
    return { ok: false, error: 'Campaign ID must be 3-64 lowercase letters, numbers, or hyphens' };
  }
  if (name.length < 2) return { ok: false, error: 'Campaign name is required' };
  if (keyword && !KEYWORD_RE.test(keyword)) return { ok: false, error: 'Keyword contains unsupported characters' };
  if (!PREFIX_RE.test(codePrefix)) return { ok: false, error: 'Code prefix contains unsupported characters' };
  if (!VOUCHER_CAMPAIGN_TYPES.includes(voucherType)) return { ok: false, error: 'Voucher type is invalid' };

  const allowedDays = [...new Set((Array.isArray(input.allowedDays) ? input.allowedDays : []).map(Number))]
    .filter(day => Number.isInteger(day) && day >= 0 && day <= 6)
    .sort((a, b) => a - b);
  if (!allowedDays.length) return { ok: false, error: 'Select at least one allowed day' };

  const startTime = clean(input.startTime, 5) || '06:00';
  const endTime = clean(input.endTime, 5) || '24:00';
  if (!TIME_RE.test(startTime) || !TIME_RE.test(endTime) || startTime === '24:00') {
    return { ok: false, error: 'Allowed time window is invalid' };
  }
  if (minuteOfDay(endTime) <= minuteOfDay(startTime)) {
    return { ok: false, error: 'End time must be later than start time' };
  }

  // The current customer booking route deliberately permits vouchers only on
  // exact 60-minute bookings. Keep the Admin contract honest until that route
  // is expanded; do not let an owner create a campaign the live app rejects.
  const exactDurationMinutes = Number(input.exactDurationMinutes ?? 60);
  if (exactDurationMinutes !== 60) return { ok: false, error: 'Voucher duration is currently fixed at 60 minutes' };

  const validFrom = parseOptionalInstant(input.validFrom, 'Valid from');
  if (!validFrom.ok) return validFrom;
  const expiresAt = parseOptionalInstant(input.expiresAt, 'Expiry');
  if (!expiresAt.ok) return expiresAt;
  if (validFrom.value !== null && expiresAt.value !== null && expiresAt.value <= validFrom.value) {
    return { ok: false, error: 'Expiry must be later than valid from' };
  }

  const maxCancellationRestores = Number(input.maxCancellationRestores ?? 0);
  if (!Number.isInteger(maxCancellationRestores) || maxCancellationRestores < 0 || maxCancellationRestores > 10) {
    return { ok: false, error: 'Cancellation restores must be an integer from 0 to 10' };
  }

  const allowedPricingTypes = [...new Set(Array.isArray(input.allowedPricingTypes) ? input.allowedPricingTypes : [])];
  if (allowedPricingTypes.some(type => !VOUCHER_PRICING_TYPES.includes(type))) {
    return { ok: false, error: 'Pricing scope contains an unsupported type' };
  }

  let discountAmount = null, discountPercent = null, maxDiscountAmount = null;
  const minFinalPrice = Number(input.minFinalPrice ?? 0);
  if (!Number.isInteger(minFinalPrice) || minFinalPrice < 0 || minFinalPrice > 100000) {
    return { ok: false, error: 'Minimum final price must be an integer from 0 to 100,000' };
  }
  if (voucherType === 'discount_amount') {
    discountAmount = Number(input.discountAmount);
    if (!Number.isInteger(discountAmount) || discountAmount < 1 || discountAmount > 100000) {
      return { ok: false, error: 'Discount amount must be an integer from 1 to 100,000' };
    }
  }
  if (voucherType === 'discount_percent') {
    discountPercent = Number(input.discountPercent);
    if (!Number.isFinite(discountPercent) || discountPercent <= 0 || discountPercent > 100) {
      return { ok: false, error: 'Discount percent must be greater than 0 and at most 100' };
    }
    const cap = input.maxDiscountAmount === '' || input.maxDiscountAmount == null ? 0 : Number(input.maxDiscountAmount);
    if (!Number.isInteger(cap) || cap < 0 || cap > 100000) {
      return { ok: false, error: 'Maximum discount must be an integer from 0 to 100,000' };
    }
    maxDiscountAmount = cap;
  }

  return {
    ok: true,
    campaignId,
    data: {
      schemaVersion: 2,
      campaignId,
      name,
      keyword: keyword || null,
      codePrefix: codePrefix || null,
      active: input.active === true,
      voucherType,
      validFromMs: validFrom.value,
      expiresAtMs: expiresAt.value,
      allowedDays,
      startTime,
      endTime,
      excludeHolidays: input.excludeHolidays === true,
      exactDurationMinutes,
      requiresLineLogin: input.requiresLineLogin !== false,
      transferable: input.transferable === true,
      maxUsesPerCode: 1,
      maxCancellationRestores,
      branchId: 'ladprao1',
      resourceId: 'room1',
      allowedPricingTypes,
      discountAmount,
      discountPercent,
      maxDiscountAmount,
      minFinalPrice,
    },
  };
}

export function normalizeCustomVoucherCode(value) {
  const code = upper(value, 64);
  return CODE_RE.test(code)
    ? { ok: true, code }
    : { ok: false, error: 'Voucher code must be 3-64 letters, numbers, underscore, or hyphen' };
}

export function normalizeRandomCodeRequest(input = {}, prefix = '') {
  const count = Number(input.count ?? 1);
  const randomLength = Number(input.randomLength ?? 8);
  if (!Number.isInteger(count) || count < 1 || count > 100) {
    return { ok: false, error: 'Generate count must be an integer from 1 to 100' };
  }
  if (!Number.isInteger(randomLength) || randomLength < 5 || randomLength > 16) {
    return { ok: false, error: 'Random length must be an integer from 5 to 16' };
  }
  const normalizedPrefix = upper(prefix, 16);
  if (!PREFIX_RE.test(normalizedPrefix) || normalizedPrefix.length + randomLength > 64) {
    return { ok: false, error: 'Campaign prefix is too long for generated codes' };
  }
  return { ok: true, count, randomLength, prefix: normalizedPrefix };
}

export function generateVoucherCodes({ count, randomLength, prefix = '' }) {
  const codes = new Set();
  while (codes.size < count) {
    const bytes = randomBytes(randomLength);
    let suffix = '';
    for (let i = 0; i < randomLength; i++) suffix += RANDOM_ALPHABET[bytes[i] % RANDOM_ALPHABET.length];
    codes.add(`${prefix}${suffix}`);
  }
  return [...codes];
}
