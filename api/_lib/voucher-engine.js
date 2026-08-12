// Voucher Engine v2. Pure business rules: no Firestore access in this module.
//
// Security model:
//   - A keyword/codePrefix identifies a campaign for reporting and routing only.
//   - An exact vouchers/{CODE} document is always required to grant value.
//   - Legacy discount vouchers remain supported without a campaign document.

export const VOUCHER_SCHEMA_VERSION = 2;

export const VOUCHER_TYPES = Object.freeze({
  FREE_BOOKING: 'free_booking',
  DISCOUNT_AMOUNT: 'discount_amount',
  DISCOUNT_PERCENT: 'discount_percent',
});

const LEGACY_DEFAULT_DISCOUNT = 50;

function millis(value) {
  if (value == null) return null;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (value instanceof Date) return value.getTime();
  if (typeof value.toMillis === 'function') return value.toMillis();
  if (typeof value.seconds === 'number') return value.seconds * 1000;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function integer(value, fallback = 0) {
  const n = Number(value);
  return Number.isInteger(n) ? n : fallback;
}

function number(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function dayOfWeek(dateISO) {
  const [y, m, d] = String(dateISO).split('-').map(Number);
  if (![y, m, d].every(Number.isInteger)) return null;
  return new Date(Date.UTC(y, m - 1, d)).getUTCDay();
}

function minuteOfDay(time) {
  const match = /^(\d{2}):(\d{2})$/.exec(String(time || ''));
  if (!match) return null;
  const h = Number(match[1]), m = Number(match[2]);
  if (h === 24 && m === 0) return 1440;
  return h >= 0 && h <= 23 && m >= 0 && m <= 59 ? h * 60 + m : null;
}

function firstDefined(...values) {
  return values.find(value => value !== undefined && value !== null);
}

export function isVoucherV2(voucher, campaign = null) {
  return integer(voucher?.schemaVersion) >= VOUCHER_SCHEMA_VERSION ||
    integer(campaign?.schemaVersion) >= VOUCHER_SCHEMA_VERSION ||
    !!voucher?.campaignId || !!campaign || !!voucher?.voucherType;
}

export function resolveVoucherDefinition(voucher, campaign = null) {
  if (!voucher) return null;
  const v2 = isVoucherV2(voucher, campaign);
  const type = firstDefined(voucher.voucherType, campaign?.voucherType,
    v2 ? VOUCHER_TYPES.DISCOUNT_AMOUNT : VOUCHER_TYPES.DISCOUNT_AMOUNT);
  return {
    schemaVersion: v2 ? VOUCHER_SCHEMA_VERSION : 1,
    lifecycleMode: v2 ? 'v2_state' : 'legacy_used_count',
    voucherType: Object.values(VOUCHER_TYPES).includes(type) ? type : null,
    campaignId: firstDefined(voucher.campaignId, campaign?.campaignId, campaign?.id, null),
    campaignName: firstDefined(voucher.campaignName, campaign?.name, campaign?.campaignName, null),
    keyword: firstDefined(voucher.keyword, campaign?.keyword, null),
    codePrefix: firstDefined(voucher.codePrefix, campaign?.codePrefix, null),
    active: voucher.active === true && (!campaign || campaign.active === true),
    state: String(voucher.state || 'available'),
    validFrom: millis(firstDefined(voucher.validFrom, campaign?.validFrom)),
    expiresAt: millis(firstDefined(voucher.expiresAt, campaign?.expiresAt)),
    issuedTo: voucher.issuedTo || null,
    transferable: firstDefined(voucher.transferable, campaign?.transferable, !voucher.issuedTo) === true,
    requiresLineLogin: firstDefined(voucher.requiresLineLogin, campaign?.requiresLineLogin, false) === true,
    maxUses: integer(firstDefined(voucher.maxUses, campaign?.maxUsesPerCode), v2 ? 1 : 0),
    usedCount: integer(voucher.usedCount, 0),
    maxCancellationRestores: integer(firstDefined(voucher.maxCancellationRestores, campaign?.maxCancellationRestores), 0),
    cancellationRestoreCount: integer(voucher.cancellationRestoreCount, 0),
    reservedBookingId: voucher.reservedBookingId || null,
    reservedUntil: millis(voucher.reservedUntil),
    allowedDays: Array.isArray(firstDefined(voucher.allowedDays, campaign?.allowedDays))
      ? firstDefined(voucher.allowedDays, campaign?.allowedDays).map(Number)
      : null,
    startTime: firstDefined(voucher.startTime, campaign?.startTime, null),
    endTime: firstDefined(voucher.endTime, campaign?.endTime, null),
    excludeHolidays: firstDefined(voucher.excludeHolidays, campaign?.excludeHolidays, false) === true,
    exactDurationMinutes: integer(firstDefined(
      voucher.exactDurationMinutes,
      campaign?.exactDurationMinutes,
      Array.isArray(campaign?.durationMinutes) && campaign.durationMinutes.length === 1 ? campaign.durationMinutes[0] : undefined,
    ), 0),
    allowedDurations: Array.isArray(firstDefined(voucher.allowedDurations, campaign?.durationMinutes))
      ? firstDefined(voucher.allowedDurations, campaign?.durationMinutes).map(Number)
      : null,
    branchId: firstDefined(voucher.branchId, campaign?.branchId, null),
    resourceId: firstDefined(voucher.resourceId, campaign?.resourceId, null),
    allowedPricingTypes: Array.isArray(firstDefined(voucher.allowedPricingTypes, campaign?.allowedPricingTypes))
      ? firstDefined(voucher.allowedPricingTypes, campaign?.allowedPricingTypes)
      : null,
    allowedBasePrice: firstDefined(voucher.allowedBasePrice, campaign?.allowedBasePrice, null),
    allowedBaseMode: firstDefined(voucher.allowedBaseMode, campaign?.allowedBaseMode, null),
    discountAmount: number(firstDefined(voucher.discountAmount, campaign?.discountAmount), LEGACY_DEFAULT_DISCOUNT),
    discountPercent: number(firstDefined(voucher.discountPercent, campaign?.discountPercent), 0),
    maxDiscountAmount: number(firstDefined(voucher.maxDiscountAmount, campaign?.maxDiscountAmount), 0),
    minFinalPrice: Math.max(0, number(firstDefined(voucher.minFinalPrice, campaign?.minFinalPrice), 0)),
  };
}

// Evaluate one exact code against a base quote. The caller must have loaded
// vouchers/{CODE}; campaign metadata never grants a voucher on its own.
export function evaluateVoucher(input = {}) {
  const {
    voucher = null, campaign = null, code = null, nowMs = Date.now(), lineUserId = null,
    date, startTime, durationMinutes = 60, isHoliday = false,
    branchId = null, resourceId = null, baseQuote = null, bookingId = null,
  } = input;
  if (!voucher) return { ok: false, reason: 'not_found' };
  if (voucher.campaignId && !campaign) return { ok: false, reason: 'campaign_not_found' };

  const def = resolveVoucherDefinition(voucher, campaign);
  if (!def.voucherType) return { ok: false, reason: 'invalid_type' };
  if (!def.active) return { ok: false, reason: 'inactive' };
  if (def.validFrom !== null && nowMs < def.validFrom) return { ok: false, reason: 'not_started' };
  if (def.expiresAt !== null && nowMs > def.expiresAt) return { ok: false, reason: 'expired' };

  if (def.lifecycleMode === 'legacy_used_count') {
    if (def.maxUses <= 0 || def.usedCount >= def.maxUses) return { ok: false, reason: 'used_up' };
  } else {
    const staleReservation = def.state === 'reserved' && def.reservedUntil !== null && def.reservedUntil <= nowMs;
    const ownReservation = def.state === 'reserved' && bookingId && def.reservedBookingId === bookingId;
    if (['disabled', 'expired'].includes(def.state)) return { ok: false, reason: 'inactive' };
    if (def.state === 'redeemed' || def.usedCount >= Math.max(1, def.maxUses)) return { ok: false, reason: 'used_up' };
    if (def.state === 'reserved' && !staleReservation && !ownReservation) return { ok: false, reason: 'reserved' };
    if (!['available', 'reserved'].includes(def.state)) return { ok: false, reason: 'inactive' };
  }

  if (def.issuedTo && !def.transferable && def.issuedTo !== lineUserId) return { ok: false, reason: 'wrong_owner' };
  if (def.issuedTo && def.lifecycleMode === 'legacy_used_count' && def.issuedTo !== lineUserId) return { ok: false, reason: 'wrong_owner' };
  if (def.requiresLineLogin && (!lineUserId || lineUserId === 'guest')) return { ok: false, reason: 'line_login_required' };

  const dow = dayOfWeek(date);
  if (def.allowedDays && !def.allowedDays.includes(dow)) return { ok: false, reason: 'day_not_allowed' };
  if (def.excludeHolidays && isHoliday) return { ok: false, reason: 'holiday_not_allowed' };

  const start = minuteOfDay(startTime);
  const end = start === null ? null : start + Number(durationMinutes || 0);
  const windowStart = def.startTime ? minuteOfDay(def.startTime) : null;
  const windowEnd = def.endTime ? minuteOfDay(def.endTime) : null;
  if (windowStart !== null && (start === null || start < windowStart)) return { ok: false, reason: 'time_not_allowed' };
  if (windowEnd !== null && (end === null || end > windowEnd)) return { ok: false, reason: 'time_not_allowed' };
  if (def.exactDurationMinutes > 0 && Number(durationMinutes) !== def.exactDurationMinutes) return { ok: false, reason: 'duration_not_allowed' };
  if (def.allowedDurations && !def.allowedDurations.includes(Number(durationMinutes))) return { ok: false, reason: 'duration_not_allowed' };
  if (def.branchId && def.branchId !== branchId) return { ok: false, reason: 'branch_not_allowed' };
  if (def.resourceId && def.resourceId !== resourceId) return { ok: false, reason: 'resource_not_allowed' };

  const originalPrice = Math.max(0, number(baseQuote?.finalPrice, number(baseQuote?.originalPrice, 0)));
  const pricingType = baseQuote?.pricingType || 'standard';
  if (def.allowedPricingTypes && !def.allowedPricingTypes.includes(pricingType)) return { ok: false, reason: 'not_applicable' };

  // Legacy compatibility: the old 350 marker means “the current standard
  // price”, while non-default or explicit exact values remain pinned.
  if (def.lifecycleMode === 'legacy_used_count') {
    const configuredBase = Number(def.allowedBasePrice);
    const exact = Number.isFinite(configuredBase) && configuredBase > 0 &&
      (def.allowedBaseMode === 'exact' || configuredBase !== 350);
    if (pricingType !== 'standard' || (exact && originalPrice !== configuredBase)) {
      return { ok: false, reason: 'not_applicable' };
    }
  }

  let discountAmount = 0;
  if (def.voucherType === VOUCHER_TYPES.FREE_BOOKING) {
    discountAmount = originalPrice;
  } else if (def.voucherType === VOUCHER_TYPES.DISCOUNT_PERCENT) {
    const percent = Math.min(100, Math.max(0, def.discountPercent));
    discountAmount = Math.round(originalPrice * percent / 100);
    if (def.maxDiscountAmount > 0) discountAmount = Math.min(discountAmount, def.maxDiscountAmount);
  } else {
    discountAmount = Math.max(0, def.discountAmount);
  }
  discountAmount = Math.min(discountAmount, Math.max(0, originalPrice - def.minFinalPrice));
  const finalPrice = Math.max(def.minFinalPrice, originalPrice - discountAmount);

  return {
    ok: true,
    code: String(code || '').toUpperCase(),
    voucherType: def.voucherType,
    lifecycleMode: def.lifecycleMode,
    campaignId: def.campaignId,
    campaignName: def.campaignName,
    keyword: def.keyword,
    codePrefix: def.codePrefix,
    originalPrice,
    finalPrice,
    discountAmount,
    isFree: def.voucherType === VOUCHER_TYPES.FREE_BOOKING && finalPrice === 0,
    definition: def,
  };
}

export function applyVoucherToQuote(baseQuote, result) {
  if (!result?.ok) {
    return { ...baseQuote, voucherApplied: false, voucherCode: null, discountAmount: 0, voucherReason: result?.reason || 'not_found' };
  }
  return {
    ...baseQuote,
    finalPrice: result.finalPrice,
    price: result.finalPrice,
    amount: result.finalPrice,
    qrAmount: result.finalPrice,
    voucherApplied: true,
    voucherReason: null,
    voucherCode: result.code,
    voucherType: result.voucherType,
    voucherLifecycle: result.lifecycleMode,
    voucherCampaignId: result.campaignId,
    voucherCampaignName: result.campaignName,
    voucherKeyword: result.keyword,
    discountAmount: result.discountAmount,
    isFreeVoucher: result.isFree,
  };
}

export function reserveVoucherUpdate(voucher, { bookingId, bookingCode, lineUserId, reservedUntil, timestamp }) {
  return {
    state: 'reserved', reservedBookingId: bookingId, reservedBookingCode: bookingCode,
    reservedBy: lineUserId || null, reservedUntil, reservedAt: timestamp, updatedAt: timestamp,
  };
}

export function redeemVoucherUpdate(voucher, { bookingId, bookingCode, lineUserId, timestamp }) {
  return {
    state: 'redeemed', usedCount: integer(voucher?.usedCount, 0) + 1,
    redeemedBookingId: bookingId, redeemedBookingCode: bookingCode,
    redeemedBy: lineUserId || null, redeemedAt: timestamp,
    reservedBookingId: null, reservedBookingCode: null, reservedBy: null, reservedUntil: null,
    lastUsedAt: timestamp, lastUsedBy: lineUserId || null, lastUsedBooking: bookingCode,
    updatedAt: timestamp,
  };
}

export function releaseVoucherUpdate(voucher, { bookingId, reason, timestamp, countRestore = false }) {
  const restores = integer(voucher?.cancellationRestoreCount, 0);
  const maxRestores = integer(voucher?.maxCancellationRestores, 0);
  const mayRestore = !countRestore || restores < maxRestores;
  if (!mayRestore) return { restored: false, update: { updatedAt: timestamp } };
  return {
    restored: true,
    update: {
      state: 'available',
      usedCount: countRestore ? Math.max(0, integer(voucher?.usedCount, 0) - 1) : integer(voucher?.usedCount, 0),
      cancellationRestoreCount: countRestore ? restores + 1 : restores,
      reservedBookingId: null, reservedBookingCode: null, reservedBy: null, reservedUntil: null,
      lastReleasedBookingId: bookingId, lastReleaseReason: reason || null, lastReleasedAt: timestamp,
      updatedAt: timestamp,
    },
  };
}
