// ════════════════════════════════════════════════════════════════════
// Pricing engine (Pricing System v2 — 2026-07). SERVER SOURCE OF TRUTH.
// ════════════════════════════════════════════════════════════════════
// Pure/read-only: callers pass in the pricing config + holiday flag + optional
// voucher doc; this module computes the quote. NO Firestore access here so it
// stays deterministic and unit-testable. Kept in _lib (not a routed function).
//
// Superset of the existing live pricing (do NOT break these):
//   • standard (default 350)            (qrType "normal")
//   • late_night (default 450) 00:00-05:59 (qrType "late_night")
//   • special_promotion  from system_settings/pricing          (qrType "special")
// New in v2:
//   • morning_weekday defaults 330 / advance 320
//       Mon-Fri, startHour 06:00-11:00 (incl 11), non-holiday; >=48h→320 else 330
//   • voucher overlay: ONLY on the standard rate → discount; no stacking.
//
// Precedence for a single-use booking (highest first):
//   late_night → special_promotion → morning → standard,  then voucher overlay
//   (voucher applies only when the resulting base type is "standard").
// NOTE(assumption): special_promotion outranks morning (admin campaign wins);
//   this preserves the existing promo-over-standard behaviour. Confirm if not.
// ════════════════════════════════════════════════════════════════════

export const PRICE_RULE_VERSION = '2026-08-v3-dynamic-store-rates';

// Safe fallbacks preserve the exact live behaviour when the Firestore pricing
// document is absent, partially migrated, or contains an invalid value.
export const DEFAULT_STORE_PRICING = Object.freeze({
  normalSingleUsePrice: 350,
  lateNightPrice: 450,
  morningPrice: 330,
  morningAdvancePrice: 320,
  morningAdvanceHours: 48,
});

const VOUCHER_DEFAULT_DISCOUNT = 50;
const LN_START = 0, LN_END = 6;       // late-night hours [0,6) — mirrors index.html

function configuredInteger(cfg, key, fallback, min = 1, max = 100000) {
  const n = Number(cfg?.[key]);
  return Number.isInteger(n) && n >= min && n <= max ? n : fallback;
}

export function resolveStorePricing(cfg) {
  return {
    normalSingleUsePrice: configuredInteger(cfg, 'normalSingleUsePrice', DEFAULT_STORE_PRICING.normalSingleUsePrice),
    lateNightPrice: configuredInteger(cfg, 'lateNightPrice', DEFAULT_STORE_PRICING.lateNightPrice),
    morningPrice: configuredInteger(cfg, 'morningPrice', DEFAULT_STORE_PRICING.morningPrice),
    morningAdvancePrice: configuredInteger(cfg, 'morningAdvancePrice', DEFAULT_STORE_PRICING.morningAdvancePrice),
    morningAdvanceHours: configuredInteger(cfg, 'morningAdvanceHours', DEFAULT_STORE_PRICING.morningAdvanceHours, 1, 720),
  };
}

// Day-of-week for a calendar date (tz-safe): 0=Sun..6=Sat.
function dowOf(dateISO) {
  const [y, m, d] = String(dateISO).split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d)).getUTCDay();
}

// Is the promo config active right now? Mirrors getPromoState() in index.html.
function promoActiveNow(cfg, nowMs, standardPrice) {
  if (!cfg || cfg.specialPromoActive !== true) return null;
  const starts = cfg.specialPromoStartsAt?.toMillis?.() ?? null;
  const ends   = cfg.specialPromoEndsAt?.toMillis?.()   ?? null;
  if (starts !== null && nowMs < starts) return null;
  if (ends   !== null && nowMs > ends)   return null;
  return {
    price: Number(cfg.specialPromoPrice) || standardPrice,
    name:  typeof cfg.specialPromoName === 'string' ? cfg.specialPromoName : 'special_promotion',
  };
}

// Validate a voucher doc against the computed base. Pure.
function validateVoucher(v, ctx) {
  if (!v) return { ok: false, reason: 'not_found' };
  if (v.active !== true) return { ok: false, reason: 'inactive' };
  const exp = v.expiresAt?.toMillis?.() ?? (typeof v.expiresAt === 'number' ? v.expiresAt : null);
  if (exp !== null && exp < ctx.nowMs) return { ok: false, reason: 'expired' };
  if ((Number(v.usedCount) || 0) >= (Number(v.maxUses) || 0)) return { ok: false, reason: 'used_up' };
  // Legacy vouchers commonly carry allowedBasePrice=350 to mean "standard
  // rate". They keep working when Art changes the current standard price. A
  // non-default allowedBasePrice (or allowedBaseMode="exact") stays pinned.
  const configuredBase = Number(v.allowedBasePrice);
  const exactBaseRequired = Number.isFinite(configuredBase) && configuredBase > 0
    && (v.allowedBaseMode === 'exact' || configuredBase !== DEFAULT_STORE_PRICING.normalSingleUsePrice);
  if (ctx.pricingType !== 'standard' || (exactBaseRequired && ctx.originalPrice !== configuredBase)) {
    return { ok: false, reason: 'not_applicable' };
  }
  if (v.issuedTo && v.issuedTo !== ctx.lineUserId) return { ok: false, reason: 'wrong_owner' };
  return { ok: true, discountAmount: Number(v.discountAmount) || VOUCHER_DEFAULT_DISCOUNT };
}

// Compute a full pricing quote. Read-only / deterministic.
//   { date:"YYYY-MM-DD", startTime:"HH:mm", nowMs, isHoliday:bool,
//     promoConfig:(system_settings/pricing data|null), payType?:"single"|"ultra"|"offpeak"|"event",
//     voucherCode?:string|null, voucher?:(vouchers/{code} data|null), lineUserId?:string }
export function computeQuote(input) {
  const {
    date, startTime, nowMs = Date.now(), isHoliday = false,
    promoConfig = null, payType = 'single',
    voucherCode = null, voucher = null, lineUserId = null,
  } = input || {};

  const startHour = parseInt(String(startTime).slice(0, 2), 10);
  const storePricing = resolveStorePricing(promoConfig);
  const dow = dowOf(date);
  const isWeekend = dow === 0 || dow === 6;
  const isWeekday = dow >= 1 && dow <= 5;

  const startMs = Date.parse(`${date}T${startTime}:00+07:00`);
  const advanceHoursRaw = Number.isFinite(startMs) ? Math.max(0, (startMs - nowMs) / 3_600_000) : 0;
  const advanceHours = Math.round(advanceHoursRaw * 10) / 10;

  // Morning promo kill-switch — admin-controlled via system_settings/pricing
  // .morningPromoActive. Absent/undefined = ON (preserves live behaviour);
  // only an explicit false disables the 330/320 morning rates.
  const morningEnabled  = !promoConfig || promoConfig.morningPromoActive !== false;
  const morningEligible = morningEnabled && isWeekday && startHour >= 6 && startHour <= 11 && !isHoliday;

  // Passes (ultra/offpeak/event) are price 0, no QR, no payment — informational.
  if (payType && payType !== 'single') {
    return {
      pricingType: 'package', originalPrice: 0, finalPrice: 0, price: 0, amount: 0,
      qrAmount: 0, qrType: null, promoCode: null, voucherCode: null, discountAmount: 0,
      priceRuleVersion: PRICE_RULE_VERSION,
      isHoliday: !!isHoliday, isWeekend, isMorningWeekday: morningEligible, advanceHours,
      voucherApplied: false, voucherReason: null,
    };
  }

  // ── Base pricing type (precedence) ────────────────────────────────
  let pricingType, originalPrice, qrType, promoCode = null;
  const promo = promoActiveNow(promoConfig, nowMs, storePricing.normalSingleUsePrice);

  if (startHour >= LN_START && startHour < LN_END) {
    pricingType = 'late_night'; originalPrice = storePricing.lateNightPrice; qrType = 'late_night';
  } else if (promo) {
    pricingType = 'special_promotion'; originalPrice = promo.price; qrType = 'special'; promoCode = promo.name;
  } else if (morningEligible) {
    if (advanceHoursRaw >= storePricing.morningAdvanceHours) { pricingType = 'morning_weekday_advance'; originalPrice = storePricing.morningAdvancePrice; }
    else { pricingType = 'morning_weekday'; originalPrice = storePricing.morningPrice; }
    qrType = 'normal';
  } else {
    pricingType = 'standard'; originalPrice = storePricing.normalSingleUsePrice; qrType = 'normal';
  }

  // ── Voucher overlay (standard base only) ──────────────────────────
  let finalPrice = originalPrice, discountAmount = 0, appliedVoucher = null;
  let voucherApplied = false, voucherReason = null;
  if (voucherCode) {
    const v = validateVoucher(voucher, { nowMs, lineUserId, pricingType, originalPrice });
    if (v.ok) {
      voucherApplied = true; appliedVoucher = String(voucherCode);
      discountAmount = v.discountAmount;
      finalPrice = Math.max(0, originalPrice - discountAmount);
    } else {
      voucherReason = v.reason;
    }
  }

  return {
    pricingType,
    originalPrice,
    finalPrice,
    price:  finalPrice,   // compat with existing bookings.price
    amount: finalPrice,   // charge amount
    qrAmount: finalPrice,
    qrType,
    promoCode,
    voucherCode: appliedVoucher,
    discountAmount,
    priceRuleVersion: PRICE_RULE_VERSION,
    isHoliday: !!isHoliday,
    isWeekend,
    isMorningWeekday: morningEligible,
    advanceHours,
    voucherApplied,
    voucherReason,
  };
}
