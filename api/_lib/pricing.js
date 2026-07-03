// ════════════════════════════════════════════════════════════════════
// Pricing engine (Pricing System v2 — 2026-07). SERVER SOURCE OF TRUTH.
// ════════════════════════════════════════════════════════════════════
// Pure/read-only: callers pass in the pricing config + holiday flag + optional
// voucher doc; this module computes the quote. NO Firestore access here so it
// stays deterministic and unit-testable. Kept in _lib (not a routed function).
//
// Superset of the existing live pricing (do NOT break these):
//   • standard 350                     (qrType "normal")
//   • late_night 450  startHour 0-5    (qrType "late_night")   — index.html LN_START=0..LN_END=6
//   • special_promotion  from system_settings/pricing          (qrType "special")
// New in v2:
//   • morning_weekday 330 / morning_weekday_advance 320
//       Mon-Fri, startHour 06:00-11:00 (incl 11), non-holiday; >=48h→320 else 330
//   • voucher overlay: ONLY on base standard 350 → -discount(50) → 300; no stacking.
//
// Precedence for a single-use booking (highest first):
//   late_night → special_promotion → morning → standard,  then voucher overlay
//   (voucher applies only when the resulting base is "standard"/350).
// NOTE(assumption): special_promotion outranks morning (admin campaign wins);
//   this preserves the existing promo-over-standard behaviour. Confirm if not.
// ════════════════════════════════════════════════════════════════════

export const PRICE_RULE_VERSION = '2026-07-v2';

const STANDARD_PRICE          = 350;
const LATE_NIGHT_PRICE        = 450;
const MORNING_PRICE           = 330;  // advanceHours < 48
const MORNING_ADVANCE_PRICE   = 320;  // advanceHours >= 48
const MORNING_ADVANCE_HOURS   = 48;
const VOUCHER_DEFAULT_DISCOUNT = 50;
const LN_START = 0, LN_END = 6;       // late-night hours [0,6) — mirrors index.html

// Day-of-week for a calendar date (tz-safe): 0=Sun..6=Sat.
function dowOf(dateISO) {
  const [y, m, d] = String(dateISO).split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d)).getUTCDay();
}

// Is the promo config active right now? Mirrors getPromoState() in index.html.
function promoActiveNow(cfg, nowMs) {
  if (!cfg || cfg.specialPromoActive !== true) return null;
  const starts = cfg.specialPromoStartsAt?.toMillis?.() ?? null;
  const ends   = cfg.specialPromoEndsAt?.toMillis?.()   ?? null;
  if (starts !== null && nowMs < starts) return null;
  if (ends   !== null && nowMs > ends)   return null;
  return {
    price: Number(cfg.specialPromoPrice) || STANDARD_PRICE,
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
  // Voucher only stacks on a plain standard 350 base.
  const allowedBase = Number(v.allowedBasePrice) || STANDARD_PRICE;
  if (ctx.pricingType !== 'standard' || ctx.originalPrice !== allowedBase) {
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
  const promo = promoActiveNow(promoConfig, nowMs);

  if (startHour >= LN_START && startHour < LN_END) {
    pricingType = 'late_night'; originalPrice = LATE_NIGHT_PRICE; qrType = 'late_night';
  } else if (promo) {
    pricingType = 'special_promotion'; originalPrice = promo.price; qrType = 'special'; promoCode = promo.name;
  } else if (morningEligible) {
    if (advanceHoursRaw >= MORNING_ADVANCE_HOURS) { pricingType = 'morning_weekday_advance'; originalPrice = MORNING_ADVANCE_PRICE; }
    else { pricingType = 'morning_weekday'; originalPrice = MORNING_PRICE; }
    qrType = 'normal';
  } else {
    pricingType = 'standard'; originalPrice = STANDARD_PRICE; qrType = 'normal';
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
