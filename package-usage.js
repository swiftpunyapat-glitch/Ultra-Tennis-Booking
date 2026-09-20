// Ultra Pass usage reporting — shared by admin.html and ultra-finance.html.
//
// A pass booking identifies its product three different ways, all of them live:
//   • packageType "ultra_pass_10" / "ultra_pass_20" — the catalog keys, written
//     by every booking made through the customer pass flow
//   • packageType "ultra_10" / "ultra_20" — the pre-catalog aliases that
//     accounting edits used to write
//   • bookingType "Ultra Pass 1" / "Ultra Pass 2" — bookings from before
//     packageType existed, plus what an accounting edit still stamps
//
// The reports used to match the aliases and bookingType only, so every booking
// a customer made with their own pass was missing from Package Usage Value.
// Each page had its own copy of the rule and the copies had drifted apart, so
// the matching lives here and the pages keep only their own arithmetic.

const TIER_BY_PACKAGE_TYPE = {
  ultra_pass_10: 1, ultra_10: 1,
  ultra_pass_20: 2, ultra_20: 2,
};

const TIER_BY_BOOKING_TYPE = {
  "Ultra Pass 1": 1,
  "Ultra Pass 2": 2,
};

const RATE_BY_TIER = { 1: 310, 2: 295 };

export function ultraPassTier(b) {
  return TIER_BY_PACKAGE_TYPE[b?.packageType] ?? TIER_BY_BOOKING_TYPE[b?.bookingType] ?? null;
}

export function isUltraPassUsage(b) {
  return b?.paymentStatus === "package" && ultraPassTier(b) !== null;
}

export function ultraPassRate(b) {
  return RATE_BY_TIER[ultraPassTier(b)] ?? 0;
}

export function ultraPassLabel(b) {
  const tier = ultraPassTier(b);
  return tier ? `Ultra Pass ${tier}` : "";
}

// Hours a booking consumed. Bookings carry durationMinutes and durationHours;
// the start/end fallback covers records written before those fields existed,
// and an hour is the smallest slot that was ever bookable.
export function bookingHours(b) {
  const minutes = Number(b?.durationMinutes);
  if (Number.isFinite(minutes) && minutes > 0) return minutes / 60;
  const hours = Number(b?.durationHours);
  if (Number.isFinite(hours) && hours > 0) return hours;
  const at = value => {
    const m = String(value || "").match(/^(\d{2}):(\d{2})$/);
    return m ? Number(m[1]) * 60 + Number(m[2]) : null;
  };
  const start = at(b?.startTime), end = at(b?.endTime);
  return start !== null && end !== null && end > start ? (end - start) / 60 : 1;
}

// What a pass booking is worth in the reports. api/booking.js does not stamp
// packageUsageValueTotal — only an accounting edit does — so the value has to
// be derived for every booking a customer made with their own pass. Anything
// that is not an Ultra Pass is worth 0 here: the other products have their own
// economics and no rate to apply.
export function ultraPassUsageValue(b, hours = bookingHours(b)) {
  const stored = Number(b?.packageUsageValueTotal);
  if (stored) return stored;
  const rate = Number(b?.packageUsageValuePerHour) || ultraPassRate(b);
  return (Number(hours) || 0) * rate;
}
