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
