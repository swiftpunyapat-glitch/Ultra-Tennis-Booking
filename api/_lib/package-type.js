// Accounting edits used to rewrite a booking's packageType to the pre-catalog
// aliases (ultra_10 / ultra_20), while customer_packages docs have always
// carried the catalog keys (ultra_pass_10 / ultra_pass_20). Bookings written
// the old way are still live, so every packageType comparison canonicalises
// both sides instead of matching the raw strings — otherwise restoring a pass
// balance on those bookings fails with PASS_RESTORE_MISMATCH.
const PACKAGE_TYPE_ALIASES = {
  ultra_10: 'ultra_pass_10',
  ultra_20: 'ultra_pass_20',
};

export function canonicalPackageType(value) {
  const type = String(value || '');
  return PACKAGE_TYPE_ALIASES[type] || type;
}

export function samePackageType(a, b) {
  return canonicalPackageType(a) === canonicalPackageType(b);
}
