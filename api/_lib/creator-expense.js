// ════════════════════════════════════════════════════════════════════
// creator-expense — marketing expense for free court time
// ════════════════════════════════════════════════════════════════════
// A free slot given to a creator/influencer is a barter deal, not a sale:
// no cash arrives, but a sellable court hour is spent. Recording it only as
// price 0 makes it vanish from the books — it is not revenue (that filter
// wants paymentStatus "paid"), and it is not package usage (that filter
// wants an Ultra Pass packageType), so nothing in the P&L ever sees it.
//
// The policy is to book it as a Marketing expense at the slot's notional
// value. Two call sites produce that expense — the Art-only accounting edit
// and the free-voucher booking route — so the document shape lives here
// rather than in either one, to stop the two from drifting apart.
//
// These are pure builders in the voucher-engine style: the caller injects
// `timestamp` and performs the write with whatever writer it holds (a
// transaction, a batch), which keeps the module unit-testable with no
// Firestore emulator.

export const CREATOR_EXPENSE_CATEGORY     = 'Marketing';
export const CREATOR_EXPENSE_SOURCE_TYPE  = 'influencer_free_slot';
export const CREATOR_EXPENSE_VENDOR       = 'Influencer Free Slot';
export const CREATOR_EXPENSE_METHOD       = 'Other';
export const CREATOR_EXPENSE_BUSINESS_UNIT = 'ultra_tennis';
// Legacy flat rate. Pricing v2 prices mornings at 330/320 and late night at
// 450, so this is only reached when a booking carries no usable stored price.
export const CREATOR_EXPENSE_HOURLY_FALLBACK = 350;

function positiveNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

// The value of the court hour being given away. Prefer what the booking was
// actually priced at over any flat rate, so an off-peak giveaway is not
// booked at a peak-hour cost.
export function resolveCreatorExpenseAmount({
  explicitAmount = null,
  storedValue = null,
  durationHours = 1,
  hourlyRate = CREATOR_EXPENSE_HOURLY_FALLBACK,
} = {}) {
  if (explicitAmount != null) {
    const explicit = Number(explicitAmount);
    if (Number.isFinite(explicit)) return Math.max(1, explicit);
  }
  const stored = positiveNumber(storedValue);
  if (stored != null) return Math.max(1, Math.ceil(stored));
  const hours = positiveNumber(durationHours) ?? 1;
  const rate  = positiveNumber(hourlyRate) ?? CREATOR_EXPENSE_HOURLY_FALLBACK;
  return Math.max(1, Math.ceil(hours * rate));
}

// Human-readable trace back to the booking, for the expense row in Finance.
export function buildCreatorExpenseNote({
  bookingCode = null, bookingId = null, customerName = '',
  customerPhone = '', date = '', startTime = '', endTime = '',
} = {}) {
  return [
    `Auto: ${bookingCode || bookingId}`,
    customerName  ? `- ${customerName}`  : '',
    customerPhone ? `(${customerPhone})` : '',
    date          ? `plays ${date}`      : '',
    (startTime && endTime) ? `${startTime}–${endTime}` : '',
  ].filter(Boolean).join(' ').slice(0, 400);
}

// A new finance_expenses document. `createdBy` names whoever caused it — an
// admin for a manual reclassification, the campaign for a self-serve
// redemption — so Finance can tell the two apart.
export function buildCreatorExpenseDoc({
  amount, note, date, bookingId,
  vendor = CREATOR_EXPENSE_VENDOR,
  createdBy = null,
  campaignId = null,
  voucherCode = null,
  timestamp = null,
} = {}) {
  return {
    businessUnit:    CREATOR_EXPENSE_BUSINESS_UNIT,
    date:            date || new Date().toISOString().slice(0, 10),
    category:        CREATOR_EXPENSE_CATEGORY,
    amount,
    paymentMethod:   CREATOR_EXPENSE_METHOD,
    vendor:          String(vendor || CREATOR_EXPENSE_VENDOR).slice(0, 200),
    note,
    deleted:         false,
    autoCreated:     true,
    sourceType:      CREATOR_EXPENSE_SOURCE_TYPE,
    sourceBookingId: bookingId,
    ...(campaignId  ? { sourceCampaignId: campaignId } : {}),
    ...(voucherCode ? { sourceVoucherCode: voucherCode } : {}),
    addedByAdmin:    createdBy,
    createdAt:       timestamp,
    updatedAt:       timestamp,
  };
}

// Re-point an existing expense at a corrected amount rather than stacking a
// second row on the same booking.
export function buildCreatorExpenseUpdate({ amount, note, adminName = null, timestamp = null } = {}) {
  return {
    amount,
    note,
    updatedByAdmin: adminName,
    updatedAt:      timestamp,
  };
}

// Soft-delete: the giveaway was undone, so the cost must come back out of
// the P&L. Finance filters on `deleted`, and the row is kept for audit.
export function buildCreatorExpenseDelete({ adminName = null, timestamp = null } = {}) {
  return {
    deleted:   true,
    deletedAt: timestamp,
    deletedBy: adminName,
  };
}
