// Coach Add-on / Mixed Payment v2 domain rules.
//
// This module is deliberately Firestore-free.  Pricing and state vocabulary
// can be tested without credentials or an emulator, while the API handlers
// remain responsible for authentication and atomic persistence.

export const COACH_ADDON_V2_SCHEMA_VERSION = 2;
export const COACH_ADDON_V2_MIN_MINUTES = 60;
export const COACH_ADDON_V2_MAX_MINUTES = 180;
export const COACH_ADDON_V2_STEP_MINUTES = 30;
export const COACH_ADDON_V2_EXTRA_PERSON_FEE = 100;

export const COACH_ADDON_V2_FLAG = 'enableCoachAddonV2';

export const COACH_ADDON_V2_BOOKING_STATES = Object.freeze([
  'held', 'confirmed', 'cancelled', 'completed', 'expired',
]);
export const COACH_ADDON_V2_CASH_STATES = Object.freeze([
  'not_required', 'unpaid', 'pending_review', 'paid', 'refunded',
]);
export const COACH_ADDON_V2_PACKAGE_STATES = Object.freeze([
  'reserved', 'consumed', 'released',
]);

export const COACH_ADDON_V2_ROOM_PACKAGE_TYPES = Object.freeze([
  'ultra_starter_3', 'ultra_pass_10', 'ultra_pass_20', 'ultra_10', 'ultra_20',
]);
export const COACH_ADDON_V2_COACHING_PACKAGE_TYPES = Object.freeze([
  'beginner_coaching_5',
]);

// Intentionally excluded until product semantics are explicitly supplied.
export const COACH_ADDON_V2_BLOCKED_PACKAGE_TYPES = Object.freeze([
  'coach_at_ultra_10',
]);

const money = value => Math.round((Number(value) + Number.EPSILON) * 100) / 100;

export function isCoachAddonV2Duration(durationMinutes) {
  return Number.isInteger(durationMinutes) &&
    durationMinutes >= COACH_ADDON_V2_MIN_MINUTES &&
    durationMinutes <= COACH_ADDON_V2_MAX_MINUTES &&
    durationMinutes % COACH_ADDON_V2_STEP_MINUTES === 0;
}

export function coachAddonV2PackageKind(packageType) {
  const type = String(packageType || '');
  if (COACH_ADDON_V2_ROOM_PACKAGE_TYPES.includes(type)) return 'ultra_pass';
  if (COACH_ADDON_V2_COACHING_PACKAGE_TYPES.includes(type)) return 'coaching_package';
  return null;
}

export function coachClaimCellStarts(startTime, durationMinutes) {
  if (!/^\d{2}:\d{2}$/.test(String(startTime || '')) || !isCoachAddonV2Duration(durationMinutes)) return null;
  const [hour, minute] = String(startTime).split(':').map(Number);
  if (!Number.isInteger(hour) || hour < 0 || hour > 23 || ![0, 30].includes(minute)) return null;
  const start = hour * 60 + minute;
  if (durationMinutes % 60 === 0 && minute !== 0) return null;
  if (start + durationMinutes > 1440) return null;
  const cells = [];
  for (let value = start; value < start + durationMinutes; value += 30) {
    cells.push(`${String(Math.floor(value / 60)).padStart(2, '0')}:${String(value % 60).padStart(2, '0')}`);
  }
  return cells;
}

export function coachClaimId(coachId, date, cellStart) {
  return `${String(coachId)}_${String(date)}_${String(cellStart).replace(':', '')}`;
}

/**
 * Server-authoritative Coach Add-on v2 calculator.
 *
 * fundingMode:
 *   cash             court and coach are paid in cash
 *   ultra_pass       pass covers court; cash covers coach
 *   coaching_package Beginner Coaching covers court + coach
 */
export function calculateCoachAddonV2Price({
  durationMinutes,
  fundingMode,
  courtGrossAmount = 0,
  coachRatePerHour,
  coachPayoutRatePerHour,
  studentCount = 1,
}) {
  if (!isCoachAddonV2Duration(durationMinutes)) {
    throw new Error('INVALID_DURATION');
  }
  if (!['cash', 'ultra_pass', 'coaching_package'].includes(fundingMode)) {
    throw new Error('INVALID_FUNDING_MODE');
  }
  if (![1, 2].includes(studentCount)) throw new Error('INVALID_STUDENT_COUNT');

  const courtGross = Number(courtGrossAmount);
  const coachRate = Number(coachRatePerHour);
  const payoutRate = Number(coachPayoutRatePerHour);
  if (!Number.isFinite(courtGross) || courtGross < 0) throw new Error('INVALID_COURT_AMOUNT');
  if (!Number.isFinite(coachRate) || coachRate <= 0) throw new Error('INVALID_COACH_RATE');
  if (!Number.isFinite(payoutRate) || payoutRate <= 0) throw new Error('INVALID_PAYOUT_RATE');

  const ratio = durationMinutes / 60;
  const extraPersonFee = studentCount === 2 ? COACH_ADDON_V2_EXTRA_PERSON_FEE : 0;
  const extraPersonCoachPayout = extraPersonFee;
  const coachGrossCharge = money(coachRate * ratio);
  const coachBasePayoutAmount = money(payoutRate * ratio);
  const packageCoversCourt = fundingMode !== 'cash';
  const packageCoversCoach = fundingMode === 'coaching_package';
  const courtCashAmount = packageCoversCourt ? 0 : money(courtGross);
  const courtPackageMinutes = packageCoversCourt ? durationMinutes : 0;
  const coachChargeAmount = packageCoversCoach ? 0 : coachGrossCharge;
  const coachPayoutAmount = money(coachBasePayoutAmount + extraPersonCoachPayout);
  const cashDueAmount = money(courtCashAmount + coachChargeAmount + extraPersonFee);
  const fundingSource = fundingMode === 'cash'
    ? 'cash'
    : cashDueAmount > 0 ? 'mixed' : fundingMode;

  return Object.freeze({
    serviceCategory: 'coach_lesson',
    fundingSource,
    fundingMode,
    durationMinutes,
    studentCount,
    courtGrossAmount: money(courtGross),
    courtCashAmount,
    courtPackageMinutes,
    coachRatePerHour: money(coachRate),
    coachGrossChargeAmount: coachGrossCharge,
    coachChargeAmount,
    coachPayoutRatePerHour: money(payoutRate),
    coachBasePayoutAmount,
    extraPersonFee,
    extraPersonCoachPayout,
    coachPayoutAmount,
    cashDueAmount,
    cashPaidAmount: 0,
  });
}

export function initialCoachAddonV2States(price) {
  const hasPackage = price.courtPackageMinutes > 0;
  const needsCash = price.cashDueAmount > 0;
  return Object.freeze({
    bookingState: needsCash ? 'held' : 'confirmed',
    cashState: needsCash ? 'unpaid' : 'not_required',
    packageUsageState: hasPackage ? (needsCash ? 'reserved' : 'consumed') : null,
    legacyBookingStatus: needsCash ? 'pending_payment' : 'confirmed',
    legacyPaymentStatus: needsCash ? 'unpaid' : 'package',
  });
}

export function isCoachAddonV2Booking(booking) {
  return Number(booking?.coachAddonSchemaVersion) === COACH_ADDON_V2_SCHEMA_VERSION &&
    booking?.serviceCategory === 'coach_lesson';
}
