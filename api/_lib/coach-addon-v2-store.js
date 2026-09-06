import { FieldValue } from 'firebase-admin/firestore';
import { isCoachAddonV2Booking, coachClaimCellStarts, coachClaimId, coachAddonV2PackageKind } from './coach-addon-v2.js';

const PACKAGE_LOGS = 'customer_package_logs';

const millis = value => value?.toMillis?.() ?? (Number.isFinite(Number(value)) ? Number(value) : null);

export function isActiveCoachClaim(claim, nowMs = Date.now()) {
  if (!claim) return false;
  if (claim.status === 'confirmed' || claim.status === 'pending_review') return true;
  if (claim.status !== 'held') return false;
  const expiresAt = millis(claim.expiresAt);
  return expiresAt === null || expiresAt > nowMs;
}

function refsForBooking(db, booking) {
  const courtSlotIds = Array.isArray(booking.bookingSlotIds) ? booking.bookingSlotIds.filter(Boolean) : [];
  const coachClaimIds = Array.isArray(booking.coachClaimIds) ? booking.coachClaimIds.filter(Boolean) : [];
  const packageId = String(booking.packageId || booking.usedPackageId || '').trim();
  return {
    courtSlotRefs: courtSlotIds.map(id => db.collection('booking_slots').doc(id)),
    courtClaimRefs: courtSlotIds.map(id => db.collection('booking_slot_claims').doc(id)),
    coachClaimRefs: coachClaimIds.map(id => db.collection('coach_slot_claims').doc(id)),
    packageRef: packageId ? db.collection('customer_packages').doc(packageId) : null,
    packageId,
  };
}

async function readTransitionDocs(t, db, bookingRef, outerBooking, validateClaims = false) {
  // Read the booking and resources together, avoiding incremental acquisition
  // of booking then claim locks while a competing release is committing.
  if (validateClaims) assertCompleteClaims(outerBooking);
  const refs = refsForBooking(db, outerBooking);
  const targets = [
    bookingRef,
    ...refs.courtSlotRefs,
    ...refs.courtClaimRefs,
    ...refs.coachClaimRefs,
    ...(refs.packageRef ? [refs.packageRef] : []),
  ];
  const [bookingSnap, ...snaps] = await t.getAll(...targets);
  if (!bookingSnap.exists) throw new Error('BOOKING_MISSING');
  const booking = bookingSnap.data();
  if (!isCoachAddonV2Booking(booking)) throw new Error('NOT_V2');
  if (validateClaims) assertCompleteClaims(booking);
  // The outer read only selects documents. The locked snapshot must still
  // name exactly those resources; otherwise no transition may be written.
  const current = refsForBooking(db, booking);
  const sameRefs = (a, b) => a.length === b.length && a.every((ref, i) => ref.path === b[i].path);
  if (!sameRefs(refs.courtSlotRefs, current.courtSlotRefs) ||
      !sameRefs(refs.coachClaimRefs, current.coachClaimRefs) || refs.packageId !== current.packageId) {
    throw new Error('CLAIM_CONFLICT');
  }
  let at = 0;
  const courtSlotSnaps = snaps.slice(at, at += refs.courtSlotRefs.length);
  const courtClaimSnaps = snaps.slice(at, at += refs.courtClaimRefs.length);
  const coachClaimSnaps = snaps.slice(at, at += refs.coachClaimRefs.length);
  const packageSnap = refs.packageRef ? snaps[at] : null;
  return { refs, bookingSnap, courtSlotSnaps, courtClaimSnaps, coachClaimSnaps, packageSnap };
}

function assertCompleteClaims(booking) {
  const cells = coachClaimCellStarts(booking.startTime, booking.durationMinutes);
  if (!cells || !booking.coachId || !booking.resourceId || !/^\d{4}-\d{2}-\d{2}$/.test(booking.date)) throw new Error('CLAIM_MISSING');
  const coachIds = cells.map(cell => coachClaimId(booking.coachId, booking.date, cell));
  const courtIds = [];
  for (let i = 0; i < cells.length; i++) {
    courtIds.push(`${booking.resourceId}_${booking.date}_${cells[i].replace(':', '')}`);
    if (cells[i].endsWith(':00') && cells[i + 1]?.endsWith(':30')) i++;
  }
  const matches = (actual, expected) => Array.isArray(actual) &&
    actual.length === expected.length && new Set(actual).size === expected.length &&
    expected.every(id => actual.includes(id));
  if (!matches(booking.coachClaimIds, coachIds) || !matches(booking.bookingSlotIds, courtIds)) throw new Error('CLAIM_MISSING');
}

function assertReservedPackage(booking, refs, docs) {
  const usesPackage = ['ultra_pass', 'coaching_package'].includes(booking.fundingMode);
  if (!usesPackage && booking.packageUsageState !== 'reserved' && !(Number(booking.courtPackageMinutes) > 0)) return;
  if (!refs.packageRef || !docs.packageSnap?.exists) throw new Error('PACKAGE_MISSING');
  const pkg = docs.packageSnap.data();
  if (booking.packageUsageState !== 'reserved' || booking.courtPackageMinutes !== booking.durationMinutes ||
      pkg.lineUserId !== booking.lineUserId || pkg.packageType !== booking.packageType ||
      coachAddonV2PackageKind(pkg.packageType) !== booking.fundingMode ||
      (booking.usedPackageId && booking.usedPackageId !== refs.packageId)) throw new Error('PACKAGE_RESERVATION_INVALID');
  if (!Number.isFinite(pkg.remainingMinutes) || pkg.remainingMinutes < 0) throw new Error('PACKAGE_BALANCE_INVALID');
  // Minutes were already deducted at reservation. Do not deduct again or apply
  // today's expiry/active policy to a previously accepted reservation.
}

function assertOwnedClaims(snaps, bookingId, code) {
  for (const snap of snaps) {
    if (!snap.exists) throw new Error('CLAIM_MISSING');
    const data = snap.data();
    if (data.bookingId !== bookingId || (data.bookingCode && data.bookingCode !== code)) {
      throw new Error('CLAIM_CONFLICT');
    }
  }
}

function releaseWrites(t, db, bookingRef, bookingId, booking, refs, docs, { terminalState, reason, actor }) {
  refs.courtClaimRefs.forEach((ref, index) => {
    const claim = docs.courtClaimSnaps[index];
    if (!claim?.exists || claim.data().bookingId !== bookingId) return;
    t.delete(ref);
    const slot = docs.courtSlotSnaps[index];
    if (slot?.exists) t.delete(refs.courtSlotRefs[index]);
  });
  refs.coachClaimRefs.forEach((ref, index) => {
    const claim = docs.coachClaimSnaps[index];
    if (claim?.exists && claim.data().bookingId === bookingId) t.delete(ref);
  });

  const shouldReleasePackage = booking.packageUsageState === 'reserved' &&
    Number(booking.courtPackageMinutes) > 0 && refs.packageRef;
  if (shouldReleasePackage) {
    if (!docs.packageSnap?.exists) throw new Error('PACKAGE_MISSING');
    const pkg = docs.packageSnap.data();
    const remaining = Number(pkg.remainingMinutes);
    if (!Number.isFinite(remaining)) throw new Error('PACKAGE_BALANCE_INVALID');
    const restored = Number(booking.courtPackageMinutes);
    t.update(refs.packageRef, {
      remainingMinutes: remaining + restored,
      updatedAt: FieldValue.serverTimestamp(),
    });
    const logRef = db.collection(PACKAGE_LOGS).doc();
    t.create(logRef, {
      packageId: refs.packageId,
      lineUserId: booking.lineUserId || '',
      packageType: booking.packageType || booking.usedPackageType || '',
      packageName: booking.packageName || booking.usedPackageName || '',
      action: 'release_reserved_minutes',
      oldRemainingMinutes: remaining,
      newRemainingMinutes: remaining + restored,
      deltaMinutes: restored,
      reason,
      bookingId,
      source: 'coach_addon_v2',
      createdAt: FieldValue.serverTimestamp(),
    });
  }

  const legacyStatus = terminalState === 'expired' ? 'expired' : 'cancelled';
  t.update(bookingRef, {
    bookingState: terminalState,
    bookingStatus: legacyStatus,
    status: legacyStatus,
    paymentStatus: 'rejected',
    ...(shouldReleasePackage ? { packageUsageState: 'released', packageReleasedAt: FieldValue.serverTimestamp() } : {}),
    releaseReason: reason,
    releasedBy: actor,
    releasedAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  });
}

/**
 * Idempotently releases a held v2 booking.  Court, coach and package are
 * restored in the same transaction.  Expiry is lazy-safe: availability and
 * create paths may call this when they encounter an expired deterministic
 * claim, while the customer timer can call it directly as well.
 */
export async function releaseCoachAddonV2Hold(db, bookingId, {
  reason = 'hold_expired', actor = 'system', requireExpired = true, nowMs = Date.now(), terminalState = 'expired',
} = {}) {
  const bookingRef = db.collection('bookings').doc(bookingId);
  const outer = await bookingRef.get();
  if (!outer.exists) return { ok: false, code: 'BOOKING_MISSING' };
  const outerBooking = outer.data();
  if (!isCoachAddonV2Booking(outerBooking)) return { ok: false, code: 'NOT_V2' };
  let outcome = null;

  await db.runTransaction(async t => {
    const docs = await readTransitionDocs(t, db, bookingRef, outerBooking);
    const { refs } = docs;
    if (!docs.bookingSnap.exists) throw new Error('BOOKING_MISSING');
    const booking = docs.bookingSnap.data();
    if (!isCoachAddonV2Booking(booking)) throw new Error('NOT_V2');
    if (['expired', 'cancelled'].includes(booking.bookingState)) {
      outcome = { ok: true, released: false, replayed: true, bookingState: booking.bookingState };
      return;
    }
    if (booking.bookingState !== 'held') throw new Error('NOT_HELD');
    // A slip submitted before the deadline freezes the hold for manual review;
    // the admin may approve it after the original 15-minute deadline.
    if (requireExpired && booking.cashState === 'pending_review') throw new Error('PENDING_REVIEW');
    const expiresAt = millis(booking.paymentExpiresAt);
    if (requireExpired && (expiresAt === null || expiresAt > nowMs)) throw new Error('NOT_EXPIRED');
    releaseWrites(t, db, bookingRef, bookingId, booking, refs, docs, { terminalState, reason, actor });
    outcome = { ok: true, released: true, replayed: false, bookingState: terminalState };
  });
  return outcome;
}

/** Confirm cash and consume any package reservation atomically. */
export async function confirmCoachAddonV2Payment(db, bookingId, {
  actor = 'admin', withoutSlip = false, clock = Date.now, manualPayment = null,
} = {}) {
  const bookingRef = db.collection('bookings').doc(bookingId);
  const outer = await bookingRef.get();
  if (!outer.exists) return { ok: false, code: 'BOOKING_MISSING' };
  const outerBooking = outer.data();
  if (!isCoachAddonV2Booking(outerBooking)) return { ok: false, code: 'NOT_V2' };
  let outcome = null;

  await db.runTransaction(async t => {
    const docs = await readTransitionDocs(t, db, bookingRef, outerBooking, true);
    const { refs } = docs;
    if (!docs.bookingSnap.exists) throw new Error('BOOKING_MISSING');
    const booking = docs.bookingSnap.data();
    if (!isCoachAddonV2Booking(booking)) throw new Error('NOT_V2');
    // mark_paid retains its unpaid-only contract and cannot edit a v2 quote.
    if (manualPayment) {
      if (booking.paymentStatus === 'paid') throw new Error('ALREADY_PAID');
      if (booking.paymentStatus !== 'unpaid' || booking.cashState !== 'unpaid') throw new Error('BAD_STATE');
      if (!Number.isFinite(manualPayment.amount) || manualPayment.amount <= 0 ||
          manualPayment.amount !== Number(booking.cashDueAmount)) throw new Error('AMOUNT_MISMATCH');
    }
    if (booking.bookingState === 'confirmed' && booking.cashState === 'paid') {
      outcome = { ok: true, confirmed: true, replayed: true };
      return;
    }
    if (booking.bookingState !== 'held' || !['unpaid', 'pending_review'].includes(booking.cashState)) {
      throw new Error('BAD_STATE');
    }
    assertCompleteClaims(booking);
    assertReservedPackage(booking, refs, docs);
    const expiresAt = millis(booking.paymentExpiresAt);
    if (booking.cashState === 'unpaid' && expiresAt === null) throw new Error('BAD_STATE');
    // Authorization occurs after all reads on every transaction attempt. There
    // are no awaited operations between this decision and queuing the writes.
    if (booking.cashState === 'unpaid' && expiresAt <= clock()) {
      releaseWrites(t, db, bookingRef, bookingId, booking, refs, docs, {
        terminalState: 'expired', reason: 'approval_after_expiry', actor,
      });
      outcome = { ok: false, code: 'HOLD_EXPIRED', released: true };
      return;
    }

    assertOwnedClaims(docs.courtClaimSnaps, bookingId, booking.bookingCode);
    assertOwnedClaims(docs.coachClaimSnaps, bookingId, booking.bookingCode);

    refs.courtSlotRefs.forEach((ref, index) => {
      if (!docs.courtSlotSnaps[index]?.exists) throw new Error('SLOT_MISSING');
      t.update(ref, { bookingStatus: 'confirmed', paymentStatus: 'paid', expiresAt: null });
      t.update(refs.courtClaimRefs[index], { status: 'confirmed', expiresAt: null, updatedAt: FieldValue.serverTimestamp() });
    });
    refs.coachClaimRefs.forEach(ref => {
      t.update(ref, { status: 'confirmed', expiresAt: null, updatedAt: FieldValue.serverTimestamp() });
    });

    if (booking.packageUsageState === 'reserved') {
      const logRef = db.collection(PACKAGE_LOGS).doc();
      t.create(logRef, {
        packageId: refs.packageId,
        lineUserId: booking.lineUserId || '',
        packageType: booking.packageType || booking.usedPackageType || '',
        packageName: booking.packageName || booking.usedPackageName || '',
        action: 'consume_reserved_minutes',
        deltaMinutes: 0,
        reservedMinutesConsumed: Number(booking.courtPackageMinutes) || 0,
        reason: `confirmed booking ${booking.bookingCode || bookingId}`,
        bookingId,
        source: 'coach_addon_v2',
        createdAt: FieldValue.serverTimestamp(),
      });
    }

    t.update(bookingRef, {
      bookingState: 'confirmed',
      cashState: 'paid',
      cashPaidAmount: Number(booking.cashDueAmount) || 0,
      'priceBreakdown.cashPaidAmount': Number(booking.cashDueAmount) || 0,
      ...(booking.packageUsageState === 'reserved' ? { packageUsageState: 'consumed', packageConsumedAt: FieldValue.serverTimestamp() } : {}),
      bookingStatus: 'confirmed', paymentStatus: 'paid', status: 'confirmed',
      paidBy: actor, paidAt: FieldValue.serverTimestamp(), confirmedAt: FieldValue.serverTimestamp(),
      adminReviewedAt: FieldValue.serverTimestamp(),
      ...(manualPayment ? { paymentMethod: manualPayment.paymentMethod, paymentNote: manualPayment.paymentNote } : {}),
      ...(withoutSlip ? { confirmedByAdmin: true, confirmedWithoutSlip: true } : {}),
      updatedAt: FieldValue.serverTimestamp(),
    });
    outcome = { ok: true, confirmed: true, replayed: false };
  });
  return outcome;
}
