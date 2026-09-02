import { FieldValue } from 'firebase-admin/firestore';
import { isCoachAddonV2Booking } from './coach-addon-v2.js';

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

async function readTransitionDocs(t, bookingRef, refs) {
  const all = [
    t.get(bookingRef),
    ...refs.courtSlotRefs.map(ref => t.get(ref)),
    ...refs.courtClaimRefs.map(ref => t.get(ref)),
    ...refs.coachClaimRefs.map(ref => t.get(ref)),
    ...(refs.packageRef ? [t.get(refs.packageRef)] : []),
  ];
  const snaps = await Promise.all(all);
  let at = 0;
  const bookingSnap = snaps[at++];
  const courtSlotSnaps = snaps.slice(at, at += refs.courtSlotRefs.length);
  const courtClaimSnaps = snaps.slice(at, at += refs.courtClaimRefs.length);
  const coachClaimSnaps = snaps.slice(at, at += refs.coachClaimRefs.length);
  const packageSnap = refs.packageRef ? snaps[at] : null;
  return { bookingSnap, courtSlotSnaps, courtClaimSnaps, coachClaimSnaps, packageSnap };
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
  const refs = refsForBooking(db, outerBooking);
  let outcome = null;

  await db.runTransaction(async t => {
    const docs = await readTransitionDocs(t, bookingRef, refs);
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
  actor = 'admin', withoutSlip = false, nowMs = Date.now(),
} = {}) {
  const bookingRef = db.collection('bookings').doc(bookingId);
  const outer = await bookingRef.get();
  if (!outer.exists) return { ok: false, code: 'BOOKING_MISSING' };
  const outerBooking = outer.data();
  if (!isCoachAddonV2Booking(outerBooking)) return { ok: false, code: 'NOT_V2' };
  const refs = refsForBooking(db, outerBooking);
  let outcome = null;

  await db.runTransaction(async t => {
    const docs = await readTransitionDocs(t, bookingRef, refs);
    if (!docs.bookingSnap.exists) throw new Error('BOOKING_MISSING');
    const booking = docs.bookingSnap.data();
    if (booking.bookingState === 'confirmed' && booking.cashState === 'paid') {
      outcome = { ok: true, confirmed: true, replayed: true };
      return;
    }
    if (booking.bookingState !== 'held' || !['unpaid', 'pending_review'].includes(booking.cashState)) {
      throw new Error('BAD_STATE');
    }
    const expiresAt = millis(booking.paymentExpiresAt);
    if (booking.cashState === 'unpaid' && expiresAt !== null && expiresAt <= nowMs) {
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
      ...(withoutSlip ? { confirmedByAdmin: true, confirmedWithoutSlip: true } : {}),
      updatedAt: FieldValue.serverTimestamp(),
    });
    outcome = { ok: true, confirmed: true, replayed: false };
  });
  return outcome;
}
