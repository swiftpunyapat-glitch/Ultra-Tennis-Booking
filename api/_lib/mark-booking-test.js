import { FieldValue } from 'firebase-admin/firestore';
import { hasBranchAccess, resolveBranchId } from './admin-auth.js';
import { isCoachAddonV2Booking } from './coach-addon-v2.js';
import { revertOwnerTestVoucher } from './test-voucher.js';

const fail = message => { throw Object.assign(new Error(message), { status: 409 }); };
const unique = values => [...new Set(values.filter(Boolean))];
const lockId = (type, source, id) => `active_${type}_${source}_${encodeURIComponent(id)}`;

/** Soft-close a real booking and undo its local side effects in ONE commit.
 * Pricing/payment evidence stays intact. Reports use isTest; terminal states
 * stop payment/reschedule paths from reactivating the booking. No bank refund.
 * Helpers reuse the booking service's legacy slot and pass calculations.
 */
export async function markBookingTest(db, { bookingId, session, reason, bookingSlotIds, passRestoreMutation }) {
  if (session?.name !== 'Art' || session?.role !== 'owner') {
    throw Object.assign(new Error('Only Art (owner) can mark a booking as test'), { status: 403 });
  }
  if (typeof reason !== 'string' || !reason.trim()) {
    throw Object.assign(new Error('Reason is required'), { status: 400 });
  }
  const ref = db.collection('bookings').doc(bookingId);
  return db.runTransaction(async t => {
    const snap = await t.get(ref);
    if (!snap.exists) throw Object.assign(new Error('Booking not found'), { status: 404 });
    const b = snap.data();
    if (!hasBranchAccess(session, resolveBranchId(b))) throw Object.assign(new Error('No access to this branch'), { status: 403 });
    if (b.testConversion) return { replayed: true, ...b.testConversion.result, calendarEventId: b.googleCalendarEventId || null };

    const slotIds = unique([...(b.bookingSlotIds || []), ...bookingSlotIds(b)]);
    const slotRefs = slotIds.map(id => db.collection('booking_slots').doc(id));
    const claimRefs = slotIds.map(id => db.collection('booking_slot_claims').doc(id));
    const coachRefs = unique(b.coachClaimIds || []).map(id => db.collection('coach_slot_claims').doc(id));
    const packageId = b.packageId || b.usedPackageId;
    const pkgRef = packageId ? db.collection('customer_packages').doc(packageId) : null;
    const voucherRef = b.voucherCode && b.voucherLifecycle === 'v2_state' ? db.collection('vouchers').doc(b.voucherCode) : null;
    const guestRef = db.collection('guest_booking_access').doc(bookingId);
    const targets = [...slotRefs, ...claimRefs, ...coachRefs, guestRef, ...(pkgRef ? [pkgRef] : []), ...(voucherRef ? [voucherRef] : [])];
    const snapshots = new Map((await t.getAll(...targets)).map(s => [s.ref.path, s]));
    const get = r => r ? snapshots.get(r.path) : null;

    // Query inside the transaction so a concurrent refund/payout is retried.
    const expenses = await t.get(db.collection('finance_expenses').where('sourceBookingId', '==', bookingId));
    const incomes = await t.get(db.collection('finance_income_manual').where('sourceBookingId', '==', bookingId));
    const financeRecords = new Map([...expenses.docs, ...incomes.docs].map(s => [s.ref.path, s]));
    const extraRefs = unique([b.refundExpenseId, b.influencerExpenseId, b.payoutExpenseId])
      .map(id => db.collection('finance_expenses').doc(id)).filter(r => !financeRecords.has(r.path));
    if (extraRefs.length) for (const s of await t.getAll(...extraRefs)) {
      if (s.exists) {
        if (s.data().sourceBookingId && s.data().sourceBookingId !== bookingId) fail('Linked expense belongs to another booking');
        financeRecords.set(s.ref.path, s);
      }
    }
    const sources = [{ type: 'booking', id: bookingId, docType: 'receipt' }, ...[...financeRecords.values()].map(s => ({
      type: s.ref.parent.id === 'finance_expenses' ? 'expense' : 'manual_income', id: s.id,
      docType: s.ref.parent.id === 'finance_expenses' ? 'payment_voucher' : 'receipt',
    }))];
    const lockRefs = sources.map(s => db.collection('finance_document_counters').doc(lockId(s.docType, s.type, s.id)));
    const locks = await t.getAll(...lockRefs);
    // Include documents from before active-document locks were introduced.
    const documentMap = new Map();
    for (const source of sources) {
      const found = await t.get(db.collection('finance_documents').where('linkedId', '==', source.id));
      for (const d of found.docs) if (d.data().linkedType === source.type) documentMap.set(d.id, d);
    }

    const now = FieldValue.serverTimestamp();
    const update = {};
    const result = { releasedSlots: 0, releasedCoachClaims: 0, restoredMinutes: 0, voucherRestored: false, excludedFinanceRecords: financeRecords.size, voidedDocuments: 0 };
    const v2 = isCoachAddonV2Booking(b);
    const restoreV2 = v2 && ['reserved', 'consumed'].includes(b.packageUsageState) && Number(b.courtPackageMinutes) > 0;
    const restoreLegacy = !v2 && pkgRef && !b.packageRestoredAt &&
      (b.createdVia === 'server_pass' || Number(b.packageMinutesUsed) > 0) &&
      (b.bookingStatus !== 'cancelled' || (b.packageType || b.usedPackageType) === 'monstr_event_pass');
    if (restoreV2 || restoreLegacy) {
      const pkg = get(pkgRef)?.data();
      if (!pkg || !pkg.lineUserId || pkg.lineUserId !== b.lineUserId) fail('Cannot verify package ownership');
      if (b.usedPackageId && b.usedPackageId !== packageId) fail('Package reference has changed');
      if (v2 && pkg.packageType !== b.packageType) fail('Package type has changed');
      let restored;
      if (restoreV2 || b.packageType === 'monstr_event_pass' || b.usedPackageType === 'monstr_event_pass') {
        const used = Number(restoreV2 ? b.courtPackageMinutes : b.packageMinutesUsed || b.durationMinutes);
        if (!Number.isFinite(pkg.remainingMinutes) || !Number.isInteger(used) || used <= 0) fail('Invalid package balance');
        if (!restoreV2 && pkg.lastUsedBooking !== b.bookingCode) fail('Event Pass has changed since this booking');
        restored = { used, update: { remainingMinutes: pkg.remainingMinutes + used, updatedAt: now, ...(!restoreV2 ? { eventUsedAt: null } : {}) } };
      } else restored = passRestoreMutation(pkg, b);
      if (restored) {
        if (pkg.lastUsedBooking === b.bookingCode) Object.assign(restored.update, { lastUsedBooking: null, lastUsedAt: null });
        t.update(pkgRef, restored.update);
        t.create(db.collection('customer_package_logs').doc(), {
          packageId, bookingId, lineUserId: b.lineUserId, action: 'restore_test_booking',
          deltaMinutes: restored.used, reason: reason.trim().slice(0, 400), isTest: true,
          actor: session.name, createdAt: now,
        });
        Object.assign(update, { packageRestoredAt: now, packageRestoredBy: session.name });
        if (restoreV2) Object.assign(update, { packageUsageState: 'released', packageReleasedAt: now });
        result.restoredMinutes = restored.used;
      }
    }
    if (voucherRef) {
      const voucher = get(voucherRef)?.data();
      // An already-cancelled/expired reservation may have legitimately released
      // its coupon, which can now belong to someone else. Never undo that use.
      const owned = voucher && [voucher.reservedBookingId, voucher.redeemedBookingId].includes(bookingId);
      if (owned) {
        const reverted = revertOwnerTestVoucher(voucher, { session, bookingId, bookingCode: b.bookingCode, timestamp: now });
        if (!reverted.ok) fail(reverted.reason);
        if (reverted.update) t.update(voucherRef, reverted.update);
        result.voucherRestored = !reverted.alreadyReverted;
      } else if (!b.voucherRestored && !['cancelled', 'expired'].includes(b.bookingStatus)) {
        fail('Cannot verify coupon ownership');
      }
    } else if (b.voucherCode && !['cancelled', 'expired'].includes(b.bookingStatus)) {
      fail('Legacy coupon requires review before marking this booking as test');
    }
    slotRefs.forEach((r, i) => {
      const claim = get(claimRefs[i]), slot = get(r);
      const ownsClaim = claim?.exists && claim.data().bookingId === bookingId;
      const ownsSlot = slot?.exists && (slot.data().bookingId === bookingId ||
        (b.bookingCode && slot.data().bookingCode === b.bookingCode));
      // Old dates can have been rebooked: never delete the replacement's slot.
      if (ownsClaim) t.delete(claimRefs[i]);
      const slotOtherOwner = (slot?.data()?.bookingId && slot.data().bookingId !== bookingId) ||
        (slot?.data()?.bookingCode && slot.data().bookingCode !== b.bookingCode);
      if (slot?.exists && !slotOtherOwner && (ownsClaim || (!claim?.exists && ownsSlot))) {
        t.delete(r); result.releasedSlots++;
      }
    });
    coachRefs.forEach(r => {
      if (get(r)?.data()?.bookingId === bookingId) { t.delete(r); result.releasedCoachClaims++; }
    });
    for (const s of financeRecords.values()) t.update(s.ref, {
      isTest: true, deleted: true, deletedAt: now, deletedBy: session.name, testBookingId: bookingId,
      testReason: reason.trim().slice(0, 400), updatedAt: now,
    });
    for (const d of documentMap.values()) {
      if (d.data().status !== 'issued') continue;
      t.update(d.ref, { status: 'void', isTest: true, voidedAt: now, voidedBy: session.name, voidReason: `Test booking: ${reason.trim()}`.slice(0, 400) });
      result.voidedDocuments++;
    }
    for (const lock of locks) {
      if (lock.exists && documentMap.has(lock.data().documentId)) t.delete(lock.ref);
    }
    if (get(guestRef)?.exists) t.update(guestRef, { tokenHash: null, revokedAt: now, revokeReason: 'admin_revoked' });
    const before = Object.fromEntries(['bookingStatus', 'status', 'paymentStatus', 'bookingState', 'cashState', 'coachPayoutStatus', 'lessonStatus', 'pendingReschedule'].map(k => [k, b[k] ?? null]));
    t.update(ref, {
      ...update, isTest: true, testSource: 'owner_reclassification',
      bookingStatus: 'cancelled', status: 'cancelled', paymentStatus: 'rejected',
      ...(v2 ? { bookingState: 'cancelled', cashState: 'cancelled' } : {}),
      pendingReschedule: false, pendingRescheduleStatus: 'cancelled',
      ...(b.coachId ? { lessonStatus: 'cancelled', coachPayoutStatus: 'void' } : {}),
      cancelledAt: now, cancelledBy: session.name, cancelReason: 'test_booking',
      testConversion: { actor: session.name, at: now, reason: reason.trim().slice(0, 400), before, result },
      ...(b.googleCalendarEventId ? { testCalendarCleanup: 'pending' } : {}), updatedAt: now,
    });
    t.create(db.collection('audit_logs').doc(), {
      actor: session.name, actorRole: session.role, branchId: resolveBranchId(b),
      action: 'mark_booking_test', targetId: bookingId, before, after: { isTest: true, ...result },
      note: reason.trim().slice(0, 400), source: 'admin', createdAt: now,
    });
    return { replayed: false, ...result, calendarEventId: b.googleCalendarEventId || null };
  });
}
