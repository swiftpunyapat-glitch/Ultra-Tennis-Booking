// ════════════════════════════════════════════════════════════════════
// Test Mode — voucher rollback (SERVER ONLY)
// ════════════════════════════════════════════════════════════════════
// Purging a test session is not a cancellation. The customer did not change
// their mind; the redemption never should have counted in the first place, so
// the voucher has to come out exactly as it went in.
//
// releaseVoucherUpdate is the wrong tool for that. It implements the
// CANCELLATION policy: it consults maxCancellationRestores, spends one of
// them, and refuses outright once the allowance is gone. Applied to a test
// that would mean a test run silently burns a real code's cancellation quota,
// and a voucher whose quota is already spent could never be rolled back at
// all. Neither is acceptable for something that is supposed to leave no trace.
//
// So this inverts the two mutations the booking path can make, and nothing
// else. It never reads and never writes cancellationRestoreCount or
// maxCancellationRestores — quota is a cancellation concept and has no bearing
// here, which is also why a voucher with no quota left reverts normally.
//
//   reserveVoucherUpdate → state 'reserved' + reserved* fields, usedCount
//                          untouched. Inverse: clear them, leave usedCount.
//   redeemVoucherUpdate  → state 'redeemed' + redeemed* fields, usedCount + 1,
//                          reserved* nulled, lastUsed* overwritten.
//                          Inverse: clear them, usedCount - 1.
//
// Ownership is decided from data the caller read INSIDE its transaction: the
// booking must name the session being purged, and the voucher must name the
// booking. Anything else is refused rather than guessed at, because the cost
// of guessing is a real customer's voucher.

import { belongsToTestSession } from '../../test-booking.js';

const integer = (value, fallback = 0) => {
  const n = Number(value);
  return Number.isInteger(n) ? n : fallback;
};

const str = value => String(value ?? '').trim();

// Written by a successful revert, and the thing that makes a rerun a no-op.
// State alone cannot carry that: a reverted voucher looks exactly like one
// that was never used, so a second pass would decrement usedCount again.
export const TEST_REVERT_MARKER = 'testRevertedFromBookingId';

export function revertTestVoucherRedemption(voucher, { booking, bookingId, bookingCode, testSessionId, timestamp } = {}) {
  const id = str(bookingId);
  const sessionId = str(testSessionId);
  if (!id) return { ok: false, reason: 'booking_id_required' };
  if (!sessionId) return { ok: false, reason: 'test_session_required' };

  // Ownership, first: only a booking that names this session may move a
  // voucher at all. The caller reads `booking` inside its transaction, so this
  // is checked against the record as it exists at write time.
  if (!belongsToTestSession(booking, sessionId)) {
    return { ok: false, reason: 'booking_not_in_test_session' };
  }
  if (!voucher) return { ok: false, reason: 'voucher_missing' };

  // Idempotency, before any state reasoning. A rerun after a successful revert
  // must change nothing, and the marker is the only evidence that survives.
  if (str(voucher[TEST_REVERT_MARKER]) === id) {
    return { ok: true, alreadyReverted: true, update: null };
  }

  const base = {
    [TEST_REVERT_MARKER]: id,
    testRevertedBySession: sessionId,
    testRevertedAt: timestamp ?? null,
    updatedAt: timestamp ?? null,
  };

  if (voucher.state === 'redeemed') {
    if (str(voucher.redeemedBookingId) !== id) {
      return { ok: false, reason: 'voucher_redeemed_by_another_booking' };
    }
    const update = {
      ...base,
      state: 'available',
      // The exact inverse of redeemVoucherUpdate's increment. Floored because
      // a stored count below zero would be worse than a lost decrement.
      usedCount: Math.max(0, integer(voucher.usedCount) - 1),
      redeemedBookingId: null,
      redeemedBookingCode: null,
      redeemedBy: null,
      redeemedAt: null,
    };
    // The last-used trail is overwritten by a redemption, and the value it
    // replaced is not recoverable from the document. Clear it only when it
    // still names this booking, so a purge never leaves a real voucher
    // pointing at a booking that no longer exists — and never destroys a trail
    // belonging to someone else's redemption.
    if (bookingCode && str(voucher.lastUsedBooking) === str(bookingCode)) {
      update.lastUsedAt = null;
      update.lastUsedBy = null;
      update.lastUsedBooking = null;
    }
    return { ok: true, alreadyReverted: false, update };
  }

  if (voucher.state === 'reserved') {
    if (str(voucher.reservedBookingId) !== id) {
      return { ok: false, reason: 'voucher_reserved_by_another_booking' };
    }
    // reserveVoucherUpdate never touched usedCount, so neither does this.
    return {
      ok: true,
      alreadyReverted: false,
      update: {
        ...base,
        state: 'available',
        reservedBookingId: null,
        reservedBookingCode: null,
        reservedBy: null,
        reservedUntil: null,
        reservedAt: null,
      },
    };
  }

  // Available, expired, or anything else, with no marker saying this booking
  // reverted it. Something other than this purge moved the voucher, and
  // nothing here can tell what — refuse and let the purge report it.
  return { ok: false, reason: `voucher_not_held_by_booking_state_${str(voucher.state) || 'unknown'}` };
}
