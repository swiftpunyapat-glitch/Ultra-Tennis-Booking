import { readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';
import { revertTestVoucherRedemption, TEST_REVERT_MARKER } from '../api/_lib/test-voucher.js';
import { redeemVoucherUpdate, releaseVoucherUpdate, reserveVoucherUpdate } from '../api/_lib/voucher-engine.js';

const accounting = readFileSync(new URL('../api/admin-edit-booking-accounting.js', import.meta.url), 'utf8');

const TS = 'ts_1';
const BOOKING_ID = 'bk_1';
const CODE = 'ULTRA50';
const TIME = '2026-09-21T10:00:00Z';

const booking = (over = {}) => ({ isTest: true, testSessionId: TS, bookingCode: CODE, ...over });
const ctx = (over = {}) => ({ booking: booking(), bookingId: BOOKING_ID, bookingCode: CODE, testSessionId: TS, timestamp: TIME, ...over });

// Start from what the booking path actually wrote, so the revert is tested
// against the real mutation rather than a hand-written approximation.
const redeemed = (base = {}) => ({
  maxUses: 5, cancellationRestoreCount: 0, maxCancellationRestores: 1, ...base,
  ...redeemVoucherUpdate({ usedCount: 2, ...base }, { bookingId: BOOKING_ID, bookingCode: CODE, lineUserId: 'U1', timestamp: TIME }),
});
const reserved = (base = {}) => ({
  usedCount: 2, maxUses: 5, cancellationRestoreCount: 0, maxCancellationRestores: 1, ...base,
  ...reserveVoucherUpdate({ usedCount: 2 }, { bookingId: BOOKING_ID, bookingCode: CODE, lineUserId: 'U1', reservedUntil: TIME, timestamp: TIME }),
});

describe('the revert is the inverse of the redemption, not a cancellation', () => {
  test('a redemption is undone field for field', () => {
    const result = revertTestVoucherRedemption(redeemed(), ctx());
    expect(result.ok).toBe(true);
    expect(result.update).toMatchObject({
      state: 'available',
      usedCount: 2,               // redeem made it 3
      redeemedBookingId: null, redeemedBookingCode: null, redeemedBy: null, redeemedAt: null,
    });
  });

  test('a reservation is undone without touching usedCount, because reserving never raised it', () => {
    const result = revertTestVoucherRedemption(reserved(), ctx());
    expect(result.ok).toBe(true);
    expect(result.update).toMatchObject({
      state: 'available',
      reservedBookingId: null, reservedBookingCode: null, reservedBy: null, reservedUntil: null, reservedAt: null,
    });
    expect(result.update).not.toHaveProperty('usedCount');
  });

  test('usedCount is floored rather than allowed to go negative', () => {
    const v = { ...redeemed(), usedCount: 0 };
    expect(revertTestVoucherRedemption(v, ctx()).update.usedCount).toBe(0);
  });

  test('the last-used trail is cleared only when it still names this booking', () => {
    expect(revertTestVoucherRedemption(redeemed(), ctx()).update).toMatchObject({ lastUsedBooking: null, lastUsedAt: null, lastUsedBy: null });
    const other = { ...redeemed(), lastUsedBooking: 'SOMEONE_ELSE' };
    expect(revertTestVoucherRedemption(other, ctx()).update).not.toHaveProperty('lastUsedBooking');
  });

  test('it records which booking and session reverted it', () => {
    const update = revertTestVoucherRedemption(redeemed(), ctx()).update;
    expect(update[TEST_REVERT_MARKER]).toBe(BOOKING_ID);
    expect(update.testRevertedBySession).toBe(TS);
  });
});

describe('cancellation quota is neither read nor written', () => {
  test.each([
    ['quota untouched', { cancellationRestoreCount: 0, maxCancellationRestores: 1 }],
    ['quota fully spent', { cancellationRestoreCount: 1, maxCancellationRestores: 1 }],
    ['no quota configured at all', { cancellationRestoreCount: 0, maxCancellationRestores: 0 }],
    ['quota already over the limit', { cancellationRestoreCount: 9, maxCancellationRestores: 1 }],
  ])('a voucher with %s reverts identically', (_label, quota) => {
    const result = revertTestVoucherRedemption(redeemed(quota), ctx());
    expect(result.ok).toBe(true);
    expect(result.update.usedCount).toBe(2);
    expect(result.update).not.toHaveProperty('cancellationRestoreCount');
    expect(result.update).not.toHaveProperty('maxCancellationRestores');
  });

  test('the cancellation path would have refused the exhausted case outright', () => {
    // The reason this function exists: releaseVoucherUpdate declines once the
    // allowance is gone, so a purge built on it could not give the use back.
    const spent = redeemed({ cancellationRestoreCount: 1, maxCancellationRestores: 1 });
    expect(releaseVoucherUpdate(spent, { bookingId: BOOKING_ID, reason: 'x', timestamp: TIME, countRestore: true }).restored).toBe(false);
    expect(revertTestVoucherRedemption(spent, ctx()).ok).toBe(true);
  });

  test('and would otherwise have spent a real quota on a test', () => {
    const fresh = redeemed();
    expect(releaseVoucherUpdate(fresh, { bookingId: BOOKING_ID, reason: 'x', timestamp: TIME, countRestore: true }).update.cancellationRestoreCount).toBe(1);
    expect(revertTestVoucherRedemption(fresh, ctx()).update).not.toHaveProperty('cancellationRestoreCount');
  });

  test('the purge no longer reaches for the cancellation helper', () => {
    const fn = accounting.slice(accounting.indexOf('async function purgeOneBooking'), accounting.indexOf('// Put available_slots back'));
    expect(fn).not.toContain('releaseVoucherUpdate');
    expect(fn).not.toContain('cancellationRestoreCount');
    expect(fn).toContain('revertTestVoucherRedemption');
  });
});

describe('ownership is refused, never guessed', () => {
  test.each([
    ['a booking from another session', { booking: booking({ testSessionId: 'ts_other' }) }, 'booking_not_in_test_session'],
    ['a booking with no session at all', { booking: { bookingCode: CODE } }, 'booking_not_in_test_session'],
    ['a booking flagged isTest but naming no session', { booking: { isTest: true } }, 'booking_not_in_test_session'],
  ])('%s is refused', (_label, over, reason) => {
    expect(revertTestVoucherRedemption(redeemed(), ctx(over))).toEqual({ ok: false, reason });
  });

  test('a voucher redeemed by a different booking is refused', () => {
    const v = { ...redeemed(), redeemedBookingId: 'bk_other' };
    expect(revertTestVoucherRedemption(v, ctx())).toEqual({ ok: false, reason: 'voucher_redeemed_by_another_booking' });
  });

  test('a voucher reserved by a different booking is refused', () => {
    const v = { ...reserved(), reservedBookingId: 'bk_other' };
    expect(revertTestVoucherRedemption(v, ctx())).toEqual({ ok: false, reason: 'voucher_reserved_by_another_booking' });
  });

  test.each([['available'], ['expired'], ['void']])('a voucher sitting at %s with no marker is refused', state => {
    const result = revertTestVoucherRedemption({ ...redeemed(), state }, ctx());
    expect(result.ok).toBe(false);
    expect(result.reason).toContain(state);
  });

  test.each([
    ['a missing voucher', { voucher: null }, 'voucher_missing'],
    ['no booking id', { bookingId: '' }, 'booking_id_required'],
    ['no session id', { testSessionId: '' }, 'test_session_required'],
  ])('%s is refused', (_label, over, reason) => {
    const voucher = 'voucher' in over ? over.voucher : redeemed();
    expect(revertTestVoucherRedemption(voucher, ctx(over))).toEqual({ ok: false, reason });
  });
});

describe('idempotency — a rerun must not decrement twice', () => {
  test('applying the revert, then running it again, is a no-op', () => {
    const first = revertTestVoucherRedemption(redeemed(), ctx());
    const after = { ...redeemed(), ...first.update };
    const second = revertTestVoucherRedemption(after, ctx());
    expect(second).toEqual({ ok: true, alreadyReverted: true, update: null });
  });

  test('usedCount survives three passes unchanged after the first', () => {
    let voucher = redeemed();
    let count = null;
    for (let i = 0; i < 3; i++) {
      const result = revertTestVoucherRedemption(voucher, ctx());
      expect(result.ok).toBe(true);
      if (result.update) voucher = { ...voucher, ...result.update };
      count = count ?? voucher.usedCount;
    }
    expect(voucher.usedCount).toBe(2);
    expect(count).toBe(2);
  });

  test('the marker, not the state, is what makes it idempotent', () => {
    // A reverted voucher is indistinguishable from an unused one by state
    // alone, which is exactly why state cannot be the guard.
    const reverted = { ...redeemed(), ...revertTestVoucherRedemption(redeemed(), ctx()).update };
    expect(reverted.state).toBe('available');
    const withoutMarker = { ...reverted, [TEST_REVERT_MARKER]: null };
    expect(revertTestVoucherRedemption(withoutMarker, ctx()).ok).toBe(false);
  });

  test("another booking's marker does not make this one idempotent", () => {
    const v = { ...redeemed(), [TEST_REVERT_MARKER]: 'bk_other' };
    expect(revertTestVoucherRedemption(v, ctx()).ok).toBe(true);
    expect(revertTestVoucherRedemption(v, ctx()).alreadyReverted).toBe(false);
  });
});

describe('a failed rollback keeps the booking, so a rerun starts from the same data', () => {
  const fn = accounting.slice(accounting.indexOf('async function purgeOneBooking'), accounting.indexOf('// Put available_slots back'));

  test('a refused revert throws instead of falling through to the delete', () => {
    expect(fn).toContain('if (!reverted.ok) throw new Error(`VOUCHER_REVERT_${reverted.reason}`);');
    expect(fn.indexOf('VOUCHER_REVERT_')).toBeLessThan(fn.indexOf('t.delete(bookingRef)'));
  });

  test('both rollbacks run before anything is deleted, inside one transaction', () => {
    expect(fn.indexOf('passRestoreMutation(pkg, bNow)')).toBeLessThan(fn.indexOf('revertTestVoucherRedemption'));
    expect(fn.indexOf('revertTestVoucherRedemption')).toBeLessThan(fn.indexOf('t.delete(slotRefs[i])'));
    expect(fn.indexOf('t.delete(slotRefs[i])')).toBeLessThan(fn.indexOf('t.delete(bookingRef)'));
  });

  test('a missing voucher document blocks the delete rather than being skipped', () => {
    expect(fn).toContain("if (!voucherSnap?.exists) throw new Error('VOUCHER_MISSING');");
  });

  test('a voucher this purge cannot revert blocks the delete too', () => {
    expect(fn).toContain("throw new Error('VOUCHER_REVERT_unsupported_legacy_lifecycle')");
  });

  test('a thrown rollback surfaces as a failed booking, which forces purge_failed', () => {
    expect(fn).toContain("return { ...outcome, status: 'failed', reason: e.message };");
    const resolved = accounting.slice(accounting.indexOf('const PURGE_RESOLVED'), accounting.indexOf('function purgeFinalState'));
    expect(resolved).not.toContain('failed');
  });

  test('nothing downgrades an unrevertable voucher to a warning any more', () => {
    expect(accounting).not.toContain('voucherWarning');
    expect(accounting).not.toContain('voucher_not_restored');
  });
});
