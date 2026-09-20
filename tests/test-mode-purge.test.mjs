import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { describe, expect, test } from 'vitest';
import { belongsToTestSession } from '../test-booking.js';

const source = readFileSync(new URL('../api/admin-edit-booking-accounting.js', import.meta.url), 'utf8');
const notifyJs = readFileSync(new URL('../api/_lib/notify.js', import.meta.url), 'utf8');
const adminOps = readFileSync(new URL('../api/admin-ops.js', import.meta.url), 'utf8');
const financeData = readFileSync(new URL('../api/finance-data.js', import.meta.url), 'utf8');
const aiReport = readFileSync(new URL('../api/_lib/ai-report.js', import.meta.url), 'utf8');
const bookingJs = readFileSync(new URL('../api/booking.js', import.meta.url), 'utf8');

// The manifest logic is module-private; run the block that holds it.
function purgeState() {
  const slice = source.slice(source.indexOf('const PURGE_RESOLVED'), source.indexOf('// One booking, one transaction'));
  const context = vm.createContext({});
  vm.runInContext(slice, context);
  return context;
}

const manifest = (over = {}) => ({ bookings: [], slots: [], slipRegistry: [], ...over });
const { purgeFinalState, manifestUnresolved } = purgeState();

describe('a session is only called purged when nothing is left behind', () => {
  test('everything resolved reaches purged', () => {
    expect(purgeFinalState(manifest({
      bookings: [{ id: 'b1', status: 'deleted' }, { id: 'b2', status: 'not_present' }],
      slots: [{ id: 's1', status: 'restored' }, { id: 's2', status: 'already_clean' }],
      slipRegistry: [{ id: 'r1', status: 'not_present' }],
    }))).toBe('purged');
  });

  test('an empty session purges cleanly', () => {
    expect(purgeFinalState(manifest())).toBe('purged');
  });

  test.each([
    ['a booking skipped on ownership', { bookings: [{ id: 'b1', status: 'skipped_ownership' }] }],
    ['a slot another session now holds', { slots: [{ id: 's1', status: 'skipped_ownership' }] }],
    ['a slot blocked behind unpurged bookings', { slots: [{ id: 's1', status: 'blocked' }] }],
    ['a slot with a live booking on it', { slots: [{ id: 's1', status: 'skipped_live_booking' }] }],
  ])('%s leaves it at purged_with_warnings', (_label, over) => {
    expect(purgeFinalState(manifest(over))).toBe('purged_with_warnings');
  });

  test.each([
    ['a booking', { bookings: [{ id: 'b1', status: 'failed', reason: 'calendar' }] }],
    ['a slot', { slots: [{ id: 's1', status: 'failed' }] }],
    ['a registry entry', { slipRegistry: [{ id: 'r1', status: 'failed' }] }],
  ])('a failure on %s is purge_failed, never a warning', (_label, over) => {
    expect(purgeFinalState(manifest(over))).toBe('purge_failed');
  });

  test('a failure outranks warnings elsewhere', () => {
    expect(purgeFinalState(manifest({
      bookings: [{ id: 'b1', status: 'deleted' }, { id: 'b2', status: 'failed' }],
      slots: [{ id: 's1', status: 'skipped_ownership' }],
    }))).toBe('purge_failed');
  });

  test('a deleted booking that left a voucher behind still blocks purged', () => {
    // The booking's own status is resolved; the leftover is what matters.
    const m = manifest({ bookings: [{ id: 'b1', status: 'deleted', voucherWarning: 'allowance exhausted' }] });
    expect(purgeFinalState(m)).toBe('purged_with_warnings');
    expect(manifestUnresolved(m)).toEqual([
      { id: 'b1', status: 'voucher_not_restored', reason: 'allowance exhausted' },
    ]);
  });

  test('every unresolved entry is reported, not just counted', () => {
    const m = manifest({
      bookings: [{ id: 'b1', status: 'failed', reason: 'calendar' }],
      slots: [{ id: 's1', status: 'blocked', reason: 'bookings remain' }],
    });
    expect(manifestUnresolved(m).map(e => e.id).sort()).toEqual(['b1', 's1']);
  });
});

describe('ownership decides scope, never the flag alone', () => {
  test('a booking flagged without naming the session is out of scope', () => {
    expect(belongsToTestSession({ isTest: true }, 'ts_1')).toBe(false);
    expect(belongsToTestSession({ isTest: true, testSessionId: 'ts_2' }, 'ts_1')).toBe(false);
    expect(belongsToTestSession({ testSessionId: 'ts_1' }, 'ts_1')).toBe(true);
  });

  test('the purge queries by session id rather than by the flag', () => {
    expect(source).toContain("db.collection('bookings').where('testSessionId', '==', testSessionId)");
    expect(source).not.toMatch(/where\('isTest', '==', true\)/);
  });

  test('ownership is re-checked inside the transaction, not only before it', () => {
    const fn = source.slice(source.indexOf('async function purgeOneBooking'), source.indexOf('// Put available_slots back'));
    expect(fn.match(/belongsToTestSession/g)).toHaveLength(2);
    expect(fn).toContain("throw new Error('OWNERSHIP_CHANGED')");
    expect(fn).toContain("throw new Error('SLOT_OWNERSHIP_MISMATCH')");
  });

  test('a slot is only restored while this session still holds it', () => {
    const fn = source.slice(source.indexOf('async function restoreReservedSlots'), source.indexOf('// Defensive only.'));
    expect(fn).toContain("if (data.status !== 'test_reserved') return 'already_clean';");
    expect(fn).toContain("if (String(data.testSessionId || '') !== testSessionId) return 'skipped_ownership';");
    expect(fn).toContain("if (isLiveBookedSlot(data)) return 'skipped_live_booking';");
  });

  test('a registry entry pointing at another booking is never deleted', () => {
    const fn = source.slice(source.indexOf('async function purgeSlipRegistry'), source.indexOf('async function handleTestSessionPurge'));
    expect(fn).toContain("throw new Error('REGISTRY_OWNERSHIP_CHANGED')");
  });
});

describe('order and preconditions', () => {
  test('an active session cannot be purged, so the record set stops moving first', () => {
    expect(source).toContain("code: 'SESSION_ACTIVE'");
    expect(source).toContain('End the test session before purging it');
  });

  test('re-purging an already purged session is a no-op that returns its manifest', () => {
    expect(source).toContain('alreadyPurged: true');
  });

  test('restores happen before the booking is deleted, since the booking says what to give back', () => {
    const fn = source.slice(source.indexOf('async function purgeOneBooking'), source.indexOf('// Put available_slots back'));
    expect(fn.indexOf('passRestoreMutation(pkg, bNow)')).toBeLessThan(fn.indexOf('t.delete(bookingRef)'));
    expect(fn.indexOf('releaseVoucherUpdate(voucher')).toBeLessThan(fn.indexOf('t.delete(bookingRef)'));
  });

  test('slots are restored only once every booking is gone', () => {
    expect(source).toContain('const bookingsCleared = manifest.bookings.every(b => PURGE_RESOLVED.has(b.status));');
    expect(source).toContain("status: 'blocked', reason: 'Bookings on this session are not fully purged yet'");
  });

  test('package restore keeps its existing idempotency marker', () => {
    const fn = source.slice(source.indexOf('async function purgeOneBooking'), source.indexOf('// Put available_slots back'));
    expect(fn).toContain('!bNow.packageRestoredAt');
  });

  test('a purge that did not finish says so and stays re-runnable', () => {
    expect(source).toContain("rerunnable: status !== 'purged'");
  });
});

describe('what a purge deliberately leaves alone', () => {
  test('notification_logs are kept as the test audit', () => {
    const fn = source.slice(source.indexOf('// handleTestSessionPurge'), source.indexOf('async function handleDeleteBooking'));
    expect(fn).not.toContain("collection('notification_logs')");
    expect(fn).toContain('notification_logs are kept as the test audit');
  });

  test('nothing reads notification_logs for business reporting, so keeping them costs nothing', () => {
    for (const reporting of [adminOps, financeData, aiReport]) {
      expect(reporting).not.toContain('notification_logs');
    }
  });

  test('a suppressed send is recorded as its own status, distinguishable from a real one', () => {
    expect(notifyJs).toContain('status: "suppressed_test"');
    expect(notifyJs).toContain('testSessionId,');
  });

  test('registered_users are never touched by a purge', () => {
    const fn = source.slice(source.indexOf('// handleTestSessionPurge'), source.indexOf('async function handleDeleteBooking'));
    expect(fn).not.toContain("collection('registered_users')");
  });

  test('and Test Mode creates none in the first place', () => {
    // Only the two test-aware creation paths matter; neither writes a user doc.
    const create = bookingJs.slice(bookingJs.indexOf('async function handleCreate('), bookingJs.indexOf('async function handleCancelPending'));
    expect(create).not.toContain("collection('registered_users')");
    const pass = bookingJs.slice(bookingJs.indexOf('async function handleCreatePassBooking'), bookingJs.indexOf('async function passPurchaseResponse'));
    expect(pass).not.toContain("collection('registered_users')");
  });
});
