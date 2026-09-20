import { readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';
import { buildAiBookingReport } from '../api/_lib/ai-report.js';
import { availableToSession, TEST_SLOT_STATUS } from '../api/_lib/test-session.js';

const read = name => readFileSync(new URL(`../${name}`, import.meta.url), 'utf8');
const bookingJs = read('api/booking.js');
const adminOps = read('api/admin-ops.js');
const financeData = read('api/finance-data.js');
const aiReport = read('api/_lib/ai-report.js');
const notifyJs = read('api/_lib/notify.js');
const lineNotify = read('api/line-notify.js');
const gcalJs = read('api/gcal.js');
const slipVerify = read('api/slip-verify.js');

// Every server file that creates a booking or totals money. If a new one
// appears it has to be added here deliberately, which is the point.
const REPORTING_SOURCES = { 'api/admin-ops.js': adminOps, 'api/finance-data.js': financeData, 'api/_lib/ai-report.js': aiReport };

describe('a test session is the only thing that can make a booking a test', () => {
  test('the booking route rejects the flags outright instead of ignoring them', () => {
    expect(bookingJs).toContain('assertNoClientTestFlags(body)');
    expect(bookingJs).toContain("code: 'TEST_FLAG_REJECTED'");
  });

  test('the route resolves the session itself rather than reading a body field', () => {
    expect(bookingJs).toContain('await resolveTestSession(req, testDb, Date.now(), body)');
    expect(bookingJs).toContain("code: 'TEST_SESSION_INVALID'");
  });

  test('no creation path writes the flags by hand', () => {
    // Only testStamp() may produce them, so the shape cannot drift.
    expect(bookingJs).not.toMatch(/isTest:\s*true/);
    expect(bookingJs.match(/\.\.\.testStamp\(testSession\)/g)).toHaveLength(2);
  });

  test('an action that does not understand Test Mode refuses rather than booking for real', () => {
    expect(bookingJs).toContain("code: 'TEST_MODE_UNSUPPORTED'");
    expect(bookingJs).toMatch(/if \(!TEST_MODE_ACTIONS\.has\(body\.action\)\)/);
  });

  test('both creation paths check the slot is one the session reserved', () => {
    expect(bookingJs.match(/slotsWithinTestSession\(testSession, segRefs\.map\(r => r\.id\)\)/g)).toHaveLength(2);
    expect(bookingJs.match(/TEST_SLOT_OUT_OF_SCOPE/g)).toHaveLength(2);
  });
});

describe('test inventory and public inventory never overlap', () => {
  const session = { testSessionId: 'ts_1' };

  test('live traffic can take an open slot', () => {
    expect(availableToSession({ status: 'open' }, null)).toBe(true);
  });

  test('live traffic cannot take a slot a test is holding', () => {
    expect(availableToSession({ status: TEST_SLOT_STATUS, testSessionId: 'ts_1' }, null)).toBe(false);
  });

  test('a test session cannot take public inventory even when it is open', () => {
    expect(availableToSession({ status: 'open' }, session)).toBe(false);
  });

  test('a test session can take only the slots reserved to it', () => {
    expect(availableToSession({ status: TEST_SLOT_STATUS, testSessionId: 'ts_1' }, session)).toBe(true);
    expect(availableToSession({ status: TEST_SLOT_STATUS, testSessionId: 'ts_2' }, session)).toBe(false);
    expect(availableToSession({ status: TEST_SLOT_STATUS }, session)).toBe(false);
  });

  test.each([{ status: 'closed' }, { status: null }, {}, null, undefined])('%o is bookable by nobody', slot => {
    expect(availableToSession(slot, null)).toBe(false);
    expect(availableToSession(slot, session)).toBe(false);
  });

  test('both availability gates in the booking route go through the helper', () => {
    expect(bookingJs.match(/availableToSession\((?:availSnap|a)\.data\(\), testSession\)/g)).toHaveLength(2);
  });

  test('every creation action Test Mode does not handle is refused, not served', () => {
    // The coach paths still gate on status directly. That is safe only while
    // they reject a test session at the dispatcher — otherwise they would
    // quietly book public inventory for a test. The allowlist is the contract.
    const allowlist = bookingJs.slice(bookingJs.indexOf('const TEST_MODE_ACTIONS'), bookingJs.indexOf(']);', bookingJs.indexOf('const TEST_MODE_ACTIONS')));
    const writeActions = (bookingJs.match(/body\.action === '([a-z_0-9]+)'/g) || [])
      .map(m => m.slice("body.action === '".length, -1))
      .filter(a => a.startsWith('create') || a === 'cancel_pending' || a === 'event_pass_redeem');
    expect(writeActions.length).toBeGreaterThan(3);
    for (const action of writeActions) {
      const handled = ['create', 'create_pass_booking'].includes(action);
      expect(allowlist.includes(`'${action}'`)).toBe(handled);
    }
  });

  test('starting a session snapshots what it overwrote, because nothing else records it', () => {
    expect(adminOps).toContain('slotRestore: previous');
    expect(adminOps).toMatch(/existed: snap\.exists/);
    expect(adminOps).toContain('SLOT_LIVE');
    expect(adminOps).toContain('SLOT_ALREADY_TEST');
  });

  test('ending a session revokes it but keeps the slots off sale until purge', () => {
    expect(adminOps).toContain("t.update(ref, { status: 'ended'");
    expect(adminOps).toContain('Slots stay reserved until the session is purged');
  });
});

describe('external side effects stop at the last gate before the call', () => {
  test('the LINE sender suppresses after building the message, not before', () => {
    const guard = notifyJs.slice(notifyJs.indexOf('// Guard 3'), notifyJs.indexOf('const result = await callLinePush'));
    expect(guard).toContain('if (testSessionId)');
    expect(guard).toContain('status: "suppressed_test"');
    expect(guard).toContain('messagePreview: preview');
    // Suppression must sit between the preview and the only outbound call.
    expect(notifyJs.indexOf('// Guard 3')).toBeGreaterThan(notifyJs.indexOf('const preview ='));
    expect(notifyJs.indexOf('// Guard 3')).toBeLessThan(notifyJs.indexOf('const result = await callLinePush'));
  });

  test('a suppressed send is not recorded as a success, so it stays retryable', () => {
    expect(notifyJs).toContain('if (prev.exists && prev.data().status === "success")');
    expect(notifyJs).not.toContain('status: "success", // test');
  });

  test('the client-callable notify route asks the stored booking, never the caller', () => {
    expect(lineNotify).toContain('testSessionIdForBookingCode');
    expect(lineNotify).toContain('testSessionId: await testSessionFor(bookingCode)');
    expect(lineNotify).toContain('const broadcastTestSessionId = await testSessionFor(bookingCode)');
    expect(lineNotify).not.toMatch(/testSessionId:\s*body\./);
  });

  test('calendar events are suppressed for a test booking', () => {
    expect(gcalJs).toContain('const testSessionId = await testSessionFor(booking.bookingCode)');
    expect(gcalJs).toContain("suppressed: 'test_mode'");
    // The suppression has to come before the event is created, not after.
    expect(gcalJs.indexOf("suppressed: 'test_mode'")).toBeLessThan(gcalJs.indexOf('await createCalendarEvent(booking)'));
  });

  test('slip verification is refused so slip_registry is never seeded by a test', () => {
    expect(slipVerify).toContain('TEST_MODE_SLIP_BLOCKED');
    expect(slipVerify.indexOf('TEST_MODE_SLIP_BLOCKED')).toBeLessThan(slipVerify.indexOf("db.collection('slip_registry')"));
  });

  test('payment in a test session settles through the server, with no slip and no verification claim', () => {
    expect(bookingJs).toContain('handleTestSimulatePayment');
    expect(bookingJs).toContain('paymentSimulated: true');
    expect(bookingJs).toContain('belongsToTestSession(b, session.testSessionId)');
    // A simulated payment must not leave anything that reads as a checked slip.
    const handler = bookingJs.slice(bookingJs.indexOf('async function handleTestSimulatePayment'), bookingJs.indexOf('// create_pass_booking — Security Hotfix'));
    expect(handler).not.toMatch(/paymentVerification:\s*\{/);
    expect(handler).toContain("throw new Error('NOT_THIS_SESSION')");
  });
});

describe('test bookings are excluded from every money total', () => {
  const range = { from: '2026-09-01', to: '2026-09-30' };
  const at = (id, extra) => ({ id, data: {
    date: '2026-09-21', startTime: '20:00', endTime: '21:00',
    bookingStatus: 'confirmed', paymentStatus: 'paid', price: 350, ...extra,
  } });

  test('a test booking contributes no revenue, no count and no hours to the AI report', () => {
    const report = buildAiBookingReport([
      at('live'),
      at('test', { isTest: true, testSessionId: 'ts_1' }),
    ], range);
    expect(report.metrics).toMatchObject({ recordsTotal: 1, bookingsTotal: 1, paidBookingCount: 1, paidRevenue: 350, totalBookedMinutes: 60 });
  });

  test('a record carrying only testSessionId is excluded too', () => {
    const report = buildAiBookingReport([at('half-marked', { testSessionId: 'ts_1' })], range);
    expect(report.metrics).toMatchObject({ recordsTotal: 0, paidRevenue: 0 });
  });

  test('test package usage does not reach packageUsageValue either', () => {
    const report = buildAiBookingReport([
      at('test-pass', { paymentStatus: 'package', packageType: 'ultra_pass_10', isTest: true, testSessionId: 'ts_1' }),
    ], range);
    expect(report.metrics).toMatchObject({ packageBookingCount: 0, packageUsageValue: 0 });
  });

  test('a test booking never appears in a breakdown, only in the totals it is kept out of', () => {
    const report = buildAiBookingReport([at('t', { isTest: true, testSessionId: 'ts_1', source: 'test-run' })], range);
    expect(JSON.stringify(report.breakdown)).not.toContain('test-run');
  });

  test.each(Object.keys(REPORTING_SOURCES))('%s filters before it totals anything', name => {
    expect(REPORTING_SOURCES[name]).toContain('isLiveBooking');
  });

  test('the Finance page cannot filter for itself, so its feed must', () => {
    // ultra-finance.html reads bookings only from /api/finance-data, and the
    // projection there does not carry the test fields — the filter has to be
    // upstream of that projection or the page can never tell.
    expect(financeData).toContain('bookSnap.docs.filter(d => isLiveBooking(d.data()))');
    expect(read('ultra-finance.html')).not.toContain('isTest');
  });

  test('the dashboard drops test bookings before any bucket is touched', () => {
    const loop = adminOps.slice(adminOps.indexOf('bkSnap.docs.forEach'), adminOps.indexOf('avSnap.docs.forEach'));
    expect(loop.indexOf('if (!isLiveBooking(bk)) return;')).toBeLessThan(loop.indexOf('bucket(resolveBranchId(bk))'));
  });
});
