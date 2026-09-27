import { beforeAll, beforeEach, afterEach, expect, test, vi } from 'vitest';
import { syncCustomerRescheduleCalendar } from '../api/gcal.js';
let db;
beforeAll(async () => {
  if (!process.env.FIRESTORE_EMULATOR_HOST) throw new Error('Emulator required');
  db = (await import('../api/_lib/firebase-admin.js')).getAdminDb();
});
beforeEach(async () => {
  for (const name of ['GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET', 'GOOGLE_REFRESH_TOKEN', 'GOOGLE_CALENDAR_ID']) vi.stubEnv(name, 'local-test-only');
  await db.collection('bookings').doc('calendar-pending-test').set({ bookingCode: 'CAL-PENDING', bookingStatus: 'confirmed', pendingReschedule: false,
    date: '2030-06-17', startTime: '16:00', endTime: '17:00', paymentStatus: 'paid', customerName: 'Private Name', lineUserId: 'private-line' });
});
afterEach(() => { vi.unstubAllGlobals(); vi.unstubAllEnvs(); });
const run = () => syncCustomerRescheduleCalendar(db, 'calendar-pending-test');
const read = async () => (await db.collection('bookings').doc('calendar-pending-test').get()).data();

test('server sync creates a privacy-safe calendar event and records success', async () => {
  const fetch = vi.fn(async url => ({ ok: true, json: async () => url.includes('oauth2') ? { access_token: 'fake' } : { id: 'event', htmlLink: 'https://example.invalid/event' } }));
  vi.stubGlobal('fetch', fetch);
  await run();
  expect(await read()).toMatchObject({ googleCalendarSyncStatus: 'updated', googleCalendarEventId: 'event' });
  const options = fetch.mock.calls[1][1];
  expect(options.signal).toBeDefined();
  expect(options.body).toContain('CAL-PENDING');
  expect(options.body).not.toContain('Private Name');
  expect(options.body).not.toContain('private-line');
});
test('calendar failure leaves booking confirmed with an admin retry status', async () => {
  vi.stubGlobal('fetch', vi.fn(async () => { throw new Error('Unavailable'); }));
  await run();
  expect(await read()).toMatchObject({ bookingStatus: 'confirmed', paymentStatus: 'paid', googleCalendarSyncStatus: 'failed' });
});
test('a previously removed event is recreated', async () => {
  await db.collection('bookings').doc('calendar-pending-test').update({ googleCalendarEventId: 'deleted-event' });
  const fetch = vi.fn(async (url, options) => options.method === 'PATCH' ? { ok: false, status: 404, text: async () => 'Deleted' } : { ok: true, json: async () => url.includes('oauth2') ? { access_token: 'fake' } : { id: 'replacement' } });
  vi.stubGlobal('fetch', fetch);
  await run();
  expect((await read()).googleCalendarEventId).toBe('replacement');
});
test.each([{ isTest: true }, { pendingReschedule: true }, { bookingStatus: 'cancelled' }])('does not sync ineligible booking %j', async changes => {
  vi.stubGlobal('fetch', vi.fn());
  await db.collection('bookings').doc('calendar-pending-test').update(changes);
  await run();
  expect(fetch).not.toHaveBeenCalled();
});
