import { beforeAll, beforeEach, describe, expect, test, vi } from 'vitest';
import { Timestamp } from 'firebase-admin/firestore';
vi.mock('../api/gcal.js', () => ({ syncCustomerRescheduleCalendar: vi.fn() }));

vi.mock('firebase-admin/auth', async original => ({ ...await original(), getAuth: () => ({ verifyIdToken: async token => {
  if (!['customer', 'other'].includes(token)) throw new Error('Invalid token');
  return { uid: token };
} }) }));
vi.mock('../api/_lib/notify.js', () => ({ sendAndLog: vi.fn(), loadActiveAdmins: vi.fn(async () => []), loadNotificationFlags: vi.fn(async () => ({})) }));
let db, handler, adminHandler, cookie;
const DATE = '2030-06-17';
const slot = time => `room1_${DATE}_${time.replace(':', '')}`;
const base = () => ({ bookingCode: 'PENDING-A', lineUserId: 'customer', customerName: 'Customer', customerPhone: '0800000000', resourceId: 'room1', branchId: 'ladprao1',
  date: DATE, startTime: '14:00', endTime: '16:00', durationMinutes: 120, durationHours: 2, bookingSlotIds: [],
  pendingRescheduleFromDate: DATE, pendingRescheduleFromStartTime: '14:00', pendingRescheduleFromEndTime: '16:00',
  bookingStatus: 'rescheduled', pendingReschedule: true, pendingRescheduleStatus: 'pending', paymentStatus: 'paid', bookingType: 'Court Booking', price: 700, amount: 700 });
beforeAll(async () => {
  if (!process.env.FIRESTORE_EMULATOR_HOST) throw new Error('Local emulator required');
  process.env.ADMIN_SESSION_SECRET = 'pending-local-test-secret';
  process.env.ADMIN_USERS_JSON = JSON.stringify({ Art: { pin: '0000', role: 'owner', branches: '*' } });
  db = (await import('../api/_lib/firebase-admin.js')).getAdminDb();
  handler = (await import('../api/booking.js')).default;
  adminHandler = (await import('../api/admin-edit-booking-accounting.js')).default;
  cookie = (await import('../api/_lib/admin-auth.js')).createSessionCookie('Art').split(';')[0];
});
beforeEach(async () => {
  for (const name of ['bookings', 'booking_slots', 'booking_slot_claims', 'available_slots', 'customer_packages', 'finance_document_counters', 'finance_documents', 'holidays', 'audit_logs', 'commerce_settings']) {
    const snap = await db.collection(name).get();
    await Promise.all(snap.docs.map(d => d.ref.delete()));
  }
  await db.collection('system_settings').doc('pricing').set({});
  await db.collection('system_settings').doc('features').set({ enableHalfHourBooking: true }, { merge: true });
  await db.collection('bookings').doc('a').set(base());
  for (let h = 6; h < 24; h++) await db.collection('available_slots').doc(slot(`${h.toString().padStart(2, '0')}:00`)).set({ resourceId: 'room1', date: DATE, status: 'open' });
});
async function call(body = {}, admin = false) {
  const result = { statusCode: 0, body: null, setHeader() {}, status(code) { this.statusCode = code; return this; }, json(data) { this.body = data; return this; } };
  await (admin ? adminHandler : handler)({ method: 'POST', headers: admin ? { cookie } : {}, socket: {}, body: admin ? body : { action: 'assign_pending_reschedule', idToken: 'customer', bookingId: 'a', newDate: DATE, newStartTime: '16:00', assignDurationMinutes: 120, ...body } }, result);
  return result;
}
const get = async id => (await db.collection('bookings').doc(id).get()).data();

describe('customer pending hours', () => {
  test('list is scoped to verified LINE identity and excludes tests and assigned bookings', async () => {
    await db.collection('bookings').doc('other').set({ ...base(), lineUserId: 'other' });
    await db.collection('bookings').doc('test').set({ ...base(), isTest: true });
    await db.collection('bookings').doc('done').set({ ...base(), bookingStatus: 'confirmed', pendingReschedule: false });
    const result = await call({ action: 'pending_reschedules', lineUserId: 'other' });
    expect(result.body.bookings.map(b => b.id)).toEqual(['a']);
    expect(result.body.totalMinutes).toBe(120);
  });
  test.each(['', 'invalid'])('rejects missing/invalid auth (%s)', async idToken => {
    expect((await call({ idToken })).statusCode).toBe(401);
    expect((await get('a')).pendingReschedule).toBe(true);
  });
  test('cannot spend another customer entitlement or forge payment/court fields', async () => {
    expect((await call({ idToken: 'other', lineUserId: 'customer' })).statusCode).toBe(404);
    expect((await call({ newResourceId: 'room2', price: 0, paymentStatus: 'unpaid' })).statusCode).toBe(200);
    expect(await get('a')).toMatchObject({ resourceId: 'room1', price: 700, paymentStatus: 'paid' });
  });
  test('full assignment preserves payment and finance documents', async () => {
    await db.collection('finance_documents').doc('receipt').set({ total: 700, bookingId: 'a' });
    expect((await call()).statusCode).toBe(200);
    expect(await get('a')).toMatchObject({ price: 700, amount: 700, paymentStatus: 'paid', bookingStatus: 'confirmed', pendingReschedule: false, startTime: '16:00' });
    expect((await db.collection('finance_documents').doc('receipt').get()).data().total).toBe(700);
    expect((await db.collection('booking_slot_claims').doc(slot('16:00')).get()).data().bookingId).toBe('a');
    expect((await call()).statusCode).toBe(409);
  });
  test('partial assignment keeps the remaining hours and total financial snapshot, including coupon', async () => {
    await db.collection('bookings').doc('a').update({ price: 600, amount: 600, pricingSnapshot: { version: 1, subtotal: 700, total: 600, promotionDiscount: 0, couponDiscount: 100, discountTotal: 100 } });
    const result = await call({ assignDurationMinutes: 60 });
    expect(result.statusCode).toBe(200);
    const assigned = await get('a'), rest = await get(result.body.remainderBookingId);
    expect(assigned.price + rest.price).toBe(600);
    expect(assigned.pricingSnapshot.total + rest.pricingSnapshot.total).toBe(600);
    expect(assigned.pricingSnapshot.couponDiscount + rest.pricingSnapshot.couponDiscount).toBe(100);
    expect(rest).toMatchObject({ durationMinutes: 60, pendingReschedule: true, bookingSlotIds: [], lineUserId: 'customer' });
    expect((await call({ bookingId: result.body.remainderBookingId, assignDurationMinutes: 60, newStartTime: '17:00' })).statusCode).toBe(200);
  });
  test('an issued receipt allows full assignment but blocks splitting', async () => {
    await db.collection('finance_document_counters').doc('active_receipt_booking_a').set({ documentId: 'receipt' });
    expect((await call({ assignDurationMinutes: 60 })).statusCode).toBe(409);
    expect((await call()).statusCode).toBe(200);
  });
  test('Ultra Pass is not deducted again and cannot move past expiry', async () => {
    await db.collection('customer_packages').doc('p').set({ lineUserId: 'customer', status: 'active', packageType: 'ultra_pass_10', remainingMinutes: 480, validUntil: Timestamp.fromDate(new Date('2030-06-18T00:00:00+07:00')) });
    await db.collection('bookings').doc('a').update({ paymentStatus: 'package', packageType: 'ultra_pass_10', packageId: 'p', usedPackageId: 'p', packageMinutesUsed: 120 });
    expect((await call({ newDate: '2030-06-19' })).statusCode).toBe(409);
    const result = await call({ assignDurationMinutes: 60 });
    expect(result.statusCode).toBe(200);
    expect((await db.collection('customer_packages').doc('p').get()).data().remainingMinutes).toBe(480);
    expect((await get('a')).packageMinutesUsed + (await get(result.body.remainderBookingId)).packageMinutesUsed).toBe(120);
  });
  test.each([{ bookingStatus: 'confirmed', pendingReschedule: false }, { refundAmount: 100 }, { packageMinutesRestored: 120 }, { paymentStatus: 'unpaid' }, { coachId: 'coach' }, { packageType: 'offpeak', paymentStatus: 'package' }, { packageType: 'monstr_event_pass' }, { isTest: true }])('rejects ineligible entitlement %j', async changes => {
    await db.collection('bookings').doc('a').update(changes);
    expect((await call()).statusCode).toBeGreaterThanOrEqual(400);
    expect((await db.collection('booking_slot_claims').get()).empty).toBe(true);
  });
  test.each([{ newDate: '2030-02-30' }, { newDate: '2020-01-01' }, { newStartTime: '99:00' }, { assignDurationMinutes: 150 }, { assignDurationMinutes: -30 }])('rejects invalid target %j', async target => {
    expect((await call(target)).statusCode).toBeGreaterThanOrEqual(400);
  });
  test('availability hides higher-priced times and committed slots; server rechecks both', async () => {
    await db.collection('bookings').doc('a').update({ price: 640, amount: 640 });
    let result = await call({ action: 'pending_reschedule_options', assignDurationMinutes: 60 });
    expect(result.body.times).toContain('10:00');
    expect(result.body.times).not.toContain('16:00');
    expect((await call()).statusCode).toBe(409);
    await db.collection('booking_slots').doc(slot('10:00')).set({ resourceId: 'room1', date: DATE, hour: '10:00', bookingStatus: 'confirmed', slotSpanMinutes: 60 });
    result = await call({ action: 'pending_reschedule_options', assignDurationMinutes: 60 });
    expect(result.body.times).not.toContain('10:00');
    expect((await call({ newStartTime: '10:00', assignDurationMinutes: 60 })).statusCode).toBe(409);
  });
  test('a parked booking cannot overwrite someone occupying its former slot', async () => {
    await db.collection('booking_slots').doc(slot('14:00')).set({ resourceId: 'room1', date: DATE, hour: '14:00', bookingStatus: 'confirmed' });
    await db.collection('booking_slot_claims').doc(slot('14:00')).set({ bookingId: 'someone-else' });
    expect((await call({ newStartTime: '14:00' })).statusCode).toBe(409);
    expect((await db.collection('booking_slot_claims').doc(slot('14:00')).get()).data().bookingId).toBe('someone-else');
  });
  test('two simultaneous spends of the same entitlement commit only once', async () => {
    const results = await Promise.all([call(), call({ newStartTime: '18:00' })]);
    expect(results.map(r => r.statusCode).sort()).toEqual([200, 409]);
    expect((await db.collection('booking_slot_claims').get()).size).toBe(2);
    expect((await db.collection('bookings').get()).size).toBe(1);
  });
  test('customer and admin assigning the same pending booking cannot leave duplicate claims', async () => {
    const results = await Promise.all([call(), call({ operation: 'reschedule_assign', bookingId: 'a', newDate: DATE, newStartTime: '18:00' }, true)]);
    expect(results.map(r => r.statusCode).sort()).toEqual([200, 409]);
    expect((await db.collection('booking_slot_claims').get()).size).toBe(2);
  });
  test('different pending bookings competing for one slot cannot both win', async () => {
    await db.collection('bookings').doc('b').set({ ...base(), bookingCode: 'PENDING-B' });
    const results = await Promise.all([call(), call({ bookingId: 'b' })]);
    expect(results.map(r => r.statusCode).sort()).toEqual([200, 409]);
    expect((await call({ action: 'pending_reschedules' })).body.totalMinutes).toBe(120);
  });
  test('same customer can park two separate bookings through the admin API', async () => {
    for (const [id, startTime] of [['a', '14:00'], ['b', '18:00']]) {
      await db.collection('bookings').doc(id).set({ ...base(), bookingCode: id, startTime, bookingStatus: 'confirmed', pendingReschedule: false });
      expect((await call({ operation: 'reschedule_park', bookingId: id }, true)).statusCode).toBe(200);
    }
    expect((await call({ action: 'pending_reschedules' })).body.totalMinutes).toBe(240);
  });
  test('parking an assigned booking again remembers the latest slot and releases it', async () => {
    expect((await call()).statusCode).toBe(200);
    expect((await call({ operation: 'reschedule_park', bookingId: 'a' }, true)).statusCode).toBe(200);
    expect(await get('a')).toMatchObject({ pendingRescheduleFromStartTime: '16:00', pendingRescheduleFromEndTime: '18:00', bookingSlotIds: [] });
    expect((await db.collection('booking_slot_claims').get()).empty).toBe(true);
    expect((await call({ newStartTime: '18:00' })).statusCode).toBe(200);
  });
  test('customer reassignment respects the half-hour booking switch', async () => {
    await db.collection('bookings').doc('a').update({ price: 800, amount: 800 });
    await db.collection('system_settings').doc('features').set({ enableHalfHourBooking: false }, { merge: true });
    const rejected = await call({ assignDurationMinutes: 30 });
    expect(rejected.statusCode).toBe(409);
    expect(rejected.body.error).toContain('เต็มชั่วโมง');
    await db.collection('system_settings').doc('features').set({ enableHalfHourBooking: true }, { merge: true });
    expect((await call({ assignDurationMinutes: 30 })).statusCode).toBe(200);
  });
});
