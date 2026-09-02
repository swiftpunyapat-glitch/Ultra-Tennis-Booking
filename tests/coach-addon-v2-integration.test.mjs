import { beforeAll, beforeEach, describe, expect, test, vi } from 'vitest';
import { Timestamp } from 'firebase-admin/firestore';

const UID = 'U_COACH_ADDON_V2_TEST';
const DATE = '2027-06-14';
const COACH = 'coach-v2';
const ULTRA = 'ultra-v2';
const BEGINNER = 'beginner-v2';

vi.mock('firebase-admin/auth', async original => {
  const actual = await original();
  return { ...actual, getAuth: () => ({ verifyIdToken: async () => ({ uid: UID }) }) };
});

let db, bookingHandler, slipHandler, accountingHandler, adminCookie;

beforeAll(async () => {
  if (!process.env.FIRESTORE_EMULATOR_HOST) throw new Error('Local Firestore emulator required');
  process.env.ADMIN_SESSION_SECRET = 'coach-addon-v2-test-secret';
  process.env.ADMIN_USERS_JSON = JSON.stringify({ Art: { pin: '0000', role: 'owner', branches: '*' } });
  const firebase = await import('../api/_lib/firebase-admin.js');
  db = firebase.getAdminDb();
  bookingHandler = (await import('../api/booking.js')).default;
  slipHandler = (await import('../api/slip-verify.js')).default;
  accountingHandler = (await import('../api/admin-edit-booking-accounting.js')).default;
  const { createSessionCookie } = await import('../api/_lib/admin-auth.js');
  adminCookie = createSessionCookie('Art').split(';')[0];
});

function request(body, admin = false) {
  return { method: 'POST', body, headers: { 'x-forwarded-for': '198.51.100.44', ...(admin ? { cookie: adminCookie } : {}) }, socket: {} };
}
function response() {
  const value = { statusCode: null, body: null, headers: {} };
  value.status = code => { value.statusCode = code; return value; };
  value.json = body => { value.body = body; return value; };
  value.setHeader = (key, data) => { value.headers[key] = data; };
  return value;
}
async function call(handler, body, admin = false) {
  const out = response();
  await handler(request(body, admin), out);
  return out;
}

const roomSlotId = time => `room1_${DATE}_${time.replace(':', '')}`;
const coachClaimId = time => `${COACH}_${DATE}_${time.replace(':', '')}`;
const storageUrl = suffix => `https://firebasestorage.googleapis.com/v0/b/ultra-tennis-booking.appspot.com/o/payment_slips%2Fcoach-v2-${suffix}.jpg?alt=media&token=test`;

async function wipe() {
  for (const collection of [
    'bookings', 'booking_slots', 'booking_slot_claims', 'coach_slot_claims',
    'coach_availability', 'coaches', 'available_slots', 'customer_packages',
    'customer_package_logs', 'idempotency_records', 'audit_logs', 'guest_booking_access',
    'rate_limits', 'holidays', 'finance_expenses',
  ]) {
    const snap = await db.collection(collection).get();
    await Promise.all(snap.docs.map(doc => doc.ref.delete()));
  }
  await db.collection('system_settings').doc('features').set({
    enableCoachAddonV2: true, useServerSlipSubmit: true,
  }, { merge: true });
  await db.collection('system_settings').doc('pricing').set({ normalPrice: 350 });
  await db.collection('coaches').doc(COACH).set({
    name: COACH, displayName: 'Coach V2', active: true,
    lessonPrice: 900, payoutPerHour: 550, branchId: 'ladprao1',
  });
  for (const hour of ['10:00', '11:00', '12:00']) {
    await db.collection('available_slots').doc(roomSlotId(hour)).set({
      resourceId: 'room1', branchId: 'ladprao1', date: DATE,
      startTime: hour, status: 'open',
    });
    await db.collection('coach_availability').doc(`${COACH}_${DATE}_${hour.replace(':', '')}`).set({
      coachId: COACH, branchId: 'ladprao1', date: DATE, hour, status: 'open',
    });
  }
  const validUntil = Timestamp.fromMillis(Date.now() + 365 * 24 * 3600_000);
  await db.collection('customer_packages').doc(ULTRA).set({
    lineUserId: UID, packageType: 'ultra_pass_10', packageName: 'Ultra Pass 10 Hours',
    remainingMinutes: 600, totalMinutes: 600, status: 'active', validUntil,
  });
  await db.collection('customer_packages').doc(BEGINNER).set({
    lineUserId: UID, packageType: 'beginner_coaching_5', packageName: 'Beginner Coaching',
    remainingMinutes: 300, totalMinutes: 300, status: 'active', validUntil,
  });
}

beforeEach(wipe);

const createBody = (overrides = {}) => ({
  action: 'create_coach_addon_v2', idToken: 'stubbed', lineUserId: UID,
  idempotencyKey: 'coach-v2-default', date: DATE, startTime: '10:00', durationMinutes: 60,
  coachId: COACH, studentCount: 1, fundingMode: 'cash',
  customerName: 'Coach Add-on Tester', customerPhone: '0810000099',
  ...overrides,
});

describe('Coach Add-on v2 feature gate and atomic claims', () => {
  test('flag off refuses with zero writes', async () => {
    await db.collection('system_settings').doc('features').set({ enableCoachAddonV2: false }, { merge: true });
    const result = await call(bookingHandler, createBody());
    expect(result.statusCode).toBe(403);
    expect((await db.collection('bookings').get()).size).toBe(0);
  });

  test('90-minute cash booking creates court and 30-minute coach claims atomically', async () => {
    const result = await call(bookingHandler, createBody({ durationMinutes: 90, idempotencyKey: 'cash-90' }));
    expect(result.statusCode).toBe(200);
    expect(result.body.booking.priceBreakdown).toMatchObject({
      courtCashAmount: 520, lessonGrossAmount: 1350, coachChargeAmount: 830,
      coachPayoutAmount: 825, cashDueAmount: 1350,
    });
    expect((await db.collection('booking_slots').get()).size).toBe(2);
    expect((await db.collection('booking_slot_claims').get()).size).toBe(2);
    expect((await db.collection('coach_slot_claims').get()).size).toBe(3);
  });

  test('two simultaneous requests for the same court and coach allow one winner', async () => {
    const results = await Promise.all([
      call(bookingHandler, createBody({ idempotencyKey: 'race-a' })),
      call(bookingHandler, createBody({ idempotencyKey: 'race-b', customerPhone: '0820000099' })),
    ]);
    expect(results.filter(item => item.statusCode === 200)).toHaveLength(1);
    expect(results.filter(item => item.statusCode === 409)).toHaveLength(1);
    expect((await db.collection('bookings').get()).size).toBe(1);
    expect((await db.collection('coach_slot_claims').get()).size).toBe(2);
  });
});

describe('Coach Add-on v2 mixed payment lifecycle', () => {
  test('Ultra Pass reserves court minutes while coach remains cash', async () => {
    const result = await call(bookingHandler, createBody({
      fundingMode: 'ultra_pass', packageId: ULTRA, durationMinutes: 90, idempotencyKey: 'ultra-90',
    }));
    expect(result.statusCode).toBe(200);
    expect(result.body.booking).toMatchObject({ fundingSource: 'mixed', bookingState: 'held', cashState: 'unpaid', packageUsageState: 'reserved' });
    expect(result.body.booking.priceBreakdown).toMatchObject({ courtPackageMinutes: 90, courtCashAmount: 0, coachChargeAmount: 830, cashDueAmount: 830 });
    expect((await db.collection('customer_packages').doc(ULTRA).get()).data().remainingMinutes).toBe(510);
  });

  test('Extra Person ฿100 is entirely frozen into coach payout', async () => {
    const result = await call(bookingHandler, createBody({
      fundingMode: 'ultra_pass', packageId: ULTRA, studentCount: 2, idempotencyKey: 'ultra-extra',
    }));
    expect(result.body.booking.priceBreakdown).toMatchObject({
      extraPersonFee: 100, extraPersonCoachPayout: 100,
      coachBasePayoutAmount: 550, coachPayoutAmount: 650, cashDueAmount: 680,
    });
  });

  test('completed lesson pays the frozen base plus full extra-person fee through the existing payout flow', async () => {
    const created = await call(bookingHandler, createBody({
      fundingMode: 'ultra_pass', packageId: ULTRA, studentCount: 2, idempotencyKey: 'payout-extra',
    }));
    const bookingId = created.body.booking.id;
    expect((await call(accountingHandler, { operation: 'approve_slip', bookingId, withoutSlip: true }, true)).statusCode).toBe(200);
    expect((await call(accountingHandler, { operation: 'coach_lesson_update', bookingId, lessonAction: 'complete' }, true)).statusCode).toBe(200);
    expect((await call(accountingHandler, { operation: 'coach_payout_paid', bookingId }, true)).statusCode).toBe(200);
    const booking = (await db.collection('bookings').doc(bookingId).get()).data();
    expect(booking).toMatchObject({ bookingState: 'completed', coachPayoutAmount: 650, coachPayoutStatus: 'paid' });
    const expenses = await db.collection('finance_expenses').where('sourceBookingId', '==', bookingId).get();
    expect(expenses.size).toBe(1);
    expect(expenses.docs[0].data().amount).toBe(650);
  });

  test('Beginner Coaching consumes entitlement and never charges base coach fee', async () => {
    const result = await call(bookingHandler, createBody({
      fundingMode: 'coaching_package', packageId: BEGINNER, durationMinutes: 120, idempotencyKey: 'beginner-120',
    }));
    expect(result.statusCode).toBe(200);
    expect(result.body.requiresPayment).toBe(false);
    expect(result.body.booking).toMatchObject({
      fundingSource: 'coaching_package', bookingState: 'confirmed', cashState: 'not_required', packageUsageState: 'consumed',
    });
    expect(result.body.booking.priceBreakdown).toMatchObject({ coachChargeAmount: 0, cashDueAmount: 0, coachPayoutAmount: 1100 });
    expect((await db.collection('customer_packages').doc(BEGINNER).get()).data().remainingMinutes).toBe(180);
  });

  test('admin cancellation of a held mixed booking releases every claim and reservation idempotently', async () => {
    const created = await call(bookingHandler, createBody({
      fundingMode: 'ultra_pass', packageId: ULTRA, durationMinutes: 150, idempotencyKey: 'cancel-held-mixed',
    }));
    const bookingId = created.body.booking.id;
    const first = await call(accountingHandler, { operation: 'reject_payment', bookingId, reason: 'customer cancelled' }, true);
    const replay = await call(accountingHandler, { operation: 'reject_payment', bookingId, reason: 'customer cancelled' }, true);
    expect(first.statusCode).toBe(200);
    expect(replay.statusCode).toBe(200);
    expect(replay.body.replayed).toBe(true);
    expect((await db.collection('bookings').doc(bookingId).get()).data()).toMatchObject({
      bookingState: 'cancelled', packageUsageState: 'released',
    });
    expect((await db.collection('customer_packages').doc(ULTRA).get()).data().remainingMinutes).toBe(600);
    expect((await db.collection('booking_slots').get()).size).toBe(0);
    expect((await db.collection('booking_slot_claims').get()).size).toBe(0);
    expect((await db.collection('coach_slot_claims').get()).size).toBe(0);
  });

  test('expired unpaid hold restores package and releases both resources idempotently', async () => {
    const created = await call(bookingHandler, createBody({ fundingMode: 'ultra_pass', packageId: ULTRA, idempotencyKey: 'expire-one' }));
    const bookingId = created.body.booking.id;
    const old = Timestamp.fromMillis(Date.now() - 1000);
    await db.collection('bookings').doc(bookingId).update({ paymentExpiresAt: old });
    for (const collection of ['booking_slots', 'booking_slot_claims', 'coach_slot_claims']) {
      const snap = await db.collection(collection).get();
      await Promise.all(snap.docs.map(doc => doc.ref.update({ expiresAt: old })));
    }
    const first = await call(bookingHandler, { action: 'expire_coach_addon_v2', bookingId, idToken: 'stubbed' });
    const second = await call(bookingHandler, { action: 'expire_coach_addon_v2', bookingId, idToken: 'stubbed' });
    expect(first.statusCode).toBe(200);
    expect(second.body.replayed).toBe(true);
    expect((await db.collection('customer_packages').doc(ULTRA).get()).data().remainingMinutes).toBe(600);
    expect((await db.collection('booking_slots').get()).size).toBe(0);
    expect((await db.collection('coach_slot_claims').get()).size).toBe(0);
  });

  test('slip submitted before deadline can be approved after original expiry', async () => {
    const created = await call(bookingHandler, createBody({ fundingMode: 'ultra_pass', packageId: ULTRA, idempotencyKey: 'slip-before-expiry' }));
    const bookingId = created.body.booking.id;
    const submitted = await call(slipHandler, {
      action: 'submit_slip', bookingId, bookingCode: created.body.booking.bookingCode,
      idToken: 'stubbed', slipUrl: storageUrl('approved-late'), idempotencyKey: 'slip-approved-late',
    });
    expect(submitted.statusCode).toBe(200);
    await db.collection('bookings').doc(bookingId).update({ paymentExpiresAt: Timestamp.fromMillis(Date.now() - 1000) });
    const approved = await call(accountingHandler, { operation: 'approve_slip', bookingId }, true);
    expect(approved.statusCode).toBe(200);
    const booking = (await db.collection('bookings').doc(bookingId).get()).data();
    expect(booking).toMatchObject({ bookingState: 'confirmed', cashState: 'paid', packageUsageState: 'consumed', cashPaidAmount: 580 });
    expect((await db.collection('customer_packages').doc(ULTRA).get()).data().remainingMinutes).toBe(540);
  });

  test('create and approval retries do not duplicate booking, deduction or transition', async () => {
    const body = createBody({ fundingMode: 'ultra_pass', packageId: ULTRA, idempotencyKey: 'retry-create' });
    const first = await call(bookingHandler, body);
    const replay = await call(bookingHandler, body);
    expect(replay.statusCode).toBe(200);
    expect(replay.body.replayed).toBe(true);
    expect(replay.body.booking.id).toBe(first.body.booking.id);
    expect((await db.collection('bookings').get()).size).toBe(1);
    expect((await db.collection('customer_packages').doc(ULTRA).get()).data().remainingMinutes).toBe(540);
    const approved = await call(accountingHandler, { operation: 'approve_slip', bookingId: first.body.booking.id, withoutSlip: true }, true);
    const approvedReplay = await call(accountingHandler, { operation: 'approve_slip', bookingId: first.body.booking.id, withoutSlip: true }, true);
    expect(approved.statusCode).toBe(200);
    expect(approvedReplay.statusCode).toBe(200);
    expect(approvedReplay.body.replayed).toBe(true);
    expect((await db.collection('customer_package_logs').where('action', '==', 'consume_reserved_minutes').get()).size).toBe(1);
  });
});
