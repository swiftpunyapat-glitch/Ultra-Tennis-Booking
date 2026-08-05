// ════════════════════════════════════════════════════════════════════
// create_pass_booking — concurrency and idempotency against the real
// handler (second review, Phase 5)
// ════════════════════════════════════════════════════════════════════
// The idempotency unit tests exercise the helpers with a stand-in mutation.
// These drive the actual endpoint, so what is asserted is the real
// transaction: booking, private slot claims, public slot documents, package
// balance and the package movement log, all under concurrent load.
//
// Firebase Auth is stubbed rather than emulated — the endpoint only needs a
// verified uid, and stubbing keeps the test to one emulator.
// ════════════════════════════════════════════════════════════════════

import { describe, test, expect, beforeAll, beforeEach, vi } from 'vitest';

const UID = 'U_CONCURRENCY_TEST_USER';
const PKG = 'pkg_concurrency';
const DATE = '2027-03-15';           // far future, weekday (Monday)
const START = '10:00';

// Stub verifyIdToken before the handler module is loaded.
vi.mock('firebase-admin/auth', async (orig) => {
  const actual = await orig();
  return {
    ...actual,
    getAuth: () => ({ verifyIdToken: async () => ({ uid: UID }) }),
  };
});

let db, handler, entitlementTypeForPackage, readHolidayInTransaction;

beforeAll(async () => {
  if (!process.env.FIRESTORE_EMULATOR_HOST) {
    throw new Error('Refusing to run: FIRESTORE_EMULATOR_HOST is not set. These tests must never touch production.');
  }
  const fa = await import('../api/_lib/firebase-admin.js');
  db = fa.getAdminDb();
  const bookingModule = await import('../api/booking.js');
  handler = bookingModule.default;
  entitlementTypeForPackage = bookingModule.entitlementTypeForPackage;
  readHolidayInTransaction = bookingModule.readHolidayInTransaction;
});

// Minimal req/res doubles matching what the Vercel handler uses.
function makeReq(body) {
  return { method: 'POST', body, headers: { 'x-forwarded-for': '203.0.113.9' }, socket: {} };
}
function makeRes() {
  const r = { statusCode: null, body: null, headers: {} };
  r.status = (c) => { r.statusCode = c; return r; };
  r.json   = (b) => { r.body = b; return r; };
  r.setHeader = (k, v) => { r.headers[k] = v; };
  return r;
}

const call = async (body) => { const res = makeRes(); await handler(makeReq(body), res); return res; };

const baseBody = (overrides = {}) => ({
  action: 'create_pass_booking',
  idToken: 'stubbed',
  payType: 'ultra',
  packageId: PKG,
  date: DATE,
  startTime: START,
  durationMinutes: 60,
  customerName: 'Concurrency Tester',
  customerPhone: '0810000009',
  idempotencyKey: 'idem-default',
  ...overrides,
});

async function wipe() {
  for (const c of ['bookings', 'booking_slots', 'booking_slot_claims', 'idempotency_records', 'customer_package_logs', 'holidays']) {
    const snap = await db.collection(c).get();
    await Promise.all(snap.docs.map(d => d.ref.delete()));
  }
  await db.collection('customer_packages').doc(PKG).set({
    lineUserId: UID, packageType: 'ultra_pass_10', packageName: 'Ultra Pass 10 Hours',
    customerName: 'Concurrency Tester', customerPhone: '0810000009',
    remainingMinutes: 600, totalMinutes: 600, status: 'active',
    validUntil: new Date(Date.now() + 90 * 24 * 3600 * 1000),
  });
  await db.collection('available_slots').doc(`room1_${DATE}_1000`).set({
    resourceId: 'room1', date: DATE, startTime: START, endTime: '11:00', status: 'open',
  });
  await db.collection('system_settings').doc('features').set({ useServerPassBooking: true }, { merge: true });
}

beforeEach(wipe);

const countOf = async (c) => (await db.collection(c).get()).size;
const balance  = async () => (await db.collection('customer_packages').doc(PKG).get()).data().remainingMinutes;

describe('Feature flag gating', () => {
  test('the endpoint refuses while the flag is off, and does not fall back', async () => {
    await db.collection('system_settings').doc('features').set({ useServerPassBooking: false }, { merge: true });
    const res = await call(baseBody());
    expect(res.statusCode).toBe(403);
    expect(res.body.code).toBe('DISABLED');
    expect(await countOf('bookings')).toBe(0);   // no direct-Firestore fallback
  });
});

describe('idempotencyKey is mandatory (RB-02)', () => {
  test('a request without one is rejected before any write', async () => {
    const body = baseBody(); delete body.idempotencyKey;
    const res = await call(body);
    expect(res.statusCode).toBe(400);
    expect(res.body.code).toBe('IDEMPOTENCY');
    expect(await countOf('bookings')).toBe(0);
    expect(await balance()).toBe(600);
  });
});

describe('Single request writes one of everything', () => {
  test('booking, slot, claim, movement log and one deduction', async () => {
    const res = await call(baseBody({ idempotencyKey: 'k-single' }));
    expect(res.statusCode).toBe(200);
    expect(res.body.ok).toBe(true);

    expect(await countOf('bookings')).toBe(1);
    expect(await countOf('booking_slots')).toBe(1);
    expect(await countOf('booking_slot_claims')).toBe(1);
    expect(await countOf('customer_package_logs')).toBe(1);
    expect(await countOf('idempotency_records')).toBe(1);
    expect(await balance()).toBe(540);
  });

  test('the public slot carries no identifiers or PII (RB-10)', async () => {
    await call(baseBody({ idempotencyKey: 'k-slot' }));
    const slot = (await db.collection('booking_slots').get()).docs[0].data();
    for (const banned of ['bookingId', 'bookingCode', 'coachId', 'customerName',
                          'customerPhone', 'lineUserId', 'customerNote', 'slipUrl']) {
      expect(slot).not.toHaveProperty(banned);
    }
    expect(Object.keys(slot).sort()).toEqual(
      ['bookingStatus', 'branchId', 'date', 'expiresAt', 'hour', 'paymentStatus', 'resourceId', 'slotSpanMinutes'].sort(),
    );
  });

  test('the private claim carries the ownership linkage', async () => {
    const res = await call(baseBody({ idempotencyKey: 'k-claim' }));
    const claim = (await db.collection('booking_slot_claims').get()).docs[0].data();
    expect(claim.bookingId).toBe(res.body.booking.id);
    expect(claim.bookingCode).toBe(res.body.booking.bookingCode);
    expect(claim.branchId).toBe('ladprao1');
    expect(claim.resourceId).toBe('room1');
    expect(claim.status).toBe('confirmed');
    expect(claim.createdAt).toBeTruthy();
    expect(claim.updatedAt).toBeTruthy();
  });
});

describe('OR-01 stored package type is authoritative', () => {
  const storedTypes = {
    ultra: 'ultra_pass_10',
    offpeak: 'offpeak',
    event: 'monstr_event_pass',
  };
  const packageFields = {
    ultra: {},
    offpeak: { weeklyLimitHours:5, monthlyLimitHours:20, weeklyUsage:{}, monthlyUsage:{} },
    event: { branchId:'ladprao1', resourceId:'room1' },
  };

  test('maps every currently recognized packageType and fails closed for unknown types', () => {
    for (const type of ['ultra_starter_3','ultra_pass_10','ultra_pass_20','ultra_10','ultra_20']) {
      expect(entitlementTypeForPackage(type)).toBe('ultra');
    }
    expect(entitlementTypeForPackage('offpeak')).toBe('offpeak');
    expect(entitlementTypeForPackage('monstr_event_pass')).toBe('event');
    expect(entitlementTypeForPackage('future_unreviewed_type')).toBeNull();
  });

  test('rejects every cross-type spoof and accepts each matching assertion', async () => {
    for (const stored of Object.keys(storedTypes)) {
      for (const asserted of ['ultra','offpeak','event']) {
        await wipe();
        await db.collection('customer_packages').doc(PKG).update({
          packageType:storedTypes[stored], ...packageFields[stored],
        });
        const res = await call(baseBody({ payType:asserted, idempotencyKey:`cross-${stored}-${asserted}` }));
        if (stored === asserted) {
          expect(res.statusCode, `${stored} asserted as ${asserted}`).toBe(200);
        } else {
          expect(res.statusCode, `${stored} asserted as ${asserted}`).toBe(409);
          expect(res.body.code).toBe('PASS_TYPE_MISMATCH');
          expect(await countOf('bookings')).toBe(0);
          expect(await balance()).toBe(600);
        }
      }
    }
  });

  test('unknown stored type is rejected without mutation', async () => {
    await db.collection('customer_packages').doc(PKG).update({ packageType:'unknown_new_pass' });
    const res = await call(baseBody({ idempotencyKey:'unknown-type' }));
    expect(res.statusCode).toBe(409);
    expect(res.body.code).toBe('PASS_TYPE_UNSUPPORTED');
    expect(await countOf('bookings')).toBe(0);
    expect(await balance()).toBe(600);
  });
});

describe('OR-02 holiday validation fails closed', () => {
  beforeEach(async () => {
    await db.collection('customer_packages').doc(PKG).update({
      packageType:'offpeak', weeklyLimitHours:5, monthlyLimitHours:20,
      weeklyUsage:{}, monthlyUsage:{},
    });
  });

  test('missing holiday document means non-holiday', async () => {
    const res = await call(baseBody({ payType:'offpeak', idempotencyKey:'holiday-missing' }));
    expect(res.statusCode).toBe(200);
  });

  test('holiday document blocks a restricted entitlement', async () => {
    await db.collection('holidays').doc(DATE).set({ isHoliday:true });
    const res = await call(baseBody({ payType:'offpeak', idempotencyKey:'holiday-true' }));
    expect(res.statusCode).toBe(409);
    expect(res.body.code).toBe('PASS');
    expect(await countOf('bookings')).toBe(0);
    expect(await balance()).toBe(600);
  });

  test('transaction read failure is controlled and never becomes non-holiday', async () => {
    const tx = { get: vi.fn().mockRejectedValue(new Error('emulator read failed')) };
    await expect(readHolidayInTransaction(tx, { path:`holidays/${DATE}` }))
      .rejects.toThrow('HOLIDAY_CHECK_UNAVAILABLE');
  });
});

describe('Retry with the same key and request', () => {
  test('returns the original response and mutates nothing further', async () => {
    const first  = await call(baseBody({ idempotencyKey: 'k-retry' }));
    const second = await call(baseBody({ idempotencyKey: 'k-retry' }));

    expect(second.statusCode).toBe(200);
    expect(second.body.replayed).toBe(true);
    expect(second.body.booking.id).toBe(first.body.booking.id);
    expect(second.body.booking.bookingCode).toBe(first.body.booking.bookingCode);

    expect(await countOf('bookings')).toBe(1);
    expect(await countOf('customer_package_logs')).toBe(1);
    expect(await balance()).toBe(540);
  });

  test('the replayed response is never empty (RB-09)', async () => {
    await call(baseBody({ idempotencyKey: 'k-empty' }));
    const replay = await call(baseBody({ idempotencyKey: 'k-empty' }));
    expect(replay.body).toBeTruthy();
    expect(replay.body.booking).toBeTruthy();
    expect(replay.body.booking.id).toBeTruthy();
  });
});

describe('Same key, different request', () => {
  test('is a conflict rather than a silent replay or a second booking', async () => {
    await call(baseBody({ idempotencyKey: 'k-fp' }));
    const clash = await call(baseBody({ idempotencyKey: 'k-fp', startTime: '11:00' }));

    expect(clash.statusCode).toBe(409);
    expect(clash.body.code).toBe('IDEMPOTENCY_CONFLICT');
    expect(await countOf('bookings')).toBe(1);
    expect(await balance()).toBe(540);
  });
});

describe('Concurrent retries', () => {
  test('five parallel requests with one key produce exactly one of everything', async () => {
    const results = await Promise.all(
      Array.from({ length: 5 }, () => call(baseBody({ idempotencyKey: 'k-race' }))),
    );

    const ok = results.filter(r => r.statusCode === 200);
    expect(ok.length).toBeGreaterThan(0);

    // The invariants that matter: one booking, one deduction, one log.
    expect(await countOf('bookings')).toBe(1);
    expect(await countOf('booking_slots')).toBe(1);
    expect(await countOf('booking_slot_claims')).toBe(1);
    expect(await countOf('customer_package_logs')).toBe(1);
    expect(await balance()).toBe(540);

    // Every success describes the same booking.
    const ids = new Set(ok.map(r => r.body.booking.id));
    expect(ids.size).toBe(1);
  });

  test('parallel requests with different keys for the same slot: one wins', async () => {
    const results = await Promise.all(
      Array.from({ length: 4 }, (_, i) => call(baseBody({ idempotencyKey: `k-slot-race-${i}` }))),
    );

    const ok       = results.filter(r => r.statusCode === 200);
    const rejected = results.filter(r => r.statusCode === 409);

    expect(ok.length).toBe(1);
    expect(rejected.length).toBe(3);
    expect(await countOf('bookings')).toBe(1);
    expect(await balance()).toBe(540);            // deducted once, not four times
  });
});

describe('Failed transaction leaves nothing behind', () => {
  test('a closed slot writes no booking, no record, and no deduction', async () => {
    await db.collection('available_slots').doc(`room1_${DATE}_1000`).set({
      resourceId: 'room1', date: DATE, startTime: START, endTime: '11:00', status: 'closed',
    });

    const res = await call(baseBody({ idempotencyKey: 'k-closed' }));
    expect(res.statusCode).toBe(409);

    expect(await countOf('bookings')).toBe(0);
    expect(await countOf('idempotency_records')).toBe(0);   // key stays reusable
    expect(await countOf('customer_package_logs')).toBe(0);
    expect(await balance()).toBe(600);
  });

  test('an insufficient balance blocks the booking entirely', async () => {
    await db.collection('customer_packages').doc(PKG).update({ remainingMinutes: 30 });
    const res = await call(baseBody({ idempotencyKey: 'k-broke' }));

    expect(res.statusCode).toBe(409);
    expect(res.body.code).toBe('PASS');
    expect(await countOf('bookings')).toBe(0);
    expect(await countOf('idempotency_records')).toBe(0);
    expect(await balance()).toBe(30);
  });
});

describe('Ownership and duration are enforced server-side', () => {
  test('a pass belonging to someone else is refused (SEC-02)', async () => {
    await db.collection('customer_packages').doc(PKG).update({ lineUserId: 'U_SOMEONE_ELSE' });
    const res = await call(baseBody({ idempotencyKey: 'k-owner' }));

    expect(res.statusCode).toBe(409);
    expect(await countOf('bookings')).toBe(0);
    expect(await balance()).toBe(600);
  });

  test('booked duration is the deducted duration, not a fixed 60 (R-02)', async () => {
    await db.collection('available_slots').doc(`room1_${DATE}_1100`).set({
      resourceId: 'room1', date: DATE, startTime: '11:00', endTime: '12:00', status: 'open',
    });
    const res = await call(baseBody({ idempotencyKey: 'k-dur', durationMinutes: 120 }));

    expect(res.statusCode).toBe(200);
    expect(await balance()).toBe(480);                  // 120 deducted, not 60
    expect(await countOf('booking_slots')).toBe(2);
    expect(await countOf('booking_slot_claims')).toBe(2);
  });

  test('a non-hour duration is rejected', async () => {
    const res = await call(baseBody({ idempotencyKey: 'k-half', durationMinutes: 30 }));
    expect(res.statusCode).toBe(400);
    expect(await balance()).toBe(600);
  });

  test('a missing token is refused before anything is read', async () => {
    const body = baseBody({ idempotencyKey: 'k-noauth' }); delete body.idToken;
    const res = await call(body);
    expect(res.statusCode).toBe(403);
    expect(res.body.code).toBe('AUTH');
    expect(await countOf('bookings')).toBe(0);
  });
});
