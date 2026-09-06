import { beforeAll, beforeEach, describe, expect, test, vi } from 'vitest';
import { Timestamp, DocumentReference } from 'firebase-admin/firestore';
import { traceTransactions } from './helpers/transaction-trace.mjs';

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

// Real Firestore transactions; wrappers only control timing or inject a late error.
async function withTransactionHook(hook, work) {
  const original = db.runTransaction.bind(db);
  const spy = vi.spyOn(db, 'runTransaction').mockImplementation((callback, options) =>
    original(async t => hook(t, callback), options));
  try { return await work(); } finally { spy.mockRestore(); }
}

async function drainCompetitor(operation, release, work) {
  // Attach rejection handling immediately, even while winner assertions run.
  const settled = operation.then(value => ({ value }), error => ({ error }));
  let result;
  try { await work(); }
  finally { release(); result = await settled; }
  if (result.error) throw result.error;
  return result.value;
}

async function snapshotCollections(names) {
  return Promise.all(names.map(async name => {
    const snap = await db.collection(name).get();
    return snap.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  }));
}

async function tracedRace(name, work) {
  if (process.env.TRACE_TRANSACTIONS !== '1') return work();
  const events = [];
  try { return await traceTransactions(db, events, work); }
  finally { console.log(JSON.stringify({ transactionTrace: name, events })); }
}

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
  test('gated race cleanup drains the loser even when a winner assertion fails', async () => {
    let release, finish, finished = false, settled = false;
    const gate = new Promise(resolve => { release = resolve; });
    const finishGate = new Promise(resolve => { finish = resolve; });
    const loser = (async () => { await gate; await finishGate; finished = true; })();
    const assertion = new Error('injected winner assertion failure');
    const observed = drainCompetitor(loser, release, async () => { throw assertion; })
      .then(value => { settled = true; return { value }; }, error => { settled = true; return { error }; });
    try {
      await new Promise(resolve => setImmediate(resolve));
      expect(finished).toBe(false);
      expect(settled).toBe(false); // Cannot finish until the loser is allowed to finish.
    } finally {
      release(); finish();
      await Promise.allSettled([observed, loser]);
    }
    expect((await observed).error).toBe(assertion);
    expect(finished).toBe(true);
  });

  test('malformed claim reference types fail closed before constructing batch references', async () => {
    const made = await call(bookingHandler, createBody());
    const ref = db.collection('bookings').doc(made.body.booking.id);
    await ref.update({ coachClaimIds: [123, 456] });
    const names = ['bookings', 'booking_slots', 'booking_slot_claims', 'coach_slot_claims'];
    const before = await snapshotCollections(names);
    const result = await call(accountingHandler, { operation: 'mark_paid', bookingId: ref.id, amount: 900, paymentMethod: 'cash' }, true);
    expect(result.statusCode).toBe(409);
    expect(await snapshotCollections(names)).toEqual(before);
  });

  test.each(['disappeared', 'non_v2'])('approval rejects a booking that becomes %s after the route read', async change => {
    const made = await call(bookingHandler, createBody());
    expect(made.statusCode).toBe(200);
    const ref = db.collection('bookings').doc(made.body.booking.id);
    const original = DocumentReference.prototype.get;
    let intercepted = 0;
    const spy = vi.spyOn(DocumentReference.prototype, 'get').mockImplementation(async function (...args) {
      const snap = await original.apply(this, args);
      if (this.path === ref.path && ++intercepted === 1) {
        if (change === 'disappeared') await ref.delete();
        else await ref.set({ bookingStatus: 'pending', paymentStatus: 'unpaid' });
      }
      return snap;
    });
    let approved;
    try {
      approved = await call(accountingHandler, { operation: 'approve_slip', bookingId: ref.id, withoutSlip: true }, true);
    } finally { spy.mockRestore(); }
    expect(intercepted).toBe(2); // Route read succeeded; helper saw the mutation.
    expect(approved.statusCode).toBe(change === 'disappeared' ? 404 : 409);
    expect(approved.body.ok).toBe(false);
    expect((await db.collection('audit_logs').where('action', '==', 'coach_addon_v2_payment_confirmed').get()).size).toBe(0);
  });

  test.each([{ ok: false, code: 'UNKNOWN_CONFIRMATION_FAILURE' }, { ok: false }, null, { ok: true, confirmed: false }])('approval requires explicit confirmation success: %j', async result => {
    const made = await call(bookingHandler, createBody());
    const store = await import('../api/_lib/coach-addon-v2-store.js');
    const spy = vi.spyOn(store, 'confirmCoachAddonV2Payment').mockResolvedValueOnce(result);
    let approved;
    try {
      approved = await call(accountingHandler, { operation: 'approve_slip', bookingId: made.body.booking.id, withoutSlip: true }, true);
      expect(spy).toHaveBeenCalledOnce();
    } finally { spy.mockRestore(); }
    expect(approved.statusCode).toBe(500);
    expect(approved.body.ok).toBe(false);
    expect((await db.collection('audit_logs').where('action', '==', 'coach_addon_v2_payment_confirmed').get()).size).toBe(0);
    expect((await db.collection('bookings').doc(made.body.booking.id).get()).data().cashState).toBe('unpaid');
  });

  test.each(['court', 'coach', 'package'].flatMap(resource => ['confirm', 'release'].map(action => ({ resource, action }))))('$action refuses $resource references changed after the outer read', async ({ resource, action }) => {
    const made = await call(bookingHandler, createBody({ fundingMode: 'ultra_pass', packageId: ULTRA }));
    expect(made.statusCode).toBe(200);
    const bookingRef = db.collection('bookings').doc(made.body.booking.id);
    const booking = (await bookingRef.get()).data();
    const names = ['bookings', 'booking_slots', 'booking_slot_claims', 'coach_slot_claims', 'customer_packages', 'customer_package_logs'];
    const store = await import('../api/_lib/coach-addon-v2-store.js');
    let before, mutated = false;
    await withTransactionHook(async (t, callback) => {
      if (!mutated) {
        mutated = true;
        if (resource === 'package') {
          await db.collection('customer_packages').doc('replacement-package').set((await db.collection('customer_packages').doc(ULTRA).get()).data());
          await bookingRef.update({ packageId: 'replacement-package', usedPackageId: 'replacement-package' });
        } else {
          const court = resource === 'court';
          const field = court ? 'bookingSlotIds' : 'coachClaimIds';
          const previousId = court ? booking.resourceId : booking.coachId;
          const nextId = court ? 'room2' : 'replacement-coach';
          const ids = booking[field].map(id => nextId + id.slice(previousId.length));
          for (const collection of court ? ['booking_slots', 'booking_slot_claims'] : ['coach_slot_claims']) {
            for (let i = 0; i < ids.length; i++) {
              await db.collection(collection).doc(ids[i]).set((await db.collection(collection).doc(booking[field][i]).get()).data());
            }
          }
          await bookingRef.update({ [court ? 'resourceId' : 'coachId']: nextId, [field]: ids });
        }
        before = await snapshotCollections(names);
      }
      return callback(t);
    }, async () => {
      if (action === 'confirm') {
        await expect(store.confirmCoachAddonV2Payment(db, bookingRef.id)).rejects.toThrow('CLAIM_CONFLICT');
      } else {
        await expect(store.releaseCoachAddonV2Hold(db, bookingRef.id, { requireExpired: false })).rejects.toThrow('CLAIM_CONFLICT');
      }
    });
    expect(mutated).toBe(true);
    expect(await snapshotCollections(names)).toEqual(before);
  });
  test.each([undefined, [], ['partial'], ['duplicate', 'duplicate']])('R1: malformed coach claim list %j cannot confirm', async list => {
    const made = await call(bookingHandler, createBody());
    const id = made.body.booking.id;
    const original = (await db.collection('bookings').doc(id).get()).data().coachClaimIds;
    const { FieldValue } = await import('firebase-admin/firestore');
    await db.collection('bookings').doc(id).update({ coachClaimIds: list === undefined ? FieldValue.delete() : list.map(() => original[0]) });
    const before = await snapshotCollections(['bookings', 'booking_slots', 'booking_slot_claims', 'coach_slot_claims']);
    const paid = await call(accountingHandler, { operation: 'mark_paid', bookingId: id, amount: 900, paymentMethod: 'cash' }, true);
    expect(paid.statusCode).toBe(409);
    expect(await snapshotCollections(['bookings', 'booking_slots', 'booking_slot_claims', 'coach_slot_claims'])).toEqual(before);
  });

  test('R2: a missing reserved package cannot be consumed', async () => {
    const made = await call(bookingHandler, createBody({ fundingMode: 'ultra_pass', packageId: ULTRA }));
    const id = made.body.booking.id;
    await db.collection('customer_packages').doc(ULTRA).delete();
    const before = await snapshotCollections(['bookings', 'coach_slot_claims', 'customer_package_logs']);
    const paid = await call(accountingHandler, { operation: 'mark_paid', bookingId: id, amount: 580, paymentMethod: 'cash' }, true);
    expect(paid.statusCode).toBe(409);
    expect(await snapshotCollections(['bookings', 'coach_slot_claims', 'customer_package_logs'])).toEqual(before);
  });

  test.each(['owner', 'type', 'minutes', 'missing_id'])('R2: invalid reservation %s cannot confirm', async problem => {
    const made = await call(bookingHandler, createBody({ fundingMode: 'ultra_pass', packageId: ULTRA }));
    const bookingId = made.body.booking.id;
    if (problem === 'owner') await db.collection('customer_packages').doc(ULTRA).update({ lineUserId: 'another-user' });
    if (problem === 'type') await db.collection('customer_packages').doc(ULTRA).update({ packageType: 'beginner_coaching_5' });
    if (problem === 'minutes') await db.collection('bookings').doc(bookingId).update({ courtPackageMinutes: 0 });
    if (problem === 'missing_id') await db.collection('bookings').doc(bookingId).update({ packageId: null, usedPackageId: null });
    const before = await snapshotCollections(['bookings', 'customer_packages', 'customer_package_logs', 'coach_slot_claims']);
    expect((await call(accountingHandler, { operation: 'mark_paid', bookingId, amount: 580, paymentMethod: 'cash' }, true)).statusCode).toBe(409);
    expect(await snapshotCollections(['bookings', 'customer_packages', 'customer_package_logs', 'coach_slot_claims'])).toEqual(before);
  });

  test.each([{ durationMinutes: 90, startTime: '10:30' }, { durationMinutes: 150, startTime: '10:00' }])('R1: valid $durationMinutes minute claims at $startTime confirm completely', async range => {
    const made = await call(bookingHandler, createBody(range));
    expect(made.statusCode).toBe(200);
    const bookingId = made.body.booking.id;
    expect((await call(accountingHandler, { operation: 'mark_paid', bookingId, amount: made.body.booking.price, paymentMethod: 'cash' }, true)).statusCode).toBe(200);
    const claims = await db.collection('coach_slot_claims').get();
    expect(claims.size).toBe(range.durationMinutes / 30);
    claims.docs.forEach(doc => expect(doc.data().status).toBe('confirmed'));
  });

  test('R1: incomplete court claim list cannot confirm a multi-hour booking', async () => {
    const made = await call(bookingHandler, createBody({ durationMinutes: 120 }));
    const bookingId = made.body.booking.id;
    await db.collection('bookings').doc(bookingId).update({ bookingSlotIds: [roomSlotId('10:00')] });
    expect((await call(accountingHandler, { operation: 'mark_paid', bookingId, amount: 1800, paymentMethod: 'cash' }, true)).statusCode).toBe(409);
    expect((await db.collection('bookings').doc(bookingId).get()).data().cashState).toBe('unpaid');
  });

  test('R4: deadline crossed during reads rejects payment on the transaction decision clock', async () => {
    const made = await call(bookingHandler, createBody());
    const id = made.body.booking.id;
    const expiry = Date.now() + 60_000;
    await db.collection('bookings').doc(id).update({ paymentExpiresAt: Timestamp.fromMillis(expiry) });
    let current = expiry - 1;
    const { confirmCoachAddonV2Payment } = await import('../api/_lib/coach-addon-v2-store.js');
    const result = await withTransactionHook(async (t, callback) => {
      const getAll = t.getAll.bind(t);
      t.getAll = async (...refs) => { const snaps = await getAll(...refs); current = expiry; return snaps; };
      return callback(t);
    }, () => confirmCoachAddonV2Payment(db, id, { clock: () => current, manualPayment: { amount: 900, paymentMethod: 'cash', paymentNote: '' } }));
    expect(result).toMatchObject({ ok: false, code: 'HOLD_EXPIRED' });
    expect((await db.collection('bookings').doc(id).get()).data().bookingState).toBe('expired');
  });

  test('R4: a transaction retry rechecks the deadline and rolls back the earlier payment writes', async () => {
    const made = await call(bookingHandler, createBody({ fundingMode: 'ultra_pass', packageId: ULTRA }));
    const bookingId = made.body.booking.id;
    const expiry = Date.now() + 60_000;
    await db.collection('bookings').doc(bookingId).update({ paymentExpiresAt: Timestamp.fromMillis(expiry) });
    let current = expiry - 1, attempts = 0;
    const { confirmCoachAddonV2Payment } = await import('../api/_lib/coach-addon-v2-store.js');
    const result = await withTransactionHook(async (t, callback) => {
      const outcome = await callback(t);
      if (++attempts === 1) { current = expiry; throw Object.assign(new Error('INJECTED_ABORT'), { code: 10 }); }
      return outcome;
    }, () => confirmCoachAddonV2Payment(db, bookingId, { clock: () => current, manualPayment: { amount: 580, paymentMethod: 'cash', paymentNote: '' } }));
    expect(attempts).toBeGreaterThan(1);
    expect(result).toMatchObject({ ok: false, code: 'HOLD_EXPIRED' });
    expect((await db.collection('customer_package_logs').where('action', '==', 'consume_reserved_minutes').get()).size).toBe(0);
    expect((await db.collection('customer_packages').doc(ULTRA).get()).data().remainingMinutes).toBe(600);
  });

  test('R3: lost guest response replays the same usable token without plaintext in any written collection', async () => {
    const body = createBody({ idToken: undefined, lineUserId: 'guest' });
    const first = await call(bookingHandler, body); // Treat this committed response as lost.
    expect(first.statusCode).toBe(200);
    const token = first.body.guestAccessToken;
    const records = await snapshotCollections(['bookings', 'guest_booking_access', 'idempotency_records', 'booking_slots', 'booking_slot_claims', 'coach_slot_claims', 'audit_logs']);
    expect(JSON.stringify(records).includes(token)).toBe(false);
    const retry = await call(bookingHandler, body);
    expect(retry.statusCode).toBe(200);
    expect(retry.body.replayed).toBe(true);
    expect(retry.body.booking.id).toBe(first.body.booking.id);
    expect(retry.body.guestAccessToken).toBe(token);
    const { verifyGuestToken } = await import('../api/_lib/firebase-admin.js');
    expect((await verifyGuestToken(db, retry.body.booking.id, retry.body.guestAccessToken, 'booking:read')).ok).toBe(true);
    expect((await db.collection('bookings').get()).size).toBe(1);
  });

  test('R3: concurrent guest retries yield one booking and the same valid token', async () => {
    const body = createBody({ idToken: undefined, lineUserId: 'guest' });
    const results = await tracedRace('guest-same-key', () => Promise.all([call(bookingHandler, body), call(bookingHandler, body)]));
    expect(results.map(r => r.statusCode)).toEqual([200, 200]);
    expect(results[0].body.guestAccessToken).toBe(results[1].body.guestAccessToken);
    expect((await db.collection('bookings').get()).size).toBe(1);
    expect(JSON.stringify(await snapshotCollections(['idempotency_records'])).includes(results[0].body.guestAccessToken)).toBe(false);
  });

  test('R3: retry remains recoverable after booking start; revoked capability never returns', async () => {
    const body = createBody({ idToken: undefined, lineUserId: 'guest' });
    const first = await call(bookingHandler, body);
    const accessRef = db.collection('guest_booking_access').doc(first.body.booking.id);
    const afterStart = Date.parse(`${DATE}T10:01:00+07:00`);
    // Keep capability valid beyond the fixture's future start for this boundary.
    await accessRef.update({ expiresAt: Timestamp.fromMillis(afterStart + 60_000) });
    const spy = vi.spyOn(Date, 'now').mockReturnValue(afterStart);
    try {
      const retry = await call(bookingHandler, body);
      expect(retry.statusCode).toBe(200);
      expect(retry.body.guestAccessToken).toBe(first.body.guestAccessToken);
    } finally { spy.mockRestore(); }
    await accessRef.update({ revokedAt: Timestamp.now() });
    const revoked = await call(bookingHandler, body);
    expect(revoked.statusCode).toBe(409);
    expect(revoked.body.guestAccessToken).toBeUndefined();
  });

  test('R3: legacy plaintext retry is encrypted on recovery; tampered envelope fails closed', async () => {
    const body = createBody({ idToken: undefined, lineUserId: 'guest' });
    const first = await call(bookingHandler, body);
    const { idempotencyRef } = await import('../api/_lib/firebase-admin.js');
    const ref = idempotencyRef(db, body.idempotencyKey, 'create_coach_addon_v2:guest');
    await ref.update({ response: first.body });
    const retry = await call(bookingHandler, body);
    expect(retry.statusCode).toBe(200);
    expect(retry.body.guestAccessToken).toBe(first.body.guestAccessToken);
    const stored = (await ref.get()).data();
    expect(JSON.stringify(stored).includes(first.body.guestAccessToken)).toBe(false);
    await ref.update({ 'response.guestRetryEnvelope.tag': Buffer.alloc(16).toString('base64url') });
    const failed = await call(bookingHandler, body);
    expect(failed.statusCode).toBe(503);
    expect(failed.body.guestAccessToken).toBeUndefined();
    expect((await db.collection('bookings').get()).size).toBe(1);
  });

  test('R3: pass retry bypasses already-deducted balance validation', async () => {
    await db.collection('customer_packages').doc(ULTRA).update({ remainingMinutes: 60 });
    const body = createBody({ fundingMode: 'ultra_pass', packageId: ULTRA });
    const first = await call(bookingHandler, body);
    expect(first.statusCode).toBe(200);
    const retry = await call(bookingHandler, body);
    expect(retry.statusCode).toBe(200);
    expect(retry.body.booking.id).toBe(first.body.booking.id);
    expect((await db.collection('customer_packages').doc(ULTRA).get()).data().remainingMinutes).toBe(0);
  });

  test('R5: guest failure after all writes are queued rolls back capability, claims and retry record', async () => {
    let queued = false;
    const body = createBody({ idToken: undefined, lineUserId: 'guest' });
    const failed = await withTransactionHook(async (t, callback) => {
      let hasRetryWrite = false;
      const create = t.create.bind(t);
      t.create = (ref, ...args) => { if (ref.parent.id === 'idempotency_records') hasRetryWrite = true; return create(ref, ...args); };
      const result = await callback(t);
      if (hasRetryWrite) { queued = true; throw new Error('INJECTED_AFTER_WRITES'); }
      return result;
    }, () => call(bookingHandler, body));
    expect(queued).toBe(true);
    expect(failed.statusCode).toBe(500);
    for (const name of ['bookings', 'guest_booking_access', 'booking_slots', 'booking_slot_claims', 'coach_slot_claims', 'idempotency_records']) {
      expect((await db.collection(name).get()).size).toBe(0);
    }
    expect((await call(bookingHandler, body)).statusCode).toBe(200);
  });
  test('C1: mark_paid consumes the reservation and confirms all claims, preventing later expiry', async () => {
    const created = await call(bookingHandler, createBody({ fundingMode: 'ultra_pass', packageId: ULTRA }));
    expect(created.statusCode).toBe(200);
    const bookingId = created.body.booking.id;
    const amount = created.body.booking.priceBreakdown.cashDueAmount;
    const paid = await call(accountingHandler, { operation: 'mark_paid', bookingId, amount, paymentMethod: 'cash', paymentNote: 'Counter payment' }, true);
    expect(paid.statusCode).toBe(200);
    expect(paid.body).toEqual({ ok: true });
    const booking = (await db.collection('bookings').doc(bookingId).get()).data();
    expect(booking).toMatchObject({ bookingState: 'confirmed', cashState: 'paid', packageUsageState: 'consumed', cashPaidAmount: amount, price: amount, paymentMethod: 'cash', paymentNote: 'Counter payment' });
    for (const collection of ['booking_slot_claims', 'coach_slot_claims']) {
      const claims = await db.collection(collection).get();
      expect(claims.empty).toBe(false);
      claims.docs.forEach(doc => expect(doc.data()).toMatchObject({ bookingId, status: 'confirmed', expiresAt: null }));
    }
    const { releaseCoachAddonV2Hold } = await import('../api/_lib/coach-addon-v2-store.js');
    await expect(releaseCoachAddonV2Hold(db, bookingId, { nowMs: Date.now() + 3600_000 })).rejects.toThrow('NOT_HELD');
    expect((await db.collection('customer_packages').doc(ULTRA).get()).data().remainingMinutes).toBe(540);
    expect((await db.collection('customer_package_logs').where('action', '==', 'consume_reserved_minutes').get()).size).toBe(1);
    expect((await call(accountingHandler, { operation: 'mark_paid', bookingId, amount, paymentMethod: 'cash' }, true)).statusCode).toBe(409);
  });

  test('C1: mark_paid cannot overwrite the frozen cash amount', async () => {
    const created = await call(bookingHandler, createBody({ fundingMode: 'ultra_pass', packageId: ULTRA }));
    const bookingId = created.body.booking.id;
    const collections = ['bookings', 'booking_slots', 'booking_slot_claims', 'coach_slot_claims', 'customer_packages', 'customer_package_logs'];
    const before = await snapshotCollections(collections);
    const result = await call(accountingHandler, { operation: 'mark_paid', bookingId, amount: 1, paymentMethod: 'cash' }, true);
    expect(result.statusCode).toBe(409);
    expect(await snapshotCollections(collections)).toEqual(before);
  });

  test('C1: a conflicting coach claim rolls back mark_paid without consuming the reservation', async () => {
    const created = await call(bookingHandler, createBody({ fundingMode: 'ultra_pass', packageId: ULTRA }));
    const bookingId = created.body.booking.id;
    await db.collection('coach_slot_claims').doc(coachClaimId('10:00')).update({ bookingId: 'another-booking' });
    const collections = ['bookings', 'booking_slots', 'booking_slot_claims', 'coach_slot_claims', 'customer_packages', 'customer_package_logs'];
    const before = await snapshotCollections(collections);
    const result = await call(accountingHandler, { operation: 'mark_paid', bookingId, amount: created.body.booking.priceBreakdown.cashDueAmount, paymentMethod: 'cash' }, true);
    expect(result.statusCode).toBe(409);
    expect(await snapshotCollections(collections)).toEqual(before);
  });

  test.each([
    { fundingMode: 'cash' },
    { fundingMode: 'coaching_package', packageId: BEGINNER, studentCount: 2 },
  ])('C1 compatibility: mark_paid confirms $fundingMode cash due without repricing', async funding => {
    const created = await call(bookingHandler, createBody(funding));
    const bookingId = created.body.booking.id;
    const before = (await db.collection('bookings').doc(bookingId).get()).data();
    const result = await call(accountingHandler, { operation: 'mark_paid', bookingId, amount: String(before.cashDueAmount), paymentMethod: 'transfer' }, true);
    expect(result.statusCode).toBe(200);
    const after = (await db.collection('bookings').doc(bookingId).get()).data();
    expect(after).toMatchObject({ bookingState: 'confirmed', cashState: 'paid', cashPaidAmount: before.cashDueAmount, price: before.price, coachPayoutAmount: before.coachPayoutAmount, paymentMethod: 'transfer', paymentNote: '' });
    expect(after.priceBreakdown).toEqual({ ...before.priceBreakdown, cashPaidAmount: before.cashDueAmount });
  });

  test('C1: expired mark_paid racing expiry cannot confirm or restore minutes twice', async () => {
    const created = await call(bookingHandler, createBody({ fundingMode: 'ultra_pass', packageId: ULTRA }));
    const bookingId = created.body.booking.id;
    await db.collection('bookings').doc(bookingId).update({ paymentExpiresAt: Timestamp.fromMillis(Date.now() - 1000) });
    const { releaseCoachAddonV2Hold } = await import('../api/_lib/coach-addon-v2-store.js');
    const results = await tracedRace('payment-vs-release', () => Promise.allSettled([
      call(accountingHandler, { operation: 'mark_paid', bookingId, amount: created.body.booking.priceBreakdown.cashDueAmount, paymentMethod: 'transfer' }, true),
      releaseCoachAddonV2Hold(db, bookingId),
    ]));
    expect(results.every(result => result.status === 'fulfilled')).toBe(true);
    const paid = results[0].value;
    expect(paid.statusCode).toBe(409);
    expect((await db.collection('bookings').doc(bookingId).get()).data()).toMatchObject({ bookingState: 'expired', packageUsageState: 'released', paymentStatus: 'rejected' });
    expect((await db.collection('customer_packages').doc(ULTRA).get()).data().remainingMinutes).toBe(600);
    expect((await db.collection('customer_package_logs').where('action', '==', 'release_reserved_minutes').get()).size).toBe(1);
    expect((await db.collection('coach_slot_claims').get()).size).toBe(0);
  });

  test.each(['mark_paid', 'approve_slip'])('R5: %s must establish invariants before the blocked competing action runs', async winner => {
    const created = await call(bookingHandler, createBody({ fundingMode: 'ultra_pass', packageId: ULTRA }));
    const bookingId = created.body.booking.id;
    const loser = winner === 'mark_paid' ? 'approve_slip' : 'mark_paid';
    const pay = operation => call(accountingHandler, { operation, bookingId, amount: 580, paymentMethod: 'cash', withoutSlip: true }, true);
    let release, enter, attempts = 0;
    const gate = new Promise(resolve => { release = resolve; });
    const entered = new Promise(resolve => { enter = resolve; });
    await withTransactionHook(async (t, callback) => {
      if (++attempts === 1) { enter(); await gate; }
      return callback(t);
    }, async () => {
      const competing = pay(loser);
      const result = await drainCompetitor(competing, release, async () => {
        await Promise.race([entered, competing.then(() => { throw new Error('Competitor ended before reaching its gate'); })]);
        expect((await pay(winner)).statusCode).toBe(200);
        // These assertions run BEFORE the other operation can repair anything.
        expect((await db.collection('bookings').doc(bookingId).get()).data()).toMatchObject({ bookingState: 'confirmed', cashState: 'paid', packageUsageState: 'consumed', cashPaidAmount: 580 });
        for (const name of ['booking_slot_claims', 'coach_slot_claims']) {
          const docs = await db.collection(name).get();
          expect(docs.size).toBe(name === 'coach_slot_claims' ? 2 : 1);
          docs.docs.forEach(doc => expect(doc.data()).toMatchObject({ bookingId, status: 'confirmed', expiresAt: null }));
        }
      });
      expect(result.statusCode).toBe(loser === 'mark_paid' ? 409 : 200);
      expect((await db.collection('customer_package_logs').where('action', '==', 'consume_reserved_minutes').get()).size).toBe(1);
      expect((await db.collection('customer_packages').doc(ULTRA).get()).data().remainingMinutes).toBe(540);
    });
  });

  test('H1: guest v2 create persists the real capability and supports protected read and slip submission', async () => {
    const created = await call(bookingHandler, createBody({ idToken: undefined, lineUserId: 'guest' }));
    expect(created.statusCode).toBe(200);
    const bookingId = created.body.booking.id;
    const guestToken = created.body.guestAccessToken;
    const { verifyGuestToken } = await import('../api/_lib/firebase-admin.js');
    expect((await verifyGuestToken(db, bookingId, guestToken, 'booking:read')).ok).toBe(true);
    const access = (await db.collection('guest_booking_access').doc(bookingId).get()).data();
    expect(JSON.stringify(access)).not.toContain(guestToken);
    expect((await db.collection('booking_slot_claims').get()).size).toBe(1);
    expect((await db.collection('coach_slot_claims').get()).size).toBe(2);
    const read = await call(bookingHandler, { action: 'guest_booking', bookingId, guestToken });
    expect(read.statusCode).toBe(200);
    const submitted = await call(slipHandler, { action: 'submit_slip', bookingId, bookingCode: created.body.booking.bookingCode, guestToken, slipUrl: storageUrl('guest'), idempotencyKey: 'guest-slip' });
    expect(submitted.statusCode).toBe(200);
    expect((await db.collection('bookings').doc(bookingId).get()).data().cashState).toBe('pending_review');
  });

  test('H1 rollback: unavailable court creates neither guest capability nor resource claims', async () => {
    await db.collection('available_slots').doc(roomSlotId('10:00')).update({ status: 'closed' });
    const failed = await call(bookingHandler, createBody({ idToken: undefined, lineUserId: 'guest' }));
    expect(failed.statusCode).toBe(409);
    for (const name of ['bookings', 'guest_booking_access', 'booking_slots', 'booking_slot_claims', 'coach_slot_claims']) {
      expect((await db.collection(name).get()).size).toBe(0);
    }
  });

  test('C1/H3 compatibility: legacy booking still supports mark_paid and coach assign/unassign', async () => {
    const created = await call(bookingHandler, {
      action: 'create', date: DATE, startTime: '10:00', durationMinutes: 60,
      idToken: 'stubbed', lineUserId: UID, customerName: 'Legacy', customerPhone: '0810000099',
    });
    expect(created.statusCode).toBe(200);
    const bookingId = created.body.booking.id;
    const paid = await call(accountingHandler, { operation: 'mark_paid', bookingId, amount: 400, paymentMethod: 'cash', paymentNote: 'Legacy override' }, true);
    expect(paid.statusCode).toBe(200);
    expect(paid.body).toEqual({ ok: true });
    expect((await db.collection('bookings').doc(bookingId).get()).data()).toMatchObject({ price: 400, paymentStatus: 'paid', paymentNote: 'Legacy override' });
    for (const coachId of [COACH, '']) {
      expect((await call(accountingHandler, { operation: 'assign_coach', bookingId, coachId }, true)).statusCode).toBe(200);
      expect((await db.collection('bookings').doc(bookingId).get()).data().coachId).toBe(coachId || null);
    }
  });

  test.each(['other-coach', ''])('H3: legacy assign_coach (%s) rejects v2 without modifying booking, claims or accounting', async coachId => {
    const created = await call(bookingHandler, createBody());
    expect(created.statusCode).toBe(200);
    await db.collection('coaches').doc('other-coach').set({ active: true, displayName: 'Other', branchId: 'ladprao1' });
    const collections = ['bookings', 'booking_slots', 'booking_slot_claims', 'coach_slot_claims', 'customer_package_logs', 'finance_expenses', 'audit_logs'];
    const before = await snapshotCollections(collections);
    const result = await call(accountingHandler, { operation: 'assign_coach', bookingId: created.body.booking.id, coachId }, true);
    expect(result.statusCode).toBe(409);
    expect(result.body.code).toBe('COACH_ADDON_V2_ASSIGN_UNSUPPORTED');
    expect(await snapshotCollections(collections)).toEqual(before);
  });

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
