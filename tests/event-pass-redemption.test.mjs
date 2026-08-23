import { beforeAll, beforeEach, describe, expect, test, vi } from 'vitest';

const UID = 'U_EVENT_PASS_TEST';
const CAMPAIGN = 'monstr-auto-test';
const mockState = vi.hoisted(() => ({ db: null, attempts: new Map() }));

vi.mock('../api/_lib/firebase-admin.js', () => ({
  getAdminDb: () => mockState.db,
  getAdminAuth: () => ({ verifyIdToken: async token => ({ uid: String(token) }) }),
  writeAuditLog: async (db, payload) => db.collection('audit_logs').add(payload),
  prepareGuestAccess: vi.fn(), verifyGuestToken: vi.fn(), revokeGuestAccess: vi.fn(),
  GUEST_ACCESS_COLLECTION: 'guest_booking_access', GUEST_BOOKING_ID_MAX_LENGTH: 200, GUEST_TOKEN_MAX_LENGTH: 200,
  checkRateLimit: vi.fn(async (_db, input) => {
    const key = `${input.bucket}|${input.key}`;
    const count = (mockState.attempts.get(key) || 0) + 1;
    mockState.attempts.set(key, count);
    return count > input.limit ? { allowed: false, retryAfterSec: 600 } : { allowed: true };
  }),
  readRateLimitGate: vi.fn(async () => ({ allowed: true })),
  RATE_LIMITS: {
    guestInvalid: { limit: 5 }, guestRead: { limit: 30 }, guestMutation: { limit: 5 },
    eventPassRedeem: { limit: 10, windowMs: 600_000, blockMs: 600_000 },
  },
  clientIp: req => String(req.headers?.['x-forwarded-for'] || 'unknown'),
  idempotencyRef: vi.fn(), fingerprintOf: vi.fn(), readIdempotencyInTx: vi.fn(),
  writeIdempotencyInTx: vi.fn(), isValidIdempotencyKey: vi.fn(() => true),
}));

vi.mock('../api/_lib/notify.js', () => ({
  sendAndLog: vi.fn(async () => ({ ok: true })),
  loadActiveAdmins: vi.fn(async () => []),
  loadNotificationFlags: vi.fn(async () => ({})),
}));

const clone = value => {
  if (Array.isArray(value)) return value.map(clone);
  if (value && typeof value === 'object') {
    if (value.constructor !== Object) return value;
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, clone(item)]));
  }
  return value;
};

class MemoryRef {
  constructor(db, collection, id) { this.db = db; this.collectionName = collection; this.id = id; }
  async get() { return this.db.snapshot(this); }
  async set(value, options = {}) {
    const collection = this.db.bucket(this.collectionName);
    collection.set(this.id, options.merge ? { ...(collection.get(this.id) || {}), ...clone(value) } : clone(value));
  }
  async update(value) {
    const collection = this.db.bucket(this.collectionName);
    if (!collection.has(this.id)) throw new Error('NOT_FOUND');
    collection.set(this.id, { ...collection.get(this.id), ...clone(value) });
  }
  async delete() { this.db.bucket(this.collectionName).delete(this.id); }
}

class MemoryDb {
  constructor() { this.reset(); }
  reset() { this.data = new Map(); this.sequence = 0; this.transactionTail = Promise.resolve(); }
  bucket(name) { if (!this.data.has(name)) this.data.set(name, new Map()); return this.data.get(name); }
  snapshot(ref) {
    const value = this.bucket(ref.collectionName).get(ref.id);
    return { id: ref.id, ref, exists: value !== undefined, data: () => clone(value) };
  }
  collection(name) {
    return {
      doc: id => new MemoryRef(this, name, id || `auto_${++this.sequence}`),
      add: async value => { const ref = new MemoryRef(this, name, `auto_${++this.sequence}`); await ref.set(value); return ref; },
      get: async () => {
        const docs = [...this.bucket(name).keys()].map(id => this.snapshot(new MemoryRef(this, name, id)));
        return { docs, size: docs.length };
      },
    };
  }
  async runTransaction(callback) {
    const previous = this.transactionTail;
    let release;
    this.transactionTail = new Promise(resolve => { release = resolve; });
    await previous;
    const writes = [];
    const transaction = {
      get: ref => ref.get(),
      create: (ref, value) => writes.push({ type: 'create', ref, value: clone(value) }),
      update: (ref, value) => writes.push({ type: 'update', ref, value: clone(value) }),
    };
    try {
      const result = await callback(transaction);
      for (const write of writes) {
        const collection = this.bucket(write.ref.collectionName);
        if (write.type === 'create') {
          if (collection.has(write.ref.id)) throw new Error('ALREADY_EXISTS');
          collection.set(write.ref.id, write.value);
        } else {
          if (!collection.has(write.ref.id)) throw new Error('NOT_FOUND');
          collection.set(write.ref.id, { ...collection.get(write.ref.id), ...write.value });
        }
      }
      return result;
    } finally { release(); }
  }
}

let handler;

beforeAll(async () => {
  mockState.db = new MemoryDb();
  handler = (await import('../api/booking.js')).default;
});

beforeEach(() => {
  mockState.db.reset();
  mockState.attempts.clear();
});

function request(body, ip = '203.0.113.44') {
  return { method: 'POST', body, headers: { 'x-forwarded-for': ip }, socket: {} };
}

function response() {
  const value = { statusCode: 200, body: null, headers: {} };
  value.status = code => { value.statusCode = code; return value; };
  value.json = body => { value.body = body; return value; };
  value.setHeader = (key, item) => { value.headers[key] = item; };
  return value;
}

async function call(code, uid = UID, ip) {
  const res = response();
  await handler(request({ action: 'event_pass_redeem', code, idToken: uid, lineDisplayName: 'Event Tester' }, ip), res);
  return res;
}

async function seed(code, approvalMode) {
  await mockState.db.collection('registered_users').doc(UID).set({
    name: 'Event Tester', phone: '0812345678', lineDisplayName: 'Event Tester',
  });
  await mockState.db.collection('voucher_campaigns').doc(CAMPAIGN).set({
    campaignId: CAMPAIGN, name: 'MONSTR Test Event', voucherType: 'event_pass', active: true,
    validFrom: { toMillis: () => Date.now() - 60_000 },
    expiresAt: { toMillis: () => Date.now() + 7 * 24 * 3600_000 },
    allowedDays: [1, 2, 3, 4, 5], excludeHolidays: true,
    branchId: 'ladprao1', resourceId: 'room1',
    ...(approvalMode ? { eventPassApprovalMode: approvalMode } : {}),
  });
  await mockState.db.collection('vouchers').doc(code).set({
    campaignId: CAMPAIGN, active: true, state: 'available', usedCount: 0, maxUses: 1,
  });
}

describe('Event Pass auto approval', () => {
  test('issues one active pass immediately and treats a repeated redeem as an idempotent replay', async () => {
    await seed('MSTR-AUTO1');
    const first = await call('MSTR-AUTO1');
    expect(first.statusCode).toBe(200);
    expect(first.body).toMatchObject({ ok: true, status: 'approved', autoApproved: true, replayed: false });
    const replay = await call('MSTR-AUTO1');
    expect(replay.statusCode).toBe(200);
    expect(replay.body).toMatchObject({ ok: true, status: 'approved', autoApproved: true, replayed: true });
    expect(replay.body.packageId).toBe(first.body.packageId);

    const [packages, requests, voucher] = await Promise.all([
      mockState.db.collection('customer_packages').get(),
      mockState.db.collection('event_pass_requests').get(),
      mockState.db.collection('vouchers').doc('MSTR-AUTO1').get(),
    ]);
    expect(packages.size).toBe(1);
    expect(requests.size).toBe(1);
    expect(packages.docs[0].data()).toMatchObject({
      lineUserId: UID, packageType: 'monstr_event_pass', remainingMinutes: 60,
      status: 'active', source: 'event_code_auto_approved',
    });
    expect(requests.docs[0].data()).toMatchObject({ status: 'approved', reviewedBy: 'SYSTEM_AUTO' });
    expect(voucher.data()).toMatchObject({ state: 'redeemed', usedCount: 1, issuedTo: UID, redeemedPackageId: first.body.packageId });
  });

  test('two simultaneous redeems converge on the same pass', async () => {
    await seed('MSTR-RACE1');
    const [a, b] = await Promise.all([call('MSTR-RACE1'), call('MSTR-RACE1')]);
    expect([a.body.replayed, b.body.replayed].sort()).toEqual([false, true]);
    expect(a.body.packageId).toBe(b.body.packageId);
    expect((await mockState.db.collection('customer_packages').get()).size).toBe(1);
    expect((await mockState.db.collection('event_pass_requests').get()).size).toBe(1);
  });

  test('keeps the existing pending flow for an explicit manual campaign', async () => {
    await seed('MSTR-MANUAL1', 'manual');
    const result = await call('MSTR-MANUAL1');
    expect(result.statusCode).toBe(200);
    expect(result.body).toMatchObject({ ok: true, status: 'pending', autoApproved: false });
    expect((await mockState.db.collection('customer_packages').get()).size).toBe(0);
    expect((await mockState.db.collection('event_pass_requests').get()).docs[0].data()).toMatchObject({ status: 'pending' });
    expect((await mockState.db.collection('vouchers').doc('MSTR-MANUAL1').get()).data()).toMatchObject({ state: 'pending_approval', issuedTo: UID });
  });

  test('rate limits repeated code guesses before they can be brute-forced', async () => {
    for (let attempt = 1; attempt <= 10; attempt++) expect((await call(`MSTR-GUESS${attempt}`)).statusCode).toBe(404);
    const blocked = await call('MSTR-GUESS11');
    expect(blocked.statusCode).toBe(429);
    expect(blocked.body).toMatchObject({ ok: false, code: 'RATE_LIMIT' });
    expect(Number(blocked.headers['Retry-After'])).toBeGreaterThan(0);
  });
});
