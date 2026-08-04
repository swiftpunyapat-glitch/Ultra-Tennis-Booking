// ════════════════════════════════════════════════════════════════════
// Idempotency and durable rate limiting — review remediation, commit 2
// ════════════════════════════════════════════════════════════════════
//   firebase emulators:exec --only firestore "npx vitest run tests/"
//
// Covers RB-02, RB-04 and RB-09.
//
// The point of these tests is that the idempotency record and the mutation
// it describes share one transaction. Everything below is written against
// that invariant: if a mutation rolls back there must be no record, and if
// a record exists it must already carry its response.
// ════════════════════════════════════════════════════════════════════

import { describe, test, expect, beforeAll, beforeEach } from 'vitest';
import {
  getAdminDb, idempotencyRef, fingerprintOf,
  readIdempotencyInTx, writeIdempotencyInTx,
  checkRateLimit, RATE_LIMITS,
} from '../api/_lib/firebase-admin.js';

let db;
const SCOPE   = 'test_scope';
const COUNTER = 'test_counters/c1';

beforeAll(() => {
  if (!process.env.FIRESTORE_EMULATOR_HOST) {
    throw new Error('Refusing to run: FIRESTORE_EMULATOR_HOST is not set. These tests must never touch production.');
  }
  db = getAdminDb();
});

beforeEach(async () => {
  const stale = await db.collection('idempotency_records').get();
  await Promise.all(stale.docs.map(d => d.ref.delete()));
  const rl = await db.collection('rate_limits').get();
  await Promise.all(rl.docs.map(d => d.ref.delete()));
  await db.doc(COUNTER).delete().catch(() => {});
});

// A minimal mutation that mirrors the real shape: read the record, do the
// work, write the record — all in one transaction.
async function runGuardedMutation(key, fingerprint, { fail = false, response = { ok: true, n: 1 } } = {}) {
  const ref = idempotencyRef(db, key, SCOPE);
  let replayed = null;
  await db.runTransaction(async (t) => {
    const idem = await readIdempotencyInTx(t, ref, fingerprint);
    if (idem.state === 'conflict') throw new Error('IDEMPOTENCY_CONFLICT');
    if (idem.state === 'replay') { replayed = idem.response; return; }

    const counter = await t.get(db.doc(COUNTER));
    const n = (counter.exists ? counter.data().n : 0) + 1;

    if (fail) throw new Error('MUTATION_FAILED');

    t.set(db.doc(COUNTER), { n });
    writeIdempotencyInTx(t, ref, { scope: SCOPE, fingerprint, response });
  });
  return replayed;
}

const counterValue = async () => {
  const s = await db.doc(COUNTER).get();
  return s.exists ? s.data().n : 0;
};

describe('RB-02 first request', () => {
  test('commits the mutation and the response snapshot together', async () => {
    const fp = fingerprintOf({ a: 1 });
    const replay = await runGuardedMutation('k1', fp);

    expect(replay).toBeNull();
    expect(await counterValue()).toBe(1);

    const rec = await idempotencyRef(db, 'k1', SCOPE).get();
    expect(rec.exists).toBe(true);
    expect(rec.data().response).toEqual({ ok: true, n: 1 });
    expect(rec.data().fingerprint).toBe(fp);
  });
});

describe('RB-09 retry never returns an empty response', () => {
  test('same key and fingerprint replays the original response', async () => {
    const fp = fingerprintOf({ a: 1 });
    await runGuardedMutation('k1', fp);
    const replay = await runGuardedMutation('k1', fp);

    expect(replay).toEqual({ ok: true, n: 1 });
    expect(await counterValue()).toBe(1);   // mutation did NOT run twice
  });

  test('a record can never exist without its response', async () => {
    await runGuardedMutation('k1', fingerprintOf({ a: 1 }));
    const all = await db.collection('idempotency_records').get();
    expect(all.empty).toBe(false);
    for (const d of all.docs) {
      expect(d.data().response).not.toBeNull();
      expect(d.data().response).toBeDefined();
    }
  });
});

describe('Concurrent retry runs the mutation exactly once', () => {
  test('five parallel attempts with one key produce one mutation', async () => {
    const fp = fingerprintOf({ a: 1 });
    const results = await Promise.allSettled(
      Array.from({ length: 5 }, () => runGuardedMutation('k-concurrent', fp)),
    );

    const ok = results.filter(r => r.status === 'fulfilled');
    expect(ok.length).toBeGreaterThan(0);
    expect(await counterValue()).toBe(1);

    const recs = await db.collection('idempotency_records').get();
    expect(recs.size).toBe(1);
    expect(recs.docs[0].data().response).toEqual({ ok: true, n: 1 });
  });
});

describe('Fingerprint mismatch', () => {
  test('same key with a different request is a conflict, not a silent replay', async () => {
    await runGuardedMutation('k1', fingerprintOf({ a: 1 }));
    await expect(runGuardedMutation('k1', fingerprintOf({ a: 999 })))
      .rejects.toThrow('IDEMPOTENCY_CONFLICT');
    expect(await counterValue()).toBe(1);
  });

  test('fingerprint is stable regardless of key order', () => {
    expect(fingerprintOf({ a: 1, b: 2 })).toBe(fingerprintOf({ b: 2, a: 1 }));
    expect(fingerprintOf({ a: 1 })).not.toBe(fingerprintOf({ a: 2 }));
  });
});

describe('Failed transaction leaves no record', () => {
  test('a rolled-back mutation writes neither the effect nor the record', async () => {
    await expect(runGuardedMutation('k-fail', fingerprintOf({ a: 1 }), { fail: true }))
      .rejects.toThrow('MUTATION_FAILED');

    expect(await counterValue()).toBe(0);
    const rec = await idempotencyRef(db, 'k-fail', SCOPE).get();
    expect(rec.exists).toBe(false);
  });

  test('the key is reusable after a failure', async () => {
    await expect(runGuardedMutation('k-retry', fingerprintOf({ a: 1 }), { fail: true })).rejects.toThrow();
    const replay = await runGuardedMutation('k-retry', fingerprintOf({ a: 1 }));
    expect(replay).toBeNull();
    expect(await counterValue()).toBe(1);
  });
});

describe('RB-04 TTL fields', () => {
  test('idempotency records carry expiresAt = createdAt + 90 days', async () => {
    await runGuardedMutation('k1', fingerprintOf({ a: 1 }));
    const d = (await idempotencyRef(db, 'k1', SCOPE).get()).data();
    const created = d.createdAt.toMillis();
    const expires = d.expiresAt.toMillis();
    expect(expires - created).toBe(90 * 24 * 60 * 60 * 1000);
  });

  test('rate-limit documents carry expiresAt past the end of the window', async () => {
    await checkRateLimit(db, { bucket: 'guestRead', key: 'k|1.2.3.4', ...RATE_LIMITS.guestRead });
    const all = await db.collection('rate_limits').get();
    expect(all.size).toBe(1);

    const d = all.docs[0].data();
    const windowEnd = d.windowStart.toMillis() + RATE_LIMITS.guestRead.windowMs;
    expect(d.expiresAt.toMillis()).toBe(windowEnd + 24 * 60 * 60 * 1000);
  });

  test('repeated attempts reuse one document rather than creating one per attempt', async () => {
    for (let i = 0; i < 6; i++) {
      await checkRateLimit(db, { bucket: 'guestMutation', key: 'same|1.2.3.4', ...RATE_LIMITS.guestMutation });
    }
    const all = await db.collection('rate_limits').get();
    expect(all.size).toBe(1);
    expect(all.docs[0].data().count).toBe(6);
  });

  test('a blocked document expires after the block, not before', async () => {
    for (let i = 0; i < 6; i++) {
      await checkRateLimit(db, { bucket: 'guestInvalid', key: '1.2.3.4', ...RATE_LIMITS.guestInvalid });
    }
    const all = await db.collection('rate_limits').get();
    const d = all.docs[0].data();
    expect(d.blockedUntil).toBeTruthy();
    expect(d.expiresAt.toMillis()).toBeGreaterThan(d.blockedUntil.toMillis());
  });
});

describe('Rate limiter behaviour', () => {
  test('allows up to the limit then denies with Retry-After', async () => {
    const opts = { bucket: 'guestMutation', key: 'rl|1.1.1.1', ...RATE_LIMITS.guestMutation };
    for (let i = 0; i < RATE_LIMITS.guestMutation.limit; i++) {
      expect((await checkRateLimit(db, opts)).allowed).toBe(true);
    }
    const denied = await checkRateLimit(db, opts);
    expect(denied.allowed).toBe(false);
    expect(denied.retryAfterSec).toBeGreaterThan(0);
  });

  test('invalid-token bucket locks out for the configured block period', async () => {
    const opts = { bucket: 'guestInvalid', key: '9.9.9.9', ...RATE_LIMITS.guestInvalid };
    for (let i = 0; i < RATE_LIMITS.guestInvalid.limit; i++) await checkRateLimit(db, opts);
    const denied = await checkRateLimit(db, opts);
    expect(denied.allowed).toBe(false);
    // 60-minute block, allowing a little slack for clock/latency.
    expect(denied.retryAfterSec).toBeGreaterThan(3500);
  });
});
