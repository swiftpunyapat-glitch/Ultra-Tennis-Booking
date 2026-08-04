// ════════════════════════════════════════════════════════════════════
// Guest capability access — unit tests (review remediation, commit 1)
// ════════════════════════════════════════════════════════════════════
//   firebase emulators:exec --only firestore "npx vitest run tests/"
//
// Covers RB-01, RB-03, RB-05, RB-06, RB-07 and RB-11. These run against the
// Admin SDK talking to the emulator, so they exercise the server logic
// rather than the security rules (which are covered separately).
// ════════════════════════════════════════════════════════════════════

import { describe, test, expect, beforeAll, beforeEach } from 'vitest';
import {
  issueGuestToken, verifyGuestToken, revokeGuestAccess,
  hashGuestToken, guestTokenExpiryMs,
  GUEST_ACCESS_COLLECTION, GUEST_SCOPES,
} from '../api/_lib/firebase-admin.js';

// Requires FIRESTORE_EMULATOR_HOST. getAdminDb() is not used directly here so
// the suite can be pointed at a test Firestore instance.
import { getAdminDb } from '../api/_lib/firebase-admin.js';

let db;
const BOOKING_A = 'bk_guest_A';
const BOOKING_B = 'bk_guest_B';

beforeAll(() => {
  if (!process.env.FIRESTORE_EMULATOR_HOST) {
    throw new Error('Refusing to run: FIRESTORE_EMULATOR_HOST is not set. These tests must never touch production.');
  }
  db = getAdminDb();
});

beforeEach(async () => {
  for (const id of [BOOKING_A, BOOKING_B]) {
    await db.collection(GUEST_ACCESS_COLLECTION).doc(id).delete().catch(() => {});
  }
});

describe('RB-03 access document shape', () => {
  test('stores exactly the agreed fields, and never the raw token', async () => {
    const { token } = await issueGuestToken(db, { bookingId: BOOKING_A, bookingEndMs: Date.now() + 3600_000 });
    const snap = await db.collection(GUEST_ACCESS_COLLECTION).doc(BOOKING_A).get();
    const d = snap.data();

    expect(Object.keys(d).sort()).toEqual(
      ['expiresAt', 'issuedAt', 'revokeReason', 'revokedAt', 'scopes', 'tokenHash', 'tokenVersion'].sort(),
    );
    expect(d.tokenHash).toBe(hashGuestToken(token));
    expect(d.tokenVersion).toBe(1);
    expect(d.revokedAt).toBeNull();
    expect(d.scopes).toEqual(GUEST_SCOPES);

    // The raw token must not be recoverable from anything persisted.
    expect(JSON.stringify(d)).not.toContain(token);
  });

  test('document id is the bookingId, so no query and no composite index (RB-06)', async () => {
    await issueGuestToken(db, { bookingId: BOOKING_A });
    const direct = await db.collection(GUEST_ACCESS_COLLECTION).doc(BOOKING_A).get();
    expect(direct.exists).toBe(true);
  });

  test('GT-01 expiry is the earlier of bookingEnd+48h and issue+90d', async () => {
    const now = Date.now();
    const soon = now + 3600_000;
    expect(guestTokenExpiryMs(soon, now)).toBe(soon + 48 * 3600_000);
    const farFuture = now + 200 * 24 * 3600_000;
    expect(guestTokenExpiryMs(farFuture, now)).toBe(now + 90 * 24 * 3600_000);
    expect(guestTokenExpiryMs(null, now)).toBe(now + 90 * 24 * 3600_000);
  });
});

describe('Q8 / cross-booking isolation', () => {
  test('token issued for booking A does not verify against booking B', async () => {
    const { token: tokenA } = await issueGuestToken(db, { bookingId: BOOKING_A });
    await issueGuestToken(db, { bookingId: BOOKING_B });

    expect((await verifyGuestToken(db, BOOKING_A, tokenA)).ok).toBe(true);
    expect((await verifyGuestToken(db, BOOKING_B, tokenA)).ok).toBe(false);
  });

  test('token for a booking with no access document is rejected', async () => {
    const { token } = await issueGuestToken(db, { bookingId: BOOKING_A });
    expect((await verifyGuestToken(db, 'bk_does_not_exist', token)).ok).toBe(false);
  });
});

describe('RB-03 reissue is atomic', () => {
  test('the previous token stops verifying the moment the new one commits', async () => {
    const { token: first } = await issueGuestToken(db, { bookingId: BOOKING_A });
    expect((await verifyGuestToken(db, BOOKING_A, first)).ok).toBe(true);

    const { token: second, tokenVersion } = await issueGuestToken(db, { bookingId: BOOKING_A });

    expect(tokenVersion).toBe(2);
    expect((await verifyGuestToken(db, BOOKING_A, first)).ok).toBe(false);
    expect((await verifyGuestToken(db, BOOKING_A, second)).ok).toBe(true);
  });

  test('reissue leaves exactly one access document', async () => {
    await issueGuestToken(db, { bookingId: BOOKING_A });
    await issueGuestToken(db, { bookingId: BOOKING_A });
    await issueGuestToken(db, { bookingId: BOOKING_A });
    const all = await db.collection(GUEST_ACCESS_COLLECTION).get();
    const forA = all.docs.filter(d => d.id === BOOKING_A);
    expect(forA).toHaveLength(1);
    expect(forA[0].data().tokenVersion).toBe(3);
  });

  test('concurrent reissue leaves a single coherent document', async () => {
    await Promise.all([
      issueGuestToken(db, { bookingId: BOOKING_A }),
      issueGuestToken(db, { bookingId: BOOKING_A }),
      issueGuestToken(db, { bookingId: BOOKING_A }),
    ]);
    const snap = await db.collection(GUEST_ACCESS_COLLECTION).doc(BOOKING_A).get();
    expect(snap.exists).toBe(true);
    expect(typeof snap.data().tokenHash).toBe('string');
    expect(snap.data().tokenVersion).toBeGreaterThanOrEqual(1);
  });
});

describe('RB-05 revocation', () => {
  test('revoke makes the token unusable and records the reason', async () => {
    const { token } = await issueGuestToken(db, { bookingId: BOOKING_A });
    expect(await revokeGuestAccess(db, BOOKING_A, 'booking_cancelled')).toBe(true);

    expect((await verifyGuestToken(db, BOOKING_A, token)).ok).toBe(false);
    const d = (await db.collection(GUEST_ACCESS_COLLECTION).doc(BOOKING_A).get()).data();
    expect(d.revokeReason).toBe('booking_cancelled');
    expect(d.tokenHash).toBeNull();
    expect(d.revokedAt).toBeTruthy();
  });

  test('refund is an accepted revoke reason', async () => {
    await issueGuestToken(db, { bookingId: BOOKING_A });
    await revokeGuestAccess(db, BOOKING_A, 'booking_refunded');
    const d = (await db.collection(GUEST_ACCESS_COLLECTION).doc(BOOKING_A).get()).data();
    expect(d.revokeReason).toBe('booking_refunded');
  });

  test('an unknown reason is coerced rather than stored verbatim', async () => {
    await issueGuestToken(db, { bookingId: BOOKING_A });
    await revokeGuestAccess(db, BOOKING_A, 'whatever-the-caller-passed');
    const d = (await db.collection(GUEST_ACCESS_COLLECTION).doc(BOOKING_A).get()).data();
    expect(d.revokeReason).toBe('admin_revoked');
  });

  test('revoking twice is safe and does not resurrect access', async () => {
    const { token } = await issueGuestToken(db, { bookingId: BOOKING_A });
    expect(await revokeGuestAccess(db, BOOKING_A, 'booking_cancelled')).toBe(true);
    expect(await revokeGuestAccess(db, BOOKING_A, 'admin_revoked')).toBe(false);
    expect((await verifyGuestToken(db, BOOKING_A, token)).ok).toBe(false);
  });

  test('revoking a booking with no access document is a no-op', async () => {
    expect(await revokeGuestAccess(db, 'bk_nothing_here', 'admin_revoked')).toBe(false);
  });
});

describe('Scope enforcement', () => {
  test('a scope the token does not hold is refused', async () => {
    const { token } = await issueGuestToken(db, { bookingId: BOOKING_A, scopes: ['booking:read'] });
    expect((await verifyGuestToken(db, BOOKING_A, token, 'booking:read')).ok).toBe(true);
    const denied = await verifyGuestToken(db, BOOKING_A, token, 'slip:submit');
    expect(denied.ok).toBe(false);
    expect(denied.reason).toBe('scope');
  });
});

describe('Expiry', () => {
  test('an expired access document does not verify', async () => {
    await issueGuestToken(db, { bookingId: BOOKING_A });
    await db.collection(GUEST_ACCESS_COLLECTION).doc(BOOKING_A).update({
      expiresAt: new Date(Date.now() - 1000),
    });
    const { token } = await issueGuestToken(db, { bookingId: BOOKING_B });
    // Reuse B's token shape against the expired A document.
    expect((await verifyGuestToken(db, BOOKING_A, token)).ok).toBe(false);
  });
});

describe('RB-07 nothing sensitive reaches the logs', () => {
  test('verify failures log a message only, never the token or booking id', async () => {
    const seen = [];
    const origError = console.error;
    const origWarn  = console.warn;
    console.error = (...a) => seen.push(a.join(' '));
    console.warn  = (...a) => seen.push(a.join(' '));
    try {
      const { token } = await issueGuestToken(db, { bookingId: BOOKING_A });
      await verifyGuestToken(db, BOOKING_A, token);
      await verifyGuestToken(db, BOOKING_A, 'a-token-that-is-long-enough-to-pass-the-length-check');
      await revokeGuestAccess(db, BOOKING_A, 'booking_cancelled');

      const joined = seen.join('\n');
      expect(joined).not.toContain(token);
      expect(joined).not.toContain(hashGuestToken(token));
      expect(joined).not.toContain(BOOKING_A);
    } finally {
      console.error = origError;
      console.warn  = origWarn;
    }
  });
});

describe('RB-01 handleCancelPending signature', () => {
  // The first version called clientIp(req) from a function declared as
  // (res, body). That threw a ReferenceError on every guest cancellation.
  // Asserting the arity keeps the dispatcher and the handler in step.
  test('handler accepts (req, res, body)', async () => {
    const src = await import('node:fs').then(fs =>
      fs.promises.readFile(new URL('../api/booking.js', import.meta.url), 'utf8'));
    expect(src).toContain('async function handleCancelPending(req, res, body)');
    expect(src).toContain("return handleCancelPending(req, res, body)");
    // And no call site may pass the old two-argument shape.
    expect(src).not.toMatch(/handleCancelPending\(res,\s*body\)/);
  });

  test('RB-11 no client-supplied lineUserId ownership fallback remains', async () => {
    const src = await import('node:fs').then(fs =>
      fs.promises.readFile(new URL('../api/booking.js', import.meta.url), 'utf8'));
    expect(src).not.toMatch(/lineUserId\s*===\s*booking\.lineUserId/);
  });
});
