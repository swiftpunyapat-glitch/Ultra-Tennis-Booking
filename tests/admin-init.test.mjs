// ════════════════════════════════════════════════════════════════════
// Admin SDK initialisation — emulator addressability (SR-01)
// ════════════════════════════════════════════════════════════════════
// Found during the second review test gate.
//
// getAdminDb() required FIREBASE_SERVICE_ACCOUNT and initialised with
// cert(sa) unconditionally. Against the emulator there is no service
// account, so every server-side test failed at import time with
// "Failed to parse private key: Invalid PEM formatted message" and all 32
// emulator tests were skipped.
//
// That made the emulator — and therefore the whole test gate this hotfix
// is gated on — impossible to run. This test pins the behaviour so the
// affordance cannot be removed again.
//
// Production is unaffected: FIRESTORE_EMULATOR_HOST is never set there, so
// the credential path is taken exactly as before. If it somehow were set,
// the SDK would try to reach a local emulator that does not exist and fail
// loudly — it cannot silently redirect production traffic.
// ════════════════════════════════════════════════════════════════════

import { describe, test, expect, beforeAll } from 'vitest';
import { getAdminDb } from '../api/_lib/firebase-admin.js';

beforeAll(() => {
  if (!process.env.FIRESTORE_EMULATOR_HOST) {
    throw new Error('Refusing to run: FIRESTORE_EMULATOR_HOST is not set. These tests must never touch production.');
  }
});

describe('SR-01 getAdminDb is usable against the emulator', () => {
  test('initialises without a service account when the emulator host is set', () => {
    expect(() => getAdminDb()).not.toThrow();
  });

  test('the returned instance actually talks to the emulator', async () => {
    const db = getAdminDb();
    const ref = db.collection('_init_probe').doc('p1');
    await ref.set({ ok: true });
    const snap = await ref.get();
    expect(snap.exists).toBe(true);
    expect(snap.data()).toEqual({ ok: true });
    await ref.delete();
  });

  test('the emulator host is a loopback address, never a remote one', () => {
    const host = process.env.FIRESTORE_EMULATOR_HOST;
    expect(host).toMatch(/^(127\.0\.0\.1|localhost|\[::1\]):\d+$/);
  });
});
