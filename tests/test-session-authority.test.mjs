import { createHmac } from 'node:crypto';
import { beforeEach, describe, expect, test } from 'vitest';
import {
  assertNoClientTestFlags, createTestSessionToken, newTestSessionId, readTestSessionToken,
  resolveTestSession, slotsWithinTestSession, testSessionTtlHours, testStamp, verifyTestSessionToken,
} from '../api/_lib/test-session.js';
import { belongsToTestSession, excludeTestBookings, isTestBooking } from '../test-booking.js';

const NOW = Date.parse('2026-09-21T10:00:00+07:00');
const LATER = NOW + 3_600_000;

beforeEach(() => { process.env.ADMIN_SESSION_SECRET = 'test-session-unit-secret'; });

const sessionDoc = (over = {}) => ({
  status: 'active', branchId: 'ladprao1', resourceId: 'room1',
  reservedSlotIds: ['room1_2026-09-21_20:00'], createdBy: 'Art',
  expiresAtMs: LATER, ...over,
});

// Minimal Firestore stand-in: one collection, one document.
const fakeDb = (id, data) => ({
  collection: name => {
    expect(name).toBe('test_sessions');
    return { doc: docId => ({ get: async () => ({ exists: docId === id && data !== null, data: () => data }) }) };
  },
});

const reqWith = (token, where = 'header') => where === 'header'
  ? { headers: { 'x-test-session': token }, body: {} }
  : { headers: {}, body: { testSessionToken: token } };

describe('token signing', () => {
  test('a freshly minted token verifies and carries its session id', () => {
    const id = newTestSessionId();
    expect(verifyTestSessionToken(createTestSessionToken(id, LATER), NOW)).toEqual({ testSessionId: id, exp: LATER });
  });

  test('session ids are unique per call', () => {
    expect(new Set(Array.from({ length: 50 }, newTestSessionId)).size).toBe(50);
  });

  test.each([
    ['an empty string', ''],
    ['a non-string', 12345],
    ['no signature at all', 'eyJ0c2lkIjoiYWJjIn0'],
    ['a truncated signature', 'eyJ0c2lkIjoiYWJjIn0.abc'],
    ['a payload that is not JSON', `${Buffer.from('nope').toString('base64url')}.${'0'.repeat(64)}`],
  ])('rejects %s', (_label, token) => {
    expect(verifyTestSessionToken(token, NOW)).toBeNull();
  });

  test('rejects a tampered payload that keeps the original signature', () => {
    const token = createTestSessionToken('ts_original', LATER);
    const forged = `${Buffer.from(JSON.stringify({ tsid: 'ts_other', exp: LATER })).toString('base64url')}.${token.split('.')[1]}`;
    expect(verifyTestSessionToken(forged, NOW)).toBeNull();
  });

  test('rejects a token signed with a different secret', () => {
    const token = createTestSessionToken('ts_a', LATER);
    process.env.ADMIN_SESSION_SECRET = 'a-different-secret';
    expect(verifyTestSessionToken(token, NOW)).toBeNull();
  });

  test('rejects an expired token', () => {
    expect(verifyTestSessionToken(createTestSessionToken('ts_a', NOW - 1), NOW)).toBeNull();
  });

  test('an admin session cookie cannot be replayed as a test session token', () => {
    // Same secret, same "<payload>.<hmac>" shape — only the scope prefix in the
    // signed string keeps the two apart.
    const adminPayload = Buffer.from(JSON.stringify({ name: 'Art', role: 'owner', exp: LATER })).toString('base64url');
    const adminSig = createHmac('sha256', process.env.ADMIN_SESSION_SECRET).update(adminPayload).digest('hex');
    expect(verifyTestSessionToken(`${adminPayload}.${adminSig}`, NOW)).toBeNull();
  });

  test('ttl is clamped to a sane window', () => {
    expect(testSessionTtlHours(undefined)).toBe(4);
    expect(testSessionTtlHours(0)).toBe(4);
    expect(testSessionTtlHours(-5)).toBe(4);
    expect(testSessionTtlHours(999)).toBe(4);
    expect(testSessionTtlHours(8)).toBe(8);
  });
});

describe('a client cannot declare itself a test', () => {
  test.each(['isTest', 'testSessionId'])('%s in a request body is rejected outright', field => {
    expect(assertNoClientTestFlags({ [field]: 'anything' })).toContain(field);
  });

  test('isTest:false is rejected too — the field simply does not belong to callers', () => {
    expect(assertNoClientTestFlags({ isTest: false })).not.toBeNull();
  });

  test.each([{}, { date: '2026-09-21' }, null, undefined, 'not-an-object'])('%o passes through', body => {
    expect(assertNoClientTestFlags(body)).toBeNull();
  });

  test('the token is read from a header or a body field, nowhere else', () => {
    expect(readTestSessionToken(reqWith('tok'))).toBe('tok');
    expect(readTestSessionToken(reqWith('tok', 'body'))).toBe('tok');
    expect(readTestSessionToken({ headers: {}, body: { isTest: true } })).toBeNull();
    expect(readTestSessionToken({ headers: {}, body: {} })).toBeNull();
  });
});

describe('resolving a token against the live session document', () => {
  const id = 'ts_live';
  const resolve = (data, token = createTestSessionToken(id, LATER)) =>
    resolveTestSession(reqWith(token), fakeDb(id, data), NOW);

  test('no token means an ordinary live request, not a failure', async () => {
    expect(await resolveTestSession({ headers: {}, body: {} }, fakeDb(id, sessionDoc()), NOW)).toBeNull();
  });

  test('an active session resolves with its reserved slots', async () => {
    const result = await resolve(sessionDoc());
    expect(result.ok).toBe(true);
    expect(result.session).toMatchObject({ testSessionId: id, branchId: 'ladprao1', reservedSlotIds: ['room1_2026-09-21_20:00'] });
  });

  test.each([
    ['ended', 'session_ended'],
    ['purged', 'session_purged'],
  ])('a %s session is refused so ending one takes effect at once', async (status, reason) => {
    expect(await resolve(sessionDoc({ status }))).toEqual({ ok: false, reason });
  });

  test('a deleted session document is refused', async () => {
    expect(await resolve(null)).toEqual({ ok: false, reason: 'session_not_found' });
  });

  test('a session past its own expiry is refused even with an unexpired token', async () => {
    expect(await resolve(sessionDoc({ expiresAtMs: NOW - 1 }))).toEqual({ ok: false, reason: 'session_expired' });
  });

  test('a forged token never reaches the database', async () => {
    const db = { collection: () => { throw new Error('must not read'); } };
    expect(await resolveTestSession(reqWith('forged.token'), db, NOW)).toEqual({ ok: false, reason: 'invalid_token' });
  });
});

describe('a test session may only book what it reserved', () => {
  const session = { testSessionId: 'ts_1', reservedSlotIds: ['s1', 's2'] };

  test('slots inside the reservation are allowed', () => {
    expect(slotsWithinTestSession(session, ['s1', 's2'])).toEqual({ ok: true, outside: [] });
  });

  test('a slot outside it is named and refused', () => {
    expect(slotsWithinTestSession(session, ['s1', 's9'])).toEqual({ ok: false, outside: ['s9'] });
  });

  test('booking nothing is not a pass', () => {
    expect(slotsWithinTestSession(session, []).ok).toBe(false);
  });

  test('a session that reserved nothing can book nothing', () => {
    expect(slotsWithinTestSession({ reservedSlotIds: [] }, ['s1']).ok).toBe(false);
  });
});

describe('the stamp the server writes', () => {
  test('marks the record and names its owner', () => {
    expect(testStamp({ testSessionId: 'ts_7' })).toEqual({ isTest: true, testSessionId: 'ts_7' });
  });

  test('a live request is stamped with nothing at all', () => {
    expect(testStamp(null)).toEqual({});
  });

  test('what is stamped is what the reports exclude and a purge claims', () => {
    const booking = { date: '2026-09-21', ...testStamp({ testSessionId: 'ts_7' }) };
    expect(isTestBooking(booking)).toBe(true);
    expect(excludeTestBookings([booking, { date: '2026-09-21' }])).toHaveLength(1);
    expect(belongsToTestSession(booking, 'ts_7')).toBe(true);
    expect(belongsToTestSession(booking, 'ts_8')).toBe(false);
  });
});
