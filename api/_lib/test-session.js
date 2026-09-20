// ════════════════════════════════════════════════════════════════════
// Test Mode — session authority (SERVER ONLY)
// ════════════════════════════════════════════════════════════════════
// A test booking runs the real booking / transaction / slot-claim path, so
// nothing about it may be decided by the caller. What a client can send is a
// token; what makes a booking a test booking is this module resolving that
// token to an ACTIVE test_sessions document. `isTest` and `testSessionId` are
// then stamped by the server from the resolved session. A boolean in a request
// body is never read, anywhere — see assertNoClientTestFlags.
//
// Two independent checks, both required on every request:
//   1. HMAC signature   — proves the token was issued by us and is unexpired
//   2. session re-read  — proves it has not been ended or purged since
// The re-read is what makes ending a session take effect immediately, the same
// reasoning as coachSessionFromToken in admin-auth.js.
//
// Tokens are domain-separated from admin session cookies: both are
// "<base64url payload>.<hmac>" over ADMIN_SESSION_SECRET, so the scope prefix
// is what stops one being replayed as the other.
// ════════════════════════════════════════════════════════════════════

import { createHmac, timingSafeEqual, randomUUID } from 'node:crypto';
import { isTestBooking, belongsToTestSession } from '../../test-booking.js';

const SCOPE = 'test-session.v1';
const TEST_SESSION_COLLECTION = 'test_sessions';
const DEFAULT_TTL_HOURS = 4;
const MAX_TTL_HOURS = 24;

export const TEST_SESSION_STATUSES = Object.freeze(['active', 'ended', 'purged']);

function sign(payload) {
  const secret = process.env.ADMIN_SESSION_SECRET;
  if (!secret) throw new Error('ADMIN_SESSION_SECRET is not configured');
  return createHmac('sha256', secret).update(`${SCOPE}.${payload}`).digest('hex');
}

export function newTestSessionId() {
  return `ts_${randomUUID().replace(/-/g, '')}`;
}

export function testSessionTtlHours(requested) {
  const n = Number(requested);
  return Number.isFinite(n) && n >= 1 && n <= MAX_TTL_HOURS ? Math.floor(n) : DEFAULT_TTL_HOURS;
}

// Mint a token for a session that has already been written to Firestore.
export function createTestSessionToken(testSessionId, expMs) {
  const id = String(testSessionId || '').trim();
  if (!id) throw new Error('testSessionId is required');
  if (!Number.isFinite(expMs)) throw new Error('expMs is required');
  const payload = Buffer.from(JSON.stringify({ tsid: id, exp: expMs })).toString('base64url');
  return `${payload}.${sign(payload)}`;
}

// Signature + expiry only. Says the token is ours; says nothing about whether
// the session is still active — resolveTestSession decides that.
export function verifyTestSessionToken(token, nowMs = Date.now()) {
  if (typeof token !== 'string') return null;
  const value = token.trim();
  const dot = value.lastIndexOf('.');
  if (dot < 0) return null;

  const payload = value.slice(0, dot);
  const sig = value.slice(dot + 1);
  if (sig.length !== 64) return null;

  let expected;
  try { expected = sign(payload); } catch { return null; }
  try {
    if (!timingSafeEqual(Buffer.from(expected, 'hex'), Buffer.from(sig, 'hex'))) return null;
  } catch { return null; }

  let data;
  try { data = JSON.parse(Buffer.from(payload, 'base64url').toString()); } catch { return null; }
  if (!data || typeof data.tsid !== 'string' || !data.tsid.trim()) return null;
  if (typeof data.exp !== 'number' || data.exp < nowMs) return null;

  return { testSessionId: data.tsid, exp: data.exp };
}

// Where a request may carry the token. Nothing here is trusted on its own.
// `body` is passed separately because routes parse it themselves and req.body
// is often still the raw string by the time this runs.
export function readTestSessionToken(req, body = null) {
  const header = req?.headers?.['x-test-session'];
  if (typeof header === 'string' && header.trim()) return header.trim();
  const parsed = body && typeof body === 'object'
    ? body
    : (req?.body && typeof req.body === 'object' ? req.body : null);
  const fromBody = parsed?.testSessionToken;
  return typeof fromBody === 'string' && fromBody.trim() ? fromBody.trim() : null;
}

// How a resolved session travels from the route entry to the handler that
// creates the record. A Symbol key cannot appear in parsed JSON, so a caller
// has no way to inject one — the only thing that can put a session here is the
// dispatcher, after resolveTestSession verified it.
export const TEST_SESSION = Symbol('testSession');

export function attachTestSession(body, session) {
  if (body && typeof body === 'object') body[TEST_SESSION] = session || null;
  return body;
}

export function testSessionOf(body) {
  return (body && typeof body === 'object' ? body[TEST_SESSION] : null) ?? null;
}

// A client may not assert test-ness. If one of these turns up in a body it is
// either a stale integration or an attempt to get a real booking excluded from
// the books, and both should fail loudly rather than be ignored.
export function assertNoClientTestFlags(body) {
  if (!body || typeof body !== 'object') return null;
  for (const field of ['isTest', 'testSessionId']) {
    if (Object.prototype.hasOwnProperty.call(body, field)) {
      return `${field} cannot be set by the caller; send a test session token instead`;
    }
  }
  return null;
}

// The full check: signature, then the live document.
// Returns { ok: true, session } | { ok: false, reason } | null when no token
// was sent at all (an ordinary, live request — the overwhelmingly common case).
export async function resolveTestSession(req, db, nowMs = Date.now(), body = null) {
  const token = readTestSessionToken(req, body);
  if (!token) return null;

  const claim = verifyTestSessionToken(token, nowMs);
  if (!claim) return { ok: false, reason: 'invalid_token' };

  const snap = await db.collection(TEST_SESSION_COLLECTION).doc(claim.testSessionId).get();
  if (!snap.exists) return { ok: false, reason: 'session_not_found' };

  const data = snap.data();
  if (data.status !== 'active') return { ok: false, reason: `session_${data.status || 'unknown'}` };

  const expiresAtMs = data.expiresAt?.toMillis?.() ?? Number(data.expiresAtMs) ?? null;
  if (Number.isFinite(expiresAtMs) && expiresAtMs < nowMs) return { ok: false, reason: 'session_expired' };

  return {
    ok: true,
    session: {
      testSessionId: claim.testSessionId,
      branchId: data.branchId || null,
      resourceId: data.resourceId || null,
      reservedSlotIds: Array.isArray(data.reservedSlotIds) ? data.reservedSlotIds : [],
      createdBy: data.createdBy || null,
      simulatePayments: data.simulatePayments !== false,
    },
  };
}

// A test session may only book the slots it took out of public sale. Without
// this a test would compete with real customers for real inventory, which is
// the thing reserving slots up front is meant to prevent.
export function slotsWithinTestSession(session, slotIds) {
  const reserved = new Set(session?.reservedSlotIds || []);
  const wanted = Array.isArray(slotIds) ? slotIds : [];
  if (!wanted.length) return { ok: false, outside: [] };
  const outside = wanted.filter(id => !reserved.has(id));
  return { ok: outside.length === 0, outside };
}

// The status an available_slots doc carries while a test session holds it.
// Every public availability read gates on status === 'open', so writing this
// value is what takes the slot out of public sale — no other read has to learn
// about Test Mode for that to hold.
export const TEST_SLOT_STATUS = 'test_reserved';

// Whether a booking transaction may take this available_slots doc.
//
// The two cases are exclusive on purpose. Live traffic sees only open slots,
// so it can never take one a test is holding. A test session sees only the
// slots reserved to it, so a test can never consume public inventory even if
// the session were somehow pointed at an open slot.
export function availableToSession(slotData, session) {
  if (session) {
    return slotData?.status === TEST_SLOT_STATUS &&
      String(slotData?.testSessionId || '') === session.testSessionId;
  }
  return slotData?.status === 'open';
}

// The fields the server stamps onto a record it creates for a test session.
// Callers spread this rather than writing the flags themselves, so a creation
// path cannot invent a different shape.
export function testStamp(session) {
  return session ? { isTest: true, testSessionId: session.testSessionId } : {};
}

// Routes a client calls directly — /api/line-notify, /api/gcal — are handed a
// booking code and nothing trustworthy alongside it. They cannot be passed a
// session, so the stored booking is the authority: look it up and let the
// record say whether this is a test.
//
// Returns the owning session id, the sentinel for a record flagged without
// one, or null when this is live traffic or not about a booking at all.
export const TEST_SESSION_UNKNOWN = 'unknown';

export async function testSessionIdForBookingCode(db, bookingCode) {
  const code = String(bookingCode || '').trim();
  if (!code) return null;
  let snap;
  try {
    snap = await db.collection('bookings').where('bookingCode', '==', code).limit(1).get();
  } catch (e) {
    // Fail closed. If we cannot tell, treat it as a test: sendAndLog only
    // blocks a retry once an attempt has SUCCEEDED, so a wrongly suppressed
    // notification can simply be sent again, while a push that has gone out
    // cannot be taken back.
    console.error('[test-session] booking lookup failed:', e.message);
    return TEST_SESSION_UNKNOWN;
  }
  if (snap.empty) return null;
  const data = snap.docs[0].data();
  if (!isTestBooking(data)) return null;
  return String(data.testSessionId || '').trim() || TEST_SESSION_UNKNOWN;
}

export { isTestBooking, belongsToTestSession, TEST_SESSION_COLLECTION };
