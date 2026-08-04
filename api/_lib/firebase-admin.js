// Shared Firebase Admin SDK initializer for Ultra Tennis API routes.
// getAdminDb() returns a Firestore instance. Safe to call on every lambda
// invocation — getApps() prevents re-initialization on warm reuse.
// Also hosts writeAuditLog — kept here (not a new _lib file) because every
// .js file under api/ counts toward the Vercel function limit.

import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getFirestore, FieldValue, Timestamp } from 'firebase-admin/firestore';
import { getAuth } from 'firebase-admin/auth';
import { getStorage } from 'firebase-admin/storage';
import { createHash, randomBytes, timingSafeEqual } from 'crypto';

export function getAdminDb() {
  if (!getApps().length) {
    const raw = process.env.FIREBASE_SERVICE_ACCOUNT;
    if (!raw) throw new Error('FIREBASE_SERVICE_ACCOUNT env var not set');
    let sa;
    try { sa = JSON.parse(raw); }
    catch { throw new Error('FIREBASE_SERVICE_ACCOUNT is not valid JSON'); }
    initializeApp({ credential: cert(sa) });
  }
  return getFirestore();
}

// Cloud Storage bucket handle (firebase-admin bundles @google-cloud/storage —
// no new dependency). Caller passes the bucket name explicitly because slip
// URLs may reference either the .firebasestorage.app or .appspot.com alias.
export function getAdminBucket(bucketName) {
  getAdminDb(); // ensure app init
  return getStorage().bucket(bucketName);
}

// Firebase Auth admin instance. getAdminDb() ensures the app is initialized.
// Because init uses a service-account cert, createCustomToken() signs locally
// with the SA private key — no "Service Account Token Creator" IAM role needed.
export function getAdminAuth() {
  getAdminDb();
  return getAuth();
}

// ── Branch helpers ───────────────────────────────────────────────────
// Additive and fail-open by design: a missing branch doc or a failed
// read must NEVER block an operation (backward compatibility with the
// pre-branch system). Kept here, not in a new _lib file — every .js
// under api/ counts toward the Vercel function limit on this project.

export const BRANCH_STATUSES = ['active', 'soft_locked', 'hard_locked', 'customer_protection'];

// Read a branch config doc. Never throws; missing doc → null (= implicitly active).
export async function getBranch(db, branchId) {
  try {
    const snap = await db.collection('branches').doc(branchId).get();
    return snap.exists ? snap.data() : null;
  } catch (e) {
    console.error('[branch] getBranch failed:', e.message);
    return null;
  }
}

// Capability flags derived from a branch status.
export function statusFlags(status) {
  switch (status) {
    case 'soft_locked':
      return { allowNewBookings: false, allowStaffAccess: true,  allowCustomerView: true, showProtectionBanner: false };
    case 'hard_locked':
      return { allowNewBookings: false, allowStaffAccess: false, allowCustomerView: true, showProtectionBanner: false };
    case 'customer_protection':
      return { allowNewBookings: false, allowStaffAccess: false, allowCustomerView: true, showProtectionBanner: true };
    case 'active':
    default:
      return { allowNewBookings: true,  allowStaffAccess: true,  allowCustomerView: true, showProtectionBanner: false };
  }
}

// capability: 'new_bookings' | 'staff_access' | 'customer_view'
// Explicit flags on the doc win over status-derived defaults.
// branch null (missing doc / read failure) → always ok.
export function assertBranchAllows(branch, capability) {
  if (!branch) return { ok: true };
  const flags = statusFlags(branch.status);
  const map = {
    new_bookings:  branch.allowNewBookings  ?? flags.allowNewBookings,
    staff_access:  branch.allowStaffAccess  ?? flags.allowStaffAccess,
    customer_view: branch.allowCustomerView ?? flags.allowCustomerView,
  };
  const allowed = map[capability];
  if (allowed === undefined) return { ok: true };
  return allowed
    ? { ok: true }
    : { ok: false, error: `branch_${branch.status || 'locked'}` };
}

// Audit log writer. Fire-and-forget safe: NEVER throws — a failed audit
// write must not break the operation it documents.
export async function writeAuditLog(db, { actor, actorRole, branchId, action, targetId, before, after, source, note }) {
  try {
    await db.collection('audit_logs').add({
      actor:     actor     ?? 'unknown',
      actorRole: actorRole ?? 'unknown',
      branchId:  branchId  ?? null,
      action:    action    ?? 'unknown',
      targetId:  targetId  ?? null,
      before:    before    ?? null,   // keep small: only the fields that changed
      after:     after     ?? null,
      source:    source    ?? 'api',
      note:      note      ?? null,
      createdAt: FieldValue.serverTimestamp(),
    });
  } catch (e) {
    console.error('[audit] writeAuditLog failed:', e.message);
  }
}

// ════════════════════════════════════════════════════════════════════
// Guest Capability Token — Security Hotfix 2026-08-04
// ════════════════════════════════════════════════════════════════════
// Why this exists: guests never sign in to Firebase, so request.auth is
// null for them forever. Once the hardened rules make `bookings` owner-only
// a guest cannot read their own booking from Firestore at all. This token
// is the SERVER-SIDE replacement — Firestore rules never see it and never
// need to. It is checked inside the API route before any data is returned.
//
// Owner decisions encoded here (GT-01 / GT-02, approved 2026-08-04):
//   • expiry  = min(bookingEnd + 48h, issuedAt + 90d)
//   • revoke on: cancel, refund, account link, admin revoke, reissue
//   • rate limits are DURABLE (Firestore), never in-memory: a serverless
//     instance is recycled constantly, so an in-process counter would reset
//     and provide no protection at all.
//
// Storage: guest_booking_access/{bookingId} — the raw token is NEVER
// persisted. It is returned to the caller exactly once, in the response of
// the request that created it.
//
// NOTE: bookingCode / bookingId are PUBLIC values (they are readable in
// booking_slots). They must never form part of the token — see RD-01.
// ════════════════════════════════════════════════════════════════════

const GUEST_TOKEN_BYTES     = 32;
const GUEST_TTL_AFTER_END_MS = 48 * 60 * 60 * 1000;        // 48h after play ends
const GUEST_TTL_MAX_MS       = 90 * 24 * 60 * 60 * 1000;   // hard cap from issue

export const GUEST_REVOKE_REASONS = [
  'booking_cancelled', 'booking_refunded', 'account_linked',
  'admin_revoked', 'reissued',
];

// ── Store layout (review remediation RB-03 / RB-06) ─────────────────
// One document per booking, keyed by bookingId:
//
//   guest_booking_access/{bookingId}
//     tokenHash     sha256 of the current token, hex
//     scopes        string[] — what the token may do
//     issuedAt      Timestamp
//     expiresAt     Timestamp
//     revokedAt     Timestamp | null
//     revokeReason  string | null
//     tokenVersion  int, incremented on every issue
//
// The first design keyed documents by token hash and found the live token
// with a two-equality-filter query. That needed a composite index (RB-06),
// and reissue was a revoke followed by a separate create, which is not
// atomic (RB-03). Keying by bookingId removes both problems: there is
// exactly one document, lookup is a direct get, and reissue is a
// single-document transaction whose commit invalidates the old token in the
// same instant it publishes the new one.
//
// This is also why callers must now pass bookingId alongside the token:
// the token alone no longer locates the document. The token is still what
// proves the caller is entitled to it.
export const GUEST_ACCESS_COLLECTION = 'guest_booking_access';
export const GUEST_SCOPES = ['booking:read', 'booking:cancel', 'slip:submit'];

// Opaque, unguessable, and not derived from any public value.
export function generateGuestToken() {
  return randomBytes(GUEST_TOKEN_BYTES).toString('base64url');
}

export function hashGuestToken(token) {
  return createHash('sha256').update(String(token)).digest('hex');
}

// Compute the expiry per GT-01. bookingEndMs may be null (unknown) — the
// 90-day cap still applies.
export function guestTokenExpiryMs(bookingEndMs, nowMs = Date.now()) {
  const hardCap = nowMs + GUEST_TTL_MAX_MS;
  if (!Number.isFinite(bookingEndMs)) return hardCap;
  return Math.min(bookingEndMs + GUEST_TTL_AFTER_END_MS, hardCap);
}

// Issue (or reissue) the token for ONE booking, atomically.
//
// The whole operation is a single-document transaction, so at the moment it
// commits the previous tokenHash is gone and the previous token stops
// verifying. There is no window where both work, and no window where
// neither does.
//
// Returns the RAW token. It exists only in this return value and in the
// HTTP response the caller builds from it. Never log it, never store it.
export async function issueGuestToken(db, { bookingId, bookingEndMs = null, scopes = GUEST_SCOPES }) {
  if (!bookingId) throw new Error('issueGuestToken: bookingId required');

  const token   = generateGuestToken();
  const nowMs   = Date.now();
  const expires = guestTokenExpiryMs(bookingEndMs, nowMs);
  const ref     = db.collection(GUEST_ACCESS_COLLECTION).doc(bookingId);

  const tokenVersion = await db.runTransaction(async (t) => {
    const snap = await t.get(ref);
    const prev = snap.exists ? (Number(snap.data().tokenVersion) || 0) : 0;
    t.set(ref, {
      tokenHash:    hashGuestToken(token),
      scopes:       Array.isArray(scopes) ? scopes : GUEST_SCOPES,
      issuedAt:     Timestamp.fromMillis(nowMs),
      expiresAt:    Timestamp.fromMillis(expires),
      revokedAt:    null,
      revokeReason: null,
      tokenVersion: prev + 1,
    });
    return prev + 1;
  });

  return { token, expiresAt: new Date(expires).toISOString(), tokenVersion };
}

// Verify a token against a specific booking.
//
// bookingId locates the document; the token proves entitlement. A token
// issued for booking A therefore cannot be presented against booking B —
// the document found under B holds a different hash.
//
// `reason` stays coarse so it cannot be used as an oracle.
export async function verifyGuestToken(db, bookingId, token, requiredScope = null) {
  if (!bookingId) return { ok: false, reason: 'invalid' };
  if (typeof token !== 'string' || token.length < 20) return { ok: false, reason: 'invalid' };

  let snap;
  try {
    snap = await db.collection(GUEST_ACCESS_COLLECTION).doc(bookingId).get();
  } catch (e) {
    // Message only. The token and the booking id never reach the log.
    console.error('[guest-access] lookup failed:', e.message);
    return { ok: false, reason: 'error' };
  }
  if (!snap.exists) return { ok: false, reason: 'invalid' };

  const d = snap.data();
  if (d.revokedAt) return { ok: false, reason: 'invalid' };

  const exp = d.expiresAt?.toMillis?.() ?? 0;
  if (!exp || exp < Date.now()) return { ok: false, reason: 'invalid' };

  // Constant-time compare of two equal-length hex digests. Both sides are
  // sha256 output, so the length check can only fail on a malformed stored
  // document, never on caller input.
  const presented = Buffer.from(hashGuestToken(token), 'utf8');
  const stored    = Buffer.from(String(d.tokenHash || ''), 'utf8');
  if (presented.length !== stored.length) return { ok: false, reason: 'invalid' };
  if (!timingSafeEqual(presented, stored)) return { ok: false, reason: 'invalid' };

  if (requiredScope && !(Array.isArray(d.scopes) && d.scopes.includes(requiredScope))) {
    return { ok: false, reason: 'scope' };
  }

  return { ok: true, bookingId, scopes: d.scopes || [], tokenVersion: d.tokenVersion ?? null };
}

// Revoke access for a booking. Direct document update — no query, no index.
// Safe to call when no access document exists.
export async function revokeGuestAccess(db, bookingId, reason) {
  if (!bookingId) return false;
  const why = GUEST_REVOKE_REASONS.includes(reason) ? reason : 'admin_revoked';
  try {
    const ref = db.collection(GUEST_ACCESS_COLLECTION).doc(bookingId);
    return await db.runTransaction(async (t) => {
      const snap = await t.get(ref);
      if (!snap.exists) return false;
      if (snap.data().revokedAt) return false;      // already revoked
      t.update(ref, {
        tokenHash:    null,                          // the token stops verifying immediately
        revokedAt:    FieldValue.serverTimestamp(),
        revokeReason: why,
      });
      return true;
    });
  } catch (e) {
    console.error('[guest-access] revoke failed:', e.message);
    return false;
  }
}

// ── Durable rate limiter (GT-02) ────────────────────────────────────
// Fixed-window counter in Firestore. Chosen over a sliding window because
// it needs exactly one transactional read+write and cannot drift when the
// lambda is recycled. `blockMs` implements the 60-minute lockout for
// repeated invalid-token attempts.
//
// Returns { allowed:true } or { allowed:false, retryAfterSec }.
// TTL grace after the window (or block) ends, per the review decision:
// expiresAt = windowEnd + 24h.
const RATE_LIMIT_TTL_GRACE_MS = 24 * 60 * 60 * 1000;

export async function checkRateLimit(db, { bucket, key, limit, windowMs, blockMs = 0 }) {
  // One document per (bucket, key) — reused across windows, never one per
  // attempt. windowStart is what rolls the window, not the document id.
  const docId = `${bucket}__${createHash('sha256').update(String(key)).digest('hex').slice(0, 32)}`;
  const ref   = db.collection('rate_limits').doc(docId);
  const nowMs = Date.now();

  try {
    return await db.runTransaction(async (t) => {
      const snap = await t.get(ref);
      const d    = snap.exists ? snap.data() : null;

      const blockedUntil = d?.blockedUntil?.toMillis?.() ?? 0;
      if (blockedUntil > nowMs) {
        return { allowed: false, retryAfterSec: Math.ceil((blockedUntil - nowMs) / 1000) };
      }

      const windowStart = d?.windowStart?.toMillis?.() ?? 0;
      const fresh = !windowStart || (nowMs - windowStart) >= windowMs;
      const count = fresh ? 1 : (Number(d.count) || 0) + 1;

      // RB-04: expiresAt is what a Firestore TTL policy acts on. It always
      // sits past the end of the current window (and past any block), so a
      // document is only reaped once it can no longer affect a decision.
      const effWindowStart = fresh ? nowMs : windowStart;
      const windowEnd      = effWindowStart + windowMs;

      if (count > limit) {
        const until = blockMs > 0 ? nowMs + blockMs : windowEnd;
        t.set(ref, {
          bucket, count,
          windowStart: Timestamp.fromMillis(effWindowStart),
          blockedUntil: Timestamp.fromMillis(until),
          expiresAt: Timestamp.fromMillis(Math.max(windowEnd, until) + RATE_LIMIT_TTL_GRACE_MS),
          updatedAt: FieldValue.serverTimestamp(),
        }, { merge: true });
        return { allowed: false, retryAfterSec: Math.ceil((until - nowMs) / 1000) };
      }

      t.set(ref, {
        bucket, count,
        windowStart: Timestamp.fromMillis(effWindowStart),
        blockedUntil: null,
        expiresAt: Timestamp.fromMillis(windowEnd + RATE_LIMIT_TTL_GRACE_MS),
        updatedAt: FieldValue.serverTimestamp(),
      }, { merge: true });
      return { allowed: true };
    });
  } catch (e) {
    // Fail CLOSED on a limiter error: an unavailable limiter must not become
    // a bypass. The caller surfaces this as a 429.
    console.error('[rate-limit] transaction failed → denying:', e.message);
    return { allowed: false, retryAfterSec: 60 };
  }
}

// GT-02 buckets. `ip` should come from x-forwarded-for (Vercel sets it).
export const RATE_LIMITS = {
  guestInvalid:  { limit: 5,  windowMs: 15 * 60 * 1000, blockMs: 60 * 60 * 1000 },
  guestRead:     { limit: 30, windowMs: 15 * 60 * 1000, blockMs: 0 },
  guestMutation: { limit: 5,  windowMs: 15 * 60 * 1000, blockMs: 0 },
};

export function clientIp(req) {
  const fwd = req.headers['x-forwarded-for'];
  if (typeof fwd === 'string' && fwd) return fwd.split(',')[0].trim();
  return req.socket?.remoteAddress || 'unknown';
}

// ── Idempotency (review remediation RB-02 / RB-09) ──────────────────
// The first version claimed the key in its own transaction, then ran the
// mutation, then wrote the response in a third step. Three problems:
//   • a mutation could commit with no record, or a record could exist with
//     no mutation (RB-02);
//   • a concurrent retry arriving between claim and response-write saw
//     response:null and returned an empty body (RB-09);
//   • a failed mutation left the key claimed forever.
//
// These helpers are transaction-native instead. The caller reads the record
// and writes it INSIDE the same transaction as the mutation, so the record
// and the effect it describes commit or fail together. A rolled-back
// transaction leaves no record, and a retry always finds a complete
// response because the response is written in the same commit.
//
// The fingerprint is a hash of the semantically significant request fields.
// Same key + same fingerprint replays; same key + different fingerprint is
// a client bug and returns 409 rather than silently applying either one.
const IDEMPOTENCY_TTL_MS = 90 * 24 * 60 * 60 * 1000;   // 90 days

export function idempotencyRef(db, key, scope) {
  const id = `${scope}__${createHash('sha256').update(String(key)).digest('hex').slice(0, 40)}`;
  return db.collection('idempotency_records').doc(id);
}

// Stable hash of the request fields that define "the same request".
// Key order is normalised so callers do not have to care.
export function fingerprintOf(fields) {
  const norm = JSON.stringify(fields, Object.keys(fields).sort());
  return createHash('sha256').update(norm).digest('hex');
}

// Call inside a transaction, before performing any write.
//   { state: 'fresh' }                      → proceed with the mutation
//   { state: 'replay', response }           → return the stored response
//   { state: 'conflict' }                   → same key, different request
export async function readIdempotencyInTx(t, ref, fingerprint) {
  const snap = await t.get(ref);
  if (!snap.exists) return { state: 'fresh' };
  const d = snap.data();
  if (d.fingerprint && fingerprint && d.fingerprint !== fingerprint) return { state: 'conflict' };
  // A record only ever exists together with its response, because both are
  // written in the same commit — so this can never be an empty replay.
  return { state: 'replay', response: d.response ?? null };
}

// Call inside the same transaction as the mutation.
// expiresAt is what a Firestore TTL policy on this collection will act on.
export function writeIdempotencyInTx(t, ref, { scope, fingerprint, response, nowMs = Date.now() }) {
  t.create(ref, {
    scope,
    fingerprint,
    response,
    createdAt: Timestamp.fromMillis(nowMs),
    expiresAt: Timestamp.fromMillis(nowMs + IDEMPOTENCY_TTL_MS),
  });
}

// Convert a Firestore Admin SDK document data object to a JSON-safe plain object.
// Firestore Timestamps (have .toDate()) are converted to ISO-8601 strings.
export function serializeFsDoc(data) {
  const out = {};
  for (const [k, v] of Object.entries(data)) {
    if (v === null || v === undefined) {
      out[k] = v;
    } else if (typeof v.toDate === 'function') {
      out[k] = v.toDate().toISOString();
    } else {
      out[k] = v;
    }
  }
  return out;
}
