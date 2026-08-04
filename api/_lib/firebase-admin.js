// Shared Firebase Admin SDK initializer for Ultra Tennis API routes.
// getAdminDb() returns a Firestore instance. Safe to call on every lambda
// invocation — getApps() prevents re-initialization on warm reuse.
// Also hosts writeAuditLog — kept here (not a new _lib file) because every
// .js file under api/ counts toward the Vercel function limit.

import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getFirestore, FieldValue, Timestamp } from 'firebase-admin/firestore';
import { getAuth } from 'firebase-admin/auth';
import { getStorage } from 'firebase-admin/storage';
import { createHash, randomBytes } from 'crypto';

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
// Storage: guest_access_tokens/{sha256(token)} — the raw token is NEVER
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

// Issue a token for ONE booking. Any previously live token for the same
// booking is revoked with reason 'reissued' (GT-01).
// Returns the RAW token — the caller must return it to the client and then
// forget it. Never log it.
export async function issueGuestToken(db, { bookingId, bookingEndMs = null, issuedFor = 'guest_booking' }) {
  if (!bookingId) throw new Error('issueGuestToken: bookingId required');
  await revokeGuestTokensForBooking(db, bookingId, 'reissued');

  const token   = generateGuestToken();
  const nowMs   = Date.now();
  const expires = guestTokenExpiryMs(bookingEndMs, nowMs);

  await db.collection('guest_access_tokens').doc(hashGuestToken(token)).set({
    bookingId,
    issuedFor,
    issuedAt:  Timestamp.fromMillis(nowMs),
    expiresAt: Timestamp.fromMillis(expires),
    revokedAt: null,
    revokeReason: null,
    useCount:  0,
    lastUsedAt: null,
  });

  return { token, expiresAt: new Date(expires).toISOString() };
}

// Verify a token. Returns { ok:true, bookingId } or { ok:false, reason }.
// `reason` is deliberately coarse so the caller cannot use it as an oracle.
export async function verifyGuestToken(db, token) {
  if (typeof token !== 'string' || token.length < 20) return { ok: false, reason: 'invalid' };
  let snap;
  try {
    snap = await db.collection('guest_access_tokens').doc(hashGuestToken(token)).get();
  } catch (e) {
    console.error('[guest-token] lookup failed:', e.message);
    return { ok: false, reason: 'error' };
  }
  if (!snap.exists) return { ok: false, reason: 'invalid' };

  const d = snap.data();
  if (d.revokedAt) return { ok: false, reason: 'revoked' };
  const exp = d.expiresAt?.toMillis?.() ?? 0;
  if (!exp || exp < Date.now()) return { ok: false, reason: 'expired' };

  // Fire-and-forget usage counter — must never fail the request.
  snap.ref.update({ useCount: FieldValue.increment(1), lastUsedAt: FieldValue.serverTimestamp() })
    .catch(e => console.warn('[guest-token] usage counter:', e.message));

  return { ok: true, bookingId: d.bookingId };
}

// Revoke every live token for a booking. Safe to call when none exist.
export async function revokeGuestTokensForBooking(db, bookingId, reason) {
  if (!bookingId) return 0;
  const why = GUEST_REVOKE_REASONS.includes(reason) ? reason : 'admin_revoked';
  try {
    const q = await db.collection('guest_access_tokens')
      .where('bookingId', '==', bookingId).where('revokedAt', '==', null).limit(50).get();
    if (q.empty) return 0;
    const batch = db.batch();
    q.docs.forEach(doc => batch.update(doc.ref, {
      revokedAt: FieldValue.serverTimestamp(), revokeReason: why,
    }));
    await batch.commit();
    return q.size;
  } catch (e) {
    console.error('[guest-token] revoke failed:', e.message);
    return 0;
  }
}

// ── Durable rate limiter (GT-02) ────────────────────────────────────
// Fixed-window counter in Firestore. Chosen over a sliding window because
// it needs exactly one transactional read+write and cannot drift when the
// lambda is recycled. `blockMs` implements the 60-minute lockout for
// repeated invalid-token attempts.
//
// Returns { allowed:true } or { allowed:false, retryAfterSec }.
export async function checkRateLimit(db, { bucket, key, limit, windowMs, blockMs = 0 }) {
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

      if (count > limit) {
        const until = blockMs > 0 ? nowMs + blockMs : windowStart + windowMs;
        t.set(ref, {
          bucket, count,
          windowStart: Timestamp.fromMillis(fresh ? nowMs : windowStart),
          blockedUntil: Timestamp.fromMillis(until),
          updatedAt: FieldValue.serverTimestamp(),
        }, { merge: true });
        return { allowed: false, retryAfterSec: Math.ceil((until - nowMs) / 1000) };
      }

      t.set(ref, {
        bucket, count,
        windowStart: Timestamp.fromMillis(fresh ? nowMs : windowStart),
        blockedUntil: null,
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

// ── Idempotency for guest mutations (GT-02) ─────────────────────────
// tx.create() fails if the doc exists, which makes "claim the key" atomic
// without a read-modify-write race.
// Returns { fresh:true } when the caller should proceed, or
// { fresh:false, response } to replay a stored result.
export async function claimIdempotencyKey(db, key, scope) {
  const ref = db.collection('idempotency_records').doc(`${scope}__${createHash('sha256').update(String(key)).digest('hex').slice(0, 40)}`);
  try {
    await db.runTransaction(async (t) => {
      const snap = await t.get(ref);
      if (snap.exists) {
        const e = new Error('IDEMPOTENT_REPLAY');
        e.stored = snap.data()?.response ?? null;
        throw e;
      }
      t.create(ref, { scope, createdAt: FieldValue.serverTimestamp(), response: null });
    });
    return { fresh: true, ref };
  } catch (e) {
    if (e.message === 'IDEMPOTENT_REPLAY') return { fresh: false, response: e.stored };
    throw e;
  }
}

export async function storeIdempotentResponse(ref, response) {
  try { await ref.update({ response, completedAt: FieldValue.serverTimestamp() }); }
  catch (e) { console.warn('[idempotency] store failed:', e.message); }
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
