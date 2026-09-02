// ════════════════════════════════════════════════════════════════════
// POST /api/slip-verify — Phase 1A slip pre-check (Auto Verify Slip)
// ════════════════════════════════════════════════════════════════════
// Called by index.html after the authenticated submit_slip action atomically
// sets paymentStatus:"pending_review" on the booking, slots, and claims.
// This verification route:
//
//   1. Re-reads the booking server-side (bookingCode must match).
//   2. Reads slipUrl FROM FIRESTORE (never from the request), downloads
//      the file via the Admin SDK, and computes the SHA-256 itself.
//      Client-provided hash / decoded QR payload are UNTRUSTED HINTS.
//   3. Duplicate checks:
//        • server-computed slipHash → slip_registry/hash_{sha256}
//          (registry entries are created ONLY for server-computed hashes)
//        • client-decoded transactionRef → advisory query against
//          bookings.paymentVerification.transactionRef → at most
//          "manual_review / suspected_duplicate_ref", never a rejection
//   4. Writes ONLY the paymentVerification map on the booking. It NEVER
//      touches paymentStatus / bookingStatus / price / slots. Admin
//      Mark as Paid (/api/admin-edit-booking-accounting) stays the sole
//      authority for "paid".
//   5. Sends the one admin LINE notification for this slip itself
//      (precheck-passed / urgent-review / legacy slip-uploaded), so the
//      client no longer fires slip_uploaded_admin on the happy path.
//
// Phase 1A status semantics (NO auto-paid in this phase):
//   pre_verified  — LOCAL checks passed (server hash computed, no
//                   duplicates found). NOT bank-verified, NOT paid.
//   manual_review — suspicious: duplicate hash / suspected duplicate ref.
//   not_checked   — server could not fetch/hash the slip; client hash is
//                   recorded as helper data only.
//   "rejected" and "verified" are reserved for Phase 2 (trusted bank
//   verification) and are never produced here.
//
// The pre-check endpoint is a server-side follow-up to that submission.
// This follow-up reads only the slip already accepted by authenticated or
// verified-guest submission; bookingCode is not used as authentication.
// Repeat calls for an unchanged slipUrl return the stored result without
// re-downloading or re-notifying (cost/spam guard).
// ════════════════════════════════════════════════════════════════════

import crypto from 'node:crypto';
import {
  getAdminDb, getAdminBucket, writeAuditLog, getAdminAuth,
  verifyGuestToken, checkRateLimit, readRateLimitGate, RATE_LIMITS, clientIp,
  idempotencyRef, fingerprintOf, readIdempotencyInTx, writeIdempotencyInTx,
  GUEST_BOOKING_ID_MAX_LENGTH, GUEST_TOKEN_MAX_LENGTH, isValidIdempotencyKey,
} from './_lib/firebase-admin.js';
import { sendAndLog, loadActiveAdmins, loadNotificationFlags } from './_lib/notify.js';
import { FieldValue } from 'firebase-admin/firestore';
import { isCoachAddonV2Booking } from './_lib/coach-addon-v2.js';
import { releaseCoachAddonV2Hold } from './_lib/coach-addon-v2-store.js';

const MAX_SLIP_BYTES   = 6 * 1024 * 1024;             // client caps at 5MB; headroom
const ALLOWED_BUCKETS  = [
  'ultra-tennis-booking.firebasestorage.app',
  'ultra-tennis-booking.appspot.com',
];
const ALLOWED_PATH_RE  = /^(payment_slips|pass_slips)\//;
const SHA256_RE        = /^[a-f0-9]{64}$/i;
const MAX_DOCUMENT_ID_LENGTH = 128;
const MAX_BOOKING_CODE_LENGTH = 128;
const MAX_SLIP_URL_LENGTH = 2048;

// Mirrors the customer-facing dynamic-QR routing in index.html (read-only
// metadata for the admin — Phase 1A never compares receivers itself).
const RECEIVER_MAIN = '0066815139905';  // phone PromptPay (standard/morning/voucher)
const RECEIVER_ALT  = '1729900373121';  // national-ID (special promotion)

const REASON_TH = {
  duplicate_slip_hash:     'พบไฟล์สลิปนี้ถูกใช้กับการจองอื่นแล้ว (ตรวจจาก hash ฝั่งเซิร์ฟเวอร์)',
  suspected_duplicate_ref: 'เลขอ้างอิงโอน (อ่านจาก QR ฝั่งลูกค้า — ยังไม่ยืนยัน) ตรงกับสลิปของการจองอื่น',
  server_fetch_failed:     'เซิร์ฟเวอร์ยังอ่านไฟล์สลิปไม่ได้ กรุณาตรวจสอบสลิปเอง',
};

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

// Firebase Storage download URL →  { bucket, path } (or null if not ours).
// Format: https://firebasestorage.googleapis.com/v0/b/<bucket>/o/<encodedPath>?...
function parseStorageUrl(url) {
  try {
    const u = new URL(url);
    if (u.hostname !== 'firebasestorage.googleapis.com') return null;
    const m = u.pathname.match(/^\/v0\/b\/([^/]+)\/o\/(.+)$/);
    if (!m) return null;
    const bucket = m[1];
    const path = decodeURIComponent(m[2]);
    if (!ALLOWED_BUCKETS.includes(bucket)) return null;
    if (!ALLOWED_PATH_RE.test(path)) return null;
    return { bucket, path };
  } catch { return null; }
}

// Best-effort transRef extraction from a client-decoded slip mini-QR.
// Thai bank slip QRs are EMV-style TLV; tag "00" holds a sub-TLV whose
// tag "01" is bankCode+transactionRef. UNTRUSTED — hint only.
function extractTransRef(rawPayload) {
  if (typeof rawPayload !== 'string') return null;
  const s = rawPayload.trim().slice(0, 512);
  if (!s || !/^[\x20-\x7E]+$/.test(s)) return null;
  const walk = (str) => {
    const out = {};
    let i = 0;
    while (i + 4 <= str.length) {
      const tag = str.slice(i, i + 2);
      const len = parseInt(str.slice(i + 2, i + 4), 10);
      if (!Number.isFinite(len) || len < 0 || i + 4 + len > str.length) return out;
      out[tag] = str.slice(i + 4, i + 4 + len);
      i += 4 + len;
    }
    return out;
  };
  const top = walk(s);
  if (top['00']) {
    const sub = walk(top['00']);
    const ref = String(sub['01'] || '').replace(/[^A-Za-z0-9]/g, '');
    if (ref.length >= 10 && ref.length <= 64) return ref.toUpperCase();
  }
  return null;
}

// Download the slip via the Admin SDK and hash it. Never throws —
// returns { hash } or { error }.
async function computeServerSlipHash(slipUrl) {
  const loc = parseStorageUrl(slipUrl);
  if (!loc) return { error: 'slipUrl is not a recognized storage URL' };
  try {
    const file = getAdminBucket(loc.bucket).file(loc.path);
    const [meta] = await file.getMetadata();
    const size = Number(meta.size) || 0;
    if (size <= 0 || size > MAX_SLIP_BYTES) return { error: `slip file size out of range (${size})` };
    const [buf] = await file.download();
    return { hash: crypto.createHash('sha256').update(buf).digest('hex') };
  } catch (e) {
    return { error: `storage fetch failed: ${e.message}` };
  }
}

// ════════════════════════════════════════════════════════════════════
// Server-side slip submission — Security Hotfix 2026-08-04
// ════════════════════════════════════════════════════════════════════
// index.html:3387 ran a browser transaction that set the booking to
// bookingStatus:"confirmed" and paymentStatus:"pending_review", and locked
// every slot, the moment a slip was uploaded. Two problems:
//
//   • It contradicts the locked business rule. Addendum 02 sec 12 states the
//     booking becomes confirmed only AFTER an admin presses Mark Paid.
//     Uploading an image is not evidence of payment.
//   • The active rules allow any caller to perform that update on any
//     booking, so "confirmed" was never a trustworthy state (SEC-03).
//
// Here the slip moves the booking, every public slot, and every private claim
// into pending_review atomically. Admin approval remains the sole path to
// confirmed, and pending_review remains occupied regardless of hold expiry.
//
// Auth: a LINE customer presents a Firebase ID token; a guest presents the
// capability token issued when the booking was created.
// ════════════════════════════════════════════════════════════════════

async function serverSlipSubmitEnabled(db) {
  try {
    const snap = await db.collection('system_settings').doc('features').get();
    return snap.exists && snap.data().useServerSlipSubmit === true;
  } catch (e) {
    console.warn('[server slip flag] read failed → OFF:', e.message);
    return false;
  }
}

// Resolves the caller against a target booking. Returns
// { ok, actor } or { ok:false, status, error }.
async function authorizeSlipCaller(req, db, { bookingId, ownerLineUserId, idToken, guestToken }) {
  // RB-11: an ID token that fails verification is a hard failure. It must
  // not silently degrade into a weaker check.
  if (idToken) {
    let uid = null;
    try { uid = (await getAdminAuth().verifyIdToken(idToken)).uid; }
    catch { return { ok: false, status: 401, error: 'เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่' }; }
    if (uid && uid === ownerLineUserId) return { ok: true, actor: uid, actorRole: 'customer' };
    return { ok: false, status: 403, error: 'บัญชีไม่ตรงกับการจอง' };
  }
  const ip = clientIp(req);
  const globalGate = await readRateLimitGate(db, { bucket: 'guestInvalid', key: ip });
  if (!globalGate.allowed) {
    return { ok: false, status: 429, error: 'Too many requests', retryAfterSec: globalGate.retryAfterSec };
  }
  if (guestToken) {
    const v = await verifyGuestToken(db, bookingId, guestToken, 'slip:submit');
    if (v.ok) {
      const gate = await checkRateLimit(db, {
        bucket: 'guestMutation', key: `${ip}|${bookingId}`, ...RATE_LIMITS.guestMutation,
      });
      if (!gate.allowed) return { ok: false, status: 429, error: 'Too many requests', retryAfterSec: gate.retryAfterSec };
      return { ok: true, actor: 'guest', actorRole: 'guest' };
    }
  }
  const bad = await checkRateLimit(db, {
    bucket: 'guestInvalid', key: ip, ...RATE_LIMITS.guestInvalid,
  });
  if (!bad.allowed) return { ok: false, status: 429, error: 'Too many attempts', retryAfterSec: bad.retryAfterSec };
  return { ok: false, status: 403, error: 'ยืนยันตัวตนไม่ผ่าน' };
}

// Accepts a storage URL only if it points at our own bucket, reusing the
// parser the hashing path already relies on. Prevents a caller from
// pointing slipUrl at an arbitrary external host.
function isOwnStorageUrl(url) {
  return !!parseStorageUrl(url);
}

// The slot ids a booking holds. Server-created bookings record them
// explicitly; older ones are reconstructed from date + start + duration the
// same way the booking route derives them.
function bookingSlotIdsOf(b) {
  if (Array.isArray(b.bookingSlotIds) && b.bookingSlotIds.length) return b.bookingSlotIds;
  if (!b.date || !b.startTime) return [];
  const toMin = t => { const [h, m] = String(t).split(':').map(Number); return h * 60 + (m || 0); };
  const pad   = n => String(n).padStart(2, '0');
  const dm    = Number(b.durationMinutes);
  const total = (Number.isInteger(dm) && dm >= 30) ? Math.min(dm, 360)
              : Math.min(Math.max(parseInt(b.durationHours, 10) || 1, 1), 6) * 60;
  const ids = [];
  let m = toMin(b.startTime);
  const end = m + total;
  while (m < end) {
    const span = (m % 60 === 0 && m + 60 <= end) ? 60 : 30;
    ids.push(`${b.resourceId || 'room1'}_${b.date}_${pad(Math.floor(m / 60))}${pad(m % 60)}`);
    m += span;
  }
  return ids;
}

async function handleSubmitBookingSlip(req, res, body, db) {
  const bookingIdRaw = typeof body.bookingId === 'string' ? body.bookingId : '';
  const bookingCodeRaw = typeof body.bookingCode === 'string' ? body.bookingCode : '';
  const slipUrlRaw = typeof body.slipUrl === 'string' ? body.slipUrl : '';
  const guestTokenRaw = typeof body.guestToken === 'string' ? body.guestToken : '';
  if (bookingIdRaw.length > GUEST_BOOKING_ID_MAX_LENGTH || bookingCodeRaw.length > MAX_BOOKING_CODE_LENGTH ||
      slipUrlRaw.length > MAX_SLIP_URL_LENGTH || guestTokenRaw.length > GUEST_TOKEN_MAX_LENGTH) {
    return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Request field is too long' });
  }
  const bookingId   = bookingIdRaw.trim();
  const bookingCode = bookingCodeRaw.trim();
  const slipUrl     = slipUrlRaw.trim();
  const idToken     = typeof body.idToken === 'string' ? body.idToken.trim() : '';
  const guestToken  = guestTokenRaw.trim();
  const idemKey     = typeof body.idempotencyKey === 'string' ? body.idempotencyKey.trim() : '';

  if (!bookingId)   return res.status(400).json({ ok: false, error: 'Missing bookingId' });
  if (!bookingCode) return res.status(400).json({ ok: false, error: 'Missing bookingCode' });
  if (!slipUrl || !isOwnStorageUrl(slipUrl)) {
    return res.status(400).json({ ok: false, error: 'slipUrl must be a Firebase Storage URL for this project' });
  }
  if (!isValidIdempotencyKey(idemKey)) return res.status(400).json({ ok: false, code: 'IDEMPOTENCY', error: 'idempotencyKey has invalid format or length' });

  // Guest capability authorization does not need booking data. Resolve it
  // before the booking read so rotating nonexistent booking IDs is still
  // charged to the single global invalid-IP bucket.
  let auth = null;
  if (!idToken) {
    auth = await authorizeSlipCaller(req, db, {
      bookingId, ownerLineUserId: null, idToken: '', guestToken,
    });
    if (!auth.ok) {
      if (auth.retryAfterSec) res.setHeader('Retry-After', String(auth.retryAfterSec));
      return res.status(auth.status).json({ ok: false, error: auth.error });
    }
  }

  const bookingRef = db.collection('bookings').doc(bookingId);
  let booking;
  try {
    const snap = await bookingRef.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Booking not found' });
    booking = snap.data();
  } catch (e) {
    console.error('[submit_slip] read:', e.message);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }

  if (!auth) {
    auth = await authorizeSlipCaller(req, db, {
      bookingId, ownerLineUserId: booking.lineUserId, idToken, guestToken: '',
    });
    if (!auth.ok) {
      if (auth.retryAfterSec) res.setHeader('Retry-After', String(auth.retryAfterSec));
      return res.status(auth.status).json({ ok: false, error: auth.error });
    }
  }

  // bookingCode is checked only after the caller has authenticated.
  if (bookingCode !== booking.bookingCode) {
    return res.status(403).json({ ok: false, error: 'ไม่ใช่การจองของคุณ' });
  }

  // RB-02/RB-09: the idempotency record lives in the same transaction as the
  // state change, so a rolled-back submission leaves no record and a replay
  // always finds a complete response.
  const idemScope = auth.actorRole === 'guest'
    ? `submit_slip:guest:${bookingId}`
    : `submit_slip:customer:${auth.actor}`;
  const idemRef = idempotencyRef(db, idemKey, idemScope);
  const idemFp  = fingerprintOf({ bookingId, slipUrl, actor: auth.actor });
  const response = { ok: true, paymentStatus: 'pending_review', bookingStatus: 'pending_review' };
  let replayed = null;

  try {
    await db.runTransaction(async (t) => {
      const idem = await readIdempotencyInTx(t, idemRef, idemFp);
      if (idem.state === 'conflict') throw new Error('IDEMPOTENCY_CONFLICT');
      if (idem.state === 'replay')  { replayed = idem.response; return; }

      // 2. Booking state.
      const snap = await t.get(bookingRef);
      if (!snap.exists) throw new Error('GONE');
      const b = snap.data();
      if (b.paymentStatus === 'paid' || b.paymentStatus === 'package') throw new Error('ALREADY_SETTLED');
      if (b.bookingStatus === 'cancelled') throw new Error('CANCELLED');
      if (b.paymentStatus !== 'unpaid' && b.paymentStatus !== 'pending_review') throw new Error('BAD_STATE');
      const isCoachV2 = isCoachAddonV2Booking(b);
      if (isCoachV2) {
        if (b.bookingState !== 'held' || !['unpaid', 'pending_review'].includes(b.cashState)) throw new Error('BAD_STATE');
        const expiry = b.paymentExpiresAt?.toMillis?.() ?? null;
        // A replay of an already-submitted slip remains idempotent.  A first
        // submission after the deadline is rejected and released below.
        if (b.cashState === 'unpaid' && expiry !== null && expiry <= Date.now()) throw new Error('HOLD_EXPIRED');
      }

      // 3. Every private slot claim must belong to this booking (RB-08).
      // The public document no longer carries an owner, so this is the only
      // place the linkage can be checked — and it is checked for every slot
      // the booking says it holds, not just one.
      const slotIds    = bookingSlotIdsOf(b);
      if (!slotIds.length) throw new Error('SLOT_OWNERSHIP_MISMATCH');
      const claimSnaps = await Promise.all(slotIds.map(id => t.get(db.collection('booking_slot_claims').doc(id))));
      const slotSnaps  = await Promise.all(slotIds.map(id => t.get(db.collection('booking_slots').doc(id))));
      const coachClaimIds = isCoachV2 && Array.isArray(b.coachClaimIds) ? b.coachClaimIds : [];
      const coachClaimSnaps = await Promise.all(coachClaimIds.map(id => t.get(db.collection('coach_slot_claims').doc(id))));

      claimSnaps.forEach((cs, i) => {
        const ss = slotSnaps[i];
        // A slot released and taken by someone else has no claim of ours.
        if (!cs.exists) {
          // Legacy compatibility: slots written before this hotfix carry the
          // identifiers inline instead of in a claim.
          const sd = ss.exists ? ss.data() : null;
          const legacyOwns = sd && (sd.bookingId === bookingId || sd.bookingCode === bookingCode);
          if (!legacyOwns) throw new Error('SLOT_OWNERSHIP_MISMATCH');
          return;
        }
        if (cs.data().bookingId !== bookingId) throw new Error('SLOT_OWNERSHIP_MISMATCH');
      });
      coachClaimSnaps.forEach(cs => {
        if (!cs.exists || cs.data().bookingId !== bookingId) throw new Error('COACH_CLAIM_CONFLICT');
      });

      // 4. pending_review is an occupied pre-confirmation state. Only admin
      // approval promotes it to confirmed.
      t.update(bookingRef, {
        bookingStatus:  'pending_review',
        paymentStatus:  'pending_review',
        ...(isCoachV2 ? { cashState: 'pending_review' } : {}),
        slipUrl,
        slipUploadedAt: FieldValue.serverTimestamp(),
        slipSubmittedVia: 'server',
        updatedAt:      FieldValue.serverTimestamp(),
      });

      // 5/6. Slots and claims move atomically to the same occupied state.
      slotSnaps.forEach((ss, i) => {
        if (!ss.exists) return;
        if (ss.data().bookingStatus === 'confirmed') return;   // already settled
        t.update(db.collection('booking_slots').doc(slotIds[i]), {
          bookingStatus: 'pending_review',
          paymentStatus: 'pending_review',
        });
        if (claimSnaps[i]?.exists) {
          t.update(db.collection('booking_slot_claims').doc(slotIds[i]), {
            status: 'pending_review', updatedAt: FieldValue.serverTimestamp(),
          });
        }
      });
      coachClaimSnaps.forEach((cs, i) => {
        if (!cs.exists || cs.data().status === 'confirmed') return;
        t.update(db.collection('coach_slot_claims').doc(coachClaimIds[i]), {
          status: 'pending_review', updatedAt: FieldValue.serverTimestamp(),
        });
      });

      writeIdempotencyInTx(t, idemRef, { scope: idemScope, fingerprint: idemFp, response });
    });
  } catch (e) {
    if (e.message === 'HOLD_EXPIRED') {
      await releaseCoachAddonV2Hold(db, bookingId, {
        reason: 'slip_submitted_after_expiry', actor: auth.actor, requireExpired: true,
      }).catch(error => console.error('[submit_slip] expired release:', error.message));
      return res.status(409).json({ ok: false, code: 'HOLD_EXPIRED', error: 'หมดเวลาชำระเงินแล้ว กรุณาจองใหม่' });
    }
    if (e.message === 'IDEMPOTENCY_CONFLICT') {
      return res.status(409).json({ ok: false, code: 'IDEMPOTENCY_CONFLICT', error: 'idempotencyKey ถูกใช้กับคำขออื่นแล้ว' });
    }
    if (e.message === 'SLOT_OWNERSHIP_MISMATCH') {
      return res.status(409).json({
        ok: false, code: 'SLOT_OWNERSHIP_MISMATCH',
        error: 'ช่องเวลาของการจองนี้ถูกเปลี่ยนไปแล้ว กรุณาติดต่อแอดมิน',
      });
    }
    if (e.message === 'COACH_CLAIM_CONFLICT') {
      return res.status(409).json({ ok: false, code: 'COACH_CLAIM_CONFLICT', error: 'เวลาของโค้ชถูกเปลี่ยนไปแล้ว กรุณาติดต่อแอดมิน' });
    }
    const map = {
      GONE:            [404, 'Booking not found'],
      ALREADY_SETTLED: [409, 'การจองนี้ชำระเงินแล้ว'],
      CANCELLED:       [409, 'การจองนี้ถูกยกเลิกแล้ว'],
      BAD_STATE:       [409, 'สถานะการจองเปลี่ยนไปแล้ว กรุณารีเฟรช'],
    };
    const [code, msg] = map[e.message] || [500, 'ส่งสลิปไม่สำเร็จ'];
    if (code === 500) console.error('[submit_slip] tx:', e.message);
    return res.status(code).json({ ok: false, error: msg });
  }

  if (replayed) return res.status(200).json({ ...replayed, replayed: true });

  await writeAuditLog(db, {
    actor: auth.actor, actorRole: auth.actorRole,
    branchId: booking.branchId ?? null,
    action: 'slip_submitted', targetId: bookingId,
    before: { paymentStatus: booking.paymentStatus, bookingStatus: booking.bookingStatus },
    after:  { paymentStatus: 'pending_review', bookingStatus: 'pending_review' },
    note: bookingCode,
  });

  return res.status(200).json(response);
}

async function handleSubmitPassSlip(req, res, body, db) {
  const purchaseIdRaw = typeof body.purchaseId === 'string' ? body.purchaseId : '';
  const slipUrlRaw = typeof body.slipUrl === 'string' ? body.slipUrl : '';
  if (purchaseIdRaw.length > MAX_DOCUMENT_ID_LENGTH || slipUrlRaw.length > MAX_SLIP_URL_LENGTH) {
    return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Request field is too long' });
  }
  const purchaseId = purchaseIdRaw.trim();
  const slipUrl    = slipUrlRaw.trim();
  const idToken    = typeof body.idToken === 'string' ? body.idToken.trim() : '';
  const idemKey    = typeof body.idempotencyKey === 'string' ? body.idempotencyKey.trim() : '';

  if (!purchaseId) return res.status(400).json({ ok: false, error: 'Missing purchaseId' });
  if (!slipUrl || !isOwnStorageUrl(slipUrl)) {
    return res.status(400).json({ ok: false, error: 'slipUrl must be a Firebase Storage URL for this project' });
  }
  if (!isValidIdempotencyKey(idemKey)) return res.status(400).json({ ok: false, code: 'IDEMPOTENCY', error: 'idempotencyKey has invalid format or length' });

  // Passes are a LINE-only product (Addendum 02 sec 9.4) — no guest path.
  let uid = null;
  try { if (idToken) uid = (await getAdminAuth().verifyIdToken(idToken)).uid; }
  catch { /* fall through */ }
  if (!uid) return res.status(403).json({ ok: false, code: 'AUTH', error: 'กรุณาเข้าสู่ระบบผ่าน LINE' });

  const ref = db.collection('pass_purchases').doc(purchaseId);
  let purchase;
  try {
    const snap = await ref.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Purchase not found' });
    purchase = snap.data();
  } catch (e) {
    console.error('[submit_pass_slip] read:', e.message);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }

  if (purchase.lineUserId !== uid) return res.status(403).json({ ok: false, error: 'ไม่ใช่รายการของบัญชีนี้' });

  const idemScope = `submit_pass_slip:${uid}`;
  const idemRef  = idempotencyRef(db, idemKey, idemScope);
  const idemFp   = fingerprintOf({ purchaseId, slipUrl, actor: uid });
  const response = { ok: true, paymentStatus: 'pending_review' };
  let replayed   = null;

  try {
    await db.runTransaction(async (t) => {
      const idem = await readIdempotencyInTx(t, idemRef, idemFp);
      if (idem.state === 'conflict') throw new Error('IDEMPOTENCY_CONFLICT');
      if (idem.state === 'replay')  { replayed = idem.response; return; }

      const snap = await t.get(ref);
      if (!snap.exists) throw new Error('GONE');
      const p = snap.data();
      if (p.issuedPackageId || p.paymentStatus === 'paid') throw new Error('ALREADY_SETTLED');
      if (p.status === 'rejected' || p.paymentStatus === 'rejected') throw new Error('REJECTED');
      t.update(ref, {
        status: 'pending_review', paymentStatus: 'pending_review',
        slipUrl, slipUploadedAt: FieldValue.serverTimestamp(),
        slipSubmittedVia: 'server',
        updatedAt: FieldValue.serverTimestamp(),
      });

      writeIdempotencyInTx(t, idemRef, { scope: idemScope, fingerprint: idemFp, response });
    });
  } catch (e) {
    if (e.message === 'IDEMPOTENCY_CONFLICT') {
      return res.status(409).json({ ok: false, code: 'IDEMPOTENCY_CONFLICT', error: 'idempotencyKey ถูกใช้กับคำขออื่นแล้ว' });
    }
    const map = {
      GONE:            [404, 'Purchase not found'],
      ALREADY_SETTLED: [409, 'รายการนี้ดำเนินการแล้ว'],
      REJECTED:        [409, 'รายการนี้ถูกปฏิเสธแล้ว'],
    };
    const [code, msg] = map[e.message] || [500, 'ส่งสลิปไม่สำเร็จ'];
    if (code === 500) console.error('[submit_pass_slip] tx:', e.message);
    return res.status(code).json({ ok: false, error: msg });
  }

  if (replayed) return res.status(200).json({ ...replayed, replayed: true });

  await writeAuditLog(db, {
    actor: uid, actorRole: 'customer', branchId: null,
    action: 'pass_slip_submitted', targetId: purchaseId,
    after: { paymentStatus: 'pending_review' },
    note: purchase.purchaseCode ?? null,
  });

  return res.status(200).json(response);
}

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method not allowed' });
  const body = parseBody(req);
  if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

  // ── Security Hotfix 2026-08-04: server-side slip submission ────────
  // Moves the protected mutation from the browser to this fail-closed action.
  if (body.action === 'submit_slip' || body.action === 'submit_pass_slip') {
    let db0;
    try { db0 = getAdminDb(); }
    catch (e) { console.error('[submit_slip] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }
    if (!(await serverSlipSubmitEnabled(db0))) {
      return res.status(403).json({ ok: false, code: 'DISABLED', error: 'Server slip submission is not enabled' });
    }
    return body.action === 'submit_slip'
      ? handleSubmitBookingSlip(req, res, body, db0)
      : handleSubmitPassSlip(req, res, body, db0);
  }

  // Stage D: pre-check for PASS PURCHASE slips (pass_purchases collection).
  // Separate handler so the Phase 1A booking path below stays untouched.
  if (body.targetType === 'pass_purchase') return handlePassSlipVerify(res, body);

  const bookingId   = typeof body.bookingId === 'string' ? body.bookingId.trim() : '';
  const bookingCode = typeof body.bookingCode === 'string' ? body.bookingCode.trim() : '';
  // Untrusted hints from the client — validated, stored as helper data only.
  const clientSlipHash = (typeof body.clientSlipHash === 'string' && SHA256_RE.test(body.clientSlipHash.trim()))
    ? body.clientSlipHash.trim().toLowerCase() : null;
  const clientQrPayload = (typeof body.clientQrPayload === 'string' && body.clientQrPayload.trim())
    ? body.clientQrPayload.trim().slice(0, 512) : null;

  if (!bookingId)   return res.status(400).json({ ok: false, error: 'Missing bookingId' });
  if (!bookingCode) return res.status(400).json({ ok: false, error: 'Missing bookingCode' });

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[slip-verify] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  // ── Read booking + preconditions (never write on paid/cancelled) ──
  const bookingRef = db.collection('bookings').doc(bookingId);
  let booking;
  try {
    const snap = await bookingRef.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Booking not found' });
    booking = snap.data();
  } catch (e) { console.error('[slip-verify] read:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  if (booking.bookingCode !== bookingCode) {
    return res.status(403).json({ ok: false, error: 'bookingCode mismatch' });
  }
  if (booking.bookingStatus === 'cancelled') {
    return res.status(200).json({ ok: true, skipped: 'cancelled' });
  }
  if (booking.paymentStatus === 'paid' || booking.paymentStatus === 'package') {
    return res.status(200).json({ ok: true, skipped: 'already_paid' });
  }
  if (booking.paymentStatus !== 'pending_review') {
    return res.status(409).json({ ok: false, error: `Slip not in review state (paymentStatus="${booking.paymentStatus}")` });
  }
  const slipUrl = typeof booking.slipUrl === 'string' ? booking.slipUrl : '';
  if (!slipUrl) return res.status(409).json({ ok: false, error: 'Booking has no slipUrl' });

  // ── Idempotency / cost guard: same slipUrl already checked → return it ──
  const prevPv = booking.paymentVerification;
  if (prevPv && prevPv.slipUrlChecked === slipUrl && prevPv.status && prevPv.status !== 'checking') {
    return res.status(200).json({ ok: true, verification: { status: prevPv.status, reason: prevPv.reason ?? null }, cached: true });
  }

  // ── Server-side hash (authoritative) ──────────────────────────────
  const hashResult = await computeServerSlipHash(slipUrl);
  const serverHash = hashResult.hash || null;
  if (!serverHash) console.warn(`[slip-verify] ${bookingCode} server hash unavailable: ${hashResult.error}`);

  // ── Untrusted transRef hint + advisory duplicate lookup ───────────
  const transRef = extractTransRef(clientQrPayload);
  let refDupBookingCode = null;
  if (transRef) {
    try {
      const dupSnap = await db.collection('bookings')
        .where('paymentVerification.transactionRef', '==', transRef)
        .limit(3).get();
      for (const d of dupSnap.docs) {
        if (d.id !== bookingId) { refDupBookingCode = d.data().bookingCode || d.id; break; }
      }
    } catch (e) { console.warn('[slip-verify] ref lookup failed (advisory only):', e.message); }
  }

  const expectedAmount = [booking.qrAmount, booking.finalPrice, booking.price]
    .map(Number).find(n => Number.isFinite(n) && n > 0) ?? null;
  const expectedReceiver = booking.qrType === 'special' ? RECEIVER_ALT
    : booking.qrType === 'late_night' ? null
    : RECEIVER_MAIN;

  // ── Transaction: registry (trusted hash only) + paymentVerification ──
  const hashRegRef = serverHash ? db.collection('slip_registry').doc(`hash_${serverHash}`) : null;
  let outcome; // { status, reason, dupOfBookingCode }
  try {
    outcome = await db.runTransaction(async (t) => {
      const bSnap = await t.get(bookingRef);
      const hSnap = hashRegRef ? await t.get(hashRegRef) : null;
      if (!bSnap.exists) throw new Error('GONE');
      const bNow = bSnap.data();
      // Paid/cancelled must never accept a verification update.
      if (bNow.bookingStatus === 'cancelled') throw new Error('CANCELLED');
      if (bNow.paymentStatus !== 'pending_review') throw new Error('BAD_STATE');

      let status = 'not_checked', reason = 'server_fetch_failed', dupOfBookingCode = null;
      if (serverHash && hSnap && hSnap.exists && hSnap.data().bookingId !== bookingId) {
        // Trusted, server-computed duplicate. Phase 1A stays conservative:
        // manual_review + urgent notify, never an automatic rejection.
        status = 'manual_review'; reason = 'duplicate_slip_hash';
        dupOfBookingCode = hSnap.data().bookingCode || hSnap.data().bookingId;
      } else if (refDupBookingCode) {
        // Untrusted client-decoded ref match → suspicion only.
        status = 'manual_review'; reason = 'suspected_duplicate_ref';
        dupOfBookingCode = refDupBookingCode;
      } else if (serverHash) {
        status = 'pre_verified'; reason = null;
      }

      // Registry entries are created ONLY from the server-computed hash.
      if (serverHash && !(hSnap && hSnap.exists)) {
        t.set(hashRegRef, {
          bookingId, bookingCode,
          source: 'server_sha256',
          createdAt: FieldValue.serverTimestamp(),
        });
      }

      // The ONLY booking field this route ever writes.
      t.update(bookingRef, {
        paymentVerification: {
          status,
          method: clientQrPayload ? 'slip_qr_decode' : 'none',
          expectedAmount,
          actualAmount: null,                 // Phase 2 (bank API) fills this
          expectedReceiver,
          actualReceiver: null,               // Phase 2 (bank API) fills this
          transactionRef: transRef,           // from CLIENT decode — untrusted
          refSource: transRef ? 'client_qr_decode_untrusted' : null,
          paidAt: null,                       // Phase 1A never asserts payment
          checkedAt: FieldValue.serverTimestamp(),
          reason,
          slipHash: serverHash,
          slipHashSource: serverHash ? 'server' : null,
          clientSlipHash,                     // helper data only
          slipUrlChecked: slipUrl,
          engine: 'slip-verify-1a',
        },
      });
      return { status, reason, dupOfBookingCode };
    });
  } catch (e) {
    if (e.message === 'GONE' || e.message === 'CANCELLED' || e.message === 'BAD_STATE') {
      return res.status(409).json({ ok: false, error: 'Booking state changed — verification not recorded' });
    }
    console.error('[slip-verify] tx:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to record verification' });
  }

  // ── Admin notification (exactly one per slip; idempotent per hash) ──
  // Admin still needs to Mark as Paid in Phase 1A, so every slip notifies —
  // the type just tells the admin how urgent it is. slipVerifyNotifications:
  // false (feature flag) silences this route entirely.
  let notify = { sent: 0, failed: 0, skipped: 0 };
  try {
    const flags = await loadNotificationFlags();
    if (flags.slipVerifyNotifications !== false) {
      const type = outcome.status === 'pre_verified' ? 'slip_precheck_admin'
                 : outcome.status === 'manual_review' ? 'slip_review_admin'
                 : 'slip_uploaded_admin';
      const hash8 = (serverHash || clientSlipHash || 'nohash').slice(0, 8);
      const payload = {
        bookingCode,
        customerName:  booking.customerName,
        customerPhone: booking.customerPhone,
        date: booking.date, startTime: booking.startTime, endTime: booking.endTime,
        expectedAmount,
        reasonText: REASON_TH[outcome.reason] || null,
        dupOfBookingCode: outcome.dupOfBookingCode || null,
      };
      const admins = await loadActiveAdmins();
      const results = await Promise.all(admins.map(a =>
        sendAndLog({
          eventId: `${bookingCode}_slipverify_${hash8}_${a.lineUserId}`,
          type, targetType: 'admin', lineUserId: a.lineUserId, bookingCode, payload,
        }).catch(e => ({ ok: false, status: 'failed', error: e.message }))
      ));
      notify.sent    = results.filter(r => r.ok && r.status === 'success').length;
      notify.skipped = results.filter(r => r.ok && r.status === 'skipped').length;
      notify.failed  = results.filter(r => !r.ok).length;
    } else {
      notify.skipped = -1; // flag-suppressed
    }
  } catch (e) {
    console.error('[slip-verify] notify:', e.message); // never fail the request
  }

  await writeAuditLog(db, {
    actor: 'system', actorRole: 'slip_verify',
    branchId: booking.branchId || 'ladprao1',
    action: 'slip_verify', targetId: bookingId,
    before: { paymentStatus: 'pending_review' },
    after:  { verificationStatus: outcome.status, reason: outcome.reason ?? null },
    note: bookingCode,
  });

  console.log(`[slip-verify] ${bookingCode} → ${outcome.status}${outcome.reason ? ` (${outcome.reason})` : ''} hash:${serverHash ? 'server' : 'none'} ref:${transRef ? 'yes' : 'no'}`);
  return res.status(200).json({
    ok: true,
    verification: { status: outcome.status, reason: outcome.reason ?? null },
    notify,
  });
}

// ════════════════════════════════════════════════════════════════════
// handlePassSlipVerify — Stage D pre-check for pass purchase slips.
// Same trust model as bookings: server re-hashes the file, slip_registry is
// shared with bookings (a slip reused across a booking AND a pass purchase is
// caught), verification NEVER issues a pass — admin approval remains the
// only path to package issuance.
// ════════════════════════════════════════════════════════════════════
async function handlePassSlipVerify(res, body) {
  const purchaseId   = typeof body.purchaseId === 'string' ? body.purchaseId.trim() : '';
  const purchaseCode = typeof body.purchaseCode === 'string' ? body.purchaseCode.trim() : '';
  const clientSlipHash = (typeof body.clientSlipHash === 'string' && SHA256_RE.test(body.clientSlipHash.trim()))
    ? body.clientSlipHash.trim().toLowerCase() : null;
  const clientQrPayload = (typeof body.clientQrPayload === 'string' && body.clientQrPayload.trim())
    ? body.clientQrPayload.trim().slice(0, 512) : null;

  if (!purchaseId)   return res.status(400).json({ ok: false, error: 'Missing purchaseId' });
  if (!purchaseCode) return res.status(400).json({ ok: false, error: 'Missing purchaseCode' });

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[slip-verify pass] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  const purchaseRef = db.collection('pass_purchases').doc(purchaseId);
  let purchase;
  try {
    const snap = await purchaseRef.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Purchase not found' });
    purchase = snap.data();
  } catch (e) { console.error('[slip-verify pass] read:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  if (purchase.purchaseCode !== purchaseCode) {
    return res.status(403).json({ ok: false, error: 'purchaseCode mismatch' });
  }
  if (purchase.status === 'rejected' || purchase.paymentStatus === 'rejected') {
    return res.status(200).json({ ok: true, skipped: 'rejected' });
  }
  if (purchase.paymentStatus === 'paid' || purchase.issuedPackageId) {
    return res.status(200).json({ ok: true, skipped: 'already_paid' });
  }
  if (purchase.paymentStatus !== 'pending_review') {
    return res.status(409).json({ ok: false, error: `Slip not in review state (paymentStatus="${purchase.paymentStatus}")` });
  }
  const slipUrl = typeof purchase.slipUrl === 'string' ? purchase.slipUrl : '';
  if (!slipUrl) return res.status(409).json({ ok: false, error: 'Purchase has no slipUrl' });

  const prevPv = purchase.paymentVerification;
  if (prevPv && prevPv.slipUrlChecked === slipUrl && prevPv.status && prevPv.status !== 'checking') {
    return res.status(200).json({ ok: true, verification: { status: prevPv.status, reason: prevPv.reason ?? null }, cached: true });
  }

  const hashResult = await computeServerSlipHash(slipUrl);
  const serverHash = hashResult.hash || null;
  if (!serverHash) console.warn(`[slip-verify pass] ${purchaseCode} server hash unavailable: ${hashResult.error}`);

  const transRef = extractTransRef(clientQrPayload);
  let refDupCode = null;
  if (transRef) {
    try {
      const [bDup, pDup] = await Promise.all([
        db.collection('bookings').where('paymentVerification.transactionRef', '==', transRef).limit(3).get(),
        db.collection('pass_purchases').where('paymentVerification.transactionRef', '==', transRef).limit(3).get(),
      ]);
      for (const d of bDup.docs) { refDupCode = d.data().bookingCode || d.id; break; }
      if (!refDupCode) for (const d of pDup.docs) {
        if (d.id !== purchaseId) { refDupCode = d.data().purchaseCode || d.id; break; }
      }
    } catch (e) { console.warn('[slip-verify pass] ref lookup failed (advisory only):', e.message); }
  }

  const expectedAmount = Number(purchase.price) > 0 ? Number(purchase.price) : null;
  const hashRegRef = serverHash ? db.collection('slip_registry').doc(`hash_${serverHash}`) : null;
  let outcome;
  try {
    outcome = await db.runTransaction(async (t) => {
      const pSnap = await t.get(purchaseRef);
      const hSnap = hashRegRef ? await t.get(hashRegRef) : null;
      if (!pSnap.exists) throw new Error('GONE');
      const pNow = pSnap.data();
      if (pNow.paymentStatus !== 'pending_review' || pNow.issuedPackageId) throw new Error('BAD_STATE');

      let status = 'not_checked', reason = 'server_fetch_failed', dupOfCode = null;
      if (serverHash && hSnap && hSnap.exists) {
        const owner = hSnap.data();
        const ownerId = owner.purchaseId || owner.bookingId || null;
        if (ownerId !== purchaseId) {
          status = 'manual_review'; reason = 'duplicate_slip_hash';
          dupOfCode = owner.purchaseCode || owner.bookingCode || ownerId;
        } else if (serverHash) {
          status = 'pre_verified'; reason = null;
        }
      } else if (refDupCode) {
        status = 'manual_review'; reason = 'suspected_duplicate_ref'; dupOfCode = refDupCode;
      } else if (serverHash) {
        status = 'pre_verified'; reason = null;
      }

      if (serverHash && !(hSnap && hSnap.exists)) {
        t.set(hashRegRef, {
          purchaseId, purchaseCode, kind: 'pass_purchase',
          source: 'server_sha256',
          createdAt: FieldValue.serverTimestamp(),
        });
      }
      t.update(purchaseRef, {
        paymentVerification: {
          status,
          method: clientQrPayload ? 'slip_qr_decode' : 'none',
          expectedAmount,
          actualAmount: null,
          expectedReceiver: RECEIVER_MAIN,   // pass QR always uses the main route
          actualReceiver: null,
          transactionRef: transRef,
          refSource: transRef ? 'client_qr_decode_untrusted' : null,
          paidAt: null,
          checkedAt: FieldValue.serverTimestamp(),
          reason,
          slipHash: serverHash,
          slipHashSource: serverHash ? 'server' : null,
          clientSlipHash,
          slipUrlChecked: slipUrl,
          engine: 'slip-verify-1a-pass',
        },
      });
      return { status, reason, dupOfCode };
    });
  } catch (e) {
    if (e.message === 'GONE' || e.message === 'BAD_STATE') {
      return res.status(409).json({ ok: false, error: 'Purchase state changed — verification not recorded' });
    }
    console.error('[slip-verify pass] tx:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to record verification' });
  }

  // One admin push per pass slip (admin still approves manually in Stage D).
  let notify = { sent: 0, failed: 0, skipped: 0 };
  try {
    const flags = await loadNotificationFlags();
    if (flags.slipVerifyNotifications !== false) {
      const isReview = outcome.status === 'manual_review';
      const type = isReview ? 'slip_review_admin' : 'pass_purchase_admin';
      const hash8 = (serverHash || clientSlipHash || 'nohash').slice(0, 8);
      const payload = isReview
        ? {
            bookingCode: purchaseCode,
            customerName: purchase.customerName, customerPhone: purchase.customerPhone,
            date: null, startTime: null, endTime: null,
            expectedAmount,
            reasonText: `${REASON_TH[outcome.reason] || 'ต้องตรวจสอบด้วยตนเอง'} (การซื้อแพ็กเกจ ${purchase.packageName || ''})`,
            dupOfBookingCode: outcome.dupOfCode || null,
          }
        : {
            bookingCode: purchaseCode, purchaseCode,
            customerName: purchase.customerName, customerPhone: purchase.customerPhone,
            packageName: purchase.packageName, price: purchase.price,
            precheckNote: outcome.status === 'pre_verified' ? 'ตรวจสลิปเบื้องต้นผ่าน ✓ (ไม่พบสลิปซ้ำ)' : null,
          };
      const admins = await loadActiveAdmins();
      const results = await Promise.all(admins.map(a =>
        sendAndLog({
          eventId: `${purchaseCode}_slipverify_${hash8}_${a.lineUserId}`,
          type, targetType: 'admin', lineUserId: a.lineUserId, bookingCode: purchaseCode, payload,
        }).catch(e => ({ ok: false, status: 'failed', error: e.message }))
      ));
      notify.sent    = results.filter(r => r.ok && r.status === 'success').length;
      notify.skipped = results.filter(r => r.ok && r.status === 'skipped').length;
      notify.failed  = results.filter(r => !r.ok).length;
    } else {
      notify.skipped = -1;
    }
  } catch (e) {
    console.error('[slip-verify pass] notify:', e.message);
  }

  await writeAuditLog(db, {
    actor: 'system', actorRole: 'slip_verify',
    branchId: 'ladprao1',
    action: 'slip_verify_pass', targetId: purchaseId,
    before: { paymentStatus: 'pending_review' },
    after:  { verificationStatus: outcome.status, reason: outcome.reason ?? null },
    note: purchaseCode,
  });

  console.log(`[slip-verify pass] ${purchaseCode} → ${outcome.status}${outcome.reason ? ` (${outcome.reason})` : ''} hash:${serverHash ? 'server' : 'none'}`);
  return res.status(200).json({
    ok: true,
    verification: { status: outcome.status, reason: outcome.reason ?? null },
    notify,
  });
}
