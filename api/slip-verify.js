// ════════════════════════════════════════════════════════════════════
// POST /api/slip-verify — Phase 1A slip pre-check (Auto Verify Slip)
// ════════════════════════════════════════════════════════════════════
// Called by index.html AFTER the existing slip-upload transaction has
// already set paymentStatus:"pending_review". This route:
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
// Public route (no session) — mirrors the slip-upload threat model: the
// caller must know both the Firestore doc id AND the bookingCode.
// Repeat calls for an unchanged slipUrl return the stored result without
// re-downloading or re-notifying (cost/spam guard).
// ════════════════════════════════════════════════════════════════════

import crypto from 'node:crypto';
import { getAdminDb, getAdminBucket, writeAuditLog } from './_lib/firebase-admin.js';
import { sendAndLog, loadActiveAdmins, loadNotificationFlags } from './_lib/notify.js';
import { FieldValue } from 'firebase-admin/firestore';

const MAX_SLIP_BYTES   = 6 * 1024 * 1024;             // client caps at 5MB; headroom
const ALLOWED_BUCKETS  = [
  'ultra-tennis-booking.firebasestorage.app',
  'ultra-tennis-booking.appspot.com',
];
const ALLOWED_PATH_RE  = /^(payment_slips|pass_slips)\//;
const SHA256_RE        = /^[a-f0-9]{64}$/i;

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

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method not allowed' });
  const body = parseBody(req);
  if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

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
