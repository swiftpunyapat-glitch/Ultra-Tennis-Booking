// ════════════════════════════════════════════════════════════════════
// POST /api/booking — customer booking route (Pricing v2)
// ════════════════════════════════════════════════════════════════════
//   action "price_quote" — READ-ONLY quote (Stage 1)
//   action "create"      — server-authoritative single-use create (Stage 2):
//       recomputes price with the engine (NEVER trusts client price),
//       validates holiday/promo/voucher, and writes bookings + booking_slots
//       (+ voucher.usedCount++) in ONE transaction with a double-booking guard.
//
// Standard guest creation is public but server-validated; signed customer,
// pass, cancellation and protected-read actions authenticate in their handlers.
// ════════════════════════════════════════════════════════════════════

import {
  getAdminDb, getAdminAuth, writeAuditLog,
  prepareGuestAccess, verifyGuestToken, revokeGuestAccess,
  GUEST_ACCESS_COLLECTION, GUEST_BOOKING_ID_MAX_LENGTH, GUEST_TOKEN_MAX_LENGTH,
  checkRateLimit, readRateLimitGate, RATE_LIMITS, clientIp,
  idempotencyRef, fingerprintOf, readIdempotencyInTx, writeIdempotencyInTx,
  isValidIdempotencyKey,
} from './_lib/firebase-admin.js';
import { computeQuote } from './_lib/pricing.js';
import {
  applyVoucherToQuote, evaluateVoucher, isVoucherV2,
  redeemVoucherUpdate, releaseVoucherUpdate, reserveVoucherUpdate,
} from './_lib/voucher-engine.js';
import { sendAndLog, loadActiveAdmins, loadNotificationFlags } from './_lib/notify.js';
import { FieldValue, Timestamp } from 'firebase-admin/firestore';
import { normalizeCustomVoucherCode } from './_lib/voucher-admin.js';
import { eventPassBookingError } from './_lib/event-pass-policy.js';
import {
  COACH_ADDON_V2_FLAG,
  calculateCoachAddonV2Price,
  coachAddonV2PackageKind,
  coachClaimCellStarts,
  coachClaimId,
  initialCoachAddonV2States,
  isCoachAddonV2Booking,
  isCoachAddonV2Duration,
} from './_lib/coach-addon-v2.js';
import { isActiveCoachClaim, releaseCoachAddonV2Hold } from './_lib/coach-addon-v2-store.js';

const RESOURCE_ID       = 'room1';
const DEFAULT_BRANCH_ID = 'ladprao1';
const PAY_MINS          = 15;    // payment window — mirrors index.html
const BUILD_VERSION     = '2026-07-22-p0.2';
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const TIME_RE = /^\d{2}:\d{2}$/;

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}
const normalizePhone = p => String(p || '').replace(/\D/g, '');
const slotIdOf   = (date, startTime) => `${RESOURCE_ID}_${date}_${String(startTime).replace(':', '')}`;
const nextHourEnd = (startTime) => {
  const n = parseInt(String(startTime).slice(0, 2), 10) + 1;
  return n >= 24 ? '00:00' : `${String(n).padStart(2, '0')}:00`;
};

// ════════════════════════════════════════════════════════════════════
// Phase B — 30-minute granularity (owner rules 2026-07):
//   • duration 30–180 min, step 30. Whole-hour durations start at :00 only;
//     x.5 durations may start at :00 (half at the end) or :30 (half first).
//   • every 30-min "half segment" costs a FLAT ฿200 — never promo, never
//     voucher. A booking containing a half prices its full hours with the
//     special promo DISABLED (single MAIN-account QR, no receiver mixing).
//   • Late Night (00:00–06:00) sells whole hours only; bookings containing a
//     half must sit entirely within 06:00–24:00.
//   • no half segment at/after 23:00 (the 23:00 round sells as 1 h only, so
//     no stranded 23:30–00:00 orphan). Nothing crosses midnight.
//   • slot docs: one per segment. Fully-covered clock hours keep the Phase A
//     hourly doc (`_HH00`, slotSpanMinutes 60 implied); halves write a
//     `slotSpanMinutes: 30` doc at `_HHMM`. Legacy docs (no field) = 60 min.
//   • kill switch: system_settings/features.enableHalfHourBooking. Half-hour
//     booking is a live product rule, so a missing field means ON; only an
//     explicit false disables it during an incident.
// ════════════════════════════════════════════════════════════════════
const HALF_HOUR_PRICE     = 200;
const MAX_DURATION_MIN    = 180;
const HALF_EARLIEST_MIN   = 6 * 60;    // halves exist only from 06:00…
const HALF_LATEST_MIN     = 23 * 60;   // …and never start at/after 23:00

const toMin  = t => { const [h, m] = String(t).split(':').map(Number); return h * 60 + (m || 0); };
const toHHMM = min => min >= 1440 ? '00:00' : `${String(Math.floor(min / 60)).padStart(2, '0')}:${String(min % 60).padStart(2, '0')}`;

// durationMinutes from the request. Accepts legacy durationHours (×60) so old
// clients keep working. Default 60. Null = invalid.
function parseDurationMinutes(body) {
  let raw = body.durationMinutes;
  if (raw === undefined || raw === null || raw === '') {
    const h = body.durationHours;
    if (h === undefined || h === null || h === '') return 60;
    raw = Number(h) * 60;
  }
  const n = Number(raw);
  return (Number.isInteger(n) && n >= 30 && n <= MAX_DURATION_MIN && n % 30 === 0) ? n : null;
}

// Segment a booking range into slot docs: span-60 for fully covered clock
// hours, span-30 for the half. Null when the shape is invalid.
function segmentsOf(startTime, durMin) {
  const s = toMin(startTime);
  const m0 = s % 60;
  if (m0 !== 0 && m0 !== 30) return null;
  const end = s + durMin;
  if (end > 1440) return null;                     // never cross midnight
  if (durMin % 60 === 0 && m0 !== 0) return null;  // whole hours start :00
  const segs = [];
  let t = s;
  while (t < end) {
    if (t % 60 === 0 && t + 60 <= end) { segs.push({ start: toHHMM(t), span: 60 }); t += 60; }
    else                               { segs.push({ start: toHHMM(t), span: 30 }); t += 30; }
  }
  return segs;
}
const endTimeAfterMin = (startTime, durMin) => toHHMM(toMin(startTime) + durMin);

// Owner placement rules for bookings that contain a half segment.
// Returns a customer-facing error string, or null when the shape is fine.
function halfPlacementError(segs) {
  const halves = segs.filter(x => x.span === 30);
  if (!halves.length) return null;
  const first = toMin(segs[0].start);
  const last  = segs[segs.length - 1];
  if (first < HALF_EARLIEST_MIN) return 'ช่วง Late Night จองเป็นชั่วโมงเต็มเท่านั้น';
  if (halves.some(x => toMin(x.start) >= HALF_LATEST_MIN)) return 'รอบ 23:00 ขายเป็นชั่วโมงเต็มเท่านั้น';
  if (toMin(last.start) + last.span > 1440) return 'เกินเที่ยงคืน — เลือกเวลาเริ่มให้เร็วขึ้น';
  return null;
}

// Half-hour feature flag — live by default. This used to fail closed, which
// made every 30/90/150-minute request fail whenever the settings document had
// not been seeded (or an older deployment omitted the field).
async function halfHourEnabled(db) {
  try {
    const snap = await db.collection('system_settings').doc('features').get();
    return !(snap.exists && snap.data().enableHalfHourBooking === false);
  } catch (e) {
    console.warn('[half flag] read failed → ON (live default):', e.message);
    return true;
  }
}

// PromptPay receiver for a qrType — special promo pays the ALT account; all
// other types pay MAIN. Segments in one booking must share ONE receiver.
const receiverOf = qrType => (qrType === 'special' ? 'alt' : 'main');

// Admin-configurable half price (system_settings/pricing.halfHourPrice) with
// the flat ฿200 default. Bounds mirror admin-user-action's save validation.
function halfPriceFrom(pricingData) {
  const n = Number(pricingData?.halfHourPrice);
  return (Number.isInteger(n) && n >= 100 && n <= 1000) ? n : HALF_HOUR_PRICE;
}

function flatHalfMetadata(date, start, isHoliday) {
  const [y, m, d] = String(date).split('-').map(Number);
  const dow = new Date(Date.UTC(y, m - 1, d)).getUTCDay();
  const hour = parseInt(String(start).slice(0, 2), 10);
  const startMs = Date.parse(`${date}T${start}:00+07:00`);
  return {
    priceRuleVersion: 'half-hour-flat-v1',
    isHoliday: isHoliday === true,
    isWeekend: dow === 0 || dow === 6,
    isMorningWeekday: dow >= 1 && dow <= 5 && isHoliday !== true && hour >= 6 && hour <= 11,
    advanceHours: Number.isFinite(startMs) ? (startMs - Date.now()) / 3_600_000 : null,
  };
}

// Quote-shaped object for one flat-price half segment. Keep every metadata
// field defined: Firestore rejects `undefined`, which previously made a pure
// 30-minute create fail even though price_quote correctly returned ฿200.
const halfSegQuote = (start, price = HALF_HOUR_PRICE, meta = {}) => ({
  pricingType: 'half_hour', originalPrice: price, finalPrice: price,
  price, amount: price, qrAmount: price,
  qrType: 'normal', promoCode: null, voucherCode: null, discountAmount: 0,
  voucherApplied: false, voucherReason: null,
  priceRuleVersion: meta.priceRuleVersion || 'half-hour-flat-v1',
  isHoliday: meta.isHoliday === true,
  isWeekend: meta.isWeekend === true,
  isMorningWeekday: meta.isMorningWeekday === true,
  advanceHours: Number.isFinite(meta.advanceHours) ? meta.advanceHours : null,
  startTime: start, span: 30,
});

// Sum per-segment quotes into one quote. Throws {code:'MIXED_RECEIVER'} when
// segments would need different PromptPay accounts (can't pay with one QR).
function combineQuotes(segQuotes) {
  const receivers = new Set(segQuotes.map(q => receiverOf(q.qrType)));
  if (receivers.size > 1) {
    const err = new Error('MIXED_RECEIVER'); err.code = 'MIXED_RECEIVER'; throw err;
  }
  const total     = segQuotes.reduce((s, q) => s + q.finalPrice, 0);
  const totalOrig = segQuotes.reduce((s, q) => s + q.originalPrice, 0);
  const allSame   = segQuotes.every(q => q.qrType === segQuotes[0].qrType);
  return {
    finalPrice: total, originalPrice: totalOrig, qrAmount: total,
    price: total, amount: total,
    qrType: allSame ? segQuotes[0].qrType : 'normal',
    pricingType: segQuotes.every(q => q.pricingType === segQuotes[0].pricingType)
      ? segQuotes[0].pricingType : 'multi_rate',
    breakdown: segQuotes.map(q => ({
      startTime: q.startTime, endTime: toHHMM(toMin(q.startTime) + (q.span || 60)),
      price: q.finalPrice, pricingType: q.pricingType, qrType: q.qrType,
    })),
  };
}

// Segments a stored booking occupies — durationMinutes when present (Phase B),
// else legacy hourly docs from durationHours (Phase A / older).
function bookingSegments(booking) {
  if (!booking?.date || !booking?.startTime) return [];
  const dm = Number(booking.durationMinutes);
  if (Number.isInteger(dm) && dm >= 30 && dm % 30 === 0) {
    return segmentsOf(booking.startTime, Math.min(dm, 360)) || [];
  }
  const n  = parseInt(booking.durationHours, 10);
  const nH = (Number.isInteger(n) && n >= 1 && n <= 6) ? n : 1;
  const h0 = parseInt(String(booking.startTime).slice(0, 2), 10);
  if (!Number.isFinite(h0)) return [];
  const segs = [];
  for (let i = 0; i < nH && h0 + i < 24; i++) segs.push({ start: `${String(h0 + i).padStart(2, '0')}:00`, span: 60 });
  return segs;
}

function isOccupiedSlot(slot, nowMs = Date.now()) {
  if (!slot) return false;
  // A terminal/released slot must never stay occupied merely because its
  // historical paymentStatus was "paid" or "package".  Pass reschedules used
  // to leave { bookingStatus:"rescheduled", paymentStatus:"package" }, which
  // permanently blocked the old hour even after its private claim was removed.
  if (['cancelled', 'rescheduled', 'expired', 'completed', 'no_show'].includes(slot.bookingStatus)) return false;
  if (slot.bookingStatus === 'confirmed' || slot.bookingStatus === 'pending_review') return true;
  if (['paid', 'package', 'pending_review'].includes(slot.paymentStatus)) return true;
  if (slot.bookingStatus === 'pending_payment') {
    const exp = slot.expiresAt?.toMillis?.() ?? 0;
    return !exp || exp > nowMs;
  }
  return false;
}
function genBookingCode() {
  const t = Date.now().toString(36).toUpperCase().slice(-5);
  const r = Math.random().toString(36).toUpperCase().slice(2, 4);
  return `UT${t}${r}`;
}
function mapVoucherReason(r) {
  return ({
    not_found:      'ไม่พบโค้ดส่วนลด',
    inactive:       'โค้ดถูกปิดใช้งาน',
    expired:        'โค้ดหมดอายุแล้ว',
    used_up:        'โค้ดถูกใช้ครบแล้ว',
    not_applicable: 'โค้ดใช้กับราคาหรือโปรโมชั่นนี้ไม่ได้',
    wrong_owner:    'โค้ดนี้ไม่ใช่ของบัญชีนี้',
    reserved:       'โค้ดนี้ถูกล็อกไว้กับการจองอื่นชั่วคราว',
    not_started:    'แคมเปญ Voucher นี้ยังไม่เริ่ม',
    day_not_allowed:'Voucher นี้ใช้ไม่ได้ในวันที่เลือก',
    holiday_not_allowed: 'Voucher นี้ใช้ไม่ได้ในวันหยุด',
    time_not_allowed: 'Voucher นี้ใช้ไม่ได้ในช่วงเวลาที่เลือก',
    duration_not_allowed: 'Voucher นี้ใช้ไม่ได้กับระยะเวลาที่เลือก',
    branch_not_allowed: 'Voucher นี้ใช้ไม่ได้ที่สาขานี้',
    resource_not_allowed: 'Voucher นี้ใช้ไม่ได้กับสนามนี้',
    campaign_not_found: 'ไม่พบข้อมูลแคมเปญของ Voucher นี้',
    invalid_type:   'ประเภท Voucher ไม่ถูกต้อง',
    line_login_required: 'กรุณาเปิด Voucher ผ่าน LINE และเข้าสู่ระบบก่อน',
  })[r] || 'โค้ดส่วนลดไม่ถูกต้อง';
}

async function loadVoucherBundle(db, code) {
  if (!code) return { voucher: null, campaign: null, campaignRef: null };
  const voucherSnap = await db.collection('vouchers').doc(code).get();
  if (!voucherSnap.exists) return { voucher: null, campaign: null, campaignRef: null };
  const voucher = voucherSnap.data();
  const campaignRef = voucher.campaignId
    ? db.collection('voucher_campaigns').doc(String(voucher.campaignId))
    : null;
  const campaignSnap = campaignRef ? await campaignRef.get() : null;
  return {
    voucher,
    campaign: campaignSnap?.exists ? { id: campaignSnap.id, ...campaignSnap.data() } : null,
    campaignRef,
  };
}

function quoteWithVoucher(baseQuote, bundle, context) {
  if (!context.voucherCode) return baseQuote;
  const result = evaluateVoucher({
    voucher: bundle.voucher, campaign: bundle.campaign, code: context.voucherCode,
    nowMs: context.nowMs, lineUserId: context.lineUserId,
    date: context.date, startTime: context.startTime,
    durationMinutes: context.durationMinutes, isHoliday: context.isHoliday,
    branchId: DEFAULT_BRANCH_ID, resourceId: RESOURCE_ID, baseQuote,
    bookingId: context.bookingId || null,
  });
  return applyVoucherToQuote(baseQuote, result);
}

// ════════════════════════════════════════════════════════════════════
// booking_slots sanitation contract — Security Hotfix 2026-08-04 (SL-02)
// ════════════════════════════════════════════════════════════════════
// booking_slots stays PUBLICLY READABLE after the rules cutover because the
// availability grid is unauthenticated by product design. Firestore rules
// cannot filter fields — `allow read` is all-or-nothing per document — so
// the ONLY control over what leaks is what we write. Every server write to
// booking_slots must go through writeSlotDoc().
//
// Owner decision SL-02: coachId must NOT appear in the public slot contract,
// and no PII of any kind may be written here.
//
// bookingId / bookingCode: retained. Existing ownership checks across the
// codebase compare slot.bookingId / slot.bookingCode when releasing or
// confirming a slot (see handleCancelPending below, admin approve_slip and
// refund). Dropping them would silently break slot release. They are already
// public on every legacy document, so this is the residual risk the owner
// accepted — NOT a new exposure. Flagged for the V2 slot redesign.
// Review remediation RB-10: bookingId and bookingCode are no longer written
// to the public document. What remains is availability data and nothing
// else — a reader learns that an hour is taken, never whose it is.
const SLOT_FIELD_ALLOWLIST = new Set([
  'date', 'hour', 'resourceId', 'slotSpanMinutes', 'branchId',
  'bookingStatus', 'paymentStatus', 'expiresAt',
]);

// Fields that must never reach the public collection under any circumstance.
// Asserted rather than silently dropped, so a future call site that tries
// fails loudly in tests instead of leaking quietly in production.
const SLOT_FORBIDDEN_FIELDS = [
  'bookingId', 'bookingCode', 'coachId',
  'customerName', 'customerPhone', 'customerPhoneNormalized',
  'lineUserId', 'lineDisplayName', 'customerNote', 'slipUrl',
  'price', 'amount', 'createdBy', 'paidBy',
];

// Strips anything outside the allowlist, and throws if a caller passed a
// field that must never be public. Dropping those silently would let a bad
// call site look correct while leaking; throwing makes it fail in tests.
export function slotDocPayload(fields) {
  for (const banned of SLOT_FORBIDDEN_FIELDS) {
    if (banned in fields) {
      throw new Error(`SLOT_CONTRACT_VIOLATION: "${banned}" must not be written to booking_slots`);
    }
  }
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    if (SLOT_FIELD_ALLOWLIST.has(k) && v !== undefined) out[k] = v;
  }
  return out;
}

// The ownership linkage moved here. booking_slot_claims is server-only —
// the rules deny it to every client — so the identifiers that used to sit
// on the public slot document now live somewhere only the Admin SDK reads.
//
// Same deterministic id as the public slot, so a claim is always a direct
// get: no query, no index.
const SLOT_CLAIMS_COLLECTION = 'booking_slot_claims';
export const slotClaimRef = (db, slotId) => db.collection(SLOT_CLAIMS_COLLECTION).doc(slotId);

// Writes the public availability document and the private claim together.
// `t` is a transaction — the pair must never be written separately, or a
// slot could be occupied with nothing recording who occupied it.
function writeSlotDoc(t, db, slotId, publicFields, claimFields) {
  const publicRef = db.collection('booking_slots').doc(slotId);
  t.set(publicRef, slotDocPayload(publicFields));
  t.set(slotClaimRef(db, slotId), {
    bookingId:   claimFields.bookingId,
    bookingCode: claimFields.bookingCode,
    ...(claimFields.coachId ? { coachId: claimFields.coachId } : {}),
    branchId: publicFields.branchId || DEFAULT_BRANCH_ID,
    resourceId: publicFields.resourceId || RESOURCE_ID,
    status: publicFields.bookingStatus,
    date: publicFields.date, hour: publicFields.hour,
    slotSpanMinutes: publicFields.slotSpanMinutes ?? 60,
    createdAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  });
  return publicRef;
}

// Booking fields a guest is allowed to see. Everything else — including
// lineUserId, pass/credit/voucher state and internal pricing metadata —
// is withheld (Addendum 02 §3.2).
function guestBookingProjection(id, b) {
  return {
    id,
    bookingCode:   b.bookingCode ?? null,
    date:          b.date ?? null,
    startTime:     b.startTime ?? null,
    endTime:       b.endTime ?? null,
    durationMinutes: b.durationMinutes ?? null,
    bookingType:   b.bookingType ?? null,
    bookingStatus: b.bookingStatus ?? null,
    paymentStatus: b.paymentStatus ?? null,
    price:         Number(b.price) || 0,
    qrType:        b.qrType ?? null,
    qrAmount:      Number(b.qrAmount) || 0,
    customerName:  b.customerName ?? null,   // their own name, on their own booking
    paymentExpiresAt: b.paymentExpiresAt?.toDate?.()?.toISOString?.() ?? null,
    slipUploadedAt:   b.slipUploadedAt?.toDate?.()?.toISOString?.() ?? null,
    hasSlip:       !!b.slipUrl,              // boolean only — never the URL (SEC-06)
  };
}

// guest_booking — a guest reads THEIR OWN booking using the capability
// token handed back when the booking was created. Replaces the direct
// Firestore read that the hardened rules deny.
async function handleGuestBooking(req, res, body) {
  // RB-03/RB-06: access documents are keyed by bookingId, so the caller
  // supplies both. The token proves entitlement; the id only locates.
  const bookingIdRaw = typeof body.bookingId === 'string' ? body.bookingId : '';
  const tokenRaw     = typeof body.guestToken === 'string' ? body.guestToken : '';
  if (bookingIdRaw.length > GUEST_BOOKING_ID_MAX_LENGTH || tokenRaw.length > GUEST_TOKEN_MAX_LENGTH) {
    return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Credential is too long' });
  }
  const bookingId = bookingIdRaw.trim();
  const token     = tokenRaw.trim();
  const ip        = clientIp(req);

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[guest_booking] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  if (!bookingId || !token) return res.status(400).json({ ok: false, code: 'TOKEN', error: 'Missing credentials' });

  // Check the global invalid-IP bucket before touching the capability record
  // or creating any IP+booking limiter. Rotating bookingId cannot bypass it.
  const globalGate = await readRateLimitGate(db, {
    bucket: 'guestInvalid', key: ip,
  });
  if (!globalGate.allowed) {
    res.setHeader('Retry-After', String(globalGate.retryAfterSec));
    return res.status(429).json({ ok: false, code: 'RATE_LIMIT', error: 'Too many requests' });
  }

  const v = await verifyGuestToken(db, bookingId, token, 'booking:read');
  if (!v.ok) {
    // Invalid attempts are counted separately and trigger the 60-minute
    // lockout (GT-02). Keyed on IP so guessing is throttled per source.
    const bad = await checkRateLimit(db, {
      bucket: 'guestInvalid', key: ip, ...RATE_LIMITS.guestInvalid,
    });
    if (!bad.allowed) {
      res.setHeader('Retry-After', String(bad.retryAfterSec));
      return res.status(429).json({ ok: false, code: 'RATE_LIMIT', error: 'Too many attempts' });
    }
    return res.status(401).json({ ok: false, code: 'TOKEN', error: 'ลิงก์ไม่ถูกต้องหรือหมดอายุ' });
  }

  // Valid capability requests retain the existing per-IP+booking read rate.
  const readGate = await checkRateLimit(db, {
    bucket: 'guestRead', key: `${ip}|${bookingId}`, ...RATE_LIMITS.guestRead,
  });
  if (!readGate.allowed) {
    res.setHeader('Retry-After', String(readGate.retryAfterSec));
    return res.status(429).json({ ok: false, code: 'RATE_LIMIT', error: 'Too many requests' });
  }

  try {
    const snap = await db.collection('bookings').doc(bookingId).get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Booking not found' });
    return res.status(200).json({ ok: true, booking: guestBookingProjection(snap.id, snap.data()) });
  } catch (e) {
    console.error('[guest_booking] read:', e.message);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
}

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method not allowed' });
  const body = parseBody(req);
  if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

  // Guest capability token (Security Hotfix 2026-08-04)
  if (body.action === 'guest_booking') return handleGuestBooking(req, res, body);

  if (body.action === 'price_quote')   return handlePriceQuote(res, body);
  if (body.action === 'create')        return handleCreate(req, res, body);
  if (body.action === 'event_pass_redeem') return handleEventPassRedeem(req, res, body);
  if (body.action === 'event_pass_status') return handleEventPassStatus(res, body);
  // Server-side pass booking (Security Hotfix 2026-08-04 — closes SEC-02).
  // Gated by system_settings/features.useServerPassBooking. The cutover client
  // has no direct-write fallback, so a disabled action fails closed.
  if (body.action === 'create_pass_booking') {
    let db;
    try { db = getAdminDb(); }
    catch (e) { console.error('[create_pass_booking] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }
    if (!(await serverPassBookingEnabled(db))) {
      return res.status(403).json({ ok: false, code: 'DISABLED', error: 'Server pass booking is not enabled' });
    }
    return handleCreatePassBooking(res, body);
  }
  // RB-01: handleCancelPending needs `req` for the rate limiter's client IP.
  // The first version referenced `req` without it being in scope, which threw
  // a ReferenceError on every guest cancellation.
  if (body.action === 'cancel_pending') return handleCancelPending(req, res, body);
  if (body.action === 'features')      return handleFeatures(res);
  // Coach lesson booking (Stage 3) — customer-facing, feature-flagged OFF by
  // default via system_settings/features.enableCoachBookingCustomer.
  if (body.action === 'coach_options')       return handleCoachOptions(res);
  if (body.action === 'coach_slots')         return handleCoachSlots(res, body);
  if (body.action === 'create_coach_lesson') return handleCreateCoachLesson(res, body);
  // Coach Add-on v2 is an additive path.  Every action independently checks
  // enableCoachAddonV2; the legacy court and coach actions above are untouched.
  if (body.action === 'coach_addon_v2_options') return handleCoachAddonV2Options(res, body);
  if (body.action === 'coach_addon_v2_quote')   return handleCoachAddonV2Quote(res, body);
  if (body.action === 'create_coach_addon_v2') return handleCreateCoachAddonV2(req, res, body);
  if (body.action === 'expire_coach_addon_v2') return handleExpireCoachAddonV2(req, res, body);
  // Pass self-purchase (Stage D) — LIVE (on by default); kill-switch:
  // system_settings/features.enablePassSelfPurchase = false.
  if (body.action === 'pass_catalog')         return handlePassCatalog(res);
  if (body.action === 'create_pass_purchase') return handleCreatePassPurchase(res, body);
  return res.status(400).json({ ok: false, error: `Unknown action "${body.action}"` });
}

async function eventPassUser(idToken) {
  if (!idToken || typeof idToken !== 'string') return null;
  try { return (await getAdminAuth().verifyIdToken(idToken.trim())).uid || null; }
  catch { return null; }
}

async function handleEventPassRedeem(req, res, body) {
  const normalized = normalizeCustomVoucherCode(body.code);
  if (!normalized.ok) return res.status(400).json({ ok: false, code: 'INVALID_CODE', error: 'รูปแบบ Event Code ไม่ถูกต้อง' });
  const uid = await eventPassUser(body.idToken);
  if (!uid) return res.status(403).json({ ok: false, code: 'AUTH', error: 'กรุณาเปิดผ่าน LINE เพื่อรับ Event Pass' });
  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[event_pass_redeem] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  // Auto approval turns possession of a code into an immediate entitlement,
  // so bound guessing attempts by both authenticated LINE user and source IP.
  const redeemGate = await checkRateLimit(db, {
    bucket: 'eventPassRedeem', key: `${uid}|${clientIp(req)}`, ...RATE_LIMITS.eventPassRedeem,
  });
  if (!redeemGate.allowed) {
    res.setHeader('Retry-After', String(redeemGate.retryAfterSec));
    return res.status(429).json({ ok: false, code: 'RATE_LIMIT', error: 'ลองใช้ Event Code หลายครั้งเกินไป กรุณารอสักครู่' });
  }

  const voucherRef = db.collection('vouchers').doc(normalized.code);
  const userRef = db.collection('registered_users').doc(uid);
  const requestRef = db.collection('event_pass_requests').doc();
  const packageRef = db.collection('customer_packages').doc();
  let result;
  try {
    await db.runTransaction(async transaction => {
      const [voucherSnap, userSnap] = await Promise.all([transaction.get(voucherRef), transaction.get(userRef)]);
      if (!voucherSnap.exists) throw new Error('NOT_FOUND');
      if (!userSnap.exists) throw new Error('REGISTER_FIRST');
      const voucher = voucherSnap.data();
      const campaignRef = voucher.campaignId ? db.collection('voucher_campaigns').doc(String(voucher.campaignId)) : null;
      if (!campaignRef) throw new Error('CAMPAIGN_MISSING');
      const campaignSnap = await transaction.get(campaignRef);
      if (!campaignSnap.exists) throw new Error('CAMPAIGN_MISSING');
      const campaign = campaignSnap.data();
      if (campaign.voucherType !== 'event_pass') throw new Error('WRONG_TYPE');
      if (campaign.active !== true || voucher.active !== true) throw new Error('INACTIVE');
      const now = Date.now();
      const validFrom = campaign.validFrom?.toMillis?.() ?? null;
      const expiresAt = campaign.expiresAt?.toMillis?.() ?? null;
      if (validFrom && now < validFrom) throw new Error('NOT_STARTED');
      if (!expiresAt || now > expiresAt) throw new Error('EXPIRED');
      if (voucher.state === 'pending_approval' && voucher.issuedTo === uid && voucher.pendingRequestId) {
        result = { requestId: voucher.pendingRequestId, status: 'pending', replayed: true };
        return;
      }
      if (voucher.state === 'redeemed' && voucher.issuedTo === uid && voucher.redeemedRequestId && voucher.redeemedPackageId) {
        result = {
          requestId: voucher.redeemedRequestId, packageId: voucher.redeemedPackageId,
          status: 'approved', autoApproved: true, replayed: true,
        };
        return;
      }
      if ((voucher.state || 'available') !== 'available' || Number(voucher.usedCount) > 0) throw new Error('UNAVAILABLE');
      const user = userSnap.data();
      const approvalMode = campaign.eventPassApprovalMode === 'manual' ? 'manual' : 'auto';
      const requestPayload = {
        code: normalized.code, campaignId: voucher.campaignId,
        status: approvalMode === 'auto' ? 'approved' : 'pending', lineUserId: uid,
        lineDisplayName: String(body.lineDisplayName || user.lineDisplayName || '').slice(0, 120),
        customerName: String(user.name || '').slice(0, 160),
        customerPhone: String(user.phone || '').slice(0, 40),
        assignedName: voucher.assignedName || '', assignedDraw: voucher.assignedDraw || '',
        assignedNickname: voucher.assignedNickname || '',
        createdAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
        ...(approvalMode === 'auto' ? {
          issuedPackageId: packageRef.id,
          reviewedAt: FieldValue.serverTimestamp(), reviewedBy: 'SYSTEM_AUTO',
        } : {}),
      };
      transaction.create(requestRef, requestPayload);

      if (approvalMode === 'manual') {
        transaction.update(voucherRef, {
          state: 'pending_approval', issuedTo: uid, pendingRequestId: requestRef.id,
          pendingAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
        });
        result = { requestId: requestRef.id, status: 'pending', autoApproved: false, replayed: false };
        return;
      }

      transaction.create(packageRef, {
        lineUserId: uid,
        customerName: user.name || '', customerPhone: user.phone || '',
        customerPhoneNormalized: normalizePhone(user.phone),
        lineDisplayName: String(body.lineDisplayName || user.lineDisplayName || '').slice(0, 120),
        packageType: 'monstr_event_pass', packageName: campaign.name || 'Event Pass',
        price: 0, ownerRole: 'customer', totalMinutes: 60, remainingMinutes: 60,
        validityDays: null, validFrom: FieldValue.serverTimestamp(), validUntil: campaign.expiresAt,
        status: 'active', isEventPass: true,
        restrictDays: Array.isArray(campaign.allowedDays) ? campaign.allowedDays : [1, 2, 3, 4, 5],
        branchId: campaign.branchId || DEFAULT_BRANCH_ID,
        resourceId: campaign.resourceId || RESOURCE_ID,
        excludeHolidays: campaign.excludeHolidays === true,
        exactDurationMinutes: 60, eventUsedAt: null,
        eventName: campaign.name || 'Event Pass',
        sourceVoucherCode: normalized.code, sourceEventPassRequestId: requestRef.id,
        addedByAdmin: 'SYSTEM_AUTO', source: 'event_code_auto_approved',
        createdAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
        weeklyUsage: {}, monthlyUsage: {}, note: '',
      });
      transaction.update(voucherRef, {
        state: 'redeemed', usedCount: 1, issuedTo: uid,
        redeemedBy: uid, redeemedAt: FieldValue.serverTimestamp(),
        redeemedPackageId: packageRef.id, redeemedRequestId: requestRef.id,
        updatedAt: FieldValue.serverTimestamp(), updatedBy: 'SYSTEM_AUTO',
      });
      result = {
        requestId: requestRef.id, packageId: packageRef.id,
        status: 'approved', autoApproved: true, replayed: false,
        packageName: campaign.name || 'Event Pass', expiresAtMs: expiresAt,
      };
    });

    if (result.autoApproved && !result.replayed) {
      await writeAuditLog(db, {
        actor: uid, actorRole: 'customer', branchId: DEFAULT_BRANCH_ID,
        action: 'event_pass_auto_approve', targetId: result.requestId,
        before: { status: 'available' },
        after: { status: 'approved', packageId: result.packageId, code: normalized.code },
      });
      try {
        await sendAndLog({
          eventId: `${normalized.code}_event_pass_activated_customer`,
          type: 'pass_activated_customer', targetType: 'customer',
          lineUserId: uid, bookingCode: normalized.code,
          payload: {
            packageName: result.packageName, remainingMinutes: 60,
            validUntil: new Date(result.expiresAtMs).toLocaleDateString('en-GB', { timeZone: 'Asia/Bangkok' }),
          },
        });
      } catch (e) { console.error('[event_pass_auto_approve] notify (non-fatal):', e.message); }
    }
    return res.status(200).json({ ok: true, code: normalized.code, ...result });
  } catch (e) {
    const map = {
      NOT_FOUND: [404, 'ไม่พบ Event Code นี้'], REGISTER_FIRST: [409, 'กรุณาลงทะเบียนชื่อและเบอร์โทรก่อนรับ Event Pass'],
      CAMPAIGN_MISSING: [409, 'ไม่พบแคมเปญของ Event Code'], WRONG_TYPE: [409, 'Code นี้ไม่ใช่ Event Pass'],
      INACTIVE: [409, 'Event Code ถูกปิดใช้งาน'], NOT_STARTED: [409, 'Event Pass ยังไม่เริ่มใช้งาน'],
      EXPIRED: [409, 'Event Code หมดอายุแล้ว'], UNAVAILABLE: [409, 'Event Code นี้ถูกส่งตรวจหรือใช้ไปแล้ว'],
    };
    const [status, error] = map[e.message] || [500, 'ไม่สามารถส่ง Event Code ได้'];
    if (status === 500) console.error('[event_pass_redeem]', e.message);
    return res.status(status).json({ ok: false, error });
  }
}

async function handleEventPassStatus(res, body) {
  const uid = await eventPassUser(body.idToken);
  if (!uid) return res.status(403).json({ ok: false, code: 'AUTH', error: 'กรุณาเปิดผ่าน LINE' });
  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[event_pass_status] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }
  try {
    const snap = await db.collection('event_pass_requests').where('lineUserId', '==', uid).limit(20).get();
    const requests = snap.docs.map(doc => {
      const value = doc.data();
      return {
        id: doc.id, code: value.code || '', status: value.status || 'pending',
        issuedPackageId: value.issuedPackageId || null, codeReturned: value.codeReturned === true,
        createdAt: value.createdAt?.toDate?.()?.toISOString?.() ?? null,
        reviewedAt: value.reviewedAt?.toDate?.()?.toISOString?.() ?? null,
      };
    }).sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
    return res.status(200).json({ ok: true, requests });
  } catch (e) {
    console.error('[event_pass_status]', e.message);
    return res.status(500).json({ ok: false, error: 'ไม่สามารถโหลดสถานะ Event Pass ได้' });
  }
}

// ── Pass self-purchase catalog — SERVER-AUTHORITATIVE prices. Only clearly
// systematized passes are sellable online; Beginner Coaching ("from ฿4,000",
// varies by coach) stays contact-admin by business decision.
const PASS_CATALOG = {
  ultra_starter_3: { packageName: 'Ultra Starter',       price: 999  },
  ultra_pass_10:   { packageName: 'Ultra Pass 10 Hours', price: 3100 },
  ultra_pass_20:   { packageName: 'Ultra Pass 20 Hours', price: 5900 },
  offpeak:         { packageName: 'Off-Peak Pass',       price: 3600 },
};

// Security Hotfix 2026-08-04 kill switch. Fails CLOSED: until the flag is
// explicitly true the endpoint refuses, so Deployment A ships the code
// without changing any behaviour (Hotfix Plan, Stage A).
async function serverPassBookingEnabled(db) {
  try {
    const snap = await db.collection('system_settings').doc('features').get();
    return snap.exists && snap.data().useServerPassBooking === true;
  } catch (e) {
    console.warn('[server pass flag] read failed → OFF:', e.message);
    return false;
  }
}

async function passSelfPurchaseEnabled(db) {
  try {
    const snap = await db.collection('system_settings').doc('features').get();
    // LIVE since 2026-07 (owner-verified): ON by default. Kill-switch stays
    // available — set system_settings/features.enablePassSelfPurchase to
    // false to hide/refuse self purchases without a redeploy.
    return !(snap.exists && snap.data().enablePassSelfPurchase === false);
  } catch (e) {
    // Fail-safe: on a read error, hide purchases rather than sell blind.
    console.warn('[pass flag] read failed → OFF:', e.message);
    return false;
  }
}

function genPurchaseCode() {
  const t = Date.now().toString(36).toUpperCase().slice(-5);
  const r = Math.random().toString(36).toUpperCase().slice(2, 4);
  return `PP${t}${r}`;
}

// Coach V2.1: 2nd student surcharge (owner rule: +฿100, hard max 2 people)
// and the coaching package types redeemable for a lesson (คล้าย Pass).
const COACH_EXTRA_PERSON_FEE = 100;
const COACH_PACKAGE_TYPES = ['beginner_coaching_5'];

// ── Coach booking feature flag — missing doc/field = OFF (safe default) ──
async function coachBookingEnabled(db) {
  try {
    const snap = await db.collection('system_settings').doc('features').get();
    return snap.exists && snap.data().enableCoachBookingCustomer === true;
  } catch (e) {
    console.warn('[coach flag] read failed → OFF:', e.message);
    return false;
  }
}
async function coachAddonV2Enabled(db) {
  try {
    const snap = await db.collection('system_settings').doc('features').get();
    return snap.exists && snap.data()[COACH_ADDON_V2_FLAG] === true;
  } catch (e) {
    console.warn('[coach addon v2 flag] read failed → OFF:', e.message);
    return false;
  }
}
const coachAvailDocId = (coachId, date, hour) => `${coachId}_${date}_${String(hour).replace(':', '')}`;

// ── price_quote — READ-ONLY. No writes anywhere. ────────────────────
// features — PUBLIC read of customer-facing feature flags (Firestore rules
// only expose system_settings/pricing to clients, so the UI asks us).
// Includes the current half-hour price so the UI note shows the real number.
async function handleFeatures(res) {
  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[features] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }
  let halfHourPrice = HALF_HOUR_PRICE;
  try {
    const p = await db.collection('system_settings').doc('pricing').get();
    halfHourPrice = halfPriceFrom(p.exists ? p.data() : null);
  } catch (e) { console.warn('[features] pricing read failed → default half price:', e.message); }
  return res.status(200).json({
    ok: true, buildVersion: BUILD_VERSION,
    enableHalfHourBooking: await halfHourEnabled(db), halfHourPrice,
    enableCoachAddonV2: await coachAddonV2Enabled(db),
  });
}

async function handlePriceQuote(res, body) {
  const date        = typeof body.date === 'string' ? body.date.trim() : '';
  const startTime   = typeof body.startTime === 'string' ? body.startTime.trim() : '';
  const payType     = typeof body.payType === 'string' && body.payType ? body.payType : 'single';
  const voucherCode = typeof body.voucherCode === 'string' && body.voucherCode.trim() ? body.voucherCode.trim().toUpperCase() : null;
  const lineUserId  = typeof body.lineUserId === 'string' ? body.lineUserId : null;
  const durationMinutes = parseDurationMinutes(body);

  if (!DATE_RE.test(date))      return res.status(400).json({ ok: false, error: 'date must be YYYY-MM-DD' });
  if (!TIME_RE.test(startTime)) return res.status(400).json({ ok: false, error: 'startTime must be HH:mm' });
  if (durationMinutes === null) return res.status(400).json({ ok: false, error: `durationMinutes must be 30-${MAX_DURATION_MIN} in steps of 30` });
  const segs = segmentsOf(startTime, durationMinutes);
  if (!segs) return res.status(400).json({ ok: false, error: 'Invalid start/duration (whole hours start at :00; nothing crosses midnight)' });
  const hasHalf = segs.some(x => x.span === 30);
  const placeErr = halfPlacementError(segs);
  if (placeErr) return res.status(409).json({ ok: false, code: 'SHAPE', error: placeErr });
  // Vouchers stay single-hour only (Phase A rule; halves never join promos).
  if (voucherCode && (durationMinutes !== 60 || hasHalf)) {
    return res.status(409).json({ ok: false, code: 'VOUCHER', error: 'โค้ดส่วนลดใช้ได้กับการจอง 1 ชั่วโมงเท่านั้น' });
  }

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[price_quote] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  if (hasHalf && !(await halfHourEnabled(db))) {
    return res.status(409).json({ ok: false, code: 'SHAPE', error: 'ยังไม่เปิดจองครึ่งชั่วโมง' });
  }

  try {
    const nowMs = Date.now();
    const [pricingSnap, holidaySnap, voucherBundle] = await Promise.all([
      db.collection('system_settings').doc('pricing').get(),
      db.collection('holidays').doc(date).get(),
      loadVoucherBundle(db, voucherCode),
    ]);
    // Owner rule: a booking containing a half joins NO promotions — full hours
    // price with the special promo disabled (single MAIN-account QR).
    const promoConfig = (!hasHalf && pricingSnap.exists) ? pricingSnap.data() : null;
    const halfPrice   = halfPriceFrom(pricingSnap.exists ? pricingSnap.data() : null);
    const isHoliday = holidaySnap.exists && holidaySnap.data().isHoliday === true;
    const quoteInput = h => ({
      date, startTime: h, nowMs,
      isHoliday,
      promoConfig, payType, voucherCode: null, voucher: null,
      lineUserId,
    });
    if (durationMinutes === 60) {
      const baseQuote = computeQuote(quoteInput(startTime));
      const quote = quoteWithVoucher(baseQuote, voucherBundle, {
        voucherCode, nowMs, lineUserId, date, startTime, durationMinutes, isHoliday,
      });
      return res.status(200).json({ ok: true, quote });
    }
    const segQuotes = segs.map(x => x.span === 30
      ? halfSegQuote(x.start, halfPrice, flatHalfMetadata(date, x.start, isHoliday))
      : { ...computeQuote(quoteInput(x.start)), startTime: x.start, span: 60 });
    const combined  = combineQuotes(segQuotes);
    return res.status(200).json({
      ok: true,
      quote: {
        ...segQuotes.find(q => q.span === 60) || segQuotes[0],  // base flags from an hour seg
        ...combined,                                            // totals + breakdown override
        durationMinutes, durationHours: durationMinutes / 60,
        endTime: endTimeAfterMin(startTime, durationMinutes),
        voucherApplied: false, voucherCode: null, discountAmount: 0,
      },
    });
  } catch (e) {
    if (e.code === 'MIXED_RECEIVER') {
      return res.status(409).json({ ok: false, code: 'MIXED_RECEIVER', error: 'ช่วงเวลาที่เลือกมีช่องทางชำระเงินต่างกัน กรุณาจองแยกรายชั่วโมง' });
    }
    console.error('[price_quote]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to compute quote' });
  }
}

// ── create — server-authoritative single-use booking (Stage 2) ──────
async function handleCreate(req, res, body) {
  const date         = typeof body.date === 'string' ? body.date.trim() : '';
  const startTime    = typeof body.startTime === 'string' ? body.startTime.trim() : '';
  const customerName = typeof body.customerName === 'string' ? body.customerName.trim() : '';
  const customerPhone= typeof body.customerPhone === 'string' ? body.customerPhone.trim() : '';
  const assertedLineUserId = typeof body.lineUserId === 'string' && body.lineUserId ? body.lineUserId : 'guest';
  const idToken      = typeof body.idToken === 'string' ? body.idToken.trim() : '';
  const lineDisplayName = typeof body.lineDisplayName === 'string' ? body.lineDisplayName : '';
  const customerNote = typeof body.customerNote === 'string' ? body.customerNote.slice(0, 500) : '';
  const voucherCode  = typeof body.voucherCode === 'string' && body.voucherCode.trim() ? body.voucherCode.trim().toUpperCase() : null;
  const durationMinutes = parseDurationMinutes(body);

  // Client NEVER sends price — it is recomputed below. Validate inputs only.
  if (!DATE_RE.test(date))      return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'date must be YYYY-MM-DD' });
  if (!TIME_RE.test(startTime)) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'startTime must be HH:mm' });
  if (!customerName)  return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerName is required' });
  if (!customerPhone) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerPhone is required' });
  if (durationMinutes === null) return res.status(400).json({ ok: false, code: 'VALIDATION', error: `durationMinutes must be 30-${MAX_DURATION_MIN} in steps of 30` });
  const segs = segmentsOf(startTime, durationMinutes);
  if (!segs) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Invalid start/duration (whole hours start at :00; nothing crosses midnight)' });
  const hasHalf = segs.some(x => x.span === 30);
  const placeErr = halfPlacementError(segs);
  if (placeErr) return res.status(409).json({ ok: false, code: 'SHAPE', error: placeErr });
  if (voucherCode && (durationMinutes !== 60 || hasHalf)) {
    return res.status(409).json({ ok: false, code: 'VOUCHER', error: 'โค้ดส่วนลดใช้ได้กับการจอง 1 ชั่วโมงเท่านั้น' });
  }

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[create] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  // Authenticated customer identity is server-derived. Without an ID token
  // this is a guest booking regardless of a client-stated lineUserId.
  let lineUserId = 'guest';
  if (idToken) {
    try { lineUserId = (await getAdminAuth().verifyIdToken(idToken)).uid; }
    catch { return res.status(401).json({ ok: false, code: 'AUTH', error: 'Session expired' }); }
    if (assertedLineUserId !== 'guest' && assertedLineUserId !== lineUserId) {
      return res.status(409).json({ ok: false, code: 'IDENTITY_MISMATCH', error: 'Authenticated account does not match request' });
    }
  }

  if (hasHalf && !(await halfHourEnabled(db))) {
    return res.status(409).json({ ok: false, code: 'SHAPE', error: 'ยังไม่เปิดจองครึ่งชั่วโมง' });
  }

  const nowMs = Date.now();
  let quote, segQuotes, isHolidayForVoucher = false;
  let voucherBundle = { voucher: null, campaign: null, campaignRef: null };
  try {
    const [pricingSnap, holidaySnap, loadedVoucherBundle] = await Promise.all([
      db.collection('system_settings').doc('pricing').get(),
      db.collection('holidays').doc(date).get(),
      loadVoucherBundle(db, voucherCode),
    ]);
    voucherBundle = loadedVoucherBundle;
    // Owner rule: bookings containing a half join NO promotions (promo off).
    const promoConfig = (!hasHalf && pricingSnap.exists) ? pricingSnap.data() : null;
    const halfPrice   = halfPriceFrom(pricingSnap.exists ? pricingSnap.data() : null);
    const isHoliday   = holidaySnap.exists && holidaySnap.data().isHoliday === true;
    isHolidayForVoucher = isHoliday;
    segQuotes = segs.map(x => x.span === 30
      ? halfSegQuote(x.start, halfPrice, flatHalfMetadata(date, x.start, isHoliday))
      : {
          ...computeQuote({
            date, startTime: x.start, nowMs,
            isHoliday,
            promoConfig,
            payType: 'single', voucherCode: null, voucher: null,
            lineUserId,
          }),
          startTime: x.start, span: 60,
        });
    quote = durationMinutes === 60
      ? quoteWithVoucher(segQuotes[0], voucherBundle, {
          voucherCode, nowMs, lineUserId, date, startTime, durationMinutes, isHoliday,
        })
      : { ...(segQuotes.find(q => q.span === 60) || segQuotes[0]), ...combineQuotes(segQuotes),
          voucherApplied: false, voucherCode: null, discountAmount: 0 };
  } catch (e) {
    if (e.code === 'MIXED_RECEIVER') {
      return res.status(409).json({ ok: false, code: 'MIXED_RECEIVER', error: 'ช่วงเวลาที่เลือกมีช่องทางชำระเงินต่างกัน กรุณาจองแยกรายชั่วโมง' });
    }
    console.error('[create] quote:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to price booking' });
  }

  // A requested voucher that can't apply is a hard rejection (no silent drop).
  if (voucherCode && !quote.voucherApplied) {
    return res.status(409).json({ ok: false, code: 'VOUCHER', error: mapVoucherReason(quote.voucherReason) });
  }

  const finalPrice        = quote.finalPrice;
  const endTime           = endTimeAfterMin(startTime, durationMinutes);
  const bookingCode       = genBookingCode();
  const freeVoucher       = quote.isFreeVoucher === true;
  const paymentExpiresAt  = freeVoucher ? null : Timestamp.fromMillis(nowMs + PAY_MINS * 60 * 1000);
  const allLateNight      = segQuotes.every(q => q.qrType === 'late_night');
  const bookingType       = allLateNight ? 'Late Night Session' : 'Single Use';
  const pricingMode       = allLateNight             ? 'late_night'
                          : quote.qrType === 'special' ? 'special_promotion'
                          : 'normal_single_use';

  // ── Cell-level conflict model (Phase B) ─────────────────────────────
  // needCells: every 30-min cell the booking covers. A booking conflicts when
  // ANY live slot doc (span 60 legacy/hour, span 30 half) overlaps a needed
  // cell — so we read BOTH cell docs of every touched clock hour, plus the
  // hourly available_slots doc (admin opens whole hours; an open hour opens
  // both halves).
  const startMin  = toMin(startTime);
  const needCells = []; for (let m = startMin; m < startMin + durationMinutes; m += 30) needCells.push(m);
  const touchedHours = [...new Set(needCells.map(m => Math.floor(m / 60)))];

  const bookingRef  = db.collection('bookings').doc();
  const segRefs     = segs.map(x => db.collection('booking_slots').doc(slotIdOf(date, x.start)));
  const cellRefs    = touchedHours.flatMap(H => [
    db.collection('booking_slots').doc(slotIdOf(date, `${String(H).padStart(2, '0')}:00`)),
    db.collection('booking_slots').doc(slotIdOf(date, `${String(H).padStart(2, '0')}:30`)),
  ]);
  const availRefs   = touchedHours.map(H => db.collection('available_slots').doc(slotIdOf(date, `${String(H).padStart(2, '0')}:00`)));
  const voucherRef  = quote.voucherApplied ? db.collection('vouchers').doc(voucherCode) : null;
  const campaignRef = quote.voucherApplied && voucherBundle.campaignRef ? voucherBundle.campaignRef : null;
  const bookingEndMs = Date.parse(`${date}T${endTime}:00+07:00`);
  const guestAccess = lineUserId === 'guest'
    ? prepareGuestAccess({ bookingEndMs: Number.isFinite(bookingEndMs) ? bookingEndMs : null, nowMs })
    : null;
  const guestAccessRef = guestAccess
    ? db.collection(GUEST_ACCESS_COLLECTION).doc(bookingRef.id)
    : null;

  try {
    await db.runTransaction(async (t) => {
      const reads = [...cellRefs.map(r => t.get(r)), ...availRefs.map(r => t.get(r))];
      if (voucherRef) reads.push(t.get(voucherRef));
      if (campaignRef) reads.push(t.get(campaignRef));
      const snaps = await Promise.all(reads);
      const cellSnaps  = snaps.slice(0, cellRefs.length);
      const availSnaps = snaps.slice(cellRefs.length, cellRefs.length + availRefs.length);
      const voucherIndex = cellRefs.length + availRefs.length;
      const voucherSnap = voucherRef ? snaps[voucherIndex] : null;
      const campaignSnap = campaignRef ? snaps[voucherIndex + 1] : null;

      // ── Room-open guard: every touched hour must be admin-open ────────
      for (const availSnap of availSnaps) {
        if (!availSnap.exists || availSnap.data().status !== 'open') throw new Error('SLOT_NOT_OPEN');
      }
      // ── Double-booking guard on EVERY covered 30-min cell ─────────────
      cellSnaps.forEach((snap, i) => {
        if (!snap.exists) return;
        const sd = snap.data();
        const docMin  = touchedHours[Math.floor(i / 2)] * 60 + (i % 2) * 30;
        const docSpan = sd.slotSpanMinutes === 30 ? 30 : 60;   // legacy docs = full hour
        const overlaps = needCells.some(c => c >= docMin && c < docMin + docSpan);
        if (!overlaps) return;
        if (isOccupiedSlot(sd, nowMs)) {
          throw new Error(sd.bookingStatus === 'pending_payment' && sd.paymentStatus !== 'pending_review'
            ? 'SLOT_HELD' : 'SLOT_TAKEN');
        }
      });

      // Exact code + campaign rules are re-validated atomically with the slot.
      if (voucherRef) {
        if (!voucherSnap.exists) throw new Error('VOUCHER_not_found');
        const v = voucherSnap.data();
        const campaign = campaignSnap?.exists ? { id: campaignSnap.id, ...campaignSnap.data() } : null;
        const txResult = evaluateVoucher({
          voucher: v, campaign, code: voucherCode, nowMs, lineUserId,
          date, startTime, durationMinutes, isHoliday: isHolidayForVoucher,
          branchId: DEFAULT_BRANCH_ID, resourceId: RESOURCE_ID,
          baseQuote: segQuotes[0], bookingId: bookingRef.id,
        });
        if (!txResult.ok) throw new Error(`VOUCHER_${txResult.reason}`);
        if (txResult.voucherType !== quote.voucherType || txResult.finalPrice !== finalPrice) {
          throw new Error('VOUCHER_changed');
        }
        const timestamp = FieldValue.serverTimestamp();
        if (isVoucherV2(v, campaign)) {
          const lifecycleUpdate = freeVoucher
            ? redeemVoucherUpdate(v, { bookingId: bookingRef.id, bookingCode, lineUserId, timestamp })
            : reserveVoucherUpdate(v, { bookingId: bookingRef.id, bookingCode, lineUserId, reservedUntil: paymentExpiresAt, timestamp });
          t.update(voucherRef, {
            ...lifecycleUpdate,
            maxCancellationRestores: txResult.definition.maxCancellationRestores,
          });
        } else {
          t.update(voucherRef, {
            usedCount: (Number(v.usedCount) || 0) + 1,
            lastUsedAt: timestamp, lastUsedBy: lineUserId || null, lastUsedBooking: bookingCode,
          });
        }
      }

      // ── Write booking (server price) + one slot lock per segment ─────
      t.set(bookingRef, {
        bookingCode, resourceId: RESOURCE_ID, branchId: DEFAULT_BRANCH_ID,
        bookingSlotIds: segRefs.map(r => r.id),
        bookingType,
        lineUserId, lineDisplayName,
        customerName, customerPhone, customerPhoneNormalized: normalizePhone(customerPhone),
        customerNote,
        date, startTime, endTime,
        durationMinutes, durationHours: durationMinutes / 60,
        ...(durationMinutes !== 60 ? { priceBreakdown: quote.breakdown } : {}),
        // Pricing v2 metadata (server-authoritative)
        price: finalPrice, amount: finalPrice,
        originalPrice: quote.originalPrice, finalPrice,
        basePrice: quote.originalPrice, effectivePrice: finalPrice,
        pricingType: quote.pricingType, pricingMode,
        promoCode: quote.promoCode, voucherCode: quote.voucherCode, discountAmount: quote.discountAmount,
        voucherType: quote.voucherType || null,
        voucherLifecycle: quote.voucherLifecycle || null,
        voucherCampaignId: quote.voucherCampaignId || null,
        voucherCampaignName: quote.voucherCampaignName || null,
        priceRuleVersion: quote.priceRuleVersion,
        qrAmount: quote.qrAmount, qrType: quote.qrType, paymentQrType: quote.qrType,
        promoApplied: quote.pricingType === 'special_promotion' || quote.voucherApplied,
        isHoliday: quote.isHoliday, isWeekend: quote.isWeekend,
        isMorningWeekday: quote.isMorningWeekday, advanceHours: quote.advanceHours,
        bookingStatus: freeVoucher ? 'confirmed' : 'pending_payment',
        paymentStatus: freeVoucher ? 'package' : 'unpaid',
        paymentExpiresAt,
        slipUrl: null, slipUploadedAt: null, cancelReason: null,
        createdVia: freeVoucher ? 'server_voucher' : 'server',
        ...(freeVoucher ? { confirmedAt: FieldValue.serverTimestamp(), paymentMethod: 'voucher' } : {}),
        createdAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
      });
      // Routed through writeSlotDoc so the public slot contract (SL-02) is
      // enforced in one place rather than at each call site.
      // Public availability document + private claim, written together.
      segs.forEach((x, i) => {
        writeSlotDoc(t, db, segRefs[i].id, {
          resourceId: RESOURCE_ID, branchId: DEFAULT_BRANCH_ID,
          date, hour: x.start, slotSpanMinutes: x.span,
          bookingStatus: freeVoucher ? 'confirmed' : 'pending_payment',
          paymentStatus: freeVoucher ? 'package' : 'unpaid',
          expiresAt: paymentExpiresAt,
        }, { bookingId: bookingRef.id, bookingCode });
      });
      if (guestAccessRef) t.create(guestAccessRef, guestAccess.record);
    });
  } catch (e) {
    const msg = e.message || '';
    if (msg.startsWith('SLOT_')) {
      const m = { SLOT_NOT_OPEN: 'ช่องเวลานี้ปิดรับจองแล้ว', SLOT_TAKEN: 'ช่องเวลานี้เพิ่งถูกจอง', SLOT_HELD: 'ช่องเวลานี้ถูกจองค้างอยู่ ลองใหม่อีกครั้ง' };
      return res.status(409).json({ ok: false, code: 'SLOT', error: m[msg] || 'ช่องเวลาไม่ว่าง' });
    }
    if (msg.startsWith('VOUCHER_')) {
      return res.status(409).json({ ok: false, code: 'VOUCHER', error: mapVoucherReason(msg.slice(8)) });
    }
    console.error('[create] tx:', msg);
    return res.status(500).json({ ok: false, error: 'Failed to create booking' });
  }

  console.log(`[create] ${bookingCode} ${quote.pricingType} ${durationMinutes}min ฿${finalPrice}${quote.voucherApplied ? ' voucher=' + voucherCode : ''}`);

  // ── Guest capability token (Security Hotfix 2026-08-04) ─────────────
  // Guests have no Firebase Auth, so hand them the scoped token whose hash
  // committed atomically with the booking. This is the only response that
  // contains the raw value; it is never logged or persisted.
  return res.status(200).json({
    ok: true,
    requiresPayment: !freeVoucher,
    paymentExpiresAt: paymentExpiresAt ? paymentExpiresAt.toDate().toISOString() : null,
    ...(guestAccess ? { guestAccessToken: guestAccess.token, guestAccessExpiresAt: guestAccess.expiresAt } : {}),
    booking: {
      id: bookingRef.id, bookingCode, date, startTime, endTime,
      bookingSlotIds: segRefs.map(r => r.id),
      durationMinutes, durationHours: durationMinutes / 60,
      bookingType,
      finalPrice, price: finalPrice, originalPrice: quote.originalPrice,
      qrType: quote.qrType, qrAmount: quote.qrAmount, paymentQrType: quote.qrType,
      pricingType: quote.pricingType, discountAmount: quote.discountAmount, voucherCode: quote.voucherCode,
      voucherType: quote.voucherType || null,
      voucherCampaignId: quote.voucherCampaignId || null,
      bookingStatus: freeVoucher ? 'confirmed' : 'pending_payment',
      paymentStatus: freeVoucher ? 'package' : 'unpaid',
      ...(durationMinutes !== 60 ? { priceBreakdown: quote.breakdown } : {}),
      lineUserId, customerName, customerPhone, customerNote,
    },
  });
}

// ── cancel_pending — customer cancels their own UNPAID pending booking ──
// Fixes the live "Insufficient Permission" bug: the client used to set
// booking_slots.paymentStatus="cancelled", which is NOT in the rules enum.
// Doing it server-side (Admin SDK bypasses rules) avoids widening the rules.
// Ownership (review remediation RB-11): a LINE customer must present a
// Firebase ID token, verified server-side. A guest must present the
// capability token for that booking. There is no longer any path that
// accepts a client-stated identity — the previous fallback trusted
// body.lineUserId whenever it equalled booking.lineUserId, and every input
// it compared is readable from public data.
//
// Only pending_payment + unpaid + not-expired bookings are cancellable by a
// customer. Does NOT touch the admin reject/refund flow, and passes/events
// (confirmed/package) are excluded.
async function handleCancelPending(req, res, body) {
  const bookingIdRaw = typeof body.bookingId === 'string' ? body.bookingId : '';
  const bookingCodeRaw = typeof body.bookingCode === 'string' ? body.bookingCode : '';
  const guestTokenRaw = typeof body.guestToken === 'string' ? body.guestToken : '';
  if (bookingIdRaw.length > GUEST_BOOKING_ID_MAX_LENGTH || bookingCodeRaw.length > 128 || guestTokenRaw.length > GUEST_TOKEN_MAX_LENGTH) {
    return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Credential is too long' });
  }
  const bookingId   = bookingIdRaw.trim();
  const bookingCode = bookingCodeRaw.trim();
  const lineUserId  = typeof body.lineUserId === 'string' ? body.lineUserId : '';
  const idToken     = typeof body.idToken === 'string' && body.idToken ? body.idToken : null;
  if (!bookingId)   return res.status(400).json({ ok: false, error: 'Missing bookingId' });
  if (!bookingCode) return res.status(400).json({ ok: false, error: 'Missing bookingCode' });

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[cancel_pending] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  const guestToken = guestTokenRaw.trim();
  const ip = clientIp(req);
  let guestAuthorized = false;
  if (!idToken) {
    const globalGate = await readRateLimitGate(db, { bucket: 'guestInvalid', key: ip });
    if (!globalGate.allowed) {
      res.setHeader('Retry-After', String(globalGate.retryAfterSec));
      return res.status(429).json({ ok: false, code: 'RATE_LIMIT', error: 'Too many requests' });
    }
    const verified = guestToken
      ? await verifyGuestToken(db, bookingId, guestToken, 'booking:cancel')
      : { ok:false };
    if (!verified.ok) {
      const bad = await checkRateLimit(db, {
        bucket: 'guestInvalid', key: ip, ...RATE_LIMITS.guestInvalid,
      });
      if (!bad.allowed) {
        res.setHeader('Retry-After', String(bad.retryAfterSec));
        return res.status(429).json({ ok: false, code: 'RATE_LIMIT', error: 'Too many attempts' });
      }
      return res.status(403).json({ ok: false, code: 'TOKEN', error: 'Guest authentication failed' });
    }
    const mutationGate = await checkRateLimit(db, {
      bucket: 'guestMutation', key: `${ip}|${bookingId}`, ...RATE_LIMITS.guestMutation,
    });
    if (!mutationGate.allowed) {
      res.setHeader('Retry-After', String(mutationGate.retryAfterSec));
      return res.status(429).json({ ok: false, code: 'RATE_LIMIT', error: 'Too many requests' });
    }
    guestAuthorized = true;
  }

  const bookingRef = db.collection('bookings').doc(bookingId);
  let booking;
  try {
    const snap = await bookingRef.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Booking not found' });
    booking = snap.data();
  } catch (e) { console.error('[cancel_pending] read:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  // ── Ownership ─────────────────────────────────────────────────────
  let owner = guestAuthorized;

  // Path 1 — LINE customer. Server-verified Firebase ID token only.
  // An invalid or expired token is a hard failure; it no longer degrades
  // into trusting whatever identity the client claimed (RB-11).
  if (idToken) {
    let decoded = null;
    try { decoded = await getAdminAuth().verifyIdToken(idToken); }
    catch { return res.status(401).json({ ok: false, code: 'AUTH', error: 'เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่' }); }
    if (decoded.uid !== booking.lineUserId) {
      return res.status(403).json({ ok: false, error: 'บัญชีไม่ตรงกับการจอง' });
    }
    owner = true;
  }

  // No third path. body.lineUserId is accepted as a request field for
  // backward compatibility with older clients but carries no authority.
  if (!owner) {
    const bad = await checkRateLimit(db, {
      bucket: 'guestInvalid', key: ip, ...RATE_LIMITS.guestInvalid,
    });
    if (!bad.allowed) {
      res.setHeader('Retry-After', String(bad.retryAfterSec));
      return res.status(429).json({ ok: false, code: 'RATE_LIMIT', error: 'Too many attempts' });
    }
    return res.status(403).json({ ok: false, error: 'ยกเลิกไม่ได้ (ยืนยันตัวตนไม่ผ่าน)' });
  }

  // bookingCode is a client assertion, checked only after authentication.
  if (bookingCode !== booking.bookingCode) {
    return res.status(403).json({ ok: false, error: 'ยกเลิกไม่ได้ (ไม่ใช่การจองของคุณ)' });
  }

  if (isCoachAddonV2Booking(booking)) {
    if (booking.cashState !== 'unpaid') {
      return res.status(409).json({ ok: false, error: 'อัปโหลดสลิป/ชำระแล้ว ยกเลิกเองไม่ได้ กรุณาติดต่อแอดมิน' });
    }
    try {
      const result = await releaseCoachAddonV2Hold(db, bookingId, {
        reason: 'customer_cancel_coach_addon_v2', actor: lineUserId || 'guest',
        requireExpired: false, terminalState: 'cancelled',
      });
      if (guestAuthorized) await revokeGuestAccess(db, bookingId, 'booking_cancelled').catch(() => null);
      return res.status(200).json({ ok: true, replayed: result.replayed === true, bookingStatus: 'cancelled', paymentStatus: 'rejected' });
    } catch (e) {
      if (e.message === 'NOT_HELD') return res.status(409).json({ ok: false, error: 'สถานะการจองเปลี่ยนไปแล้ว กรุณารีเฟรช' });
      console.error('[cancel_pending coach addon v2]', e.message);
      return res.status(500).json({ ok: false, error: 'ยกเลิก Coach Add-on ไม่สำเร็จ' });
    }
  }

  // ── Preconditions (customer may only cancel an unpaid, not-yet-expired hold) ──
  if (booking.bookingStatus !== 'pending_payment') {
    return res.status(409).json({ ok: false, error: 'การจองนี้ยกเลิกเองไม่ได้ กรุณาติดต่อแอดมิน' });
  }
  if (booking.paymentStatus !== 'unpaid') {
    return res.status(409).json({ ok: false, error: 'อัปโหลดสลิป/ชำระแล้ว ยกเลิกเองไม่ได้ กรุณาติดต่อแอดมิน' });
  }
  const expMs = booking.paymentExpiresAt?.toMillis?.() ?? null;
  if (expMs !== null && expMs < Date.now()) {
    return res.status(409).json({ ok: false, error: 'การจองหมดเวลาแล้ว' });
  }

  // Release EVERY slot segment the booking holds (Phase B: hour + half docs).
  const slotRefs = bookingSegments(booking)
    .map(x => db.collection('booking_slots').doc(`${RESOURCE_ID}_${booking.date}_${String(x.start).replace(':', '')}`));
  // Coach lesson: the coach hour was locked in the create transaction —
  // release it here (ownership-checked below).
  const coachAvailRef = (booking.serviceType === 'coach_lesson' && booking.coachId && booking.date && booking.startTime)
    ? db.collection('coach_availability').doc(coachAvailDocId(booking.coachId, booking.date, booking.startTime))
    : null;
  const voucherRef = booking.voucherLifecycle === 'v2_state' && booking.voucherCode
    ? db.collection('vouchers').doc(booking.voucherCode)
    : null;

  try {
    await db.runTransaction(async (t) => {
      const bSnap = await t.get(bookingRef);
      const slotSnaps  = await Promise.all(slotRefs.map(r => t.get(r)));
      const claimSnaps = await Promise.all(slotRefs.map(r => t.get(slotClaimRef(db, r.id))));
      const caSnap = coachAvailRef ? await t.get(coachAvailRef) : null;
      const voucherSnap = voucherRef ? await t.get(voucherRef) : null;
      if (!bSnap.exists) throw new Error('GONE');
      const bNow = bSnap.data();
      if (bNow.bookingStatus !== 'pending_payment') throw new Error('BAD_STATE');
      if (bNow.paymentStatus !== 'unpaid') throw new Error('BAD_STATE');
      if (voucherRef && voucherSnap?.exists) {
        const voucher = voucherSnap.data();
        if (voucher.state === 'reserved' && voucher.reservedBookingId === bookingId) {
          const released = releaseVoucherUpdate(voucher, {
            bookingId, reason: 'customer_cancel_pending_payment',
            timestamp: FieldValue.serverTimestamp(), countRestore: false,
          });
          t.update(voucherRef, released.update);
        }
      }
      // Reopen the coach hour only when this booking still owns the lock.
      if (caSnap && caSnap.exists) {
        const ca = caSnap.data();
        if (ca.status === 'booked' && ca.bookingId === bookingId) {
          t.set(coachAvailRef, {
            coachId: ca.coachId, branchId: ca.branchId || DEFAULT_BRANCH_ID,
            date: ca.date, hour: ca.hour, status: 'open',
            updatedAt: FieldValue.serverTimestamp(),
          });
        }
      }

      // Keep paymentStatus inside the existing enum ("rejected", same as the
      // admin reject flow) — never introduce a new "cancelled" paymentStatus.
      t.update(bookingRef, {
        bookingStatus: 'cancelled',
        status:        'cancelled',
        paymentStatus: 'rejected',
        cancelReason:  'customer_cancel_pending_payment',
        cancelledAt:   FieldValue.serverTimestamp(),
        cancelledBy:   'customer',
        updatedAt:     FieldValue.serverTimestamp(),
      });
      // Release each slot only if it still belongs to this booking and isn't
      // confirmed. RB-10: ownership now comes from the private claim, since
      // the public document no longer carries bookingId or bookingCode.
      // A legacy slot written before this hotfix has no claim; those still
      // carry the identifiers inline, so fall back to them for compatibility.
      slotSnaps.forEach((slotSnap, i) => {
        if (!slotSnap.exists) return;
        const sd    = slotSnap.data();
        const claim = claimSnaps[i]?.exists ? claimSnaps[i].data() : null;
        const owns  = claim
          ? claim.bookingId === bookingId
          : (sd.bookingId === bookingId || sd.bookingCode === booking.bookingCode);
        if (owns && sd.bookingStatus !== 'confirmed') {
          t.update(slotRefs[i], { bookingStatus: 'cancelled', paymentStatus: 'rejected' });
          if (claim) t.delete(slotClaimRef(db, slotRefs[i].id));
        }
      });
    });
  } catch (e) {
    if (e.message === 'BAD_STATE' || e.message === 'GONE') {
      return res.status(409).json({ ok: false, error: 'สถานะการจองเปลี่ยนไปแล้ว กรุณารีเฟรช' });
    }
    console.error('[cancel_pending] tx:', e.message);
    return res.status(500).json({ ok: false, error: 'ยกเลิกไม่สำเร็จ กรุณาลองใหม่' });
  }

  // GT-01: a cancelled booking must not remain reachable by its guest token.
  // Non-fatal — the cancellation itself already committed.
  await revokeGuestAccess(db, bookingId, 'booking_cancelled')
    .catch(e => console.warn('[cancel_pending] access revoke:', e.message));

  await writeAuditLog(db, {
    actor: 'customer', actorRole: 'customer', branchId: booking.branchId || DEFAULT_BRANCH_ID,
    action: 'cancel_pending_customer', targetId: bookingId,
    before: { bookingStatus: 'pending_payment', paymentStatus: 'unpaid' },
    after:  { bookingStatus: 'cancelled', paymentStatus: 'rejected' },
    note: bookingCode,
  });
  console.log(`[cancel_pending] ${bookingCode} cancelled by customer`);

  // ── Admin notification — flag-gated, DEFAULT OFF ────────────────────
  // Every cancel_pending is by definition an unpaid, no-slip booking (the
  // preconditions above guarantee it), so the safe default is silence: no
  // admin action is needed and the slot is already released. Set
  // system_settings/notification_flags.notifyAdminOnCustomerPendingCancel
  // to true to broadcast "ลูกค้ายกเลิกก่อนชำระเงิน" to all admins.
  // Never fails the request — the cancel already succeeded above.
  try {
    const flags = await loadNotificationFlags();
    if (flags.notifyAdminOnCustomerPendingCancel === true) {
      const admins = await loadActiveAdmins();
      await Promise.all(admins.map(a =>
        sendAndLog({
          eventId: `${bookingCode}_customer_cancel_${a.lineUserId}`,
          type: 'customer_cancel_pending_admin',
          targetType: 'admin',
          lineUserId: a.lineUserId,
          bookingCode,
          payload: {
            bookingCode,
            customerName:  booking.customerName,
            customerPhone: booking.customerPhone,
            date: booking.date, startTime: booking.startTime, endTime: booking.endTime,
          },
        }).catch(e => ({ ok: false, error: e.message }))
      ));
    }
  } catch (e) {
    console.error('[cancel_pending] notify (non-fatal):', e.message);
  }

  return res.status(200).json({ ok: true });
}

// ════════════════════════════════════════════════════════════════════
// create_pass_booking — Security Hotfix 2026-08-04  (closes SEC-02)
// ════════════════════════════════════════════════════════════════════
// Replaces the client-side Firestore transaction at index.html:2891, which
// deducted customer_packages.remainingMinutes straight from the browser.
// The baseline rules never constrained remainingMinutes on update, so any
// caller could set any pass balance to any value.
//
// Two things this fixes that the client path could not:
//   1. Ownership is proved by a Firebase ID token (uid == verified LINE
//      userId, minted by /api/auth-line), not by a client-stated field.
//   2. Duration is enforced server-side. The client restricted pass
//      bookings to 60 minutes in JS (index.html:1810) but always deducted
//      a hard-coded 60, so a modified client could book 180 minutes and
//      pay 60 (R-02). Here the booked duration IS the deducted amount.
//
// Guests cannot reach this path at all (Addendum 02 sec 9.4).
// ════════════════════════════════════════════════════════════════════

const PASS_PAY_TYPES   = ['ultra', 'offpeak', 'event'];
const OFFPEAK_HOURS    = { startHour: 9, endHour: 15 };
// OR-01: the stored package type, never the request assertion, selects the
// entitlement policy. These are every room-pass packageType currently issued
// or recognized by index.html/admin-user-action.js. Coaching types are
// intentionally absent and unknown values fail closed.
export const PACKAGE_TYPE_TO_ENTITLEMENT = Object.freeze({
  ultra_starter_3: 'ultra',
  ultra_pass_10: 'ultra',
  ultra_pass_20: 'ultra',
  ultra_10: 'ultra',
  ultra_20: 'ultra',
  offpeak: 'offpeak',
  monstr_event_pass: 'event',
});

export function entitlementTypeForPackage(packageType) {
  return PACKAGE_TYPE_TO_ENTITLEMENT[String(packageType || '')] || null;
}

export async function readHolidayInTransaction(transaction, holidayRef) {
  try {
    const snap = await transaction.get(holidayRef);
    return snap.exists && snap.data().isHoliday === true;
  } catch {
    throw new Error('HOLIDAY_CHECK_UNAVAILABLE');
  }
}

// ISO-8601 week key, ported 1:1 from index.html:1482 so quota buckets stay
// identical across the old and new path during migration.
function isoWeekKeyOf(dateISO) {
  const [y, m, dd] = String(dateISO).split('-').map(Number);
  const d = new Date(Date.UTC(y, m - 1, dd));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return `${d.getUTCFullYear()}-W${String(weekNo).padStart(2, '0')}`;
}
const monthKeyOf = dateISO => String(dateISO).slice(0, 7);
const dowOfDate  = (dateISO) => {
  const [y, m, d] = String(dateISO).split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d)).getUTCDay();   // 0=Sun..6=Sat
};

// Validates a pass against the booking context and returns the package
// mutation to apply. Pure apart from the values passed in, so the rules are
// readable in one place. Throws Error(code) — mapped to Thai text by caller.
export function validatePassAndBuildUpdate({ entitlementType, pkg, uid, dateISO, startTime, durationMinutes, isHoliday, nowMs }) {
  if (pkg.lineUserId !== uid)  throw new Error('PASS_NOT_OWNED');
  if (pkg.status !== 'active') throw new Error('PASS_INACTIVE');

  const validUntil = pkg.validUntil?.toMillis?.() ?? null;
  if (!validUntil || validUntil < nowMs) throw new Error('PASS_EXPIRED');

  const remaining = Number(pkg.remainingMinutes);
  const dow       = dowOfDate(dateISO);
  const startH    = parseInt(String(startTime).slice(0, 2), 10);

  if (entitlementType === 'ultra') {
    if (!Number.isFinite(remaining) || remaining < durationMinutes) throw new Error('PASS_INSUFFICIENT');
    return { remainingMinutes: remaining - durationMinutes };
  }

  if (entitlementType === 'event') {
    const policyError = eventPassBookingError({
      pkg, dateISO, startTime, durationMinutes, isHoliday, nowMs,
      branchId: DEFAULT_BRANCH_ID, resourceId: RESOURCE_ID,
    });
    if (policyError) throw new Error(policyError);
    return { remainingMinutes: remaining - durationMinutes, eventUsedAt: FieldValue.serverTimestamp() };
  }

  // ── offpeak: day/time window + weekly/monthly quota + total cap ──
  if (entitlementType !== 'offpeak') throw new Error('PASS_TYPE_UNSUPPORTED');
  if (dow < 1 || dow > 5) throw new Error('PASS_WEEKDAY_ONLY');
  if (isHoliday)          throw new Error('PASS_NO_HOLIDAY');
  if (startH < OFFPEAK_HOURS.startHour || startH + (durationMinutes / 60) > OFFPEAK_HOURS.endHour) {
    throw new Error('PASS_OFFPEAK_WINDOW');
  }

  const weekKey = isoWeekKeyOf(dateISO);
  const monKey  = monthKeyOf(dateISO);
  const wkUsage = pkg.weeklyUsage  || {};
  const moUsage = pkg.monthlyUsage || {};
  const wkUsed  = Number(wkUsage[weekKey]) || 0;
  const moUsed  = Number(moUsage[monKey])  || 0;
  const wkLimit = (Number(pkg.weeklyLimitHours)  || 0) * 60;
  const moLimit = (Number(pkg.monthlyLimitHours) || 0) * 60;

  // A configured-but-unset limit blocks rather than silently allowing —
  // same fail-closed stance as the client path it replaces.
  if (!wkLimit || !moLimit) throw new Error('PASS_QUOTA_UNCONFIGURED');
  if (wkUsed + durationMinutes > wkLimit) throw new Error('PASS_WEEKLY_LIMIT');
  if (moUsed + durationMinutes > moLimit) throw new Error('PASS_MONTHLY_LIMIT');

  // Legacy off-peak passes predate totalMinutes; for those the monthly cap
  // governs and remainingMinutes is not tracked.
  const hasTotal = Number(pkg.totalMinutes) > 0;
  if (hasTotal) {
    if (!Number.isFinite(remaining))      throw new Error('PASS_HOURS_UNCONFIGURED');
    if (remaining < durationMinutes)      throw new Error('PASS_INSUFFICIENT');
  }

  return {
    ...(hasTotal ? { remainingMinutes: remaining - durationMinutes } : {}),
    weeklyUsage:  { ...wkUsage, [weekKey]: wkUsed + durationMinutes },
    monthlyUsage: { ...moUsage, [monKey]:  moUsed + durationMinutes },
  };
}

const PASS_ERROR_TEXT = {
  PASS_NOT_OWNED:          'แพ็คเกจนี้ไม่ใช่ของบัญชีนี้',
  PASS_INACTIVE:           'แพ็คเกจไม่พร้อมใช้งาน',
  PASS_EXPIRED:            'แพ็คเกจหมดอายุแล้ว',
  PASS_INSUFFICIENT:       'ชั่วโมงในแพ็คเกจไม่พอ',
  PASS_WRONG_BRANCH:       'แพ็คเกจนี้ใช้ได้กับสาขาอื่น',
  PASS_WRONG_RESOURCE:     'แพ็คเกจนี้ใช้ได้กับคอร์ทอื่น',
  PASS_WEEKDAY_ONLY:       'แพ็คเกจนี้ใช้ได้เฉพาะวันจันทร์–ศุกร์',
  PASS_NO_HOLIDAY:         'แพ็คเกจนี้ใช้ในวันหยุดไม่ได้',
  PASS_EVENT_ONE_HOUR:     'Event Pass ใช้จองได้ครั้งละ 1 ชั่วโมงเท่านั้น',
  PASS_BOOKING_AFTER_EXPIRY:'วันที่ใช้บริการต้องไม่เกินวันหมดอายุของ Event Pass',
  PASS_OFFPEAK_WINDOW:     'Off-Peak ใช้ได้เฉพาะ 09:00–15:00',
  PASS_QUOTA_UNCONFIGURED: 'โควตาของแพ็คเกจยังไม่ถูกตั้งค่า กรุณาติดต่อร้าน',
  PASS_HOURS_UNCONFIGURED: 'ชั่วโมงของแพ็คเกจยังไม่ถูกตั้งค่า กรุณาติดต่อร้าน',
  PASS_WEEKLY_LIMIT:       'ใช้ครบโควตารายสัปดาห์แล้ว',
  PASS_MONTHLY_LIMIT:      'ใช้ครบโควตารายเดือนแล้ว',
  PASS_GONE:               'ไม่พบแพ็คเกจ กรุณารีเฟรช',
};

async function handleCreatePassBooking(res, body) {
  const date            = typeof body.date === 'string' ? body.date.trim() : '';
  const startTime       = typeof body.startTime === 'string' ? body.startTime.trim() : '';
  const payType         = typeof body.payType === 'string' ? body.payType.trim() : '';
  const packageId       = typeof body.packageId === 'string' ? body.packageId.trim() : '';
  const customerName    = typeof body.customerName === 'string' ? body.customerName.trim() : '';
  const customerPhone   = typeof body.customerPhone === 'string' ? body.customerPhone.trim() : '';
  const customerNote    = typeof body.customerNote === 'string' ? body.customerNote.slice(0, 500) : '';
  const lineDisplayName = typeof body.lineDisplayName === 'string' ? body.lineDisplayName : '';
  const idToken         = typeof body.idToken === 'string' ? body.idToken.trim() : '';
  const idemKey         = typeof body.idempotencyKey === 'string' ? body.idempotencyKey.trim() : '';
  const durationMinutes = parseDurationMinutes(body);

  if (!DATE_RE.test(date))      return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'date must be YYYY-MM-DD' });
  if (!TIME_RE.test(startTime)) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'startTime must be HH:mm' });
  if (!PASS_PAY_TYPES.includes(payType)) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Unknown payType' });
  if (!packageId)     return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'packageId is required' });
  // RB-02: mandatory. A pass booking spends real balance; a retry without a
  // key would book twice and deduct twice.
  if (!isValidIdempotencyKey(idemKey)) {
    return res.status(400).json({ ok: false, code: 'IDEMPOTENCY', error: 'idempotencyKey has invalid format or length' });
  }
  if (!customerName)  return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerName is required' });
  if (!customerPhone) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerPhone is required' });

  // Pass bookings are whole-hour only. The client enforced 60 minutes in JS
  // and then deducted a fixed 60; enforcing it here is what actually closes
  // the bypass (R-02).
  if (durationMinutes === null || durationMinutes % 60 !== 0) {
    return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'durationMinutes must be a whole number of hours' });
  }

  const segs = segmentsOf(startTime, durationMinutes);
  if (!segs) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Invalid start/duration' });

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[create_pass_booking] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  // ── Identity is mandatory. No token, no pass. ──────────────────────
  let uid = null;
  try {
    if (idToken) uid = (await getAdminAuth().verifyIdToken(idToken)).uid;
  } catch { /* fall through to 403 */ }
  if (!uid) return res.status(403).json({ ok: false, code: 'AUTH', error: 'กรุณาเข้าสู่ระบบผ่าน LINE เพื่อใช้แพ็คเกจ' });

  const nowMs   = Date.now();
  const startMs = Date.parse(`${date}T${startTime}:00+07:00`);
  if (!Number.isFinite(startMs) || startMs <= nowMs) {
    return res.status(409).json({ ok: false, code: 'SLOT', error: 'ช่วงเวลานี้ผ่านมาแล้ว' });
  }

  let isHoliday = false;

  const endTime          = endTimeAfterMin(startTime, durationMinutes);
  const bookingCode      = genBookingCode();
  const startMin         = toMin(startTime);
  const needCells        = []; for (let m = startMin; m < startMin + durationMinutes; m += 30) needCells.push(m);
  const touchedHours     = [...new Set(needCells.map(m => Math.floor(m / 60)))];

  const bookingRef = db.collection('bookings').doc();
  const segRefs    = segs.map(x => db.collection('booking_slots').doc(slotIdOf(date, x.start)));
  const cellRefs   = touchedHours.flatMap(H => [
    db.collection('booking_slots').doc(slotIdOf(date, `${String(H).padStart(2, '0')}:00`)),
    db.collection('booking_slots').doc(slotIdOf(date, `${String(H).padStart(2, '0')}:30`)),
  ]);
  const availRefs  = touchedHours.map(H => db.collection('available_slots').doc(slotIdOf(date, `${String(H).padStart(2, '0')}:00`)));
  const pkgRef     = db.collection('customer_packages').doc(packageId);
  const holidayRef = db.collection('holidays').doc(date);

  // RB-02: the idempotency record is read and written INSIDE the booking
  // transaction, so the record and the booking commit or roll back together.
  // The fingerprint covers everything that defines "the same request" —
  // reusing a key for a different booking is a client bug, not a replay.
  const idemScope = `create_pass_booking:${uid}`;
  const idemRef  = idempotencyRef(db, idemKey, idemScope);
  const idemFp   = fingerprintOf({
    uid, payType, packageId, date, startTime, durationMinutes,
    customerName, customerPhone, customerNote, lineDisplayName,
  });
  const ledgerRef = db.collection('customer_package_logs').doc();

  let pkgSnapshot = null;
  let replayed    = null;
  try {
    await db.runTransaction(async (t) => {
      // Read the record first: on replay nothing else needs reading.
      const idem = await readIdempotencyInTx(t, idemRef, idemFp);
      if (idem.state === 'conflict') throw new Error('IDEMPOTENCY_CONFLICT');
      if (idem.state === 'replay')  { replayed = idem.response; return; }

      const snaps     = await Promise.all([...cellRefs.map(r => t.get(r)), ...availRefs.map(r => t.get(r)), t.get(pkgRef)]);
      const cellSnaps = snaps.slice(0, cellRefs.length);
      const availSnaps= snaps.slice(cellRefs.length, cellRefs.length + availRefs.length);
      const pkgSnap   = snaps[snaps.length - 1];

      for (const a of availSnaps) {
        if (!a.exists || a.data().status !== 'open') throw new Error('SLOT_NOT_OPEN');
      }
      // Same cell-level conflict model as handleCreate: legacy docs with no
      // slotSpanMinutes are full-hour by definition.
      cellSnaps.forEach((snap, i) => {
        if (!snap.exists) return;
        const sd = snap.data();
        const docMin  = touchedHours[Math.floor(i / 2)] * 60 + (i % 2) * 30;
        const docSpan = sd.slotSpanMinutes === 30 ? 30 : 60;
        const overlaps = needCells.some(c => c >= docMin && c < docMin + docSpan);
        if (!overlaps) return;
        if (isOccupiedSlot(sd, nowMs)) {
          throw new Error(sd.bookingStatus === 'pending_payment' && sd.paymentStatus !== 'pending_review'
            ? 'SLOT_HELD' : 'SLOT_TAKEN');
        }
      });

      if (!pkgSnap.exists) throw new Error('PASS_GONE');
      const pkg = pkgSnap.data();
      pkgSnapshot = pkg;

      const entitlementType = entitlementTypeForPackage(pkg.packageType);
      if (!entitlementType) throw new Error('PASS_TYPE_UNSUPPORTED');
      if (payType !== entitlementType) throw new Error('PASS_TYPE_MISMATCH');

      // OR-02: restricted-entitlement holiday state is read in the same
      // transaction that spends the package and creates the booking.
      isHoliday = false;
      if (entitlementType === 'offpeak' || entitlementType === 'event') {
        isHoliday = await readHolidayInTransaction(t, holidayRef);
      }

      // Re-validated INSIDE the transaction so a concurrent booking cannot
      // spend the same minutes twice.
      const pkgUpdate = validatePassAndBuildUpdate({
        entitlementType, pkg, uid, dateISO: date, startTime, durationMinutes, isHoliday, nowMs,
      });

      t.set(bookingRef, {
        bookingCode, resourceId: RESOURCE_ID, branchId: DEFAULT_BRANCH_ID,
        bookingSlotIds: segRefs.map(r => r.id),
        bookingType: pkg.packageName || 'Package Booking',
        lineUserId: uid, lineDisplayName,
        customerName, customerPhone, customerPhoneNormalized: normalizePhone(customerPhone),
        customerNote,
        date, startTime, endTime,
        durationMinutes, durationHours: durationMinutes / 60,
        price: 0, amount: 0, originalPrice: 0, finalPrice: 0, basePrice: 0, effectivePrice: 0,
        pricingType: 'package', pricingMode: 'package',
        promoCode: null, voucherCode: null, discountAmount: 0,
        priceRuleVersion: 'pass-server-v1',
        qrAmount: 0, qrType: null, paymentQrType: null, promoApplied: false,
        isHoliday, isWeekend: [0, 6].includes(dowOfDate(date)),
        packageId, packageType: pkg.packageType, packageName: pkg.packageName || pkg.packageType,
        usedPackageId: packageId, usedPackageType: pkg.packageType, usedPackageName: pkg.packageName || pkg.packageType,
        packageMinutesUsed: durationMinutes,
        ...(entitlementType === 'event' ? { isEventBooking: true } : {}),
        bookingStatus: 'confirmed', paymentStatus: 'package',
        paymentExpiresAt: null,
        slipUrl: null, slipUploadedAt: null, cancelReason: null,
        confirmedAt: FieldValue.serverTimestamp(),
        createdVia: 'server_pass',
        createdAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
      });

      segs.forEach((x, i) => {
        writeSlotDoc(t, db, segRefs[i].id, {
          resourceId: RESOURCE_ID, branchId: DEFAULT_BRANCH_ID,
          date, hour: x.start, slotSpanMinutes: x.span,
          bookingStatus: 'confirmed', paymentStatus: 'package',
          expiresAt: null,
        }, { bookingId: bookingRef.id, bookingCode });
      });

      t.update(pkgRef, {
        ...pkgUpdate,
        lastUsedAt: FieldValue.serverTimestamp(),
        lastUsedBooking: bookingCode,
        updatedAt: FieldValue.serverTimestamp(),
      });

      // Package movement entry, in the same commit as the deduction.
      // NOTE: this writes customer_package_logs, the collection the admin
      // adjustment flow already uses. It is deliberately NOT the V2
      // pass_ledger — Addendum 03 sec 3 forbids creating that during the
      // hotfix, and the review's "package ledger entry" requirement is
      // satisfied by recording the movement atomically here.
      const beforeMin = Number(pkg.remainingMinutes);
      const afterMin  = pkgUpdate.remainingMinutes;
      if (Number.isFinite(beforeMin) && Number.isFinite(afterMin)) {
        t.create(ledgerRef, {
          packageId, lineUserId: uid,
          packageType: pkg.packageType,
          packageName: pkg.packageName || pkg.packageType,
          action: 'deduct_minutes',
          oldRemainingMinutes: beforeMin,
          newRemainingMinutes: afterMin,
          deltaMinutes: afterMin - beforeMin,
          reason: `booking ${bookingCode}`,
          bookingId: bookingRef.id,
          source: 'create_pass_booking',
          createdAt: FieldValue.serverTimestamp(),
        });
      }

      // Response snapshot written in the same commit — a retry can never
      // observe a record without one (RB-09).
      writeIdempotencyInTx(t, idemRef, {
        scope: idemScope,
        fingerprint: idemFp,
        response: buildPassBookingResponse({
          bookingId: bookingRef.id, bookingCode, date, startTime, endTime,
          slotIds: segRefs.map(r => r.id), durationMinutes,
          packageId, packageName: pkg.packageName || pkg.packageType,
          uid, customerName, customerPhone, customerNote,
        }),
        nowMs,
      });
    });
  } catch (e) {
    const msg = e.message || '';
    if (msg === 'IDEMPOTENCY_CONFLICT') {
      return res.status(409).json({
        ok: false, code: 'IDEMPOTENCY_CONFLICT',
        error: 'idempotencyKey ถูกใช้กับคำขออื่นแล้ว',
      });
    }
    if (msg === 'PASS_TYPE_MISMATCH') {
      return res.status(409).json({ ok: false, code: 'PASS_TYPE_MISMATCH', error: 'Stored package type does not match requested payType' });
    }
    if (msg === 'PASS_TYPE_UNSUPPORTED') {
      return res.status(409).json({ ok: false, code: 'PASS_TYPE_UNSUPPORTED', error: 'Stored package type is not valid for room booking' });
    }
    if (msg === 'HOLIDAY_CHECK_UNAVAILABLE') {
      return res.status(503).json({ ok: false, code: 'HOLIDAY_CHECK_UNAVAILABLE', error: 'Holiday validation is temporarily unavailable' });
    }
    if (msg.startsWith('SLOT_')) {
      const m = { SLOT_NOT_OPEN: 'ช่องเวลานี้ปิดรับจองแล้ว', SLOT_TAKEN: 'ช่องเวลานี้เพิ่งถูกจอง', SLOT_HELD: 'ช่องเวลานี้ถูกจองค้างอยู่ ลองใหม่อีกครั้ง' };
      return res.status(409).json({ ok: false, code: 'SLOT', error: m[msg] || 'ช่องเวลาไม่ว่าง' });
    }
    if (PASS_ERROR_TEXT[msg]) {
      return res.status(409).json({ ok: false, code: 'PASS', error: PASS_ERROR_TEXT[msg] });
    }
    console.error('[create_pass_booking] tx:', msg);
    return res.status(500).json({ ok: false, error: 'Failed to create booking' });
  }

  // Replay: return the snapshot stored alongside the original mutation.
  // It is always populated, because it was written in the same commit.
  if (replayed) return res.status(200).json({ ...replayed, replayed: true });

  // RB-07: no uid, no packageId, no phone, no name. bookingCode is enough to
  // correlate with the audit log, which is access-controlled.
  console.log(`[create_pass_booking] ${bookingCode} ${payType} ${durationMinutes}min`);
  await writeAuditLog(db, {
    actor: uid, actorRole: 'customer', branchId: DEFAULT_BRANCH_ID,
    action: 'pass_booking_created', targetId: bookingRef.id,
    after: { packageId, packageType: pkgSnapshot?.packageType ?? null, minutesUsed: durationMinutes, bookingCode },
    note: `${date} ${startTime}-${endTime}`,
  });

  return res.status(200).json(buildPassBookingResponse({
    bookingId: bookingRef.id, bookingCode, date, startTime, endTime,
    slotIds: segRefs.map(r => r.id), durationMinutes,
    packageId, packageName: pkgSnapshot?.packageName ?? null,
    uid, customerName, customerPhone, customerNote,
  }));
}

// Single definition of the success payload so the live response and the
// stored idempotency snapshot can never drift apart.
function buildPassBookingResponse({
  bookingId, bookingCode, date, startTime, endTime, slotIds, durationMinutes,
  packageId, packageName,
}) {
  return {
    ok: true,
    booking: {
      id: bookingId, bookingCode, date, startTime, endTime,
      bookingSlotIds: slotIds,
      durationMinutes, durationHours: durationMinutes / 60,
      bookingType: packageName || 'Package Booking',
      price: 0, finalPrice: 0, originalPrice: 0,
      pricingType: 'package',
      bookingStatus: 'confirmed', paymentStatus: 'package',
      packageId, packageName: packageName ?? null,
    },
  };
}

// ════════════════════════════════════════════════════════════════════
// Pass self-purchase — Stage D (LIVE; kill-switch flag = false to disable)
// ════════════════════════════════════════════════════════════════════
// Flow: create purchase (here) → customer pays Dynamic QR → uploads slip
// (existing pass branch: pass_purchases → pending_review) → slip pre-check →
// ADMIN approves (/api/admin-user-action approve_pass_purchase) → package
// issued. A pass is NEVER issued from a slip upload alone.

// pass_catalog — PUBLIC read; enabled:false + empty list while the flag is off.
async function handlePassCatalog(res) {
  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[pass_catalog] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }
  try {
    if (!(await passSelfPurchaseEnabled(db))) {
      return res.status(200).json({ ok: true, enabled: false, passes: [] });
    }
    const passes = Object.entries(PASS_CATALOG)
      .map(([type, p]) => ({ packageType: type, packageName: p.packageName, price: p.price }));
    return res.status(200).json({ ok: true, enabled: true, passes });
  } catch (e) {
    console.error('[pass_catalog]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to load catalog' });
  }
}

// create_pass_purchase — creates the pending pass_purchases doc with the
// SERVER price (client price is never trusted). No slot is held and there is
// no expiry; the purchase just waits for payment + slip + admin approval.
async function handleCreatePassPurchase(res, body) {
  const packageType  = typeof body.packageType === 'string' ? body.packageType.trim() : '';
  const customerName = typeof body.customerName === 'string' ? body.customerName.trim() : '';
  const customerPhone= typeof body.customerPhone === 'string' ? body.customerPhone.trim() : '';
  const lineUserId   = typeof body.lineUserId === 'string' && body.lineUserId ? body.lineUserId : 'guest';
  const lineDisplayName = typeof body.lineDisplayName === 'string' ? body.lineDisplayName : '';

  if (!PASS_CATALOG[packageType]) return res.status(400).json({ ok: false, error: 'Unknown packageType' });
  if (!customerName)  return res.status(400).json({ ok: false, error: 'customerName is required' });
  if (!customerPhone) return res.status(400).json({ ok: false, error: 'customerPhone is required' });
  // Pass approval adds the package to this LINE account — guests can't buy.
  if (lineUserId === 'guest') return res.status(403).json({ ok: false, error: 'กรุณาเปิดผ่าน LINE เพื่อซื้อแพ็กเกจ' });

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[create_pass_purchase] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  if (!(await passSelfPurchaseEnabled(db))) {
    return res.status(403).json({ ok: false, error: 'การซื้อแพ็กเกจออนไลน์ยังไม่เปิดให้บริการ' });
  }

  const cat = PASS_CATALOG[packageType];
  const purchaseCode = genPurchaseCode();
  const ref = db.collection('pass_purchases').doc();
  try {
    await ref.set({
      purchaseCode,
      packageType,
      packageName: cat.packageName,
      price: cat.price,                       // server-authoritative snapshot
      customerName, customerPhone,
      customerPhoneNormalized: normalizePhone(customerPhone),
      lineUserId, lineDisplayName,
      status: 'pending_payment', paymentStatus: 'unpaid',
      slipUrl: null, slipUploadedAt: null,
      issuedPackageId: null,                  // idempotency anchor for approval
      createdVia: 'self_service',
      createdAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
    });
  } catch (e) {
    console.error('[create_pass_purchase] write:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to create purchase' });
  }

  console.log(`[create_pass_purchase] ${purchaseCode} ${packageType} ฿${cat.price} ${lineUserId}`);
  return res.status(200).json({
    ok: true,
    purchase: { id: ref.id, purchaseCode, packageType, packageName: cat.packageName, price: cat.price, customerName },
  });
}

// ════════════════════════════════════════════════════════════════════
// Coach lesson booking — Stage 3 (feature-flagged, OFF by default)
// ════════════════════════════════════════════════════════════════════

// coach_options — PUBLIC read. Lists bookable coaches (active + lessonPrice
// set). Returns enabled:false with an empty list while the flag is off, so
// the client renders nothing. Never exposes payout or auth data.
async function handleCoachOptions(res) {
  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[coach_options] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }
  try {
    if (!(await coachBookingEnabled(db))) {
      return res.status(200).json({ ok: true, enabled: false, coaches: [] });
    }
    const snap = await db.collection('coaches').get();
    const coaches = snap.docs
      .map(d => ({ id: d.id, ...d.data() }))
      .filter(c => c.active !== false && Number.isInteger(c.lessonPrice) && c.lessonPrice > 0)
      .map(c => ({ id: c.id, displayName: c.displayName || c.id, lessonPrice: c.lessonPrice }))
      .sort((a, b) => a.displayName.localeCompare(b.displayName));
    return res.status(200).json({ ok: true, enabled: true, coaches });
  } catch (e) {
    console.error('[coach_options]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to load coaches' });
  }
}

// coach_slots — PUBLIC read. Hours where ROOM availability intersects COACH
// availability for a date: room open, room not live-booked, coach hour open
// (or locked by an expired unpaid hold), and the hour is still in the future.
async function handleCoachSlots(res, body) {
  const date    = typeof body.date === 'string' ? body.date.trim() : '';
  const coachId = typeof body.coachId === 'string' ? body.coachId.trim() : '';
  if (!DATE_RE.test(date)) return res.status(400).json({ ok: false, error: 'date must be YYYY-MM-DD' });
  if (!coachId)            return res.status(400).json({ ok: false, error: 'Missing coachId' });

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[coach_slots] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  try {
    if (!(await coachBookingEnabled(db))) {
      return res.status(200).json({ ok: true, enabled: false, hours: [] });
    }
    const coachSnap = await db.collection('coaches').doc(coachId).get();
    if (!coachSnap.exists) return res.status(404).json({ ok: false, error: 'Coach not found' });
    const coach = coachSnap.data();
    if (coach.active === false || !Number.isInteger(coach.lessonPrice) || coach.lessonPrice <= 0) {
      return res.status(409).json({ ok: false, error: 'โค้ชคนนี้ยังไม่เปิดรับจองผ่านระบบ' });
    }

    const allHours = [];
    for (let h = 0; h < 24; h++) allHours.push(`${String(h).padStart(2, '0')}:00`);
    const caRefs = allHours.map(h => db.collection('coach_availability').doc(coachAvailDocId(coachId, date, h)));
    const [availSnap, slotSnap, caSnaps] = await Promise.all([
      db.collection('available_slots').where('date', '==', date).where('resourceId', '==', RESOURCE_ID).get(),
      db.collection('booking_slots').where('date', '==', date).where('resourceId', '==', RESOURCE_ID).get(),
      db.getAll(...caRefs),
    ]);

    const roomOpen = new Set(availSnap.docs.filter(d => d.data().status === 'open').map(d => d.data().startTime));
    const nowMs = Date.now();
    const roomLive = new Set(slotSnap.docs.filter(d => {
      const sd = d.data();
      return isOccupiedSlot(sd, nowMs);
    }).map(d => d.data().hour));

    const hours = [];
    caSnaps.forEach(s => {
      if (!s.exists) return;
      const ca = s.data();
      const h = ca.hour;
      if (!roomOpen.has(h) || roomLive.has(h)) return;
      const holdExp = ca.holdExpiresAt?.toMillis?.() ?? 0;
      const takeable = ca.status === 'open' || (ca.status === 'booked' && holdExp > 0 && holdExp < nowMs);
      if (!takeable) return;
      // Hour must still be in the future (Bangkok wall clock).
      const start = new Date(`${date}T${h}:00+07:00`).getTime();
      if (!Number.isFinite(start) || start <= nowMs) return;
      hours.push(h);
    });
    hours.sort();
    return res.status(200).json({
      ok: true, enabled: true, coachId,
      coachName: coach.displayName || coachId,
      lessonPrice: coach.lessonPrice,
      hours,
    });
  } catch (e) {
    console.error('[coach_slots]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to load coach slots' });
  }
}

// create_coach_lesson — locks ROOM slot + COACH hour + writes the booking in
// ONE transaction. Price/payout are snapshot from the coaches doc at create
// time (rate changes never affect existing bookings). No vouchers/passes.
async function handleCreateCoachLesson(res, body) {
  const date          = typeof body.date === 'string' ? body.date.trim() : '';
  const startTime     = typeof body.startTime === 'string' ? body.startTime.trim() : '';
  const coachId       = typeof body.coachId === 'string' ? body.coachId.trim() : '';
  const customerName  = typeof body.customerName === 'string' ? body.customerName.trim() : '';
  const customerPhone = typeof body.customerPhone === 'string' ? body.customerPhone.trim() : '';
  const lineUserId    = typeof body.lineUserId === 'string' && body.lineUserId ? body.lineUserId : 'guest';
  const lineDisplayName = typeof body.lineDisplayName === 'string' ? body.lineDisplayName : '';
  const customerNote  = typeof body.customerNote === 'string' ? body.customerNote.slice(0, 500) : '';
  // Coach V2.1: group size (owner rule: 2nd student +฿100, hard max 2) and
  // optional coaching-package redemption (คล้าย Pass แต่ With Coach).
  const students   = body.students === 2 || body.students === '2' ? 2 : 1;
  const usePackage = body.payType === 'coach_package';
  const packageId  = typeof body.packageId === 'string' ? body.packageId.trim() : '';

  if (!DATE_RE.test(date))      return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'date must be YYYY-MM-DD' });
  if (!TIME_RE.test(startTime)) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'startTime must be HH:mm' });
  if (!coachId)       return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'coachId is required' });
  if (!customerName)  return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerName is required' });
  if (!customerPhone) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerPhone is required' });
  if (usePackage && !packageId) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'packageId is required' });
  if (usePackage && students === 2) {
    return res.status(409).json({ ok: false, code: 'VALIDATION', error: 'ใช้แพ็คเกจ + เรียน 2 คน กรุณาแจ้งที่ร้าน (ชำระค่าคนที่ 2 หน้าร้าน)' });
  }

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[create_coach_lesson] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  if (!(await coachBookingEnabled(db))) {
    return res.status(403).json({ ok: false, error: 'การจองพร้อมโค้ชยังไม่เปิดให้บริการ' });
  }

  let coach;
  try {
    const snap = await db.collection('coaches').doc(coachId).get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Coach not found' });
    coach = snap.data();
  } catch (e) { console.error('[create_coach_lesson] coach read:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  if (coach.active === false || !Number.isInteger(coach.lessonPrice) || coach.lessonPrice <= 0) {
    return res.status(409).json({ ok: false, error: 'โค้ชคนนี้ยังไม่เปิดรับจองผ่านระบบ' });
  }
  const extraPersonFee = students === 2 ? COACH_EXTRA_PERSON_FEE : 0;
  const customerPrice  = usePackage ? 0 : coach.lessonPrice + extraPersonFee;   // QR total INCLUDES room (+2nd student)
  const coachPayoutAmount = (Number.isInteger(coach.payoutPerHour) && coach.payoutPerHour >= 500)
    ? coach.payoutPerHour : 500;             // business minimum 500 THB/hour

  // ── Package redemption guards (identity REQUIRED via Firebase ID token —
  //    minted by /api/auth-line, uid = verified LINE userId) ─────────────
  let pkg = null;
  if (usePackage) {
    let uid = null;
    try {
      if (typeof body.idToken === 'string' && body.idToken.trim()) {
        uid = (await getAdminAuth().verifyIdToken(body.idToken.trim())).uid;
      }
    } catch { /* fall through to the 403 below */ }
    if (!uid) return res.status(403).json({ ok: false, error: 'กรุณาเข้าสู่ระบบผ่าน LINE เพื่อใช้แพ็คเกจ' });
    try {
      const ps = await db.collection('customer_packages').doc(packageId).get();
      if (!ps.exists) return res.status(404).json({ ok: false, error: 'ไม่พบแพ็คเกจ' });
      pkg = ps.data();
    } catch (e) { console.error('[create_coach_lesson] pkg read:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }
    if (pkg.lineUserId !== uid) return res.status(403).json({ ok: false, error: 'แพ็คเกจนี้ไม่ใช่ของบัญชีนี้' });
    if (!COACH_PACKAGE_TYPES.includes(pkg.packageType)) {
      return res.status(409).json({ ok: false, error: 'แพ็คเกจนี้ใช้จองคาบสอนโค้ชไม่ได้' });
    }
    if (pkg.status !== 'active') return res.status(409).json({ ok: false, error: 'แพ็คเกจไม่พร้อมใช้งาน' });
    if ((Number(pkg.remainingMinutes) || 0) < 60) {
      return res.status(409).json({ ok: false, error: 'ชั่วโมงในแพ็คเกจไม่พอ (ต้องมีอย่างน้อย 1 ชั่วโมง)' });
    }
    const validUntil = pkg.validUntil?.toDate?.()?.toISOString?.()?.slice(0, 10) ?? null;
    if (validUntil && date > validUntil) {
      return res.status(409).json({ ok: false, error: `แพ็คเกจหมดอายุ ${validUntil} — เลือกวันก่อนหมดอายุ` });
    }
  }

  const nowMs = Date.now();
  const startMs = new Date(`${date}T${startTime}:00+07:00`).getTime();
  if (!Number.isFinite(startMs) || startMs <= nowMs) {
    return res.status(409).json({ ok: false, code: 'SLOT', error: 'ช่วงเวลานี้ผ่านมาแล้ว' });
  }

  const endTime          = nextHourEnd(startTime);
  const bookingCode      = genBookingCode();
  const paymentExpiresAt = Timestamp.fromMillis(nowMs + PAY_MINS * 60 * 1000);

  const bookingRef    = db.collection('bookings').doc();
  const slotRef       = db.collection('booking_slots').doc(slotIdOf(date, startTime));
  const availRef      = db.collection('available_slots').doc(slotIdOf(date, startTime));
  const coachAvailRef = db.collection('coach_availability').doc(coachAvailDocId(coachId, date, startTime));
  const pkgRef        = usePackage ? db.collection('customer_packages').doc(packageId) : null;

  // Package bookings are confirmed instantly (no payment window, no QR).
  const bkStatus  = usePackage ? 'confirmed' : 'pending_payment';
  const payStatus = usePackage ? 'package'   : 'unpaid';
  const holdExp   = usePackage ? null        : paymentExpiresAt;

  try {
    await db.runTransaction(async (t) => {
      const [slotSnap, availSnap, caSnap, pkgSnap] = await Promise.all([
        t.get(slotRef), t.get(availRef), t.get(coachAvailRef),
        pkgRef ? t.get(pkgRef) : Promise.resolve(null),
      ]);

      // ── Package re-validation INSIDE the transaction (atomic deduct) ──
      if (pkgRef) {
        if (!pkgSnap.exists) throw new Error('PKG_GONE');
        const p = pkgSnap.data();
        if (p.status !== 'active' || (Number(p.remainingMinutes) || 0) < 60) throw new Error('PKG_EMPTY');
      }

      // ── Room guards (identical rules to handleCreate) ───────────────
      if (!availSnap.exists || availSnap.data().status !== 'open') throw new Error('SLOT_NOT_OPEN');
      if (slotSnap.exists) {
        const sd = slotSnap.data();
        if (isOccupiedSlot(sd, nowMs)) {
          throw new Error(sd.bookingStatus === 'pending_payment' && sd.paymentStatus !== 'pending_review'
            ? 'SLOT_HELD' : 'SLOT_TAKEN');
        }
      }

      // ── Coach guards: hour must be offered and not live-locked ──────
      if (!caSnap.exists) throw new Error('COACH_NOT_OPEN');
      const ca = caSnap.data();
      if (ca.status === 'booked') {
        const holdExp = ca.holdExpiresAt?.toMillis?.() ?? 0;
        // A dead unpaid hold is reusable; anything else is locked. Slip-
        // uploaded lessons keep the ROOM slot confirmed, so the room guard
        // above already blocks them — this covers the pure coach lock.
        if (!(holdExp > 0 && holdExp < nowMs)) throw new Error('COACH_HELD');
      } else if (ca.status !== 'open') {
        throw new Error('COACH_NOT_OPEN');
      }

      // ── Writes: booking + room lock + coach lock (+ package deduct) ──
      t.set(bookingRef, {
        bookingCode, resourceId: RESOURCE_ID, branchId: ca.branchId || DEFAULT_BRANCH_ID,
        bookingType: 'Coach Lesson',
        serviceType: 'coach_lesson',
        coachId, coachName: coach.displayName || coachId,
        customerPrice, coachPayoutAmount, coachPayoutStatus: 'pending',
        studentCount: students, extraPersonFee,
        lessonStatus: 'scheduled',
        lineUserId, lineDisplayName,
        customerName, customerPhone, customerPhoneNormalized: normalizePhone(customerPhone),
        customerNote,
        date, startTime, endTime, durationHours: 1, durationMinutes: 60,
        price: customerPrice, amount: customerPrice,
        originalPrice: customerPrice, finalPrice: customerPrice,
        basePrice: customerPrice, effectivePrice: customerPrice,
        pricingType: 'coach_lesson', pricingMode: 'coach_lesson',
        promoCode: null, voucherCode: null, discountAmount: 0,
        qrAmount: customerPrice, qrType: 'normal', paymentQrType: 'normal',
        promoApplied: false,
        bookingStatus: bkStatus, paymentStatus: payStatus,
        paymentExpiresAt: holdExp,
        ...(usePackage ? {
          packageId, packageType: pkg.packageType, packageName: pkg.packageName || pkg.packageType,
          usedPackageId: packageId, usedPackageType: pkg.packageType, usedPackageName: pkg.packageName || pkg.packageType,
          packageLessonValue: coach.lessonPrice,   // reference value of the redeemed lesson
          confirmedAt: FieldValue.serverTimestamp(),
        } : {}),
        slipUrl: null, slipUploadedAt: null, cancelReason: null,
        createdVia: 'server',
        createdAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
      });
      // SL-02: coachId is deliberately NOT written here. booking_slots stays
      // publicly readable, and which coach teaches which hour is the coach's
      // information, not availability data. The coach lock lives on
      // coach_availability (written below), which is server-only.
      writeSlotDoc(t, db, slotRef.id, {
        resourceId: RESOURCE_ID,
        branchId: ca.branchId || DEFAULT_BRANCH_ID,
        date, hour: startTime, slotSpanMinutes: 60,
        bookingStatus: bkStatus, paymentStatus: payStatus,
        expiresAt: holdExp,
      }, { bookingId: bookingRef.id, bookingCode, coachId });
      t.set(coachAvailRef, {
        coachId, branchId: ca.branchId || DEFAULT_BRANCH_ID,
        date, hour: startTime,
        status: 'booked',
        bookingId: bookingRef.id, bookingCode,
        holdExpiresAt: holdExp,
        updatedAt: FieldValue.serverTimestamp(),
      });
      if (pkgRef) {
        const p = pkgSnap.data();
        t.update(pkgRef, {
          remainingMinutes: (Number(p.remainingMinutes) || 0) - 60,
          lastUsedAt: FieldValue.serverTimestamp(),
          lastUsedBooking: bookingCode,
          updatedAt: FieldValue.serverTimestamp(),
        });
      }
    });
  } catch (e) {
    const msg = e.message || '';
    const m = {
      SLOT_NOT_OPEN:  'ช่องเวลานี้ปิดรับจองแล้ว',
      SLOT_TAKEN:     'ช่องเวลานี้เพิ่งถูกจอง',
      SLOT_HELD:      'ช่องเวลานี้ถูกจองค้างอยู่ ลองใหม่อีกครั้ง',
      COACH_NOT_OPEN: 'โค้ชไม่ได้เปิดรับสอนช่วงเวลานี้แล้ว',
      COACH_HELD:     'ช่วงเวลานี้ของโค้ชเพิ่งถูกจอง',
      PKG_GONE:       'ไม่พบแพ็คเกจ กรุณารีเฟรช',
      PKG_EMPTY:      'ชั่วโมงในแพ็คเกจไม่พอแล้ว กรุณารีเฟรช',
    };
    if (m[msg]) return res.status(409).json({ ok: false, code: 'SLOT', error: m[msg] });
    console.error('[create_coach_lesson] tx:', msg);
    return res.status(500).json({ ok: false, error: 'Failed to create coach lesson booking' });
  }

  console.log(`[create_coach_lesson] ${bookingCode} coach:${coachId} ${usePackage ? `PKG:${packageId}` : `฿${customerPrice}`} students:${students} payout:฿${coachPayoutAmount}`);
  return res.status(200).json({
    ok: true,
    usedPackage: usePackage,
    paymentExpiresAt: usePackage ? null : paymentExpiresAt.toDate().toISOString(),
    booking: {
      id: bookingRef.id, bookingCode, date, startTime, endTime,
      bookingType: 'Coach Lesson', serviceType: 'coach_lesson',
      coachId, coachName: coach.displayName || coachId,
      studentCount: students, extraPersonFee,
      finalPrice: customerPrice, price: customerPrice, originalPrice: customerPrice,
      qrType: 'normal', qrAmount: customerPrice, paymentQrType: 'normal',
      pricingType: 'coach_lesson', discountAmount: 0, voucherCode: null,
      bookingStatus: bkStatus, paymentStatus: payStatus,
      lineUserId, customerName, customerPhone, customerNote,
      ...(usePackage ? { packageName: pkg.packageName || pkg.packageType } : {}),
    },
  });
}

// ════════════════════════════════════════════════════════════════════
// Coach Add-on / Mixed Payment v2 (explicit flag, OFF by default)
// ════════════════════════════════════════════════════════════════════

const v2TouchedHours = cells => [...new Set(cells.map(cell => `${cell.slice(0, 2)}:00`))];
const v2BookingIsLive = (booking, nowMs = Date.now()) => {
  if (!booking || ['cancelled', 'expired', 'completed', 'no_show', 'rescheduled'].includes(booking.bookingState || booking.bookingStatus)) return false;
  if ((booking.bookingState || booking.bookingStatus) === 'held' || booking.bookingStatus === 'pending_payment') {
    const exp = booking.paymentExpiresAt?.toMillis?.() ?? 0;
    return !exp || exp > nowMs;
  }
  return ['confirmed', 'pending_review'].includes(booking.bookingState || booking.bookingStatus) ||
    ['paid', 'package', 'pending_review'].includes(booking.paymentStatus);
};
const v2RangesOverlap = (aStart, aDuration, bStart, bDuration) => {
  const a = toMin(aStart), b = toMin(bStart);
  return a < b + bDuration && b < a + aDuration;
};

function v2CourtQuoteFromSnapshots({ date, startTime, durationMinutes, lineUserId, pricing, isHoliday }) {
  const segs = segmentsOf(startTime, durationMinutes);
  if (!segs) throw new Error('INVALID_DURATION');
  const hasHalf = segs.some(segment => segment.span === 30);
  const promoConfig = (!hasHalf && pricing) ? pricing : null;
  const halfPrice = halfPriceFrom(pricing);
  const segQuotes = segs.map(segment => segment.span === 30
    ? halfSegQuote(segment.start, halfPrice, flatHalfMetadata(date, segment.start, isHoliday))
    : {
        ...computeQuote({
          date, startTime: segment.start, nowMs: Date.now(), isHoliday,
          promoConfig, payType: 'single', voucherCode: null, voucher: null, lineUserId,
        }),
        startTime: segment.start, span: 60,
      });
  return {
    segs,
    segQuotes,
    quote: durationMinutes === 60
      ? segQuotes[0]
      : { ...segQuotes.find(q => q.span === 60) || segQuotes[0], ...combineQuotes(segQuotes) },
  };
}

async function v2VerifiedUid(body) {
  const idToken = typeof body.idToken === 'string' ? body.idToken.trim() : '';
  if (!idToken) return null;
  try { return (await getAdminAuth().verifyIdToken(idToken)).uid || null; }
  catch { return null; }
}

async function v2LoadPackage(db, body, uid, durationMinutes) {
  const fundingMode = String(body.fundingMode || 'cash');
  if (fundingMode === 'cash') return { fundingMode, packageId: '', pkg: null, kind: null };
  if (!uid) throw new Error('AUTH_REQUIRED');
  const packageId = typeof body.packageId === 'string' ? body.packageId.trim() : '';
  if (!packageId) throw new Error('PACKAGE_REQUIRED');
  const snap = await db.collection('customer_packages').doc(packageId).get();
  if (!snap.exists) throw new Error('PACKAGE_MISSING');
  const pkg = snap.data();
  const kind = coachAddonV2PackageKind(pkg.packageType);
  if (!kind) throw new Error('PACKAGE_TYPE_UNSUPPORTED');
  if (kind !== fundingMode) throw new Error('PACKAGE_TYPE_MISMATCH');
  if (pkg.lineUserId !== uid) throw new Error('PACKAGE_NOT_OWNED');
  if (pkg.status !== 'active') throw new Error('PACKAGE_INACTIVE');
  const validUntil = pkg.validUntil?.toMillis?.() ?? null;
  if (!validUntil || validUntil < Date.now()) throw new Error('PACKAGE_EXPIRED');
  if (!Number.isFinite(Number(pkg.remainingMinutes)) || Number(pkg.remainingMinutes) < durationMinutes) throw new Error('PACKAGE_INSUFFICIENT');
  return { fundingMode, packageId, pkg, kind };
}

const V2_ERROR_TEXT = {
  AUTH_REQUIRED: 'กรุณาเข้าสู่ระบบผ่าน LINE เพื่อใช้แพ็คเกจ',
  PACKAGE_REQUIRED: 'กรุณาเลือกแพ็คเกจ',
  PACKAGE_MISSING: 'ไม่พบแพ็คเกจ',
  PACKAGE_TYPE_UNSUPPORTED: 'แพ็คเกจนี้ยังไม่รองรับ Coach Add-on v2',
  PACKAGE_TYPE_MISMATCH: 'ประเภทแพ็คเกจไม่ตรงกับวิธีชำระ',
  PACKAGE_NOT_OWNED: 'แพ็คเกจนี้ไม่ใช่ของบัญชีนี้',
  PACKAGE_INACTIVE: 'แพ็คเกจไม่พร้อมใช้งาน',
  PACKAGE_EXPIRED: 'แพ็คเกจหมดอายุแล้ว',
  PACKAGE_INSUFFICIENT: 'เวลาในแพ็คเกจไม่เพียงพอ',
  INVALID_LESSON_RATE: 'ราคาคาบสอนไม่ถูกต้อง กรุณาแจ้ง Admin',
  LESSON_PRICE_BELOW_COURT: 'ราคาคาบสอนต่ำกว่าค่าคอร์ท กรุณาแจ้ง Admin',
};

async function handleCoachAddonV2Options(res, body) {
  const date = typeof body.date === 'string' ? body.date.trim() : '';
  const startTime = typeof body.startTime === 'string' ? body.startTime.trim() : '';
  const durationMinutes = Number(body.durationMinutes);
  if (!DATE_RE.test(date) || !TIME_RE.test(startTime) || !isCoachAddonV2Duration(durationMinutes)) {
    return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Coach Add-on requires 60-180 minutes in 30-minute steps' });
  }
  const cells = coachClaimCellStarts(startTime, durationMinutes);
  if (!cells) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Invalid coach time range' });
  let db;
  try { db = getAdminDb(); }
  catch { return res.status(500).json({ ok: false, error: 'Server error' }); }
  if (!(await coachAddonV2Enabled(db))) return res.status(200).json({ ok: true, enabled: false, coaches: [] });

  try {
    const [coachSnap, legacyBookingsSnap] = await Promise.all([
      db.collection('coaches').get(),
      db.collection('bookings').where('date', '==', date).get(),
    ]);
    const nowMs = Date.now();
    const legacyLive = legacyBookingsSnap.docs.map(doc => ({ id: doc.id, ...doc.data() }))
      .filter(booking => booking.coachId && v2BookingIsLive(booking, nowMs));
    const coaches = [];
    for (const doc of coachSnap.docs) {
      const coach = doc.data();
      if (coach.active === false || !Number.isFinite(Number(coach.lessonPrice)) || Number(coach.lessonPrice) <= 0) continue;
      const hours = v2TouchedHours(cells);
      const scheduleRefs = hours.map(hour => db.collection('coach_availability').doc(coachAvailDocId(doc.id, date, hour)));
      const claimRefs = cells.map(cell => db.collection('coach_slot_claims').doc(coachClaimId(doc.id, date, cell)));
      const [scheduleSnaps, claimSnaps] = await Promise.all([db.getAll(...scheduleRefs), db.getAll(...claimRefs)]);
      if (scheduleSnaps.some(snap => !snap.exists || snap.data().status !== 'open')) continue;
      if (claimSnaps.some(snap => snap.exists && isActiveCoachClaim(snap.data(), nowMs))) continue;
      if (legacyLive.some(booking => booking.coachId === doc.id &&
          v2RangesOverlap(startTime, durationMinutes, booking.startTime, Number(booking.durationMinutes) || 60))) continue;
      coaches.push({
        id: doc.id,
        displayName: coach.displayName || doc.id,
        bio: typeof coach.bio === 'string' ? coach.bio : '',
        photoUrl: typeof coach.photoUrl === 'string' ? coach.photoUrl : null,
        lessonPrice: Number(coach.lessonPrice),
      });
    }
    coaches.sort((a, b) => a.displayName.localeCompare(b.displayName));
    return res.status(200).json({ ok: true, enabled: true, date, startTime, durationMinutes, coaches });
  } catch (e) {
    console.error('[coach_addon_v2_options]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to load available coaches' });
  }
}

async function handleCoachAddonV2Quote(res, body) {
  const date = typeof body.date === 'string' ? body.date.trim() : '';
  const startTime = typeof body.startTime === 'string' ? body.startTime.trim() : '';
  const coachId = typeof body.coachId === 'string' ? body.coachId.trim() : '';
  const durationMinutes = Number(body.durationMinutes);
  const studentCount = body.studentCount === 2 || body.studentCount === '2' ? 2 : 1;
  if (!DATE_RE.test(date) || !TIME_RE.test(startTime) || !coachId || !isCoachAddonV2Duration(durationMinutes)) {
    return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Invalid Coach Add-on quote' });
  }
  let db;
  try { db = getAdminDb(); }
  catch { return res.status(500).json({ ok: false, error: 'Server error' }); }
  if (!(await coachAddonV2Enabled(db))) return res.status(403).json({ ok: false, code: 'DISABLED', error: 'Coach Add-on v2 is disabled' });
  const uid = await v2VerifiedUid(body);
  try {
    const [coachSnap, pricingSnap, holidaySnap] = await Promise.all([
      db.collection('coaches').doc(coachId).get(),
      db.collection('system_settings').doc('pricing').get(),
      db.collection('holidays').doc(date).get(),
    ]);
    if (!coachSnap.exists || coachSnap.data().active === false) throw new Error('COACH_UNAVAILABLE');
    const coach = coachSnap.data();
    const packageCtx = await v2LoadPackage(db, body, uid, durationMinutes);
    const court = v2CourtQuoteFromSnapshots({
      date, startTime, durationMinutes, lineUserId: uid || 'guest',
      pricing: pricingSnap.exists ? pricingSnap.data() : null,
      isHoliday: holidaySnap.exists && holidaySnap.data().isHoliday === true,
    });
    const price = calculateCoachAddonV2Price({
      durationMinutes, fundingMode: packageCtx.fundingMode,
      courtGrossAmount: court.quote.finalPrice,
      lessonRatePerHour: Number(coach.lessonPrice),
      coachPayoutRatePerHour: Number(coach.payoutPerHour) || 500,
      studentCount,
    });
    return res.status(200).json({ ok: true, quote: price, coach: { id: coachId, displayName: coach.displayName || coachId } });
  } catch (e) {
    if (V2_ERROR_TEXT[e.message]) return res.status(409).json({ ok: false, code: e.message, error: V2_ERROR_TEXT[e.message] });
    if (e.message === 'COACH_UNAVAILABLE') return res.status(409).json({ ok: false, code: 'COACH_UNAVAILABLE', error: 'โค้ชไม่พร้อมรับจอง' });
    if (e.code === 'MIXED_RECEIVER') return res.status(409).json({ ok: false, code: 'MIXED_RECEIVER', error: 'ช่วงเวลานี้ไม่สามารถรวมยอดชำระได้' });
    console.error('[coach_addon_v2_quote]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to quote Coach Add-on' });
  }
}

async function handleCreateCoachAddonV2(req, res, body) {
  const date = typeof body.date === 'string' ? body.date.trim() : '';
  const startTime = typeof body.startTime === 'string' ? body.startTime.trim() : '';
  const coachId = typeof body.coachId === 'string' ? body.coachId.trim() : '';
  const customerName = typeof body.customerName === 'string' ? body.customerName.trim() : '';
  const customerPhone = typeof body.customerPhone === 'string' ? body.customerPhone.trim() : '';
  const customerNote = typeof body.customerNote === 'string' ? body.customerNote.slice(0, 500) : '';
  const lineDisplayName = typeof body.lineDisplayName === 'string' ? body.lineDisplayName : '';
  const durationMinutes = Number(body.durationMinutes);
  const studentCount = body.studentCount === 2 || body.studentCount === '2' ? 2 : 1;
  const idemKey = typeof body.idempotencyKey === 'string' ? body.idempotencyKey.trim() : '';
  if (!DATE_RE.test(date) || !TIME_RE.test(startTime) || !coachId || !customerName || !customerPhone || !isCoachAddonV2Duration(durationMinutes)) {
    return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Invalid Coach Add-on booking' });
  }
  if (!isValidIdempotencyKey(idemKey)) return res.status(400).json({ ok: false, code: 'IDEMPOTENCY', error: 'idempotencyKey has invalid format or length' });
  const cells = coachClaimCellStarts(startTime, durationMinutes);
  if (!cells) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Invalid coach time range' });

  let db;
  try { db = getAdminDb(); }
  catch { return res.status(500).json({ ok: false, error: 'Server error' }); }
  if (!(await coachAddonV2Enabled(db))) return res.status(403).json({ ok: false, code: 'DISABLED', error: 'Coach Add-on v2 is disabled' });

  const uid = await v2VerifiedUid(body);
  const assertedUid = typeof body.lineUserId === 'string' ? body.lineUserId : 'guest';
  if (body.idToken && !uid) return res.status(401).json({ ok: false, code: 'AUTH', error: 'Session expired' });
  if (uid && assertedUid !== 'guest' && assertedUid !== uid) return res.status(409).json({ ok: false, code: 'IDENTITY_MISMATCH', error: 'Authenticated account does not match request' });
  const lineUserId = uid || 'guest';

  const nowMs = Date.now();
  const startMs = Date.parse(`${date}T${startTime}:00+07:00`);
  if (!Number.isFinite(startMs) || startMs <= nowMs) return res.status(409).json({ ok: false, code: 'SLOT', error: 'ช่วงเวลานี้ผ่านมาแล้ว' });

  let packageCtx;
  try { packageCtx = await v2LoadPackage(db, body, uid, durationMinutes); }
  catch (e) { return res.status(409).json({ ok: false, code: e.message, error: V2_ERROR_TEXT[e.message] || 'แพ็คเกจไม่พร้อมใช้งาน' }); }

  const coachClaimRefs = cells.map(cell => db.collection('coach_slot_claims').doc(coachClaimId(coachId, date, cell)));
  // Lazy cleanup keeps package reservations from remaining deducted after an
  // abandoned 15-minute hold.  The create transaction itself still rechecks
  // every deterministic claim, so this pre-pass is never the concurrency gate.
  try {
    const stale = await db.getAll(...coachClaimRefs);
    const expiredOwners = [...new Set(stale.filter(s => s.exists && s.data().status === 'held' &&
      (s.data().expiresAt?.toMillis?.() ?? Infinity) <= nowMs).map(s => s.data().bookingId).filter(Boolean))];
    for (const expiredBookingId of expiredOwners) {
      await releaseCoachAddonV2Hold(db, expiredBookingId, { nowMs, reason: 'lazy_claim_reclaim', actor: 'system' }).catch(() => null);
    }
  } catch (e) { console.warn('[coach addon v2] lazy cleanup:', e.message); }

  const startMin = toMin(startTime);
  const needCells = []; for (let m = startMin; m < startMin + durationMinutes; m += 30) needCells.push(m);
  const touchedHours = [...new Set(needCells.map(m => Math.floor(m / 60)))];
  const segs = segmentsOf(startTime, durationMinutes);
  const bookingRef = db.collection('bookings').doc();
  const segRefs = segs.map(segment => db.collection('booking_slots').doc(slotIdOf(date, segment.start)));
  const cellRefs = touchedHours.flatMap(hour => [
    db.collection('booking_slots').doc(slotIdOf(date, `${String(hour).padStart(2, '0')}:00`)),
    db.collection('booking_slots').doc(slotIdOf(date, `${String(hour).padStart(2, '0')}:30`)),
  ]);
  const availRefs = touchedHours.map(hour => db.collection('available_slots').doc(slotIdOf(date, `${String(hour).padStart(2, '0')}:00`)));
  const scheduleRefs = v2TouchedHours(cells).map(hour => db.collection('coach_availability').doc(coachAvailDocId(coachId, date, hour)));
  const coachRef = db.collection('coaches').doc(coachId);
  const pricingRef = db.collection('system_settings').doc('pricing');
  const holidayRef = db.collection('holidays').doc(date);
  const packageRef = packageCtx.packageId ? db.collection('customer_packages').doc(packageCtx.packageId) : null;
  const bookingCode = genBookingCode();
  const endTime = endTimeAfterMin(startTime, durationMinutes);
  const holdExpiresAt = Timestamp.fromMillis(nowMs + PAY_MINS * 60 * 1000);
  const idemScope = `create_coach_addon_v2:${lineUserId}`;
  const idemRef = idempotencyRef(db, idemKey, idemScope);
  const idemFp = fingerprintOf({ lineUserId, date, startTime, durationMinutes, coachId, studentCount, fundingMode: packageCtx.fundingMode, packageId: packageCtx.packageId, customerName, customerPhone, customerNote });
  const guestAccess = lineUserId === 'guest' ? prepareGuestAccess({ bookingEndMs: Date.parse(`${date}T${endTime}:00+07:00`), nowMs }) : null;
  const guestAccessRef = guestAccess ? db.collection(GUEST_ACCESS_COLLECTION).doc(bookingRef.id) : null;
  let replayed = null;
  let response = null;

  try {
    await db.runTransaction(async t => {
      const idem = await readIdempotencyInTx(t, idemRef, idemFp);
      if (idem.state === 'conflict') throw new Error('IDEMPOTENCY_CONFLICT');
      if (idem.state === 'replay') { replayed = idem.response; return; }
      const snaps = await Promise.all([
        ...cellRefs.map(ref => t.get(ref)), ...availRefs.map(ref => t.get(ref)),
        ...scheduleRefs.map(ref => t.get(ref)), ...coachClaimRefs.map(ref => t.get(ref)),
        t.get(coachRef), t.get(pricingRef), t.get(holidayRef),
        ...(packageRef ? [t.get(packageRef)] : []),
      ]);
      let at = 0;
      const cellSnaps = snaps.slice(at, at += cellRefs.length);
      const availSnaps = snaps.slice(at, at += availRefs.length);
      const scheduleSnaps = snaps.slice(at, at += scheduleRefs.length);
      const coachClaimSnaps = snaps.slice(at, at += coachClaimRefs.length);
      const coachSnap = snaps[at++], pricingSnap = snaps[at++], holidaySnap = snaps[at++];
      const packageSnap = packageRef ? snaps[at] : null;

      if (!coachSnap.exists || coachSnap.data().active === false) throw new Error('COACH_UNAVAILABLE');
      const coach = coachSnap.data();
      if (!Number.isFinite(Number(coach.lessonPrice)) || Number(coach.lessonPrice) <= 0) throw new Error('COACH_UNAVAILABLE');
      if (availSnaps.some(snap => !snap.exists || snap.data().status !== 'open')) throw new Error('SLOT_NOT_OPEN');
      if (scheduleSnaps.some(snap => !snap.exists || snap.data().status !== 'open')) throw new Error('COACH_NOT_OPEN');

      cellSnaps.forEach((snap, index) => {
        if (!snap.exists) return;
        const data = snap.data();
        const docMin = touchedHours[Math.floor(index / 2)] * 60 + (index % 2) * 30;
        const docSpan = data.slotSpanMinutes === 30 ? 30 : 60;
        if (needCells.some(cell => cell >= docMin && cell < docMin + docSpan) && isOccupiedSlot(data, nowMs)) {
          throw new Error('SLOT_TAKEN');
        }
      });
      coachClaimSnaps.forEach(snap => {
        if (!snap.exists) return;
        if (isActiveCoachClaim(snap.data(), nowMs)) throw new Error('COACH_TAKEN');
        throw new Error('COACH_EXPIRED_RETRY');
      });

      const isHoliday = holidaySnap.exists && holidaySnap.data().isHoliday === true;
      const court = v2CourtQuoteFromSnapshots({
        date, startTime, durationMinutes, lineUserId,
        pricing: pricingSnap.exists ? pricingSnap.data() : null, isHoliday,
      });

      let packageUpdate = null;
      let packageData = null;
      if (packageRef) {
        if (!packageSnap.exists) throw new Error('PACKAGE_MISSING');
        packageData = packageSnap.data();
        const kind = coachAddonV2PackageKind(packageData.packageType);
        if (!kind) throw new Error('PACKAGE_TYPE_UNSUPPORTED');
        if (kind !== packageCtx.fundingMode) throw new Error('PACKAGE_TYPE_MISMATCH');
        if (packageData.lineUserId !== uid) throw new Error('PACKAGE_NOT_OWNED');
        if (packageData.status !== 'active') throw new Error('PACKAGE_INACTIVE');
        const validUntil = packageData.validUntil?.toMillis?.() ?? null;
        if (!validUntil || validUntil < nowMs) throw new Error('PACKAGE_EXPIRED');
        const remaining = Number(packageData.remainingMinutes);
        if (!Number.isFinite(remaining) || remaining < durationMinutes) throw new Error('PACKAGE_INSUFFICIENT');
        packageUpdate = { remainingMinutes: remaining - durationMinutes };
      }

      const price = calculateCoachAddonV2Price({
        durationMinutes, fundingMode: packageCtx.fundingMode,
        courtGrossAmount: court.quote.finalPrice,
        lessonRatePerHour: Number(coach.lessonPrice),
        coachPayoutRatePerHour: Number(coach.payoutPerHour) || 500,
        studentCount,
      });
      const states = initialCoachAddonV2States(price);
      const expiresAt = states.bookingState === 'held' ? holdExpiresAt : null;
      const claimStatus = states.bookingState === 'held' ? 'held' : 'confirmed';
      const claimPaymentStatus = states.cashState === 'not_required' ? 'package' : 'unpaid';
      const coachClaimIds = coachClaimRefs.map(ref => ref.id);

      const bookingData = {
        coachAddonSchemaVersion: 2,
        bookingCode, resourceId: RESOURCE_ID, branchId: DEFAULT_BRANCH_ID,
        bookingSlotIds: segRefs.map(ref => ref.id), coachClaimIds,
        bookingType: 'Coach Add-on v2', serviceType: 'coach_lesson',
        serviceCategory: price.serviceCategory, fundingSource: price.fundingSource,
        bookingState: states.bookingState, cashState: states.cashState,
        packageUsageState: states.packageUsageState,
        coachId, coachName: coach.displayName || coachId,
        lessonPriceAtBooking: Number(coach.lessonPrice),
        coachPriceAtBooking: Number(coach.lessonPrice),
        coachPayoutRateAtBooking: Number(coach.payoutPerHour) || 500,
        coachPayoutStatus: 'pending', lessonStatus: 'scheduled',
        ...price,
        priceBreakdown: { ...price },
        coachPayoutBreakdown: {
          coachPayoutRatePerHour: price.coachPayoutRatePerHour,
          coachBasePayoutAmount: price.coachBasePayoutAmount,
          extraPersonCoachPayout: price.extraPersonCoachPayout,
          coachPayoutAmount: price.coachPayoutAmount,
        },
        lineUserId, lineDisplayName,
        customerName, customerPhone, customerPhoneNormalized: normalizePhone(customerPhone), customerNote,
        date, startTime, endTime, durationMinutes, durationHours: durationMinutes / 60,
        price: price.cashDueAmount, amount: price.cashDueAmount,
        finalPrice: price.cashDueAmount, originalPrice: price.cashDueAmount,
        basePrice: price.cashDueAmount, effectivePrice: price.cashDueAmount,
        pricingType: 'coach_addon_v2', pricingMode: 'coach_addon_v2', priceRuleVersion: 'coach-addon-v2',
        qrAmount: price.cashDueAmount,
        qrType: packageCtx.fundingMode === 'cash' ? court.quote.qrType : 'normal',
        paymentQrType: packageCtx.fundingMode === 'cash' ? court.quote.qrType : 'normal',
        bookingStatus: states.legacyBookingStatus, paymentStatus: states.legacyPaymentStatus,
        status: states.legacyBookingStatus, paymentExpiresAt: expiresAt,
        slipUrl: null, slipUploadedAt: null, cancelReason: null,
        ...(packageRef ? {
          packageId: packageCtx.packageId,
          packageType: packageData.packageType,
          packageName: packageData.packageName || packageData.packageType,
          usedPackageId: packageCtx.packageId,
          usedPackageType: packageData.packageType,
          usedPackageName: packageData.packageName || packageData.packageType,
          packageMinutesUsed: durationMinutes,
        } : {}),
        ...(states.bookingState === 'confirmed' ? { confirmedAt: FieldValue.serverTimestamp() } : {}),
        createdVia: 'coach_addon_v2',
        createdAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
      };
      t.create(bookingRef, bookingData);
      segs.forEach((segment, index) => writeSlotDoc(t, db, segRefs[index].id, {
        resourceId: RESOURCE_ID, branchId: DEFAULT_BRANCH_ID,
        date, hour: segment.start, slotSpanMinutes: segment.span,
        bookingStatus: states.legacyBookingStatus, paymentStatus: claimPaymentStatus, expiresAt,
      }, { bookingId: bookingRef.id, bookingCode, coachId }));
      coachClaimRefs.forEach((ref, index) => t.set(ref, {
        coachId, branchId: DEFAULT_BRANCH_ID, date, cellStart: cells[index], slotSpanMinutes: 30,
        bookingId: bookingRef.id, bookingCode, status: claimStatus, expiresAt,
        createdAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
      }));

      if (packageRef) {
        t.update(packageRef, { ...packageUpdate, lastReservedAt: FieldValue.serverTimestamp(), lastReservedBooking: bookingCode, updatedAt: FieldValue.serverTimestamp() });
        t.create(db.collection('customer_package_logs').doc(), {
          packageId: packageCtx.packageId, lineUserId,
          packageType: packageData.packageType, packageName: packageData.packageName || packageData.packageType,
          action: states.packageUsageState === 'reserved' ? 'reserve_minutes' : 'consume_minutes',
          oldRemainingMinutes: Number(packageData.remainingMinutes),
          newRemainingMinutes: packageUpdate.remainingMinutes,
          deltaMinutes: -durationMinutes,
          reason: `coach add-on booking ${bookingCode}`,
          bookingId: bookingRef.id, source: 'coach_addon_v2', createdAt: FieldValue.serverTimestamp(),
        });
      }
      if (guestAccessRef) t.create(guestAccessRef, guestAccess.document);

      response = {
        ok: true,
        requiresPayment: states.bookingState === 'held',
        paymentExpiresAt: expiresAt ? expiresAt.toDate().toISOString() : null,
        ...(guestAccess ? { guestAccessToken: guestAccess.token, guestAccessExpiresAt: guestAccess.expiresAt } : {}),
        booking: {
          id: bookingRef.id, bookingCode, date, startTime, endTime, durationMinutes,
          bookingType: bookingData.bookingType, serviceCategory: price.serviceCategory,
          fundingSource: price.fundingSource, bookingState: states.bookingState,
          cashState: states.cashState, packageUsageState: states.packageUsageState,
          bookingStatus: states.legacyBookingStatus, paymentStatus: states.legacyPaymentStatus,
          coachId, coachName: bookingData.coachName, studentCount,
          finalPrice: price.cashDueAmount, price: price.cashDueAmount,
          qrAmount: price.cashDueAmount, qrType: bookingData.qrType,
          priceBreakdown: price,
        },
      };
      writeIdempotencyInTx(t, idemRef, { scope: idemScope, fingerprint: idemFp, response, nowMs });
    });
  } catch (e) {
    const msg = e.message || '';
    if (msg === 'IDEMPOTENCY_CONFLICT') return res.status(409).json({ ok: false, code: msg, error: 'idempotencyKey ถูกใช้กับคำขออื่นแล้ว' });
    if (V2_ERROR_TEXT[msg]) return res.status(409).json({ ok: false, code: msg, error: V2_ERROR_TEXT[msg] });
    const map = {
      COACH_UNAVAILABLE: 'โค้ชไม่พร้อมรับจอง', COACH_NOT_OPEN: 'โค้ชไม่ได้เปิดรับสอนครบทั้งช่วง',
      COACH_TAKEN: 'โค้ชเพิ่งถูกจองในช่วงเวลานี้', COACH_EXPIRED_RETRY: 'กำลังคืน hold เก่า กรุณาลองอีกครั้ง',
      SLOT_NOT_OPEN: 'คอร์ทยังไม่เปิดครบทั้งช่วง', SLOT_TAKEN: 'คอร์ทเพิ่งถูกจองในช่วงเวลานี้',
    };
    if (map[msg]) return res.status(409).json({ ok: false, code: msg.startsWith('COACH') ? 'COACH_SLOT' : 'SLOT', error: map[msg] });
    if (e.code === 'MIXED_RECEIVER') return res.status(409).json({ ok: false, code: 'MIXED_RECEIVER', error: 'ช่วงเวลานี้ไม่สามารถรวมยอดชำระได้' });
    console.error('[create_coach_addon_v2]', msg);
    return res.status(500).json({ ok: false, error: 'Failed to create Coach Add-on booking' });
  }
  if (replayed) return res.status(200).json({ ...replayed, replayed: true });
  return res.status(200).json(response);
}

async function handleExpireCoachAddonV2(req, res, body) {
  const bookingId = typeof body.bookingId === 'string' ? body.bookingId.trim() : '';
  if (!bookingId) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Missing bookingId' });
  let db;
  try { db = getAdminDb(); }
  catch { return res.status(500).json({ ok: false, error: 'Server error' }); }
  if (!(await coachAddonV2Enabled(db))) return res.status(403).json({ ok: false, code: 'DISABLED', error: 'Coach Add-on v2 is disabled' });
  const snap = await db.collection('bookings').doc(bookingId).get();
  if (!snap.exists) return res.status(404).json({ ok: false, error: 'Booking not found' });
  const booking = snap.data();
  let authorized = false;
  const uid = await v2VerifiedUid(body);
  if (uid && uid === booking.lineUserId) authorized = true;
  if (!authorized && typeof body.guestToken === 'string' && body.guestToken.trim()) {
    authorized = (await verifyGuestToken(db, bookingId, body.guestToken.trim(), 'booking:cancel')).ok;
  }
  if (!authorized) return res.status(403).json({ ok: false, code: 'AUTH', error: 'ยืนยันตัวตนไม่ผ่าน' });
  try {
    const result = await releaseCoachAddonV2Hold(db, bookingId, { reason: 'customer_timer_expired', actor: uid || 'guest', requireExpired: true });
    return res.status(200).json(result);
  } catch (e) {
    if (e.message === 'NOT_EXPIRED') return res.status(409).json({ ok: false, code: 'NOT_EXPIRED', error: 'Hold ยังไม่หมดอายุ' });
    if (e.message === 'NOT_HELD') return res.status(409).json({ ok: false, code: 'NOT_HELD', error: 'Booking ไม่ได้อยู่ในสถานะ hold' });
    console.error('[expire_coach_addon_v2]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to release expired hold' });
  }
}
