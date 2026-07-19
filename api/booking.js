// ════════════════════════════════════════════════════════════════════
// POST /api/booking — customer booking route (Pricing v2)
// ════════════════════════════════════════════════════════════════════
//   action "price_quote" — READ-ONLY quote (Stage 1)
//   action "create"      — server-authoritative single-use create (Stage 2):
//       recomputes price with the engine (NEVER trusts client price),
//       validates holiday/promo/voucher, and writes bookings + booking_slots
//       (+ voucher.usedCount++) in ONE transaction with a double-booking guard.
//
// Public (no session) — mirrors the existing client-direct create threat model;
// the win is that PRICE is now computed server-side. Passes (ultra/offpeak/event)
// are NOT handled here — they stay on the legacy client path.
// ════════════════════════════════════════════════════════════════════

import { getAdminDb, getAdminAuth, writeAuditLog } from './_lib/firebase-admin.js';
import { computeQuote } from './_lib/pricing.js';
import { sendAndLog, loadActiveAdmins, loadNotificationFlags } from './_lib/notify.js';
import { FieldValue, Timestamp } from 'firebase-admin/firestore';

const RESOURCE_ID       = 'room1';
const DEFAULT_BRANCH_ID = 'ladprao1';
const PAY_MINS          = 15;    // payment window — mirrors index.html
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

// ── Multi-hour (Phase A) helpers — bookings may span 1–3 consecutive hours ──
const MAX_DURATION_HOURS = 3;
function parseDurationHours(raw) {
  if (raw === undefined || raw === null || raw === '') return 1;
  const n = Number(raw);
  return (Number.isInteger(n) && n >= 1 && n <= MAX_DURATION_HOURS) ? n : null;
}
// Hourly start times covered by [startTime, startTime + n hours). Null when the
// range would cross midnight — a booking must stay within one calendar date.
function hourListOf(startTime, n) {
  const h0 = parseInt(String(startTime).slice(0, 2), 10);
  if (!Number.isFinite(h0) || h0 + n > 24) return null;
  return Array.from({ length: n }, (_, i) => `${String(h0 + i).padStart(2, '0')}:00`);
}
const endTimeAfter = (startTime, n) => {
  const h = parseInt(String(startTime).slice(0, 2), 10) + n;
  return h >= 24 ? '00:00' : `${String(h).padStart(2, '0')}:00`;
};
// PromptPay receiver for a qrType — special promo pays the ALT account; all
// other types pay MAIN. Hours in one booking must share ONE receiver (one QR).
const receiverOf = qrType => (qrType === 'special' ? 'alt' : 'main');

// Sum per-hour quotes into one multi-hour quote. Throws {code:'MIXED_RECEIVER'}
// when hours would need different PromptPay accounts (can't pay with one QR).
function combineQuotes(hourQuotes) {
  const receivers = new Set(hourQuotes.map(q => receiverOf(q.qrType)));
  if (receivers.size > 1) {
    const err = new Error('MIXED_RECEIVER'); err.code = 'MIXED_RECEIVER'; throw err;
  }
  const total     = hourQuotes.reduce((s, q) => s + q.finalPrice, 0);
  const totalOrig = hourQuotes.reduce((s, q) => s + q.originalPrice, 0);
  const allSame   = hourQuotes.every(q => q.qrType === hourQuotes[0].qrType);
  return {
    finalPrice: total, originalPrice: totalOrig, qrAmount: total,
    price: total, amount: total,
    qrType: allSame ? hourQuotes[0].qrType : 'normal',
    pricingType: hourQuotes.every(q => q.pricingType === hourQuotes[0].pricingType)
      ? hourQuotes[0].pricingType : 'multi_rate',
    breakdown: hourQuotes.map(q => ({
      startTime: q.startTime, endTime: nextHourEnd(q.startTime),
      price: q.finalPrice, pricingType: q.pricingType, qrType: q.qrType,
    })),
  };
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
    not_applicable: 'โค้ดใช้กับราคานี้ไม่ได้ (ใช้ได้เฉพาะราคาปกติ 350)',
    wrong_owner:    'โค้ดนี้ไม่ใช่ของบัญชีนี้',
  })[r] || 'โค้ดส่วนลดไม่ถูกต้อง';
}

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'Method not allowed' });
  const body = parseBody(req);
  if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

  if (body.action === 'price_quote')   return handlePriceQuote(res, body);
  if (body.action === 'create')        return handleCreate(res, body);
  if (body.action === 'cancel_pending') return handleCancelPending(res, body);
  // Coach lesson booking (Stage 3) — customer-facing, feature-flagged OFF by
  // default via system_settings/features.enableCoachBookingCustomer.
  if (body.action === 'coach_options')       return handleCoachOptions(res);
  if (body.action === 'coach_slots')         return handleCoachSlots(res, body);
  if (body.action === 'create_coach_lesson') return handleCreateCoachLesson(res, body);
  // Pass self-purchase (Stage D) — LIVE (on by default); kill-switch:
  // system_settings/features.enablePassSelfPurchase = false.
  if (body.action === 'pass_catalog')         return handlePassCatalog(res);
  if (body.action === 'create_pass_purchase') return handleCreatePassPurchase(res, body);
  return res.status(400).json({ ok: false, error: `Unknown action "${body.action}"` });
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
const coachAvailDocId = (coachId, date, hour) => `${coachId}_${date}_${String(hour).replace(':', '')}`;

// ── price_quote — READ-ONLY. No writes anywhere. ────────────────────
async function handlePriceQuote(res, body) {
  const date        = typeof body.date === 'string' ? body.date.trim() : '';
  const startTime   = typeof body.startTime === 'string' ? body.startTime.trim() : '';
  const payType     = typeof body.payType === 'string' && body.payType ? body.payType : 'single';
  const voucherCode = typeof body.voucherCode === 'string' && body.voucherCode.trim() ? body.voucherCode.trim() : null;
  const lineUserId  = typeof body.lineUserId === 'string' ? body.lineUserId : null;
  const durationHours = parseDurationHours(body.durationHours);

  if (!DATE_RE.test(date))      return res.status(400).json({ ok: false, error: 'date must be YYYY-MM-DD' });
  if (!TIME_RE.test(startTime)) return res.status(400).json({ ok: false, error: 'startTime must be HH:mm' });
  if (durationHours === null)   return res.status(400).json({ ok: false, error: `durationHours must be an integer 1-${MAX_DURATION_HOURS}` });
  const hours = hourListOf(startTime, durationHours);
  if (!hours) return res.status(400).json({ ok: false, error: 'Booking cannot cross midnight' });
  // Vouchers stay single-hour only (rule: one voucher = one standard hour).
  if (voucherCode && durationHours > 1) {
    return res.status(409).json({ ok: false, code: 'VOUCHER', error: 'โค้ดส่วนลดใช้ได้กับการจอง 1 ชั่วโมงเท่านั้น' });
  }

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[price_quote] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  try {
    const [pricingSnap, holidaySnap, voucherSnap] = await Promise.all([
      db.collection('system_settings').doc('pricing').get(),
      db.collection('holidays').doc(date).get(),
      voucherCode ? db.collection('vouchers').doc(voucherCode).get() : Promise.resolve(null),
    ]);
    const quoteInput = h => ({
      date, startTime: h, nowMs: Date.now(),
      isHoliday: holidaySnap.exists && holidaySnap.data().isHoliday === true,
      promoConfig: pricingSnap.exists ? pricingSnap.data() : null,
      payType, voucherCode,
      voucher: voucherSnap && voucherSnap.exists ? voucherSnap.data() : null,
      lineUserId,
    });
    if (durationHours === 1) {
      return res.status(200).json({ ok: true, quote: computeQuote(quoteInput(startTime)) });
    }
    const hourQuotes = hours.map(h => ({ ...computeQuote(quoteInput(h)), startTime: h }));
    const combined   = combineQuotes(hourQuotes);
    return res.status(200).json({
      ok: true,
      quote: {
        ...hourQuotes[0],                    // base flags from the first hour
        ...combined,                         // totals + breakdown override
        durationHours, endTime: endTimeAfter(startTime, durationHours),
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
async function handleCreate(res, body) {
  const date         = typeof body.date === 'string' ? body.date.trim() : '';
  const startTime    = typeof body.startTime === 'string' ? body.startTime.trim() : '';
  const customerName = typeof body.customerName === 'string' ? body.customerName.trim() : '';
  const customerPhone= typeof body.customerPhone === 'string' ? body.customerPhone.trim() : '';
  const lineUserId   = typeof body.lineUserId === 'string' && body.lineUserId ? body.lineUserId : 'guest';
  const lineDisplayName = typeof body.lineDisplayName === 'string' ? body.lineDisplayName : '';
  const customerNote = typeof body.customerNote === 'string' ? body.customerNote.slice(0, 500) : '';
  const voucherCode  = typeof body.voucherCode === 'string' && body.voucherCode.trim() ? body.voucherCode.trim() : null;
  const durationHours = parseDurationHours(body.durationHours);

  // Client NEVER sends price — it is recomputed below. Validate inputs only.
  if (!DATE_RE.test(date))      return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'date must be YYYY-MM-DD' });
  if (!TIME_RE.test(startTime)) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'startTime must be HH:mm' });
  if (!customerName)  return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerName is required' });
  if (!customerPhone) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerPhone is required' });
  if (durationHours === null) return res.status(400).json({ ok: false, code: 'VALIDATION', error: `durationHours must be an integer 1-${MAX_DURATION_HOURS}` });
  const hours = hourListOf(startTime, durationHours);
  if (!hours) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'Booking cannot cross midnight' });
  if (voucherCode && durationHours > 1) {
    return res.status(409).json({ ok: false, code: 'VOUCHER', error: 'โค้ดส่วนลดใช้ได้กับการจอง 1 ชั่วโมงเท่านั้น' });
  }

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[create] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  const nowMs = Date.now();
  let quote, hourQuotes;
  try {
    const [pricingSnap, holidaySnap, voucherSnap] = await Promise.all([
      db.collection('system_settings').doc('pricing').get(),
      db.collection('holidays').doc(date).get(),
      voucherCode ? db.collection('vouchers').doc(voucherCode).get() : Promise.resolve(null),
    ]);
    hourQuotes = hours.map(h => ({
      ...computeQuote({
        date, startTime: h, nowMs,
        isHoliday: holidaySnap.exists && holidaySnap.data().isHoliday === true,
        promoConfig: pricingSnap.exists ? pricingSnap.data() : null,
        payType: 'single', voucherCode,
        voucher: voucherSnap && voucherSnap.exists ? voucherSnap.data() : null,
        lineUserId,
      }),
      startTime: h,
    }));
    quote = durationHours === 1
      ? hourQuotes[0]
      : { ...hourQuotes[0], ...combineQuotes(hourQuotes),
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
  const endTime           = endTimeAfter(startTime, durationHours);
  const bookingCode       = genBookingCode();
  const paymentExpiresAt  = Timestamp.fromMillis(nowMs + PAY_MINS * 60 * 1000);
  const allLateNight      = hourQuotes.every(q => q.qrType === 'late_night');
  const bookingType       = allLateNight ? 'Late Night Session' : 'Single Use';
  const pricingMode       = allLateNight             ? 'late_night'
                          : quote.qrType === 'special' ? 'special_promotion'
                          : 'normal_single_use';

  const bookingRef = db.collection('bookings').doc();
  const slotRefs   = hours.map(h => db.collection('booking_slots').doc(slotIdOf(date, h)));
  const availRefs  = hours.map(h => db.collection('available_slots').doc(slotIdOf(date, h)));
  const voucherRef = quote.voucherApplied ? db.collection('vouchers').doc(voucherCode) : null;

  try {
    await db.runTransaction(async (t) => {
      const reads = [...slotRefs.map(r => t.get(r)), ...availRefs.map(r => t.get(r))];
      if (voucherRef) reads.push(t.get(voucherRef));
      const snaps = await Promise.all(reads);
      const slotSnaps  = snaps.slice(0, hours.length);
      const availSnaps = snaps.slice(hours.length, hours.length * 2);
      const voucherSnap = voucherRef ? snaps[hours.length * 2] : null;

      // ── Double-booking guard on EVERY hour (mirrors the 1-hour rules) ──
      for (let i = 0; i < hours.length; i++) {
        const availSnap = availSnaps[i], slotSnap = slotSnaps[i];
        if (!availSnap.exists || availSnap.data().status !== 'open') throw new Error('SLOT_NOT_OPEN');
        if (slotSnap.exists) {
          const sd = slotSnap.data();
          if (sd.bookingStatus === 'confirmed') throw new Error('SLOT_TAKEN');
          if (sd.bookingStatus === 'pending_payment') {
            const exp = sd.expiresAt?.toMillis?.() ?? 0;
            if (!exp || exp > nowMs) throw new Error('SLOT_HELD');
          }
        }
      }

      // ── Voucher re-validate + mark used (same transaction) ──────────
      if (voucherRef) {
        if (!voucherSnap.exists) throw new Error('VOUCHER_not_found');
        const v = voucherSnap.data();
        const exp = v.expiresAt?.toMillis?.() ?? null;
        if (v.active !== true) throw new Error('VOUCHER_inactive');
        if (exp !== null && exp < nowMs) throw new Error('VOUCHER_expired');
        if ((Number(v.usedCount) || 0) >= (Number(v.maxUses) || 0)) throw new Error('VOUCHER_used_up');
        if (v.issuedTo && v.issuedTo !== lineUserId) throw new Error('VOUCHER_wrong_owner');
        t.update(voucherRef, {
          usedCount:       (Number(v.usedCount) || 0) + 1,
          lastUsedAt:      FieldValue.serverTimestamp(),
          lastUsedBy:      lineUserId || null,
          lastUsedBooking: bookingCode,
        });
      }

      // ── Write booking (server price) + one slot lock per hour ───────
      t.set(bookingRef, {
        bookingCode, resourceId: RESOURCE_ID, branchId: DEFAULT_BRANCH_ID,
        bookingType,
        lineUserId, lineDisplayName,
        customerName, customerPhone, customerPhoneNormalized: normalizePhone(customerPhone),
        customerNote,
        date, startTime, endTime, durationHours,
        ...(durationHours > 1 ? { priceBreakdown: quote.breakdown } : {}),
        // Pricing v2 metadata (server-authoritative)
        price: finalPrice, amount: finalPrice,
        originalPrice: quote.originalPrice, finalPrice,
        basePrice: quote.originalPrice, effectivePrice: finalPrice,
        pricingType: quote.pricingType, pricingMode,
        promoCode: quote.promoCode, voucherCode: quote.voucherCode, discountAmount: quote.discountAmount,
        priceRuleVersion: quote.priceRuleVersion,
        qrAmount: quote.qrAmount, qrType: quote.qrType, paymentQrType: quote.qrType,
        promoApplied: quote.pricingType === 'special_promotion' || quote.voucherApplied,
        isHoliday: quote.isHoliday, isWeekend: quote.isWeekend,
        isMorningWeekday: quote.isMorningWeekday, advanceHours: quote.advanceHours,
        bookingStatus: 'pending_payment', paymentStatus: 'unpaid',
        paymentExpiresAt,
        slipUrl: null, slipUploadedAt: null, cancelReason: null,
        createdVia: 'server',
        createdAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
      });
      hours.forEach((h, i) => {
        t.set(slotRefs[i], {
          bookingCode, bookingId: bookingRef.id, resourceId: RESOURCE_ID, branchId: DEFAULT_BRANCH_ID,
          date, hour: h,
          bookingStatus: 'pending_payment', paymentStatus: 'unpaid',
          expiresAt: paymentExpiresAt,
        });
      });
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
    return res.status(500).json({ ok: false, error: 'Failed to create booking' }); // no code → client may fall back
  }

  console.log(`[create] ${bookingCode} ${quote.pricingType} ${durationHours}h ฿${finalPrice}${quote.voucherApplied ? ' voucher=' + voucherCode : ''}`);
  return res.status(200).json({
    ok: true,
    paymentExpiresAt: paymentExpiresAt.toDate().toISOString(),
    booking: {
      id: bookingRef.id, bookingCode, date, startTime, endTime, durationHours,
      bookingType,
      finalPrice, price: finalPrice, originalPrice: quote.originalPrice,
      qrType: quote.qrType, qrAmount: quote.qrAmount, paymentQrType: quote.qrType,
      pricingType: quote.pricingType, discountAmount: quote.discountAmount, voucherCode: quote.voucherCode,
      ...(durationHours > 1 ? { priceBreakdown: quote.breakdown } : {}),
      lineUserId, customerName, customerPhone, customerNote,
    },
  });
}

// ── cancel_pending — customer cancels their own UNPAID pending booking ──
// Fixes the live "Insufficient Permission" bug: the client used to set
// booking_slots.paymentStatus="cancelled", which is NOT in the rules enum.
// Doing it server-side (Admin SDK bypasses rules) avoids widening the rules.
// Ownership: bookingCode must match AND (verified Firebase uid == lineUserId,
// or stated lineUserId == booking.lineUserId). Only pending_payment + unpaid +
// not-expired bookings are cancellable by a customer. Does NOT touch the admin
// reject/refund flow, and passes/events (confirmed/package) are excluded.
async function handleCancelPending(res, body) {
  const bookingId   = typeof body.bookingId === 'string' ? body.bookingId.trim() : '';
  const bookingCode = typeof body.bookingCode === 'string' ? body.bookingCode.trim() : '';
  const lineUserId  = typeof body.lineUserId === 'string' ? body.lineUserId : '';
  const idToken     = typeof body.idToken === 'string' && body.idToken ? body.idToken : null;
  if (!bookingId)   return res.status(400).json({ ok: false, error: 'Missing bookingId' });
  if (!bookingCode) return res.status(400).json({ ok: false, error: 'Missing bookingCode' });

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[cancel_pending] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  const bookingRef = db.collection('bookings').doc(bookingId);
  let booking;
  try {
    const snap = await bookingRef.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Booking not found' });
    booking = snap.data();
  } catch (e) { console.error('[cancel_pending] read:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  // ── Ownership ─────────────────────────────────────────────────────
  if (bookingCode !== booking.bookingCode) {
    return res.status(403).json({ ok: false, error: 'ยกเลิกไม่ได้ (ไม่ใช่การจองของคุณ)' });
  }
  let owner = false;
  if (idToken) {
    try {
      const decoded = await getAdminAuth().verifyIdToken(idToken);
      if (decoded.uid === booking.lineUserId) owner = true;
      else return res.status(403).json({ ok: false, error: 'บัญชีไม่ตรงกับการจอง' });
    } catch { /* token invalid/expired → fall back to stated lineUserId */ }
  }
  if (!owner && lineUserId && lineUserId === booking.lineUserId) owner = true;
  if (!owner) return res.status(403).json({ ok: false, error: 'ยกเลิกไม่ได้ (ยืนยันตัวตนไม่ผ่าน)' });

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

  // Multi-hour (Phase A): release EVERY hourly slot the booking holds.
  const heldHours = (booking.date && booking.startTime)
    ? (hourListOf(booking.startTime, parseDurationHours(booking.durationHours) || 1) || [booking.startTime])
    : [];
  const slotRefs = heldHours.map(h => db.collection('booking_slots').doc(`${RESOURCE_ID}_${booking.date}_${String(h).replace(':', '')}`));
  // Coach lesson: the coach hour was locked in the create transaction —
  // release it here (ownership-checked below).
  const coachAvailRef = (booking.serviceType === 'coach_lesson' && booking.coachId && booking.date && booking.startTime)
    ? db.collection('coach_availability').doc(coachAvailDocId(booking.coachId, booking.date, booking.startTime))
    : null;

  try {
    await db.runTransaction(async (t) => {
      const bSnap = await t.get(bookingRef);
      const slotSnaps = await Promise.all(slotRefs.map(r => t.get(r)));
      const caSnap = coachAvailRef ? await t.get(coachAvailRef) : null;
      if (!bSnap.exists) throw new Error('GONE');
      const bNow = bSnap.data();
      if (bNow.bookingStatus !== 'pending_payment') throw new Error('BAD_STATE');
      if (bNow.paymentStatus !== 'unpaid') throw new Error('BAD_STATE');
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
      // Release each slot only if it still belongs to this booking and isn't confirmed.
      slotSnaps.forEach((slotSnap, i) => {
        if (!slotSnap.exists) return;
        const sd = slotSnap.data();
        const owns = sd.bookingId === bookingId || sd.bookingCode === booking.bookingCode;
        if (owns && sd.bookingStatus !== 'confirmed') {
          t.update(slotRefs[i], { bookingStatus: 'cancelled', paymentStatus: 'rejected' });
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
      if (sd.bookingStatus === 'confirmed') return true;
      if (sd.bookingStatus === 'pending_payment') {
        const exp = sd.expiresAt?.toMillis?.() ?? 0;
        return exp > nowMs;
      }
      return false;
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

  if (!DATE_RE.test(date))      return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'date must be YYYY-MM-DD' });
  if (!TIME_RE.test(startTime)) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'startTime must be HH:mm' });
  if (!coachId)       return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'coachId is required' });
  if (!customerName)  return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerName is required' });
  if (!customerPhone) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerPhone is required' });

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
  const customerPrice = coach.lessonPrice;   // one clear total, INCLUDES room
  const coachPayoutAmount = (Number.isInteger(coach.payoutPerHour) && coach.payoutPerHour >= 500)
    ? coach.payoutPerHour : 500;             // business minimum 500 THB/hour

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

  try {
    await db.runTransaction(async (t) => {
      const [slotSnap, availSnap, caSnap] = await Promise.all([
        t.get(slotRef), t.get(availRef), t.get(coachAvailRef),
      ]);

      // ── Room guards (identical rules to handleCreate) ───────────────
      if (!availSnap.exists || availSnap.data().status !== 'open') throw new Error('SLOT_NOT_OPEN');
      if (slotSnap.exists) {
        const sd = slotSnap.data();
        if (sd.bookingStatus === 'confirmed') throw new Error('SLOT_TAKEN');
        if (sd.bookingStatus === 'pending_payment') {
          const exp = sd.expiresAt?.toMillis?.() ?? 0;
          if (!exp || exp > nowMs) throw new Error('SLOT_HELD');
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

      // ── Writes: booking + room lock + coach lock (one commit) ───────
      t.set(bookingRef, {
        bookingCode, resourceId: RESOURCE_ID, branchId: ca.branchId || DEFAULT_BRANCH_ID,
        bookingType: 'Coach Lesson',
        serviceType: 'coach_lesson',
        coachId, coachName: coach.displayName || coachId,
        customerPrice, coachPayoutAmount, coachPayoutStatus: 'pending',
        lessonStatus: 'scheduled',
        lineUserId, lineDisplayName,
        customerName, customerPhone, customerPhoneNormalized: normalizePhone(customerPhone),
        customerNote,
        date, startTime, endTime, durationHours: 1,
        price: customerPrice, amount: customerPrice,
        originalPrice: customerPrice, finalPrice: customerPrice,
        basePrice: customerPrice, effectivePrice: customerPrice,
        pricingType: 'coach_lesson', pricingMode: 'coach_lesson',
        promoCode: null, voucherCode: null, discountAmount: 0,
        qrAmount: customerPrice, qrType: 'normal', paymentQrType: 'normal',
        promoApplied: false,
        bookingStatus: 'pending_payment', paymentStatus: 'unpaid',
        paymentExpiresAt,
        slipUrl: null, slipUploadedAt: null, cancelReason: null,
        createdVia: 'server',
        createdAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
      });
      t.set(slotRef, {
        bookingCode, bookingId: bookingRef.id, resourceId: RESOURCE_ID,
        branchId: ca.branchId || DEFAULT_BRANCH_ID,
        date, hour: startTime,
        bookingStatus: 'pending_payment', paymentStatus: 'unpaid',
        expiresAt: paymentExpiresAt,
        coachId,
      });
      t.set(coachAvailRef, {
        coachId, branchId: ca.branchId || DEFAULT_BRANCH_ID,
        date, hour: startTime,
        status: 'booked',
        bookingId: bookingRef.id, bookingCode,
        holdExpiresAt: paymentExpiresAt,
        updatedAt: FieldValue.serverTimestamp(),
      });
    });
  } catch (e) {
    const msg = e.message || '';
    const m = {
      SLOT_NOT_OPEN:  'ช่องเวลานี้ปิดรับจองแล้ว',
      SLOT_TAKEN:     'ช่องเวลานี้เพิ่งถูกจอง',
      SLOT_HELD:      'ช่องเวลานี้ถูกจองค้างอยู่ ลองใหม่อีกครั้ง',
      COACH_NOT_OPEN: 'โค้ชไม่ได้เปิดรับสอนช่วงเวลานี้แล้ว',
      COACH_HELD:     'ช่วงเวลานี้ของโค้ชเพิ่งถูกจอง',
    };
    if (m[msg]) return res.status(409).json({ ok: false, code: 'SLOT', error: m[msg] });
    console.error('[create_coach_lesson] tx:', msg);
    return res.status(500).json({ ok: false, error: 'Failed to create coach lesson booking' });
  }

  console.log(`[create_coach_lesson] ${bookingCode} coach:${coachId} ฿${customerPrice} payout:฿${coachPayoutAmount}`);
  return res.status(200).json({
    ok: true,
    paymentExpiresAt: paymentExpiresAt.toDate().toISOString(),
    booking: {
      id: bookingRef.id, bookingCode, date, startTime, endTime,
      bookingType: 'Coach Lesson', serviceType: 'coach_lesson',
      coachId, coachName: coach.displayName || coachId,
      finalPrice: customerPrice, price: customerPrice, originalPrice: customerPrice,
      qrType: 'normal', qrAmount: customerPrice, paymentQrType: 'normal',
      pricingType: 'coach_lesson', discountAmount: 0, voucherCode: null,
      lineUserId, customerName, customerPhone, customerNote,
    },
  });
}
