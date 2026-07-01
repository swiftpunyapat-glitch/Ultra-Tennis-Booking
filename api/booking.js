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
  return res.status(400).json({ ok: false, error: `Unknown action "${body.action}"` });
}

// ── price_quote — READ-ONLY. No writes anywhere. ────────────────────
async function handlePriceQuote(res, body) {
  const date        = typeof body.date === 'string' ? body.date.trim() : '';
  const startTime   = typeof body.startTime === 'string' ? body.startTime.trim() : '';
  const payType     = typeof body.payType === 'string' && body.payType ? body.payType : 'single';
  const voucherCode = typeof body.voucherCode === 'string' && body.voucherCode.trim() ? body.voucherCode.trim() : null;
  const lineUserId  = typeof body.lineUserId === 'string' ? body.lineUserId : null;

  if (!DATE_RE.test(date))      return res.status(400).json({ ok: false, error: 'date must be YYYY-MM-DD' });
  if (!TIME_RE.test(startTime)) return res.status(400).json({ ok: false, error: 'startTime must be HH:mm' });

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[price_quote] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  try {
    const [pricingSnap, holidaySnap, voucherSnap] = await Promise.all([
      db.collection('system_settings').doc('pricing').get(),
      db.collection('holidays').doc(date).get(),
      voucherCode ? db.collection('vouchers').doc(voucherCode).get() : Promise.resolve(null),
    ]);
    const quote = computeQuote({
      date, startTime, nowMs: Date.now(),
      isHoliday: holidaySnap.exists && holidaySnap.data().isHoliday === true,
      promoConfig: pricingSnap.exists ? pricingSnap.data() : null,
      payType, voucherCode,
      voucher: voucherSnap && voucherSnap.exists ? voucherSnap.data() : null,
      lineUserId,
    });
    return res.status(200).json({ ok: true, quote });
  } catch (e) {
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

  // Client NEVER sends price — it is recomputed below. Validate inputs only.
  if (!DATE_RE.test(date))      return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'date must be YYYY-MM-DD' });
  if (!TIME_RE.test(startTime)) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'startTime must be HH:mm' });
  if (!customerName)  return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerName is required' });
  if (!customerPhone) return res.status(400).json({ ok: false, code: 'VALIDATION', error: 'customerPhone is required' });

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[create] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  const nowMs = Date.now();
  let quote;
  try {
    const [pricingSnap, holidaySnap, voucherSnap] = await Promise.all([
      db.collection('system_settings').doc('pricing').get(),
      db.collection('holidays').doc(date).get(),
      voucherCode ? db.collection('vouchers').doc(voucherCode).get() : Promise.resolve(null),
    ]);
    quote = computeQuote({
      date, startTime, nowMs,
      isHoliday: holidaySnap.exists && holidaySnap.data().isHoliday === true,
      promoConfig: pricingSnap.exists ? pricingSnap.data() : null,
      payType: 'single', voucherCode,
      voucher: voucherSnap && voucherSnap.exists ? voucherSnap.data() : null,
      lineUserId,
    });
  } catch (e) {
    console.error('[create] quote:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to price booking' });
  }

  // A requested voucher that can't apply is a hard rejection (no silent drop).
  if (voucherCode && !quote.voucherApplied) {
    return res.status(409).json({ ok: false, code: 'VOUCHER', error: mapVoucherReason(quote.voucherReason) });
  }

  const finalPrice        = quote.finalPrice;
  const endTime           = nextHourEnd(startTime);
  const bookingCode       = genBookingCode();
  const paymentExpiresAt  = Timestamp.fromMillis(nowMs + PAY_MINS * 60 * 1000);
  const bookingType       = quote.qrType === 'late_night' ? 'Late Night Session' : 'Single Use';
  const pricingMode       = quote.qrType === 'late_night' ? 'late_night'
                          : quote.qrType === 'special'    ? 'special_promotion'
                          : 'normal_single_use';

  const bookingRef = db.collection('bookings').doc();
  const slotRef    = db.collection('booking_slots').doc(slotIdOf(date, startTime));
  const availRef   = db.collection('available_slots').doc(slotIdOf(date, startTime));
  const voucherRef = quote.voucherApplied ? db.collection('vouchers').doc(voucherCode) : null;

  try {
    await db.runTransaction(async (t) => {
      const reads = [t.get(slotRef), t.get(availRef)];
      if (voucherRef) reads.push(t.get(voucherRef));
      const snaps = await Promise.all(reads);
      const slotSnap = snaps[0], availSnap = snaps[1], voucherSnap = voucherRef ? snaps[2] : null;

      // ── Double-booking guard (mirrors index.html client transaction) ──
      if (!availSnap.exists || availSnap.data().status !== 'open') throw new Error('SLOT_NOT_OPEN');
      if (slotSnap.exists) {
        const sd = slotSnap.data();
        if (sd.bookingStatus === 'confirmed') throw new Error('SLOT_TAKEN');
        if (sd.bookingStatus === 'pending_payment') {
          const exp = sd.expiresAt?.toMillis?.() ?? 0;
          if (!exp || exp > nowMs) throw new Error('SLOT_HELD');
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

      // ── Write booking (server price) + slot lock ────────────────────
      t.set(bookingRef, {
        bookingCode, resourceId: RESOURCE_ID, branchId: DEFAULT_BRANCH_ID,
        bookingType,
        lineUserId, lineDisplayName,
        customerName, customerPhone, customerPhoneNormalized: normalizePhone(customerPhone),
        customerNote,
        date, startTime, endTime, durationHours: 1,
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
      t.set(slotRef, {
        bookingCode, bookingId: bookingRef.id, resourceId: RESOURCE_ID, branchId: DEFAULT_BRANCH_ID,
        date, hour: startTime,
        bookingStatus: 'pending_payment', paymentStatus: 'unpaid',
        expiresAt: paymentExpiresAt,
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

  console.log(`[create] ${bookingCode} ${quote.pricingType} ฿${finalPrice}${quote.voucherApplied ? ' voucher=' + voucherCode : ''}`);
  return res.status(200).json({
    ok: true,
    paymentExpiresAt: paymentExpiresAt.toDate().toISOString(),
    booking: {
      id: bookingRef.id, bookingCode, date, startTime, endTime,
      bookingType,
      finalPrice, price: finalPrice, originalPrice: quote.originalPrice,
      qrType: quote.qrType, qrAmount: quote.qrAmount, paymentQrType: quote.qrType,
      pricingType: quote.pricingType, discountAmount: quote.discountAmount, voucherCode: quote.voucherCode,
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

  const slotId  = (booking.date && booking.startTime)
    ? `${RESOURCE_ID}_${booking.date}_${String(booking.startTime).replace(':', '')}`
    : null;
  const slotRef = slotId ? db.collection('booking_slots').doc(slotId) : null;

  try {
    await db.runTransaction(async (t) => {
      const bSnap = await t.get(bookingRef);
      const slotSnap = slotRef ? await t.get(slotRef) : null;
      if (!bSnap.exists) throw new Error('GONE');
      const bNow = bSnap.data();
      if (bNow.bookingStatus !== 'pending_payment') throw new Error('BAD_STATE');
      if (bNow.paymentStatus !== 'unpaid') throw new Error('BAD_STATE');

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
      // Release the slot only if it still belongs to this booking and isn't confirmed.
      if (slotSnap && slotSnap.exists) {
        const sd = slotSnap.data();
        const owns = sd.bookingId === bookingId || sd.bookingCode === booking.bookingCode;
        if (owns && sd.bookingStatus !== 'confirmed') {
          t.update(slotRef, { bookingStatus: 'cancelled', paymentStatus: 'rejected' });
        }
      }
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
  return res.status(200).json({ ok: true });
}
