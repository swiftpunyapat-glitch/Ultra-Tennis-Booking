// ════════════════════════════════════════════════════════════════════
// POST /api/booking — customer booking route (Pricing v2)
// ════════════════════════════════════════════════════════════════════
// Stage 1: action "price_quote" ONLY — READ-ONLY. Reads system_settings/pricing
// + holidays/{date} (+ vouchers/{code} if a voucherCode is supplied) and returns
// a server-computed quote. It writes NOTHING (no bookings/booking_slots/vouchers).
// Server-side create + voucher transaction land in Stage 2 (behind a flag).
//
// Public (no session) — a price quote is public info, and the customer isn't
// authenticated. The voucher doc is never returned; only the resulting quote.
// ════════════════════════════════════════════════════════════════════

import { getAdminDb } from './_lib/firebase-admin.js';
import { computeQuote } from './_lib/pricing.js';

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const TIME_RE = /^\d{2}:\d{2}$/;

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }
  const body = parseBody(req);
  if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

  if (body.action === 'price_quote') {
    return handlePriceQuote(res, body);
  }
  return res.status(400).json({ ok: false, error: `Unknown action "${body.action}"` });
}

// price_quote — READ-ONLY. No writes anywhere.
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
    // Reads only.
    const [pricingSnap, holidaySnap, voucherSnap] = await Promise.all([
      db.collection('system_settings').doc('pricing').get(),
      db.collection('holidays').doc(date).get(),
      voucherCode ? db.collection('vouchers').doc(voucherCode).get() : Promise.resolve(null),
    ]);

    const promoConfig = pricingSnap.exists ? pricingSnap.data() : null;
    const isHoliday   = holidaySnap.exists && holidaySnap.data().isHoliday === true;
    const voucher     = voucherSnap && voucherSnap.exists ? voucherSnap.data() : null;

    const quote = computeQuote({
      date, startTime, nowMs: Date.now(), isHoliday,
      promoConfig, payType, voucherCode, voucher, lineUserId,
    });

    return res.status(200).json({ ok: true, quote });
  } catch (e) {
    console.error('[price_quote]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to compute quote' });
  }
}
