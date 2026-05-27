// ════════════════════════════════════════════════════════════════════
// GET /api/finance-data?month=YYYY-MM
// ════════════════════════════════════════════════════════════════════
// Returns booking revenue, expenses, and manual income for the given month.
// All Firestore reads go through Firebase Admin SDK — the browser never
// touches Firestore directly for finance data.
//
// Auth:   requires valid adminSession cookie (set by /api/admin-login).
// Query:  ?month=YYYY-MM  (e.g. ?month=2026-05)
// Return:
//   { ok: true, bookings: [...], expenses: [...], income: [...] }
//   { ok: false, error: "..." }
//
// Soft-deleted records (deleted === true) are filtered out server-side.
// ════════════════════════════════════════════════════════════════════

import { verifySessionCookie } from './_lib/admin-auth.js';
import { getAdminDb, serializeFsDoc } from './_lib/firebase-admin.js';

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  const adminName = verifySessionCookie(req);
  if (!adminName) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }

  const { month } = req.query;
  if (!month || !/^\d{4}-\d{2}$/.test(month)) {
    return res.status(400).json({ ok: false, error: 'Invalid month — expected YYYY-MM' });
  }

  const [year, m] = month.split('-');
  const startDate = `${year}-${m}-01`;
  const endDate   = `${year}-${m}-31`;  // safe upper bound for all months via string comparison

  let db;
  try { db = getAdminDb(); }
  catch (e) {
    console.error('[finance-data] DB init failed:', e.message);
    return res.status(500).json({ ok: false, error: 'Database not available' });
  }

  try {
    const [bookSnap, expSnap, incSnap] = await Promise.all([
      db.collection('bookings')
        .where('date', '>=', startDate)
        .where('date', '<=', endDate)
        .orderBy('date', 'asc')
        .get(),
      db.collection('finance_expenses')
        .where('date', '>=', startDate)
        .where('date', '<=', endDate)
        .orderBy('date', 'asc')
        .get(),
      db.collection('finance_income_manual')
        .where('date', '>=', startDate)
        .where('date', '<=', endDate)
        .orderBy('date', 'asc')
        .get(),
    ]);

    // Return only the booking fields the finance page needs — not the full document.
    const bookings = bookSnap.docs.map(d => {
      const b = d.data();
      return {
        id:            d.id,
        date:          b.date          ?? '',
        startTime:     b.startTime     ?? '',
        endTime:       b.endTime       ?? '',
        customerName:  b.customerName  ?? '',
        paymentStatus: b.paymentStatus ?? '',
        bookingStatus: b.bookingStatus ?? '',
        price:         Number(b.price) || 0,
        bookingType:   b.bookingType   ?? '',
        bookingCode:   b.bookingCode   ?? '',
      };
    });

    // Soft-delete filter: server excludes deleted records before sending to client.
    const expenses = expSnap.docs
      .map(d => ({ id: d.id, ...serializeFsDoc(d.data()) }))
      .filter(e => !e.deleted);

    const income = incSnap.docs
      .map(d => ({ id: d.id, ...serializeFsDoc(d.data()) }))
      .filter(i => !i.deleted);

    return res.status(200).json({ ok: true, bookings, expenses, income });
  } catch (e) {
    console.error('[finance-data] query error:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to load finance data' });
  }
}
