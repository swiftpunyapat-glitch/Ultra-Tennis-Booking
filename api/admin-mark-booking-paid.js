// ════════════════════════════════════════════════════════════════════
// POST /api/admin-mark-booking-paid
// ════════════════════════════════════════════════════════════════════
// Auth: requires valid adminSession cookie (set by /api/admin-login).
//
// Body: { bookingId, amount, paymentMethod, paymentNote? }
//
// Valid paymentMethod values: cash | transfer | manual_confirmed | other
//
// Updates booking:
//   paymentStatus → "paid", bookingStatus → "confirmed",
//   price = amount, paidAt, paidBy, paymentMethod, paymentNote,
//   adminReviewedAt (so the admin Paid Summary date filter works),
//   confirmedAt, updatedAt.
// Also mirrors paymentStatus / bookingStatus to the booking_slots doc.
//
// Guards:
//   • Booking must exist.
//   • booking.paymentStatus must be "unpaid" — rejects already-paid,
//     cancelled, or other statuses (pending_review, package, etc.).
//
// Returns: { ok: true } on success | { ok: false, error: "..." }
// ════════════════════════════════════════════════════════════════════

import { verifySessionCookie } from './_lib/admin-auth.js';
import { getAdminDb } from './_lib/firebase-admin.js';
import { FieldValue } from 'firebase-admin/firestore';

const RESOURCE_ID   = 'room1';
const VALID_METHODS = ['cash', 'transfer', 'manual_confirmed', 'other'];

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

export default async function handler(req, res) {
  const adminName = verifySessionCookie(req);
  if (!adminName) return res.status(401).json({ ok: false, error: 'Unauthorized' });

  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  const body = parseBody(req);
  if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

  const { bookingId, amount, paymentMethod, paymentNote = '' } = body;

  if (!bookingId || typeof bookingId !== 'string' || !bookingId.trim()) {
    return res.status(400).json({ ok: false, error: 'Missing bookingId' });
  }
  if (!Number.isFinite(Number(amount)) || Number(amount) <= 0) {
    return res.status(400).json({ ok: false, error: 'Amount must be a positive number' });
  }
  if (!VALID_METHODS.includes(paymentMethod)) {
    return res.status(400).json({ ok: false, error: `Invalid paymentMethod. Must be one of: ${VALID_METHODS.join(', ')}` });
  }

  let db;
  try { db = getAdminDb(); }
  catch (e) {
    console.error('[mark-paid] DB init:', e.message);
    return res.status(500).json({ ok: false, error: 'Database not available' });
  }

  const bookingRef = db.collection('bookings').doc(bookingId.trim());
  let booking;
  try {
    const snap = await bookingRef.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Booking not found' });
    booking = snap.data();
  } catch (e) {
    console.error('[mark-paid] read booking:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to read booking' });
  }

  if (booking.bookingStatus === 'cancelled') {
    return res.status(409).json({ ok: false, error: 'Cannot mark a cancelled booking as paid' });
  }
  if (booking.paymentStatus === 'paid') {
    return res.status(409).json({ ok: false, error: 'Booking is already paid' });
  }
  if (booking.paymentStatus !== 'unpaid') {
    return res.status(409).json({ ok: false, error: `Cannot mark paid: current paymentStatus is "${booking.paymentStatus}"` });
  }

  const slotId  = `${RESOURCE_ID}_${booking.date}_${booking.startTime?.replace(':', '')}`;
  const slotRef = db.collection('booking_slots').doc(slotId);

  try {
    const batch = db.batch();
    batch.update(bookingRef, {
      paymentStatus:   'paid',
      bookingStatus:   'confirmed',
      status:          'confirmed',
      price:           Number(amount),
      paidAt:          FieldValue.serverTimestamp(),
      paidBy:          adminName,
      paymentMethod:   paymentMethod,
      paymentNote:     String(paymentNote).slice(0, 400),
      adminReviewedAt: FieldValue.serverTimestamp(),
      confirmedAt:     FieldValue.serverTimestamp(),
      updatedAt:       FieldValue.serverTimestamp(),
    });
    batch.update(slotRef, {
      paymentStatus: 'paid',
      bookingStatus: 'confirmed',
    });
    await batch.commit();
  } catch (e) {
    console.error('[mark-paid] write:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to update booking' });
  }

  console.log(`[mark-paid] id:${bookingId} amount:${amount} method:${paymentMethod} admin:${adminName}`);
  return res.status(200).json({ ok: true });
}
