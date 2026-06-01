// ════════════════════════════════════════════════════════════════════
// POST /api/admin-refund-booking
// ════════════════════════════════════════════════════════════════════
// Any logged-in admin can process a refund for a customer booking.
//
// Auth:   valid adminSession cookie (any admin — not Art-only).
// Body:   {
//           bookingId:      string   (required)
//           refundAmount:   number   (required, > 0)
//           refundMode:     "full_refund" | "partial_refund"  (required)
//           refundReason:   "machine_issue" | "safety_incident" | "customer_request"
//                           | "admin_mistake" | "duplicate_payment" | "other"  (required)
//           refundNote:     string   (required for machine_issue / safety_incident)
//           incidentType:   "machine_malfunction" | "ball_hit_customer" | "room_issue"
//                           | "booking_error" | "none" | "other"  (required)
//           incidentNote:   string   (optional)
//           releaseSlot:    boolean  (default false)
//         }
//
// Accounting design:
//   • Original paymentStatus is NEVER changed — the booking stays "paid" / "package"
//     so the original month's revenue is preserved in Finance reports.
//   • refundStatus: "refunded" | "partial_refunded" is added as a separate field.
//   • A finance_expenses record (category:"Refund") is created / updated in the
//     REFUND month (today's date), not the play month.  This way:
//       - Original month: full booking revenue remains.
//       - Refund month: Refund expense appears under Finance > Expenses.
//       - Net P/L of refund month is correctly reduced.
//
// Idempotency:
//   If booking.refundExpenseId already exists, the existing finance_expenses doc
//   is updated instead of creating a duplicate.
//
// Slot release (releaseSlot === true):
//   • Sets bookingStatus:"cancelled" on the booking.
//   • Updates matching booking_slots doc: bookingStatus:"cancelled", paymentStatus:"refunded".
//   • Does NOT reopen available_slots — a machine/safety incident warrants admin review
//     before the slot is made available again.  Admin can reopen via Slot Manager.
//
// Writes (Firestore batch):
//   bookings/{bookingId}        refund fields + audit snapshot
//   booking_slots/{slotId}      bookingStatus + paymentStatus  (only if releaseSlot)
//   finance_expenses/{id}       create or update Refund expense
//
// Does NOT touch: paymentStatus, price, slipUrl, package fields, Google Calendar.
// ════════════════════════════════════════════════════════════════════

import { verifySessionCookie } from './_lib/admin-auth.js';
import { getAdminDb }          from './_lib/firebase-admin.js';
import { FieldValue }          from 'firebase-admin/firestore';

const RESOURCE_ID = 'room1';

const VALID_REFUND_REASONS  = ['machine_issue','safety_incident','customer_request',
                                'admin_mistake','duplicate_payment','other'];
const VALID_INCIDENT_TYPES  = ['machine_malfunction','ball_hit_customer','room_issue',
                                'booking_error','none','other'];
const VALID_REFUND_MODES    = ['full_refund','partial_refund'];
const NOTES_REQUIRED_FOR    = new Set(['machine_issue','safety_incident']);

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

// Bangkok date (UTC+7) for the finance expense record.
// The refund expense date should reflect when the refund happened,
// not when the customer played.
function bangkokDateISO() {
  const now = new Date(Date.now() + 7 * 3600 * 1000);
  return now.toISOString().slice(0, 10);
}

function snapshotRefundFields(b) {
  return {
    refundStatus:     b.refundStatus     ?? null,
    refundAmount:     b.refundAmount     ?? null,
    refundReason:     b.refundReason     ?? null,
    refundNote:       b.refundNote       ?? null,
    incidentType:     b.incidentType     ?? null,
    incidentNote:     b.incidentNote     ?? null,
    refundedBy:       b.refundedBy       ?? null,
    refundExpenseId:  b.refundExpenseId  ?? null,
  };
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  // ── Auth: any valid admin ─────────────────────────────────────────
  const adminName = verifySessionCookie(req);
  if (!adminName) return res.status(401).json({ ok: false, error: 'Unauthorized' });

  const body = parseBody(req);
  if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

  const {
    bookingId,
    refundAmount:   rawAmount,
    refundMode,
    refundReason,
    refundNote   = '',
    incidentType = 'none',
    incidentNote = '',
    releaseSlot  = false,
  } = body;

  // ── Input validation ──────────────────────────────────────────────
  if (!bookingId || typeof bookingId !== 'string' || !bookingId.trim())
    return res.status(400).json({ ok: false, error: 'Missing bookingId' });

  const refundAmount = Number(rawAmount);
  if (!Number.isFinite(refundAmount) || refundAmount <= 0)
    return res.status(400).json({ ok: false, error: 'refundAmount must be a positive number' });

  if (!VALID_REFUND_MODES.includes(refundMode))
    return res.status(400).json({ ok: false, error: `Invalid refundMode. Must be one of: ${VALID_REFUND_MODES.join(', ')}` });

  if (!VALID_REFUND_REASONS.includes(refundReason))
    return res.status(400).json({ ok: false, error: `Invalid refundReason. Must be one of: ${VALID_REFUND_REASONS.join(', ')}` });

  if (!VALID_INCIDENT_TYPES.includes(incidentType))
    return res.status(400).json({ ok: false, error: `Invalid incidentType. Must be one of: ${VALID_INCIDENT_TYPES.join(', ')}` });

  if (NOTES_REQUIRED_FOR.has(refundReason) && !String(refundNote).trim())
    return res.status(400).json({ ok: false, error: `refundNote is required for refundReason "${refundReason}"` });

  // ── DB ────────────────────────────────────────────────────────────
  let db;
  try { db = getAdminDb(); }
  catch (e) {
    console.error('[refund] DB init:', e.message);
    return res.status(500).json({ ok: false, error: 'Database not available' });
  }

  // ── Read booking ──────────────────────────────────────────────────
  const bookingRef = db.collection('bookings').doc(bookingId.trim());
  let booking;
  try {
    const snap = await bookingRef.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Booking not found' });
    booking = snap.data();
  } catch (e) {
    console.error('[refund] read booking:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to read booking' });
  }

  // ── Derive refundStatus ───────────────────────────────────────────
  const originalPrice = Number(booking.price) || 0;
  const refundStatus  = refundAmount >= originalPrice ? 'refunded' : 'partial_refunded';

  // ── Finance expense: build note ───────────────────────────────────
  const today = bangkokDateISO();
  const noteSegments = [
    `Auto refund: ${booking.bookingCode || bookingId}`,
    booking.customerName  ? `- ${booking.customerName}`  : '',
    booking.customerPhone ? `(${booking.customerPhone})` : '',
    booking.date          ? `plays ${booking.date}`      : '',
    (booking.startTime && booking.endTime)
      ? `${booking.startTime}–${booking.endTime}` : '',
    `| Reason: ${refundReason}`,
    incidentType !== 'none' ? `| Incident: ${incidentType}` : '',
    refundNote ? `| Note: ${String(refundNote).slice(0, 120)}` : '',
  ];
  const expNote = noteSegments.filter(Boolean).join(' ').slice(0, 400);

  // ── Build Firestore batch ─────────────────────────────────────────
  const batch = db.batch();

  // ── Finance expense: create or update (idempotency) ───────────────
  const existingExpId = booking.refundExpenseId || null;
  let newExpId = existingExpId;

  if (existingExpId) {
    // Update the existing expense — same booking, possibly changed amount/reason.
    const expRef = db.collection('finance_expenses').doc(existingExpId);
    batch.update(expRef, {
      amount:         refundAmount,
      date:           today,
      note:           expNote,
      vendor:         String(booking.customerName || '—').slice(0, 200),
      updatedByAdmin: adminName,
      updatedAt:      FieldValue.serverTimestamp(),
      // Un-delete in case it was voided previously
      deleted:        false,
      deletedAt:      null,
      deletedBy:      null,
    });
    console.log(`[refund] updated expense: ${existingExpId}`);
  } else {
    // Create a new expense record.
    const expRef = db.collection('finance_expenses').doc();
    newExpId = expRef.id;
    batch.set(expRef, {
      businessUnit:    'ultra_tennis',
      date:            today,
      category:        'Refund',
      amount:          refundAmount,
      paymentMethod:   'Transfer',
      vendor:          String(booking.customerName || '—').slice(0, 200),
      note:            expNote,
      deleted:         false,
      autoCreated:     true,
      sourceType:      'booking_refund',
      sourceBookingId: bookingId.trim(),
      addedByAdmin:    adminName,
      createdAt:       FieldValue.serverTimestamp(),
      updatedAt:       FieldValue.serverTimestamp(),
    });
    console.log(`[refund] created expense: ${newExpId}`);
  }

  // ── Update booking document ───────────────────────────────────────
  const bookingUpdate = {
    // Refund fields — do NOT overwrite paymentStatus / price / slipUrl / packageType
    refundStatus,
    refundAmount,
    refundMode,
    refundReason,
    refundNote:                String(refundNote  || '').slice(0, 400),
    incidentType,
    incidentNote:              String(incidentNote|| '').slice(0, 400),
    refundedBy:                adminName,
    refundedAt:                FieldValue.serverTimestamp(),
    refundExpenseId:           newExpId,
    previousRefundSnapshot:    snapshotRefundFields(booking),
    updatedAt:                 FieldValue.serverTimestamp(),
  };

  // If slot release is requested, also cancel the booking lifecycle.
  if (releaseSlot === true) {
    bookingUpdate.bookingStatus = 'cancelled';
    bookingUpdate.cancelledAt   = FieldValue.serverTimestamp();
    bookingUpdate.cancelReason  = `Refund: ${refundReason}`;
  }

  batch.update(bookingRef, bookingUpdate);

  // ── Update booking_slots (only when releasing the slot) ───────────
  if (releaseSlot === true) {
    const slotId  = `${booking.resourceId || RESOURCE_ID}_${booking.date}_${(booking.startTime || '').replace(':', '')}`;
    const slotRef = db.collection('booking_slots').doc(slotId);
    try {
      const slotSnap = await slotRef.get();
      if (slotSnap.exists) {
        batch.update(slotRef, {
          bookingStatus: 'cancelled',
          // Use "refunded" to distinguish refund cancels from normal slip-rejected cancels.
          // available_slots is intentionally NOT reopened — the admin reviews machine/
          // safety incidents before re-opening slots via Slot Manager.
          paymentStatus: 'refunded',
          updatedAt:     FieldValue.serverTimestamp(),
        });
      }
    } catch (e) {
      // Non-fatal — log and continue; slot inconsistency can be fixed via Slot Manager.
      console.error('[refund] slot read (non-fatal):', e.message);
    }
  }

  // ── Commit ────────────────────────────────────────────────────────
  try {
    await batch.commit();
  } catch (e) {
    console.error('[refund] batch commit:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to save refund' });
  }

  console.log(`[refund] OK — booking:${bookingId} amount:${refundAmount} status:${refundStatus} admin:${adminName}`);
  return res.status(200).json({ ok: true, refundStatus, refundExpenseId: newExpId });
}
