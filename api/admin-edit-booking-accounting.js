// ════════════════════════════════════════════════════════════════════
// POST /api/admin-edit-booking-accounting
// ════════════════════════════════════════════════════════════════════
// Art-only owner tool to correct booking/payment/accounting status.
//
// Auth:   valid adminSession cookie AND adminName === "Art" (server-enforced).
// Body:   {
//           bookingId:               string   (required)
//           accountingType:          string   (required — see VALID_TYPES below)
//           bookingStatus?:          string   (optional lifecycle override)
//           price?:                  number   (for normal_paid / normal_unpaid)
//           influencerExpenseAmount?: number  (for influencer_free)
//           reason:                  string   (required — saved to audit log)
//         }
//
// accountingType values:
//   "normal_paid"      paymentStatus:paid, bookingStatus:confirmed
//   "normal_unpaid"    paymentStatus:unpaid
//   "pending_review"   paymentStatus:pending_review
//   "rejected"         paymentStatus:rejected, bookingStatus:cancelled (forced)
//   "ultra_pass_1"     paymentStatus:package, bookingType:Ultra Pass 1, 310 THB/hr
//   "ultra_pass_2"     paymentStatus:package, bookingType:Ultra Pass 2, 295 THB/hr
//   "influencer_free"  paymentStatus:package, bookingType:Influencer Free, auto Marketing expense
//
// Writes:
//   bookings/{bookingId}      accounting + audit fields
//   booking_slots/{slotId}    paymentStatus + bookingStatus (if slot doc exists)
//   finance_expenses (create / update / soft-delete for influencer_free)
//
// Does NOT touch: booking date/time, Google Calendar, customer notifications.
// ════════════════════════════════════════════════════════════════════

import { verifySessionCookie } from './_lib/admin-auth.js';
import { getAdminDb }          from './_lib/firebase-admin.js';
import { FieldValue }          from 'firebase-admin/firestore';

const OWNER       = 'Art';
const RESOURCE_ID = 'room1';
const VALID_TYPES = [
  'normal_paid', 'normal_unpaid', 'pending_review', 'rejected',
  'ultra_pass_1', 'ultra_pass_2', 'influencer_free',
];

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

// Compute booking duration in hours from stored field or from HH:MM times.
function calcDurationHours(booking) {
  const stored = Number(booking.durationHours);
  if (stored > 0) return stored;
  if (booking.startTime && booking.endTime) {
    const [sh, sm] = booking.startTime.split(':').map(Number);
    const [eh, em] = booking.endTime.split(':').map(Number);
    const mins = (eh * 60 + em) - (sh * 60 + sm);
    if (mins > 0) return mins / 60;
  }
  return 1; // safe fallback — 1-hour slot
}

// Snapshot the accounting fields before this edit so the change is reversible.
function snapshotFields(b) {
  return {
    bookingStatus:            b.bookingStatus            ?? null,
    paymentStatus:            b.paymentStatus            ?? null,
    price:                    b.price                    ?? null,
    bookingType:              b.bookingType              ?? null,
    packageType:              b.packageType              ?? null,
    packageUsageValuePerHour: b.packageUsageValuePerHour ?? null,
    packageUsageValueTotal:   b.packageUsageValueTotal   ?? null,
    isInfluencerBooking:      b.isInfluencerBooking      ?? false,
    influencerExpenseAmount:  b.influencerExpenseAmount  ?? null,
    influencerExpenseId:      b.influencerExpenseId      ?? null,
  };
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  // ── Auth: valid session + Art-only ────────────────────────────────
  const adminName = verifySessionCookie(req);
  if (!adminName) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  if (adminName !== OWNER) {
    return res.status(403).json({ ok: false, error: 'Owner access only.' });
  }

  const body = parseBody(req);
  if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

  const {
    bookingId,
    accountingType,
    bookingStatus:            requestedBookingStatus,
    price:                    rawPrice,
    influencerExpenseAmount:  rawInfluencerAmt,
    reason,
  } = body;

  if (!bookingId || typeof bookingId !== 'string' || !bookingId.trim())
    return res.status(400).json({ ok: false, error: 'Missing bookingId' });
  if (!VALID_TYPES.includes(accountingType))
    return res.status(400).json({ ok: false, error: `Invalid accountingType. Must be one of: ${VALID_TYPES.join(', ')}` });
  if (!reason || typeof reason !== 'string' || !reason.trim())
    return res.status(400).json({ ok: false, error: 'Reason is required' });

  // ── DB ────────────────────────────────────────────────────────────
  let db;
  try { db = getAdminDb(); }
  catch (e) {
    console.error('[acct-edit] DB init:', e.message);
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
    console.error('[acct-edit] read booking:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to read booking' });
  }

  // ── Derived values ────────────────────────────────────────────────
  const dur            = calcDurationHours(booking);
  const price          = (rawPrice != null) ? Math.max(0, Number(rawPrice)) : 350;
  const influencerAmt  = (rawInfluencerAmt != null) ? Math.max(1, Number(rawInfluencerAmt)) : Math.ceil(dur * 350);

  // ── Build accounting fields per type ──────────────────────────────
  let accountingFields = {};

  switch (accountingType) {
    case 'normal_paid':
      accountingFields = {
        bookingStatus:            requestedBookingStatus || 'confirmed',
        status:                   requestedBookingStatus || 'confirmed',
        paymentStatus:            'paid',
        price:                    price,
        packageType:              null,
        packageUsageValuePerHour: null,
        packageUsageValueTotal:   null,
        isInfluencerBooking:      false,
        influencerExpenseAmount:  null,
      };
      break;

    case 'normal_unpaid':
      accountingFields = {
        bookingStatus:            requestedBookingStatus || 'confirmed',
        status:                   requestedBookingStatus || 'confirmed',
        paymentStatus:            'unpaid',
        price:                    price,
        packageType:              null,
        packageUsageValuePerHour: null,
        packageUsageValueTotal:   null,
        isInfluencerBooking:      false,
        influencerExpenseAmount:  null,
      };
      break;

    case 'pending_review':
      accountingFields = {
        bookingStatus:            requestedBookingStatus || booking.bookingStatus || 'confirmed',
        status:                   requestedBookingStatus || booking.bookingStatus || 'confirmed',
        paymentStatus:            'pending_review',
      };
      break;

    case 'rejected':
      accountingFields = {
        bookingStatus:            'cancelled',   // always forced for rejected
        status:                   'cancelled',
        paymentStatus:            'rejected',
        isInfluencerBooking:      false,
        influencerExpenseAmount:  null,
      };
      break;

    case 'ultra_pass_1':
      accountingFields = {
        bookingStatus:            requestedBookingStatus || 'confirmed',
        status:                   requestedBookingStatus || 'confirmed',
        paymentStatus:            'package',
        bookingType:              'Ultra Pass 1',
        packageType:              'ultra_10',
        packageUsageValuePerHour: 310,
        packageUsageValueTotal:   dur * 310,
        price:                    0,
        isInfluencerBooking:      false,
        influencerExpenseAmount:  null,
      };
      break;

    case 'ultra_pass_2':
      accountingFields = {
        bookingStatus:            requestedBookingStatus || 'confirmed',
        status:                   requestedBookingStatus || 'confirmed',
        paymentStatus:            'package',
        bookingType:              'Ultra Pass 2',
        packageType:              'ultra_20',
        packageUsageValuePerHour: 295,
        packageUsageValueTotal:   dur * 295,
        price:                    0,
        isInfluencerBooking:      false,
        influencerExpenseAmount:  null,
      };
      break;

    case 'influencer_free':
      accountingFields = {
        bookingStatus:            requestedBookingStatus || 'confirmed',
        status:                   requestedBookingStatus || 'confirmed',
        paymentStatus:            'package',   // filters out of cash revenue safely
        bookingType:              'Influencer Free',
        packageType:              null,
        packageUsageValuePerHour: null,
        packageUsageValueTotal:   null,
        isInfluencerBooking:      true,
        price:                    0,
        influencerExpenseAmount:  influencerAmt,
      };
      break;
  }

  // ── Influencer expense: create / update / soft-delete ─────────────
  const wasInfluencer   = booking.isInfluencerBooking === true;
  const isNowInfluencer = accountingType === 'influencer_free';
  const existingExpId   = booking.influencerExpenseId || null;

  const batch = db.batch();
  let expIdUpdate = {};   // will be merged into booking update if expense ID changes

  if (!isNowInfluencer && wasInfluencer && existingExpId) {
    // Switching AWAY from influencer_free — soft-delete the auto-created expense.
    // finance-data.js already filters !e.deleted, so it vanishes from Finance totals.
    const expRef = db.collection('finance_expenses').doc(existingExpId);
    batch.update(expRef, {
      deleted:   true,
      deletedAt: FieldValue.serverTimestamp(),
      deletedBy: adminName,
    });
    expIdUpdate = { influencerExpenseId: null };
    console.log(`[acct-edit] soft-deleted influencer expense: ${existingExpId}`);

  } else if (isNowInfluencer) {
    const expNote = [
      `Auto: ${booking.bookingCode || bookingId}`,
      booking.customerName  ? `- ${booking.customerName}`  : '',
      booking.customerPhone ? `(${booking.customerPhone})` : '',
      booking.date          ? `plays ${booking.date}`      : '',
      (booking.startTime && booking.endTime)
        ? `${booking.startTime}–${booking.endTime}`
        : '',
    ].filter(Boolean).join(' ').slice(0, 400);

    if (existingExpId) {
      // Update existing expense in-place (amount may have changed).
      const expRef = db.collection('finance_expenses').doc(existingExpId);
      batch.update(expRef, {
        amount:         influencerAmt,
        note:           expNote,
        updatedByAdmin: adminName,
        updatedAt:      FieldValue.serverTimestamp(),
      });
      // influencerExpenseId on the booking stays the same — no update needed.
      console.log(`[acct-edit] updated influencer expense: ${existingExpId}`);
    } else {
      // Create a new expense and store its auto-generated ID back on the booking.
      const expRef = db.collection('finance_expenses').doc();
      batch.set(expRef, {
        businessUnit:    'ultra_tennis',
        date:            booking.date || new Date().toISOString().slice(0, 10),
        category:        'Marketing',
        amount:          influencerAmt,
        paymentMethod:   'Other',
        vendor:          'Influencer Free Slot',
        note:            expNote,
        deleted:         false,
        autoCreated:     true,
        sourceType:      'influencer_free_slot',
        sourceBookingId: bookingId.trim(),
        addedByAdmin:    adminName,
        createdAt:       FieldValue.serverTimestamp(),
        updatedAt:       FieldValue.serverTimestamp(),
      });
      expIdUpdate = { influencerExpenseId: expRef.id };
      console.log(`[acct-edit] created influencer expense: ${expRef.id}`);
    }
  }

  // ── Update booking document ────────────────────────────────────────
  batch.update(bookingRef, {
    ...accountingFields,
    ...expIdUpdate,
    accountingEditedBy:          adminName,
    accountingEditedAt:          FieldValue.serverTimestamp(),
    accountingEditReason:        String(reason).trim().slice(0, 400),
    previousAccountingSnapshot:  snapshotFields(booking),
    updatedAt:                   FieldValue.serverTimestamp(),
  });

  // ── Update booking_slots (best-effort, non-fatal if missing) ─────
  const slotId  = `${booking.resourceId || RESOURCE_ID}_${booking.date}_${(booking.startTime || '').replace(':', '')}`;
  const slotRef = db.collection('booking_slots').doc(slotId);
  try {
    const slotSnap = await slotRef.get();
    if (slotSnap.exists) {
      batch.update(slotRef, {
        paymentStatus: accountingFields.paymentStatus,
        bookingStatus: accountingFields.bookingStatus || booking.bookingStatus,
        updatedAt:     FieldValue.serverTimestamp(),
      });
    }
  } catch (e) {
    // Non-fatal — log and continue; slot inconsistency can be fixed separately.
    console.error('[acct-edit] slot read (non-fatal):', e.message);
  }

  // ── Commit ────────────────────────────────────────────────────────
  try {
    await batch.commit();
  } catch (e) {
    console.error('[acct-edit] batch commit error:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to save accounting changes' });
  }

  console.log(`[acct-edit] OK — booking:${bookingId} type:${accountingType} admin:${adminName}`);
  return res.status(200).json({ ok: true });
}
