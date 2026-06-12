// ════════════════════════════════════════════════════════════════════
// POST /api/admin-edit-booking-accounting
// ════════════════════════════════════════════════════════════════════
// Consolidated route for admin operations on a booking
// (operation "delete_booking" was merged in from the former
//  /api/admin-delete-booking route to keep the Vercel function count down):
//
//   operation: "accounting_edit"  (Art-only)
//     Correct wrong booking/payment/accounting status.
//     Body fields: bookingId, accountingType, bookingStatus?, price?,
//                  influencerExpenseAmount?, reason
//
//   operation: "refund"           (any logged-in admin)
//     Process a customer refund.
//     Body fields: bookingId, refundAmount, refundMode, refundReason,
//                  refundNote, incidentType, incidentNote?, releaseSlot?
//
//   operation: "delete_booking"   (owner-only)
//     Permanently delete a booking record + its booking_slot + its
//     Google Calendar event. Body fields: bookingId
//
// Auth rules:
//   • Both operations require a valid adminSession cookie.
//   • "accounting_edit" additionally requires role "owner"
//     (legacy sessions map Art → owner, so behavior is unchanged).
//   • "refund" is allowed for any valid admin.
//
// ─── accounting_edit ───────────────────────────────────────────────
// accountingType values:
//   "normal_paid"      paymentStatus:paid, bookingStatus:confirmed
//   "normal_unpaid"    paymentStatus:unpaid
//   "pending_review"   paymentStatus:pending_review
//   "rejected"         paymentStatus:rejected, bookingStatus:cancelled (forced)
//   "ultra_pass_1"     paymentStatus:package, bookingType:Ultra Pass 1, 310 THB/hr
//   "ultra_pass_2"     paymentStatus:package, bookingType:Ultra Pass 2, 295 THB/hr
//   "influencer_free"  paymentStatus:package, bookingType:Influencer Free, auto Marketing expense
//
// Writes: bookings/{id}, booking_slots/{id}, finance_expenses (influencer_free)
// Does NOT touch: booking date/time, Google Calendar, customer notifications.
//
// ─── refund ────────────────────────────────────────────────────────
// Accounting design:
//   • Original paymentStatus is NEVER changed — revenue preserved in Finance.
//   • refundStatus: "refunded" | "partial_refunded" added as a separate field.
//   • finance_expenses record (category:"Refund") created/updated on today's date
//     (Bangkok UTC+7) so the refund appears in the Finance month it happened,
//     not the original play month.
//   • Idempotency via booking.refundExpenseId — re-refund updates existing expense.
//
// releaseSlot:
//   false (default) — bookingStatus unchanged; slot stays occupied.
//   true            — bookingStatus:"cancelled", booking_slots updated.
//                     available_slots intentionally NOT reopened (admin reviews
//                     machine/safety incidents before re-opening via Slot Manager).
//
// Writes: bookings/{id}, booking_slots/{id} (if releaseSlot), finance_expenses
// Does NOT touch: paymentStatus, price, slipUrl, package fields, Google Calendar.
// ════════════════════════════════════════════════════════════════════

import { verifySession, requireRole, resolveBranchId } from './_lib/admin-auth.js';
import { getAdminDb, writeAuditLog } from './_lib/firebase-admin.js';
import { FieldValue }          from 'firebase-admin/firestore';

// ── Shared constants ──────────────────────────────────────────────
const RESOURCE_ID = 'room1';

// ── accounting_edit constants ─────────────────────────────────────
const VALID_ACCOUNTING_TYPES = [
  'normal_paid', 'normal_unpaid', 'pending_review', 'rejected',
  'ultra_pass_1', 'ultra_pass_2', 'influencer_free',
];

// ── refund constants ──────────────────────────────────────────────
const VALID_REFUND_REASONS = [
  'machine_issue', 'safety_incident', 'customer_request',
  'admin_mistake', 'duplicate_payment', 'other',
];
const VALID_INCIDENT_TYPES = [
  'machine_malfunction', 'ball_hit_customer', 'room_issue',
  'booking_error', 'none', 'other',
];
const VALID_REFUND_MODES   = ['full_refund', 'partial_refund'];
const NOTES_REQUIRED_FOR   = new Set(['machine_issue', 'safety_incident']);

// ── Shared helpers ────────────────────────────────────────────────
function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

// ── Google Calendar OAuth — exchange refresh token for access token ──
// Mirrors the same logic as api/gcal.js (that function is not exported).
// Returns null on failure — caller treats null as "skip Calendar delete".
async function getCalendarAccessToken() {
  const { GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN } = process.env;
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !GOOGLE_REFRESH_TOKEN) return null;
  const params = new URLSearchParams({
    grant_type:    'refresh_token',
    client_id:     GOOGLE_CLIENT_ID,
    client_secret: GOOGLE_CLIENT_SECRET,
    refresh_token: GOOGLE_REFRESH_TOKEN,
  });
  try {
    const res = await fetch('https://oauth2.googleapis.com/token', {
      method:  'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body:    params.toString(),
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data.access_token || null;
  } catch {
    return null;
  }
}

// ── Delete a Google Calendar event by ID ────────────────────────────
// Returns true on success (204) or already-gone (404).
// Returns false on token failure or any other API error.
// Caller treats false as a hard stop — see handleDeleteBooking Step 1.
// sendUpdates=all — notifies BaiMon of cancellation.
async function deleteCalendarEvent(eventId) {
  const calendarId = process.env.GOOGLE_CALENDAR_ID;
  if (!calendarId || !eventId) return true; // nothing to delete

  const accessToken = await getCalendarAccessToken();
  if (!accessToken) {
    console.error('[delete-booking] calendar: could not obtain access token');
    return false;
  }

  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}?sendUpdates=all`;
  try {
    const res = await fetch(url, {
      method:  'DELETE',
      headers: { 'Authorization': `Bearer ${accessToken}` },
    });
    if (res.status === 204 || res.status === 404) return true;
    const preview = await res.text().catch(() => '');
    console.error(`[delete-booking] calendar delete failed — status:${res.status}`, preview.slice(0, 200));
    return false;
  } catch (e) {
    console.error('[delete-booking] calendar delete threw:', e.message);
    return false;
  }
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
  return 1;
}

// Bangkok date (UTC+7) for refund expense records.
function bangkokDateISO() {
  const now = new Date(Date.now() + 7 * 3600 * 1000);
  return now.toISOString().slice(0, 10);
}

// Snapshot accounting fields for reversibility audit.
function snapshotAccountingFields(b) {
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

// Snapshot refund fields for reversibility audit.
function snapshotRefundFields(b) {
  return {
    refundStatus:    b.refundStatus    ?? null,
    refundAmount:    b.refundAmount    ?? null,
    refundReason:    b.refundReason    ?? null,
    refundNote:      b.refundNote      ?? null,
    incidentType:    b.incidentType    ?? null,
    incidentNote:    b.incidentNote    ?? null,
    refundedBy:      b.refundedBy      ?? null,
    refundExpenseId: b.refundExpenseId ?? null,
  };
}

// ── Main handler ──────────────────────────────────────────────────
export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  // ── Auth: any valid session required for both operations ──────────
  const session = verifySession(req);
  if (!session) return res.status(401).json({ ok: false, error: 'Unauthorized' });
  const adminName = session.name;

  const body = parseBody(req);
  if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

  // ── Route by operation ────────────────────────────────────────────
  const operation = body.operation || 'accounting_edit';

  const VALID_OPERATIONS = ['accounting_edit', 'refund', 'mark_paid', 'delete_booking'];
  if (!VALID_OPERATIONS.includes(operation)) {
    return res.status(400).json({ ok: false, error: `Invalid operation. Must be one of: ${VALID_OPERATIONS.join(', ')}.` });
  }

  // ── Per-operation auth + input validation (before any DB call) ────

  if (operation === 'accounting_edit') {
    // Owner-only (legacy sessions: Art → owner)
    if (!requireRole(session, 'owner')) {
      return res.status(403).json({ ok: false, error: 'Owner access only.' });
    }
    const { accountingType, reason } = body;
    if (!VALID_ACCOUNTING_TYPES.includes(accountingType))
      return res.status(400).json({ ok: false, error: `Invalid accountingType. Must be one of: ${VALID_ACCOUNTING_TYPES.join(', ')}` });
    if (!reason || typeof reason !== 'string' || !reason.trim())
      return res.status(400).json({ ok: false, error: 'Reason is required' });
  }

  if (operation === 'refund') {
    // Any valid admin — no extra auth needed
    const { refundAmount: rawAmt, refundMode, refundReason, refundNote = '', incidentType = 'none' } = body;
    const refundAmount = Number(rawAmt);
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
  }

  if (operation === 'delete_booking') {
    // Owner-only: enforce server-side regardless of frontend hiding.
    if (!requireRole(session, 'owner')) {
      return res.status(403).json({ ok: false, error: 'Only the owner can permanently delete bookings.' });
    }
  }

  if (operation === 'mark_paid') {
    const { amount, paymentMethod } = body;
    if (!Number.isFinite(Number(amount)) || Number(amount) <= 0) {
      return res.status(400).json({ ok: false, error: 'Amount must be a positive number' });
    }
    const VALID_MARK_PAID_METHODS = ['cash', 'transfer', 'manual_confirmed', 'other'];
    if (!VALID_MARK_PAID_METHODS.includes(paymentMethod)) {
      return res.status(400).json({ ok: false, error: `Invalid paymentMethod. Must be one of: ${VALID_MARK_PAID_METHODS.join(', ')}` });
    }
  }

  // ── bookingId — required for all operations ──────────────────────
  const { bookingId } = body;
  if (!bookingId || typeof bookingId !== 'string' || !bookingId.trim())
    return res.status(400).json({ ok: false, error: 'Missing bookingId' });

  // ── DB init ───────────────────────────────────────────────────────
  let db;
  try { db = getAdminDb(); }
  catch (e) {
    console.error(`[${operation}] DB init:`, e.message);
    return res.status(500).json({ ok: false, error: 'Database not available' });
  }

  // ── Read booking (common to both operations) ──────────────────────
  const bookingRef = db.collection('bookings').doc(bookingId.trim());
  let booking;
  try {
    const snap = await bookingRef.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Booking not found' });
    booking = snap.data();
  } catch (e) {
    console.error(`[${operation}] read booking:`, e.message);
    return res.status(500).json({ ok: false, error: 'Failed to read booking' });
  }

  // ── Dispatch ──────────────────────────────────────────────────────
  if (operation === 'accounting_edit') {
    return handleAccountingEdit({ req, res, adminName, session, db, booking, bookingRef, bookingId: bookingId.trim(), body });
  }
  if (operation === 'refund') {
    return handleRefund({ req, res, adminName, session, db, booking, bookingRef, bookingId: bookingId.trim(), body });
  }
  if (operation === 'delete_booking') {
    return handleDeleteBooking({ res, adminName, session, db, booking, bookingRef, bookingId: bookingId.trim() });
  }
  return handleMarkPaid({ req, res, adminName, session, db, booking, bookingRef, bookingId: bookingId.trim(), body });
}

// ════════════════════════════════════════════════════════════════════
// handleAccountingEdit — Art-only accounting correction
// ════════════════════════════════════════════════════════════════════
async function handleAccountingEdit({ res, adminName, session, db, booking, bookingRef, bookingId, body }) {
  const {
    accountingType,
    bookingStatus:           requestedBookingStatus,
    price:                   rawPrice,
    influencerExpenseAmount: rawInfluencerAmt,
    reason,
  } = body;

  const dur           = calcDurationHours(booking);
  const price         = (rawPrice != null) ? Math.max(0, Number(rawPrice)) : 350;
  const influencerAmt = (rawInfluencerAmt != null) ? Math.max(1, Number(rawInfluencerAmt)) : Math.ceil(dur * 350);

  // ── Build accounting fields per type ──────────────────────────────
  let accountingFields = {};

  switch (accountingType) {
    case 'normal_paid':
      accountingFields = {
        bookingStatus:            requestedBookingStatus || booking.bookingStatus || 'confirmed',
        status:                   requestedBookingStatus || booking.bookingStatus || 'confirmed',
        paymentStatus:            'paid',
        price,
        packageType:              null,
        packageUsageValuePerHour: null,
        packageUsageValueTotal:   null,
        isInfluencerBooking:      false,
        influencerExpenseAmount:  null,
      };
      break;

    case 'normal_unpaid':
      accountingFields = {
        bookingStatus:            requestedBookingStatus || booking.bookingStatus || 'confirmed',
        status:                   requestedBookingStatus || booking.bookingStatus || 'confirmed',
        paymentStatus:            'unpaid',
        price,
        packageType:              null,
        packageUsageValuePerHour: null,
        packageUsageValueTotal:   null,
        isInfluencerBooking:      false,
        influencerExpenseAmount:  null,
      };
      break;

    case 'pending_review':
      accountingFields = {
        bookingStatus: requestedBookingStatus || booking.bookingStatus || 'confirmed',
        status:        requestedBookingStatus || booking.bookingStatus || 'confirmed',
        paymentStatus: 'pending_review',
      };
      break;

    case 'rejected':
      accountingFields = {
        bookingStatus:           'cancelled',   // always forced for rejected
        status:                  'cancelled',
        paymentStatus:           'rejected',
        isInfluencerBooking:     false,
        influencerExpenseAmount: null,
      };
      break;

    case 'ultra_pass_1':
      accountingFields = {
        bookingStatus:            requestedBookingStatus || booking.bookingStatus || 'confirmed',
        status:                   requestedBookingStatus || booking.bookingStatus || 'confirmed',
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
        bookingStatus:            requestedBookingStatus || booking.bookingStatus || 'confirmed',
        status:                   requestedBookingStatus || booking.bookingStatus || 'confirmed',
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
        bookingStatus:            requestedBookingStatus || booking.bookingStatus || 'confirmed',
        status:                   requestedBookingStatus || booking.bookingStatus || 'confirmed',
        paymentStatus:            'package',
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
  let expIdUpdate = {};

  if (!isNowInfluencer && wasInfluencer && existingExpId) {
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
      (booking.startTime && booking.endTime) ? `${booking.startTime}–${booking.endTime}` : '',
    ].filter(Boolean).join(' ').slice(0, 400);

    if (existingExpId) {
      const expRef = db.collection('finance_expenses').doc(existingExpId);
      batch.update(expRef, {
        amount:         influencerAmt,
        note:           expNote,
        updatedByAdmin: adminName,
        updatedAt:      FieldValue.serverTimestamp(),
      });
      console.log(`[acct-edit] updated influencer expense: ${existingExpId}`);
    } else {
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
        sourceBookingId: bookingId,
        addedByAdmin:    adminName,
        createdAt:       FieldValue.serverTimestamp(),
        updatedAt:       FieldValue.serverTimestamp(),
      });
      expIdUpdate = { influencerExpenseId: expRef.id };
      console.log(`[acct-edit] created influencer expense: ${expRef.id}`);
    }
  }

  // ── Update booking document ───────────────────────────────────────
  batch.update(bookingRef, {
    ...accountingFields,
    ...expIdUpdate,
    accountingEditedBy:         adminName,
    accountingEditedAt:         FieldValue.serverTimestamp(),
    accountingEditReason:       String(reason).trim().slice(0, 400),
    previousAccountingSnapshot: snapshotAccountingFields(booking),
    updatedAt:                  FieldValue.serverTimestamp(),
  });

  // ── Update booking_slots (best-effort, non-fatal if missing) ─────
  const slotId  = `${booking.resourceId || RESOURCE_ID}_${booking.date}_${(booking.startTime || '').replace(':', '')}`;
  const slotRef = db.collection('booking_slots').doc(slotId);
  try {
    const slotSnap = await slotRef.get();
    if (slotSnap.exists) {   // Admin SDK: .exists is a boolean property, not a method
      const slotData = slotSnap.data();
      const ownsSlot = slotData.bookingId === bookingId || slotData.bookingCode === booking.bookingCode;
      if (ownsSlot) {
        batch.update(slotRef, {
          paymentStatus: accountingFields.paymentStatus,
          bookingStatus: accountingFields.bookingStatus || booking.bookingStatus,
          updatedAt:     FieldValue.serverTimestamp(),
        });
      }
    }
  } catch (e) {
    console.error('[acct-edit] slot read (non-fatal):', e.message);
  }

  // ── Commit ────────────────────────────────────────────────────────
  try {
    await batch.commit();
  } catch (e) {
    console.error('[acct-edit] batch commit:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to save accounting changes' });
  }

  console.log(`[acct-edit] OK — booking:${bookingId} type:${accountingType} admin:${adminName}`);
  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role,
    branchId: resolveBranchId(booking),
    action: 'accounting_edit', targetId: bookingId,
    before: snapshotAccountingFields(booking),
    after: { accountingType, paymentStatus: accountingFields.paymentStatus ?? null, bookingStatus: accountingFields.bookingStatus ?? null, price: accountingFields.price ?? null },
    note: String(reason).trim().slice(0, 400),
  });
  return res.status(200).json({ ok: true });
}

// ════════════════════════════════════════════════════════════════════
// handleRefund — any valid admin
// ════════════════════════════════════════════════════════════════════
async function handleRefund({ res, adminName, session, db, booking, bookingRef, bookingId, body }) {
  const {
    refundAmount:  rawAmount,
    refundMode,
    refundReason,
    refundNote   = '',
    incidentType = 'none',
    incidentNote = '',
    releaseSlot  = false,
  } = body;

  const refundAmount  = Number(rawAmount);
  const originalPrice = Number(booking.price) || 0;
  const refundStatus  = refundAmount >= originalPrice ? 'refunded' : 'partial_refunded';

  // ── Finance expense note ──────────────────────────────────────────
  const today = bangkokDateISO();
  const expNote = [
    `Auto refund: ${booking.bookingCode || bookingId}`,
    booking.customerName  ? `- ${booking.customerName}`  : '',
    booking.customerPhone ? `(${booking.customerPhone})` : '',
    booking.date          ? `plays ${booking.date}`      : '',
    (booking.startTime && booking.endTime) ? `${booking.startTime}–${booking.endTime}` : '',
    `| Reason: ${refundReason}`,
    incidentType !== 'none' ? `| Incident: ${incidentType}` : '',
    refundNote ? `| Note: ${String(refundNote).slice(0, 120)}` : '',
  ].filter(Boolean).join(' ').slice(0, 400);

  const batch = db.batch();

  // ── Finance expense: create or update (idempotency via refundExpenseId) ──
  const existingExpId = booking.refundExpenseId || null;
  let newExpId = existingExpId;

  if (existingExpId) {
    const expRef = db.collection('finance_expenses').doc(existingExpId);
    batch.update(expRef, {
      amount:         refundAmount,
      date:           today,
      note:           expNote,
      vendor:         String(booking.customerName || '—').slice(0, 200),
      updatedByAdmin: adminName,
      updatedAt:      FieldValue.serverTimestamp(),
      deleted:        false,   // un-delete if previously voided
      deletedAt:      null,
      deletedBy:      null,
    });
    console.log(`[refund] updated expense: ${existingExpId}`);
  } else {
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
      sourceBookingId: bookingId,
      addedByAdmin:    adminName,
      createdAt:       FieldValue.serverTimestamp(),
      updatedAt:       FieldValue.serverTimestamp(),
    });
    console.log(`[refund] created expense: ${newExpId}`);
  }

  // ── Update booking (refund fields only — paymentStatus/price unchanged) ──
  const bookingUpdate = {
    refundStatus,
    refundAmount,
    refundMode,
    refundReason,
    refundNote:              String(refundNote   || '').slice(0, 400),
    incidentType,
    incidentNote:            String(incidentNote || '').slice(0, 400),
    refundedBy:              adminName,
    refundedAt:              FieldValue.serverTimestamp(),
    refundExpenseId:         newExpId,
    previousRefundSnapshot:  snapshotRefundFields(booking),
    updatedAt:               FieldValue.serverTimestamp(),
  };

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
      if (slotSnap.exists) {   // Admin SDK: .exists is a boolean property, not a method
        const slotData = slotSnap.data();
        const ownsSlot = slotData.bookingId === bookingId || slotData.bookingCode === booking.bookingCode;
        if (ownsSlot) {
          batch.update(slotRef, {
            bookingStatus: 'cancelled',
            paymentStatus: 'refunded',   // distinct from normal "rejected" cancels
            updatedAt:     FieldValue.serverTimestamp(),
          });
        }
        // NOTE: available_slots intentionally NOT reopened — machine/safety incidents
        // warrant admin review before the slot is offered again (use Slot Manager).
      }
    } catch (e) {
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
  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role,
    branchId: resolveBranchId(booking),
    action: 'refund', targetId: bookingId,
    before: snapshotRefundFields(booking),
    after: { refundStatus, refundAmount, refundReason, releaseSlot: releaseSlot === true },
  });
  return res.status(200).json({ ok: true, refundStatus, refundExpenseId: newExpId });
}

// ════════════════════════════════════════════════════════════════════
// handleMarkPaid — any valid admin, marks unpaid booking as paid
// ════════════════════════════════════════════════════════════════════
async function handleMarkPaid({ res, adminName, session, db, booking, bookingRef, bookingId, body }) {
  const { amount, paymentMethod, paymentNote = '' } = body;

  if (booking.bookingStatus === 'cancelled') {
    return res.status(409).json({ ok: false, error: 'Cannot mark a cancelled booking as paid' });
  }
  if (booking.paymentStatus === 'paid') {
    return res.status(409).json({ ok: false, error: 'Booking is already paid' });
  }
  if (booking.paymentStatus !== 'unpaid') {
    return res.status(409).json({ ok: false, error: `Cannot mark paid: current paymentStatus is "${booking.paymentStatus}"` });
  }

  const slotId  = `${booking.resourceId || RESOURCE_ID}_${booking.date}_${booking.startTime?.replace(':', '')}`;
  const slotRef = db.collection('booking_slots').doc(slotId);

  try {
    const slotSnap = await slotRef.get();
    if (!slotSnap.exists) {
      return res.status(409).json({ ok: false, error: 'Slot does not exist' });
    }
    const slotData = slotSnap.data();
    const ownsSlot = slotData.bookingId === bookingId || slotData.bookingCode === booking.bookingCode;
    if (!ownsSlot) {
      return res.status(409).json({ ok: false, error: 'Conflict: Slot is owned by another booking' });
    }

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
  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role,
    branchId: resolveBranchId(booking),
    action: 'mark_paid', targetId: bookingId,
    before: { paymentStatus: booking.paymentStatus, bookingStatus: booking.bookingStatus },
    after: { paymentStatus: 'paid', bookingStatus: 'confirmed', price: Number(amount), paymentMethod },
  });
  return res.status(200).json({ ok: true });
}

// ════════════════════════════════════════════════════════════════════
// handleDeleteBooking — owner-only permanent delete
// (moved verbatim from the former /api/admin-delete-booking route)
// ════════════════════════════════════════════════════════════════════
// What is deleted:
//   1. Google Calendar event (if googleCalendarEventId is set on the booking)
//   2. booking_slots/{resourceId}_{date}_{startTime} — only if it belongs to this booking
//   3. bookings/{bookingId}
// What is NOT deleted: customer profile, customer_packages,
// notification_logs, unrelated bookings/slots, available_slots.
// Use case: test data cleanup / mistaken booking records.
async function handleDeleteBooking({ res, adminName, session, db, booking, bookingRef, bookingId }) {
  const { resourceId, date, startTime, bookingCode, googleCalendarEventId } = booking;
  console.log(`[delete-booking] START — admin:${adminName} id:${bookingId} code:${bookingCode}`);

  // ── Step 1: delete Google Calendar event (BLOCKING) ─────────────
  // If the booking has a Calendar event, it MUST be deleted before any
  // Firestore writes. A failure here stops the entire operation so we
  // never create an orphan Calendar event with no admin UI record to
  // retry or clean up from.
  // 404 from Google = event already gone → treat as success and continue.
  if (googleCalendarEventId) {
    const calOk = await deleteCalendarEvent(googleCalendarEventId);
    if (!calOk) {
      console.error(`[delete-booking] calendar delete failed for ${googleCalendarEventId} — booking NOT deleted`);
      return res.status(200).json({
        ok: false,
        error: 'Calendar delete failed. Booking was not deleted. Please retry.',
      });
    }
    console.log(`[delete-booking] calendar event removed: ${googleCalendarEventId}`);
  }

  // ── Step 2: delete booking_slot doc if it belongs to this booking ──
  // Slot ID pattern: {resourceId}_{date}_{startTime without colon}
  // Safety: only delete if the slot's bookingId/bookingCode matches.
  // Protects against accidentally releasing a slot reassigned to another booking.
  if (resourceId && date && startTime) {
    const slotId = `${resourceId}_${date}_${startTime.replace(':', '')}`;
    const slotRef = db.collection('booking_slots').doc(slotId);
    try {
      const slotSnap = await slotRef.get();
      if (!slotSnap.exists) {
        console.log(`[delete-booking] booking_slot ${slotId} — not found, skipped`);
      } else {
        const slotData = slotSnap.data();
        const ownsSlot = slotData.bookingId === bookingId || slotData.bookingCode === bookingCode;
        if (ownsSlot) {
          await slotRef.delete();
          console.log(`[delete-booking] booking_slot deleted: ${slotId}`);
        } else {
          // Slot belongs to a different booking — do NOT touch it.
          console.log(`[delete-booking] booking_slot ${slotId} belongs to ${slotData.bookingCode} — skipped`);
        }
      }
    } catch (e) {
      // Non-fatal — log and continue to delete the booking document.
      console.error('[delete-booking] booking_slot delete error:', e.message);
    }
  } else {
    console.log(`[delete-booking] missing resourceId/date/startTime — slot cleanup skipped`);
  }

  // ── Step 3: delete the booking document ───────────────────────────
  try {
    await bookingRef.delete();
    console.log(`[delete-booking] DONE — booking deleted: ${bookingId} code:${bookingCode}`);
  } catch (e) {
    console.error('[delete-booking] failed to delete booking doc:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to delete booking record' });
  }

  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role,
    branchId: resolveBranchId(booking),
    action: 'delete_booking', targetId: bookingId,
    before: { bookingCode, date, startTime, bookingStatus: booking.bookingStatus, paymentStatus: booking.paymentStatus },
  });

  return res.status(200).json({ ok: true });
}
