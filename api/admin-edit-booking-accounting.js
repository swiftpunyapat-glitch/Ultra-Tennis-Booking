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

import { verifySession, requireRole, resolveBranchId, hasBranchAccess } from './_lib/admin-auth.js';
import { getAdminDb, writeAuditLog } from './_lib/firebase-admin.js';
import { FieldValue }          from 'firebase-admin/firestore';

// ── Shared constants ──────────────────────────────────────────────
const RESOURCE_ID = 'room1';

// ── Reschedule helpers (ported 1:1 from admin.html) ───────────────
const RESCHED_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const RESCHED_TIME_RE = /^\d{2}:\d{2}$/;

// Pending reschedule is a derived state — a flag on a 'rescheduled' booking,
// NOT a raw status (rules enum has no 'pending_reschedule'). admin.html:1233.
function isPendingRescheduleBooking(b) {
  return b?.bookingStatus === 'pending_reschedule' ||
         (b?.bookingStatus === 'rescheduled' && b?.pendingReschedule === true);
}

// Deterministic slot id: room1_<date>_<HHMM> (startTime with ':' stripped).
function reschedSlotId(resourceId, date, startTime) {
  return `${resourceId || RESOURCE_ID}_${date}_${String(startTime).replace(':', '')}`;
}

// End time one hour after an "HH:00" start; midnight-crossing → "00:00".
function nextHourEnd(startTime) {
  const h = Number(String(startTime).split(':')[0]) + 1;
  return h >= 24 ? '00:00' : `${String(h).padStart(2, '0')}:00`;
}

// Ported 1:1 from admin.html:927-935. A booking_slots doc blocks its hour only
// when confirmed, or pending_payment that has not yet expired. Expiry field is
// `expiresAt` (a Firestore Timestamp on the slot doc).
function isLiveBookedSlot(slotData, nowMs = Date.now()) {
  if (!slotData) return false;
  if (slotData.bookingStatus === 'confirmed') return true;
  if (slotData.bookingStatus === 'pending_payment') {
    const exp = slotData.expiresAt;
    const expMs = exp && typeof exp.toMillis === 'function' ? exp.toMillis() : null;
    return expMs !== null && expMs > nowMs;
  }
  return false;
}

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

  const VALID_OPERATIONS = ['accounting_edit', 'refund', 'mark_paid', 'approve_slip', 'reject_payment', 'delete_booking', 'reschedule_park', 'reschedule_assign', 'reschedule_cancel', 'assign_coach', 'coach_lesson_update'];
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
  if (operation === 'approve_slip') {
    return handleApproveSlip({ res, adminName, session, db, booking, bookingRef, bookingId: bookingId.trim(), body });
  }
  if (operation === 'reject_payment') {
    return handleRejectPayment({ res, adminName, session, db, booking, bookingRef, bookingId: bookingId.trim(), body });
  }
  if (operation === 'reschedule_park') {
    return handleReschedulePark({ res, adminName, session, db, booking, bookingRef, bookingId: bookingId.trim() });
  }
  if (operation === 'reschedule_assign') {
    return handleRescheduleAssign({ res, adminName, session, db, booking, bookingRef, bookingId: bookingId.trim(), body });
  }
  if (operation === 'reschedule_cancel') {
    return handleRescheduleCancel({ res, adminName, session, db, booking, bookingRef, bookingId: bookingId.trim() });
  }
  if (operation === 'assign_coach') {
    return handleAssignCoach({ res, adminName, session, db, booking, bookingRef, bookingId: bookingId.trim(), body });
  }
  if (operation === 'coach_lesson_update') {
    return handleCoachLessonUpdate({ res, adminName, session, db, booking, bookingRef, bookingId: bookingId.trim(), body });
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
  // Pricing v2: when the client omits a value, fall back to what the booking
  // was actually priced at (mornings 330/320, late night 450) before the flat
  // 350 legacy default.
  const storedValue   = [booking.basePrice, booking.originalPrice, booking.price]
                          .map(Number).find(n => Number.isFinite(n) && n > 0) || null;
  const price         = (rawPrice != null) ? Math.max(0, Number(rawPrice)) : (storedValue ?? 350);
  const influencerAmt = (rawInfluencerAmt != null) ? Math.max(1, Number(rawInfluencerAmt))
                                                   : Math.max(1, Math.ceil(storedValue ?? dur * 350));

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

    // Coach lesson (Stage 3): a refund that releases the room also releases
    // the coach hour — ownership-checked, non-fatal on read error.
    if (booking.serviceType === 'coach_lesson' && booking.coachId && booking.date && booking.startTime) {
      const caRef = db.collection('coach_availability')
        .doc(`${booking.coachId}_${booking.date}_${String(booking.startTime).replace(':', '')}`);
      try {
        const caSnap = await caRef.get();
        if (caSnap.exists) {
          const ca = caSnap.data();
          if (ca.status === 'booked' && ca.bookingId === bookingId) {
            batch.set(caRef, {
              coachId: ca.coachId, branchId: ca.branchId || null,
              date: ca.date, hour: ca.hour, status: 'open',
              updatedAt: FieldValue.serverTimestamp(),
            });
          }
        }
      } catch (e) {
        console.error('[refund] coach availability read (non-fatal):', e.message);
      }
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

  // Branch access (filter in memory — never where(branchId); legacy → ladprao1).
  if (!hasBranchAccess(session, resolveBranchId(booking))) {
    return res.status(403).json({ ok: false, error: 'No access to this branch' });
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

  const slotId  = `${booking.resourceId || RESOURCE_ID}_${booking.date}_${booking.startTime?.replace(':', '')}`;
  const slotRef = db.collection('booking_slots').doc(slotId);

  // Atomic read-check-write: a transaction (not batch) closes the TOCTOU gap
  // between reading slot ownership and committing. All reads precede all writes.
  try {
    await db.runTransaction(async (t) => {
      const slotSnap = await t.get(slotRef);
      if (!slotSnap.exists) throw new Error('SLOT_MISSING');
      const slotData = slotSnap.data();
      const ownsSlot = slotData.bookingId === bookingId || slotData.bookingCode === booking.bookingCode;
      if (!ownsSlot) throw new Error('SLOT_CONFLICT');

      const bSnap = await t.get(bookingRef);
      if (!bSnap.exists) throw new Error('BOOKING_MISSING');
      const bNow = bSnap.data();
      if (bNow.paymentStatus === 'paid') throw new Error('ALREADY_PAID');
      if (bNow.paymentStatus !== 'unpaid') throw new Error('BAD_STATE');
      if (bNow.bookingStatus === 'cancelled') throw new Error('CANCELLED');

      t.update(bookingRef, {
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
      t.update(slotRef, { paymentStatus: 'paid', bookingStatus: 'confirmed' });
    });
  } catch (e) {
    const map = {
      SLOT_MISSING:    [409, 'Slot does not exist'],
      SLOT_CONFLICT:   [409, 'Conflict: Slot is owned by another booking'],
      BOOKING_MISSING: [404, 'Booking not found'],
      ALREADY_PAID:    [409, 'Booking is already paid'],
      BAD_STATE:       [409, 'Booking is no longer unpaid'],
      CANCELLED:       [409, 'Booking was cancelled'],
    };
    const [code, msg] = map[e.message] || [500, 'Failed to update booking'];
    if (code === 500) console.error('[mark-paid] write:', e.message);
    return res.status(code).json({ ok: false, error: msg });
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
// handleApproveSlip — any valid admin, approves a slip / confirms payment.
// Source paymentStatus: "pending_review" (normal slip approve) or "unpaid"
// (Confirm Paid No-Slip). Price is NOT changed — keeps booking.price.
// Calendar + customer notification stay client-side (fire-and-forget after ok).
// Legal transition: paymentStatus {pending_review,unpaid}→paid, bookingStatus→confirmed.
// ════════════════════════════════════════════════════════════════════
async function handleApproveSlip({ res, adminName, session, db, booking, bookingRef, bookingId, body }) {
  const withoutSlip = body.withoutSlip === true;

  if (!hasBranchAccess(session, resolveBranchId(booking))) {
    return res.status(403).json({ ok: false, error: 'No access to this branch' });
  }
  // Pre-transaction guards (Admin SDK bypasses rules → validate transition here).
  if (booking.bookingStatus === 'cancelled') {
    return res.status(409).json({ ok: false, error: 'Cannot approve a cancelled booking' });
  }
  if (booking.paymentStatus === 'paid') {
    return res.status(409).json({ ok: false, error: 'Booking is already paid' });
  }
  if (!['pending_review', 'unpaid'].includes(booking.paymentStatus)) {
    return res.status(409).json({ ok: false, error: `Cannot approve: current paymentStatus is "${booking.paymentStatus}"` });
  }

  const slotId  = `${booking.resourceId || RESOURCE_ID}_${booking.date}_${booking.startTime?.replace(':', '')}`;
  const slotRef = db.collection('booking_slots').doc(slotId);

  try {
    await db.runTransaction(async (t) => {
      const slotSnap = await t.get(slotRef);
      if (!slotSnap.exists) throw new Error('SLOT_MISSING');
      const slotData = slotSnap.data();
      const ownsSlot = slotData.bookingId === bookingId || slotData.bookingCode === booking.bookingCode;
      if (!ownsSlot) throw new Error('SLOT_CONFLICT');

      const bSnap = await t.get(bookingRef);
      if (!bSnap.exists) throw new Error('BOOKING_MISSING');
      const bNow = bSnap.data();
      if (bNow.paymentStatus === 'paid') throw new Error('ALREADY_PAID');
      if (bNow.bookingStatus === 'cancelled') throw new Error('CANCELLED');
      if (!['pending_review', 'unpaid'].includes(bNow.paymentStatus)) throw new Error('BAD_STATE');

      const update = {
        paymentStatus:   'paid',
        bookingStatus:   'confirmed',
        status:          'confirmed',
        paidBy:          adminName,
        paidAt:          FieldValue.serverTimestamp(),
        confirmedAt:     FieldValue.serverTimestamp(),
        adminReviewedAt: FieldValue.serverTimestamp(),
        updatedAt:       FieldValue.serverTimestamp(),
      };
      if (withoutSlip) {
        update.confirmedByAdmin     = true;
        update.confirmedWithoutSlip = true;
        update.paymentNote          = 'Admin confirmed payment without slip';
      }
      t.update(bookingRef, update);
      t.update(slotRef, {
        paymentStatus: 'paid',
        bookingStatus: 'confirmed',
        bookingId,
        bookingCode:   booking.bookingCode,
      });
    });
  } catch (e) {
    const map = {
      SLOT_MISSING:    [409, 'Slot does not exist'],
      SLOT_CONFLICT:   [409, 'Conflict: Slot is owned by another booking'],
      BOOKING_MISSING: [404, 'Booking not found'],
      ALREADY_PAID:    [409, 'Booking is already paid'],
      CANCELLED:       [409, 'Booking was cancelled'],
      BAD_STATE:       [409, 'Booking is no longer awaiting payment'],
    };
    const [code, msg] = map[e.message] || [500, 'Failed to approve booking'];
    if (code === 500) console.error('[approve_slip] write:', e.message);
    return res.status(code).json({ ok: false, error: msg });
  }

  console.log(`[approve_slip] id:${bookingId} from:${booking.paymentStatus} withoutSlip:${withoutSlip} admin:${adminName}`);
  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role,
    branchId: resolveBranchId(booking),
    action: 'approve_slip', targetId: bookingId,
    before: { paymentStatus: booking.paymentStatus, bookingStatus: booking.bookingStatus },
    after:  { paymentStatus: 'paid', bookingStatus: 'confirmed', withoutSlip },
  });
  return res.status(200).json({
    ok: true,
    booking: {
      id:                    bookingId,
      bookingCode:           booking.bookingCode,
      lineUserId:            booking.lineUserId ?? null,
      date:                  booking.date,
      startTime:             booking.startTime,
      endTime:               booking.endTime,
      googleCalendarEventId: booking.googleCalendarEventId ?? null,
    },
  });
}

// ════════════════════════════════════════════════════════════════════
// handleRejectPayment — any valid admin, rejects a slip / cancels an unpaid
// booking and releases its slot. Paid bookings are blocked (use refund).
// Legal transition: paymentStatus {pending_review,unpaid}→rejected, bookingStatus→cancelled.
// ════════════════════════════════════════════════════════════════════
async function handleRejectPayment({ res, adminName, session, db, booking, bookingRef, bookingId, body }) {
  if (!hasBranchAccess(session, resolveBranchId(booking))) {
    return res.status(403).json({ ok: false, error: 'No access to this branch' });
  }
  if (booking.bookingStatus === 'cancelled') {
    return res.status(409).json({ ok: false, error: 'Booking is already cancelled' });
  }
  if (!['pending_review', 'unpaid'].includes(booking.paymentStatus)) {
    return res.status(409).json({ ok: false, error: `Cannot reject: paymentStatus is "${booking.paymentStatus}" (use refund for paid bookings)` });
  }

  const reason = (typeof body.reason === 'string' && body.reason.trim())
    ? body.reason.trim().slice(0, 400)
    : 'Slip rejected or payment not received';

  const slotId  = `${booking.resourceId || RESOURCE_ID}_${booking.date}_${booking.startTime?.replace(':', '')}`;
  const slotRef = db.collection('booking_slots').doc(slotId);
  // Coach lesson (Stage 3): admin reject must release the coach hour too.
  const coachAvailRef = (booking.serviceType === 'coach_lesson' && booking.coachId && booking.date && booking.startTime)
    ? db.collection('coach_availability').doc(`${booking.coachId}_${booking.date}_${String(booking.startTime).replace(':', '')}`)
    : null;

  try {
    await db.runTransaction(async (t) => {
      const bSnap = await t.get(bookingRef);
      if (!bSnap.exists) throw new Error('BOOKING_MISSING');
      const bNow = bSnap.data();
      if (bNow.bookingStatus === 'cancelled') throw new Error('ALREADY_CANCELLED');
      if (bNow.paymentStatus === 'paid') throw new Error('ALREADY_PAID');

      const slotSnap = await t.get(slotRef);
      const caSnap = coachAvailRef ? await t.get(coachAvailRef) : null;

      t.update(bookingRef, {
        bookingStatus: 'cancelled',
        status:        'cancelled',
        paymentStatus: 'rejected',
        cancelReason:  reason,
        cancelledBy:   adminName,
        cancelledAt:   FieldValue.serverTimestamp(),
        updatedAt:     FieldValue.serverTimestamp(),
      });
      if (slotSnap.exists) {
        const slotData = slotSnap.data();
        const ownsSlot = slotData.bookingId === bookingId || slotData.bookingCode === booking.bookingCode;
        if (ownsSlot) {
          t.update(slotRef, { bookingStatus: 'cancelled', paymentStatus: 'rejected' });
        }
      }
      // Reopen the coach hour only when this booking still owns the lock.
      if (caSnap && caSnap.exists) {
        const ca = caSnap.data();
        if (ca.status === 'booked' && ca.bookingId === bookingId) {
          t.set(coachAvailRef, {
            coachId: ca.coachId, branchId: ca.branchId || null,
            date: ca.date, hour: ca.hour, status: 'open',
            updatedAt: FieldValue.serverTimestamp(),
          });
        }
      }
    });
  } catch (e) {
    const map = {
      BOOKING_MISSING:   [404, 'Booking not found'],
      ALREADY_CANCELLED: [409, 'Booking is already cancelled'],
      ALREADY_PAID:      [409, 'Booking is already paid (use refund)'],
    };
    const [code, msg] = map[e.message] || [500, 'Failed to reject booking'];
    if (code === 500) console.error('[reject_payment] write:', e.message);
    return res.status(code).json({ ok: false, error: msg });
  }

  console.log(`[reject_payment] id:${bookingId} from:${booking.paymentStatus} admin:${adminName}`);
  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role,
    branchId: resolveBranchId(booking),
    action: 'reject_payment', targetId: bookingId,
    before: { paymentStatus: booking.paymentStatus, bookingStatus: booking.bookingStatus },
    after:  { paymentStatus: 'rejected', bookingStatus: 'cancelled' },
  });
  return res.status(200).json({
    ok: true,
    booking: {
      id:           bookingId,
      bookingCode:  booking.bookingCode,
      lineUserId:   booking.lineUserId ?? null,
      date:         booking.date,
      startTime:    booking.startTime,
      endTime:      booking.endTime,
      cancelReason: reason,
    },
  });
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

// ════════════════════════════════════════════════════════════════════
// handleReschedulePark — confirmed → rescheduled + pendingReschedule flag.
// Releases the original slot; assigns no new slot yet. Calendar delete +
// admin notification stay client-side (fire-and-forget after ok).
// ════════════════════════════════════════════════════════════════════
async function handleReschedulePark({ res, adminName, session, db, booking, bookingRef, bookingId }) {
  if (!hasBranchAccess(session, resolveBranchId(booking))) {
    return res.status(403).json({ ok: false, error: 'No access to this branch' });
  }
  if (booking.bookingStatus === 'cancelled') {
    return res.status(409).json({ ok: false, error: 'Cannot reschedule a cancelled booking' });
  }
  if (isPendingRescheduleBooking(booking)) {
    return res.status(409).json({ ok: false, error: 'Booking is already pending reschedule' });
  }
  if (!booking.date || !booking.startTime) {
    return res.status(400).json({ ok: false, error: 'Booking date/time is missing' });
  }

  const fromDate  = booking.pendingRescheduleFromDate      || booking.previousDate      || booking.date;
  const fromStart = booking.pendingRescheduleFromStartTime || booking.previousStartTime || booking.startTime;
  const fromEnd   = booking.pendingRescheduleFromEndTime   || booking.previousEndTime   || booking.endTime;

  const oldSlotRef = db.collection('booking_slots').doc(reschedSlotId(booking.resourceId, booking.date, booking.startTime));

  try {
    await db.runTransaction(async (t) => {
      const slotSnap = await t.get(oldSlotRef);
      const bSnap = await t.get(bookingRef);
      if (!bSnap.exists) throw new Error('BOOKING_MISSING');
      const bNow = bSnap.data();
      if (bNow.bookingStatus === 'cancelled') throw new Error('CANCELLED');
      if (isPendingRescheduleBooking(bNow)) throw new Error('ALREADY_PENDING');

      t.update(bookingRef, {
        bookingStatus:                  'rescheduled',
        pendingReschedule:              true,
        pendingRescheduleStatus:        'pending',
        pendingRescheduleAt:            FieldValue.serverTimestamp(),
        pendingRescheduleBy:            adminName,
        pendingRescheduleFromDate:      fromDate,
        pendingRescheduleFromStartTime: fromStart,
        pendingRescheduleFromEndTime:   fromEnd,
        updatedAt:                      FieldValue.serverTimestamp(),
      });
      if (slotSnap.exists) {
        const sd = slotSnap.data();
        if (sd.bookingId === bookingId || sd.bookingCode === booking.bookingCode) {
          t.update(oldSlotRef, {
            bookingStatus:               'rescheduled',
            paymentStatus:               booking.paymentStatus,
            pendingRescheduleReleasedAt: FieldValue.serverTimestamp(),
            updatedAt:                   FieldValue.serverTimestamp(),
          });
        }
      }
    });
  } catch (e) {
    const map = {
      BOOKING_MISSING: [404, 'Booking not found'],
      CANCELLED:       [409, 'Cannot reschedule a cancelled booking'],
      ALREADY_PENDING: [409, 'Booking is already pending reschedule'],
    };
    const [code, msg] = map[e.message] || [500, 'Failed to move to pending reschedule'];
    if (code === 500) console.error('[reschedule_park] tx:', e.message);
    return res.status(code).json({ ok: false, error: msg });
  }

  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role, branchId: resolveBranchId(booking),
    action: 'reschedule_park', targetId: bookingId,
    before: { bookingStatus: booking.bookingStatus, date: booking.date, startTime: booking.startTime },
    after:  { bookingStatus: 'rescheduled', pendingRescheduleStatus: 'pending' },
  });

  return res.status(200).json({
    ok: true,
    booking: {
      id: bookingId, bookingCode: booking.bookingCode, lineUserId: booking.lineUserId ?? null,
      customerName: booking.customerName ?? '', customerPhone: booking.customerPhone ?? '',
      paymentStatus: booking.paymentStatus, bookingType: booking.bookingType ?? null,
      googleCalendarEventId: booking.googleCalendarEventId ?? null,
      previousDate: fromDate, previousStartTime: fromStart, previousEndTime: fromEnd,
    },
  });
}

// ════════════════════════════════════════════════════════════════════
// handleRescheduleAssign — assign a new date/time (first-time reschedule of a
// confirmed booking, OR assigning a pending-reschedule booking). The slot grab
// is atomic; the new booking_slot lock is ALWAYS written as "confirmed" so the
// customer UI reliably blocks it (it only blocks confirmed / unexpired pending).
// Customer + admin notifications and Calendar sync stay client-side.
// ════════════════════════════════════════════════════════════════════
async function handleRescheduleAssign({ res, adminName, session, db, booking, bookingRef, bookingId, body }) {
  if (!hasBranchAccess(session, resolveBranchId(booking))) {
    return res.status(403).json({ ok: false, error: 'No access to this branch' });
  }
  if (booking.bookingStatus === 'cancelled') {
    return res.status(409).json({ ok: false, error: 'Cannot reschedule a cancelled booking' });
  }
  const { newDate, newStartTime } = body;
  if (typeof newDate !== 'string' || !RESCHED_DATE_RE.test(newDate)) {
    return res.status(400).json({ ok: false, error: 'newDate must be YYYY-MM-DD' });
  }
  if (typeof newStartTime !== 'string' || !RESCHED_TIME_RE.test(newStartTime)) {
    return res.status(400).json({ ok: false, error: 'newStartTime must be HH:mm' });
  }

  const newEndTime = nextHourEnd(newStartTime);
  const resourceId = booking.resourceId || RESOURCE_ID;
  const oldSlotId  = reschedSlotId(resourceId, booking.date, booking.startTime);
  const newSlotId  = reschedSlotId(resourceId, newDate, newStartTime);
  const avRef      = db.collection('available_slots').doc(newSlotId);
  const newSlotRef = db.collection('booking_slots').doc(newSlotId);
  const oldSlotRef = db.collection('booking_slots').doc(oldSlotId);

  let meta;
  try {
    meta = await db.runTransaction(async (t) => {
      const [avSnap, newSnap, oldSnap, bSnap] = await Promise.all([
        t.get(avRef), t.get(newSlotRef), t.get(oldSlotRef), t.get(bookingRef),
      ]);
      if (!bSnap.exists) throw new Error('BOOKING_MISSING');
      const bNow = bSnap.data();
      if (bNow.bookingStatus === 'cancelled') throw new Error('CANCELLED');
      if (!avSnap.exists || avSnap.data().status !== 'open') throw new Error('SLOT_NOT_OPEN');
      if (newSnap.exists && oldSlotId !== newSlotId) {
        const nd = newSnap.data();
        const ownsNew = nd.bookingId === bookingId || nd.bookingCode === booking.bookingCode;
        if (!ownsNew && isLiveBookedSlot(nd)) throw new Error('SLOT_TAKEN');
      }

      const wasPending = isPendingRescheduleBooking(bNow);
      const nextStatus = wasPending ? 'confirmed' : bNow.bookingStatus;
      // QC: the slot lock must ALWAYS be "confirmed" regardless of the booking's
      // own flow status, or the customer UI would treat the slot as free.
      const slotNextStatus = 'confirmed';

      const update = {
        date: newDate, startTime: newStartTime, endTime: newEndTime,
        bookingStatus: nextStatus,
        previousDate:      booking.pendingRescheduleFromDate      || booking.previousDate      || booking.date,
        previousStartTime: booking.pendingRescheduleFromStartTime || booking.previousStartTime || booking.startTime,
        previousEndTime:   booking.pendingRescheduleFromEndTime   || booking.previousEndTime   || booking.endTime,
        rescheduledAt: FieldValue.serverTimestamp(),
        updatedAt:     FieldValue.serverTimestamp(),
      };
      if (wasPending) {
        update.pendingReschedule = false;
        update.pendingRescheduleStatus = 'assigned';
        update.assignedFromPendingRescheduleAt = FieldValue.serverTimestamp();
      }
      t.update(bookingRef, update);

      if (oldSlotId !== newSlotId && oldSnap.exists) {
        const sd = oldSnap.data();
        if (sd.bookingId === bookingId || sd.bookingCode === booking.bookingCode) {
          t.update(oldSlotRef, { bookingStatus: 'rescheduled', paymentStatus: booking.paymentStatus });
        }
      }

      t.set(newSlotRef, {
        bookingCode:   booking.bookingCode,
        bookingId,
        resourceId,
        date:          newDate,
        hour:          newStartTime,
        bookingStatus: slotNextStatus,
        paymentStatus: booking.paymentStatus,
        expiresAt:     null,
      });

      return { wasPending, nextStatus };
    });
  } catch (e) {
    const map = {
      BOOKING_MISSING: [404, 'Booking not found'],
      CANCELLED:       [409, 'Cannot reschedule a cancelled booking'],
      SLOT_NOT_OPEN:   [409, 'Selected slot is not open'],
      SLOT_TAKEN:      [409, 'Selected slot is already booked'],
    };
    const [code, msg] = map[e.message] || [500, 'Failed to reschedule'];
    if (code === 500) console.error('[reschedule_assign] tx:', e.message);
    return res.status(code).json({ ok: false, error: msg });
  }

  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role, branchId: resolveBranchId(booking),
    action: 'reschedule_assign', targetId: bookingId,
    before: { bookingStatus: booking.bookingStatus, date: booking.date, startTime: booking.startTime },
    after:  { bookingStatus: meta.nextStatus, date: newDate, startTime: newStartTime, wasPending: meta.wasPending },
  });

  return res.status(200).json({
    ok: true,
    wasPending: meta.wasPending, nextStatus: meta.nextStatus,
    newDate, newStartTime, newEndTime,
    booking: {
      id: bookingId, bookingCode: booking.bookingCode, lineUserId: booking.lineUserId ?? null,
      paymentStatus: booking.paymentStatus, googleCalendarEventId: booking.googleCalendarEventId ?? null,
      previousDate: booking.date, previousStartTime: booking.startTime, previousEndTime: booking.endTime,
    },
  });
}

// ════════════════════════════════════════════════════════════════════
// handleRescheduleCancel — best-effort restore a pending-reschedule booking to
// its original slot. Writes an audit entry for BOTH outcomes (restored or not)
// so the restore attempt is always traceable.
// ════════════════════════════════════════════════════════════════════
async function handleRescheduleCancel({ res, adminName, session, db, booking, bookingRef, bookingId }) {
  if (!hasBranchAccess(session, resolveBranchId(booking))) {
    return res.status(403).json({ ok: false, error: 'No access to this branch' });
  }
  if (!isPendingRescheduleBooking(booking)) {
    return res.status(409).json({ ok: false, error: 'Booking is not pending reschedule' });
  }

  const origDate  = booking.pendingRescheduleFromDate      || booking.previousDate      || booking.date;
  const origStart = booking.pendingRescheduleFromStartTime || booking.previousStartTime || booking.startTime;
  const origEnd   = booking.pendingRescheduleFromEndTime   || booking.previousEndTime   || booking.endTime;
  const resourceId = booking.resourceId || RESOURCE_ID;
  const slotId  = reschedSlotId(resourceId, origDate, origStart);
  const slotRef = db.collection('booking_slots').doc(slotId);
  const avRef   = db.collection('available_slots').doc(slotId);

  let restored;
  try {
    restored = await db.runTransaction(async (t) => {
      const [avSnap, slotSnap, bSnap] = await Promise.all([
        t.get(avRef), t.get(slotRef), t.get(bookingRef),
      ]);
      if (!bSnap.exists) throw new Error('BOOKING_MISSING');
      if (!isPendingRescheduleBooking(bSnap.data())) throw new Error('NOT_PENDING');

      const slotOpen = avSnap.exists && avSnap.data().status === 'open';
      const sd = slotSnap.exists ? slotSnap.data() : null;
      const notOccupiedByOther = !sd || !isLiveBookedSlot(sd) ||
        sd.bookingId === bookingId || sd.bookingCode === booking.bookingCode;

      if (origDate && origStart && origEnd && slotOpen && notOccupiedByOther) {
        t.set(slotRef, {
          bookingCode: booking.bookingCode, bookingId, resourceId,
          date: origDate, hour: origStart,
          bookingStatus: 'confirmed', paymentStatus: booking.paymentStatus,
          expiresAt: null, updatedAt: FieldValue.serverTimestamp(),
        });
        t.update(bookingRef, {
          date: origDate, startTime: origStart, endTime: origEnd,
          bookingStatus: 'confirmed', pendingReschedule: false,
          pendingRescheduleStatus: 'cancelled_restored',
          cancelledRestoredAt: FieldValue.serverTimestamp(),
          updatedAt: FieldValue.serverTimestamp(),
        });
        return true;
      }
      return false;
    });
  } catch (e) {
    const map = {
      BOOKING_MISSING: [404, 'Booking not found'],
      NOT_PENDING:     [409, 'Booking is not pending reschedule'],
    };
    const [code, msg] = map[e.message] || [500, 'Failed to cancel pending reschedule'];
    if (code === 500) console.error('[reschedule_cancel] tx:', e.message);
    return res.status(code).json({ ok: false, error: msg });
  }

  // QC: audit BOTH outcomes so a failed restore attempt is still traceable.
  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role, branchId: resolveBranchId(booking),
    action: 'reschedule_cancel', targetId: bookingId,
    before: { bookingStatus: booking.bookingStatus, pendingRescheduleStatus: booking.pendingRescheduleStatus || 'pending' },
    after:  restored
      ? { restored: true, bookingStatus: 'confirmed', pendingRescheduleStatus: 'cancelled_restored', date: origDate, startTime: origStart }
      : { restored: false, reason: 'original_slot_unavailable' },
  });

  return res.status(200).json({
    ok: true, restored,
    booking: {
      id: bookingId, bookingCode: booking.bookingCode, lineUserId: booking.lineUserId ?? null,
      paymentStatus: booking.paymentStatus, googleCalendarEventId: booking.googleCalendarEventId ?? null,
      date: origDate, startTime: origStart, endTime: origEnd,
    },
  });
}

// ════════════════════════════════════════════════════════════════════
// handleAssignCoach — admin assigns (or clears) a coach on a booking.
// Sets coachName/coachId + lessonStatus:"assigned". Does NOT touch
// bookingStatus/paymentStatus. Coach actions on the lesson come later via
// coach.html → coach_lesson_update (Coach Phase 2).
// ════════════════════════════════════════════════════════════════════
async function handleAssignCoach({ res, adminName, session, db, booking, bookingRef, bookingId, body }) {
  // Admin only — coaches cannot assign themselves.
  if (!requireRole(session, 'owner', 'ultra_admin', 'branch_manager')) {
    return res.status(403).json({ ok: false, error: 'Requires branch_manager or above' });
  }
  if (!hasBranchAccess(session, resolveBranchId(booking))) {
    return res.status(403).json({ ok: false, error: 'No access to this branch' });
  }
  if (booking.bookingStatus === 'cancelled') {
    return res.status(409).json({ ok: false, error: 'Cannot assign a coach to a cancelled booking' });
  }

  // Input carries the STABLE coach id (= coaches doc id = login name).
  // (Accept legacy `coachName` too for back-compat.)
  const rawId = typeof body.coachId === 'string' ? body.coachId.trim()
              : typeof body.coachName === 'string' ? body.coachName.trim() : '';

  // ── Unassign (empty coachId) ───────────────────────────────────────
  // Clear all coach fields. lessonStatus is cleared (never left "assigned").
  if (!rawId) {
    try {
      await bookingRef.update({
        coachId:         null,
        coachName:       null,
        lessonStatus:    null,
        coachAssignedAt: null,
        coachAssignedBy: null,
        lessonUpdatedAt: FieldValue.serverTimestamp(),
        updatedAt:       FieldValue.serverTimestamp(),
      });
    } catch (e) {
      console.error('[assign_coach] unassign:', e.message);
      return res.status(500).json({ ok: false, error: 'Failed to unassign coach' });
    }
    await writeAuditLog(db, {
      actor: adminName, actorRole: session.role, branchId: resolveBranchId(booking),
      action: 'assign_coach', targetId: bookingId,
      before: { coachId: booking.coachId ?? null }, after: { coachId: null },
    });
    return res.status(200).json({ ok: true, coachId: null });
  }

  // ── Assign — coach must exist and be active ────────────────────────
  let coach;
  try {
    const snap = await db.collection('coaches').doc(rawId).get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: `Coach "${rawId}" not found` });
    coach = snap.data();
  } catch (e) {
    console.error('[assign_coach] read coach:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to read coach' });
  }
  if (coach.active === false) {
    return res.status(409).json({ ok: false, error: 'Coach is inactive' });
  }

  // coachId = stable id (doc id = login name); coachName = display name.
  const coachDisplay = (typeof coach.displayName === 'string' && coach.displayName.trim())
    ? coach.displayName.trim()
    : rawId;

  try {
    await bookingRef.update({
      coachId:         rawId,
      coachName:       coachDisplay,
      lessonStatus:    'assigned',
      coachAssignedAt: FieldValue.serverTimestamp(),
      coachAssignedBy: adminName,
      lessonUpdatedAt: FieldValue.serverTimestamp(),
      updatedAt:       FieldValue.serverTimestamp(),
    });
  } catch (e) {
    console.error('[assign_coach] write:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to assign coach' });
  }

  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role, branchId: resolveBranchId(booking),
    action: 'assign_coach', targetId: bookingId,
    before: { coachId: booking.coachId ?? null },
    after:  { coachId: rawId, coachName: coachDisplay, lessonStatus: 'assigned' },
  });
  return res.status(200).json({ ok: true, coachId: rawId, coachName: coachDisplay });
}

// ════════════════════════════════════════════════════════════════════
// handleCoachLessonUpdate — coach (owns the booking) or admin updates the
// lesson lifecycle: check_in / complete / no_show / note. Tracks a SEPARATE
// `lessonStatus` field — NEVER touches bookingStatus/paymentStatus.
// ════════════════════════════════════════════════════════════════════
async function handleCoachLessonUpdate({ res, session, db, booking, bookingRef, bookingId, body }) {
  const isCoach = session.role === 'coach';
  const isAdmin = requireRole(session, 'owner', 'ultra_admin', 'branch_manager', 'branch_staff');
  if (!isCoach && !isAdmin) {
    return res.status(403).json({ ok: false, error: 'Coach or admin only' });
  }
  // A coach may only act on bookings assigned to them.
  if (isCoach && booking.coachId !== session.name) {
    return res.status(403).json({ ok: false, error: 'This booking is not assigned to you' });
  }
  // Coach 2A: enforce session revocation (sv). A PIN reset invalidates old cookies.
  if (isCoach) {
    try {
      const a = await db.collection('coach_auth').doc(session.name).get();
      const storedSv = a.exists ? (Number(a.data().sessionVersion) || 1) : 1;
      if ((session.sv ?? 1) !== storedSv) {
        return res.status(401).json({ ok: false, error: 'Session revoked — please log in again' });
      }
    } catch (e) { console.warn('[coach_lesson_update] sv check:', e.message); }
  }
  if (!hasBranchAccess(session, resolveBranchId(booking))) {
    return res.status(403).json({ ok: false, error: 'No access to this branch' });
  }
  if (!booking.coachId) {
    return res.status(409).json({ ok: false, error: 'No coach assigned to this booking' });
  }
  if (booking.bookingStatus === 'cancelled') {
    return res.status(409).json({ ok: false, error: 'Booking is cancelled' });
  }

  const { lessonAction } = body;
  const VALID = ['check_in', 'complete', 'no_show', 'note'];
  if (!VALID.includes(lessonAction)) {
    return res.status(400).json({ ok: false, error: `Invalid lessonAction. Must be one of: ${VALID.join(', ')}` });
  }
  const hasNote = typeof body.lessonNote === 'string';
  const note    = hasNote ? body.lessonNote.slice(0, 500) : undefined;
  if (lessonAction === 'note' && !hasNote) {
    return res.status(400).json({ ok: false, error: 'lessonNote is required for the note action' });
  }

  const update = {
    lessonUpdatedAt: FieldValue.serverTimestamp(),
    lessonUpdatedBy: session.name,
    updatedAt:       FieldValue.serverTimestamp(),
  };
  if (note !== undefined) update.lessonNote = note;

  // Status actions set lessonStatus + a timestamp — NEVER bookingStatus/paymentStatus.
  if (lessonAction !== 'note') {
    const statusMap = { check_in: 'checked_in', complete: 'completed', no_show: 'no_show' };
    const tsMap     = { check_in: 'coachCheckedInAt', complete: 'coachCompletedAt', no_show: 'coachNoShowAt' };
    update.lessonStatus     = statusMap[lessonAction];
    update[tsMap[lessonAction]] = FieldValue.serverTimestamp();
  }

  try {
    await bookingRef.update(update);
  } catch (e) {
    console.error('[coach_lesson_update] write:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to update lesson' });
  }

  await writeAuditLog(db, {
    actor: session.name, actorRole: session.role, branchId: resolveBranchId(booking),
    action: 'coach_lesson_update', targetId: bookingId,
    before: { lessonStatus: booking.lessonStatus ?? null },
    after:  { lessonAction, lessonStatus: update.lessonStatus ?? booking.lessonStatus ?? null },
  });
  return res.status(200).json({ ok: true, lessonStatus: update.lessonStatus ?? booking.lessonStatus ?? null });
}
