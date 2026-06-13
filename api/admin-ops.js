// ════════════════════════════════════════════════════════════════════
// POST /api/admin-ops — Partner Studio operations (Phase 2A)
// ════════════════════════════════════════════════════════════════════
// Single multiplexed route (Vercel function limit) dispatched by `action`.
// INERT in Phase 2A: no UI calls it yet — deployed ahead of 2B/2C.
//
// Auth model:
//   • `rating_submit` is PUBLIC (customer flow, no admin session) but
//     heavily validated server-side — see handleRatingSubmit.
//   • Every other action requires a valid admin session (verifySession).
//   • Role + branch scope are enforced per action (see dispatch table).
//
// Branch scoping rule (iron rule): NEVER filter by branchId in a
// Firestore where() — legacy docs lack the field and would vanish.
// All branch filtering happens in memory via resolveBranchId().
//
// Actions:
//   branch_get | branch_save | branch_set_status
//   incident_create | incident_list | incident_update_status
//   rating_submit (public) | rating_list
//   dashboard
//   audit_log_write | audit_log_list
// ════════════════════════════════════════════════════════════════════

import {
  verifySession, requireRole, hasBranchAccess, resolveBranchId, DEFAULT_BRANCH_ID,
} from './_lib/admin-auth.js';
import {
  getAdminDb, serializeFsDoc, writeAuditLog,
  BRANCH_STATUSES, statusFlags,
} from './_lib/firebase-admin.js';
import { FieldValue } from 'firebase-admin/firestore';

// ── Constants ─────────────────────────────────────────────────────
const VALID_ACTIONS = [
  'branch_get', 'branch_save', 'branch_set_status',
  'incident_create', 'incident_list', 'incident_update_status',
  'rating_submit', 'rating_list',
  'dashboard',
  'audit_log_write', 'audit_log_list',
];

const INCIDENT_SEVERITIES = ['low', 'medium', 'high', 'critical'];
const INCIDENT_STATUSES   = ['open', 'reviewing', 'resolved'];

// Whitelist for client-reported audit entries (admin.html fires these
// after its client-direct Firestore writes — wired up in Phase 2C).
const CLIENT_AUDIT_ACTIONS = [
  'mark_paid_client', 'cancel_booking_client', 'slip_reject_client',
  'manual_booking_client', 'reschedule_client',
  'pending_reschedule_client', 'reschedule_cancel_client',
  'slot_open_client', 'slot_close_client', 'holiday_set_client',
  'pass_adjust_client', 'pass_deactivate_client',
];

const BRANCH_ID_RE = /^[a-z0-9_-]{2,30}$/;

// ── Shared helpers ────────────────────────────────────────────────
function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

function clampStr(v, max) {
  return String(v ?? '').slice(0, max);
}

function sanitizeAuditSnapshot(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const out = {};
  for (const [key, raw] of Object.entries(value).slice(0, 12)) {
    const safeKey = clampStr(key, 50);
    if (raw === null || typeof raw === 'boolean' || typeof raw === 'number') {
      out[safeKey] = raw;
    } else if (typeof raw === 'string') {
      out[safeKey] = clampStr(raw, 160);
    }
  }
  return Object.keys(out).length ? out : null;
}

// HQ roles see every branch; everyone else is partner-tier.
function isHQ(session) {
  return requireRole(session, 'owner', 'ultra_admin');
}

// In-memory branch filter: HQ ('*' or owner/ultra_admin) sees all,
// partner-tier sees only their own branches.
function filterToSessionBranches(session, docs) {
  if (session.branches === '*') return docs;
  return docs.filter(d => hasBranchAccess(session, resolveBranchId(d)));
}

// Add N days to a YYYY-MM-DD string without local-timezone surprises.
// (Same logic as addDaysToISODate in api/gcal.js.)
function addDaysISO(dateISO, days) {
  const [y, m, d] = dateISO.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d + days));
  const pad = n => String(n).padStart(2, '0');
  return `${dt.getUTCFullYear()}-${pad(dt.getUTCMonth() + 1)}-${pad(dt.getUTCDate())}`;
}

// True if the booking's slot has finished, evaluated in Bangkok (+07:00).
// Midnight-crossing slots (endTime <= startTime, e.g. 23:00–00:00) end on
// the next calendar day — same rule as buildBangkokEventTimes in gcal.js.
// Unparseable data → false (NOT ended) so the rating is rejected, never allowed early.
function bookingEndedBangkok(booking) {
  const { date, startTime, endTime } = booking;
  if (!date || !endTime || !/^\d{4}-\d{2}-\d{2}$/.test(date) || !/^\d{2}:\d{2}$/.test(endTime)) return false;
  const endDate = (startTime && endTime <= startTime) ? addDaysISO(date, 1) : date;
  const end = new Date(`${endDate}T${endTime}:00+07:00`);
  if (isNaN(end.getTime())) return false;
  return end.getTime() <= Date.now();
}

// Verify a LIFF access token against LINE's profile API.
// Returns true only when the token is valid AND belongs to expectedUserId.
async function verifyLineToken(token, expectedUserId) {
  try {
    const res = await fetch('https://api.line.me/v2/profile', {
      headers: { 'Authorization': `Bearer ${token}` },
    });
    if (!res.ok) return false;
    const profile = await res.json();
    return profile?.userId === expectedUserId;
  } catch (e) {
    console.error('[admin-ops] LINE token verify threw:', e.message);
    return false;
  }
}

function getDbOr500(res) {
  try { return getAdminDb(); }
  catch (e) {
    console.error('[admin-ops] DB init:', e.message);
    res.status(500).json({ ok: false, error: 'Database not available' });
    return null;
  }
}

// ── Main handler ──────────────────────────────────────────────────
export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  const body = parseBody(req);
  if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

  const { action } = body;
  if (!VALID_ACTIONS.includes(action)) {
    return res.status(400).json({ ok: false, error: `Unknown action "${action}". Valid actions: ${VALID_ACTIONS.join(', ')}` });
  }

  // ── Public action (no admin session) ──────────────────────────────
  if (action === 'rating_submit') {
    return handleRatingSubmit(req, res, body);
  }

  // ── Everything else requires a valid admin session ─────────────────
  const session = verifySession(req);
  if (!session) return res.status(401).json({ ok: false, error: 'Unauthorized' });

  switch (action) {
    case 'branch_get':             return handleBranchGet(res, session, body);
    case 'branch_save':            return handleBranchSave(res, session, body);
    case 'branch_set_status':      return handleBranchSetStatus(res, session, body);
    case 'incident_create':        return handleIncidentCreate(res, session, body);
    case 'incident_list':          return handleIncidentList(res, session, body);
    case 'incident_update_status': return handleIncidentUpdateStatus(res, session, body);
    case 'rating_list':            return handleRatingList(res, session, body);
    case 'dashboard':              return handleDashboard(res, session, body);
    case 'audit_log_write':        return handleAuditLogWrite(res, session, body);
    case 'audit_log_list':         return handleAuditLogList(res, session, body);
    default:
      // Unreachable (VALID_ACTIONS gate above) — defensive.
      return res.status(400).json({ ok: false, error: `Unknown action "${action}"` });
  }
}

// ════════════════════════════════════════════════════════════════════
// Branch config
// ════════════════════════════════════════════════════════════════════

// branch_get — any role; partner-tier limited to their own branches.
async function handleBranchGet(res, session, body) {
  const branchId = typeof body.branchId === 'string' && body.branchId ? body.branchId : DEFAULT_BRANCH_ID;
  if (!hasBranchAccess(session, branchId)) {
    return res.status(403).json({ ok: false, error: 'No access to this branch' });
  }
  const db = getDbOr500(res); if (!db) return;
  try {
    const snap = await db.collection('branches').doc(branchId).get();
    return res.status(200).json({ ok: true, branch: snap.exists ? serializeFsDoc(snap.data()) : null });
  } catch (e) {
    console.error('[branch_get]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to read branch' });
  }
}

// branch_save — owner only. General config fields; status is NOT editable
// here (branch_set_status is the only path, so every status change is audited).
async function handleBranchSave(res, session, body) {
  if (!requireRole(session, 'owner')) {
    return res.status(403).json({ ok: false, error: 'Owner only' });
  }
  const { branchId } = body;
  if (typeof branchId !== 'string' || !BRANCH_ID_RE.test(branchId)) {
    return res.status(400).json({ ok: false, error: 'branchId must be 2-30 chars of a-z 0-9 _ -' });
  }

  const update = { updatedAt: FieldValue.serverTimestamp() };
  if (body.displayName !== undefined) update.displayName = clampStr(body.displayName, 100);
  if (body.address     !== undefined) update.address     = clampStr(body.address, 300);
  if (body.contact     !== undefined) update.contact     = clampStr(body.contact, 300);
  if (body.operatingHours !== undefined) {
    const oh = body.operatingHours;
    if (!oh || typeof oh !== 'object' || !Number.isInteger(oh.open) || !Number.isInteger(oh.close)) {
      return res.status(400).json({ ok: false, error: 'operatingHours must be {open: int, close: int}' });
    }
    update.operatingHours = { open: oh.open, close: oh.close };
  }
  if (body.resources !== undefined) {
    if (!Array.isArray(body.resources) || body.resources.some(r => !r || typeof r.resourceId !== 'string' || !r.resourceId)) {
      return res.status(400).json({ ok: false, error: 'resources must be an array of {resourceId, displayName?, active?}' });
    }
    update.resources = body.resources.map(r => ({
      resourceId:  clampStr(r.resourceId, 50),
      displayName: clampStr(r.displayName ?? r.resourceId, 100),
      active:      r.active !== false,
    }));
  }

  const db = getDbOr500(res); if (!db) return;
  try {
    const ref  = db.collection('branches').doc(branchId);
    const snap = await ref.get();
    if (!snap.exists) {
      // New branch: seed safe defaults — created ACTIVE with derived flags.
      Object.assign(update, {
        branchId,
        name: branchId,
        displayName: update.displayName ?? branchId,
        status: 'active',
        ...statusFlags('active'),
        isDefault: false,
        createdAt: FieldValue.serverTimestamp(),
      });
    }
    await ref.set(update, { merge: true });
    await writeAuditLog(db, {
      actor: session.name, actorRole: session.role, branchId,
      action: 'branch_save', targetId: `branches/${branchId}`,
      after: { fields: Object.keys(update).filter(k => !['updatedAt', 'createdAt'].includes(k)) },
    });
    return res.status(200).json({ ok: true, branchId, created: !snap.exists });
  } catch (e) {
    console.error('[branch_save]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to save branch' });
  }
}

// branch_set_status — owner only, reason required, always audited.
async function handleBranchSetStatus(res, session, body) {
  if (!requireRole(session, 'owner')) {
    return res.status(403).json({ ok: false, error: 'Owner only' });
  }
  const { branchId, status } = body;
  const reason = typeof body.reason === 'string' ? body.reason.trim() : '';
  if (typeof branchId !== 'string' || !branchId) {
    return res.status(400).json({ ok: false, error: 'Missing branchId' });
  }
  if (!BRANCH_STATUSES.includes(status)) {
    return res.status(400).json({ ok: false, error: `Invalid status. Must be one of: ${BRANCH_STATUSES.join(', ')}` });
  }
  if (!reason) {
    return res.status(400).json({ ok: false, error: 'reason is required for a branch status change' });
  }

  const db = getDbOr500(res); if (!db) return;
  try {
    const ref  = db.collection('branches').doc(branchId);
    const snap = await ref.get();
    if (!snap.exists) {
      return res.status(404).json({ ok: false, error: `Branch "${branchId}" not found — run scripts/seed-branches.js first` });
    }
    const before = snap.data();
    const flags  = statusFlags(status);
    await ref.set({
      status,
      ...flags,
      statusReason:    clampStr(reason, 400),
      statusChangedBy: session.name,
      statusChangedAt: FieldValue.serverTimestamp(),
      updatedAt:       FieldValue.serverTimestamp(),
    }, { merge: true });
    await writeAuditLog(db, {
      actor: session.name, actorRole: session.role, branchId,
      action: 'branch_set_status', targetId: `branches/${branchId}`,
      before: { status: before.status ?? 'active' },
      after:  { status, ...flags },
      note:   clampStr(reason, 400),
    });
    return res.status(200).json({ ok: true, branchId, status, flags });
  } catch (e) {
    console.error('[branch_set_status]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to update branch status' });
  }
}

// ════════════════════════════════════════════════════════════════════
// Incidents
// ════════════════════════════════════════════════════════════════════

// incident_create — any role except viewer; partner-tier limited to own
// branches. Intentionally allowed even when the branch is locked —
// incidents must always be reportable.
async function handleIncidentCreate(res, session, body) {
  if (session.role === 'viewer') {
    return res.status(403).json({ ok: false, error: 'Viewers cannot create incidents' });
  }
  const branchId    = typeof body.branchId === 'string' && body.branchId ? body.branchId : DEFAULT_BRANCH_ID;
  const incidentType = typeof body.incidentType === 'string' ? body.incidentType.trim() : '';
  const description  = typeof body.description === 'string' ? body.description.trim() : '';
  if (!hasBranchAccess(session, branchId)) {
    return res.status(403).json({ ok: false, error: 'No access to this branch' });
  }
  if (!INCIDENT_SEVERITIES.includes(body.severity)) {
    return res.status(400).json({ ok: false, error: `Invalid severity. Must be one of: ${INCIDENT_SEVERITIES.join(', ')}` });
  }
  if (!incidentType) return res.status(400).json({ ok: false, error: 'incidentType is required' });
  if (!description)  return res.status(400).json({ ok: false, error: 'description is required' });

  const db = getDbOr500(res); if (!db) return;
  try {
    const docData = {
      branchId,
      bookingId:        typeof body.bookingId === 'string' && body.bookingId ? clampStr(body.bookingId, 100) : null,
      incidentType:     clampStr(incidentType, 100),
      severity:         body.severity,
      description:      clampStr(description, 2000),
      customerInvolved: body.customerInvolved === true,
      injury:           body.injury === true,
      actionTaken:      clampStr(body.actionTaken ?? '', 2000),
      reportedBy:       session.name,
      reportedByRole:   session.role,
      status:           'open',
      createdAt:        FieldValue.serverTimestamp(),
      updatedAt:        FieldValue.serverTimestamp(),
    };
    const ref = await db.collection('incidents').add(docData);
    await writeAuditLog(db, {
      actor: session.name, actorRole: session.role, branchId,
      action: 'incident_create', targetId: ref.id,
      after: { incidentType: docData.incidentType, severity: docData.severity, injury: docData.injury },
    });
    return res.status(200).json({ ok: true, incidentId: ref.id });
  } catch (e) {
    console.error('[incident_create]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to create incident' });
  }
}

// incident_list — any role; partner-tier force-filtered to own branches.
async function handleIncidentList(res, session, body) {
  const limit = Math.min(Math.max(Number(body.limit) || 100, 1), 200);
  const db = getDbOr500(res); if (!db) return;
  try {
    // orderBy on a single field uses the automatic index. Branch + status
    // filtering happens in memory (iron rule: no branchId in where()).
    const snap = await db.collection('incidents').orderBy('createdAt', 'desc').limit(limit).get();
    let items = snap.docs.map(d => ({ id: d.id, ...serializeFsDoc(d.data()) }));
    items = filterToSessionBranches(session, items);
    if (typeof body.branchId === 'string' && body.branchId) {
      if (!hasBranchAccess(session, body.branchId)) {
        return res.status(403).json({ ok: false, error: 'No access to this branch' });
      }
      items = items.filter(i => resolveBranchId(i) === body.branchId);
    }
    if (typeof body.status === 'string' && body.status) {
      items = items.filter(i => i.status === body.status);
    }
    return res.status(200).json({ ok: true, incidents: items });
  } catch (e) {
    console.error('[incident_list]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to list incidents' });
  }
}

// incident_update_status — manager and above; partner-tier own branches only.
async function handleIncidentUpdateStatus(res, session, body) {
  if (!requireRole(session, 'owner', 'ultra_admin', 'branch_manager')) {
    return res.status(403).json({ ok: false, error: 'Requires branch_manager or above' });
  }
  const { incidentId, status } = body;
  if (typeof incidentId !== 'string' || !incidentId) {
    return res.status(400).json({ ok: false, error: 'Missing incidentId' });
  }
  if (!INCIDENT_STATUSES.includes(status)) {
    return res.status(400).json({ ok: false, error: `Invalid status. Must be one of: ${INCIDENT_STATUSES.join(', ')}` });
  }
  const db = getDbOr500(res); if (!db) return;
  try {
    const ref  = db.collection('incidents').doc(incidentId);
    const snap = await ref.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Incident not found' });
    const incident = snap.data();
    if (!hasBranchAccess(session, resolveBranchId(incident))) {
      return res.status(403).json({ ok: false, error: 'No access to this branch' });
    }
    await ref.update({ status, statusChangedBy: session.name, updatedAt: FieldValue.serverTimestamp() });
    await writeAuditLog(db, {
      actor: session.name, actorRole: session.role, branchId: resolveBranchId(incident),
      action: 'incident_update_status', targetId: incidentId,
      before: { status: incident.status }, after: { status },
    });
    return res.status(200).json({ ok: true });
  } catch (e) {
    console.error('[incident_update_status]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to update incident' });
  }
}

// ════════════════════════════════════════════════════════════════════
// Ratings
// ════════════════════════════════════════════════════════════════════

// rating_submit — PUBLIC. Defense order: cheap input validation first,
// then booking checks, then optional LIFF-token identity verification,
// then duplicate-proof create (doc ID = bookingId).
async function handleRatingSubmit(req, res, body) {
  const { bookingId, lineUserId, liffAccessToken } = body;
  const rating = Number(body.rating);

  if (typeof bookingId !== 'string' || !bookingId.trim()) {
    return res.status(400).json({ ok: false, error: 'Missing bookingId' });
  }
  if (!Number.isInteger(rating) || rating < 1 || rating > 5) {
    return res.status(400).json({ ok: false, error: 'rating must be an integer 1-5' });
  }
  if (typeof lineUserId !== 'string' || !lineUserId.trim()) {
    return res.status(400).json({ ok: false, error: 'Missing lineUserId' });
  }
  if (lineUserId === 'guest' || lineUserId === 'manual') {
    return res.status(400).json({ ok: false, error: 'Guest and manual bookings cannot be rated' });
  }
  const comment = clampStr(body.comment ?? '', 500);

  const db = getDbOr500(res); if (!db) return;

  let booking;
  try {
    const snap = await db.collection('bookings').doc(bookingId.trim()).get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Booking not found' });
    booking = snap.data();
  } catch (e) {
    console.error('[rating_submit] read booking:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to read booking' });
  }

  // Ownership — the server-stored lineUserId is the source of truth.
  if (booking.lineUserId !== lineUserId) {
    return res.status(403).json({ ok: false, error: 'This booking belongs to a different account' });
  }
  if (booking.bookingStatus === 'cancelled') {
    return res.status(400).json({ ok: false, error: 'Cancelled bookings cannot be rated' });
  }
  if (!bookingEndedBangkok(booking)) {
    return res.status(400).json({ ok: false, error: 'Booking has not ended yet' });
  }

  // Identity: token present → MUST verify (failure rejects).
  // Token absent → accepted but stored as verified:false.
  let verified = false;
  if (liffAccessToken) {
    verified = await verifyLineToken(liffAccessToken, lineUserId);
    if (!verified) {
      return res.status(403).json({ ok: false, error: 'LINE identity verification failed' });
    }
  }

  try {
    // .create() fails if the doc exists → exactly one rating per booking.
    // branchId comes from the booking doc only — never from the client.
    await db.collection('ratings').doc(bookingId.trim()).create({
      bookingId:    bookingId.trim(),
      bookingCode:  booking.bookingCode ?? null,
      branchId:     resolveBranchId(booking),
      rating,
      comment,
      lineUserId,
      customerName: booking.customerName ?? '',
      date:         booking.date ?? null,
      startTime:    booking.startTime ?? null,
      verified,
      createdAt:    FieldValue.serverTimestamp(),
    });
  } catch (e) {
    if (e?.code === 6 || /ALREADY_EXISTS/i.test(e?.message ?? '')) {
      return res.status(409).json({ ok: false, error: 'This booking has already been rated' });
    }
    console.error('[rating_submit] create:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to save rating' });
  }

  console.log(`[rating_submit] OK — booking:${bookingId} rating:${rating} verified:${verified}`);
  return res.status(200).json({ ok: true, verified });
}

// rating_list — any role; partner-tier limited to own branches. No export format.
async function handleRatingList(res, session, body) {
  const limit = Math.min(Math.max(Number(body.limit) || 100, 1), 200);
  const db = getDbOr500(res); if (!db) return;
  try {
    const snap = await db.collection('ratings').orderBy('createdAt', 'desc').limit(limit).get();
    let items = snap.docs.map(d => ({ id: d.id, ...serializeFsDoc(d.data()) }));
    items = filterToSessionBranches(session, items);
    if (typeof body.branchId === 'string' && body.branchId) {
      if (!hasBranchAccess(session, body.branchId)) {
        return res.status(403).json({ ok: false, error: 'No access to this branch' });
      }
      items = items.filter(r => resolveBranchId(r) === body.branchId);
    }
    return res.status(200).json({ ok: true, ratings: items });
  } catch (e) {
    console.error('[rating_list]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to list ratings' });
  }
}

// ════════════════════════════════════════════════════════════════════
// Dashboard (owner / ultra_admin)
// ════════════════════════════════════════════════════════════════════
// All queries are single-field date ranges (same pattern as finance-data)
// or single-field status filters — no composite index needed. Grouping
// and branch resolution happen in memory.
async function handleDashboard(res, session, body) {
  if (!isHQ(session)) {
    return res.status(403).json({ ok: false, error: 'Owner or ultra_admin only' });
  }
  const { month } = body;
  if (typeof month !== 'string' || !/^\d{4}-\d{2}$/.test(month)) {
    return res.status(400).json({ ok: false, error: 'month must be YYYY-MM' });
  }
  const wantBranch = typeof body.branchId === 'string' && body.branchId && body.branchId !== 'all' ? body.branchId : null;
  const start = `${month}-01`, end = `${month}-31`; // lexical range covers all real dates

  const db = getDbOr500(res); if (!db) return;
  try {
    const [bkSnap, avSnap, brSnap, incSnap] = await Promise.all([
      db.collection('bookings').where('date', '>=', start).where('date', '<=', end).get(),
      db.collection('available_slots').where('date', '>=', start).where('date', '<=', end).get(),
      db.collection('branches').get(),
      db.collection('incidents').where('status', 'in', ['open', 'reviewing']).get(),
    ]);

    const branchStatuses = {};
    brSnap.docs.forEach(d => { branchStatuses[d.id] = d.data().status ?? 'active'; });

    const branches = {}; // branchId → metrics
    const bucket = (branchId) => {
      if (!branches[branchId]) {
        branches[branchId] = {
          branchStatus: branchStatuses[branchId] ?? 'active',
          bookingsTotal: 0, bookingsByDay: {},
          paidRevenue: 0, refundTotal: 0, packageValue: 0, packageCount: 0,
          cancelledCount: 0, unpaidCount: 0, pendingReviewCount: 0,
          utilization: {}, // resourceId → {openHours, bookedHours, rate}
          openCriticalIncidents: 0,
        };
      }
      return branches[branchId];
    };
    const resBucket = (b, resourceId) => {
      const rid = resourceId || 'room1';
      if (!b.utilization[rid]) b.utilization[rid] = { openHours: 0, bookedHours: 0, rate: null };
      return b.utilization[rid];
    };

    bkSnap.docs.forEach(d => {
      const bk = d.data();
      const b  = bucket(resolveBranchId(bk));
      if (bk.bookingStatus === 'cancelled') { b.cancelledCount++; return; }
      b.bookingsTotal++;
      if (bk.date) b.bookingsByDay[bk.date] = (b.bookingsByDay[bk.date] ?? 0) + 1;
      if (bk.paymentStatus === 'paid')           b.paidRevenue += Number(bk.price) || 0;
      if (bk.paymentStatus === 'unpaid')         b.unpaidCount++;
      if (bk.paymentStatus === 'pending_review') b.pendingReviewCount++;
      if (bk.paymentStatus === 'package') {
        b.packageCount++;
        b.packageValue += Number(bk.packageUsageValueTotal) || 0;
      }
      if (bk.refundStatus) b.refundTotal += Number(bk.refundAmount) || 0;
      if (bk.bookingStatus === 'confirmed') resBucket(b, bk.resourceId).bookedHours += Number(bk.durationHours) || 1;
    });

    avSnap.docs.forEach(d => {
      const av = d.data();
      if (av.status !== 'open') return;
      resBucket(bucket(resolveBranchId(av)), av.resourceId).openHours++;
    });

    incSnap.docs.forEach(d => {
      const inc = d.data();
      if (inc.severity === 'critical') bucket(resolveBranchId(inc)).openCriticalIncidents++;
    });

    // Finish utilization rates; round money fields.
    Object.values(branches).forEach(b => {
      Object.values(b.utilization).forEach(u => {
        u.rate = u.openHours > 0 ? Math.round((u.bookedHours / u.openHours) * 1000) / 1000 : null;
      });
    });

    const result = wantBranch
      ? { [wantBranch]: branches[wantBranch] ?? bucket(wantBranch) }
      : branches;

    return res.status(200).json({ ok: true, month, branches: result, branchStatuses });
  } catch (e) {
    console.error('[dashboard]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to compute dashboard' });
  }
}

// ════════════════════════════════════════════════════════════════════
// Audit log
// ════════════════════════════════════════════════════════════════════

// audit_log_write — client-reported audit entries for admin.html's
// client-direct Firestore writes. Actor/role ALWAYS from the session;
// the action must be in the client-report whitelist.
async function handleAuditLogWrite(res, session, body) {
  if (session.role === 'viewer') {
    return res.status(403).json({ ok: false, error: 'Viewers cannot write audit entries' });
  }
  const clientAction = body.auditAction;
  if (!CLIENT_AUDIT_ACTIONS.includes(clientAction)) {
    return res.status(400).json({ ok: false, error: `Invalid audit action. Allowed: ${CLIENT_AUDIT_ACTIONS.join(', ')}` });
  }
  const branchId = typeof body.branchId === 'string' && body.branchId
    ? clampStr(body.branchId, 40)
    : DEFAULT_BRANCH_ID;
  if (!hasBranchAccess(session, branchId)) {
    return res.status(403).json({ ok: false, error: 'No access to this branch' });
  }
  const db = getDbOr500(res); if (!db) return;
  await writeAuditLog(db, {
    actor:     session.name,
    actorRole: session.role,
    branchId,
    action:    clientAction,
    targetId:  typeof body.targetId === 'string' ? clampStr(body.targetId, 100) : null,
    before:    sanitizeAuditSnapshot(body.before),
    after:     sanitizeAuditSnapshot(body.after),
    note:      clampStr(body.note ?? body.summary ?? '', 400),
    source:    'client_report',
  });
  // writeAuditLog never throws — always acknowledge so the caller's
  // fire-and-forget flow is never disturbed.
  return res.status(200).json({ ok: true });
}

// audit_log_list — owner / ultra_admin only.
async function handleAuditLogList(res, session, body) {
  if (!isHQ(session)) {
    return res.status(403).json({ ok: false, error: 'Owner or ultra_admin only' });
  }
  const limit = Math.min(Math.max(Number(body.limit) || 100, 1), 500);
  const db = getDbOr500(res); if (!db) return;
  try {
    const snap = await db.collection('audit_logs').orderBy('createdAt', 'desc').limit(limit).get();
    let items = snap.docs.map(d => ({ id: d.id, ...serializeFsDoc(d.data()) }));
    if (typeof body.branchId === 'string' && body.branchId) {
      items = items.filter(a => (a.branchId ?? DEFAULT_BRANCH_ID) === body.branchId);
    }
    return res.status(200).json({ ok: true, entries: items });
  } catch (e) {
    console.error('[audit_log_list]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to list audit log' });
  }
}
