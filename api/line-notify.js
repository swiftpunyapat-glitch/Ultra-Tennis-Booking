// ════════════════════════════════════════════════════════════════════
// POST /api/line-notify — consolidated LINE notification route
// ════════════════════════════════════════════════════════════════════
// Merged from line-push.js (single-user send) + notify-admins.js
// (broadcast to all registered admins) to keep the Vercel function
// count down. Behavior per audience is identical to the old routes.
//
// Request:
//   POST /api/line-notify
//   Headers:
//     Content-Type: application/json
//     x-internal-secret: <NOTIFY_INTERNAL_SECRET>
//   Body:
//     { "audience": "user",                       ← old /api/line-push
//       "eventId": "...", "type": "...", "lineUserId": "...",
//       "bookingCode": "...", ...type-specific fields }
//   or
//     { "audience": "admins",                     ← old /api/notify-admins
//       "type": "..._admin", "bookingCode": "...", "eventSuffix"?: "...",
//       ...type-specific fields }
//
// Responses are unchanged from the old routes (always 200 unless the
// request itself is malformed — notification failures return 200 with
// ok:false so callers never retry or surface errors to end users).
//
// Auth — TODO(auth-step-2): replace with Firebase ID token verification.
// ════════════════════════════════════════════════════════════════════

import { checkInternalSecret, sendAndLog, loadActiveAdmins, VALID_TYPES } from "./_lib/notify.js";

export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  const auth = checkInternalSecret(req);
  if (!auth.ok) return res.status(auth.status).json({ ok: false, error: auth.error });

  // Parse body. Vercel parses JSON automatically when Content-Type is set,
  // but we handle string fallback defensively.
  const body = (req.body && typeof req.body === "object")
    ? req.body
    : safeJSON(req.body);
  if (!body || typeof body !== "object") {
    return res.status(400).json({ ok: false, error: "Invalid JSON body" });
  }

  if (body.audience === "admins") return notifyAdmins(res, body);
  return pushToUser(res, body);
}

// ── single-user send (old /api/line-push) ───────────────────────────
async function pushToUser(res, body) {
  const { eventId, type, lineUserId, bookingCode } = body;
  if (!eventId)         return res.status(400).json({ ok: false, error: "Missing eventId" });
  if (!type)            return res.status(400).json({ ok: false, error: "Missing type" });
  if (!VALID_TYPES.includes(type)) {
    return res.status(400).json({
      ok: false,
      error: `Unknown type "${type}". Allowed: ${VALID_TYPES.join(", ")}`,
    });
  }

  try {
    const result = await sendAndLog({
      eventId,
      type,
      targetType: type.endsWith("_admin") ? "admin" : "customer",
      lineUserId,
      bookingCode,
      payload: body,
    });
    return res.status(200).json(result);
  } catch (e) {
    // sendAndLog should never throw — but if it does (e.g. Firestore
    // outage), still return 200 so the caller's flow isn't disrupted.
    console.error("[/api/line-notify user] unhandled:", e);
    return res.status(200).json({ ok: false, status: "failed", error: e.message });
  }
}

// ── broadcast to all registered admins (old /api/notify-admins) ─────
// Sends the same notification to every admin in admin_notify_users
// (enabled == true). Each admin gets a unique eventId so failed sends
// can be retried per-admin without re-sending to the others.
// eventId for each admin = `{bookingCode}_{type}_{adminLineUserId}`.
async function notifyAdmins(res, body) {
  // eventSuffix is optional — when provided it is appended to the per-admin
  // eventId so that multiple events of the same type for the same booking
  // (e.g. rescheduled twice) each produce a unique idempotency key.
  const { type, bookingCode, eventSuffix } = body;
  if (!type)        return res.status(400).json({ ok: false, error: "Missing type" });
  if (!bookingCode) return res.status(400).json({ ok: false, error: "Missing bookingCode" });
  if (!VALID_TYPES.includes(type)) {
    return res.status(400).json({
      ok: false,
      error: `Unknown type "${type}". Allowed: ${VALID_TYPES.join(", ")}`,
    });
  }
  if (!type.endsWith("_admin")) {
    return res.status(400).json({
      ok: false,
      error: `Type "${type}" is not an *_admin notification. Use audience:"user" for customer notifications.`,
    });
  }

  let admins;
  try {
    admins = await loadActiveAdmins();
  } catch (e) {
    console.error("[/api/line-notify admins] failed to load admins:", e);
    return res.status(200).json({
      ok: false,
      status: "failed",
      error: `Could not load admin recipients: ${e.message}`,
      summary: { total: 0, successes: 0, skipped: 0, failed: 0 },
      results: [],
    });
  }

  if (!admins.length) {
    return res.status(200).json({
      ok: true,
      status: "skipped",
      reason: "no_admins_registered",
      summary: { total: 0, successes: 0, skipped: 0, failed: 0 },
      results: [],
    });
  }

  // Fan out — each admin gets its own eventId for independent idempotency.
  // A sanitised eventSuffix (if provided) is appended so that the same booking
  // can be notified more than once under different events (e.g. rescheduled twice).
  const safeSuffix = eventSuffix
    ? `_${String(eventSuffix).replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 30)}`
    : '';
  const results = await Promise.all(admins.map(async (admin) => {
    const eventId = `${bookingCode}_${type}_${admin.lineUserId}${safeSuffix}`;
    try {
      const r = await sendAndLog({
        eventId,
        type,
        targetType: "admin",
        lineUserId: admin.lineUserId,
        bookingCode,
        payload: body,
      });
      return {
        adminLineUserId: admin.lineUserId,
        adminName: admin.adminName || null,
        ...r,
      };
    } catch (e) {
      console.error(`[/api/line-notify admins] sendAndLog threw for ${admin.lineUserId}:`, e);
      return {
        adminLineUserId: admin.lineUserId,
        adminName: admin.adminName || null,
        ok: false,
        status: "failed",
        error: e.message,
      };
    }
  }));

  const successes = results.filter(r => r.ok && r.status === "success").length;
  const skipped   = results.filter(r => r.ok && r.status === "skipped").length;
  const failed    = results.filter(r => !r.ok || r.status === "failed").length;

  return res.status(200).json({
    ok: failed === 0,
    summary: { total: results.length, successes, skipped, failed },
    results,
  });
}

function safeJSON(raw) {
  if (typeof raw !== "string") return null;
  try { return JSON.parse(raw); } catch { return null; }
}
