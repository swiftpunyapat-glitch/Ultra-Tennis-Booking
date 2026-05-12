// ════════════════════════════════════════════════════════════════════
// POST /api/notify-admins  — broadcast to all registered admins
// ════════════════════════════════════════════════════════════════════
// Sends the same notification to every admin in admin_notify_users
// (enabled == true). Each admin gets a unique eventId so failed sends
// can be retried per-admin without re-sending to the others.
//
// Request:
//   POST /api/notify-admins
//   Headers:
//     Content-Type: application/json
//     x-internal-secret: <NOTIFY_INTERNAL_SECRET>
//   Body:
//     {
//       "type":         "slip_uploaded_admin" | "new_booking_admin",
//       "bookingCode":  "UT-2026-1234",
//       ...type-specific fields (see api/_lib/notify.js TEMPLATES)
//     }
//
// Response (always 200 unless request itself is malformed):
//   {
//     ok: true | false,            // false if any admin send failed
//     summary: { total, successes, skipped, failed },
//     results: [
//       { adminLineUserId, adminName, ok, status, reason?, error? }, ...
//     ]
//   }
//
// eventId for each admin = `{bookingCode}_{type}_{adminLineUserId}`.
// ════════════════════════════════════════════════════════════════════

import { checkInternalSecret, sendAndLog, loadActiveAdmins, VALID_TYPES } from "./_lib/notify.js";

export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  // Auth — TODO(auth-step-2): replace with Firebase ID token verification.
  const auth = checkInternalSecret(req);
  if (!auth.ok) return res.status(auth.status).json({ ok: false, error: auth.error });

  const body = (req.body && typeof req.body === "object")
    ? req.body
    : safeJSON(req.body);
  if (!body || typeof body !== "object") {
    return res.status(400).json({ ok: false, error: "Invalid JSON body" });
  }

  const { type, bookingCode } = body;
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
      error: `Type "${type}" is not an *_admin notification. Use /api/line-push for customer notifications.`,
    });
  }

  let admins;
  try {
    admins = await loadActiveAdmins();
  } catch (e) {
    console.error("[/api/notify-admins] failed to load admins:", e);
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
  const results = await Promise.all(admins.map(async (admin) => {
    const eventId = `${bookingCode}_${type}_${admin.lineUserId}`;
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
      console.error(`[/api/notify-admins] sendAndLog threw for ${admin.lineUserId}:`, e);
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
