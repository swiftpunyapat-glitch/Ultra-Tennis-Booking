// ════════════════════════════════════════════════════════════════════
// POST /api/line-push  — send one LINE notification to one user
// ════════════════════════════════════════════════════════════════════
// Used for customer notifications (booking paid, cancelled, rescheduled,
// pass activated). Admin broadcasts go through /api/notify-admins.
//
// Request:
//   POST /api/line-push
//   Headers:
//     Content-Type: application/json
//     x-internal-secret: <NOTIFY_INTERNAL_SECRET>
//   Body:
//     {
//       "eventId":     "UT-2026-1234_booking_paid_customer",
//       "type":        "booking_paid_customer",
//       "lineUserId":  "Uxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
//       "bookingCode": "UT-2026-1234",
//       ...type-specific fields (see api/_lib/notify.js TEMPLATES)
//     }
//
// Response (always 200 unless request itself is malformed):
//   { ok:true,  status:"success" }
//   { ok:true,  status:"skipped", reason:"no_user_id" | "duplicate" }
//   { ok:false, status:"failed",  error }
//
// Notification failures intentionally return 200 with ok:false so the
// caller (booking flow / admin Mark Paid) does not retry or surface
// errors to the end user.
// ════════════════════════════════════════════════════════════════════

import { checkInternalSecret, sendAndLog, VALID_TYPES } from "./_lib/notify.js";

export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ ok: false, error: "Method not allowed" });
  }

  // Auth — TODO(auth-step-2): replace with Firebase ID token verification.
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
    console.error("[/api/line-push] unhandled:", e);
    return res.status(200).json({ ok: false, status: "failed", error: e.message });
  }
}

function safeJSON(raw) {
  if (typeof raw !== "string") return null;
  try { return JSON.parse(raw); } catch { return null; }
}
