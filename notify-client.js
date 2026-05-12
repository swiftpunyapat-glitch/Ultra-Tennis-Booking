// ════════════════════════════════════════════════════════════════════
// Ultra Tennis Booking — frontend LINE notification helpers
// ════════════════════════════════════════════════════════════════════
// Two globals are exposed for use from index.html and admin.html:
//
//   sendLineNotification(payload)   → POST /api/line-push
//   notifyAdmins(payload)           → POST /api/notify-admins
//
// Both are FIRE-AND-FORGET. They:
//   • never throw
//   • swallow any error and log to console with [notify-client] prefix
//   • return a Promise that resolves to the API result (or a synthetic
//     {ok:false,...} on transport error), but the caller does NOT need
//     to await it
//
// Usage from HTML:
//
//   <script>
//     // TODO(auth-step-2): when Firebase Auth lands, delete this and
//     // change notify-client.js to send `Authorization: Bearer <idToken>`.
//     window.NOTIFY_INTERNAL_SECRET = "REPLACE_WITH_YOUR_VERCEL_SECRET";
//   </script>
//   <script src="/notify-client.js" defer></script>
//
//   // ... later, fire-and-forget:
//   sendLineNotification({
//     eventId:    `${booking.bookingCode}_booking_paid_customer`,
//     type:       "booking_paid_customer",
//     lineUserId: booking.lineUserId,
//     bookingCode: booking.bookingCode,
//     date: booking.date, startTime: booking.startTime, endTime: booking.endTime,
//   });
//
// ─── IMPORTANT SECURITY NOTE ────────────────────────────────────────
// `window.NOTIFY_INTERNAL_SECRET` is visible to anyone who loads the
// page. The x-internal-secret model is a stop-gap meant to filter out
// drive-by abuse, not authenticated attackers. Plan to migrate to
// Firebase Auth ID-token verification on the API routes as soon as the
// rest of the app gets auth (see firestore.rules → ROADMAP).
// ════════════════════════════════════════════════════════════════════

(function () {
  // Same-origin by default. Override window.NOTIFY_API_BASE if you ever
  // host the functions on a different domain.
  function apiBase() {
    return (typeof window !== "undefined" && window.NOTIFY_API_BASE) || "";
  }

  function authHeader() {
    const secret = (typeof window !== "undefined" && window.NOTIFY_INTERNAL_SECRET) || "";
    if (!secret) {
      console.warn("[notify-client] window.NOTIFY_INTERNAL_SECRET is not set — request will 401.");
    }
    return {
      "Content-Type": "application/json",
      "x-internal-secret": secret,
    };
  }

  async function call(path, payload) {
    const url = apiBase() + path;
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: authHeader(),
        body: JSON.stringify(payload || {}),
        keepalive: true, // best-effort delivery even if user navigates away
      });
      let body = null;
      try { body = await res.json(); } catch (_) { /* response wasn't JSON */ }
      if (!res.ok) {
        console.warn(`[notify-client] ${path} → HTTP ${res.status}`, body);
        return body || { ok: false, status: "failed", error: `HTTP ${res.status}` };
      }
      if (body && body.ok === false) {
        console.warn(`[notify-client] ${path} non-fatal failure:`, body);
      }
      return body || { ok: false, status: "failed", error: "empty response" };
    } catch (e) {
      // Network errors, CORS preflight failures, etc. Never propagate.
      console.warn(`[notify-client] ${path} threw:`, e);
      return { ok: false, status: "failed", error: e.message };
    }
  }

  // ─── public API ───────────────────────────────────────────────────
  window.sendLineNotification = function (payload) {
    return call("/api/line-push", payload);
  };

  window.notifyAdmins = function (payload) {
    return call("/api/notify-admins", payload);
  };
})();
