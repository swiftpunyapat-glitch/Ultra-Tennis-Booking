// ════════════════════════════════════════════════════════════════════
// Ultra Tennis Booking — server-side LINE notification helpers
// ════════════════════════════════════════════════════════════════════
// Used by:
//   /api/line-notify  — single-target send (audience:"user") and
//                       broadcast to all registered admins (audience:"admins")
//
// Conventions:
//   • All Firestore writes use the Firebase Admin SDK, which bypasses
//     security rules. That is intentional for server-side code — these
//     functions run with full credentials via FIREBASE_SERVICE_ACCOUNT.
//   • Notification work must NEVER throw upstream — every failure is
//     logged and returned in the response body. The booking + admin
//     flows treat the notify call as fire-and-forget.
//   • Per-target idempotency keyed by notification_logs/{eventId}.
//
// Env vars (set these in Vercel project settings → Environment Variables):
//   LINE_CHANNEL_ACCESS_TOKEN    — long-lived OA channel token
//   NOTIFY_INTERNAL_SECRET       — shared secret in `x-internal-secret` header
//   FIREBASE_SERVICE_ACCOUNT     — JSON string of the service account key
//
// ─── TODO(auth-step-2) ──────────────────────────────────────────────
// The x-internal-secret model is a stop-gap. Once the broader app
// migrates to Firebase Auth (see firestore.rules → ROADMAP), replace
// checkInternalSecret() with verification of a Firebase ID token:
//
//   import { getAuth } from "firebase-admin/auth";
//   const idToken = (req.headers.authorization || "").replace(/^Bearer /, "");
//   const decoded = await getAuth().verifyIdToken(idToken);
//   if (notifyAdminsRoute && !decoded.admin) return 403;
//
// At that point the client stops sending NOTIFY_INTERNAL_SECRET entirely.
// ════════════════════════════════════════════════════════════════════

import { initializeApp, getApps, cert } from "firebase-admin/app";
import { getFirestore, FieldValue } from "firebase-admin/firestore";

// ─── Firebase Admin singleton (survives warm invocations) ───────────
function getDb() {
  if (!getApps().length) {
    const raw = process.env.FIREBASE_SERVICE_ACCOUNT;
    if (!raw) throw new Error("FIREBASE_SERVICE_ACCOUNT env var not set");
    let sa;
    try { sa = JSON.parse(raw); }
    catch (e) { throw new Error("FIREBASE_SERVICE_ACCOUNT is not valid JSON: " + e.message); }
    initializeApp({ credential: cert(sa) });
  }
  return getFirestore();
}

// ─── Internal-secret auth check ─────────────────────────────────────
// Returns { ok:true } or { ok:false, status, error }.
export function checkInternalSecret(req) {
  const expected = process.env.NOTIFY_INTERNAL_SECRET;
  if (!expected) {
    return { ok: false, status: 500, error: "NOTIFY_INTERNAL_SECRET not configured on server" };
  }
  const got = req.headers["x-internal-secret"];
  if (!got || got !== expected) {
    return { ok: false, status: 401, error: "unauthorized" };
  }
  return { ok: true };
}

// ─── Message templates ──────────────────────────────────────────────
// Each builder returns the `messages` array for the LINE Messaging API
// (max 5 message objects per push request, per LINE docs).
// All templates emit plain text — easy to read on every LINE client.
const PAYMENT_STATUS_TH = {
  pending_payment: "รอชำระเงิน",
  unpaid: "ยังไม่ชำระ",
  paid: "ชำระแล้ว",
  pending_review: "รอตรวจสอบสลิป",
  package: "แพ็กเกจ",
  rejected: "ไม่ผ่าน / ยกเลิก",
};

const BOOKING_TYPE_TH = {
  manual: "จองโดยแอดมิน",
  "Manual Single Use": "จองรายครั้งโดยแอดมิน",
  "Paid Outside": "ชำระเงินภายนอก",
  "Pay at Counter": "ชำระที่เคาน์เตอร์",
  "Ultra Pass Manual": "ใช้ Ultra Pass โดยแอดมิน",
  "Off-Peak Manual": "ใช้ Off-Peak Pass โดยแอดมิน",
  "Single Use": "จองรายครั้ง",
  "Late Night Session": "รอบดึก",
  "Coach Lesson": "คาบสอนกับโค้ช",
};

const REFUND_REASON_TH = {
  machine_issue: "เครื่องมีปัญหา",
  safety_incident: "เหตุการณ์ด้านความปลอดภัย",
  customer_request: "ลูกค้าขอคืนเงิน",
  admin_mistake: "แอดมินดำเนินการผิดพลาด",
  duplicate_payment: "ชำระเงินซ้ำ",
  other: "อื่น ๆ",
};

const INCIDENT_TYPE_TH = {
  machine_malfunction: "เครื่องขัดข้อง",
  ball_hit_customer: "ลูกค้าถูกลูกบอล",
  room_issue: "ห้องมีปัญหา",
  booking_error: "ข้อมูลการจองผิดพลาด",
  none: "ไม่มี",
  other: "อื่น ๆ",
};

function thaiValue(map, value) {
  return map[value] || value || "—";
}

const TEMPLATES = {
  booking_paid_customer: (p) => [{
    type: "text",
    text:
`✅ ยืนยันการจองแล้ว

Ultra Tennis
รหัสจอง: ${p.bookingCode}
วันที่: ${p.date}
เวลา: ${p.startTime}–${p.endTime}
สถานะ: ชำระเงินเรียบร้อย

พบกันที่ Ultra Tennis 🎾`,
  }],

  booking_cancelled_customer: (p) => [{
    type: "text",
    text:
`❌ การจองถูกยกเลิก

รหัสจอง: ${p.bookingCode}
วันที่: ${p.date}
เวลา: ${p.startTime}–${p.endTime}

เหตุผล: ${p.reason || "สลิปไม่ถูกต้องหรือไม่พบยอดโอน"}
กรุณาติดต่อแอดมินหรือจองใหม่อีกครั้ง`,
  }],

  booking_rescheduled_customer: (p) => [{
    type: "text",
    text:
`📅 เปลี่ยนเวลาจองเรียบร้อย

รหัสจอง: ${p.bookingCode}
เวลาเดิม: ${p.previousDate || "—"} ${p.previousStartTime || "—"}–${p.previousEndTime || "—"}
เวลาใหม่: ${p.date} ${p.startTime}–${p.endTime}

กรุณาตรวจสอบใน My Booking`,
  }],

  // Distinguishes payment paths via p.paymentStatus (sent by the client):
  //   "package"        → booked with a Pass — CONFIRMED, no slip will come.
  //   anything else    → single-use / coach lesson — waiting for payment+slip.
  // p.bookingType carries the pass name; p.coachName marks coach lessons.
  new_booking_admin: (p) => [{
    type: "text",
    text: p.paymentStatus === "package"
      ?
`🎾 มีการจองคอร์ท (ใช้ Pass)

รหัสจอง: ${p.bookingCode}
ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
วันที่: ${p.date}
เวลา: ${p.startTime}–${p.endTime}
วิธีชำระ: ใช้ ${thaiValue(BOOKING_TYPE_TH, p.bookingType)}
สถานะ: ยืนยันแล้ว ไม่ต้องรอสลิป`
      :
`🎾 มีการจองคอร์ทใหม่

รหัสจอง: ${p.bookingCode}
ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
วันที่: ${p.date}
เวลา: ${p.startTime}–${p.endTime}${p.coachName ? `
โค้ช: ${p.coachName} (คาบสอนพร้อมคอร์ท)` : ""}
สถานะ: รอชำระเงิน (จองช่วงเวลาคอร์ท)`,
  }],

  // Customer cancelled their own UNPAID pending booking (cancel_pending).
  // Sent server-side by /api/booking, gated by the
  // notifyAdminOnCustomerPendingCancel flag (default OFF — see notify flags).
  customer_cancel_pending_admin: (p) => [{
    type: "text",
    text:
`❌ ลูกค้ายกเลิกก่อนชำระเงิน

รหัสจอง: ${p.bookingCode}
ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
วันที่: ${p.date || "—"}
เวลา: ${p.startTime || "—"}–${p.endTime || "—"}
สถานะเดิม: รอชำระเงิน / ยังไม่มีสลิป
หมายเหตุ: ลูกค้ากดยกเลิกเอง ช่องเวลาถูกปล่อยแล้ว`,
  }],

  // Admin visibility on a cancellation that mattered (had a slip, was in
  // review, or was confirmed/paid). Quick unpaid-pending cancels do NOT use
  // this — callers decide (see admin.html cancelBooking).
  booking_cancelled_admin: (p) => [{
    type: "text",
    text:
`✖️ การจองถูกยกเลิก

รหัสจอง: ${p.bookingCode}
ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
วันที่: ${p.date || "—"}
เวลา: ${p.startTime || "—"}–${p.endTime || "—"}
สถานะก่อนยกเลิก: ${thaiValue(PAYMENT_STATUS_TH, p.prevPaymentStatus)}${p.hadSlip ? `
มีสลิปแนบอยู่ — ตรวจสอบใน Slip Review` : ""}
ยกเลิกโดย: ${p.actionBy || "—"}${p.reason ? `
เหตุผล: ${p.reason}` : ""}`,
  }],

  // Pass purchase slip — explicitly NOT a court time-slot booking, so admins
  // don't confuse it with a room reservation.
  pass_purchase_admin: (p) => [{
    type: "text",
    text:
`🎫 มีการซื้อแพ็กเกจ (Pass)
⚠️ ไม่ใช่การจองคอร์ท — ไม่มีช่วงเวลาคอร์ทถูกจอง

รหัสซื้อ: ${p.purchaseCode || p.bookingCode || "—"}
ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
แพ็กเกจ: ${p.packageName || "—"}
ยอด: ฿${p.price ?? "—"}
สถานะ: รอตรวจสอบสลิป${p.precheckNote ? `
${p.precheckNote}` : ""}

กรุณาตรวจสลิปและเปิดใช้งาน Pass ในหน้า Admin`,
  }],

  // Pass purchase approved by admin → package auto-issued (Stage D).
  pass_issued_admin: (p) => [{
    type: "text",
    text:
`✅ เปิดใช้งาน Pass แล้ว (ซื้อออนไลน์)

รหัสซื้อ: ${p.purchaseCode || "—"}
ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
แพ็กเกจ: ${p.packageName || "—"}
ยอด: ฿${p.price ?? "—"}
อนุมัติโดย: ${p.actionBy || "—"}

Pass ถูกเพิ่มเข้าบัญชี LINE ของลูกค้าแล้วอัตโนมัติ`,
  }],

  pass_purchase_rejected_customer: (p) => [{
    type: "text",
    text:
`❌ การซื้อแพ็กเกจไม่ผ่านการตรวจสอบ

รหัสซื้อ: ${p.purchaseCode || "—"}
แพ็กเกจ: ${p.packageName || "—"}

เหตุผล: ${p.reason || "สลิปไม่ถูกต้องหรือไม่พบยอดโอน"}
กรุณาติดต่อแอดมิน Ultra Tennis`,
  }],

  slip_uploaded_admin: (p) => [{
    type: "text",
    text:
`📩 ลูกค้าอัปโหลดสลิปแล้ว

รหัสจอง: ${p.bookingCode}
ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
วันที่: ${p.date}
เวลา: ${p.startTime}–${p.endTime}

กรุณาตรวจสอบสลิปในหน้า Admin`,
  }],

  // Phase 1A slip pre-check (server-side, /api/slip-verify). pre_verified
  // means LOCAL checks passed only — NOT bank-verified, NOT paid. Admin
  // still confirms via Mark as Paid.
  slip_precheck_admin: (p) => [{
    type: "text",
    text:
`📩 สลิปใหม่ · ตรวจเบื้องต้นผ่าน ✓

รหัสจอง: ${p.bookingCode}
ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
วันที่: ${p.date || "—"}
เวลา: ${p.startTime || "—"}–${p.endTime || "—"}
ยอดที่ต้องได้รับ: ฿${p.expectedAmount ?? "—"}

ไม่พบสลิปซ้ำในระบบ (ตรวจเบื้องต้นเท่านั้น ยังไม่ยืนยันยอดกับธนาคาร)
กรุณาตรวจสลิปและกด Mark as Paid ในหน้า Admin`,
  }],

  slip_review_admin: (p) => [{
    type: "text",
    text:
`🚨 สลิปต้องตรวจสอบเป็นพิเศษ

รหัสจอง: ${p.bookingCode}
ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
วันที่: ${p.date || "—"}
เวลา: ${p.startTime || "—"}–${p.endTime || "—"}
ยอดที่ต้องได้รับ: ฿${p.expectedAmount ?? "—"}

เหตุผล: ${p.reasonText || "ต้องตรวจสอบด้วยตนเอง"}${p.dupOfBookingCode ? `
อาจซ้ำกับรหัสจอง: ${p.dupOfBookingCode}` : ""}

โปรดตรวจสอบให้แน่ใจก่อนกด Mark as Paid`,
  }],

  pass_activated_customer: (p) => [{
    type: "text",
    text:
`🎫 Pass พร้อมใช้งาน

${p.packageName || "Ultra Pass"} ของคุณพร้อมใช้งานแล้ว${p.remainingMinutes ? `
เหลือ ${p.remainingMinutes} นาที` : ""}
ใช้ได้ถึง: ${p.validUntil || "—"}

จองได้ที่ Ultra Tennis LINE`,
  }],

  manual_booking_admin: (p) => [{
    type: "text",
    text:
`📝 แอดมินสร้างรายการจอง

รหัสจอง: ${p.bookingCode || "—"}
ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
วันที่: ${p.date || "—"}
เวลา: ${p.startTime || "—"}–${p.endTime || "—"}
ประเภท: ${thaiValue(BOOKING_TYPE_TH, p.bookingType)}
สถานะชำระเงิน: ${thaiValue(PAYMENT_STATUS_TH, p.paymentStatus)}
ดำเนินการโดย: ${p.actionBy || "—"}${p.note ? `
หมายเหตุ: ${p.note}` : ""}`,
  }],

  booking_rescheduled_admin: (p) => [{
    type: "text",
    text:
`🔁 เปลี่ยนเวลาจองแล้ว

ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
รหัสจอง: ${p.bookingCode || "—"}
เวลาเดิม: ${p.previousDate || "—"} ${p.previousStartTime || "—"}–${p.previousEndTime || "—"}
เวลาใหม่: ${p.date || "—"} ${p.startTime || "—"}–${p.endTime || "—"}
ประเภท: ${thaiValue(BOOKING_TYPE_TH, p.bookingType)}
สถานะชำระเงิน: ${thaiValue(PAYMENT_STATUS_TH, p.paymentStatus)}
ดำเนินการโดย: ${p.actionBy || "—"}`,
  }],

  booking_pending_reschedule_admin: (p) => [{
    type: "text",
    text:
`⏳ รอจัดเวลาใหม่

ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
รหัสจอง: ${p.bookingCode || "—"}
เวลาเดิม: ${p.previousDate || "—"} ${p.previousStartTime || "—"}–${p.previousEndTime || "—"}
สถานะชำระเงิน: ${thaiValue(PAYMENT_STATUS_TH, p.paymentStatus)}
ดำเนินการโดย: ${p.actionBy || "—"}`,
  }],

  booking_refunded_admin: (p) => [{
    type: "text",
    text:
`↩ ดำเนินการคืนเงินแล้ว

ลูกค้า: ${p.customerName || "—"}
เบอร์โทร: ${p.customerPhone || "—"}
รหัสจอง: ${p.bookingCode || "—"}
ยอดคืนเงิน: ฿${p.refundAmount || "—"}
เหตุผล: ${thaiValue(REFUND_REASON_TH, p.refundReason)}
เหตุการณ์: ${thaiValue(INCIDENT_TYPE_TH, p.incidentType)}
วันเวลาเล่น: ${p.date || "—"} ${p.startTime || "—"}–${p.endTime || "—"}
ดำเนินการโดย: ${p.actionBy || "—"}`,
  }],

  admin_alert_customer: (p) => [{
    type: "text",
    text: p.message || "(ไม่มีข้อความ)",
  }],
};

export const VALID_TYPES = Object.keys(TEMPLATES);

function buildMessages(type, payload) {
  const fn = TEMPLATES[type];
  if (!fn) throw new Error(`Unknown notification type: ${type}`);
  return fn(payload || {});
}

// ─── Raw LINE Messaging API call ────────────────────────────────────
// Returns { ok, status, error, requestId } — never throws.
async function callLinePush({ to, messages }) {
  const token = process.env.LINE_CHANNEL_ACCESS_TOKEN;
  if (!token) {
    return { ok: false, status: 500, error: "LINE_CHANNEL_ACCESS_TOKEN not set", requestId: null };
  }
  let res;
  try {
    res = await fetch("https://api.line.me/v2/bot/message/push", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
      },
      body: JSON.stringify({ to, messages }),
    });
  } catch (e) {
    return { ok: false, status: 0, error: `fetch failed: ${e.message}`, requestId: null };
  }
  const requestId = res.headers.get("x-line-request-id") || null;
  if (!res.ok) {
    let body = "";
    try { body = await res.text(); } catch (_) { /* ignore */ }
    return { ok: false, status: res.status, error: body || res.statusText, requestId };
  }
  return { ok: true, status: res.status, error: null, requestId };
}

// ─── sendAndLog: idempotent send + audit log ────────────────────────
// Single source of truth used by BOTH endpoints. Returns one of:
//   { ok:true,  status:"success" }
//   { ok:true,  status:"skipped",   reason:"no_user_id" | "duplicate" }
//   { ok:false, status:"failed",    error }
//
// Audit doc lives at notification_logs/{eventId}. eventId MUST be unique
// per (booking, type, target) — the caller is responsible for choosing it.
export async function sendAndLog({
  eventId,
  type,
  targetType,    // "customer" | "admin"
  lineUserId,
  bookingCode,
  payload,       // raw fields used to build the message
}) {
  const db = getDb();
  const logRef = db.collection("notification_logs").doc(eventId);

  // Guard 1: missing or guest lineUserId — never call LINE, log skipped.
  if (!lineUserId || lineUserId === "guest") {
    await logRef.set({
      eventId, type, targetType,
      lineUserId: lineUserId || null,
      bookingCode: bookingCode || null,
      status: "skipped",
      messagePreview: null,
      lineApiStatus: null,
      lineApiError: "missing or guest lineUserId",
      requestId: null,
      createdAt: FieldValue.serverTimestamp(),
    }, { merge: true });
    return { ok: true, status: "skipped", reason: "no_user_id" };
  }

  // Guard 2: idempotency — if we've already SUCCEEDED on this eventId, skip.
  // Failed attempts are allowed to retry (overwrite the failed log).
  const prev = await logRef.get();
  if (prev.exists && prev.data().status === "success") {
    return { ok: true, status: "skipped", reason: "duplicate" };
  }

  // Build message body.
  let messages;
  try { messages = buildMessages(type, payload); }
  catch (e) {
    await logRef.set({
      eventId, type, targetType, lineUserId,
      bookingCode: bookingCode || null,
      status: "failed",
      messagePreview: null,
      lineApiStatus: null,
      lineApiError: `template error: ${e.message}`,
      requestId: null,
      createdAt: FieldValue.serverTimestamp(),
    }, { merge: true });
    return { ok: false, status: "failed", error: e.message };
  }

  const preview = (messages[0] && typeof messages[0].text === "string")
    ? messages[0].text.slice(0, 120)
    : null;

  const result = await callLinePush({ to: lineUserId, messages });

  await logRef.set({
    eventId, type, targetType, lineUserId,
    bookingCode: bookingCode || null,
    status: result.ok ? "success" : "failed",
    messagePreview: preview,
    lineApiStatus: result.status,
    lineApiError: result.error,
    requestId: result.requestId,
    createdAt: FieldValue.serverTimestamp(),
  }, { merge: true });

  return result.ok
    ? { ok: true, status: "success" }
    : { ok: false, status: "failed", error: result.error };
}

// ─── Notification feature flags ─────────────────────────────────────
// system_settings/notification_flags — additive, default-safe: a missing
// doc, missing field, or read error always means "current behavior".
// Known flags:
//   suppressNewBookingAdmin: true → skip new_booking_admin broadcasts
//   slipVerifyNotifications: false → /api/slip-verify sends no admin push
//   notifyAdminOnCustomerPendingCancel: true → cancel_pending notifies admins
//     (default OFF: every cancel_pending is by definition an unpaid no-slip
//      booking, so notifying by default would spam admins with non-actionable
//      messages — especially when suppressNewBookingAdmin hid the create too)
export async function loadNotificationFlags() {
  try {
    const snap = await getDb()
      .collection("system_settings").doc("notification_flags").get();
    return snap.exists ? (snap.data() || {}) : {};
  } catch (e) {
    console.error("[notify] flags read failed (fail-open):", e.message);
    return {};
  }
}

// ─── Load registered admin notify recipients ────────────────────────
// Filters to enabled === true and drops any docs without a lineUserId.
export async function loadActiveAdmins() {
  const db = getDb();
  const snap = await db
    .collection("admin_notify_users")
    .where("enabled", "==", true)
    .get();
  return snap.docs
    .map(d => ({ id: d.id, ...d.data() }))
    .filter(u => typeof u.lineUserId === "string" && u.lineUserId.length > 0);
}
