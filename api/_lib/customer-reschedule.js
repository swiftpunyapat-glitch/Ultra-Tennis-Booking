import { readPricing } from './store-settings.js';
import { courtQuote } from './court-quote.js';
import { bookingFinancials, resourceError, storeConfig, money } from '../../commerce.js';

export const isPending = b => b?.bookingStatus === 'pending_reschedule' || (b?.bookingStatus === 'rescheduled' && b?.pendingReschedule === true);
export function pendingMinutes(b) {
  return Number(b.durationMinutes) || Number(b.durationHours) * 60 || 60;
}
const courtPasses = new Set(['ultra_starter_3', 'ultra_pass_10', 'ultra_pass_20', 'ultra_10', 'ultra_20']);
export function pendingRestriction(b, uid) {
  if (!uid || b.lineUserId !== uid) return 'ไม่พบสิทธิ์การจองของคุณ';
  if (!isPending(b) || b.isTest || b.testSessionId || b.deletedAt) return 'รายการนี้ไม่ได้อยู่ระหว่างรอเลื่อนแล้ว กรุณาโหลดใหม่';
  if (b.refundStatus || Number(b.refundAmount) > 0 || Number(b.refundedAmount) > 0 || b.packageMinutesRestored || b.packageRestoredAt) return 'รายการนี้มีการคืนเงินหรือคืนสิทธิ์แล้ว กรุณาติดต่อแอดมิน';
  if (b.coachId || b.coachAddonSchemaVersion || b.coachBookingId || b.requiresCoachOrAdminBooking || b.isEventBooking || /coach|lesson|event|off.?peak/i.test(`${b.bookingType || ''} ${b.packageType || ''} ${b.usedPackageType || ''}`)) return 'คาบโค้ชและ Pass เงื่อนไขพิเศษ กรุณาให้แอดมินจัดเวลา';
  if (!['paid', 'package'].includes(b.paymentStatus)) return 'กรุณาให้แอดมินตรวจสอบการชำระเงินก่อนเลือกเวลา';
  if (b.paymentStatus === 'package' && (!courtPasses.has(b.packageType || b.usedPackageType) || !(b.usedPackageId || b.packageId))) return 'กรุณาให้แอดมินตรวจสอบสิทธิ์ Pass นี้';
  if (!Number.isInteger(pendingMinutes(b)) || pendingMinutes(b) < 30 || pendingMinutes(b) > 360 || pendingMinutes(b) % 30) return 'กรุณาให้แอดมินตรวจสอบชั่วโมงของรายการนี้';
  return null;
}

export function validPendingDate(date) {
  return typeof date === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(date) && Number.isFinite(Date.parse(date)) && new Date(date).toISOString().slice(0, 10) === date;
}
export function targetSegments(b, time, duration) {
  if (!/^([01]\d|2[0-3]):(00|30)$/.test(time || '') || !Number.isInteger(duration) || duration < 30 || duration % 30 || duration > pendingMinutes(b)) return null;
  let start = Number(time.slice(0, 2)) * 60 + Number(time.slice(3));
  if (start + duration > 1440 || (b.source !== 'admin_manual' && duration % 60 === 0 && start % 60)) return null;
  const segments = [];
  for (let remaining = duration; remaining; ) {
    const span = start % 60 ? 30 : Math.min(60, remaining);
    segments.push({ start: `${String(Math.floor(start / 60)).padStart(2, '0')}:${String(start % 60).padStart(2, '0')}`, span });
    start += span; remaining -= span;
  }
  return segments;
}

// Called inside the same transaction that takes the slot. Reads only; no
// second payment, voucher redemption or package deduction is ever performed.
export async function readPendingPolicy(db, b, date, transaction = null) {
  const get = ref => transaction ? transaction.get(ref) : ref.get();
  const pricing = await readPricing(db, transaction);
  const features = await get(db.collection('system_settings').doc('features'));
  const holiday = await get(db.collection('holidays').doc(date));
  const packageId = b.usedPackageId || b.packageId;
  const pkg = packageId && b.paymentStatus === 'package' ? await get(db.collection('customer_packages').doc(packageId)) : null;
  const receipt = await get(db.collection('finance_document_counters').doc(`active_receipt_booking_${encodeURIComponent(b.id)}`));
  return { pricing, halfHoursEnabled: !features.exists || features.data().enableHalfHourBooking !== false, isHoliday: holiday.exists && holiday.data().isHoliday === true, pkg: pkg?.exists ? pkg.data() : null, hasReceipt: receipt.exists };
}

export function pendingTargetError(b, uid, date, time, duration, policy, now = Date.now()) {
  const restriction = pendingRestriction(b, uid);
  if (restriction) return restriction;
  const segments = targetSegments(b, time, duration);
  const start = Date.parse(`${date}T${time}:00+07:00`);
  if (!validPendingDate(date) || !segments || !Number.isFinite(start) || start <= now) return 'กรุณาเลือกวันและเวลาในอนาคตที่ถูกต้อง';
  if (policy.halfHoursEnabled === false && (duration % 60 || time.endsWith(':30'))) return 'ขณะนี้เปิดให้เลือกเวลาเต็มชั่วโมงเท่านั้น';
  const resourceId = b.resourceId || 'room1';
  const resourceIssue = resourceError(storeConfig(policy.pricing), resourceId, time, duration);
  if (resourceIssue) return resourceIssue;
  if (duration < pendingMinutes(b) && policy.hasReceipt) return 'รายการนี้ออกใบเสร็จแล้ว กรุณาใช้เต็มจำนวนหรือให้แอดมินจัดการแบ่งชั่วโมง';
  if (b.paymentStatus === 'package') {
    const p = policy.pkg;
    if (!p || p.lineUserId !== uid || p.status !== 'active' || !courtPasses.has(p.packageType)) return 'Pass นี้ไม่พร้อมใช้งาน กรุณาติดต่อแอดมิน';
    const expiry = p.validUntil?.toMillis?.();
    const validFrom = p.validFrom?.toMillis?.() || 0;
    if (!expiry || start < validFrom || start + duration * 60000 > expiry) return 'วันใหม่ต้องอยู่ภายในอายุ Pass เดิม';
  } else {
    const original = bookingFinancials(b);
    // Retain an existing coupon, but do not turn a discounted time band into
    // a more expensive entitlement. A cheaper time never creates a refund.
    const allowance = money((original.total + original.couponDiscount) * duration / pendingMinutes(b));
    let quote;
    try { ({ quote } = courtQuote({ segments, date, startTime: time, durationMinutes: duration, resourceId, pricing: policy.pricing, isHoliday: policy.isHoliday, nowMs: now, lineUserId: uid })); }
    catch { return 'ช่วงเวลานี้มีเงื่อนไขราคา กรุณาติดต่อแอดมิน'; }
    if (quote.finalPrice > allowance + 0.001) return 'ช่วงเวลานี้ราคาสูงกว่าสิทธิ์เดิม กรุณาเลือกเวลาอื่นหรือติดต่อแอดมิน';
  }
  return null;
}

export async function pendingOptions(db, booking, uid, date, duration) {
  const policy = await readPendingPolicy(db, booking, date);
  const resourceId = booking.resourceId || 'room1';
  const [availability, slots] = await Promise.all([
    db.collection('available_slots').where('date', '==', date).get(),
    db.collection('booking_slots').where('date', '==', date).get(),
  ]);
  const open = new Set(availability.docs.filter(d => d.data().resourceId === resourceId && d.data().status === 'open' && !d.data().testSessionId && !d.data().isTest).map(d => d.id));
  const occupied = new Set();
  for (const doc of slots.docs) {
    const s = doc.data();
    if (s.resourceId !== resourceId || !(['confirmed', 'pending_review'].includes(s.bookingStatus) || (s.bookingStatus === 'pending_payment' && (s.expiresAt?.toMillis?.() || 0) > Date.now()))) continue;
    const hour = s.hour || s.startTime;
    if (!hour) continue;
    const start = Number(hour.slice(0, 2)) * 60 + Number(hour.slice(3));
    for (let m = start; m < start + (s.slotSpanMinutes === 30 ? 30 : 60); m += 30) occupied.add(m);
  }
  const times = [];
  for (let m = 0; m + duration <= 1440; m += 30) {
    const time = `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`;
    if (pendingTargetError(booking, uid, date, time, duration, policy)) continue;
    let available = true;
    for (let c = m; c < m + duration; c += 30) if (occupied.has(c) || !open.has(`${resourceId}_${date}_${String(Math.floor(c / 60)).padStart(2, '0')}00`)) available = false;
    if (available) times.push(time);
  }
  return { times, fullDurationOnly: policy.hasReceipt };
}
