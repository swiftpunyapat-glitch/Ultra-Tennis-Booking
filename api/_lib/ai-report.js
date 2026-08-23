import { createHash, randomBytes } from 'node:crypto';

const TOKEN_RE = /^[A-Za-z0-9_-]{40,128}$/;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const MONTH_RE = /^\d{4}-\d{2}$/;
const MAX_RANGE_DAYS = 366;
const DAY_MS = 24 * 60 * 60 * 1000;

export function createAiReportToken() {
  return randomBytes(32).toString('base64url');
}

export function normalizeAiReportToken(value) {
  const token = String(value || '').trim();
  return TOKEN_RE.test(token) ? token : null;
}

export function hashAiReportToken(value) {
  const token = normalizeAiReportToken(value);
  return token ? createHash('sha256').update(token).digest('hex') : null;
}

export function normalizeAiReportAccessInput(input = {}) {
  const label = String(input.label || '').trim().slice(0, 80);
  const expiresDays = Number(input.expiresDays ?? 365);
  if (label.length < 2) return { ok: false, error: 'Link label is required' };
  if (!Number.isInteger(expiresDays) || expiresDays < 1 || expiresDays > 3650) {
    return { ok: false, error: 'Expiry must be between 1 and 3,650 days' };
  }
  return { ok: true, label, expiresDays };
}

function validDate(value) {
  if (!DATE_RE.test(String(value || ''))) return false;
  const date = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value;
}

function lastDayOfMonth(month) {
  const [year, value] = month.split('-').map(Number);
  return new Date(Date.UTC(year, value, 0)).toISOString().slice(0, 10);
}

function bangkokMonth(nowMs) {
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en', {
    timeZone: 'Asia/Bangkok', year: 'numeric', month: '2-digit',
  }).formatToParts(new Date(nowMs)).map(part => [part.type, part.value]));
  return `${parts.year}-${parts.month}`;
}

export function resolveAiReportRange(query = {}, nowMs = Date.now()) {
  const month = String(query.month || '').trim();
  const from = String(query.from || '').trim();
  const to = String(query.to || '').trim();

  if (month && (from || to)) return { ok: false, error: 'Use month or from/to, not both' };
  if (month) {
    if (!MONTH_RE.test(month)) return { ok: false, error: 'month must be YYYY-MM' };
    return { ok: true, mode: 'month', month, from: `${month}-01`, to: lastDayOfMonth(month) };
  }
  if (from || to) {
    if (!validDate(from) || !validDate(to)) return { ok: false, error: 'from and to must be valid YYYY-MM-DD dates' };
    const fromMs = Date.parse(`${from}T00:00:00Z`);
    const toMs = Date.parse(`${to}T00:00:00Z`);
    if (toMs < fromMs) return { ok: false, error: 'to must not be earlier than from' };
    if (Math.floor((toMs - fromMs) / DAY_MS) + 1 > MAX_RANGE_DAYS) {
      return { ok: false, error: `Date range cannot exceed ${MAX_RANGE_DAYS} days; request additional years separately` };
    }
    return { ok: true, mode: 'range', month: null, from, to };
  }
  const current = bangkokMonth(nowMs);
  return { ok: true, mode: 'month', month: current, from: `${current}-01`, to: lastDayOfMonth(current) };
}

function minutesOfBooking(booking) {
  const explicit = Number(booking.durationMinutes);
  if (Number.isFinite(explicit) && explicit > 0) return explicit;
  const hours = Number(booking.durationHours);
  if (Number.isFinite(hours) && hours > 0) return Math.round(hours * 60);
  const toMinutes = value => {
    const match = String(value || '').match(/^(\d{2}):(\d{2})$/);
    return match ? Number(match[1]) * 60 + Number(match[2]) : null;
  };
  const start = toMinutes(booking.startTime), end = toMinutes(booking.endTime);
  return start !== null && end !== null && end > start ? end - start : 60;
}

function increment(target, key, amount = 1) {
  const value = String(key || 'unknown');
  target[value] = (target[value] || 0) + amount;
}

function bookingCategory(booking) {
  const type = String(booking.packageType || booking.usedPackageType || '');
  if (booking.isEventBooking === true || type === 'monstr_event_pass') return 'event_pass';
  if (booking.voucherCode) return 'voucher';
  if (booking.paymentStatus === 'package' || booking.packageId || booking.usedPackageId) return 'package';
  if (booking.coachId || booking.serviceType === 'coach') return 'coaching';
  return 'single_use';
}

export function sanitizeAiBooking(id, booking = {}) {
  return {
    id,
    bookingCode: booking.bookingCode || null,
    date: booking.date || null,
    startTime: booking.startTime || null,
    endTime: booking.endTime || null,
    durationMinutes: minutesOfBooking(booking),
    category: bookingCategory(booking),
    bookingType: booking.bookingType || null,
    serviceType: booking.serviceType || null,
    bookingStatus: booking.bookingStatus || booking.status || null,
    paymentStatus: booking.paymentStatus || null,
    price: Number(booking.price ?? booking.amount) || 0,
    pricingType: booking.pricingType || null,
    promoCode: booking.promoCode || null,
    voucherCode: booking.voucherCode || null,
    packageType: booking.packageType || booking.usedPackageType || null,
    isEventBooking: booking.isEventBooking === true,
    source: booking.source || booking.createdVia || null,
  };
}

export function buildAiBookingReport(records = [], range, { details = false, page = 1, limit = 200 } = {}) {
  const bookings = records
    .map(record => ({ ...record.data, id: record.id }))
    .filter(booking => validDate(booking.date) && booking.date >= range.from && booking.date <= range.to)
    .sort((a, b) => `${a.date} ${a.startTime || ''} ${a.id}`.localeCompare(`${b.date} ${b.startTime || ''} ${b.id}`));

  const metrics = {
    recordsTotal: bookings.length,
    bookingsTotal: 0,
    confirmedCount: 0,
    cancelledCount: 0,
    pendingPaymentCount: 0,
    pendingReviewCount: 0,
    pendingRescheduleCount: 0,
    paidBookingCount: 0,
    paidRevenue: 0,
    packageBookingCount: 0,
    packageUsageValue: 0,
    totalBookedMinutes: 0,
    totalBookedHours: 0,
  };
  const breakdown = {
    byMonth: {}, byDay: {}, byCategory: {}, byBookingStatus: {},
    byPaymentStatus: {}, byStartHour: {}, bySource: {},
  };

  for (const booking of bookings) {
    const status = booking.bookingStatus || booking.status || 'unknown';
    const payment = booking.paymentStatus || 'unknown';
    const cancelled = status === 'cancelled';
    increment(breakdown.byBookingStatus, status);
    increment(breakdown.byPaymentStatus, payment);
    increment(breakdown.byMonth, booking.date.slice(0, 7));
    increment(breakdown.byDay, booking.date);
    increment(breakdown.byCategory, bookingCategory(booking));
    increment(breakdown.byStartHour, String(booking.startTime || 'unknown').slice(0, 2));
    increment(breakdown.bySource, booking.source || booking.createdVia || 'unknown');
    if (cancelled) { metrics.cancelledCount++; continue; }

    metrics.bookingsTotal++;
    const minutes = minutesOfBooking(booking);
    metrics.totalBookedMinutes += minutes;
    if (status === 'confirmed') metrics.confirmedCount++;
    if (status === 'pending_payment') metrics.pendingPaymentCount++;
    if (payment === 'pending_review') metrics.pendingReviewCount++;
    if (status === 'pending_reschedule' || booking.pendingReschedule === true) metrics.pendingRescheduleCount++;
    if (payment === 'paid') {
      metrics.paidBookingCount++;
      metrics.paidRevenue += Number(booking.price ?? booking.amount) || 0;
    }
    if (payment === 'package') {
      metrics.packageBookingCount++;
      metrics.packageUsageValue += Number(booking.packageUsageValueTotal) || 0;
    }
  }
  metrics.paidRevenue = Math.round(metrics.paidRevenue * 100) / 100;
  metrics.packageUsageValue = Math.round(metrics.packageUsageValue * 100) / 100;
  metrics.totalBookedHours = Math.round((metrics.totalBookedMinutes / 60) * 100) / 100;

  const safePage = Number.isInteger(Number(page)) && Number(page) > 0 ? Number(page) : 1;
  const safeLimit = Number.isInteger(Number(limit)) ? Math.min(Math.max(Number(limit), 1), 500) : 200;
  const start = (safePage - 1) * safeLimit;
  const detailItems = details ? bookings.slice(start, start + safeLimit).map(item => sanitizeAiBooking(item.id, item)) : undefined;

  return {
    metrics,
    breakdown,
    ...(details ? {
      bookings: detailItems,
      pagination: {
        page: safePage, limit: safeLimit, total: bookings.length,
        nextPage: start + safeLimit < bookings.length ? safePage + 1 : null,
      },
    } : {}),
  };
}

export const AI_REPORT_MAX_RANGE_DAYS = MAX_RANGE_DAYS;
