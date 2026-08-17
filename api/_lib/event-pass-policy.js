const millis = value => value?.toMillis?.() ?? (value instanceof Date ? value.getTime() : Number(value) || null);

export function eventPassBookingError({
  pkg, dateISO, startTime, durationMinutes, isHoliday,
  nowMs = Date.now(), branchId = 'ladprao1', resourceId = 'room1',
}) {
  if (durationMinutes !== 60) return 'PASS_EVENT_ONE_HOUR';
  if (pkg?.status !== 'active') return 'PASS_INACTIVE';
  if (Number(pkg?.remainingMinutes) < 60) return 'PASS_INSUFFICIENT';
  if (pkg?.branchId && pkg.branchId !== branchId) return 'PASS_WRONG_BRANCH';
  if (pkg?.resourceId && pkg.resourceId !== resourceId) return 'PASS_WRONG_RESOURCE';
  const validUntil = millis(pkg?.validUntil);
  if (!validUntil || validUntil < nowMs) return 'PASS_EXPIRED';
  const [year, month, day] = String(dateISO || '').split('-').map(Number);
  const dow = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
  if (dow < 1 || dow > 5) return 'PASS_WEEKDAY_ONLY';
  if (isHoliday) return 'PASS_NO_HOLIDAY';
  const bookingEndMs = Date.parse(`${dateISO}T${startTime}:00+07:00`) + durationMinutes * 60_000;
  if (!Number.isFinite(bookingEndMs) || bookingEndMs > validUntil) return 'PASS_BOOKING_AFTER_EXPIRY';
  return null;
}

export function isEventPassPackageType(value) {
  return String(value || '') === 'monstr_event_pass';
}
