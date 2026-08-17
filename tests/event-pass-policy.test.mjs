import { describe, expect, test } from 'vitest';
import { eventPassBookingError, isEventPassPackageType } from '../api/_lib/event-pass-policy.js';
import { readFileSync } from 'node:fs';

const adminAccounting = readFileSync(new URL('../api/admin-edit-booking-accounting.js', import.meta.url), 'utf8');
const adminHtml = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');
const customerHtml = readFileSync(new URL('../index.html', import.meta.url), 'utf8');

const validUntil = Date.parse('2026-09-24T23:59:59+07:00');
const pkg = {
  status: 'active', remainingMinutes: 60,
  branchId: 'ladprao1', resourceId: 'room1', validUntil,
};
const context = (extra = {}) => ({
  pkg, dateISO: '2026-09-24', startTime: '20:00', durationMinutes: 60,
  isHoliday: false, nowMs: Date.parse('2026-08-24T00:00:00+07:00'),
  ...extra,
});

describe('Event Pass booking policy', () => {
  test('accepts exactly one weekday hour on or before 24 September 2026', () => {
    expect(eventPassBookingError(context())).toBeNull();
  });

  test.each([
    [{ durationMinutes: 120 }, 'PASS_EVENT_ONE_HOUR'],
    [{ dateISO: '2026-09-20' }, 'PASS_WEEKDAY_ONLY'],
    [{ isHoliday: true }, 'PASS_NO_HOLIDAY'],
    [{ dateISO: '2026-09-25' }, 'PASS_BOOKING_AFTER_EXPIRY'],
    [{ pkg: { ...pkg, remainingMinutes: 0 } }, 'PASS_INSUFFICIENT'],
  ])('rejects restricted case %#', (override, error) => {
    expect(eventPassBookingError(context(override))).toBe(error);
  });

  test('recognizes only the approved Event Pass package type', () => {
    expect(isEventPassPackageType('monstr_event_pass')).toBe(true);
    expect(isEventPassPackageType('ultra_pass_10')).toBe(false);
  });

  test('forfeits the pass on cancellation and blocks reschedule in UI and server', () => {
    expect(adminAccounting).toContain("if (type === 'monstr_event_pass') return null");
    expect(adminAccounting).toContain('Event Pass bookings cannot be rescheduled');
    expect(adminHtml).toContain('const canResch=!isEventPassBooking');
    expect(customerHtml).toContain('Cancel แล้วไม่คืน Pass');
  });

  test('ships the locked MONSTR dates and the customer/admin redemption surfaces', () => {
    expect(adminHtml).toContain('2026-08-24');
    expect(adminHtml).toContain('2026-09-24');
    expect(adminHtml).toContain('Event Pass Approval');
    expect(adminHtml).toContain('voucher_import_codes');
    expect(customerHtml).toContain('Redeem Event Code');
    expect(customerHtml).toContain('event_pass_redeem');
  });
});
