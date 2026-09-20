import { readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';
import { isUltraPassUsage, ultraPassLabel, ultraPassRate, ultraPassTier } from '../package-usage.js';

const adminHtml = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');
const financeHtml = readFileSync(new URL('../ultra-finance.html', import.meta.url), 'utf8');
const serverCatalog = readFileSync(new URL('../api/admin-user-action.js', import.meta.url), 'utf8');

const pkgBooking = extra => ({ paymentStatus: 'package', ...extra });

describe('every live shape of an Ultra Pass booking is recognised', () => {
  test.each([
    // What api/booking.js writes for a booking the customer makes with their
    // own pass — the shape the reports used to miss entirely.
    [{ packageType: 'ultra_pass_10', bookingType: 'Ultra Pass 10 Hours' }, 1, 310],
    [{ packageType: 'ultra_pass_20', bookingType: 'Ultra Pass 20 Hours' }, 2, 295],
    // What an accounting edit writes.
    [{ packageType: 'ultra_pass_10', bookingType: 'Ultra Pass 1' }, 1, 310],
    [{ packageType: 'ultra_pass_20', bookingType: 'Ultra Pass 2' }, 2, 295],
    // The pre-catalog aliases an accounting edit used to write.
    [{ packageType: 'ultra_10', bookingType: 'Ultra Pass 1' }, 1, 310],
    [{ packageType: 'ultra_20', bookingType: 'Ultra Pass 2' }, 2, 295],
    // Bookings from before packageType existed.
    [{ bookingType: 'Ultra Pass 1' }, 1, 310],
    [{ bookingType: 'Ultra Pass 2' }, 2, 295],
  ])('%o counts as tier %s at %s THB/hr', (booking, tier, rate) => {
    expect(isUltraPassUsage(pkgBooking(booking))).toBe(true);
    expect(ultraPassTier(booking)).toBe(tier);
    expect(ultraPassRate(booking)).toBe(rate);
    expect(ultraPassLabel(booking)).toBe(`Ultra Pass ${tier}`);
  });

  test('the customer-flow booking is what regressed — guard it explicitly', () => {
    // Before the shared module, the reports asked for packageType ultra_10 /
    // ultra_20 or bookingType "Ultra Pass 1" / "Ultra Pass 2". This booking
    // matches none of them, so it was absent from Package Usage Value.
    const booking = pkgBooking({ packageType: 'ultra_pass_10', bookingType: 'Ultra Pass 10 Hours' });
    expect(isUltraPassUsage(booking)).toBe(true);
    expect([...adminHtml.matchAll(/packageType\s*===?\s*"ultra_(10|20)"/g)]).toHaveLength(0);
    expect([...financeHtml.matchAll(/packageType\s*===?\s*"ultra_(10|20)"/g)]).toHaveLength(0);
  });
});

describe('non-Ultra-Pass bookings stay out of package usage', () => {
  test.each(['ultra_starter_3', 'beginner_coaching_5', 'coach_at_ultra_10', 'offpeak', 'monstr_event_pass'])(
    '%s has its own economics and is not counted at an Ultra Pass rate', packageType => {
      expect(ultraPassTier({ packageType })).toBeNull();
      expect(isUltraPassUsage(pkgBooking({ packageType }))).toBe(false);
      expect(ultraPassRate({ packageType })).toBe(0);
      expect(ultraPassLabel({ packageType })).toBe('');
    });

  test.each(['paid', 'unpaid', 'pending_review', 'rejected'])(
    'a %s booking is not package usage even on an Ultra Pass', paymentStatus => {
      expect(isUltraPassUsage({ paymentStatus, packageType: 'ultra_pass_10' })).toBe(false);
    });

  test.each([{}, { packageType: '' }, { packageType: 'unknown' }, { bookingType: 'Single Use' }, null, undefined])(
    'handles %o without throwing', booking => {
      expect(isUltraPassUsage(booking)).toBe(false);
      expect(ultraPassTier(booking)).toBeNull();
    });
});

describe('the reports and the pass catalog cannot drift apart again', () => {
  test('every Ultra Pass the server issues maps to a tier', () => {
    const catalog = serverCatalog.slice(serverCatalog.indexOf('const ACTIVE_PACKAGES = {'), serverCatalog.indexOf('const normalizePhone'));
    const ultraKeys = [...catalog.matchAll(/packageType:\s*"(ultra_pass_[^"]+)"/g)].map(m => m[1]);
    expect(ultraKeys).toEqual(['ultra_pass_10', 'ultra_pass_20']);
    for (const packageType of ultraKeys) expect(ultraPassTier({ packageType })).not.toBeNull();
  });

  test('both pages read the rule from the shared module instead of inlining it', () => {
    for (const html of [adminHtml, financeHtml]) {
      expect(html).toContain('from "./package-usage.js?v=20260920-pkgusage1"');
      expect(html).toContain('isUltraPassUsage');
      expect(html).not.toMatch(/bookingType\s*===?\s*"Ultra Pass [12]"/);
    }
  });
});
