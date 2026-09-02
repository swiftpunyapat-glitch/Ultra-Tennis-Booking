import { describe, expect, test } from 'vitest';
import {
  calculateCoachAddonV2Price,
  coachAddonV2PackageKind,
  coachClaimCellStarts,
  initialCoachAddonV2States,
  isCoachAddonV2Duration,
} from '../api/_lib/coach-addon-v2.js';

const base = overrides => ({
  durationMinutes: 60,
  fundingMode: 'cash',
  courtGrossAmount: 350,
  lessonRatePerHour: 900,
  coachPayoutRatePerHour: 550,
  studentCount: 1,
  ...overrides,
});

describe('Coach Add-on v2 pricing', () => {
  test('cash court + coach is one cash total with frozen components', () => {
    expect(calculateCoachAddonV2Price(base())).toMatchObject({
      serviceCategory: 'coach_lesson', fundingSource: 'cash',
      courtCashAmount: 350, courtPackageMinutes: 0,
      lessonGrossAmount: 900, coachChargeAmount: 550, coachBasePayoutAmount: 550,
      extraPersonFee: 0, extraPersonCoachPayout: 0,
      coachPayoutAmount: 550, cashDueAmount: 900, cashPaidAmount: 0,
    });
  });

  test('Ultra Pass reserves court minutes and charges only coach cash', () => {
    expect(calculateCoachAddonV2Price(base({ fundingMode: 'ultra_pass', durationMinutes: 90, courtGrossAmount: 520 }))).toMatchObject({
      fundingSource: 'mixed', courtCashAmount: 0, courtPackageMinutes: 90,
      lessonGrossAmount: 1350, coachChargeAmount: 830, coachBasePayoutAmount: 825,
      coachPayoutAmount: 825, cashDueAmount: 830,
    });
  });

  test('Ultra Pass + coach + extra person sends all ฿100 to coach payout', () => {
    expect(calculateCoachAddonV2Price(base({ fundingMode: 'ultra_pass', studentCount: 2 }))).toMatchObject({
      courtPackageMinutes: 60, coachChargeAmount: 550,
      extraPersonFee: 100, extraPersonCoachPayout: 100,
      coachBasePayoutAmount: 550, coachPayoutAmount: 650,
      cashDueAmount: 650,
    });
  });

  test('Beginner Coaching covers court and coach without reinterpreting its entitlement', () => {
    const price = calculateCoachAddonV2Price(base({ fundingMode: 'coaching_package', durationMinutes: 120 }));
    expect(price).toMatchObject({
      fundingSource: 'coaching_package', courtCashAmount: 0, courtPackageMinutes: 120,
      coachChargeAmount: 0, coachBasePayoutAmount: 1100,
      coachPayoutAmount: 1100, cashDueAmount: 0,
    });
    expect(initialCoachAddonV2States(price)).toMatchObject({
      bookingState: 'confirmed', cashState: 'not_required', packageUsageState: 'consumed',
    });
  });

  test('Beginner Coaching charges only the second-person fee and assigns it to coach payout', () => {
    const price = calculateCoachAddonV2Price(base({ fundingMode: 'coaching_package', durationMinutes: 150, studentCount: 2 }));
    expect(price).toMatchObject({
      fundingSource: 'mixed', coachChargeAmount: 0,
      extraPersonFee: 100, extraPersonCoachPayout: 100,
      coachBasePayoutAmount: 1375, coachPayoutAmount: 1475,
      cashDueAmount: 100,
    });
    expect(initialCoachAddonV2States(price)).toMatchObject({
      bookingState: 'held', cashState: 'unpaid', packageUsageState: 'reserved',
    });
  });

  test('rates are proportional at every supported duration', () => {
    const expected = new Map([[60, 550], [90, 825], [120, 1100], [150, 1375], [180, 1650]]);
    for (const [durationMinutes, coachChargeAmount] of expected) {
      expect(calculateCoachAddonV2Price(base({
        durationMinutes,
        courtGrossAmount: 350 * (durationMinutes / 60),
      })).coachChargeAmount).toBe(coachChargeAmount);
    }
  });

  test('30 minutes and unsupported increments fail closed', () => {
    for (const value of [30, 45, 181, 210]) expect(isCoachAddonV2Duration(value)).toBe(false);
    expect(() => calculateCoachAddonV2Price(base({ durationMinutes: 30 }))).toThrow('INVALID_DURATION');
  });
});

describe('Coach Add-on v2 package and claim vocabulary', () => {
  test('coach_at_ultra_10 remains explicitly unsupported', () => {
    expect(coachAddonV2PackageKind('coach_at_ultra_10')).toBeNull();
    expect(coachAddonV2PackageKind('beginner_coaching_5')).toBe('coaching_package');
    expect(coachAddonV2PackageKind('ultra_pass_10')).toBe('ultra_pass');
  });

  test('multi-slot coach claims use deterministic 30-minute cells', () => {
    expect(coachClaimCellStarts('10:00', 150)).toEqual(['10:00', '10:30', '11:00', '11:30', '12:00']);
    expect(coachClaimCellStarts('10:30', 60)).toBeNull();
  });
});
