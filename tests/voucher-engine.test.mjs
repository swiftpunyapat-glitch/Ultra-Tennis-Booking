import { describe, expect, test } from 'vitest';
import {
  applyVoucherToQuote,
  evaluateVoucher,
  redeemVoucherUpdate,
  releaseVoucherUpdate,
  reserveVoucherUpdate,
} from '../api/_lib/voucher-engine.js';

const nowMs = Date.parse('2026-08-12T10:00:00+07:00');
const baseQuote = {
  pricingType: 'morning_weekday', originalPrice: 330, finalPrice: 330,
  price: 330, amount: 330, qrAmount: 330, qrType: 'normal',
};
const campaign = {
  id: 'monstr-2026', schemaVersion: 2, campaignId: 'monstr-2026',
  name: 'MONSTR Sponsor', keyword: 'MONSTR', codePrefix: 'MSTR-',
  active: true, voucherType: 'free_booking',
  allowedDays: [1, 2, 3, 4, 5], startTime: '06:00', endTime: '24:00',
  excludeHolidays: true, exactDurationMinutes: 60,
  requiresLineLogin: true, maxUsesPerCode: 1, maxCancellationRestores: 2,
};
const voucher = {
  schemaVersion: 2, campaignId: 'monstr-2026', active: true,
  state: 'available', usedCount: 0, maxUses: 1,
};

const evaluate = (extra = {}) => evaluateVoucher({
  voucher, campaign, code: 'MSTR-ABCDE', nowMs, lineUserId: 'U123',
  date: '2026-08-12', startTime: '13:30', durationMinutes: 60,
  isHoliday: false, branchId: 'ladprao1', resourceId: 'room1', baseQuote,
  ...extra,
});

describe('campaign voucher engine', () => {
  test('free-booking campaigns can replace any eligible base rate', () => {
    const result = evaluate();
    expect(result).toMatchObject({
      ok: true, voucherType: 'free_booking', isFree: true,
      finalPrice: 0, discountAmount: 330, campaignId: 'monstr-2026', keyword: 'MONSTR',
    });
    expect(applyVoucherToQuote(baseQuote, result)).toMatchObject({
      finalPrice: 0, qrAmount: 0, voucherApplied: true,
      voucherCode: 'MSTR-ABCDE', voucherLifecycle: 'v2_state', isFreeVoucher: true,
    });
  });

  test.each([
    [{ date: '2026-08-15' }, 'day_not_allowed'],
    [{ isHoliday: true }, 'holiday_not_allowed'],
    [{ startTime: '05:00' }, 'time_not_allowed'],
    [{ durationMinutes: 120 }, 'duration_not_allowed'],
    [{ lineUserId: 'guest' }, 'line_login_required'],
  ])('enforces campaign eligibility %#', (override, reason) => {
    expect(evaluate(override)).toEqual({ ok: false, reason });
  });

  test('a campaign keyword without an exact voucher never grants value', () => {
    expect(evaluate({ voucher: null, code: 'MONSTR' })).toEqual({ ok: false, reason: 'not_found' });
  });

  test('a campaign reference fails closed when its campaign is missing', () => {
    expect(evaluate({ campaign: null })).toEqual({ ok: false, reason: 'campaign_not_found' });
  });

  test('supports fixed and percentage discount campaigns separately', () => {
    const fixed = evaluate({
      campaign: { ...campaign, voucherType: 'discount_amount', discountAmount: 80, allowedPricingTypes: ['morning_weekday'] },
    });
    expect(fixed).toMatchObject({ ok: true, finalPrice: 250, discountAmount: 80, isFree: false });

    const percent = evaluate({
      campaign: { ...campaign, voucherType: 'discount_percent', discountPercent: 25, maxDiscountAmount: 70 },
    });
    expect(percent).toMatchObject({ ok: true, finalPrice: 260, discountAmount: 70, isFree: false });
  });

  test('reserves discount vouchers, redeems on payment, and releases unpaid holds', () => {
    const timestamp = { server: true };
    expect(reserveVoucherUpdate(voucher, {
      bookingId: 'B1', bookingCode: 'UT1', lineUserId: 'U123', reservedUntil: 123, timestamp,
    })).toMatchObject({ state: 'reserved', reservedBookingId: 'B1', reservedUntil: 123 });
    expect(redeemVoucherUpdate({ ...voucher, state: 'reserved' }, {
      bookingId: 'B1', bookingCode: 'UT1', lineUserId: 'U123', timestamp,
    })).toMatchObject({ state: 'redeemed', usedCount: 1, redeemedBookingId: 'B1', reservedBookingId: null });
    expect(releaseVoucherUpdate({ ...voucher, state: 'reserved' }, {
      bookingId: 'B1', reason: 'unpaid_cancel', timestamp, countRestore: false,
    })).toMatchObject({ restored: true, update: { state: 'available', usedCount: 0 } });
  });

  test('limits free-voucher cancellation restores per code', () => {
    const redeemed = { ...voucher, state: 'redeemed', usedCount: 1, maxCancellationRestores: 2 };
    expect(releaseVoucherUpdate({ ...redeemed, cancellationRestoreCount: 1 }, {
      bookingId: 'B1', timestamp: 1, countRestore: true,
    })).toMatchObject({ restored: true, update: { state: 'available', usedCount: 0, cancellationRestoreCount: 2 } });
    expect(releaseVoucherUpdate({ ...redeemed, cancellationRestoreCount: 2 }, {
      bookingId: 'B2', timestamp: 2, countRestore: true,
    })).toMatchObject({ restored: false });
  });

  test('keeps legacy standard amount vouchers compatible', () => {
    const legacy = evaluateVoucher({
      voucher: { active: true, usedCount: 0, maxUses: 1, allowedBasePrice: 350, discountAmount: 50 },
      code: 'LEGACY50', nowMs, lineUserId: 'U123', date: '2026-08-15', startTime: '13:00', durationMinutes: 60,
      baseQuote: { ...baseQuote, pricingType: 'standard', originalPrice: 390, finalPrice: 390 },
    });
    expect(legacy).toMatchObject({ ok: true, lifecycleMode: 'legacy_used_count', finalPrice: 340, discountAmount: 50 });
  });
});
