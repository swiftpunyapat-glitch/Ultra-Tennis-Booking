import { describe, expect, test } from 'vitest';
import {
  computeQuote,
  DEFAULT_STORE_PRICING,
  PRICE_RULE_VERSION,
  resolveStorePricing,
} from '../api/_lib/pricing.js';

const standardInput = (extra = {}) => ({
  date: '2026-08-15', // Saturday: morning rule cannot interfere.
  startTime: '12:00',
  nowMs: Date.parse('2026-08-11T12:00:00+07:00'),
  payType: 'single',
  ...extra,
});

describe('dynamic store pricing foundation', () => {
  test('keeps the existing live rates as safe fallbacks', () => {
    expect(resolveStorePricing(null)).toEqual(DEFAULT_STORE_PRICING);
    expect(computeQuote(standardInput())).toMatchObject({
      pricingType: 'standard', originalPrice: 350, finalPrice: 350,
      priceRuleVersion: PRICE_RULE_VERSION,
    });
  });

  test('reads standard and late-night prices from the store config', () => {
    const promoConfig = { normalSingleUsePrice: 390, lateNightPrice: 475 };
    expect(computeQuote(standardInput({ promoConfig }))).toMatchObject({
      pricingType: 'standard', finalPrice: 390,
    });
    expect(computeQuote(standardInput({ startTime: '02:00', promoConfig }))).toMatchObject({
      pricingType: 'late_night', finalPrice: 475,
    });
  });

  test('reads both morning prices and the advance threshold from config', () => {
    const date = '2026-08-12'; // Wednesday
    const startTime = '09:00';
    const startMs = Date.parse(`${date}T${startTime}:00+07:00`);
    const promoConfig = {
      morningPrice: 340,
      morningAdvancePrice: 315,
      morningAdvanceHours: 24,
    };
    expect(computeQuote({ date, startTime, nowMs: startMs - 23 * 3600000, promoConfig })).toMatchObject({
      pricingType: 'morning_weekday', finalPrice: 340,
    });
    expect(computeQuote({ date, startTime, nowMs: startMs - 25 * 3600000, promoConfig })).toMatchObject({
      pricingType: 'morning_weekday_advance', finalPrice: 315,
    });
  });

  test('special promotion still outranks morning and uses its configured receiver', () => {
    const date = '2026-08-12';
    const startTime = '09:00';
    const nowMs = Date.parse('2026-08-11T09:00:00+07:00');
    const promoConfig = {
      normalSingleUsePrice: 390,
      morningPrice: 340,
      specialPromoActive: true,
      specialPromoName: 'Test Promo',
      specialPromoPrice: 325,
    };
    expect(computeQuote({ date, startTime, nowMs, promoConfig })).toMatchObject({
      pricingType: 'special_promotion', originalPrice: 325, finalPrice: 325,
      qrType: 'special', promoCode: 'Test Promo',
    });
  });

  test('legacy standard vouchers survive a standard-rate change', () => {
    const quote = computeQuote(standardInput({
      promoConfig: { normalSingleUsePrice: 390 },
      voucherCode: 'LEGACY50',
      voucher: { active: true, usedCount: 0, maxUses: 1, allowedBasePrice: 350, discountAmount: 50 },
      lineUserId: 'U1',
    }));
    expect(quote).toMatchObject({
      pricingType: 'standard', originalPrice: 390, finalPrice: 340,
      voucherApplied: true, voucherCode: 'LEGACY50', discountAmount: 50,
    });
  });

  test('an explicitly exact voucher remains pinned to its configured base price', () => {
    const quote = computeQuote(standardInput({
      promoConfig: { normalSingleUsePrice: 390 },
      voucherCode: 'EXACT350',
      voucher: { active: true, usedCount: 0, maxUses: 1, allowedBasePrice: 350, allowedBaseMode: 'exact', discountAmount: 50 },
      lineUserId: 'U1',
    }));
    expect(quote).toMatchObject({ voucherApplied: false, voucherReason: 'not_applicable', finalPrice: 390 });
  });

  test('invalid config values fall back instead of corrupting a quote', () => {
    expect(resolveStorePricing({
      normalSingleUsePrice: -1,
      morningPrice: 'bad',
      morningAdvanceHours: 9999,
    })).toEqual(DEFAULT_STORE_PRICING);
  });
});
