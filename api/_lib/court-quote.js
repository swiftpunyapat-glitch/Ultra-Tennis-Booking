import { computeQuote } from './pricing.js';
import { applyRateRule, applyPromotion, storeConfig, money, minutes } from '../../commerce.js';

export function courtQuote({ segments, date, startTime, durationMinutes, resourceId = 'room1', pricing, isHoliday = false, nowMs = Date.now(), lineUserId }) {
  const config = storeConfig(pricing);
  const hasHalf = segments.some(s => s.span === 30);
  // Keep Ultra's legacy half-hour promotion restriction while retaining its
  // configured base prices. Explicit new rules can price these durations.
  const promoConfig = hasHalf ? { ...pricing, specialPromoActive: false } : pricing;
  const halfPrice = Number(pricing?.halfHourPrice) || 200;
  const segQuotes = segments.map(segment => {
    const ctx = { date, startTime: segment.start, durationMinutes: segment.span, resourceId, isHoliday };
    const base = segment.span === 30 ? {
      price: halfPrice, amount: halfPrice, originalPrice: halfPrice, finalPrice: halfPrice, qrAmount: halfPrice,
      qrType: 'normal', pricingType: 'half_hour', discountAmount: 0, voucherApplied: false, voucherCode: null,
      promoCode: null, priceRuleVersion: 'half-hour-flat-v1', isHoliday,
    } : computeQuote({ ...ctx, nowMs, promoConfig, lineUserId });
    return { ...applyRateRule(base, config, ctx), startTime: segment.start, span: segment.span };
  });
  if (new Set(segQuotes.map(q => q.qrType === 'special' ? 'alt' : 'main')).size > 1) {
    throw Object.assign(new Error('MIXED_RECEIVER'), { code: 'MIXED_RECEIVER' });
  }
  const subtotal = money(segQuotes.reduce((sum, q) => sum + q.finalPrice, 0));
  const same = field => segQuotes.every(q => q[field] === segQuotes[0][field]);
  const quote = applyPromotion({ ...segQuotes[0], originalPrice: subtotal, finalPrice: subtotal, price: subtotal, amount: subtotal, qrAmount: subtotal,
    pricingType: same('pricingType') ? segQuotes[0].pricingType : 'multi_rate', qrType: same('qrType') ? segQuotes[0].qrType : 'normal',
    breakdown: segQuotes.map(q => ({ startTime: q.startTime, span: q.span,
      endTime: minutes(q.startTime) + q.span === 1440 ? '00:00' : `${String(Math.floor((minutes(q.startTime) + q.span) / 60)).padStart(2, '0')}:${String((minutes(q.startTime) + q.span) % 60).padStart(2, '0')}`,
      price: q.finalPrice, pricingType: q.pricingType, qrType: q.qrType, rateRuleId: q.rateRuleId || null, rateRuleName: q.rateRuleName || null })),
  }, config, { date, startTime, durationMinutes, resourceId, isHoliday });
  return { quote, segQuotes };
}
