// Shared, deterministic store configuration and pricing rules. No credentials.
export const DEFAULT_COMMERCE = Object.freeze({
  version: 1, revision: 0, brandName: 'Ultra Tennis',
  resources: [{ id: 'room1', name: 'Ultra Tennis', active: true, openTime: '00:00', closeTime: '24:00' }],
  rateRules: [], promotions: [], paymentVerificationMode: 'manual',
});
const idPattern = /^[a-zA-Z0-9-]{1,40}$/;
const timePattern = /^(?:[01]\d|2[0-3]):[03]0$/;
const datePattern = /^\d{4}-\d{2}-\d{2}$/;
export const money = n => Math.round(Number(n || 0) * 100) / 100;
export const minutes = time => String(time).split(':').reduce((h, m) => Number(h) * 60 + Number(m));
export function storeConfig(pricing) {
  return { ...DEFAULT_COMMERCE, ...(pricing?.commerce || {}) };
}
// What an unauthenticated page is allowed to know. `features` has no session,
// so everything it returns is public — and rateRules and promotions are the
// shop's commercial plans, including ones dated to start weeks from now. A
// page never needs them: every price it shows comes from a server-computed
// quote, so publishing the rules only lets anyone read a promotion or a price
// change before it launches.
//
// Inactive courts are dropped rather than sent with a flag, because "do not
// show this" is not something a client should be trusted to honour. `active`
// stays on each row so existing callers that filter on it keep working.
export function publicStoreConfig(pricing) {
  const config = storeConfig(pricing);
  return {
    brandName: config.brandName,
    resources: (config.resources || [])
      .filter(r => r.active)
      .map(r => ({ id: r.id, name: r.name, active: true, openTime: r.openTime, closeTime: r.closeTime })),
  };
}
export function validateCommerce(input) {
  const fail = message => { throw new Error(message); };
  if (!input || typeof input !== 'object' || Array.isArray(input)) fail('Invalid store settings');
  if (input.paymentVerificationMode && input.paymentVerificationMode !== 'manual') fail('Automatic verification is not connected. Use manual mode.');
  const text = (v, max) => String(v || '').trim().slice(0, max);
  const amount = (v, label, min = 0, max = 100000) => {
    const n = Number(v); if (!Number.isFinite(n) || n < min || n > max || money(n) !== n) fail(`Invalid ${label}`); return n;
  };
  const unique = rows => { if (new Set(rows.map(r => r.id)).size !== rows.length) fail('IDs must be unique'); return rows; };
  const resources = unique((Array.isArray(input.resources) ? input.resources : []).map(r => {
    if (!idPattern.test(r.id) || !text(r.name, 80)) fail('Court requires a valid ID and name');
    if (!timePattern.test(r.openTime) || !(timePattern.test(r.closeTime) || r.closeTime === '24:00') || minutes(r.openTime) >= minutes(r.closeTime)) fail('Invalid court opening hours');
    return { id: r.id, name: text(r.name, 80), active: r.active !== false, openTime: r.openTime, closeTime: r.closeTime };
  }));
  if (!resources.length || resources.length > 20 || !resources.some(r => r.active)) fail('Keep at least one active court (maximum 20)');
  const parseRules = (list, promo) => unique((Array.isArray(list) ? list : []).map(r => {
    if (!idPattern.test(r.id) || !text(r.name, 100)) fail('Rule requires an ID and name');
    const resourceIds = Array.isArray(r.resourceIds) ? [...new Set(r.resourceIds)] : [];
    if (resourceIds.some(id => !resources.some(c => c.id === id))) fail('Rule references an unknown court');
    const days = Array.isArray(r.days) ? [...new Set(r.days.map(Number))] : [];
    if (days.some(d => !Number.isInteger(d) || d < 0 || d > 6)) fail('Invalid weekdays');
    const startTime = r.startTime || '00:00', endTime = r.endTime || '24:00';
    if (!timePattern.test(startTime) || !(timePattern.test(endTime) || endTime === '24:00') || minutes(startTime) >= minutes(endTime)) fail('Invalid rule time range');
    const startDate = r.startDate || '', endDate = r.endDate || '';
    if ((startDate && !datePattern.test(startDate)) || (endDate && !datePattern.test(endDate)) || (startDate && endDate && startDate > endDate)) fail('Invalid rule dates');
    const row = { id: r.id, name: text(r.name, 100), active: r.active !== false, resourceIds, days, startTime, endTime, startDate, endDate,
      excludeHolidays: r.excludeHolidays === true, priority: amount(r.priority || 0, 'priority', 0, 1000) };
    if (!Number.isInteger(row.priority)) fail('Priority must be an integer');
    if (!promo) return { ...row, hourlyPrice: amount(r.hourlyPrice, 'hourly price', 1), halfHourPrice: amount(r.halfHourPrice, 'half-hour price', 1) };
    if (!['amount', 'percent', 'fixed'].includes(r.type)) fail('Invalid promotion type');
    return { ...row, type: r.type, value: amount(r.value, 'promotion value', 0, r.type === 'percent' ? 100 : 100000),
      maxDiscount: amount(r.maxDiscount || 0, 'discount cap'), minSubtotal: amount(r.minSubtotal || 0, 'minimum subtotal'),
      minDuration: amount(r.minDuration || 30, 'minimum minutes', 30, 180), allowCoupon: r.allowCoupon === true };
  }));
  const rateRules = parseRules(input.rateRules, false), promotions = parseRules(input.promotions, true);
  if (rateRules.length > 100 || promotions.length > 100) fail('Maximum 100 rules per group');
  return { version: 1, brandName: text(input.brandName, 100) || 'Ultra Tennis', resources, rateRules, promotions, paymentVerificationMode: 'manual' };
}
export function resourceError(config, resourceId, startTime, durationMinutes) {
  const resource = config.resources.find(r => r.id === resourceId && r.active);
  if (!resource) return 'คอร์ตนี้ไม่เปิดรับจอง';
  if (startTime && durationMinutes) {
    const start = minutes(startTime), end = start + Number(durationMinutes);
    if (!Number.isFinite(start) || start < minutes(resource.openTime) || end > minutes(resource.closeTime)) return 'เวลาจองอยู่นอกเวลาเปิดของคอร์ต';
  }
  return null;
}
export function matchingRules(rules, ctx) {
  // UTC midnight avoids converting a Thai midnight to the previous weekday.
  const day = new Date(`${ctx.date}T00:00:00Z`).getUTCDay();
  const start = minutes(ctx.startTime), end = start + Number(ctx.durationMinutes || 60);
  return (rules || []).filter(r => r.active && (!r.resourceIds.length || r.resourceIds.includes(ctx.resourceId || 'room1')) &&
    (!r.days.length || r.days.includes(day)) && (!r.excludeHolidays || !ctx.isHoliday) &&
    (!r.startDate || ctx.date >= r.startDate) && (!r.endDate || ctx.date <= r.endDate) &&
    start >= minutes(r.startTime) && end <= minutes(r.endTime))
    .sort((a, b) => b.priority - a.priority || a.id.localeCompare(b.id));
}
export function applyRateRule(quote, config, ctx) {
  const rule = matchingRules(config.rateRules, ctx)[0];
  if (!rule) return quote;
  const price = ctx.durationMinutes === 30 ? rule.halfHourPrice : rule.hourlyPrice;
  return { ...quote, price, amount: price, originalPrice: price, finalPrice: price, qrAmount: price,
    qrType: 'normal', pricingType: 'custom_rate', promoCode: null, discountAmount: 0,
    rateRuleId: rule.id, rateRuleName: rule.name };
}
export function applyPromotion(quote, config, ctx) {
  const subtotal = money(quote.finalPrice);
  const promo = matchingRules(config.promotions, ctx).find(r => subtotal >= r.minSubtotal && ctx.durationMinutes >= r.minDuration);
  const base = { ...quote, subtotal, promotionDiscount: 0, couponDiscount: 0, allowCoupon: true, pricingRevision: config.revision || 0 };
  if (!promo) return base;
  let discount = promo.type === 'percent' ? money(subtotal * promo.value / 100) : promo.type === 'fixed' ? Math.max(0, subtotal - promo.value) : promo.value;
  if (promo.maxDiscount > 0) discount = Math.min(discount, promo.maxDiscount);
  discount = money(Math.min(subtotal, discount));
  const finalPrice = money(subtotal - discount);
  return { ...base, originalPrice: subtotal, finalPrice, price: finalPrice, amount: finalPrice, qrAmount: finalPrice,
    promotionDiscount: discount, discountAmount: discount, promotionId: promo.id, promotionName: promo.name,
    allowCoupon: promo.allowCoupon, promoCode: promo.id };
}
export function financialSnapshot(quote) {
  const total = money(quote.finalPrice ?? quote.price);
  const subtotal = money(quote.subtotal ?? quote.originalPrice ?? total);
  const promotionDiscount = money(quote.promotionDiscount);
  const couponDiscount = money(quote.couponDiscount ?? Math.max(0, subtotal - total - promotionDiscount));
  return { version: 1, currency: 'THB', subtotal, promotionDiscount, couponDiscount,
    discountTotal: money(subtotal - total), total, pricingRevision: quote.pricingRevision || 0,
    priceRuleVersion: quote.priceRuleVersion || null,
    promotionId: quote.promotionId || null, promotionName: quote.promotionName || quote.promoCode || null,
    voucherCode: quote.voucherCode || null, voucherCampaignId: quote.voucherCampaignId || null,
    voucherCampaignName: quote.voucherCampaignName || null, lines: quote.breakdown || [] };
}
export function bookingFinancials(booking) {
  const stored = booking.pricingSnapshot;
  const total = money(booking.price ?? booking.amount);
  // Historical manual corrections take precedence over the original quote.
  if (stored && money(stored.total) === total) return stored;
  if (stored) return financialSnapshot({ price: total, originalPrice: total, priceRuleVersion: 'manual-adjustment' });
  const originalPrice = Math.max(total, money(booking.originalPrice ?? total));
  return financialSnapshot({ ...booking, finalPrice: total, originalPrice });
}
