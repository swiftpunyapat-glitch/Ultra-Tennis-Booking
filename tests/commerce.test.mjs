import { describe, expect, test } from 'vitest';
import { DEFAULT_COMMERCE, validateCommerce, resourceError, financialSnapshot, bookingFinancials } from '../commerce.js';
import { courtQuote } from '../api/_lib/court-quote.js';
import { evaluateVoucher, applyVoucherToQuote } from '../api/_lib/voucher-engine.js';
const rate = { id:'peak',name:'Peak',active:true,priority:10,resourceIds:['court2'],days:[6],startTime:'12:00',endTime:'18:00',hourlyPrice:400,halfHourPrice:220 };
const promo = { id:'summer',name:'Summer',active:true,priority:5,resourceIds:[],days:[],startTime:'00:00',endTime:'24:00',type:'percent',value:10,minDuration:60,allowCoupon:true };
const config = extra => validateCommerce({ ...DEFAULT_COMMERCE, resources:[...DEFAULT_COMMERCE.resources,{id:'court2',name:'Court 2',active:true,openTime:'06:00',closeTime:'24:00'}],rateRules:[rate],promotions:[promo],...extra });
const args = { date:'2027-05-15',startTime:'12:00',durationMinutes:120,resourceId:'court2',segments:[{start:'12:00',span:60},{start:'13:00',span:60}] };
const quote = extra => courtQuote({...args,pricing:{commerce:config()},...extra}).quote;
describe('commercial pricing',()=>{
  test('preserves Ultra defaults until rules are configured',()=>{
    expect(quote({pricing:null}).finalPrice).toBe(700);
    expect(quote({pricing:null,resourceId:'room1',date:'2027-05-17',startTime:'06:00',durationMinutes:60,segments:[{start:'06:00',span:60}],nowMs:Date.parse('2027-05-17T01:00:00+07:00')}).finalPrice).toBe(330);
  });
  test('court/day rate then one order promotion then one coupon produces 670',()=>{
    const base=quote();expect(base).toMatchObject({subtotal:800,promotionDiscount:80,finalPrice:720});
    const result=evaluateVoucher({baseQuote:base,date:args.date,startTime:args.startTime,durationMinutes:120,resourceId:'court2',code:'SAVE50',voucher:{active:true,state:'available',voucherType:'discount_amount',discountAmount:50,maxUses:1,allowedDurations:[120]}});
    expect(result.ok).toBe(true);
    const q=applyVoucherToQuote(base,result);
    expect(financialSnapshot(q)).toMatchObject({subtotal:800,promotionDiscount:80,couponDiscount:50,discountTotal:130,total:670});
  });
  test('does not leak court-specific prices to another court',()=>expect(quote({resourceId:'room1'}).subtotal).toBe(700));
  test('requires the whole booking to fit the promotion time window',()=>{
    expect(quote({pricing:{commerce:config({promotions:[{...promo,endTime:'13:00'}]})}}).promotionDiscount).toBe(0);
  });
  test('promotion priority wins, and explicit stacking opt-out blocks coupons',()=>{
    const q=quote({pricing:{commerce:config({promotions:[promo,{...promo,id:'priority',priority:20,type:'amount',value:25,allowCoupon:false}]})}});
    expect(q.finalPrice).toBe(775);expect(q.promotionId).toBe('priority');
    expect(evaluateVoucher({baseQuote:q,voucher:{active:true}}).reason).toBe('not_applicable');
  });
  test('percentage cap and monetary rounding never produce negative totals',()=>{
    expect(quote({pricing:{commerce:config({promotions:[{...promo,value:100,maxDiscount:55.55}]})}}).finalPrice).toBe(744.45);
    expect(quote({pricing:{commerce:config({promotions:[{...promo,type:'amount',value:9999}]})}}).finalPrice).toBe(0);
  });
  test('configured half prices and full rates survive a 90-minute booking',()=>{
    const q=quote({durationMinutes:90,segments:[{start:'12:00',span:60},{start:'13:00',span:30}]});
    expect(q.subtotal).toBe(620);expect(q.finalPrice).toBe(558);
  });
  test('holiday exclusions, dates and opening hours are enforced',()=>{
    expect(resourceError(config(),'court2','05:30',60)).toBeTruthy();
    expect(resourceError(config(),'unknown','12:00',60)).toBeTruthy();
    expect(quote({isHoliday:true,pricing:{commerce:config({rateRules:[{...rate,excludeHolidays:true}],promotions:[]})}}).finalPrice).toBe(700);
    expect(quote({pricing:{commerce:config({promotions:[{...promo,startDate:'2027-06-01'}]})}}).promotionDiscount).toBe(0);
  });
  test('manual-only configuration rejects activation of an unconnected provider',()=>expect(()=>config({paymentVerificationMode:'automatic'})).toThrow());
  test('invalid court IDs and duplicate rule IDs are rejected',()=>{
    expect(()=>config({resources:[{...DEFAULT_COMMERCE.resources[0],id:'a/b'}]})).toThrow();
    expect(()=>config({rateRules:[rate,rate]})).toThrow();
  });
  test('stored prices survive future rules and historical corrections remain explicit',()=>{
    const snapshot=financialSnapshot(quote());
    expect(bookingFinancials({price:720,pricingSnapshot:snapshot})).toBe(snapshot);
    expect(bookingFinancials({price:600,pricingSnapshot:snapshot})).toMatchObject({total:600,subtotal:600,discountTotal:0,priceRuleVersion:'manual-adjustment'});
  });
});

test('a coupon minimum final price cannot increase the price after a promotion',()=>{
  const result=evaluateVoucher({baseQuote:{finalPrice:20,pricingType:'custom_rate'},voucher:{active:true,state:'available',voucherType:'discount_amount',discountAmount:50,minFinalPrice:100,maxUses:1}});
  expect(result).toEqual({ok:false,reason:'not_applicable'});
});
