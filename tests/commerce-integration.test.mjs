import { beforeAll, beforeEach, afterAll, test, expect } from 'vitest';
import { DEFAULT_COMMERCE } from '../commerce.js';
let db, booking, admin, accounting, finance, cookie, staff;
const date='2027-05-15';
const config={...DEFAULT_COMMERCE,resources:[...DEFAULT_COMMERCE.resources,...[2,3,4].map(n=>({id:`court${n}`,name:`Court ${n}`,active:true,openTime:'06:00',closeTime:'24:00'}))],
  rateRules:[{id:'court2-rate',name:'Court 2 rate',active:true,resourceIds:['court2'],days:[],startTime:'00:00',endTime:'24:00',priority:10,hourlyPrice:400,halfHourPrice:220}],
  promotions:[{id:'ten-percent',name:'10 percent',active:true,resourceIds:[],days:[],startTime:'00:00',endTime:'24:00',priority:10,type:'percent',value:10,minDuration:60,minSubtotal:0,allowCoupon:true}]};
beforeAll(async()=>{
  if(!process.env.FIRESTORE_EMULATOR_HOST?.startsWith('127.0.0.1:')) throw new Error('Local emulator required');
  process.env.ADMIN_SESSION_SECRET='commerce-local-test';
  process.env.ADMIN_USERS_JSON=JSON.stringify({CustomerOwner:{pin:'0000',role:'owner',branches:'*'},Staff:{pin:'0000',role:'branch_staff',branches:['ladprao1']}});
  db=(await import('../api/_lib/firebase-admin.js')).getAdminDb();
  booking=(await import('../api/booking.js')).default;
  admin=(await import('../api/admin-user-action.js')).default;
  accounting=(await import('../api/admin-edit-booking-accounting.js')).default;
  finance=(await import('../api/finance-data.js')).default;
  const {createSessionCookie}=await import('../api/_lib/admin-auth.js');
  cookie=createSessionCookie('CustomerOwner').split(';')[0];staff=createSessionCookie('Staff').split(';')[0];
});
async function call(handler,body,auth='',method='POST',query={}){
  const res={statusCode:200,status(n){this.statusCode=n;return this},json(b){this.body=b;return this},setHeader(){}};
  await handler({method,body,query,headers:{cookie:auth,'x-forwarded-for':'198.51.100.123'},socket:{}},res);return res;
}
beforeEach(async()=>{
  for(const name of ['bookings','booking_slots','booking_slot_claims','available_slots','vouchers','voucher_campaigns','system_settings','commerce_settings','rate_limits','finance_documents','finance_document_counters','finance_expenses','finance_income_manual','holidays']){
    const snap=await db.collection(name).get();await Promise.all(snap.docs.map(d=>d.ref.delete()));
  }
  expect((await call(admin,{action:'commerce_save',commerce:config,revision:0},cookie)).statusCode).toBe(200);
  for(const resourceId of ['room1','court2','court3','court4'])for(const hour of [12,13,14]){
    await db.collection('available_slots').doc(`${resourceId}_${date}_${hour}00`).set({resourceId,date,startTime:`${hour}:00`,status:'open'});
  }
});
const create=(extra={})=>call(booking,{action:'create',date,startTime:'12:00',durationMinutes:120,resourceId:'court2',customerName:'Demo',customerPhone:'0800000000',...extra});
test('only owners can configure commerce and revisions prevent lost updates',async()=>{
  expect((await call(admin,{action:'commerce_get'},staff)).statusCode).toBe(403);
  expect((await call(admin,{action:'commerce_save',commerce:config,revision:0},cookie)).statusCode).toBe(409);
  expect((await call(admin,{action:'commerce_save',commerce:{...config,paymentVerificationMode:'automatic'},revision:1},cookie)).statusCode).toBe(400);
});
test('four courts book independently and concurrent attempts on one court cannot both win',async()=>{
  const results=await Promise.all([create(),create(),create({resourceId:'court3'}),create({resourceId:'court4'}),create({resourceId:'room1'})]);
  expect(results.filter(r=>r.statusCode===200)).toHaveLength(4);
  expect(results.filter(r=>r.statusCode===409)).toHaveLength(1);
});
test('multi-hour coupon, manual payment and receipt all preserve the same breakdown',async()=>{
  await db.collection('vouchers').doc('SAVE50').set({active:true,state:'available',voucherType:'discount_amount',discountAmount:50,maxUses:1,allowedDurations:[120]});
  const created=await create({voucherCode:'SAVE50'});expect(created.statusCode,JSON.stringify(created.body)).toBe(200);
  const id=created.body.booking.id;
  const before=(await db.collection('bookings').doc(id).get()).data();
  expect(before.pricingSnapshot).toMatchObject({subtotal:800,promotionDiscount:80,couponDiscount:50,total:670});
  expect((await call(accounting,{operation:'mark_paid',bookingId:id,amount:800,paymentMethod:'cash'},cookie)).statusCode).toBe(409);
  const paid=await call(accounting,{operation:'mark_paid',bookingId:id,amount:670,paymentMethod:'cash'},cookie);
  expect(paid.statusCode,JSON.stringify(paid.body)).toBe(200);
  const after=(await db.collection('bookings').doc(id).get()).data();
  expect(after.paymentApproval.mode).toBe('manual');
  expect((await db.collection('vouchers').doc('SAVE50').get()).data().state).toBe('redeemed');
  const report=await call(finance,{},cookie,'GET',{month:'2027-05'});
  expect(report.body.bookings.find(b=>b.id===id).financials.total).toBe(670);
  const receipt=await call(finance,{action:'documents:issue',docType:'receipt',linkedType:'booking',linkedId:id},cookie);
  expect(receipt.statusCode,JSON.stringify(receipt.body)).toBe(200);
  expect((await db.collection('finance_documents').doc(receipt.body.document.id).get()).data()).toMatchObject({subtotal:800,discount:130,total:670});
  expect((await call(finance,{action:'documents:issue',docType:'receipt',linkedType:'booking',linkedId:id},cookie)).statusCode).toBe(409);
});
test('changing rules does not rewrite an existing booking',async()=>{
  const created=await create();expect(created.statusCode).toBe(200);
  await call(admin,{action:'commerce_save',commerce:{...config,promotions:[]},revision:1},cookie);
  const stored=(await db.collection('bookings').doc(created.body.booking.id).get()).data();
  expect(stored.price).toBe(720);expect(stored.pricingSnapshot.promotionDiscount).toBe(80);
  const next=await create({startTime:'14:00',durationMinutes:60});expect(next.body.booking.finalPrice).toBe(400);
});
test('unknown or disabled courts cannot be booked even if slot documents exist',async()=>{
  expect((await create({resourceId:'unknown'})).statusCode).toBe(400);
  await call(admin,{action:'commerce_save',commerce:{...config,resources:config.resources.map(r=>({...r,active:r.id!=='court2'}))},revision:1},cookie);
  expect((await create()).statusCode).toBe(400);
});
test('a stale displayed quote cannot silently charge a new amount',async()=>{
  const response=await create({expectedPrice:800});
  expect(response.statusCode).toBe(409);expect(response.body.code).toBe('PRICE_CHANGED');
  expect((await db.collection('bookings').get()).empty).toBe(true);
});
test('moving a paid booking to a different court keeps its price and releases only its original court',async()=>{
  const created=await create();const id=created.body.booking.id;
  await call(accounting,{operation:'mark_paid',bookingId:id,amount:720,paymentMethod:'cash'},cookie);
  const moved=await call(accounting,{operation:'reschedule_assign',bookingId:id,newDate:date,newStartTime:'12:00',newResourceId:'court3'},cookie);
  expect(moved.statusCode,JSON.stringify(moved.body)).toBe(200);
  const saved=(await db.collection('bookings').doc(id).get()).data();
  expect(saved).toMatchObject({resourceId:'court3',price:720,pricingSnapshot:{total:720}});
  expect((await create()).statusCode).toBe(200);
  expect((await create({resourceId:'court3'})).statusCode).toBe(409);
});
test('manual preview matches actual booking across the hour boundary',async()=>{
  const body={date,startTime:'12:30',durationMinutes:60,resourceId:'court2',bookingType:'Manual Single Use'};
  const preview=await call(accounting,{operation:'manual_quote',...body},cookie);
  expect(preview.statusCode,JSON.stringify(preview.body)).toBe(200);
  expect(preview.body.quote.finalPrice).toBe(396);
  const created=await call(accounting,{operation:'manual_create',...body,customerName:'Manual',customerPhone:'0800000000',expectedPrice:396},cookie);
  expect(created.statusCode,JSON.stringify(created.body)).toBe(200);
  expect((await db.collection('bookings').doc(created.body.booking.id).get()).data().pricingSnapshot.total).toBe(396);
});
test('payment month uses paidAt while service month uses play date',async()=>{
  const created=await create();const id=created.body.booking.id;
  await call(accounting,{operation:'mark_paid',bookingId:id,amount:720,paymentMethod:'cash'},cookie);
  const {Timestamp}=await import('firebase-admin/firestore');
  await db.collection('bookings').doc(id).update({paidAt:Timestamp.fromDate(new Date('2027-04-30T23:59:00+07:00'))});
  const april=await call(finance,{},cookie,'GET',{month:'2027-04',basis:'payment'});
  const may=await call(finance,{},cookie,'GET',{month:'2027-05',basis:'payment'});
  const service=await call(finance,{},cookie,'GET',{month:'2027-05',basis:'service'});
  expect(april.body.bookings.map(b=>b.id)).toContain(id);
  expect(may.body.bookings.map(b=>b.id)).not.toContain(id);
  expect(service.body.bookings.map(b=>b.id)).toContain(id);
});

test('a full promotion confirms online and manual bookings without creating cash revenue',async()=>{
  const free={...config,promotions:config.promotions.map(p=>({...p,value:100}))};
  expect((await call(admin,{action:'commerce_save',commerce:free,revision:1},cookie)).statusCode).toBe(200);
  const online=await create();
  expect(online.statusCode,JSON.stringify(online.body)).toBe(200);
  const manual=await call(accounting,{operation:'manual_create',date,startTime:'14:00',durationMinutes:60,resourceId:'court2',bookingType:'Pay at Counter',customerName:'Demo',customerPhone:'0800000000'},cookie);
  expect(manual.statusCode,JSON.stringify(manual.body)).toBe(200);
  for(const id of [online.body.booking.id,manual.body.booking.id]){
    const saved=(await db.collection('bookings').doc(id).get()).data();
    expect(saved).toMatchObject({price:0,paymentStatus:'package',bookingStatus:'confirmed',paymentMethod:'promotion',pricingSnapshot:{total:0}});
    expect(saved.paidAt).toBeUndefined();
  }
});

afterAll(async()=>{
  await db.collection('commerce_settings').doc('current').delete();
});
test('commercial plans are stored outside the public pricing document and features exposes only active courts',async()=>{
  const publicDoc=await db.collection('system_settings').doc('pricing').get();
  expect(publicDoc.data()?.commerce).toBeUndefined();
  const privateDoc=await db.collection('commerce_settings').doc('current').get();
  expect(privateDoc.data().commerce.rateRules[0].id).toBe('court2-rate');
  const features=await call(booking,{action:'features'});
  expect(features.statusCode).toBe(200);
  expect(features.body.commerce.resources).toHaveLength(4);
  expect(features.body.commerce).not.toHaveProperty('rateRules');
  expect(features.body.commerce).not.toHaveProperty('promotions');
  const owner=await call(admin,{action:'commerce_get'},cookie);
  expect(owner.body.commerce.rateRules[0].id).toBe('court2-rate');
  expect((await create()).body.booking.finalPrice).toBe(720);
});
test('saving migrates provisional inline rules without changing public base prices',async()=>{
  await db.collection('commerce_settings').doc('current').delete();
  await db.collection('system_settings').doc('pricing').set({normalSingleUsePrice:375,commerce:{...config,revision:7}});
  const saved=await call(admin,{action:'commerce_save',commerce:config,revision:7},cookie);
  expect(saved.statusCode,JSON.stringify(saved.body)).toBe(200);
  expect(saved.body.commerce.revision).toBe(8);
  const publicDoc=(await db.collection('system_settings').doc('pricing').get()).data();
  expect(publicDoc).toEqual({normalSingleUsePrice:375});
  const created=await create();
  expect(created.statusCode,JSON.stringify(created.body)).toBe(200);
  expect(created.body.booking.finalPrice).toBe(720);
});
