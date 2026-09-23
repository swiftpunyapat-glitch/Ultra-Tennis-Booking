import { beforeAll, beforeEach, test, expect, vi } from 'vitest';
import { Timestamp, DocumentReference } from 'firebase-admin/firestore';

let db, accounting, ops, finance, booking, cookies;
const date = '2027-06-14', slotId = `room1_${date}_1200`;
beforeAll(async () => {
  if (!process.env.FIRESTORE_EMULATOR_HOST?.startsWith('127.0.0.1:')) throw new Error('Local emulator required');
  process.env.ADMIN_SESSION_SECRET = 'mark-test-local';
  process.env.ADMIN_USERS_JSON = JSON.stringify(Object.fromEntries([
    ['Art','owner'], ['OtherOwner','owner'], ['Ultra','ultra_admin'], ['Staff','branch_staff'],
  ].map(([name,role]) => [name,{pin:'0000',role,branches:'*'}])));
  delete process.env.GOOGLE_CALENDAR_ID;
  db = (await import('../api/_lib/firebase-admin.js')).getAdminDb();
  accounting = (await import('../api/admin-edit-booking-accounting.js')).default;
  ops = (await import('../api/admin-ops.js')).default;
  finance = (await import('../api/finance-data.js')).default;
  booking = (await import('../api/booking.js')).default;
  const { createSessionCookie } = await import('../api/_lib/admin-auth.js');
  cookies = Object.fromEntries(['Art','OtherOwner','Ultra','Staff'].map(name => [name,createSessionCookie(name).split(';')[0]]));
});
async function call(handler, body, actor='Art', method='POST', query={}) {
  const res={statusCode:200,status(n){this.statusCode=n;return this},json(b){this.body=b;return this},setHeader(){}};
  await handler({method,body,query,headers:{cookie:cookies[actor]||'', 'x-forwarded-for':'198.51.100.126'},socket:{}},res);
  return res;
}
const mark = (id='demo', actor='Art') => call(accounting,{operation:'mark_booking_test',bookingId:id,reason:'ทดสอบการจอง'},actor);
const data = async (collection,id) => (await db.collection(collection).doc(id).get()).data();
const exists = async (collection,id) => (await db.collection(collection).doc(id).get()).exists;
async function seed(extra={}) {
  await db.collection('bookings').doc('demo').set({
    bookingCode:'TEST-001', branchId:'ladprao1', resourceId:'room1', date, startTime:'12:00', endTime:'13:00',
    durationMinutes:60, bookingSlotIds:[slotId], bookingStatus:'confirmed', status:'confirmed', paymentStatus:'paid',
    price:350, customerName:'Test customer', lineUserId:'test-user', createdAt:Timestamp.now(), paidAt:Timestamp.fromDate(new Date(`${date}T12:00:00+07:00`)), ...extra,
  });
  await db.collection('booking_slots').doc(slotId).set({bookingId:'demo',bookingCode:'TEST-001',bookingStatus:'confirmed',paymentStatus:'paid'});
  await db.collection('booking_slot_claims').doc(slotId).set({bookingId:'demo',bookingCode:'TEST-001',status:'confirmed'});
  await db.collection('available_slots').doc(slotId).set({resourceId:'room1',date,startTime:'12:00',status:'open'});
}
beforeEach(async () => {
  for (const collection of ['bookings','booking_slots','booking_slot_claims','coach_slot_claims','available_slots','customer_packages','customer_package_logs','vouchers','finance_expenses','finance_income_manual','finance_documents','finance_document_counters','audit_logs','guest_booking_access','system_settings','commerce_settings','rate_limits','holidays']) {
    const snap=await db.collection(collection).get(); await Promise.all(snap.docs.map(d=>d.ref.delete()));
  }
  await db.collection('system_settings').doc('pricing').set({normalPrice:350});
});
test('only authenticated Art with owner role can reclassify or read test history',async()=>{
  await seed();
  for (const actor of ['OtherOwner','Ultra','Staff','']) {
    expect((await mark('demo',actor)).statusCode).toBe(actor?403:401);
    expect((await call(ops,{action:'admin_read',resource:'bookings',testsOnly:true},actor)).statusCode).toBe(actor?403:401);
  }
  const saved=process.env.ADMIN_USERS_JSON;
  process.env.ADMIN_USERS_JSON=JSON.stringify({Art:{pin:'0000',role:'branch_staff',branches:'*'}});
  const {createSessionCookie}=await import('../api/_lib/admin-auth.js');
  const previous=cookies.Art;cookies.Art=createSessionCookie('Art').split(';')[0];
  try { expect((await mark()).statusCode).toBe(403); }
  finally { cookies.Art=previous;process.env.ADMIN_USERS_JSON=saved; }
  expect((await data('bookings','demo')).isTest).toBeUndefined();
});
test('real booking flow releases its slot immediately, excludes reports and keeps an audited history',async()=>{
  await db.collection('available_slots').doc(slotId).set({resourceId:'room1',date,startTime:'12:00',status:'open'});
  const payload={action:'create',date,startTime:'12:00',durationMinutes:60,resourceId:'room1',customerName:'Demo',customerPhone:'0800000000'};
  const first=await call(booking,payload,'');
  expect(first.statusCode,JSON.stringify(first.body)).toBe(200);
  const id=first.body.booking.id;
  expect((await call(accounting,{operation:'mark_paid',bookingId:id,amount:350,paymentMethod:'cash'})).statusCode).toBe(200);
  expect((await mark(id)).statusCode).toBe(200);
  expect(await exists('booking_slots',slotId)).toBe(false);
  expect(await exists('booking_slot_claims',slotId)).toBe(false);
  const original=await data('bookings',id);
  expect(original).toMatchObject({isTest:true,price:350,bookingStatus:'cancelled',paymentStatus:'rejected',testConversion:{actor:'Art',before:{paymentStatus:'paid'}}});
  expect((await call(ops,{action:'admin_read',resource:'bookings'})).body.items).toEqual([]);
  expect((await call(ops,{action:'admin_read',resource:'bookings',testsOnly:true})).body.items.map(b=>b.id)).toEqual([id]);
  const report=await call(finance,{},'Art','GET',{month:'2027-06'});
  expect(report.body.bookings).toEqual([]);
  expect((await db.collection('audit_logs').where('action','==','mark_booking_test').get()).size).toBe(1);
  expect((await data('guest_booking_access',id)).revokedAt).toBeTruthy();
  expect((await call(booking,payload,'')).statusCode).toBe(200);
  expect((await mark(id)).body.replayed).toBe(true);
  expect(await exists('booking_slots',slotId)).toBe(true); // retry cannot remove new customer
});
test('linked refunds, payouts, income and issued documents are voided together; unrelated money stays',async()=>{
  await seed({refundExpenseId:'refund'});
  await db.collection('finance_expenses').doc('refund').set({businessUnit:'ultra_tennis',sourceBookingId:'demo',date,category:'Refund',amount:50,deleted:false});
  await db.collection('finance_expenses').doc('payout').set({businessUnit:'ultra_tennis',sourceBookingId:'demo',date,category:'Staff',amount:100,deleted:false});
  await db.collection('finance_income_manual').doc('income').set({businessUnit:'ultra_tennis',sourceBookingId:'demo',date,category:'Other',amount:50,deleted:false});
  await db.collection('finance_expenses').doc('other').set({businessUnit:'ultra_tennis',sourceBookingId:'real',date,amount:90,deleted:false});
  for (const [linkedType,linkedId,docType] of [['booking','demo','receipt'],['expense','refund','payment_voucher'],['manual_income','income','receipt']]) {
    const issued=await call(finance,{action:'documents:issue',linkedType,linkedId,docType});
    expect(issued.statusCode,JSON.stringify(issued.body)).toBe(200);
  }
  const res=await mark();expect(res.statusCode,JSON.stringify(res.body)).toBe(200);
  expect(res.body).toMatchObject({excludedFinanceRecords:3,voidedDocuments:3});
  expect((await data('finance_expenses','other')).deleted).toBe(false);
  expect((await data('finance_expenses','refund'))).toMatchObject({isTest:true,deleted:true,amount:50});
  expect((await db.collection('finance_documents').get()).docs.every(d=>d.data().status==='void')).toBe(true);
  expect(await exists('finance_document_counters','active_receipt_booking_demo')).toBe(false);
  expect((await call(finance,{action:'documents:issue',linkedType:'booking',linkedId:'demo',docType:'receipt'})).statusCode).toBe(409);
  const report=(await call(finance,{},'Art','GET',{month:'2027-06'})).body;
  expect(report.bookings).toEqual([]);expect(report.expenses.map(x=>x.id)).toEqual(['other']);
  expect(report.income).toEqual([]);
});
test.each(['reserved','redeemed'])('coupon %s reverts without spending cancellation quota',async state=>{
  await seed({voucherLifecycle:'v2_state',voucherCode:'COUPON'});
  await db.collection('vouchers').doc('COUPON').set({state,usedCount:state==='redeemed'?1:0,[`${state}BookingId`]:'demo',cancellationRestoreCount:3,maxCancellationRestores:3});
  expect((await mark()).statusCode).toBe(200);
  expect((await data('vouchers','COUPON'))).toMatchObject({state:'available',usedCount:0,cancellationRestoreCount:3});
  expect((await mark()).body.replayed).toBe(true);
});
test('a failed ownership check rolls back package restoration, slots and accounting too',async()=>{
  await seed({createdVia:'server_pass',packageId:'pass',packageType:'ultra_pass_10',packageMinutesUsed:60,paymentStatus:'package',voucherLifecycle:'v2_state',voucherCode:'OTHER'});
  await db.collection('customer_packages').doc('pass').set({lineUserId:'test-user',packageType:'ultra_pass_10',remainingMinutes:540});
  await db.collection('vouchers').doc('OTHER').set({state:'redeemed',redeemedBookingId:'another',usedCount:1});
  expect((await mark()).statusCode).toBe(409);
  expect((await data('customer_packages','pass')).remainingMinutes).toBe(540);
  expect(await exists('booking_slots',slotId)).toBe(true);
  expect((await data('bookings','demo')).isTest).toBeUndefined();
  expect((await db.collection('audit_logs').get()).size).toBe(0);
});
test('concurrent duplicate actions restore package minutes only once',async()=>{
  await seed({createdVia:'server_pass',packageId:'pass',packageType:'ultra_pass_10',packageMinutesUsed:60,paymentStatus:'package'});
  await db.collection('customer_packages').doc('pass').set({lineUserId:'test-user',packageType:'ultra_pass_10',remainingMinutes:540,lastUsedBooking:'TEST-001'});
  const outcomes=await Promise.all([mark(),mark()]);
  expect(outcomes.map(r=>r.statusCode)).toEqual([200,200]);
  expect(outcomes.filter(r=>r.body.replayed)).toHaveLength(1);
  expect((await data('customer_packages','pass')).remainingMinutes).toBe(600);
  expect((await db.collection('customer_package_logs').get()).size).toBe(1);
});
test.each(['reserved','consumed'])('coach v2 %s package and coach claims are released',async packageUsageState=>{
  await seed({coachAddonSchemaVersion:2,serviceCategory:'coach_lesson',coachId:'coach',coachClaimIds:['coach-cell'],packageId:'pass',packageType:'beginner_coaching_5',courtPackageMinutes:60,packageUsageState,bookingState:'confirmed',cashState:'paid'});
  await db.collection('customer_packages').doc('pass').set({lineUserId:'test-user',packageType:'beginner_coaching_5',remainingMinutes:240});
  await db.collection('coach_slot_claims').doc('coach-cell').set({bookingId:'demo'});
  const res=await mark();expect(res.statusCode,JSON.stringify(res.body)).toBe(200);
  expect(res.body).toMatchObject({restoredMinutes:60,releasedCoachClaims:1});
  expect((await data('customer_packages','pass')).remainingMinutes).toBe(300);
  expect(await exists('coach_slot_claims','coach-cell')).toBe(false);
  expect((await data('bookings','demo'))).toMatchObject({packageUsageState:'released',bookingState:'cancelled',coachPayoutStatus:'void'});
});
test('an old cancelled booking never deletes replacement claims or restores a pass twice',async()=>{
  await seed({bookingStatus:'cancelled',packageId:'pass',packageType:'ultra_pass_10',packageMinutesUsed:60,packageRestoredAt:Timestamp.now(),voucherLifecycle:'v2_state',voucherCode:'REUSED'});
  await db.collection('customer_packages').doc('pass').set({lineUserId:'test-user',remainingMinutes:600});
  await db.collection('vouchers').doc('REUSED').set({state:'redeemed',redeemedBookingId:'real',usedCount:1});
  for(const c of ['booking_slots','booking_slot_claims'])await db.collection(c).doc(slotId).set({bookingId:'real',bookingCode:'REAL'});
  expect((await mark()).statusCode).toBe(200);
  expect((await data('customer_packages','pass')).remainingMinutes).toBe(600);
  expect((await data('booking_slot_claims',slotId)).bookingId).toBe('real');
  expect((await data('booking_slots',slotId)).bookingId).toBe('real');
  expect((await data('vouchers','REUSED')).redeemedBookingId).toBe('real');
});
test('all edit/reactivation actions reject a reclassified booking',async()=>{
  await seed();await mark();
  for(const fields of [
    {operation:'mark_paid',amount:350,paymentMethod:'cash'}, {operation:'approve_slip'},
    {operation:'reschedule_assign',newDate:date,newStartTime:'13:00'}, {operation:'delete_booking'},
    {operation:'coach_payout_paid'}, {operation:'assign_coach'},
    {operation:'accounting_edit',accountingType:'normal',reason:'try'},
  ]) {
    const res=await call(accounting,{bookingId:'demo',...fields});
    expect([400,409]).toContain(res.statusCode);
  }
});
test('missing calendar credentials leaves a durable retry without undoing the conversion',async()=>{
  await seed({googleCalendarEventId:'event'});
  const result=await mark();expect(result.body.calendarPending).toBe(true);
  expect((await data('bookings','demo')).testCalendarCleanup).toBe('pending');
  expect(await exists('booking_slots',slotId)).toBe(false);
  expect((await mark()).body).toMatchObject({replayed:true,calendarPending:true});
});

test('off-peak usage quotas and Event Pass consumption are reversed for test bookings',async()=>{
  await seed({createdVia:'server_pass',paymentStatus:'package',packageId:'offpeak',packageType:'offpeak',packageMinutesUsed:60});
  await db.collection('customer_packages').doc('offpeak').set({lineUserId:'test-user',packageType:'offpeak',totalMinutes:600,remainingMinutes:540,weeklyUsage:{'2027-W24':60},monthlyUsage:{'2027-06':60}});
  expect((await mark()).statusCode).toBe(200);
  expect(await data('customer_packages','offpeak')).toMatchObject({remainingMinutes:600,weeklyUsage:{'2027-W24':0},monthlyUsage:{'2027-06':0}});
  await seed({createdVia:'server_pass',paymentStatus:'package',packageId:'event',packageType:'monstr_event_pass',packageMinutesUsed:60});
  await db.collection('customer_packages').doc('event').set({lineUserId:'test-user',packageType:'monstr_event_pass',remainingMinutes:0,lastUsedBooking:'TEST-001',eventUsedAt:Timestamp.now()});
  expect((await mark()).statusCode).toBe(200);
  expect(await data('customer_packages','event')).toMatchObject({remainingMinutes:60,eventUsedAt:null,lastUsedBooking:null});
});

test('a stale coach payout cannot recreate a finance expense after conversion',async()=>{
  await seed({coachId:'coach',coachName:'Coach',lessonStatus:'completed',coachPayoutStatus:'payable',coachPayoutAmount:200});
  const original=DocumentReference.prototype.get;
  let armed=true;
  const spy=vi.spyOn(DocumentReference.prototype,'get').mockImplementation(async function(...args){
    const snap=await original.apply(this,args);
    if(armed&&this.path==='bookings/demo'){
      armed=false;
      expect((await mark()).statusCode).toBe(200);
    }
    return snap;
  });
  try{
    const res=await call(accounting,{operation:'coach_payout_paid',bookingId:'demo'});
    expect(res.statusCode).toBe(409);
  }finally{spy.mockRestore();}
  expect((await db.collection('finance_expenses').get()).size).toBe(0);
  expect((await data('bookings','demo')).coachPayoutStatus).toBe('void');
});

test('calendar cleanup retries without sending notifications or applying money changes again',async()=>{
  await seed({googleCalendarEventId:'test-event'});
  expect((await mark()).body.calendarPending).toBe(true);
  const names=['GOOGLE_CALENDAR_ID','GOOGLE_CLIENT_ID','GOOGLE_CLIENT_SECRET','GOOGLE_REFRESH_TOKEN'];
  const previous=names.map(name=>process.env[name]);
  for(const name of names)process.env[name]='local-test';
  const fetchMock=vi.spyOn(globalThis,'fetch').mockImplementation(async (url,options)=>{
    if(String(url).includes('oauth2.googleapis.com'))return {ok:true,json:async()=>({access_token:'test-token'})};
    expect(String(url)).toContain('/events/test-event?sendUpdates=none');
    expect(options.method).toBe('DELETE');
    return {status:204};
  });
  try{
    expect((await mark()).body).toMatchObject({replayed:true,calendarPending:false});
    expect((await data('bookings','demo'))).toMatchObject({testCalendarCleanup:'done',googleCalendarEventId:null});
    expect((await mark()).body.replayed).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  }finally{
    fetchMock.mockRestore();
    names.forEach((name,i)=>{if(previous[i]===undefined)delete process.env[name];else process.env[name]=previous[i];});
  }
  expect((await db.collection('audit_logs').where('action','==','mark_booking_test').get()).size).toBe(1);
});

test('filtering a test page does not lose live bookings with the exact same creation timestamp',async()=>{
  const createdAt=new Timestamp(1800000000,123456789);
  for(const id of ['a','b','c'])await db.collection('bookings').doc(id).set({createdAt,isTest:id==='c'});
  const read=cursorBookingId=>call(ops,{action:'admin_read',resource:'bookings',limit:1,...(cursorBookingId?{cursorBookingId}:{})});
  const first=await read();expect(first.body.items).toEqual([]);expect(first.body.nextCursorId).toBe('c');
  const second=await read(first.body.nextCursorId);expect(second.body.items.map(b=>b.id)).toEqual(['b']);
  const third=await read(second.body.nextCursorId);expect(third.body.items.map(b=>b.id)).toEqual(['a']);
});

test('changed package type cannot receive restoration and a replacement public slot is preserved',async()=>{
  await seed({coachAddonSchemaVersion:2,serviceCategory:'coach_lesson',packageId:'pass',packageType:'beginner_coaching_5',courtPackageMinutes:60,packageUsageState:'consumed'});
  await db.collection('customer_packages').doc('pass').set({lineUserId:'test-user',packageType:'ultra_pass_10',remainingMinutes:540});
  expect((await mark()).statusCode).toBe(409);
  expect(await exists('booking_slot_claims',slotId)).toBe(true);
  await seed();
  await db.collection('booking_slots').doc(slotId).set({bookingCode:'OTHER'});
  expect((await mark()).statusCode).toBe(200);
  expect((await data('booking_slots',slotId)).bookingCode).toBe('OTHER');
});
