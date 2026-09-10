import {beforeAll,beforeEach,test,expect} from 'vitest';
import {Timestamp} from 'firebase-admin/firestore';
import {checkedWriteBatch} from '../api/_lib/checked-write-batch.js';
let db,ops,accounting,cookie;
const date='2027-10-10',id=`room1_${date}_1000`,half=`room1_${date}_1030`;
beforeAll(async()=>{
  if(!/^127\.0\.0\.1:\d+$/.test(process.env.FIRESTORE_EMULATOR_HOST||'')) throw new Error('Local emulator required');
  process.env.ADMIN_SESSION_SECRET='incident-test-secret';
  process.env.ADMIN_USERS_JSON=JSON.stringify({Art:{pin:'0000',role:'owner',branches:'*'}});
  db=(await import('../api/_lib/firebase-admin.js')).getAdminDb();
  ops=(await import('../api/admin-ops.js')).default;
  accounting=(await import('../api/admin-edit-booking-accounting.js')).default;
  cookie=(await import('../api/_lib/admin-auth.js')).createSessionCookie('Art').split(';')[0];
});
const ref=(collection,key=id)=>db.collection(collection).doc(key);
async function call(handler,body){
  const res={statusCode:0,body:null,status(c){this.statusCode=c;return this;},json(b){this.body=b;return this;},setHeader(){}};
  await handler({method:'POST',body,headers:{cookie},socket:{}},res);return res;
}
beforeEach(async()=>{
  for(const coll of ['available_slots','booking_slots','booking_slot_claims']) for(const key of [id,half]) await ref(coll,key).delete();
  await ref('bookings','incident-booking').delete();
  await ref('available_slots').set({resourceId:'room1',branchId:'ladprao1',date,startTime:'10:00',endTime:'11:00',status:'open'});
});
async function seed(){
  await ref('bookings','incident-booking').set({bookingCode:'INCIDENT',bookingSlotIds:[id],resourceId:'room1',branchId:'ladprao1',date,startTime:'10:00',endTime:'11:00',durationHours:1,durationMinutes:60,price:350,bookingStatus:'confirmed',paymentStatus:'paid',customerName:'Test'});
  await ref('booking_slots').set({resourceId:'room1',branchId:'ladprao1',date,hour:'10:00',bookingStatus:'confirmed',paymentStatus:'paid'});
  await ref('booking_slot_claims').set({bookingId:'incident-booking',bookingCode:'INCIDENT',status:'confirmed'});
}
test.each(['slot_toggle','slot_close_unbooked','slot_bulk_set'])('%s preserves a live :30 booking',async action=>{
  await ref('booking_slots',half).set({resourceId:'room1',date,hour:'10:30',slotSpanMinutes:30,bookingStatus:'confirmed'});
  const r=await call(ops,{action,date,dates:[date],hour:10,hourSet:'normal',op:'close'});
  expect([200,409]).toContain(r.statusCode);
  expect((await ref('available_slots').get()).data().status).toBe('open');
});
test('expired half-hour hold does not prevent closing',async()=>{
  await ref('booking_slots',half).set({resourceId:'room1',date,hour:'10:30',slotSpanMinutes:30,bookingStatus:'pending_payment',expiresAt:Timestamp.fromMillis(Date.now()-1000)});
  expect((await call(ops,{action:'slot_toggle',date,hour:10,op:'close'})).statusCode).toBe(200);
  expect((await ref('available_slots').get()).data().status).toBe('closed');
});
test('bulk close still creates explicitly closed schedule entries',async()=>{
  const r=await call(ops,{action:'slot_bulk_set',dates:[date],hourSet:'normal',op:'close'});
  expect(r.statusCode).toBe(200);
  expect((await ref('available_slots',`room1_${date}_1100`).get()).data()).toMatchObject({status:'closed',startTime:'11:00',date});
});
test('single close of a missing schedule retains the not-found response',async()=>{
  await ref('available_slots').delete();
  expect((await call(ops,{action:'slot_toggle',date,hour:10,op:'close'})).statusCode).toBe(404);
});
test('a pending hold with missing expiry is preserved for review',async()=>{
  await ref('booking_slots',half).set({resourceId:'room1',date,hour:'10:30',slotSpanMinutes:30,bookingStatus:'pending_payment'});
  expect((await call(ops,{action:'slot_toggle',date,hour:10,op:'close'})).statusCode).toBe(409);
  expect((await ref('available_slots').get()).data().status).toBe('open');
});
test('manual booking racing a close never commits into a closed slot',async()=>{
  const [booking,close]=await Promise.all([
    call(accounting,{operation:'manual_create',customerName:'Race',customerPhone:'0800000000',date,startTime:'10:00',bookingType:'Pay at Counter'}),
    call(ops,{action:'slot_toggle',date,hour:10,op:'close'}),
  ]);
  expect([200,409]).toContain(booking.statusCode);expect([200,409]).toContain(close.statusCode);
  const av=(await ref('available_slots').get()).data(),slot=await ref('booking_slots').get();
  expect(av.status==='closed' && slot.exists && slot.data().bookingStatus==='confirmed').toBe(false);
});
test.each(['accounting_edit','refund'])('%s rejects an ownership change before commit',async operation=>{
  await seed();
  const run=db.runTransaction;
  let injected=false;
  db.runTransaction=async function(fn,...args){
    if(!injected){injected=true;await ref('booking_slot_claims').set({bookingId:'new-owner',status:'confirmed'});}
    return run.call(this,fn,...args);
  };
  try{
    const r=await call(accounting,{operation,bookingId:'incident-booking',accountingType:'rejected',reason:'test',refundAmount:350,refundMode:'full_refund',refundReason:'customer_request',releaseSlot:true});
    expect(r.statusCode).toBe(409);expect(r.body.code).toBe('ADMIN_DATA_CHANGED');
    expect((await ref('booking_slot_claims').get()).data().bookingId).toBe('new-owner');
    expect((await ref('booking_slots').get()).data().bookingStatus).toBe('confirmed');
    expect((await ref('bookings','incident-booking').get()).data().refundExpenseId).toBeUndefined();
  }finally{db.runTransaction=run;}
});
test('refund commits booking, slot and expense together',async()=>{
  await seed();
  const r=await call(accounting,{operation:'refund',bookingId:'incident-booking',refundAmount:350,refundMode:'full_refund',refundReason:'customer_request',releaseSlot:true});
  expect(r.statusCode).toBe(200);
  expect((await ref('booking_slots').get()).data().bookingStatus).toBe('cancelled');
  expect((await ref('booking_slot_claims').get()).exists).toBe(false);
  expect((await ref('finance_expenses',r.body.refundExpenseId).get()).data().amount).toBe(350);
});
test('changed booking version aborts all queued writes',async()=>{
  await seed();
  const booking=ref('bookings','incident-booking');
  const batch=checkedWriteBatch(db,[await booking.get()]);
  batch.delete(ref('booking_slots'));
  await booking.update({price:400});
  await expect(batch.commit()).rejects.toMatchObject({code:'ADMIN_DATA_CHANGED'});
  expect((await ref('booking_slots').get()).exists).toBe(true);
});
test('accounting cannot confirm an old booking after the slot was reassigned',async()=>{
  await seed();
  await ref('booking_slot_claims').set({bookingId:'new-owner',status:'confirmed'});
  const r=await call(accounting,{operation:'accounting_edit',bookingId:'incident-booking',accountingType:'normal_paid',reason:'test'});
  expect(r.statusCode).toBe(409);
  expect(r.body.code).toBe('SLOT_OWNERSHIP_MISMATCH');
  expect((await ref('booking_slot_claims').get()).data().bookingId).toBe('new-owner');
});
test('historical accounting corrections can keep a completed booking completed',async()=>{
  await seed();
  await ref('bookings','incident-booking').update({bookingStatus:'completed'});
  await ref('booking_slot_claims').set({bookingId:'new-owner',status:'confirmed'});
  const r=await call(accounting,{operation:'accounting_edit',bookingId:'incident-booking',accountingType:'normal_paid',reason:'historical correction'});
  expect(r.statusCode).toBe(200);
  expect((await ref('bookings','incident-booking').get()).data().bookingStatus).toBe('completed');
  expect((await ref('booking_slot_claims').get()).data().bookingId).toBe('new-owner');
});
