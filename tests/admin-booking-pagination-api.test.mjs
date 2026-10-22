import { beforeAll, beforeEach, test, expect } from 'vitest';
import { Timestamp, FieldValue } from 'firebase-admin/firestore';
import { collectAdminBookingPages, paidSummary } from '../admin-booking-data.js';

let db,ops,finance,cookie;
beforeAll(async()=>{
  if (!/^127\.0\.0\.1:\d+$/.test(process.env.FIRESTORE_EMULATOR_HOST||'') || !String(process.env.GCLOUD_PROJECT||'').startsWith('demo-')) throw new Error('Loopback emulator and demo project required; never production');
  process.env.ADMIN_SESSION_SECRET='local-pagination-test-only';
  process.env.ADMIN_USERS_JSON=JSON.stringify({Art:{pin:'0000',role:'owner',branches:'*'},Staff:{pin:'0000',role:'branch_staff',branches:['ladprao1']}});
  db=(await import('../api/_lib/firebase-admin.js')).getAdminDb();
  ops=(await import('../api/admin-ops.js')).default;finance=(await import('../api/finance-data.js')).default;
  cookie=(await import('../api/_lib/admin-auth.js')).createSessionCookie;
});
beforeEach(async()=>{
  for(const name of ['bookings','finance_expenses','finance_income_manual']){
    const snap=await db.collection(name).get();await Promise.all(snap.docs.map(d=>d.ref.delete()));
  }
});
function res(){return{statusCode:200,headers:{},status(n){this.statusCode=n;return this},json(b){this.body=b;return this},setHeader(k,v){this.headers[k]=v}};}
async function page(options={},who='Art'){
  const out=res();await ops({method:'POST',body:{action:'admin_read',resource:'bookings',...options},headers:{cookie:cookie(who).split(';')[0]}},out);
  if(out.statusCode!==200)throw new Error(`HTTP ${out.statusCode}`);return out.body;
}
async function seed(id,extra={}){
  await db.collection('bookings').doc(id).set({bookingCode:id,resourceId:'room1',branchId:'ladprao1',date:'2026-10-23',startTime:'12:00',endTime:'13:00',durationHours:1,price:350,bookingStatus:'confirmed',paymentStatus:'paid',createdAt:Timestamp.fromMillis(1000),...extra});
}

test('real Admin projection and Finance agree on paidAt across Bangkok month/day boundaries',async()=>{
  await seed('before-oct',{paidAt:Timestamp.fromDate(new Date('2026-09-30T16:59:59.999Z'))});
  await seed('oct-start',{paidAt:Timestamp.fromDate(new Date('2026-09-30T17:00:00.000Z'))});
  await seed('oct-end',{paidAt:Timestamp.fromDate(new Date('2026-10-31T16:59:59.999Z')),bookingStatus:'cancelled'});
  await seed('nov-start',{paidAt:Timestamp.fromDate(new Date('2026-10-31T17:00:00.000Z'))});
  await seed('review-only',{adminReviewedAt:Timestamp.fromDate(new Date('2026-10-22T04:49:00Z'))});
  await seed('play-next-day',{paidAt:Timestamp.fromDate(new Date('2026-10-22T04:49:00Z'))});
  const admin=await collectAdminBookingPages(options=>page(options),{limit:2});
  const out=res();await finance({method:'GET',query:{month:'2026-10',basis:'payment'},headers:{cookie:cookie('Art').split(';')[0]}},out);
  expect(out.statusCode).toBe(200);
  expect(out.body.bookings.map(b=>b.id).sort()).toEqual(['oct-end','oct-start','play-next-day']);
  expect(paidSummary(admin,'2026-10-22','room1').rows.map(b=>b.id)).toEqual(['play-next-day']);
  expect(paidSummary(admin,'2026-10-31','room1').rows.map(b=>b.id)).toEqual(['oct-end']);
  expect(paidSummary(admin,'2026-10-22','room1').missingPaidAt).toBe(1);
});

test('raw server cursors traverse >500 docs, equal timestamps, empty branch pages and cancellations without duplicates',async()=>{
  for(let start=0;start<505;start+=400){
    const batch=db.batch();
    for(let i=start;i<Math.min(start+400,505);i++)batch.set(db.collection('bookings').doc(`a-recent-${String(i).padStart(4,'0')}`),{
      branchId:'other',date:'2026-10-22',bookingStatus:'confirmed',paymentStatus:'paid',createdAt:Timestamp.fromMillis(2000),
    });
    await batch.commit();
  }
  await seed('z-july-cancelled',{date:'2026-07-12',bookingStatus:'cancelled'});
  await seed('z-aug-pending',{date:'2026-08-01',bookingStatus:'rescheduled',pendingReschedule:true});
  await seed('z-test-only',{isTest:true});
  await seed('z-no-created');await db.collection('bookings').doc('z-no-created').update({createdAt:FieldValue.delete()});
  const first=await page({limit:500,bookingOrder:'id'},'Staff');expect(first.items).toEqual([]);expect(first.nextCursorId).toBeTruthy();
  const out=await collectAdminBookingPages(options=>page(options,'Staff'),{limit:500});
  expect(out.map(x=>x.id).sort()).toEqual(['z-aug-pending','z-july-cancelled','z-no-created']);
  const owner=await collectAdminBookingPages(options=>page(options),{limit:500});
  expect(owner).toHaveLength(508);expect(new Set(owner.map(x=>x.id)).size).toBe(508);
  expect(out.find(x=>x.id==='z-july-cancelled').bookingStatus).toBe('cancelled');
  expect(out.find(x=>x.id==='z-aug-pending').pendingReschedule).toBe(true);
});


test('stored id fields cannot collapse distinct booking documents or alter their canonical IDs',async()=>{
  await seed('first-real-id',{id:'spoofed-same-id'});
  await seed('second-real-id',{id:'spoofed-same-id'});
  const out=await collectAdminBookingPages(options=>page(options),{limit:1});
  expect(out.map(b=>b.id).sort()).toEqual(['first-real-id','second-real-id']);
});
