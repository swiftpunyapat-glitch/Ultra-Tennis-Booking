import { describe, test, expect, beforeAll, beforeEach } from 'vitest';
import { Timestamp } from 'firebase-admin/firestore';

const DATE='2027-05-17';
let db, opsHandler, accountingHandler, userActionHandler, cookies;

beforeAll(async()=>{
  if(!process.env.FIRESTORE_EMULATOR_HOST) throw new Error('Local Firestore emulator required');
  process.env.ADMIN_SESSION_SECRET='admin-cutover-local-secret';
  process.env.ADMIN_USERS_JSON=JSON.stringify({
    Art:{pin:'0000',role:'owner',branches:'*'},
    Boss:{pin:'0000',role:'ultra_admin',branches:'*'},
    Manager:{pin:'0000',role:'branch_manager',branches:['ladprao1']},
    ManagerOther:{pin:'0000',role:'branch_manager',branches:['chiangmai']},
    Staff:{pin:'0000',role:'branch_staff',branches:['ladprao1']},
    Other:{pin:'0000',role:'branch_staff',branches:['chiangmai']},
    View:{pin:'0000',role:'viewer',branches:['ladprao1']},
    Coach1:{pin:'0000',role:'coach',branches:['ladprao1']},
  });
  db=(await import('../api/_lib/firebase-admin.js')).getAdminDb();
  opsHandler=(await import('../api/admin-ops.js')).default;
  accountingHandler=(await import('../api/admin-edit-booking-accounting.js')).default;
  userActionHandler=(await import('../api/admin-user-action.js')).default;
  const {createSessionCookie}=await import('../api/_lib/admin-auth.js');
  cookies=Object.fromEntries(['Art','Boss','Manager','ManagerOther','Staff','Other','View','Coach1'].map(n=>[n,createSessionCookie(n).split(';')[0]]));
});

function makeReq(body,who){return{method:'POST',body,headers:who?{cookie:cookies[who]}:{},socket:{}};}
function makeRes(){const r={statusCode:null,body:null,headers:{}};r.status=c=>{r.statusCode=c;return r;};r.json=b=>{r.body=b;return r;};r.setHeader=(k,v)=>r.headers[k]=v;return r;}
async function call(handler,body,who){const r=makeRes();await handler(makeReq(body,who),r);return r;}
const slotId=t=>`room1_${DATE}_${t.replace(':','')}`;
async function open(t){await db.collection('available_slots').doc(slotId(t)).set({resourceId:'room1',branchId:'ladprao1',date:DATE,startTime:t,endTime:`${String(Number(t.slice(0,2))+1).padStart(2,'0')}:00`,status:'open'});}

async function seedUltraPassBooking(time,{bookingId=`pass_${time.replace(':','')}`,packageId=`pkg_${time.replace(':','')}`}={}){
  await open(time);
  const end=`${String(Number(time.slice(0,2))+1).padStart(2,'0')}:00`;
  const code=`PASS${time.replace(':','')}`;
  await db.collection('customer_packages').doc(packageId).set({
    branchId:'ladprao1',lineUserId:'UPASS1',packageType:'ultra_pass_10',packageName:'Ultra Pass 10 Hours',
    remainingMinutes:540,totalMinutes:600,status:'active',
  });
  await db.collection('bookings').doc(bookingId).set({
    bookingCode:code,resourceId:'room1',branchId:'ladprao1',bookingSlotIds:[slotId(time)],
    bookingType:'Ultra Pass 10 Hours',lineUserId:'UPASS1',customerName:'Pass User',customerPhone:'0800000001',
    date:DATE,startTime:time,endTime:end,durationMinutes:60,durationHours:1,
    packageId,usedPackageId:packageId,packageType:'ultra_pass_10',packageName:'Ultra Pass 10 Hours',
    packageMinutesUsed:60,createdVia:'server_pass',bookingStatus:'confirmed',paymentStatus:'package',
  });
  await db.collection('booking_slots').doc(slotId(time)).set({
    resourceId:'room1',branchId:'ladprao1',date:DATE,hour:time,slotSpanMinutes:60,
    bookingStatus:'confirmed',paymentStatus:'package',expiresAt:null,
  });
  await db.collection('booking_slot_claims').doc(slotId(time)).set({
    bookingId,bookingCode:code,branchId:'ladprao1',resourceId:'room1',status:'confirmed',
    date:DATE,hour:time,slotSpanMinutes:60,
  });
  return {bookingId,packageId,code};
}

async function seedTwoHourPaidBooking(start='10:00',{bookingId=`paid2h_${start.replace(':','')}`}={}){
  const startHour=Number(start.slice(0,2));
  const times=[start,`${String(startHour+1).padStart(2,'0')}:00`];
  const ids=times.map(slotId);
  const code=`PAID2H${start.replace(':','')}`;
  for(const time of times) await open(time);
  await db.collection('bookings').doc(bookingId).set({
    bookingCode:code,resourceId:'room1',branchId:'ladprao1',bookingSlotIds:ids,
    bookingType:'Court Booking',lineUserId:'U2H',customerName:'Two Hour User',customerPhone:'0800000002',
    date:DATE,startTime:start,endTime:`${String(startHour+2).padStart(2,'0')}:00`,
    durationMinutes:120,durationHours:2,price:700,amount:700,
    bookingStatus:'confirmed',paymentStatus:'paid',
  });
  for(const time of times){
    await db.collection('booking_slots').doc(slotId(time)).set({
      resourceId:'room1',branchId:'ladprao1',date:DATE,hour:time,slotSpanMinutes:60,
      bookingStatus:'confirmed',paymentStatus:'paid',expiresAt:null,
    });
    await db.collection('booking_slot_claims').doc(slotId(time)).set({
      bookingId,bookingCode:code,branchId:'ladprao1',resourceId:'room1',status:'confirmed',
      date:DATE,hour:time,slotSpanMinutes:60,
    });
  }
  return {bookingId,code,ids};
}

async function wipe(){
  for(const c of ['bookings','booking_slots','booking_slot_claims','available_slots','holidays','customer_packages','customer_package_logs','registered_users','audit_logs']){
    const s=await db.collection(c).get();await Promise.all(s.docs.map(d=>d.ref.delete()));
  }
  await db.collection('system_settings').doc('pricing').set({});
}
beforeEach(wipe);

describe('admin_read authentication, scope, limits, and projections',()=>{
  beforeEach(async()=>{
    const batch=db.batch();
    for(let i=0;i<502;i++){
      const branchId=i%2?'chiangmai':'ladprao1';
      batch.set(db.collection('bookings').doc(`read_${String(i).padStart(3,'0')}`),{
        bookingCode:`READ${i}`,branchId,resourceId:'room1',lineUserId:`U${i}`,
        customerName:`Customer ${i}`,customerPhone:`08${String(i).padStart(8,'0')}`,
        slipUrl:`https://example.invalid/${i}`,date:DATE,startTime:'10:00',endTime:'11:00',
        bookingStatus:'pending_review',paymentStatus:'pending_review',createdAt:Timestamp.fromMillis(Date.now()+i),
        secretUnprojectedField:'must-never-leave-server',
      });
      if(i===499){await batch.commit();break;}
    }
    const tail=db.batch();
    for(let i=500;i<502;i++) tail.set(db.collection('bookings').doc(`read_${i}`),{
      bookingCode:`READ${i}`,branchId:'ladprao1',resourceId:'room1',lineUserId:`U${i}`,
      customerName:`Customer ${i}`,customerPhone:'0800000000',slipUrl:`https://example.invalid/${i}`,
      date:DATE,startTime:'10:00',endTime:'11:00',bookingStatus:'pending_review',paymentStatus:'pending_review',
      createdAt:Timestamp.fromMillis(Date.now()+i),secretUnprojectedField:'must-never-leave-server',
    });
    await tail.commit();
  });

  test('requires a session and caps oversized requests at 500',async()=>{
    expect((await call(opsHandler,{action:'admin_read',resource:'bookings'})).statusCode).toBe(401);
    const out=await call(opsHandler,{action:'admin_read',resource:'bookings',limit:999},'Art');
    expect(out.statusCode).toBe(200);expect(out.body.limit).toBe(500);expect(out.body.items).toHaveLength(500);
    expect(out.body.nextCursor).toBeTruthy();
  });

  test('viewer is branch-scoped and receives neither PII, slip URL, nor unprojected fields',async()=>{
    const out=await call(opsHandler,{action:'admin_read',resource:'bookings',limit:100},'View');
    expect(out.statusCode).toBe(200);
    expect(out.body.items.length).toBeGreaterThan(0);
    for(const item of out.body.items){
      expect(item.branchId).toBe('ladprao1');
      expect(item).not.toHaveProperty('customerName');
      expect(item).not.toHaveProperty('customerPhone');
      expect(item).not.toHaveProperty('slipUrl');
      expect(item).not.toHaveProperty('secretUnprojectedField');
    }
  });

  test('staff gets branch contact data without slip URL; manager gets slip review projection',async()=>{
    const staff=await call(opsHandler,{action:'admin_read',resource:'bookings',limit:20},'Staff');
    expect(staff.statusCode).toBe(200);
    expect(staff.body.items.every(x=>x.branchId==='ladprao1')).toBe(true);
    expect(staff.body.items[0]).toHaveProperty('customerName');
    expect(staff.body.items[0]).not.toHaveProperty('slipUrl');
    expect(staff.body.items[0]).not.toHaveProperty('secretUnprojectedField');

    const manager=await call(opsHandler,{action:'admin_read',resource:'bookings',limit:20},'Manager');
    expect(manager.statusCode).toBe(200);
    expect(manager.body.items.every(x=>x.branchId==='ladprao1')).toBe(true);
    expect(manager.body.items[0]).toHaveProperty('customerName');
    expect(manager.body.items[0]).toHaveProperty('slipUrl');
  });
});

describe('locked owner role and capability matrix',()=>{
  const financialBodies={
    approve_slip:{operation:'approve_slip',bookingId:'missing'},
    reject_payment:{operation:'reject_payment',bookingId:'missing'},
    mark_paid:{operation:'mark_paid',bookingId:'missing',amount:350,paymentMethod:'cash'},
    refund:{operation:'refund',bookingId:'missing',refundAmount:350,refundMode:'full_refund',refundReason:'customer_request'},
    accounting_edit:{operation:'accounting_edit',bookingId:'missing',accountingType:'normal_paid',reason:'test'},
    coach_payout_paid:{operation:'coach_payout_paid',bookingId:'missing'},
  };

  test('branch staff and viewer are denied every financial approval action',async()=>{
    for(const [name,body] of Object.entries(financialBodies)){
      expect((await call(accountingHandler,body,'Staff')).statusCode,`staff ${name}`).toBe(403);
      expect((await call(accountingHandler,body,'View')).statusCode,`viewer ${name}`).toBe(403);
    }
    const passActions=[
      {action:'add_pass_to_registered_user'},
      {action:'adjust_pass_minutes'},
      {action:'deactivate_pass'},
      {action:'list_pending_pass_purchases'},
      {action:'approve_pass_purchase'},
      {action:'reject_pass_purchase'},
      {action:'save_special_promotion'},
    ];
    for(const body of passActions){
      expect((await call(userActionHandler,body,'Staff')).statusCode,`staff ${body.action}`).toBe(403);
      expect((await call(userActionHandler,body,'View')).statusCode,`viewer ${body.action}`).toBe(403);
    }
  });

  test('branch manager retains assigned-branch slip review while full-only actions stay restricted',async()=>{
    expect((await call(accountingHandler,financialBodies.approve_slip,'Manager')).statusCode).toBe(404);
    expect((await call(accountingHandler,financialBodies.reject_payment,'Manager')).statusCode).toBe(404);
    expect((await call(accountingHandler,financialBodies.accounting_edit,'Manager')).statusCode).toBe(403);
    expect((await call(accountingHandler,{operation:'delete_booking',bookingId:'missing'},'Manager')).statusCode).toBe(403);
  });

  test('owner and ultra_admin pass full-authority gates',async()=>{
    for(const who of ['Art','Boss']){
      for(const [name,body] of Object.entries(financialBodies)){
        expect((await call(accountingHandler,body,who)).statusCode,`${who} ${name}`).not.toBe(403);
      }
      expect((await call(accountingHandler,{operation:'delete_booking',bookingId:'missing'},who)).statusCode).not.toBe(403);
      expect((await call(userActionHandler,{action:'save_special_promotion'},who)).statusCode).not.toBe(403);
    }
  });

  test('branch staff manual booking is unpaid-only',async()=>{
    await open('15:00');
    const base={operation:'manual_create',customerName:'Staff Manual',customerPhone:'0855555555',date:DATE,startTime:'15:00'};
    expect((await call(accountingHandler,{...base,bookingType:'Paid Outside'},'Staff')).statusCode).toBe(403);
    expect((await call(accountingHandler,{...base,bookingType:'Ultra Pass Manual'},'Staff')).statusCode).toBe(403);
    const unpaid=await call(accountingHandler,{...base,bookingType:'Pay at Counter'},'Staff');
    expect(unpaid.statusCode).toBe(200);
    expect(unpaid.body.booking.paymentStatus).toBe('unpaid');
  });

  test('coach schedule action is pinned to the coach session identity',async()=>{
    const today=new Date(Date.now()+7*3600_000).toISOString().slice(0,10);
    await db.collection('bookings').doc('coach-own').set({branchId:'ladprao1',date:today,startTime:'10:00',endTime:'11:00',coachId:'Coach1',bookingStatus:'confirmed',customerName:'Own'});
    await db.collection('bookings').doc('coach-other').set({branchId:'ladprao1',date:today,startTime:'11:00',endTime:'12:00',coachId:'Coach2',bookingStatus:'confirmed',customerName:'Other'});
    const out=await call(opsHandler,{action:'coach_my_bookings',coachId:'Coach2'},'Coach1');
    expect(out.statusCode).toBe(200);
    expect(out.body.coachId).toBe('Coach1');
    expect(out.body.bookings.map(x=>x.id)).toEqual(['coach-own']);
    expect((await call(opsHandler,{action:'admin_read',resource:'bookings'},'Coach1')).statusCode).toBe(403);
  });
});

describe('manual booking and calendar sync cutover',()=>{
  test('manual create enforces role/branch and derives price and payment state server-side',async()=>{
    await open('10:00');
    const body={operation:'manual_create',customerName:'Manual',customerPhone:'0811111111',date:DATE,startTime:'10:00',bookingType:'Manual Single Use',price:1,paymentStatus:'paid'};
    expect((await call(accountingHandler,body,'View')).statusCode).toBe(403);
    expect((await call(accountingHandler,body,'Other')).statusCode).toBe(403);
    const made=await call(accountingHandler,body,'Staff');
    expect(made.statusCode).toBe(200);
    expect(made.body.booking.paymentStatus).toBe('unpaid');
    expect(made.body.booking.price).toBeGreaterThan(1);
    const claim=(await db.collection('booking_slot_claims').doc(slotId('10:00')).get()).data();
    expect(claim).toMatchObject({bookingId:made.body.booking.id,branchId:'ladprao1',resourceId:'room1',status:'confirmed'});
  });

  test('manual create supports independent 30-minute cells without false hour conflicts',async()=>{
    await open('11:00');
    const base={operation:'manual_create',customerName:'Half Manual',customerPhone:'0822222222',date:DATE,bookingType:'Manual Single Use',durationMinutes:30};
    const secondHalf=await call(accountingHandler,{...base,startTime:'11:30'},'Staff');
    expect(secondHalf.statusCode).toBe(200);
    expect(secondHalf.body.booking).toMatchObject({startTime:'11:30',endTime:'12:00',durationMinutes:30,durationHours:0.5,price:200});
    expect((await db.collection('booking_slots').doc(slotId('11:30')).get()).data()).toMatchObject({hour:'11:30',slotSpanMinutes:30});

    const firstHalf=await call(accountingHandler,{...base,customerPhone:'0833333333',startTime:'11:00'},'Staff');
    expect(firstHalf.statusCode).toBe(200);
    expect(firstHalf.body.booking.bookingSlotIds).toEqual([slotId('11:00')]);
    expect((await db.collection('booking_slots').doc(slotId('11:00')).get()).data()).toMatchObject({hour:'11:00',slotSpanMinutes:30});

    const overlappingHour=await call(accountingHandler,{...base,customerPhone:'0844444444',startTime:'11:00',durationMinutes:60},'Staff');
    expect(overlappingHour.statusCode).toBe(409);
  });

  test('manual one-hour booking can cross an hour boundary using two half cells',async()=>{
    await open('13:00');
    await open('14:00');
    const base={operation:'manual_create',customerName:'Cross Manual',date:DATE,bookingType:'Manual Single Use'};
    const firstHalf=await call(accountingHandler,{...base,customerPhone:'0851111111',startTime:'13:00',durationMinutes:30},'Staff');
    expect(firstHalf.statusCode).toBe(200);

    const crossed=await call(accountingHandler,{...base,customerPhone:'0852222222',startTime:'13:30',durationMinutes:60},'Staff');
    expect(crossed.statusCode).toBe(200);
    expect(crossed.body.booking).toMatchObject({startTime:'13:30',endTime:'14:30',durationMinutes:60,durationHours:1,price:400});
    expect(crossed.body.booking.bookingSlotIds).toEqual([slotId('13:30'),slotId('14:00')]);
    expect((await db.collection('booking_slots').doc(slotId('13:30')).get()).data()).toMatchObject({hour:'13:30',slotSpanMinutes:30});
    expect((await db.collection('booking_slots').doc(slotId('14:00')).get()).data()).toMatchObject({hour:'14:00',slotSpanMinutes:30});

    const overlap=await call(accountingHandler,{...base,customerPhone:'0853333333',startTime:'14:00',durationMinutes:30},'Staff');
    expect(overlap.statusCode).toBe(409);

    const parked=await call(accountingHandler,{operation:'reschedule_park',bookingId:crossed.body.booking.id},'Staff');
    expect(parked.statusCode).toBe(200);
    expect((await db.collection('booking_slots').doc(slotId('13:30')).get()).exists).toBe(false);
    expect((await db.collection('booking_slots').doc(slotId('14:00')).get()).exists).toBe(false);
  });

  test('manual half-hour validation rejects late-night and invalid alignment',async()=>{
    await open('05:00');
    const base={operation:'manual_create',customerName:'Half Manual',customerPhone:'0822222222',date:DATE,bookingType:'Manual Single Use',durationMinutes:30};
    expect((await call(accountingHandler,{...base,startTime:'05:30'},'Staff')).statusCode).toBe(409);
    expect((await call(accountingHandler,{...base,startTime:'10:15'},'Staff')).statusCode).toBe(400);
    expect((await call(accountingHandler,{...base,startTime:'22:30',durationMinutes:60},'Staff')).statusCode).toBe(409);
  });

  test('calendar mutation has an explicit allowlist and cannot alter price or ownership',async()=>{
    await db.collection('bookings').doc('cal1').set({branchId:'ladprao1',lineUserId:'OWNER_UID',price:350,bookingStatus:'confirmed',paymentStatus:'paid'});
    const body={operation:'calendar_sync_fields',bookingId:'cal1',googleCalendarSyncStatus:'created',googleCalendarEventId:'evt1',googleCalendarHtmlLink:'https://calendar.google.com/x',googleCalendarSyncError:null,price:1,lineUserId:'ATTACKER',paymentStatus:'unpaid'};
    expect((await call(accountingHandler,body,'View')).statusCode).toBe(403);
    expect((await call(accountingHandler,body,'Other')).statusCode).toBe(403);
    const out=await call(accountingHandler,body,'Staff');
    expect(out.statusCode).toBe(200);
    const saved=(await db.collection('bookings').doc('cal1').get()).data();
    expect(saved).toMatchObject({price:350,lineUserId:'OWNER_UID',paymentStatus:'paid',googleCalendarSyncStatus:'created',googleCalendarEventId:'evt1'});
    const bad=await call(accountingHandler,{operation:'calendar_sync_fields',bookingId:'cal1',googleCalendarSyncStatus:'not-allowlisted'},'Staff');
    expect(bad.statusCode).toBe(400);
  });
});

describe('admin operational mutation matrix',()=>{
  test('slot and holiday mutations enforce role and branch scope',async()=>{
    expect((await call(opsHandler,{action:'slot_toggle',date:DATE,hour:12,op:'open'},'View')).statusCode).toBe(403);
    expect((await call(opsHandler,{action:'slot_toggle',date:DATE,hour:12,op:'open'},'Other')).statusCode).toBe(403);
    expect((await call(opsHandler,{action:'slot_toggle',date:DATE,hour:12,op:'open'},'Staff')).statusCode).toBe(200);
    expect((await db.collection('available_slots').doc(slotId('12:00')).get()).data().status).toBe('open');
    expect((await call(opsHandler,{action:'holiday_set',date:DATE,isHoliday:true,name:'Test'},'Staff')).statusCode).toBe(200);
    expect((await db.collection('holidays').doc(DATE).get()).data().isHoliday).toBe(true);
  });

  test('package adjustment and deactivation are branch-scoped and audited',async()=>{
    await db.collection('customer_packages').doc('pkg1').set({branchId:'ladprao1',packageType:'ultra_pass_10',packageName:'Ultra',remainingMinutes:60,status:'active',lineUserId:'U1'});
    const adjust={action:'adjust_pass_minutes',passId:'pkg1',adjustAction:'add_minutes',value:60,reason:'test'};
    expect((await call(userActionHandler,adjust,'Staff')).statusCode).toBe(403);
    expect((await call(userActionHandler,adjust,'ManagerOther')).statusCode).toBe(403);
    expect((await call(userActionHandler,adjust,'Manager')).statusCode).toBe(200);
    expect((await db.collection('customer_packages').doc('pkg1').get()).data().remainingMinutes).toBe(120);
    expect((await call(userActionHandler,{action:'deactivate_pass',passId:'pkg1'},'Staff')).statusCode).toBe(403);
    expect((await call(userActionHandler,{action:'deactivate_pass',passId:'pkg1'},'ManagerOther')).statusCode).toBe(403);
    expect((await call(userActionHandler,{action:'deactivate_pass',passId:'pkg1'},'Manager')).statusCode).toBe(200);
    expect((await db.collection('customer_packages').doc('pkg1').get()).data().status).toBe('inactive');
  });

  test('pending reschedule releases and reassigns private claims atomically',async()=>{
    await open('10:00');await open('11:00');
    const made=await call(accountingHandler,{operation:'manual_create',customerName:'Move',customerPhone:'0822222222',date:DATE,startTime:'10:00',bookingType:'Pay at Counter'},'Staff');
    expect(made.statusCode).toBe(200);const bookingId=made.body.booking.id;
    const parked=await call(accountingHandler,{operation:'reschedule_park',bookingId},'Staff');
    expect(parked.statusCode).toBe(200);
    expect((await db.collection('booking_slot_claims').doc(slotId('10:00')).get()).exists).toBe(false);
    const assigned=await call(accountingHandler,{operation:'reschedule_assign',bookingId,newDate:DATE,newStartTime:'11:00'},'Staff');
    expect(assigned.statusCode).toBe(200);
    expect((await db.collection('booking_slot_claims').doc(slotId('11:00')).get()).data().bookingId).toBe(bookingId);
  });

  test('two-hour pending reschedule clears active slot IDs, releases both hours, and assigns both new IDs',async()=>{
    const {bookingId,ids}=await seedTwoHourPaidBooking('10:00');
    const parked=await call(accountingHandler,{operation:'reschedule_park',bookingId},'Staff');
    expect(parked.statusCode).toBe(200);
    const pending=(await db.collection('bookings').doc(bookingId).get()).data();
    expect(pending.bookingSlotIds).toEqual([]);
    expect(pending.pendingRescheduleFromSlotIds).toEqual(ids);
    for(const id of ids){
      expect((await db.collection('booking_slots').doc(id).get()).exists).toBe(false);
      expect((await db.collection('booking_slot_claims').doc(id).get()).exists).toBe(false);
    }

    await open('12:00');await open('13:00');
    const assigned=await call(accountingHandler,{operation:'reschedule_assign',bookingId,newDate:DATE,newStartTime:'12:00'},'Staff');
    expect(assigned.statusCode).toBe(200);
    const moved=(await db.collection('bookings').doc(bookingId).get()).data();
    expect(moved.bookingSlotIds).toEqual([slotId('12:00'),slotId('13:00')]);
    expect(moved.startTime).toBe('12:00');
    expect(moved.endTime).toBe('14:00');
    expect((await db.collection('booking_slot_claims').doc(slotId('12:00')).get()).data().bookingId).toBe(bookingId);
    expect((await db.collection('booking_slot_claims').doc(slotId('13:00')).get()).data().bookingId).toBe(bookingId);
  });

  test('cancelling a two-hour pending reschedule restores both original active slot IDs',async()=>{
    const {bookingId,ids}=await seedTwoHourPaidBooking('10:00',{bookingId:'paid2h_restore'});
    expect((await call(accountingHandler,{operation:'reschedule_park',bookingId},'Staff')).statusCode).toBe(200);
    const restored=await call(accountingHandler,{operation:'reschedule_cancel',bookingId},'Staff');
    expect(restored.statusCode).toBe(200);
    expect(restored.body.restored).toBe(true);
    const booking=(await db.collection('bookings').doc(bookingId).get()).data();
    expect(booking.bookingSlotIds).toEqual(ids);
    expect(booking.bookingStatus).toBe('confirmed');
    for(const id of ids){
      expect((await db.collection('booking_slots').doc(id).get()).exists).toBe(true);
      expect((await db.collection('booking_slot_claims').doc(id).get()).data().bookingId).toBe(bookingId);
    }
  });

  test('Ultra Pass reschedule removes the old package slot and does not alter pass minutes',async()=>{
    const {bookingId,packageId}=await seedUltraPassBooking('10:00');
    await open('11:00');
    const moved=await call(accountingHandler,{operation:'reschedule_assign',bookingId,newDate:DATE,newStartTime:'11:00'},'Art');
    expect(moved.statusCode).toBe(200);
    expect((await db.collection('booking_slots').doc(slotId('10:00')).get()).exists).toBe(false);
    expect((await db.collection('booking_slot_claims').doc(slotId('10:00')).get()).exists).toBe(false);
    expect((await db.collection('booking_slot_claims').doc(slotId('11:00')).get()).data().bookingId).toBe(bookingId);
    expect((await db.collection('customer_packages').doc(packageId).get()).data().remainingMinutes).toBe(540);
  });

  test('Ultra Pass pending-reschedule also removes its old package slot',async()=>{
    const {bookingId,packageId}=await seedUltraPassBooking('10:00');
    const parked=await call(accountingHandler,{operation:'reschedule_park',bookingId},'Art');
    expect(parked.statusCode).toBe(200);
    expect((await db.collection('booking_slots').doc(slotId('10:00')).get()).exists).toBe(false);
    expect((await db.collection('booking_slot_claims').doc(slotId('10:00')).get()).exists).toBe(false);
    expect((await db.collection('customer_packages').doc(packageId).get()).data().remainingMinutes).toBe(540);
  });

  test('Ultra Pass cancel restores minutes once and releases public/private slot state',async()=>{
    const {bookingId,packageId}=await seedUltraPassBooking('12:00');
    const cancelled=await call(accountingHandler,{operation:'reject_payment',bookingId,reason:'customer requested'},'Art');
    expect(cancelled.statusCode).toBe(200);
    expect(cancelled.body.booking.restoredMinutes).toBe(60);
    expect((await db.collection('customer_packages').doc(packageId).get()).data().remainingMinutes).toBe(600);
    expect((await db.collection('booking_slots').doc(slotId('12:00')).get()).exists).toBe(false);
    expect((await db.collection('booking_slot_claims').doc(slotId('12:00')).get()).exists).toBe(false);
    expect((await db.collection('bookings').doc(bookingId).get()).data()).toMatchObject({
      bookingStatus:'cancelled',paymentStatus:'package',packageMinutesRestored:60,
    });
    const logs=await db.collection('customer_package_logs').where('bookingId','==',bookingId).get();
    expect(logs.docs.map(d=>d.data().action)).toContain('restore_minutes');

    const repeated=await call(accountingHandler,{operation:'reject_payment',bookingId},'Art');
    expect(repeated.statusCode).toBe(409);
    expect((await db.collection('customer_packages').doc(packageId).get()).data().remainingMinutes).toBe(600);
  });

  test('Art delete atomically releases an Ultra Pass slot and restores its minutes',async()=>{
    const {bookingId,packageId}=await seedUltraPassBooking('14:00');
    const deleted=await call(accountingHandler,{operation:'delete_booking',bookingId},'Art');
    expect(deleted.statusCode).toBe(200);
    expect(deleted.body.restoredMinutes).toBe(60);
    expect((await db.collection('customer_packages').doc(packageId).get()).data().remainingMinutes).toBe(600);
    expect((await db.collection('booking_slots').doc(slotId('14:00')).get()).exists).toBe(false);
    expect((await db.collection('booking_slot_claims').doc(slotId('14:00')).get()).exists).toBe(false);
    expect((await db.collection('bookings').doc(bookingId).get()).exists).toBe(false);
  });

  test('accounting and delete paths authorize by branch and use private claims',async()=>{
    await open('13:00');await open('14:00');
    const first=await call(accountingHandler,{operation:'manual_create',customerName:'Review',customerPhone:'0833333333',date:DATE,startTime:'13:00',bookingType:'Pay at Counter'},'Staff');
    const bookingId=first.body.booking.id;
    const publicSlot=(await db.collection('booking_slots').doc(slotId('13:00')).get()).data();
    expect(publicSlot).not.toHaveProperty('bookingId');
    expect((await call(accountingHandler,{operation:'accounting_edit',bookingId,accountingType:'pending_review',reason:'test'},'Art')).statusCode).toBe(200);
    expect((await db.collection('booking_slot_claims').doc(slotId('13:00')).get()).data().status).toBe('pending_review');
    expect((await call(accountingHandler,{operation:'accounting_edit',bookingId,accountingType:'rejected',reason:'test'},'Art')).statusCode).toBe(200);
    expect((await db.collection('booking_slots').doc(slotId('13:00')).get()).exists).toBe(false);
    expect((await db.collection('booking_slot_claims').doc(slotId('13:00')).get()).exists).toBe(false);

    const second=await call(accountingHandler,{operation:'manual_create',customerName:'Delete',customerPhone:'0844444444',date:DATE,startTime:'14:00',bookingType:'Pay at Counter'},'Staff');
    expect((await call(accountingHandler,{operation:'delete_booking',bookingId:second.body.booking.id},'Other')).statusCode).toBe(403);
    expect((await call(accountingHandler,{operation:'delete_booking',bookingId:second.body.booking.id},'Art')).statusCode).toBe(200);
    expect((await db.collection('booking_slot_claims').doc(slotId('14:00')).get()).exists).toBe(false);
    expect((await db.collection('bookings').doc(second.body.booking.id).get()).exists).toBe(false);
  });
});
