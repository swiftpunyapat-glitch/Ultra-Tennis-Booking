import { describe, test, expect, beforeAll, beforeEach, vi } from 'vitest';

const UID = 'U_OWNER_REVIEW_FLOW_USER';
const DATE = '2027-04-12';
const IP = '198.51.100.77';

vi.mock('firebase-admin/auth', async (orig) => {
  const actual = await orig();
  return { ...actual, getAuth: () => ({ verifyIdToken: async () => ({ uid:UID }) }) };
});

let db, bookingHandler, slipHandler, accountingHandler, adminCookie;

beforeAll(async () => {
  if (!process.env.FIRESTORE_EMULATOR_HOST) {
    throw new Error('Refusing to run without the local Firestore emulator');
  }
  process.env.ADMIN_SESSION_SECRET = 'owner-review-local-test-secret';
  process.env.ADMIN_USERS_JSON = JSON.stringify({ Art:{ pin:'0000', role:'owner', branches:'*' } });
  const fa = await import('../api/_lib/firebase-admin.js');
  db = fa.getAdminDb();
  bookingHandler = (await import('../api/booking.js')).default;
  slipHandler = (await import('../api/slip-verify.js')).default;
  accountingHandler = (await import('../api/admin-edit-booking-accounting.js')).default;
  const { createSessionCookie } = await import('../api/_lib/admin-auth.js');
  adminCookie = createSessionCookie('Art').split(';')[0];
});

function req(body, { admin=false, ip=IP }={}) {
  return {
    method:'POST', body,
    headers:{ 'x-forwarded-for':ip, ...(admin?{cookie:adminCookie}:{}) },
    socket:{ remoteAddress:ip },
  };
}
function res() {
  const r={ statusCode:null, body:null, headers:{} };
  r.status=c=>{r.statusCode=c;return r;};
  r.json=b=>{r.body=b;return r;};
  r.setHeader=(k,v)=>{r.headers[k]=v;};
  return r;
}
async function call(handler, body, opts) {
  const out=res(); await handler(req(body,opts),out); return out;
}
const slotId = time => `room1_${DATE}_${time.replace(':','')}`;
const storageUrl = id => `https://firebasestorage.googleapis.com/v0/b/ultra-tennis-booking.appspot.com/o/payment_slips%2F${id}.jpg?alt=media&token=test`;

async function wipe() {
  for (const name of [
    'bookings','booking_slots','booking_slot_claims','guest_booking_access',
    'rate_limits','idempotency_records','audit_logs','available_slots','holidays',
  ]) {
    const snap=await db.collection(name).get();
    await Promise.all(snap.docs.map(d=>d.ref.delete()));
  }
  await db.collection('system_settings').doc('features').set({
    useServerSlipSubmit:true, useServerPassBooking:true,
  },{merge:true});
  await db.collection('system_settings').doc('pricing').set({});
}

beforeEach(wipe);

async function open(time) {
  await db.collection('available_slots').doc(slotId(time)).set({
    resourceId:'room1', branchId:'ladprao1', date:DATE,
    startTime:time, endTime:`${String(Number(time.slice(0,2))+1).padStart(2,'0')}:00`, status:'open',
  });
}

async function createGuest(time='10:00') {
  await open(time);
  return call(bookingHandler, {
    action:'create', date:DATE, startTime:time, durationMinutes:60,
    customerName:'Guest Test', customerPhone:'0812345678', lineUserId:'spoofed-client-id',
  });
}

async function submitGuestSlip(created, suffix='one', opts) {
  return call(slipHandler, {
    action:'submit_slip', bookingId:created.body.booking.id,
    bookingCode:created.body.booking.bookingCode,
    guestToken:created.body.guestAccessToken,
    slipUrl:storageUrl(suffix), idempotencyKey:`slip-${suffix}`,
  },opts);
}

describe('OR-05 atomic guest access issuance', () => {
  test('booking, public slot, private claim, and guest access commit together', async () => {
    const created=await createGuest();
    expect(created.statusCode).toBe(200);
    expect(created.body.guestAccessToken).toMatch(/^[A-Za-z0-9_-]{40,}$/);
    expect(created.body.booking.lineUserId).toBe('guest');
    const id=created.body.booking.id;
    const [booking,slot,claim,access]=await Promise.all([
      db.collection('bookings').doc(id).get(),
      db.collection('booking_slots').doc(slotId('10:00')).get(),
      db.collection('booking_slot_claims').doc(slotId('10:00')).get(),
      db.collection('guest_booking_access').doc(id).get(),
    ]);
    expect([booking.exists,slot.exists,claim.exists,access.exists]).toEqual([true,true,true,true]);
    expect(JSON.stringify(access.data())).not.toContain(created.body.guestAccessToken);
    expect(claim.data()).toMatchObject({bookingId:id,branchId:'ladprao1',resourceId:'room1',status:'pending_payment'});
  });

  test('a failed booking transaction leaves no partial guest capability or booking state', async () => {
    await open('10:00');
    await db.collection('available_slots').doc(slotId('10:00')).update({status:'closed'});
    const failed=await call(bookingHandler, {
      action:'create',date:DATE,startTime:'10:00',durationMinutes:60,
      customerName:'Rollback Test',customerPhone:'0800000000',
    });
    expect(failed.statusCode).toBe(409);
    for (const name of ['bookings','booking_slots','booking_slot_claims','guest_booking_access']) {
      expect((await db.collection(name).get()).size,name).toBe(0);
    }
  });
});

describe('OR-03 pending_review remains occupied', () => {
  test('slip submission is atomic and blocks replacement after the original expiry', async () => {
    const created=await createGuest();
    const submitted=await submitGuestSlip(created,'pending-review');
    expect(submitted.statusCode).toBe(200);
    expect(submitted.body).toMatchObject({paymentStatus:'pending_review',bookingStatus:'pending_review'});

    const old=new Date(Date.now()-3600_000);
    await db.collection('bookings').doc(created.body.booking.id).update({paymentExpiresAt:old});
    await db.collection('booking_slots').doc(slotId('10:00')).update({expiresAt:old});

    const replacement=await call(bookingHandler, {
      action:'create',date:DATE,startTime:'10:00',durationMinutes:60,
      customerName:'Second Customer',customerPhone:'0899999999',
    });
    expect(replacement.statusCode).toBe(409);
    expect(replacement.body.code).toBe('SLOT');
    expect((await db.collection('bookings').get()).size).toBe(1);
  });

  test('approval confirms booking, slot, and claim; rejection releases slot and claim', async () => {
    const approveCreated=await createGuest('10:00');
    expect((await submitGuestSlip(approveCreated,'approve')).statusCode).toBe(200);
    const approved=await call(accountingHandler,{operation:'approve_slip',bookingId:approveCreated.body.booking.id},{admin:true});
    expect(approved.statusCode).toBe(200);
    expect((await db.collection('bookings').doc(approveCreated.body.booking.id).get()).data()).toMatchObject({bookingStatus:'confirmed',paymentStatus:'paid'});
    expect((await db.collection('booking_slots').doc(slotId('10:00')).get()).data()).toMatchObject({bookingStatus:'confirmed',paymentStatus:'paid'});
    expect((await db.collection('booking_slot_claims').doc(slotId('10:00')).get()).data().status).toBe('confirmed');

    const rejectCreated=await createGuest('11:00');
    expect((await submitGuestSlip(rejectCreated,'reject')).statusCode).toBe(200);
    const rejected=await call(accountingHandler,{operation:'reject_payment',bookingId:rejectCreated.body.booking.id},{admin:true});
    expect(rejected.statusCode).toBe(200);
    expect((await db.collection('booking_slots').doc(slotId('11:00')).get()).data()).toMatchObject({bookingStatus:'cancelled',paymentStatus:'rejected'});
    expect((await db.collection('booking_slot_claims').doc(slotId('11:00')).get()).exists).toBe(false);
  });
});

describe('OR-04 bounded guest rate-limit keys', () => {
  test('100 random tokens for one IP and one booking use one global invalid bucket', async () => {
    const created=await createGuest();
    const bookingId=created.body.booking.id;
    for(let i=0;i<100;i++) {
      await call(bookingHandler,{action:'guest_booking',bookingId,guestToken:`invalid-token-${String(i).padStart(4,'0')}-xxxxxxxxxxxxxxxxxxxx`});
    }
    let docs=await db.collection('rate_limits').get();
    expect(docs.size).toBe(1);

    await call(bookingHandler,{
      action:'cancel_pending',bookingId,bookingCode:created.body.booking.bookingCode,
      guestToken:'invalid-token-cancel-xxxxxxxxxxxxxxxxxxxx',
    });
    await call(slipHandler,{
      action:'submit_slip',bookingId,bookingCode:created.body.booking.bookingCode,
      guestToken:'invalid-token-slip-xxxxxxxxxxxxxxxxxxxx',slipUrl:storageUrl('invalid'),idempotencyKey:'invalid-slip-key',
    });
    docs=await db.collection('rate_limits').get();
    expect(docs.size).toBe(1);
    expect(JSON.stringify(docs.docs.map(d=>d.id))).not.toContain('invalid-token');
  });

  test('100 random tokens with 100 random booking IDs from one IP remain globally bounded', async () => {
    const ip='198.51.100.78';
    for(let i=0;i<100;i++){
      await call(bookingHandler,{
        action:'guest_booking',bookingId:`random-booking-${String(i).padStart(3,'0')}`,
        guestToken:`invalid-random-token-${String(i).padStart(3,'0')}-xxxxxxxxxxxxxxxx`,
      },{ip});
    }
    const docs=await db.collection('rate_limits').get();
    expect(docs.size).toBe(1);
    expect(docs.docs[0].data().bucket).toBe('guestInvalid');
  });

  test('a blocked global IP creates no new per-booking documents across read, cancel, and slip', async () => {
    const ip='198.51.100.79';
    for(let i=0;i<6;i++){
      await call(bookingHandler,{
        action:'guest_booking',bookingId:`block-${i}`,
        guestToken:`invalid-block-token-${i}-xxxxxxxxxxxxxxxxxxxx`,
      },{ip});
    }
    const before=await db.collection('rate_limits').get();
    const ids=before.docs.map(d=>d.id).sort();
    expect(before.size).toBe(1);

    const read=await call(bookingHandler,{action:'guest_booking',bookingId:'new-read-id',guestToken:'invalid-read-token-xxxxxxxxxxxxxxxxxxxx'},{ip});
    const cancel=await call(bookingHandler,{action:'cancel_pending',bookingId:'new-cancel-id',bookingCode:'CODE',guestToken:'invalid-cancel-token-xxxxxxxxxxxxxxxx'},{ip});
    const slip=await call(slipHandler,{action:'submit_slip',bookingId:'new-slip-id',bookingCode:'CODE',guestToken:'invalid-slip-token-xxxxxxxxxxxxxxxxxx',slipUrl:storageUrl('blocked'),idempotencyKey:'blocked-slip-key'},{ip});
    expect([read.statusCode,cancel.statusCode,slip.statusCode]).toEqual([429,429,429]);
    const after=await db.collection('rate_limits').get();
    expect(after.docs.map(d=>d.id).sort()).toEqual(ids);
  });

  test('valid guest read, cancel, and slip retain per-IP+booking limits', async () => {
    const readBooking=await createGuest('10:00');
    const read=await call(bookingHandler,{action:'guest_booking',bookingId:readBooking.body.booking.id,guestToken:readBooking.body.guestAccessToken},{ip:'198.51.100.80'});
    expect(read.statusCode).toBe(200);

    const cancelBooking=await createGuest('11:00');
    const cancel=await call(bookingHandler,{action:'cancel_pending',bookingId:cancelBooking.body.booking.id,bookingCode:cancelBooking.body.booking.bookingCode,guestToken:cancelBooking.body.guestAccessToken},{ip:'198.51.100.81'});
    expect(cancel.statusCode).toBe(200);

    const slipBooking=await createGuest('12:00');
    const slip=await submitGuestSlip(slipBooking,'valid-rate-limit',{ip:'198.51.100.82'});
    expect(slip.statusCode).toBe(200);

    const docs=await db.collection('rate_limits').get();
    expect(docs.size).toBe(3);
    expect(docs.docs.map(d=>d.data().bucket).sort()).toEqual(['guestMutation','guestMutation','guestRead']);
  });

  test('oversized credentials fail before hashing or lookup', async () => {
    const created=await createGuest();
    const out=await call(bookingHandler,{
      action:'guest_booking',bookingId:created.body.booking.id,guestToken:'x'.repeat(129),
    });
    expect(out.statusCode).toBe(400);
    expect((await db.collection('rate_limits').get()).size).toBe(0);
  });
});
