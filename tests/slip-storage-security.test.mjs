import { beforeAll, beforeEach, afterAll, test, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import crypto from 'node:crypto';
import sharp from 'sharp';
import { initializeTestEnvironment, assertFails } from '@firebase/rules-unit-testing';
import { ref, uploadBytes, getBytes, listAll, deleteObject, updateMetadata, getDownloadURL } from 'firebase/storage';
import { Timestamp } from 'firebase-admin/firestore';
import { getAdminDb, getAdminBucket, getAdminAuth, issueGuestToken, revokeGuestAccess } from '../api/_lib/firebase-admin.js';
import { createSessionCookie } from '../api/_lib/admin-auth.js';
import { MAX_UPLOAD_BYTES, SLIP_BUCKET } from '../api/_lib/slip-storage.mjs';
import handler from '../api/slip-verify.js';

const projectId = 'demo-slip-security';
let db, env, image, users, ownerCookie, staffCookie, otherBranchCookie, adminAccounting, adminRead, passAdmin, bookingApi, seedSequence=0;
const collections = ['bookings','pass_purchases','booking_slots','booking_slot_claims','guest_booking_access',
  'private_slip_uploads','rate_limits','idempotency_records','slip_registry','audit_logs','system_settings','customer_packages','available_slots','holidays','commerce_settings'];
beforeAll(async () => {
  for (const name of ['FIRESTORE_EMULATOR_HOST','FIREBASE_STORAGE_EMULATOR_HOST','FIREBASE_AUTH_EMULATOR_HOST']) {
    if (!process.env[name]?.startsWith('127.0.0.1:')) throw new Error(`Local ${name} required`);
  }
  if (process.env.GCLOUD_PROJECT !== projectId) throw new Error('Refusing non-demo project');
  process.env.ADMIN_SESSION_SECRET = 'synthetic-slip-emulator-secret';
  process.env.ADMIN_USERS_JSON = JSON.stringify({ Owner:{pin:'0000',role:'owner',branches:'*'},
    Staff:{pin:'0000',role:'branch_staff',branches:['ladprao1']}, Other:{pin:'0000',role:'branch_manager',branches:['elsewhere']} });
  ownerCookie = createSessionCookie('Owner').split(';')[0]; staffCookie = createSessionCookie('Staff').split(';')[0]; otherBranchCookie = createSessionCookie('Other').split(';')[0];
  db = getAdminDb();
  env = await initializeTestEnvironment({ projectId, storage:{ host:'127.0.0.1', port:9199, rules:readFileSync(new URL('../storage.rules',import.meta.url),'utf8') } });
  image = await sharp({create:{width:32,height:32,channels:3,background:'white'}}).png().toBuffer();
  users = await Promise.all(['a','b'].map(async name => {
    const response = await fetch(`http://${process.env.FIREBASE_AUTH_EMULATOR_HOST}/identitytoolkit.googleapis.com/v1/accounts:signUp?key=fake-emulator-key`,
      { method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:`slip-${name}-${Date.now()}@example.test`,password:'emulator-only-123',returnSecureToken:true}) });
    const result = await response.json(); if (!result.idToken) throw new Error(JSON.stringify(result));
    return { uid: result.localId, token: result.idToken };
  }));
  adminAccounting = (await import('../api/admin-edit-booking-accounting.js')).default;
  adminRead = (await import('../api/admin-ops.js')).default;
  passAdmin = (await import('../api/admin-user-action.js')).default;
  bookingApi = (await import('../api/booking.js')).default;
});
beforeEach(async () => {
  seedSequence=0;
  for (const collection of collections) {
    const snapshot = await db.collection(collection).get();
    await Promise.all(snapshot.docs.map(doc=>doc.ref.delete()));
  }
  await getAdminBucket(SLIP_BUCKET).deleteFiles({force:true}); // emulator only, guarded beforeAll
  await db.collection('system_settings').doc('features').set({useServerSlipSubmit:true,slipVerifyNotifications:false});
  delete process.env.SLIP_UPLOADS_DISABLED;
});
afterAll(async()=>{ await env?.cleanup(); });
function request(body, overrides={}) { return { method:'POST', body, query:{}, socket:{remoteAddress:'127.0.0.1'},
  headers:{'content-type':'application/json',host:'booking.example.test',origin:'https://booking.example.test','x-forwarded-for':'198.51.100.10',...overrides} }; }
async function call(body, headers={}, fn=handler) {
  const res = { statusCode:200,headers:{},status(code){this.statusCode=code;return this},setHeader(key,value){this.headers[key]=value},json(body){this.body=body;return this},end(bytes){this.bytes=bytes;return this} };
  await fn(request(body,headers),res); return res;
}
async function seed(id='bookingA', user=users[0], type='booking') {
  const code=`CODE_${id}`;
  const hour=13+seedSequence++, slotId=`room1_2027-03-15_${hour}00`;
  const record = {lineUserId:user?.uid||'guest',bookingCode:code,purchaseCode:code,branchId:'ladprao1',resourceId:'room1',date:'2027-03-15',startTime:`${hour}:00`,endTime:`${hour+1}:00`,durationMinutes:60,price:350,finalPrice:350,
    durationHours:1, bookingStatus:'pending_payment',status:'pending_payment',paymentStatus:'unpaid',paymentExpiresAt:Timestamp.fromMillis(Date.now()+600_000),createdAt:Timestamp.now(),bookingSlotIds:[slotId]};
  await db.collection(type==='booking'?'bookings':'pass_purchases').doc(id).set(record);
  if(type==='booking') {
    await db.collection('booking_slot_claims').doc(slotId).set({bookingId:id,status:'pending_payment'});
    await db.collection('booking_slots').doc(slotId).set({bookingStatus:'pending_payment',paymentStatus:'unpaid'});
  }
  const guest = user ? null : await issueGuestToken(db,{bookingId:id});
  return {targetType:type,...(type==='booking'?{bookingId:id,bookingCode:code}:{purchaseId:id,purchaseCode:code}),...(user?{idToken:user.token}:{guestToken:guest.token})};
}
async function upload(target, extra={}) { return call({...target,action:'upload_slip',fileBase64:image.toString('base64'),contentType:'image/png',idempotencyKey:'upload-synthetic-1',...extra}); }
async function submit(target, handle, extra={}) { return call({...target,action:target.targetType==='booking'?'submit_slip':'submit_pass_slip',slipUrl:handle,idempotencyKey:'submit-synthetic-1',...extra}); }
async function attach(target) {
  const uploaded=await upload(target);expect(uploaded.statusCode,JSON.stringify(uploaded.body)).toBe(200);
  const submitted=await submit(target,uploaded.body.slipReceipt);expect(submitted.statusCode,JSON.stringify(submitted.body)).toBe(200);
  return uploaded.body.slipReceipt;
}
test.each(['payment_slips/CODE/synthetic.jpg','pass_slips/CODE/synthetic.jpg','private_slips/booking/x/synthetic.jpg'])('rules deny anonymous and signed-in client get/list/create/update/delete: %s', async path => {
  await getAdminBucket(SLIP_BUCKET).file(path).save(image,{resumable:false,metadata:{contentType:'image/png'}});
  for(const context of [env.unauthenticatedContext(),env.authenticatedContext(users[0].uid)]) {
    const storage=context.storage(`gs://${SLIP_BUCKET}`);const object=ref(storage,path);
    await assertFails(getBytes(object)); await assertFails(getDownloadURL(object));
    await assertFails(listAll(ref(storage,path.split('/')[0])));
    await assertFails(uploadBytes(ref(storage,`${path}.new`),image,{contentType:'image/png'}));
    await assertFails(uploadBytes(object,image,{contentType:'image/png'}));
    await assertFails(updateMetadata(object,{contentType:'image/jpeg'}));await assertFails(deleteObject(object));
  }
});
test.each(['booking','pass_purchase'])('%s owner uploads private image, submits and verifies; never auto-paid',async type=>{
  const target=await seed('recordA',users[0],type);const handle=await attach(target);
  const receipt=(await db.collection('private_slip_uploads').doc(handle.slice(8)).get()).data();
  expect(receipt.state).toBe('consumed');expect(receipt.object.path).toMatch(/^private_slips\//);
  const [meta]=await getAdminBucket(SLIP_BUCKET).file(receipt.object.path).getMetadata();
  expect(meta.metadata?.firebaseStorageDownloadTokens).toBeFalsy();expect(meta.cacheControl).toBe('private, no-store');
  const viewed=await call({...target,action:'read_slip'});expect(viewed.statusCode).toBe(200);expect(viewed.bytes[0]).toBe(255);
  expect(viewed.headers['Cache-Control']).toBe('private, no-store');expect(viewed.headers['X-Content-Type-Options']).toBe('nosniff');
  const verified=await call(target);expect(verified.statusCode,JSON.stringify(verified.body)).toBe(200);expect(verified.body.verification.status).toBe('pre_verified');
  const record=(await db.collection(type==='booking'?'bookings':'pass_purchases').doc('recordA').get()).data();
  expect(record.paymentStatus).toBe('pending_review');expect(record.issuedPackageId).toBeUndefined();
  expect(record.paymentVerification.slipHash).toBe(receipt.object.sourceSha256);
  expect((await call(target)).body.cached).toBe(true);
});
test('verified guest upload/read works; code alone, cross-booking and expired/revoked capabilities fail',async()=>{
  const a=await seed('guestA',null);const b=await seed('guestB',null);await attach(a);
  expect((await call({...a,action:'read_slip'})).statusCode).toBe(200);
  expect((await call({...a,guestToken:undefined,action:'read_slip'})).statusCode).toBe(401);
  expect((await upload({...b,guestToken:a.guestToken})).statusCode).toBe(403);
  await db.collection('guest_booking_access').doc('guestA').update({expiresAt:Timestamp.fromMillis(Date.now()-1)});
  expect((await call({...a,action:'read_slip'})).statusCode).toBe(403);
  await revokeGuestAccess(db,'guestB','synthetic');expect((await upload(b)).statusCode).toBe(403);
});
test('actual guest booking API issues the capability used for private upload, submission and viewing',async()=>{
  await db.collection('available_slots').doc('room1_2027-03-15_1300').set({resourceId:'room1',date:'2027-03-15',startTime:'13:00',endTime:'14:00',status:'open'});
  const created=await call({action:'create',date:'2027-03-15',startTime:'13:00',durationMinutes:60,customerName:'Synthetic Guest',customerPhone:'0800000000'},{},bookingApi);
  expect(created.statusCode,JSON.stringify(created.body)).toBe(200);
  const target={targetType:'booking',bookingId:created.body.booking.id,bookingCode:created.body.booking.bookingCode,guestToken:created.body.guestAccessToken};
  await attach(target);expect((await call({...target,action:'read_slip'})).statusCode).toBe(200);
});
test('LINE customer B cannot read, upload, attach receipt or verify customer A; invalid LINE token cannot downgrade to guest',async()=>{
  const a=await seed();const b=await seed('bookingB',users[1]);const handle=await attach(a);
  for(const action of ['read_slip','upload_slip',undefined]) expect((await call({...a,idToken:users[1].token,action})).statusCode).toBe(403);
  expect((await submit(b,handle)).statusCode).toBe(403);
  const guest=await seed('guestC',null);expect((await upload({...guest,idToken:'invalid'})).statusCode).toBe(401);
});
test('Pass purchases require LINE auth; public URL/path and target injection are refused',async()=>{
  const pass=await seed('passA',users[0],'pass_purchase');expect((await upload({...pass,idToken:undefined,guestToken:'code'})).statusCode).toBe(401);
  const target=await seed();const before=(await getAdminBucket(SLIP_BUCKET).getFiles())[0].length;
  expect((await submit(target,'https://firebasestorage.googleapis.com/v0/b/x/o/y?token=secret')).statusCode).toBe(400);
  expect((await upload({...target,bookingId:'../escape'})).statusCode).toBe(400);
  expect((await getAdminBucket(SLIP_BUCKET).getFiles())[0]).toHaveLength(before);
});
test('full image decode rejects MIME mismatch, SVG/HTML, corrupt image and oversized/pixel bomb input',async()=>{
  const target=await seed();
  const bomb=await sharp({create:{width:5000,height:4000,channels:3,background:'white'}}).png().toBuffer();
  for(const [index,extra] of [
    {contentType:'image/jpeg'}, {contentType:'image/svg+xml'}, {fileBase64:Buffer.from('<html>bad</html>').toString('base64')},
    {fileBase64:image.subarray(0,32).toString('base64')},
    {fileBase64:Buffer.alloc(MAX_UPLOAD_BYTES+1).toString('base64')},
    {fileBase64:bomb.toString('base64')},
  ].entries()) {
    const result=await upload(target,{...extra,idempotencyKey:`bad-file-${index}`});expect([413,415]).toContain(result.statusCode);
  }
  expect((await getAdminBucket(SLIP_BUCKET).getFiles())[0]).toHaveLength(0);
});
test('disabled LINE/Firebase session and expired ID token cannot read a previously submitted slip',async()=>{
  const target=await seed();await attach(target);
  await getAdminAuth().updateUser(users[0].uid,{disabled:true});
  try { expect((await call({...target,action:'read_slip'})).statusCode).toBe(401); }
  finally { await getAdminAuth().updateUser(users[0].uid,{disabled:false}); }
  const segments=users[0].token.split('.');const payload=JSON.parse(Buffer.from(segments[1],'base64url').toString());
  payload.exp=Math.floor(Date.now()/1000)-60;segments[1]=Buffer.from(JSON.stringify(payload)).toString('base64url');
  expect((await call({...target,idToken:segments.join('.'),action:'read_slip'})).statusCode).toBe(401);
});
test('upload retries are idempotent, conflicting reuse fails, receipt is one-use and expires',async()=>{
  const target=await seed();const first=await upload(target);const again=await upload(target);
  expect(again.body).toMatchObject({slipReceipt:first.body.slipReceipt,replayed:true});
  expect((await upload(target,{fileBase64:Buffer.from('different').toString('base64')})).statusCode).toBe(409);
  expect((await getAdminBucket(SLIP_BUCKET).getFiles())[0]).toHaveLength(1);
  expect((await submit(target,first.body.slipReceipt)).statusCode).toBe(200);
  expect((await submit(target,first.body.slipReceipt)).body.replayed).toBe(true);
  expect((await submit(target,first.body.slipReceipt,{idempotencyKey:'different-submit-key'})).statusCode).toBe(409);
  await db.collection('rate_limits').get().then(s=>Promise.all(s.docs.map(d=>d.ref.delete())));
  const next=await upload(target,{idempotencyKey:'upload-next-key'});
  await db.collection('private_slip_uploads').doc(next.body.slipReceipt.slice(8)).update({expiresAt:Timestamp.fromMillis(Date.now()-1)});
  expect((await submit(target,next.body.slipReceipt,{idempotencyKey:'expired-submit'})).statusCode).toBe(410);
});
test('concurrent uploads and submissions cannot overwrite an object or consume one receipt twice',async()=>{
  const target=await seed(); const results=await Promise.all([upload(target),upload(target)]);
  expect(results.filter(r=>r.statusCode===200).length).toBeGreaterThan(0);expect(results.every(r=>[200,409].includes(r.statusCode))).toBe(true);
  expect((await getAdminBucket(SLIP_BUCKET).getFiles())[0]).toHaveLength(1);
  const handle=results.find(r=>r.statusCode===200).body.slipReceipt;
  const submitted=await Promise.all(['concurrent-a','concurrent-b'].map(idempotencyKey=>submit(target,handle,{idempotencyKey})));
  expect(submitted.map(r=>r.statusCode).sort()).toEqual([200,409]);
});
test('cancelled, expired and already paid records reject uploads; cancellation between upload and submit wins',async()=>{
  const target=await seed();const uploaded=await upload(target);
  await db.collection('bookings').doc('bookingA').update({bookingStatus:'cancelled'});
  expect((await submit(target,uploaded.body.slipReceipt)).statusCode).toBe(409);
  expect((await upload(target,{idempotencyKey:'new-cancelled'})).statusCode).toBe(409);
  await db.collection('bookings').doc('bookingA').update({bookingStatus:'pending_payment',paymentExpiresAt:Timestamp.fromMillis(Date.now()-1)});
  expect((await upload(target)).statusCode).toBe(409);
  await db.collection('bookings').doc('bookingA').update({paymentStatus:'paid'});expect((await upload(target)).statusCode).toBe(409);
});
test('admin owner reviews a private slip and approval remains manual; staff, expired session and wrong branch are denied',async()=>{
  const target=await seed();await attach(target);
  const view={action:'read_slip',bookingId:target.bookingId};
  expect((await call(view,{cookie:ownerCookie})).statusCode).toBe(200);
  expect((await call(view,{cookie:staffCookie})).statusCode).toBe(403);
  expect((await call(view,{cookie:otherBranchCookie})).statusCode).toBe(403);
  const payload=Buffer.from(JSON.stringify({name:'Owner',role:'owner',branches:'*',exp:Date.now()-10})).toString('base64url');
  const expired=`adminSession=${payload}.${crypto.createHmac('sha256',process.env.ADMIN_SESSION_SECRET).update(payload).digest('hex')}`;
  expect((await call(view,{cookie:expired})).statusCode).toBe(401);
  const list=await call({action:'admin_read',resource:'bookings'},{cookie:ownerCookie},adminRead);
  expect(list.statusCode).toBe(200);expect(JSON.stringify(list.body)).not.toContain('private:');
  const approved=await call({operation:'approve_slip',bookingId:target.bookingId},{cookie:ownerCookie},adminAccounting);
  expect(approved.statusCode,JSON.stringify({result:approved.body,record:(await db.collection('bookings').doc(target.bookingId).get()).data()})).toBe(200);
  expect((await db.collection('bookings').doc(target.bookingId).get()).data().paymentStatus).toBe('paid');
});
test('legacy viewing uses stored, exact record-bound path and never returns the download token',async()=>{
  const target=await seed();const path=`payment_slips/${target.bookingCode}/old.png`;
  await getAdminBucket(SLIP_BUCKET).file(path).save(image,{resumable:false,metadata:{contentType:'image/png'}});
  const url=`https://firebasestorage.googleapis.com/v0/b/${SLIP_BUCKET}/o/${encodeURIComponent(path)}?alt=media&token=synthetic-legacy-token`;
  await db.collection('bookings').doc(target.bookingId).update({slipUrl:url});
  const view=await call({...target,action:'read_slip'});expect(view.statusCode).toBe(200);expect(JSON.stringify(view.headers)).not.toContain('token=');
  const list=await call({action:'admin_read',resource:'bookings'},{cookie:ownerCookie},adminRead);
  expect(JSON.stringify(list.body)).not.toContain('synthetic-legacy-token');
  await db.collection('bookings').doc(target.bookingId).update({slipUrl:url.replace(encodeURIComponent(target.bookingCode),'OTHER')});
  expect((await call({...target,action:'read_slip'})).statusCode).toBe(409);
});
test('duplicate precheck spans booking and Pass; never records payment or issues credit',async()=>{
  const booking=await seed();await attach(booking);expect((await call(booking)).statusCode).toBe(200);
  const pass=await seed('passB',users[0],'pass_purchase');await attach(pass);
  const checked=await call(pass);expect(checked.body.verification).toMatchObject({status:'manual_review',reason:'duplicate_slip_hash'});
  expect((await db.collection('pass_purchases').doc('passB').get()).data().issuedPackageId).toBeUndefined();
});
test('new normalized upload still detects the original-byte hash already registered by the legacy flow',async()=>{
  const hash=crypto.createHash('sha256').update(image).digest('hex');
  await db.collection('slip_registry').doc(`hash_${hash}`).set({bookingId:'legacy-synthetic',bookingCode:'LEGACY_SYNTHETIC',source:'server_sha256'});
  const pass=await seed('passLegacyDuplicate',users[0],'pass_purchase');await attach(pass);
  const checked=await call(pass);expect(checked.body.verification).toMatchObject({status:'manual_review',reason:'duplicate_slip_hash'});
});
test('Pass admin views private slip and issues credit only on explicit approval, once',async()=>{
  const pass=await seed('passAdmin',users[0],'pass_purchase');
  await db.collection('pass_purchases').doc(pass.purchaseId).update({packageType:'ultra_starter_3',customerName:'Synthetic',customerPhone:'0800000000'});
  await attach(pass);
  const list=await call({action:'list_pending_pass_purchases'},{cookie:ownerCookie},passAdmin);
  expect(list.statusCode).toBe(200);expect(list.body.purchases[0].hasSlip).toBe(true);expect(list.body.purchases[0]).not.toHaveProperty('slipUrl');
  expect((await call({action:'read_slip',targetType:'pass_purchase',purchaseId:pass.purchaseId},{cookie:ownerCookie})).statusCode).toBe(200);
  expect((await db.collection('customer_packages').get()).empty).toBe(true);
  expect((await call({action:'approve_pass_purchase',purchaseId:pass.purchaseId},{cookie:ownerCookie},passAdmin)).statusCode).toBe(200);
  expect((await db.collection('customer_packages').get()).size).toBe(1);
  expect((await call({action:'approve_pass_purchase',purchaseId:pass.purchaseId},{cookie:ownerCookie},passAdmin)).statusCode).toBe(409);
  expect((await db.collection('customer_packages').get()).size).toBe(1);
});
test('durable rate limits and maintenance switch fail closed; cross-site cookie requests are rejected',async()=>{
  const target=await seed();
  for(let index=0;index<6;index++) expect((await upload(target,{idempotencyKey:`invalid-${index}`,contentType:'text/html'})).statusCode).toBe(415);
  expect((await upload(target,{idempotencyKey:'over-limit'})).statusCode).toBe(429);
  process.env.SLIP_UPLOADS_DISABLED='true';expect((await upload(target)).statusCode).toBe(503);
  expect((await call({action:'read_slip',bookingId:target.bookingId},{cookie:ownerCookie,origin:'https://attacker.example'})).statusCode).toBe(403);
  expect((await call({action:'read_slip',bookingId:target.bookingId},{cookie:ownerCookie,'content-type':'text/plain'})).statusCode).toBe(415);
  delete process.env.SLIP_UPLOADS_DISABLED;
  await db.collection('system_settings').doc('features').update({useServerSlipSubmit:false});
  expect((await upload(target)).statusCode).toBe(503);
});
