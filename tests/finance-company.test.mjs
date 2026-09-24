import { beforeAll, beforeEach, afterAll, test, expect } from 'vitest';
import { DEFAULT_COMPANY_PROFILE, COMPANY_FIELDS, validateCompanyProfile, companyHeaderHTML, companyFooterHTML } from '../finance-company.js';

let db, handler, cookies;
const profile = (changes={}) => ({ ...Object.fromEntries(Object.keys(COMPANY_FIELDS).map(k=>[k,DEFAULT_COMPANY_PROFILE[k]])), ...changes });
beforeAll(async()=>{
  if(!process.env.FIRESTORE_EMULATOR_HOST?.startsWith('127.0.0.1:'))throw new Error('Local emulator required');
  process.env.ADMIN_SESSION_SECRET='finance-company-tests';
  process.env.ADMIN_USERS_JSON=JSON.stringify({Art:{pin:'0000',role:'owner',branches:'*'},OtherOwner:{pin:'0000',role:'owner',branches:'*'},Admin:{pin:'0000',role:'ultra_admin',branches:'*'},Staff:{pin:'0000',role:'branch_staff',branches:'*'}});
  db=(await import('../api/_lib/firebase-admin.js')).getAdminDb();
  handler=(await import('../api/finance-data.js')).default;
  const {createSessionCookie}=await import('../api/_lib/admin-auth.js');
  cookies=Object.fromEntries(['Art','OtherOwner','Admin','Staff'].map(name=>[name,createSessionCookie(name).split(';')[0]]));
});
async function call(body={},actor='Art',method='POST',query={}){
  const res={statusCode:200,headers:{},status(n){this.statusCode=n;return this},json(b){this.body=b;return this},setHeader(k,v){this.headers[k]=v}};
  await handler({method,body,query,headers:{cookie:cookies[actor]||''}},res);return res;
}
const get=actor=>call({},actor,'GET',{action:'company:get'});
const save=(p=profile(),revision=0,actor='Art')=>call({action:'company:save',profile:p,revision},actor);
beforeEach(async()=>{
  for(const name of ['bookings','finance_documents','finance_document_counters','finance_expenses','finance_income_manual','audit_logs']){
    const snap=await db.collection(name).get();await Promise.all(snap.docs.map(d=>d.ref.delete()));
  }
  await db.doc('system_settings/finance_company').delete();
});
afterAll(async()=>{await db.doc('system_settings/finance_company').delete();});

test('defaults match the existing issuer and reading does not initialize settings',async()=>{
  const res=await get();expect(res.statusCode).toBe(200);
  expect(res.body).toEqual({ok:true,profile:DEFAULT_COMPANY_PROFILE,revision:0});
  expect(res.headers['Cache-Control']).toBe('no-store');
  expect((await db.doc('system_settings/finance_company').get()).exists).toBe(false);
});
test('only owners save; finance admins can read, staff and anonymous cannot',async()=>{
  for(const actor of ['Admin','Staff',''])expect((await save(profile(),0,actor)).statusCode).toBe(actor?403:401);
  expect((await get('Admin')).statusCode).toBe(200);
  expect((await get('Staff')).statusCode).toBe(403);
  expect((await get('')).statusCode).toBe(401);
  expect((await save(profile({legalNameTh:'เจ้าของร้านลูกค้า'}),0,'OtherOwner')).statusCode).toBe(200);
});
test('saves normalized fields and audit atomically, with revision protection',async()=>{
  const res=await save(profile({legalNameTh:'  สนามตัวอย่าง  ',legalNameEn:'',address:'บรรทัดที่หนึ่ง\nบรรทัดที่สอง',taxId:'0123456789012'}));
  expect(res.body).toMatchObject({ok:true,revision:1,profile:{legalNameTh:'สนามตัวอย่าง',legalNameEn:'',taxId:'0123456789012',vatRegistered:false}});
  const audit=await db.collection('audit_logs').where('action','==','finance_company_updated').get();
  expect(audit.size).toBe(1);
  expect(audit.docs[0].data()).toMatchObject({actor:'Art',before:{legalNameTh:DEFAULT_COMPANY_PROFILE.legalNameTh},after:{legalNameTh:'สนามตัวอย่าง'}});
  expect((await save(profile({legalNameTh:'Stale'}),0)).statusCode).toBe(409);
  expect((await get()).body.profile.legalNameTh).toBe('สนามตัวอย่าง');
  expect((await db.collection('audit_logs').get()).size).toBe(1);
});
test('two owners editing the same version cannot overwrite each other',async()=>{
  const res=await Promise.all([save(profile({legalNameTh:'A'})),save(profile({legalNameTh:'B'}),0,'OtherOwner')]);
  expect(res.map(r=>r.statusCode).sort()).toEqual([200,409]);
  expect((await get()).body.revision).toBe(1);
});
test.each([
  {legalNameTh:''},{legalNameTh:'x'.repeat(201)},{taxId:'123'}, {taxId:'abcdefghijklmn'},
  {email:'not-an-email'},{branch:'a\nb'},{phone:42},{address:'bad\u0000value'},
  {vatRegistered:true},{taxInvoiceEnabled:true},{vatRate:0.2},{revision:99},
])('invalid or unauthorized fields are rejected: %j',async change=>{
  expect((await save(profile(change))).statusCode).toBe(400);
  expect((await db.doc('system_settings/finance_company').get()).exists).toBe(false);
});
test('blank optional fields stay blank in the rendered header and footer follows the issuer',()=>{
  const p=validateCompanyProfile(profile({legalNameTh:'ร้านทดสอบ <script>alert(1)</script>',legalNameEn:'',brandName:'',branch:'',address:'ชั้น 1\nถนนตัวอย่าง',taxId:'',phone:'',email:''}));
  const html=companyHeaderHTML(p);
  expect(html).not.toContain('Swift');expect(html).not.toContain('Ultra');expect(html).not.toContain('สำนักงานใหญ่');
  expect(html).not.toContain('Tax ID:');expect(html).not.toContain('<script>');expect(html).toContain('&lt;script&gt;');
  expect(html).toContain('white-space:pre-wrap');
  expect(companyFooterHTML(p,true)).toContain('not a tax invoice');
  expect(companyFooterHTML(p,false)).not.toContain('not a tax invoice');
  expect(companyHeaderHTML(undefined)).toContain(DEFAULT_COMPANY_PROFILE.legalNameTh);
});
async function source(id,type='booking'){
  const collection=type==='booking'?'bookings':type==='expense'?'finance_expenses':'finance_income_manual';
  await db.collection(collection).doc(id).set({date:'2027-07-01',paymentStatus:'paid',bookingStatus:'confirmed',price:350,amount:350,customerName:'Customer',businessUnit:'ultra_tennis',category:'Misc',vendor:'Supplier',deleted:false});
  return call({action:'documents:issue',docType:type==='expense'?'payment_voucher':'receipt',linkedType:type,linkedId:id});
}
test('new documents freeze their issuer; old documents and amounts survive later settings changes',async()=>{
  const old=await source('old');expect(old.statusCode,JSON.stringify(old.body)).toBe(200);
  const oldRef=db.doc(`finance_documents/${old.body.document.id}`), original=(await oldRef.get()).data();
  await save(profile({legalNameTh:'ร้านใหม่',legalNameEn:'',brandName:'New Court'}));
  for(const type of ['booking','manual_income','expense']){
    const issued=await source(`new-${type}`,type);expect(issued.statusCode,JSON.stringify(issued.body)).toBe(200);
    const saved=(await db.doc(`finance_documents/${issued.body.document.id}`).get()).data();
    expect(saved).toMatchObject({companyProfileSnapshot:{legalNameTh:'ร้านใหม่',legalNameEn:'',taxInvoiceEnabled:false},companyProfileRevision:1,total:350,vatMode:'non_vat',vatAmount:0});
  }
  await save(profile({legalNameTh:'ร้านที่สาม'}),1);
  expect((await oldRef.get()).data()).toEqual(original);
  const list=await call({},'Art','GET',{action:'documents:list',month:'2027-07'});
  const header=companyHeaderHTML(list.body.documents.find(d=>d.id===oldRef.id).companyProfileSnapshot);
  expect(header).toContain(DEFAULT_COMPANY_PROFILE.legalNameTh);expect(header).not.toContain('ร้านที่สาม');
  expect((await source('old')).statusCode).toBe(409);
});
test('concurrent issue and save produce a consistent profile and revision',async()=>{
  await db.doc('bookings/race').set({date:'2027-07-01',paymentStatus:'paid',price:350,customerName:'C'});
  const [issued,saved]=await Promise.all([
    call({action:'documents:issue',docType:'receipt',linkedType:'booking',linkedId:'race'}),
    save(profile({legalNameTh:'New name',brandName:'New brand'})),
  ]);
  expect(issued.statusCode).toBe(200);expect(saved.statusCode).toBe(200);
  const d=(await db.doc(`finance_documents/${issued.body.document.id}`).get()).data();
  expect([0,1]).toContain(d.companyProfileRevision);
  expect(d.companyProfileSnapshot.legalNameTh).toBe(d.companyProfileRevision===1?'New name':DEFAULT_COMPANY_PROFILE.legalNameTh);
  expect(d.companyProfileSnapshot.brandName).toBe(d.companyProfileRevision===1?'New brand':DEFAULT_COMPANY_PROFILE.brandName);
});
