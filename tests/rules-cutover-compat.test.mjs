import { readFileSync } from 'node:fs';
import { initializeTestEnvironment, assertSucceeds } from '@firebase/rules-unit-testing';
import { doc, getDoc, getDocs, setDoc, collection, query, where } from 'firebase/firestore';
import { beforeAll, afterAll, beforeEach, describe, test } from 'vitest';

const RULES_FILE=process.env.RULES_FILE;
if(!RULES_FILE) throw new Error('RULES_FILE is required');
const UID='U_COMPAT_123456789012345678901234';
let env;

beforeAll(async()=>{
  env=await initializeTestEnvironment({
    projectId:`ultra-rules-compat-${process.env.RULES_VARIANT||'local'}`,
    firestore:{rules:readFileSync(RULES_FILE,'utf8')},
  });
});
afterAll(async()=>env?.cleanup());
beforeEach(async()=>{
  await env.clearFirestore();
  await env.withSecurityRulesDisabled(async ctx=>{
    const db=ctx.firestore();
    await setDoc(doc(db,'bookings/b1'),{lineUserId:UID,bookingCode:'C1',date:'2027-01-01'});
    await setDoc(doc(db,'customer_packages/p1'),{lineUserId:UID,status:'active'});
    await setDoc(doc(db,'pass_purchases/x1'),{lineUserId:UID,status:'pending_payment'});
    await setDoc(doc(db,'booking_slots/room1_2027-01-01_1000'),{date:'2027-01-01',hour:'10:00',resourceId:'room1',bookingStatus:'confirmed',paymentStatus:'paid'});
    await setDoc(doc(db,'available_slots/room1_2027-01-01_1000'),{date:'2027-01-01',startTime:'10:00',resourceId:'room1',status:'open'});
    await setDoc(doc(db,'holidays/2027-01-01'),{date:'2027-01-01',isHoliday:false});
    await setDoc(doc(db,'system_settings/pricing'),{normalPrice:350});
  });
});

describe('new client compatibility across rules cutover and rules-first rollback',()=>{
  test('unauthenticated availability and pricing reads remain available',async()=>{
    const db=env.unauthenticatedContext().firestore();
    await assertSucceeds(getDoc(doc(db,'booking_slots/room1_2027-01-01_1000')));
    await assertSucceeds(getDoc(doc(db,'available_slots/room1_2027-01-01_1000')));
    await assertSucceeds(getDoc(doc(db,'holidays/2027-01-01')));
    await assertSucceeds(getDoc(doc(db,'system_settings/pricing')));
  });

  test('authenticated owner-filtered protected reads work',async()=>{
    const db=env.authenticatedContext(UID).firestore();
    await assertSucceeds(getDocs(query(collection(db,'bookings'),where('lineUserId','==',UID))));
    await assertSucceeds(getDocs(query(collection(db,'customer_packages'),where('lineUserId','==',UID))));
    await assertSucceeds(getDocs(query(collection(db,'pass_purchases'),where('lineUserId','==',UID))));
  });

  test('the remaining direct profile write has the complete allowed shape',async()=>{
    const db=env.authenticatedContext(UID).firestore();
    await assertSucceeds(setDoc(doc(db,'registered_users',UID),{
      lineUserId:UID,lineDisplayName:'Compat',pictureUrl:null,name:'Compat User',
      phone:'0812345678',phoneNormalized:'0812345678',createdAt:new Date(),updatedAt:new Date(),source:'liff_register',
    }));
  });
});
