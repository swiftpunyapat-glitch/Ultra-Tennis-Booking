import { readFileSync } from 'node:fs';
import { describe, test, expect } from 'vitest';

const read = name => readFileSync(new URL(`../${name}`, import.meta.url),'utf8');
const index=read('index.html');
const admin=read('admin.html');
const coach=read('coach.html');
const availability=read('availability.html');

describe('customer authentication and fail-closed cutover',()=>{
  test('Firebase authentication is awaited before protected customer reads',()=>{
    expect(index).toContain('await ensureFirebaseAuth(state.lineProfile.userId)');
    for(const functionName of ['checkEligibility','openRegisterView','prefillCustomerInfo','openMyBookings']){
      const start=index.indexOf(`async function ${functionName}`);
      expect(start,functionName).toBeGreaterThan(-1);
      const end=index.indexOf('\n}',start);
      const body=index.slice(start,end);
      expect(body,functionName).toMatch(/await (protectedIdToken|ensureFirebaseAuth)\(/);
    }
  });

  test('all booking, pass, slip, cancellation and guest mutations use server actions',()=>{
    for(const action of ['action:"create"','action:"create_pass_booking"','action:"submit_slip"','action:"submit_pass_slip"','action:"cancel_pending"','action:"guest_booking"']){
      expect(index).toContain(action);
    }
    expect(index).toContain('server-create failed — no direct-write fallback');
    expect(index).toContain('server pass create failed; no direct-write fallback');
    expect(index).not.toMatch(/\brunTransaction\s*\(/);
    expect(index).not.toMatch(/\b(updateDoc|addDoc|deleteDoc|writeBatch)\s*\(/);
  });
});

describe('customer promotions purchase flow',()=>{
  test('shows only online package purchasing plus one fallback admin contact',()=>{
    expect(index).toContain('id="passBuyWrap"');
    expect(index).toContain('ยืนยันซื้อ ${p.packageName}');
    expect(index.match(/data-i18n="contactToBuy"/g)||[]).toHaveLength(1);
    expect(index).not.toContain('<div class="pkggrid">');
    expect(index).not.toContain('ทักแอดมินเพื่อซื้อ');
    expect(index).not.toContain('Contact admin to buy');
  });
});

describe('active protected direct-write inventory',()=>{
  test('customer client has only the owner-scoped registered_users profile write',()=>{
    const calls=[...index.matchAll(/\b(setDoc|updateDoc|addDoc|deleteDoc|writeBatch|runTransaction)\s*\(/g)];
    expect(calls.map(x=>x[1])).toEqual(['setDoc']);
    expect(index).toContain('doc(db, "registered_users", userId)');
  });

  test('admin, coach and availability clients have zero Firestore write calls',()=>{
    const direct=/\b(setDoc|updateDoc|addDoc|deleteDoc|writeBatch|runTransaction)\s*\(/g;
    expect(admin.match(direct)||[]).toHaveLength(0);
    expect(coach.match(direct)||[]).toHaveLength(0);
    expect(availability.match(direct)||[]).toHaveLength(0);
  });

  test('admin protected reads and mutations are routed through existing authenticated dispatchers',()=>{
    expect(admin).toContain('action:"admin_read"');
    expect(admin).toContain('operation:"manual_create"');
    expect(admin).toContain('operation:"calendar_sync_fields"');
    expect(admin).toContain('operation:"approve_slip"');
    expect(admin).toContain('operation:"reject_payment"');
    expect(admin).toContain('operation:"reschedule_assign"');
    expect(admin).toContain('action:"slot_toggle"');
    expect(admin).toContain('action:"holiday_set"');
    expect(admin).toContain('action:"deactivate_pass"');
  });
});

describe('admin pending-reschedule view consistency',()=>{
  test('Daily View excludes historical pending/terminal slot mappings and refreshes the released date',()=>{
    expect(admin).toContain('const activeBookingsOnDay=bookingsOnDay.filter');
    expect(admin).toContain('!isPendingRescheduleBooking(b)');
    expect(admin).toContain('const releasedDate=reschBooking.date;');
    expect(admin).toContain('$("dvDate").value===releasedDate');
    expect(admin).not.toContain('$("dvDate").value===reschBooking.date');
  });

  test('Pending views expose a LINE-only customer alert with an editable preset',()=>{
    expect(admin).toContain('data-pending-alert=');
    expect(admin).toContain('data-pt-alert=');
    expect(admin).toContain('function pendingRescheduleAlertMessage(b)');
    expect(admin).toContain('openAlertModal(b,pendingRescheduleAlertMessage(b))');
    expect(admin).toContain('function openAlertModal(c,presetMessage="")');
    expect(admin).toContain('$("alertMessage").value=presetMessage');
    expect(admin).toContain('type:"admin_alert_customer"');
  });

  test('Pending multi-hour bookings expose partial assignment and preserve the remainder',()=>{
    expect(admin).toContain('id="rAssignDuration"');
    expect(admin).toContain('assignDurationMinutes');
    expect(admin).toContain('remainderDurationMinutes');
    expect(admin).toContain('เวลาที่เหลือจะอยู่ใน Pending Reschedule เป็นรายการแยก');
    expect(admin).toContain('const assignDuration=selectedReschDuration()');
  });
});
