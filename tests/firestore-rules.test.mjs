// ════════════════════════════════════════════════════════════════════
// Firestore rules tests — Security Hotfix 2026-08-04
// ════════════════════════════════════════════════════════════════════
// Run against the Firestore emulator, never against production:
//
//   npm i -D @firebase/rules-unit-testing firebase vitest
//   firebase emulators:exec --only firestore "npx vitest run tests/"
//
// RULES_FILE selects which ruleset is under test. Both are expected to be
// exercised: the baseline is included so the tests demonstrate the exact
// holes being closed rather than only asserting the fixed behaviour.
//
//   RULES_FILE=firestore.rules.hotfix-proposed   → everything below passes
//   RULES_FILE=firestore.rules.baseline-...      → the SEC-* blocks FAIL,
//                                                  which is the point
//
// The rules files live in the planning workspace and are copied in by the
// deploy runbook; they are intentionally not committed to this public
// repository.
// ════════════════════════════════════════════════════════════════════

import { readFileSync } from 'node:fs';
import { initializeTestEnvironment, assertFails, assertSucceeds } from '@firebase/rules-unit-testing';
import { doc, getDoc, setDoc, updateDoc, collection, getDocs, query, where } from 'firebase/firestore';
import { beforeAll, afterAll, beforeEach, describe, test } from 'vitest';

const PROJECT_ID = 'ultra-tennis-rules-test';
const RULES_FILE = process.env.RULES_FILE || 'firestore.rules.hotfix-proposed';

const UID_A = 'U_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
const UID_B = 'U_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB';

let testEnv;

beforeAll(async () => {
  testEnv = await initializeTestEnvironment({
    projectId: PROJECT_ID,
    firestore: { rules: readFileSync(RULES_FILE, 'utf8') },
  });
});

afterAll(async () => { await testEnv?.cleanup(); });

// Seed with rules disabled so the fixtures themselves are never the thing
// under test.
beforeEach(async () => {
  await testEnv.clearFirestore();
  await testEnv.withSecurityRulesDisabled(async (ctx) => {
    const db = ctx.firestore();
    await setDoc(doc(db, 'bookings/bk_A1'), {
      lineUserId: UID_A, bookingCode: 'UTAAA01', resourceId: 'room1',
      bookingType: 'Single Use', customerName: 'A', customerPhone: '0810000001',
      date: '2026-08-10', startTime: '14:00', endTime: '15:00',
      price: 350, bookingStatus: 'pending_payment', paymentStatus: 'unpaid',
      slipUrl: 'https://firebasestorage.googleapis.com/v0/b/x/o/y?token=z',
    });
    await setDoc(doc(db, 'bookings/bk_B1'), {
      lineUserId: UID_B, bookingCode: 'UTBBB01', resourceId: 'room1',
      bookingType: 'Single Use', customerName: 'B', customerPhone: '0810000002',
      date: '2026-08-10', startTime: '16:00', endTime: '17:00',
      price: 350, bookingStatus: 'confirmed', paymentStatus: 'paid',
    });
    await setDoc(doc(db, 'bookings/bk_G1'), {
      lineUserId: 'guest', bookingCode: 'UTGGG01', resourceId: 'room1',
      bookingType: 'Single Use', customerName: 'G', customerPhone: '0810000003',
      date: '2026-08-10', startTime: '18:00', endTime: '19:00',
      price: 350, bookingStatus: 'pending_payment', paymentStatus: 'unpaid',
    });
    await setDoc(doc(db, 'customer_packages/pkg_A'), {
      lineUserId: UID_A, packageType: 'ultra_pass_10', packageName: 'Ultra Pass 10 Hours',
      customerName: 'A', customerPhone: '0810000001',
      remainingMinutes: 120, totalMinutes: 600, status: 'active',
    });
    await setDoc(doc(db, 'customer_packages/pkg_B'), {
      lineUserId: UID_B, packageType: 'ultra_pass_10', packageName: 'Ultra Pass 10 Hours',
      customerName: 'B', customerPhone: '0810000002',
      remainingMinutes: 60, totalMinutes: 600, status: 'active',
    });
    await setDoc(doc(db, 'booking_slots/room1_2026-08-10_1400'), {
      bookingCode: 'UTAAA01', bookingId: 'bk_A1', resourceId: 'room1', branchId: 'ladprao1',
      date: '2026-08-10', hour: '14:00', slotSpanMinutes: 60,
      bookingStatus: 'confirmed', paymentStatus: 'paid',
    });
    // Legacy slot: no slotSpanMinutes — must still be readable.
    await setDoc(doc(db, 'booking_slots/room1_2026-08-10_1500'), {
      bookingCode: 'UTOLD01', bookingId: 'bk_old', resourceId: 'room1',
      date: '2026-08-10', hour: '15:00',
      bookingStatus: 'confirmed', paymentStatus: 'paid',
    });
    await setDoc(doc(db, 'available_slots/room1_2026-08-10_1400'), {
      resourceId: 'room1', date: '2026-08-10', startTime: '14:00', endTime: '15:00', status: 'open',
    });
    await setDoc(doc(db, 'pass_purchases/pp_A'), { lineUserId: UID_A, status: 'pending_payment', purchaseCode: 'PPAAA' });
    await setDoc(doc(db, 'pass_purchases/pp_B'), { lineUserId: UID_B, status: 'pending_payment', purchaseCode: 'PPBBB' });
    await setDoc(doc(db, 'registered_users/' + UID_A), {
      lineUserId: UID_A, name: 'A', phone: '0810000001', source: 'liff_register',
    });
    await setDoc(doc(db, 'registered_users/' + UID_B), {
      lineUserId: UID_B, name: 'B', phone: '0810000002', source: 'liff_register',
    });
    await setDoc(doc(db, 'customer_package_logs/log_A'), {
      packageId: 'pkg_A', lineUserId: UID_A, customerName: 'A', customerPhone: '0810000001',
      packageType: 'ultra_10', action: 'add_minutes',
      oldRemainingMinutes: 0, newRemainingMinutes: 120, deltaMinutes: 120,
    });
    await setDoc(doc(db, 'system_settings/pricing'), { normalSingleUsePrice: 350 });
    await setDoc(doc(db, 'system_settings/features'), { enableHalfHourBooking: true });
    await setDoc(doc(db, 'holidays/2026-08-12'), { date: '2026-08-12', isHoliday: true });
    await setDoc(doc(db, 'audit_logs/al_1'), { action: 'x', actor: 'y' });
    await setDoc(doc(db, 'vouchers/V1'), { code: 'V1', active: true });
    await setDoc(doc(db, 'guest_access_tokens/deadbeef'), { bookingId: 'bk_G1', revokedAt: null });
  });
});

const anon  = () => testEnv.unauthenticatedContext().firestore();
const asA   = () => testEnv.authenticatedContext(UID_A).firestore();
const asB   = () => testEnv.authenticatedContext(UID_B).firestore();
// A caller who forged an admin claim. isAdmin() was removed from the
// proposed rules precisely so this cannot become a privilege path.
const fake  = () => testEnv.authenticatedContext('attacker', { admin: true }).firestore();

// ── SEC-01 — bookings must not be world-readable ────────────────────
describe('SEC-01 bookings read isolation', () => {
  test('MT-01 anonymous cannot list bookings', async () => {
    await assertFails(getDocs(collection(anon(), 'bookings')));
  });
  test('MT-02 anonymous cannot get a booking', async () => {
    await assertFails(getDoc(doc(anon(), 'bookings/bk_A1')));
  });
  test('MT-03 customer reads own booking', async () => {
    await assertSucceeds(getDoc(doc(asA(), 'bookings/bk_A1')));
  });
  test('MT-04 customer cannot read another customer booking', async () => {
    await assertFails(getDoc(doc(asA(), 'bookings/bk_B1')));
  });
  test('QL-01 owner-scoped query succeeds', async () => {
    await assertSucceeds(getDocs(query(collection(asA(), 'bookings'), where('lineUserId', '==', UID_A))));
  });
  test('QL-02 query scoped to another owner fails', async () => {
    await assertFails(getDocs(query(collection(asA(), 'bookings'), where('lineUserId', '==', UID_B))));
  });
  test('QL-03 unscoped list fails even when signed in', async () => {
    await assertFails(getDocs(collection(asA(), 'bookings')));
  });
  test('QL-04 non-owner filter does not satisfy the rule', async () => {
    await assertFails(getDocs(query(collection(asA(), 'bookings'), where('date', '==', '2026-08-10'))));
  });
  test('AT-12 slip URLs are not reachable anonymously', async () => {
    await assertFails(getDoc(doc(anon(), 'bookings/bk_A1')));
  });
});

// ── SEC-03 — bookings must not be client-writable ───────────────────
describe('SEC-03 bookings write lockdown', () => {
  test('AT-03 customer cannot mark own booking paid', async () => {
    await assertFails(updateDoc(doc(asA(), 'bookings/bk_A1'), { paymentStatus: 'paid' }));
  });
  test('AT-04 customer cannot confirm own booking', async () => {
    await assertFails(updateDoc(doc(asA(), 'bookings/bk_A1'), { bookingStatus: 'confirmed' }));
  });
  test('AT-05 customer cannot change price', async () => {
    await assertFails(updateDoc(doc(asA(), 'bookings/bk_A1'), { price: 0 }));
  });
  test('AT-06 customer cannot cancel another customer booking', async () => {
    await assertFails(updateDoc(doc(asA(), 'bookings/bk_B1'), { bookingStatus: 'cancelled' }));
  });
  test('anonymous cannot create a booking', async () => {
    await assertFails(setDoc(doc(anon(), 'bookings/bk_new'), {
      bookingCode: 'X', resourceId: 'room1', bookingType: 'Single Use', lineUserId: 'guest',
      customerName: 'x', customerPhone: '08', date: '2026-08-11', startTime: '10:00',
      endTime: '11:00', price: 0, bookingStatus: 'pending_payment', paymentStatus: 'unpaid',
    }));
  });
});

// ── SEC-02 — pass balance is server-only ────────────────────────────
describe('SEC-02 customer_packages lockdown', () => {
  test('AT-01 customer cannot inflate own pass balance', async () => {
    await assertFails(updateDoc(doc(asA(), 'customer_packages/pkg_A'), { remainingMinutes: 99999 }));
  });
  test('AT-02 customer cannot drain another customer pass', async () => {
    await assertFails(updateDoc(doc(asA(), 'customer_packages/pkg_B'), { remainingMinutes: 0 }));
  });
  test('customer reads own pass', async () => {
    await assertSucceeds(getDoc(doc(asA(), 'customer_packages/pkg_A')));
  });
  test('CX-07 customer cannot read another customer pass', async () => {
    await assertFails(getDoc(doc(asA(), 'customer_packages/pkg_B')));
  });
  test('anonymous cannot read passes', async () => {
    await assertFails(getDoc(doc(anon(), 'customer_packages/pkg_A')));
  });
});

// ── SEC-04 — slots readable, never client-writable ──────────────────
describe('SEC-04 booking_slots', () => {
  test('MT-13 anonymous may read the availability grid', async () => {
    await assertSucceeds(getDocs(query(
      collection(anon(), 'booking_slots'),
      where('date', '==', '2026-08-10'), where('resourceId', '==', 'room1'),
    )));
  });
  test('legacy slot without slotSpanMinutes is still readable', async () => {
    await assertSucceeds(getDoc(doc(anon(), 'booking_slots/room1_2026-08-10_1500')));
  });
  test('AT-07 anonymous cannot occupy slots', async () => {
    await assertFails(setDoc(doc(anon(), 'booking_slots/room1_2026-08-11_0900'), {
      bookingCode: 'X', resourceId: 'room1', date: '2026-08-11', hour: '09:00',
      bookingStatus: 'confirmed', paymentStatus: 'paid',
    }));
  });
  test('AT-08 customer cannot release someone else slot', async () => {
    await assertFails(updateDoc(doc(asA(), 'booking_slots/room1_2026-08-10_1400'), {
      bookingStatus: 'cancelled', paymentStatus: 'rejected',
    }));
  });
  test('available_slots readable, not writable', async () => {
    await assertSucceeds(getDoc(doc(anon(), 'available_slots/room1_2026-08-10_1400')));
    await assertFails(updateDoc(doc(asA(), 'available_slots/room1_2026-08-10_1400'), { status: 'closed' }));
  });
});

// ── SEC-05 — pass purchases ─────────────────────────────────────────
describe('SEC-05 pass_purchases', () => {
  test('MT-10 anonymous cannot write a pass purchase', async () => {
    await assertFails(updateDoc(doc(anon(), 'pass_purchases/pp_A'), { status: 'paid' }));
  });
  test('AT-09 customer cannot modify another customer purchase', async () => {
    await assertFails(updateDoc(doc(asA(), 'pass_purchases/pp_B'), { status: 'paid' }));
  });
  test('AT-10 customer cannot inject a slip URL onto another purchase', async () => {
    await assertFails(updateDoc(doc(asA(), 'pass_purchases/pp_B'), { slipUrl: 'https://evil/x' }));
  });
  test('MT-11 customer reads own purchase', async () => {
    await assertSucceeds(getDoc(doc(asA(), 'pass_purchases/pp_A')));
  });
  test('MT-12 customer cannot read another customer purchase', async () => {
    await assertFails(getDoc(doc(asA(), 'pass_purchases/pp_B')));
  });
});

// ── Guest isolation ─────────────────────────────────────────────────
// Guests never authenticate, so they must reach booking data only through
// the server API and their capability token. Losing direct Firestore read
// is intended (Addendum 03 sec 5), not a regression.
describe('Guest isolation', () => {
  test('MT-15 / GA-01 guest cannot read even their own booking directly', async () => {
    await assertFails(getDoc(doc(anon(), 'bookings/bk_G1')));
  });
  test('GA-04 guest may still read availability', async () => {
    await assertSucceeds(getDocs(query(
      collection(anon(), 'booking_slots'),
      where('date', '==', '2026-08-10'), where('resourceId', '==', 'room1'),
    )));
  });
  test('GA-05 guest may read pricing settings', async () => {
    await assertSucceeds(getDoc(doc(anon(), 'system_settings/pricing')));
  });
  test('GT-13 capability tokens are not readable from the client', async () => {
    await assertFails(getDoc(doc(anon(), 'guest_access_tokens/deadbeef')));
    await assertFails(getDoc(doc(asA(), 'guest_access_tokens/deadbeef')));
  });
});

// ── registered_users identity binding ───────────────────────────────
describe('registered_users identity binding', () => {
  test('CX-01 customer cannot read another profile', async () => {
    await assertFails(getDoc(doc(asA(), 'registered_users/' + UID_B)));
  });
  test('CX-02 customer cannot overwrite another profile', async () => {
    await assertFails(updateDoc(doc(asA(), 'registered_users/' + UID_B), { phone: '0899999999' }));
  });
  test('CX-03 customer updates own profile', async () => {
    await assertSucceeds(updateDoc(doc(asA(), 'registered_users/' + UID_A), {
      name: 'A2', phone: '0810000001',
    }));
  });
  test('CX-05 customer cannot repoint lineUserId', async () => {
    await assertFails(updateDoc(doc(asA(), 'registered_users/' + UID_A), {
      lineUserId: UID_B, name: 'A', phone: '0810000001',
    }));
  });
  test('CX-06 fields outside the allowlist are rejected', async () => {
    await assertFails(updateDoc(doc(asA(), 'registered_users/' + UID_A), {
      name: 'A', phone: '0810000001', isAdmin: true,
    }));
  });
  test('QL-10 anonymous cannot list profiles', async () => {
    await assertFails(getDocs(collection(anon(), 'registered_users')));
  });
});

// ── Server-only collections and forged claims ───────────────────────
describe('Server-only collections', () => {
  test('AT-14 server-only collections are unreachable', async () => {
    await assertFails(getDoc(doc(asA(), 'audit_logs/al_1')));
    await assertFails(getDoc(doc(asA(), 'vouchers/V1')));
    await assertFails(getDoc(doc(anon(), 'audit_logs/al_1')));
  });
  test('AT-15 only the pricing settings doc is public', async () => {
    await assertSucceeds(getDoc(doc(anon(), 'system_settings/pricing')));
    await assertFails(getDoc(doc(anon(), 'system_settings/features')));
  });
  test('AT-16 package logs carry PII and must be closed', async () => {
    await assertFails(getDoc(doc(anon(), 'customer_package_logs/log_A')));
    await assertFails(getDoc(doc(asA(), 'customer_package_logs/log_A')));
  });
  test('AT-13 a forged admin claim grants nothing', async () => {
    await assertFails(updateDoc(doc(fake(), 'available_slots/room1_2026-08-10_1400'), { status: 'closed' }));
    await assertFails(updateDoc(doc(fake(), 'holidays/2026-08-12'), { isHoliday: false }));
    await assertFails(getDocs(collection(fake(), 'bookings')));
    await assertFails(updateDoc(doc(fake(), 'customer_packages/pkg_A'), { remainingMinutes: 9999 }));
  });
  test('holidays remain publicly readable for pricing', async () => {
    await assertSucceeds(getDoc(doc(anon(), 'holidays/2026-08-12')));
  });
});
