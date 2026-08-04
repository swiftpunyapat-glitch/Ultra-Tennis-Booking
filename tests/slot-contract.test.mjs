// ════════════════════════════════════════════════════════════════════
// Public slot contract — review remediation, commit 3 (RB-08 / RB-10)
// ════════════════════════════════════════════════════════════════════
// booking_slots stays publicly readable so the availability grid works
// without a login. Firestore rules cannot filter fields — `allow read` is
// all-or-nothing per document — so the only control over what leaks is what
// gets written. These tests hold that contract.
//
// No emulator needed: slotDocPayload is pure.
// ════════════════════════════════════════════════════════════════════

import { describe, test, expect } from 'vitest';
import { slotDocPayload } from '../api/booking.js';

const VALID = {
  date: '2026-08-10', hour: '14:00', resourceId: 'room1',
  slotSpanMinutes: 60, branchId: 'ladprao1',
  bookingStatus: 'pending_payment', paymentStatus: 'unpaid',
  expiresAt: null,
};

describe('RB-10 public slot documents carry no identifiers', () => {
  test('a valid payload passes through unchanged', () => {
    expect(slotDocPayload(VALID)).toEqual(VALID);
  });

  test.each([
    'bookingId', 'bookingCode', 'coachId',
    'customerName', 'customerPhone', 'customerPhoneNormalized',
    'lineUserId', 'lineDisplayName', 'customerNote', 'slipUrl',
    'price', 'amount', 'createdBy', 'paidBy',
  ])('%s is refused rather than silently dropped', (field) => {
    expect(() => slotDocPayload({ ...VALID, [field]: 'x' }))
      .toThrow(/SLOT_CONTRACT_VIOLATION/);
  });

  test('an unknown field is dropped without throwing', () => {
    const out = slotDocPayload({ ...VALID, somethingNew: 'x' });
    expect(out.somethingNew).toBeUndefined();
    expect(out).toEqual(VALID);
  });

  test('the projection contains only availability data', () => {
    const keys = Object.keys(slotDocPayload(VALID)).sort();
    expect(keys).toEqual([
      'bookingStatus', 'branchId', 'date', 'expiresAt',
      'hour', 'paymentStatus', 'resourceId', 'slotSpanMinutes',
    ]);
  });

  test('a reader learns that an hour is taken, never whose it is', () => {
    const serialised = JSON.stringify(slotDocPayload({
      ...VALID, bookingStatus: 'confirmed', paymentStatus: 'paid',
    }));
    for (const leak of ['UTAAA01', 'bk_A1', 'coach_', '081', 'U_A']) {
      expect(serialised).not.toContain(leak);
    }
  });

  test('undefined values are omitted rather than stored as null', () => {
    const out = slotDocPayload({ ...VALID, branchId: undefined });
    expect('branchId' in out).toBe(false);
  });
});
