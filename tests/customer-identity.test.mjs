import { describe, expect, test } from 'vitest';
import {
  buildCustomerIdentityDryRun,
  CUSTOMER_IDENTITY_RULE_VERSION,
  normalizeIdentityPhone,
} from '../api/_lib/customer-identity.js';

describe('customer identity dry-run', () => {
  test('normalizes only usable Thai phone identities', () => {
    expect(normalizeIdentityPhone('081-234-5678')).toBe('0812345678');
    expect(normalizeIdentityPhone('+66 81 234 5678')).toBe('0812345678');
    expect(normalizeIdentityPhone('02-123-4567')).toBe('021234567');
    expect(normalizeIdentityPhone('12345')).toBe('');
    expect(normalizeIdentityPhone('')).toBe('');
  });

  test('falls back to a registered-user document ID when a stored LINE field is a placeholder', () => {
    const report = buildCustomerIdentityDryRun({
      users: [{ id:'U1', lineUserId:'guest', phone:'0812345678' }],
      bookings: [{ id:'B1', lineUserId:'manual', customerPhone:'0812345678' }],
    });
    expect(report.proposedLinks).toContainEqual(expect.objectContaining({
      aliasCustomerId:'phone:0812345678', canonicalCustomerId:'line:U1',
    }));
  });

  test('proposes a phone soft-link only when one registered LINE owner exists', () => {
    const report = buildCustomerIdentityDryRun({
      generatedAt: '2026-08-27T00:00:00.000Z',
      users: [{ id:'U1', name:'Noppon', phone:'+66 81 234 5678' }],
      bookings: [
        { id:'B1', lineUserId:'manual', customerName:'Noppon', customerPhone:'081-234-5678', date:'2026-06-01', bookingStatus:'confirmed' },
        { id:'B2', lineUserId:'U1', customerName:'Noppon', customerPhone:'0812345678', date:'2026-07-01', bookingStatus:'cancelled' },
      ],
      packages: [{ id:'P1', lineUserId:'manual', customerName:'Noppon', customerPhone:'0812345678' }],
    });

    expect(report).toMatchObject({
      ok:true, dryRun:true, writesPerformed:0, ruleVersion:CUSTOMER_IDENTITY_RULE_VERSION,
      summary:{ canonicalProfiles:1, proposedLinks:1, identityConflicts:0 },
    });
    expect(report.proposedLinks[0]).toMatchObject({
      aliasCustomerId:'phone:0812345678', canonicalCustomerId:'line:U1', confidence:'high',
      bookingIds:['B1'], packageIds:['P1'], evidenceCount:2,
    });
    expect(report.derivedProfiles[0]).toMatchObject({
      canonicalCustomerId:'line:U1', totalBookings:2, confirmedBookings:1,
      cancelledBookings:1, firstBookingDate:'2026-06-01', lastBookingDate:'2026-07-01', packageIds:['P1'],
    });
  });

  test('never auto-links a phone shared by multiple LINE accounts', () => {
    const report = buildCustomerIdentityDryRun({
      users: [
        { id:'U1', name:'One', phone:'0810000000' },
        { id:'U2', name:'Two', phone:'0810000000' },
      ],
      bookings: [{ id:'B1', lineUserId:'manual', customerPhone:'0810000000', date:'2026-08-01' }],
    });
    expect(report.proposedLinks).toHaveLength(0);
    expect(report.conflicts).toContainEqual(expect.objectContaining({
      type:'shared_phone_multiple_line_accounts', phone:'0810000000', lineUserIds:['U1','U2'],
    }));
  });

  test('names never merge identities and LINE/phone disagreement is manual review', () => {
    const report = buildCustomerIdentityDryRun({
      users: [
        { id:'U1', name:'Same Name', phone:'0811111111' },
        { id:'U2', name:'Same Name', phone:'0822222222' },
      ],
      bookings: [
        { id:'B1', lineUserId:'U1', customerName:'Same Name', customerPhone:'0822222222', date:'2026-08-01' },
        { id:'B2', lineUserId:'manual', customerName:'Same Name', customerPhone:'0833333333', date:'2026-08-02' },
      ],
    });
    expect(report.summary.canonicalProfiles).toBe(3);
    expect(report.conflicts).toContainEqual(expect.objectContaining({
      type:'line_phone_disagreement', recordId:'B1', lineUserId:'U1', registeredPhoneOwner:'U2',
    }));
  });

  test('checks package ownership by document ID and never shape-deduplicates passes', () => {
    const report = buildCustomerIdentityDryRun({
      users: [
        { id:'U1', name:'Noppon', phone:'0811111111' },
        { id:'U2', name:'Other', phone:'0822222222' },
      ],
      bookings: [
        { id:'B1', lineUserId:'manual', customerPhone:'0811111111', packageId:'P1', date:'2026-08-01' },
        { id:'B2', lineUserId:'U1', customerPhone:'0811111111', packageId:'P3', date:'2026-08-02' },
        { id:'B3', lineUserId:'U1', customerPhone:'0811111111', usedPackageId:'P_OTHER', date:'2026-08-03' },
      ],
      packages: [
        { id:'P1', lineUserId:'manual', customerPhone:'0811111111', packageName:'Ultra', remainingMinutes:600 },
        { id:'P2', lineUserId:'U1', customerPhone:'0811111111', packageName:'Ultra', remainingMinutes:600 },
        { id:'P_OTHER', lineUserId:'U2', customerPhone:'0822222222', packageName:'Ultra', remainingMinutes:600 },
      ],
    });
    expect(report.passReview).toContainEqual(expect.objectContaining({
      type:'multiple_distinct_package_documents', canonicalCustomerId:'line:U1', packageIds:['P1','P2'],
    }));
    expect(report.packageConflicts).toContainEqual(expect.objectContaining({ type:'missing_package_document', bookingId:'B2', packageId:'P3' }));
    expect(report.packageConflicts).toContainEqual(expect.objectContaining({ type:'package_owner_identity_mismatch', bookingId:'B3', packageId:'P_OTHER' }));
  });

  test('keeps records without LINE or usable phone unresolved', () => {
    const report = buildCustomerIdentityDryRun({
      bookings: [{ id:'B1', lineUserId:'manual', customerName:'Walk-in', customerPhone:'-' }],
    });
    expect(report.unresolvedRecords).toEqual([expect.objectContaining({ source:'booking', recordId:'B1' })]);
    expect(report.summary.unresolvedRecords).toBe(1);
  });
});
