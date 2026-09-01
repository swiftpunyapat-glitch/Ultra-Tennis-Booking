import { describe, expect, test } from 'vitest';
import {
  buildCustomerIdentityDryRun,
  CUSTOMER_IDENTITY_RULE_VERSION,
  normalizeIdentityPhone,
  usableIdentityLineId,
} from '../api/_lib/customer-identity.js';

const LINE_A = `U${'a'.repeat(32)}`;
const LINE_B = `U${'b'.repeat(32)}`;
const LINE_C = `U${'c'.repeat(32)}`;
const CONTAMINATED_LINE = 'U60929ad30400dcb9b8ab70527f104c3f';
const NOPPON_LINE = 'U6741a143a06b5a63cab0a840d9bb8197';

const booking = (id, fields = {}) => ({
  id, date:'2026-08-01', bookingStatus:'confirmed', ...fields,
});

function profile(report, canonicalCustomerId) {
  return report.derivedProfiles.find(item => item.canonicalCustomerId === canonicalCustomerId);
}

describe('customer identity dry-run safety patch #2', () => {
  test('normalizes only valid Thai mobile phones and accepts only real LINE identifiers', () => {
    expect(normalizeIdentityPhone('081-234-5678')).toBe('0812345678');
    expect(normalizeIdentityPhone('+66 81 234 5678')).toBe('0812345678');
    expect(normalizeIdentityPhone('06 1234 5678')).toBe('0612345678');
    expect(normalizeIdentityPhone('02-123-4567')).toBe('');
    expect(normalizeIdentityPhone('0')).toBe('');
    expect(normalizeIdentityPhone('+1 202 555 0100')).toBe('');
    expect(usableIdentityLineId(LINE_A)).toBe(LINE_A);
    for (const invalid of ['', 'manual', 'guest', 'U1', '0812345678']) {
      expect(usableIdentityLineId(invalid)).toBe('');
    }
  });

  test('LINE with one valid phone remains normal and can receive a proposed link', () => {
    const report = buildCustomerIdentityDryRun({
      users: [{ id:LINE_A, name:'Normal', phone:'0812345678' }],
      bookings: [booking('B1', { lineUserId:'manual', customerPhone:'0812345678' })],
    });
    expect(report.suspiciousLineIdentities).toHaveLength(0);
    expect(report.proposedLinks).toContainEqual(expect.objectContaining({
      aliasCustomerId:'phone:0812345678', canonicalCustomerId:`line:${LINE_A}`,
    }));
  });

  test('registered-user document ID remains the LINE source when its stored field is a placeholder', () => {
    const report = buildCustomerIdentityDryRun({
      users: [{ id:LINE_A, lineUserId:'guest', phone:'0812345678' }],
      bookings: [booking('B1', { lineUserId:'manual', customerPhone:'0812345678' })],
    });
    expect(report.proposedLinks).toContainEqual(expect.objectContaining({ canonicalCustomerId:`line:${LINE_A}` }));
  });

  test('LINE with two distinct valid phones is manual review and cannot receive a proposed link', () => {
    const report = buildCustomerIdentityDryRun({
      users: [{ id:LINE_A, name:'One', phone:'0811111111' }],
      bookings: [
        booking('B-LINE', { lineUserId:LINE_A, customerName:'Two', customerPhone:'0822222222' }),
        booking('B-MANUAL', { lineUserId:'manual', customerPhone:'0811111111' }),
      ],
    });
    expect(report.suspiciousLineIdentities).toContainEqual(expect.objectContaining({
      type:'suspicious_line_identity', lineUserId:LINE_A,
      phones:['0811111111','0822222222'], severity:'manual_review', bookingIds:['B-LINE'],
    }));
    expect(report.summary).toMatchObject({ suspiciousLineIdentities:1, hardBlockedLineIdentities:0, affectedBookings:1 });
    expect(report.proposedLinks).toHaveLength(0);
    expect(profile(report, `line:${LINE_A}`).totalBookings).toBe(1);
    expect(profile(report, 'phone:0811111111').totalBookings).toBe(1);
  });

  test('LINE with three or more distinct valid phones is hard-blocked', () => {
    const report = buildCustomerIdentityDryRun({
      users: [{ id:LINE_A, phone:'0811111111' }],
      bookings: [
        booking('B2', { lineUserId:LINE_A, customerPhone:'0822222222' }),
        booking('B3', { lineUserId:LINE_A, customerPhone:'0833333333' }),
      ],
    });
    expect(report.suspiciousLineIdentities[0]).toMatchObject({ lineUserId:LINE_A, severity:'hard_block' });
    expect(report.suspiciousLineIdentities[0].phones).toEqual(['0811111111','0822222222','0833333333']);
    expect(report.summary).toMatchObject({ suspiciousLineIdentities:1, hardBlockedLineIdentities:1, affectedBookings:2 });
  });

  test('placeholder manual records and repeated invalid phones never collapse into one identity', () => {
    const report = buildCustomerIdentityDryRun({
      bookings: [
        booking('M1', { lineUserId:'manual', customerName:'One', customerPhone:'0' }),
        booking('M2', { lineUserId:'manual', customerName:'Two', customerPhone:'0' }),
      ],
    });
    expect(report.derivedProfiles.map(item => item.canonicalCustomerId)).toEqual(['record:booking:M1','record:booking:M2']);
    expect(report.unresolvedRecords).toHaveLength(2);
    expect(report.proposedLinks).toHaveLength(0);
  });

  test('suspicious LINE and registered phone disagreement coexist', () => {
    const report = buildCustomerIdentityDryRun({
      users: [
        { id:LINE_A, phone:'0811111111' },
        { id:LINE_B, phone:'0822222222' },
      ],
      bookings: [booking('f2nJjwbhVBIq4rk0tcp0', { lineUserId:LINE_A, customerPhone:'0822222222' })],
    });
    expect(report.suspiciousLineIdentities).toContainEqual(expect.objectContaining({ lineUserId:LINE_A, severity:'manual_review' }));
    expect(report.conflicts).toContainEqual(expect.objectContaining({
      type:'line_phone_disagreement', recordId:'f2nJjwbhVBIq4rk0tcp0',
      lineUserId:LINE_A, registeredPhoneOwner:LINE_B,
    }));
  });

  test('known contaminated LINE is hard-blocked, keeps bookings, names and existing disagreement', () => {
    const report = buildCustomerIdentityDryRun({
      users: [
        { id:CONTAMINATED_LINE, name:'BaiMon', phone:'0811111111' },
        { id:LINE_B, name:'Phone owner', phone:'0822222222' },
      ],
      bookings: [
        booking('f2nJjwbhVBIq4rk0tcp0', { lineUserId:CONTAMINATED_LINE, customerName:'K.ปิ่น', customerPhone:'0822222222' }),
        booking('CONTAM-2', { lineUserId:CONTAMINATED_LINE, customerName:'K.ภัทร', customerPhone:'0833333333' }),
        booking('CONTAM-3', { lineUserId:CONTAMINATED_LINE, customerName:'ก้องภพ', customerPhone:'0844444444' }),
        booking('WOULD-LINK', { lineUserId:'manual', customerName:'BaiMon', customerPhone:'0811111111' }),
      ],
    });
    const suspicious = report.suspiciousLineIdentities.find(item => item.lineUserId === CONTAMINATED_LINE);
    expect(suspicious).toMatchObject({
      type:'suspicious_line_identity', severity:'hard_block',
      bookingIds:['CONTAM-2','CONTAM-3','f2nJjwbhVBIq4rk0tcp0'],
      names:['BaiMon','K.ปิ่น','K.ภัทร','ก้องภพ'],
    });
    expect(suspicious.phones).toHaveLength(4);
    expect(profile(report, `line:${CONTAMINATED_LINE}`).totalBookings).toBe(3);
    expect(report.proposedLinks.some(item => item.canonicalCustomerId === `line:${CONTAMINATED_LINE}`)).toBe(false);
    expect(report.conflicts).toContainEqual(expect.objectContaining({ type:'line_phone_disagreement', recordId:'f2nJjwbhVBIq4rk0tcp0' }));
    expect(report.writesPerformed).toBe(0);
  });

  test('Karn regression: proposed link and 9 / 8 / 1 booking totals remain intact', () => {
    const bookings = Array.from({ length:9 }, (_, index) => booking(`K${index+1}`, {
      lineUserId:'manual', customerName:'Karn', customerPhone:'0861111111',
      bookingStatus:index === 8 ? 'cancelled' : 'confirmed', date:`2026-08-${String(index+1).padStart(2,'0')}`,
    }));
    const report = buildCustomerIdentityDryRun({ users:[{ id:LINE_A, name:'Karn', phone:'0861111111' }], bookings });
    expect(report.proposedLinks).toContainEqual(expect.objectContaining({ canonicalCustomerId:`line:${LINE_A}`, bookingIds:bookings.map(item => item.id) }));
    expect(profile(report, `line:${LINE_A}`)).toMatchObject({ totalBookings:9, confirmedBookings:8, cancelledBookings:1 });
  });

  test('zeazea regression: explicit LINE and manual-phone history remain one profile with 7 confirmed', () => {
    const bookings = Array.from({ length:7 }, (_, index) => booking(`Z${index+1}`, {
      lineUserId:index < 3 ? LINE_B : 'manual', customerName:'zeazea', customerPhone:'0892222222',
    }));
    const report = buildCustomerIdentityDryRun({ users:[{ id:LINE_B, name:'zeazea', phone:'0892222222' }], bookings });
    expect(profile(report, `line:${LINE_B}`)).toMatchObject({ totalBookings:7, confirmedBookings:7 });
    expect(report.derivedProfiles.filter(item => item.names.includes('zeazea'))).toHaveLength(1);
  });

  test('Anuphab regression: phone 0800508899 links to registered LINE with 10 confirmed', () => {
    const bookings = Array.from({ length:10 }, (_, index) => booking(`A${index+1}`, {
      lineUserId:'manual', customerName:'Anuphab', customerPhone:'0800508899',
    }));
    const report = buildCustomerIdentityDryRun({ users:[{ id:LINE_C, name:'Anuphab', phone:'0800508899' }], bookings });
    expect(report.proposedLinks).toContainEqual(expect.objectContaining({
      aliasCustomerId:'phone:0800508899', canonicalCustomerId:`line:${LINE_C}`,
    }));
    expect(profile(report, `line:${LINE_C}`).confirmedBookings).toBe(10);
  });

  test('Noppon real-data regression: two valid phones require review and package balances are never summed', () => {
    const manualBookings = Array.from({ length:4 }, (_, index) => booking(`NM${index+1}`, {
      lineUserId:'manual', customerName:'Noppon ✅', customerPhone:'0809660850',
    }));
    const lineBookings = Array.from({ length:5 }, (_, index) => booking(`NL${index+1}`, {
      lineUserId:NOPPON_LINE, customerName:index%2?'นพพล':'Noppon ✅', customerPhone:'0923924850',
      bookingStatus:index===4?'cancelled':'confirmed',
    }));
    const report = buildCustomerIdentityDryRun({
      users: [{ id:NOPPON_LINE, name:'Noppon ✅', phone:'0809660850' }],
      bookings: [...manualBookings,...lineBookings],
      packages: [
        { id:'P1', lineUserId:NOPPON_LINE, customerName:'Noppon ✅', customerPhone:'0923924850', packageName:'Ultra', remainingMinutes:600 },
        { id:'P2', lineUserId:NOPPON_LINE, customerName:'Noppon ✅', customerPhone:'0923924850', packageName:'Ultra', remainingMinutes:600 },
      ],
    });
    const noppon = profile(report, `line:${NOPPON_LINE}`);
    expect(report.suspiciousLineIdentities).toContainEqual(expect.objectContaining({
      lineUserId:NOPPON_LINE, phones:['0809660850','0923924850'], severity:'manual_review',
    }));
    expect(report.proposedLinks.some(item=>item.canonicalCustomerId===`line:${NOPPON_LINE}`)).toBe(false);
    expect(noppon).toMatchObject({ totalBookings:5, confirmedBookings:4, cancelledBookings:1 });
    expect(profile(report,'phone:0809660850')).toMatchObject({ totalBookings:4, confirmedBookings:4 });
    expect(noppon.packageIds).toEqual(['P1','P2']);
    expect(noppon).not.toHaveProperty('remainingMinutes');
    expect(JSON.stringify(noppon)).not.toContain('1200');
    expect(report.passReview).toContainEqual(expect.objectContaining({
      type:'multiple_distinct_package_documents', canonicalCustomerId:`line:${NOPPON_LINE}`, packageIds:['P1','P2'],
    }));
  });

  test('shared registered phone conflict and missing/mismatched packages remain reported', () => {
    const report = buildCustomerIdentityDryRun({
      generatedAt:'2026-09-01T00:00:00.000Z',
      users: [
        { id:LINE_A, phone:'0810000000' },
        { id:LINE_B, phone:'0810000000' },
        { id:LINE_C, phone:'0899999999' },
      ],
      bookings: [
        booking('B-SHARED', { lineUserId:'manual', customerPhone:'0810000000' }),
        booking('B-MISSING', { lineUserId:LINE_C, customerPhone:'0899999999', packageId:'P-MISSING' }),
        booking('B-MISMATCH', { lineUserId:LINE_C, customerPhone:'0899999999', usedPackageId:'P-OTHER' }),
      ],
      packages: [{ id:'P-OTHER', lineUserId:LINE_B, customerPhone:'0810000000' }],
    });
    expect(report).toMatchObject({ ok:true, dryRun:true, writesPerformed:0, ruleVersion:CUSTOMER_IDENTITY_RULE_VERSION });
    expect(report.conflicts).toContainEqual(expect.objectContaining({ type:'shared_phone_multiple_line_accounts', phone:'0810000000' }));
    expect(report.packageConflicts).toContainEqual(expect.objectContaining({ type:'missing_package_document', bookingId:'B-MISSING' }));
    expect(report.packageConflicts).toContainEqual(expect.objectContaining({ type:'package_owner_identity_mismatch', bookingId:'B-MISMATCH' }));
  });
});
