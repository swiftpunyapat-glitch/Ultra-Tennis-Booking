import { describe, expect, test } from 'vitest';
import {
  buildReadOnlyCustomerAnalytics,
  CUSTOMER_ANALYTICS_VERSION,
} from '../api/_lib/customer-analytics.js';

const AUTO_LINE = 'U192bdc0f5d57d09f4949edda2b96d549';
const ART_LINE = 'U8ef026374d200e21bcf615a3bbc6bb2d';
const NOPPON_LINE = 'U6741a143a06b5a63cab0a840d9bb8197';
const PAKSADA_LINE = 'U283f715a63f878c05fb3902a4cbef2c0';
const ANUTHAT_LINE = 'U1652aadd4d9e2c5da0ae93f0d8af0fb6';
const CONTAMINATED_LINE = 'U60929ad30400dcb9b8ab70527f104c3f';
const RANGE = { from: '2026-08-01', to: '2026-08-31' };
const SALT = 'f'.repeat(64);

const booking = (id, fields = {}) => ({
  id,
  date: '2026-08-10',
  startTime: '13:30',
  endTime: '14:30',
  bookingStatus: 'confirmed',
  paymentStatus: 'paid',
  price: 390,
  ...fields,
});

describe('read-only customer analytics', () => {
  test('uses approved phone-to-LINE mapping and derives returning status from lifetime bookings', () => {
    const report = buildReadOnlyCustomerAnalytics([
      booking('old-line', { date: '2026-07-15', lineUserId: AUTO_LINE, customerPhone: '0800508899' }),
      booking('aug-phone', { lineUserId: 'manual', customerPhone: '0800508899' }),
    ], [{ id: AUTO_LINE, phone: '0800508899' }], RANGE, { referenceSalt: SALT });
    expect(report.summary).toMatchObject({ activeCustomers: 1, returningCustomers: 1, newCustomers: 0 });
    expect(report.customers[0]).toMatchObject({ lifetimePlayed: 2, periodPlayed: 1, resolution: 'safe_line', hadPlayedBeforePeriod: true });
    expect(report.resolutionCounts).toEqual({ approved_phone_to_line: 1 });
  });

  test('approved Art, Noppon and PAKSADA LINE histories resolve as one customer each', () => {
    const report = buildReadOnlyCustomerAnalytics([
      booking('art-1', { lineUserId: ART_LINE, customerPhone: '0962825392' }),
      booking('art-2', { lineUserId: ART_LINE, customerPhone: '0962828392' }),
      booking('nop-1', { lineUserId: NOPPON_LINE, customerPhone: '0809660850' }),
      booking('nop-2', { lineUserId: NOPPON_LINE, customerPhone: '0923924850' }),
      booking('pak-1', { lineUserId: PAKSADA_LINE, customerPhone: '0837824549' }),
    ], [], RANGE, { referenceSalt: SALT });
    expect(report.summary).toMatchObject({ activeCustomers: 3, repeatCustomersInPeriod: 2 });
    expect(report.customers.filter(item => item.resolution === 'approved_human')).toHaveLength(3);
  });

  test('contaminated U609 bookings always stay record-level isolated', () => {
    const report = buildReadOnlyCustomerAnalytics([
      booking('u609-admin', { lineUserId: CONTAMINATED_LINE, customerPhone: '0649649222' }),
      booking('u609-customer', { lineUserId: CONTAMINATED_LINE, customerPhone: '0829646369' }),
    ], [], RANGE, { referenceSalt: SALT });
    expect(report.summary).toMatchObject({ activeCustomers: 2, reviewRequiredCustomers: 2, reviewRequiredBookings: 2 });
    expect(report.customers.every(item => item.resolution === 'contaminated_isolated')).toBe(true);
    expect(new Set(report.customers.map(item => item.customerRef)).size).toBe(2);
  });

  test('Anuthat remains isolated manual review rather than selecting a phone', () => {
    const report = buildReadOnlyCustomerAnalytics([
      booking('anu-1', { lineUserId: ANUTHAT_LINE, customerPhone: '0935822219' }),
      booking('anu-2', { lineUserId: ANUTHAT_LINE, customerPhone: '0935822249' }),
    ], [], RANGE, { referenceSalt: SALT });
    expect(report.customers).toHaveLength(2);
    expect(report.customers.every(item => item.resolution === 'manual_review_isolated')).toBe(true);
  });

  test('valid manual phone can group safely while placeholder manual is never a LINE identity', () => {
    const report = buildReadOnlyCustomerAnalytics([
      booking('manual-1', { lineUserId: 'manual', customerPhone: '0899999999' }),
      booking('manual-2', { lineUserId: 'manual', customerPhone: '089-999-9999' }),
    ], [], RANGE, { referenceSalt: SALT });
    expect(report.summary).toMatchObject({ activeCustomers: 1, repeatCustomersInPeriod: 1 });
    expect(report.customers[0].resolution).toBe('safe_phone');
  });

  test('statistics are booking-derived and ignore cancelled revenue', () => {
    const report = buildReadOnlyCustomerAnalytics([
      booking('played', { customerPhone: '0899999999', durationMinutes: 90, price: 500 }),
      booking('cancelled', { customerPhone: '0899999999', bookingStatus: 'cancelled', price: 900 }),
    ], [], RANGE, { referenceSalt: SALT });
    expect(report.summary).toMatchObject({ periodRecords: 2, periodPlayedBookings: 1, periodCancelledBookings: 1, activeCustomers: 1, totalCustomerPaidRevenue: 500, totalCustomerBookedHours: 1.5 });
    expect(report.customers[0]).toMatchObject({ periodRecords: 2, periodPlayed: 1, periodCancelled: 1, periodPaidRevenue: 500 });
  });

  test('never returns PII and never reads or combines package balances', () => {
    const report = buildReadOnlyCustomerAnalytics([
      booking('private', {
        lineUserId: ART_LINE, customerName: 'Secret Person', customerPhone: '0962825392',
        packageId: 'package-secret', remainingMinutes: 9999, paymentStatus: 'package',
      }),
    ], [{ id: ART_LINE, name: 'Secret Person', phone: '0962825392' }], RANGE, { referenceSalt: SALT });
    const json = JSON.stringify(report);
    for (const value of ['Secret Person', '0962825392', ART_LINE, 'package-secret', '9999']) expect(json).not.toContain(value);
    expect(report).toMatchObject({ analyticsVersion: CUSTOMER_ANALYTICS_VERSION, readOnly: true, writesPerformed: 0, piiIncluded: false, packageDocumentsRead: 0, packageBalancesCombined: false });
  });

  test('customer references are stable per access link but unlinkable across different links', () => {
    const rows = [booking('one', { customerPhone: '0899999999' })];
    const first = buildReadOnlyCustomerAnalytics(rows, [], RANGE, { referenceSalt: SALT });
    const repeat = buildReadOnlyCustomerAnalytics(rows, [], RANGE, { referenceSalt: SALT });
    const otherLink = buildReadOnlyCustomerAnalytics(rows, [], RANGE, { referenceSalt: 'e'.repeat(64) });
    expect(first.customers[0].customerRef).toBe(repeat.customers[0].customerRef);
    expect(first.customers[0].customerRef).not.toBe(otherLink.customers[0].customerRef);
  });

  test('requires a private per-link salt rather than emitting reversible phone hashes', () => {
    expect(() => buildReadOnlyCustomerAnalytics([booking('one', { customerPhone: '0899999999' })], [], RANGE)).toThrow('reference salt');
  });
});
