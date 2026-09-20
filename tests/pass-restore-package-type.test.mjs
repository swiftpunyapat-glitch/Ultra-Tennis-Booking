import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { describe, expect, test } from 'vitest';
import { canonicalPackageType, samePackageType } from '../api/_lib/package-type.js';

const source = readFileSync(new URL('../api/admin-edit-booking-accounting.js', import.meta.url), 'utf8');

// passRestoreMutation is module-private. Slice the block that holds it plus the
// helpers it calls, and run it with the module's imports stubbed.
function restoreFn() {
  const slice = source.slice(source.indexOf('const ULTRA_PACKAGE_TYPES'), source.indexOf('const slotClaimRef'));
  const context = vm.createContext({
    samePackageType,
    bookingDurationMin: booking => Number(booking?.durationMinutes) || 60,
    FieldValue: { serverTimestamp: () => 'ts' },
  });
  vm.runInContext(slice, context);
  return context.passRestoreMutation;
}

const ultraPass = { packageType: 'ultra_pass_10', remainingMinutes: 120 };
const booking = extra => ({ packageType: 'ultra_pass_10', durationMinutes: 60, date: '2026-09-21', ...extra });

describe('pass restore tolerates legacy packageType aliases', () => {
  test.each([
    ['ultra_pass_10', 'ultra_pass_10'],
    ['ultra_pass_10', 'ultra_10'],
    ['ultra_pass_20', 'ultra_20'],
    ['ultra_20', 'ultra_pass_20'],
  ])('package %s restores a booking stored as %s', (pkgType, bookingType) => {
    const restore = restoreFn();
    const result = restore({ packageType: pkgType, remainingMinutes: 120 }, booking({ packageType: bookingType }));
    expect(result.update.remainingMinutes).toBe(180);
    expect(result.used).toBe(60);
  });

  test('still rejects a booking that belongs to a different product', () => {
    const restore = restoreFn();
    expect(() => restore(ultraPass, booking({ packageType: 'ultra_starter_3' })))
      .toThrow('PASS_RESTORE_MISMATCH');
  });

  test('keeps restoring when the booking carries no packageType at all', () => {
    const restore = restoreFn();
    const result = restore(ultraPass, { usedPackageType: 'ultra_10', durationMinutes: 90, date: '2026-09-21' });
    expect(result.update.remainingMinutes).toBe(210);
  });
});

describe('accounting edits write the catalog packageType', () => {
  test.each([
    ['ultra_pass_1', 'ultra_pass_10'],
    ['ultra_pass_2', 'ultra_pass_20'],
  ])('%s stores %s so the booking stays matched to its pass', (accountingType, expected) => {
    const branch = source.slice(source.indexOf(`case '${accountingType}':`), source.indexOf('break;', source.indexOf(`case '${accountingType}':`)));
    expect(branch).toContain(`packageType:              '${expected}'`);
    expect(branch).not.toMatch(/packageType:\s+'ultra_(10|20)'/);
  });

  test('an edited Ultra Pass booking can still be cancelled and refunded', () => {
    const restore = restoreFn();
    // What handleAccountingEdit now writes, against the pass the customer holds.
    const edited = booking({ packageType: 'ultra_pass_10' });
    expect(() => restore(ultraPass, edited)).not.toThrow();
  });
});

describe('packageType canonicalisation', () => {
  test.each([
    ['ultra_10', 'ultra_pass_10'],
    ['ultra_20', 'ultra_pass_20'],
    ['ultra_pass_10', 'ultra_pass_10'],
    ['offpeak', 'offpeak'],
    ['', ''],
    [undefined, ''],
  ])('%s canonicalises to %s', (input, expected) => {
    expect(canonicalPackageType(input)).toBe(expected);
  });

  test('does not collapse unrelated products into one another', () => {
    expect(samePackageType('ultra_pass_10', 'ultra_pass_20')).toBe(false);
    expect(samePackageType('ultra_10', 'ultra_20')).toBe(false);
    expect(samePackageType('offpeak', 'ultra_pass_10')).toBe(false);
  });
});
