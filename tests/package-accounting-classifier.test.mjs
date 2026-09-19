import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { describe, expect, test } from 'vitest';

const html = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');
const source = html.slice(html.indexOf('function inferAccountingType(b){'), html.indexOf('function closeAcctModal()'));
const packages = [
  ['ultra_pass_10', 'ultra_pass_1'],
  ['ultra_10', 'ultra_pass_1'],
  ['ultra_pass_20', 'ultra_pass_2'],
  ['ultra_20', 'ultra_pass_2'],
  // These products have no dedicated accounting-edit option yet. This scoped
  // alias fix intentionally preserves their existing fallback, not an approval
  // of that fallback as the correct accounting treatment.
  ['ultra_starter_3', 'normal_unpaid'],
  ['beginner_coaching_5', 'normal_unpaid'],
  ['coach_at_ultra_10', 'normal_unpaid'],
  ['offpeak', 'normal_unpaid'],
  ['monstr_event_pass', 'normal_unpaid'],
];

function client() {
  const elements = new Map();
  const $ = id => {
    if (!elements.has(id)) elements.set(id, { value: '', textContent: '', style: {}, disabled: false });
    return elements.get(id);
  };
  const context = vm.createContext({ $, acctBooking: null, calcBookingDuration: () => 2, bookingHourlyRate: () => 350, esc: value => value });
  vm.runInContext(source, context);
  return { context, $ };
}

describe('package accounting classifier', () => {
  test.each(packages)('%s selects %s for a package-funded booking', (packageType, expected) => {
    const { context } = client();
    const booking = { packageType, paymentStatus: 'package', bookingStatus: 'confirmed' };
    expect(context.inferAccountingType(booking)).toBe(expected);
    expect(booking).toEqual({ packageType, paymentStatus: 'package', bookingStatus: 'confirmed' });
  });

  test('covers every current issuance key and both legacy aliases', () => {
    const server = readFileSync(new URL('../api/admin-user-action.js', import.meta.url), 'utf8');
    const catalog = server.slice(server.indexOf('const ACTIVE_PACKAGES = {'), server.indexOf('const normalizePhone'));
    const issuedKeys = [...catalog.matchAll(/packageType:\s*"([^"]+)"/g)].map(match => match[1]);
    expect(issuedKeys.length).toBeGreaterThan(0);
    expect(packages.map(([key]) => key).sort()).toEqual([...issuedKeys, 'ultra_10', 'ultra_20'].sort());
  });

  test.each(packages)('%s preserves non-package statuses and influencer precedence', (packageType) => {
    const { context } = client();
    for (const [paymentStatus, expected] of [
      ['paid', 'normal_paid'], ['unpaid', 'normal_unpaid'],
      ['pending_review', 'pending_review'], ['rejected', 'rejected'],
    ]) {
      expect(context.inferAccountingType({ packageType, paymentStatus })).toBe(expected);
    }
    expect(context.inferAccountingType({ packageType, paymentStatus: 'package', isInfluencerBooking: true })).toBe('influencer_free');
  });

  test.each(['unknown_package', '', undefined])('preserves fallback for unknown/missing key %s', packageType => {
    expect(client().context.inferAccountingType({ packageType, paymentStatus: 'package' })).toBe('normal_unpaid');
  });

  test.each(packages)('%s follows legacy accounting precedence even when cancelled', (packageType, expected) => {
    expect(client().context.inferAccountingType({ packageType, paymentStatus: 'package', bookingStatus: 'cancelled' })).toBe(expected === 'normal_unpaid' ? 'rejected' : expected);
  });

  test.each([
    ['ultra_pass_10', 'ultra_10', 'ultra_pass_1', 620],
    ['ultra_pass_20', 'ultra_20', 'ultra_pass_2', 590],
  ])('%s opens the same accounting option, amount and preview as %s', (current, legacy, expectedType, expectedAmount) => {
    const results = [current, legacy].map(packageType => {
      const { context, $ } = client();
      context.openAcctModal({ packageType, paymentStatus: 'package', bookingStatus: 'confirmed', price: 0 });
      return { type: $('acctType').value, amount: $('acctPrice').value, status: $('acctBookingStatus').value, preview: $('acctPreview').textContent };
    });
    expect(results[0]).toEqual(results[1]);
    expect(results[0]).toMatchObject({ type: expectedType, amount: expectedAmount, status: 'confirmed' });
    expect(results[0].preview).toContain('→ package');
    expect(results[0].preview).toContain(`฿${expectedAmount}`);
  });
});
