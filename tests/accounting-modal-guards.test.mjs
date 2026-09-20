import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { describe, expect, test } from 'vitest';

const html = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');
const source = html.slice(html.indexOf('function inferAccountingType(b){'), html.indexOf('function closeAcctModal()'));

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

const open = (booking, extra = {}) => {
  const { context, $ } = client();
  context.openAcctModal({ bookingStatus: 'confirmed', price: 0, ...booking, ...extra });
  return { context, $ };
};

// Every package product the admin can issue, minus the two Ultra Passes that
// have their own accounting option. Voucher bookings land here too: they are
// paymentStatus "package" with no packageType.
const unmapped = ['ultra_starter_3', 'beginner_coaching_5', 'coach_at_ultra_10', 'offpeak', 'monstr_event_pass', undefined];
const mapped = [['ultra_pass_10', 'ultra_pass_1'], ['ultra_10', 'ultra_pass_1'], ['ultra_pass_20', 'ultra_pass_2'], ['ultra_20', 'ultra_pass_2']];

describe('package bookings with no matching accounting option', () => {
  test.each(unmapped)('%s opens unselected with Save blocked', packageType => {
    const { $ } = open({ paymentStatus: 'package', packageType });
    expect($('acctType').value).toBe('');
    expect($('acctConfirm').disabled).toBe(true);
    expect($('acctUnmappedWarn').style.display).toBe('');
    expect($('acctUnmappedWarn').textContent).toContain('bill the customer again');
  });

  test.each(unmapped)('%s never pre-selects normal_unpaid or a price to charge', packageType => {
    const { $ } = open({ paymentStatus: 'package', packageType });
    expect($('acctType').value).not.toBe('normal_unpaid');
    expect($('acctPreview').textContent).toBe('');
  });

  test('the warning names the package so the admin knows what is unmapped', () => {
    expect(open({ paymentStatus: 'package', packageType: 'offpeak' }).$('acctUnmappedWarn').textContent).toContain('offpeak');
  });

  test('choosing a type clears the warning and unblocks Save', () => {
    const { context, $ } = open({ paymentStatus: 'package', packageType: 'offpeak' });
    $('acctType').value = 'normal_paid';
    context.syncAcctModal(true);
    expect($('acctConfirm').disabled).toBe(false);
    expect($('acctUnmappedWarn').style.display).toBe('none');
  });
});

describe('bookings that do have a matching option are untouched', () => {
  test.each(mapped)('%s still pre-selects %s with Save enabled', (packageType, expected) => {
    const { $ } = open({ paymentStatus: 'package', packageType });
    expect($('acctType').value).toBe(expected);
    expect($('acctConfirm').disabled).toBe(false);
    expect($('acctUnmappedWarn').style.display).toBe('none');
  });

  test.each([['paid', 'normal_paid'], ['unpaid', 'normal_unpaid'], ['pending_review', 'pending_review'], ['rejected', 'rejected']])(
    'a %s booking is unaffected by the package guard', (paymentStatus, expected) => {
      const { $ } = open({ paymentStatus });
      expect($('acctType').value).toBe(expected);
      expect($('acctConfirm').disabled).toBe(false);
    });

  test('an influencer booking is still recognised even on an unmapped package', () => {
    const { $ } = open({ paymentStatus: 'package', packageType: 'offpeak', isInfluencerBooking: true });
    expect($('acctType').value).toBe('influencer_free');
    expect($('acctConfirm').disabled).toBe(false);
  });
});

describe('opening the modal does not rewrite the booking status', () => {
  test.each([
    ['paid', 'cancelled'],
    ['package', 'cancelled'],
    ['paid', 'pending_payment'],
    ['unpaid', 'rescheduled'],
    ['pending_review', 'pending_reschedule'],
  ])('a %s booking in %s keeps that status in the dropdown', (paymentStatus, bookingStatus) => {
    const { $ } = open({ paymentStatus, packageType: 'ultra_pass_10', bookingStatus });
    expect($('acctBookingStatus').value).toBe(bookingStatus);
  });

  test('the canonical status still applies once the admin changes the type', () => {
    const { context, $ } = open({ paymentStatus: 'paid', bookingStatus: 'cancelled' });
    expect($('acctBookingStatus').value).toBe('cancelled');
    $('acctType').value = 'normal_paid';
    context.syncAcctModal(true);
    expect($('acctBookingStatus').value).toBe('confirmed');
  });

  test('rejected still forces cancelled when the admin picks it', () => {
    const { context, $ } = open({ paymentStatus: 'paid', bookingStatus: 'confirmed' });
    $('acctType').value = 'rejected';
    context.syncAcctModal(true);
    expect($('acctBookingStatus').value).toBe('cancelled');
  });
});

describe('save handler', () => {
  test('refuses to submit without an accounting type', () => {
    expect(html).toContain('if(!type){showToast("Select an accounting type first.",true);$("acctType").focus();return;}');
  });

  test('the placeholder option cannot be chosen by hand', () => {
    expect(html).toContain('<option value="" disabled>— Select accounting type —</option>');
  });
});
