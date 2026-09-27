import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { describe, expect, test, vi } from 'vitest';

const html = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');
const source = html.slice(html.indexOf('function reschDurationMinutes(b){'), html.indexOf('$("mPendingResch").addEventListener("click",moveToPendingReschedule);'));

function client() {
  const elements = new Map();
  const $ = id => {
    if (!elements.has(id)) elements.set(id, {
      value: '', textContent: '', innerHTML: '', disabled: false, style: {}, options: [],
      addEventListener() {}, replaceChildren() {}, append() {},
    });
    return elements.get(id);
  };
  const courtFetch = vi.fn(async () => ({ ok: true, json: async () => ({ ok: true }) }));
  const context = vm.createContext({
    $, courtFetch, reschBooking: null, reschTime: null,
    Option: function(text, value) { this.text = text; this.value = value; },
    document: { createElement: () => ({}) },
    isPendingRescheduleBooking: b => b.bookingStatus === 'rescheduled' && b.pendingReschedule === true,
    calcBookingDuration: () => 1, confirm: () => true, window: {},
    showToast: vi.fn(), loadBookings: vi.fn(), loadDailyView: vi.fn(),
    console: { error: vi.fn() },
  });
  vm.runInContext(source, context);
  return { context, $, courtFetch };
}

const booking = id => ({ id, bookingCode: id, lineUserId: 'same-customer', customerName: 'Same Customer', customerPhone: '0800000000', date: '2027-05-17', startTime: '10:00', endTime: '11:00', durationMinutes: 60, bookingStatus: 'confirmed' });

describe('Move to Pending button lifecycle', () => {
  test('two bookings for the same customer can be parked consecutively in one page session', async () => {
    const { context, $, courtFetch } = client();
    for (const id of ['first', 'second']) {
      context.openResch(booking(id));
      expect($('mPendingResch').disabled).toBe(false);
      const request = context.moveToPendingReschedule();
      expect($('mPendingResch').disabled).toBe(true);
      await request;
      expect($('mPendingResch').disabled).toBe(false);
      expect($('mPendingResch').textContent).toBe('Move to Pending');
    }
    expect(courtFetch.mock.calls.map(([, options]) => JSON.parse(options.body).bookingId)).toEqual(['first', 'second']);
  });

  test.each(['rejected', 'network'])('a %s request restores the button for retry', async failure => {
    const { context, $, courtFetch } = client();
    if (failure === 'network') courtFetch.mockRejectedValueOnce(new Error('Network unavailable'));
    else courtFetch.mockResolvedValueOnce({ ok: false, json: async () => ({ ok: false, error: 'Rejected' }) });
    context.openResch(booking('first'));
    await context.moveToPendingReschedule();
    expect($('mPendingResch').disabled).toBe(false);
    expect($('mPendingResch').textContent).toBe('Move to Pending');
  });

  test('opening another booking resets stale button state but an already pending booking cannot be parked again', async () => {
    const { context, $, courtFetch } = client();
    $('mPendingResch').disabled = true;
    $('mPendingResch').textContent = 'Moving...';
    context.openResch(booking('second'));
    expect($('mPendingResch').disabled).toBe(false);
    expect($('mPendingResch').textContent).toBe('Move to Pending');
    context.openResch({ ...booking('first'), bookingStatus: 'rescheduled', pendingReschedule: true });
    expect($('mPendingResch').style.display).toBe('none');
    await context.moveToPendingReschedule();
    expect(courtFetch).not.toHaveBeenCalled();
  });
});
