import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { randomUUID } from 'node:crypto';
import { describe, expect, test } from 'vitest';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const source = html.slice(html.indexOf('let coachAddonV2RetryIntent='), html.indexOf('// ── CONFIRM BOOKING'));
function client(fetch, storage = new Map()) {
  const sessionStorage = { getItem: k => storage.get(k) || null, setItem: (k, v) => storage.set(k, v), removeItem: k => storage.delete(k) };
  const noop = () => {};
  const context = vm.createContext({
    fetch, sessionStorage, localStorage: sessionStorage, crypto: { randomUUID }, window: {},
    state: { coachAddonV2: { active: true, selectedCoach: { id: 'c1' }, quote: {}, fundingMode: 'cash', studentCount: 1 }, time: '10:00', durationMinutes: 60, lineProfile: { userId: 'guest', displayName: 'Guest' } },
    coachAddonV2AuthFields: async () => ({}), protectedIdToken: async () => null,
    showToast: noop, t: x => x, refreshCoachAddonV2Options: noop, checkEligibility: noop,
    showSuccess: noop, showPaymentPanel: noop, scheduleCoachAddonV2Expiry: noop, showView: noop,
  });
  vm.runInContext(source, context);
  return { call: () => context.tryCoachAddonV2Create({ dateISO: '2027-06-14', name: 'Customer', phone: '0810000001', note: '' }), storage };
}
const success = () => ({ ok: true, status: 200, json: async () => ({ ok: true, requiresPayment: true, guestAccessToken: 'token', paymentExpiresAt: '2027-06-14T04:00:00Z', booking: { id: 'b1', bookingCode: 'CODE', endTime: '11:00', finalPrice: 900, qrAmount: 900 } }) });

describe('v2 client lost-response recovery', () => {
  test('network loss and a later auth failure retain the same key, including across page reload', async () => {
    const sent = [];
    const storage = new Map();
    const first = client(async (_url, init) => { sent.push(JSON.parse(init.body)); throw new Error('response lost'); }, storage);
    await first.call();
    const reloaded = client(async (_url, init) => { sent.push(JSON.parse(init.body)); return { ok: false, status: 401, json: async () => ({ ok: false }) }; }, storage);
    await reloaded.call();
    const recovered = client(async (_url, init) => { sent.push(JSON.parse(init.body)); return success(); }, storage);
    expect(await recovered.call()).toBe(true);
    expect(new Set(sent.map(body => body.idempotencyKey)).size).toBe(1);
    expect(storage.has('ut_coach_v2_retry')).toBe(false);
  });
  test('malformed success response retains the key; success clears it for a new intent', async () => {
    const sent = [];
    const app = client(async (_url, init) => {
      sent.push(JSON.parse(init.body));
      return sent.length === 1 ? { ok: true, status: 200, json: async () => { throw new Error('truncated response'); } } : success();
    });
    await app.call();await app.call();await app.call();
    expect(sent[0].idempotencyKey).toBe(sent[1].idempotencyKey);
    expect(sent[2].idempotencyKey).not.toBe(sent[1].idempotencyKey);
  });
});
