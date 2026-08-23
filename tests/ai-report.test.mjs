import { beforeEach, describe, expect, test, vi } from 'vitest';
import { Timestamp } from 'firebase-admin/firestore';
import {
  buildAiBookingReport, createAiReportToken, hashAiReportToken,
  normalizeAiReportAccessInput, normalizeAiReportToken, resolveAiReportRange,
} from '../api/_lib/ai-report.js';

const mockState = vi.hoisted(() => ({
  access: null,
  bookings: [],
  rateAllowed: true,
  usageSet: vi.fn(),
}));

vi.mock('../api/_lib/firebase-admin.js', () => ({
  getAdminDb: () => ({
    collection(name) {
      if (name === 'ai_report_access') return {
        doc: id => ({
          get: async () => ({
            exists: !!mockState.access,
            data: () => mockState.access,
            ref: { set: mockState.usageSet },
            id,
          }),
        }),
      };
      if (name === 'bookings') {
        const query = {
          where: vi.fn(() => query),
          get: async () => ({ docs: mockState.bookings.map(item => ({ id: item.id, data: () => item.data })) }),
        };
        return query;
      }
      throw new Error(`Unexpected collection ${name}`);
    },
  }),
  checkRateLimit: async () => ({ allowed: mockState.rateAllowed, retryAfterSec: 60 }),
  clientIp: () => '203.0.113.7',
  RATE_LIMITS: { aiReportRead: { limit: 60, windowMs: 3600000, blockMs: 3600000 } },
}));

const { default: handler } = await import('../api/ai-report.js');
const TOKEN = 'A'.repeat(43);

function call({ method = 'GET', query = {}, authorization = '' } = {}) {
  let statusCode = 200;
  let payload;
  const headers = {};
  const req = { method, query, headers: { authorization }, socket: {} };
  const res = {
    status(code) { statusCode = code; return this; },
    json(value) { payload = value; return this; },
    setHeader(key, value) { headers[key] = value; return this; },
  };
  return Promise.resolve(handler(req, res)).then(() => ({ statusCode, payload, headers }));
}

beforeEach(() => {
  mockState.access = {
    active: true,
    label: 'Ace Report',
    expiresAt: Timestamp.fromMillis(Date.now() + 86400000),
    scopes: ['booking_summary', 'booking_details_sanitized'],
  };
  mockState.bookings = [];
  mockState.rateAllowed = true;
  mockState.usageSet.mockReset();
});

describe('AI Report token and range rules', () => {
  test('creates opaque tokens and stores a stable hash rather than the raw token', () => {
    const token = createAiReportToken();
    expect(normalizeAiReportToken(token)).toBe(token);
    expect(token).toHaveLength(43);
    expect(hashAiReportToken(token)).toMatch(/^[a-f0-9]{64}$/);
    expect(hashAiReportToken(token)).not.toContain(token);
    expect(normalizeAiReportToken('short')).toBeNull();
  });

  test('validates owner-created labels and bounded expiry', () => {
    expect(normalizeAiReportAccessInput({ label: 'Ace', expiresDays: 365 })).toEqual({ ok: true, label: 'Ace', expiresDays: 365 });
    expect(normalizeAiReportAccessInput({ label: '', expiresDays: 365 })).toMatchObject({ ok: false });
    expect(normalizeAiReportAccessInput({ label: 'Ace', expiresDays: 3651 })).toMatchObject({ ok: false });
  });

  test('supports a month or a maximum 366-day explicit range', () => {
    expect(resolveAiReportRange({ month: '2026-02' })).toMatchObject({ ok: true, from: '2026-02-01', to: '2026-02-28' });
    expect(resolveAiReportRange({ from: '2026-01-01', to: '2026-12-31' })).toMatchObject({ ok: true, mode: 'range' });
    expect(resolveAiReportRange({ month: '2026-08', from: '2026-08-01', to: '2026-08-31' })).toMatchObject({ ok: false });
    expect(resolveAiReportRange({ from: '2025-01-01', to: '2026-12-31' })).toMatchObject({ ok: false });
  });
});

describe('AI Report aggregation and privacy', () => {
  test('totals live bookings and revenue while retaining cancelled counts', () => {
    const report = buildAiBookingReport([
      { id: 'paid', data: { date: '2026-08-10', startTime: '13:30', endTime: '14:30', bookingStatus: 'confirmed', paymentStatus: 'paid', price: 390 } },
      { id: 'pass', data: { date: '2026-08-11', startTime: '10:00', endTime: '11:00', bookingStatus: 'confirmed', paymentStatus: 'package', packageUsageValueTotal: 350, packageType: 'ultra_pass_10' } },
      { id: 'cancelled', data: { date: '2026-08-12', startTime: '09:00', endTime: '10:00', bookingStatus: 'cancelled', paymentStatus: 'paid', price: 390 } },
    ], { from: '2026-08-01', to: '2026-08-31' });
    expect(report.metrics).toMatchObject({ recordsTotal: 3, bookingsTotal: 2, cancelledCount: 1, paidRevenue: 390, packageUsageValue: 350, totalBookedMinutes: 120 });
    expect(report.breakdown.byStartHour).toMatchObject({ '13': 1, '10': 1, '09': 1 });
  });

  test('details are explicitly projected and never leak customer PII or slips', () => {
    const report = buildAiBookingReport([{ id: 'safe-id', data: {
      id: 'spoofed-id', date: '2026-08-10', startTime: '13:30', endTime: '14:30',
      bookingStatus: 'confirmed', paymentStatus: 'paid', price: 390,
      customerName: 'Secret Name', customerPhone: '0800000000', lineUserId: 'U-secret',
      slipUrl: 'https://secret.invalid/slip.jpg', secretUnprojectedField: 'do-not-leak',
    } }], { from: '2026-08-01', to: '2026-08-31' }, { details: true });
    expect(report.bookings[0].id).toBe('safe-id');
    const json = JSON.stringify(report);
    for (const secret of ['Secret Name', '0800000000', 'U-secret', 'secret.invalid', 'do-not-leak']) expect(json).not.toContain(secret);
  });
});

describe('AI Report HTTP security boundary', () => {
  test('rejects missing, revoked and expired access', async () => {
    expect((await call()).statusCode).toBe(401);
    mockState.access = { ...mockState.access, active: false };
    expect((await call({ query: { token: TOKEN } })).statusCode).toBe(401);
    mockState.access = { ...mockState.access, active: true, expiresAt: Timestamp.fromMillis(Date.now() - 1) };
    expect((await call({ authorization: `Bearer ${TOKEN}` })).statusCode).toBe(410);
  });

  test('returns no-store sanitized data to a valid bearer or link token', async () => {
    mockState.bookings = [{ id: 'b1', data: {
      date: '2026-08-10', startTime: '13:30', endTime: '14:30', bookingStatus: 'confirmed', paymentStatus: 'paid', price: 390,
      customerName: 'Hidden', customerPhone: '0800000000', lineUserId: 'U-hidden', slipUrl: 'https://secret.invalid/slip',
    } }];
    const out = await call({ query: { token: TOKEN, month: '2026-08', details: '1' } });
    expect(out.statusCode).toBe(200);
    expect(out.headers['Cache-Control']).toContain('no-store');
    expect(out.payload).toMatchObject({ ok: true, privacy: { piiIncluded: false }, metrics: { bookingsTotal: 1, paidRevenue: 390 } });
    expect(JSON.stringify(out.payload)).not.toContain('Hidden');
    expect(mockState.usageSet).toHaveBeenCalledOnce();
  });

  test('enforces rate limits and sanitized-detail scope', async () => {
    mockState.rateAllowed = false;
    expect((await call({ query: { token: TOKEN } })).statusCode).toBe(429);
    mockState.rateAllowed = true;
    mockState.access.scopes = ['booking_summary'];
    expect((await call({ query: { token: TOKEN, details: '1' } })).statusCode).toBe(403);
  });
});
