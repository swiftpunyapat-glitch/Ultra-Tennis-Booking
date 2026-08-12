import { describe, expect, test } from 'vitest';
import { readFileSync } from 'node:fs';
import {
  generateVoucherCodes,
  normalizeCampaignInput,
  normalizeCustomVoucherCode,
  normalizeRandomCodeRequest,
} from '../api/_lib/voucher-admin.js';

const adminHtml = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');
const adminApi = readFileSync(new URL('../api/admin-user-action.js', import.meta.url), 'utf8');

const validCampaign = (extra = {}) => ({
  campaignId: 'monstr-2026', name: 'MONSTR Sponsor 2026',
  keyword: 'MONSTR', codePrefix: 'MSTR-', active: true,
  voucherType: 'free_booking',
  validFrom: '2026-08-12T00:00:00+07:00', expiresAt: '2026-12-31T23:59:59+07:00',
  allowedDays: [1, 2, 3, 4, 5], startTime: '06:00', endTime: '24:00',
  exactDurationMinutes: 60, excludeHolidays: true, requiresLineLogin: true,
  transferable: true, maxCancellationRestores: 2, allowedPricingTypes: [],
  minFinalPrice: 0,
  ...extra,
});

describe('Voucher Manager campaign validation', () => {
  test('normalizes an owner-authored free campaign into the engine schema', () => {
    const result = normalizeCampaignInput(validCampaign());
    expect(result).toMatchObject({
      ok: true, campaignId: 'monstr-2026',
      data: {
        schemaVersion: 2, voucherType: 'free_booking', active: true,
        allowedDays: [1, 2, 3, 4, 5], exactDurationMinutes: 60,
        maxUsesPerCode: 1, maxCancellationRestores: 2,
        branchId: 'ladprao1', resourceId: 'room1',
      },
    });
  });

  test('supports amount and percent configuration with bounded values', () => {
    expect(normalizeCampaignInput(validCampaign({
      voucherType: 'discount_amount', discountAmount: 80,
      allowedPricingTypes: ['standard'], minFinalPrice: 100,
    }))).toMatchObject({ ok: true, data: { discountAmount: 80, minFinalPrice: 100 } });
    expect(normalizeCampaignInput(validCampaign({
      voucherType: 'discount_percent', discountPercent: 25, maxDiscountAmount: 100,
    }))).toMatchObject({ ok: true, data: { discountPercent: 25, maxDiscountAmount: 100 } });
  });

  test.each([
    [{ campaignId: '../bad' }, 'Campaign ID'],
    [{ allowedDays: [] }, 'Select at least one'],
    [{ startTime: '20:00', endTime: '06:00' }, 'End time'],
    [{ exactDurationMinutes: 30 }, 'fixed at 60'],
    [{ validFrom: '2026-12-31T00:00:00+07:00', expiresAt: '2026-01-01T00:00:00+07:00' }, 'Expiry'],
    [{ voucherType: 'discount_amount', discountAmount: 0 }, 'Discount amount'],
    [{ voucherType: 'discount_percent', discountPercent: 101 }, 'Discount percent'],
    [{ allowedPricingTypes: ['unknown_rate'] }, 'Pricing scope'],
  ])('rejects invalid campaign rule %#', (override, message) => {
    const result = normalizeCampaignInput(validCampaign(override));
    expect(result.ok).toBe(false);
    expect(result.error).toContain(message);
  });
});

describe('Voucher Manager code creation', () => {
  test('normalizes custom codes and rejects unsafe document IDs', () => {
    expect(normalizeCustomVoucherCode(' vip-art_01 ')).toEqual({ ok: true, code: 'VIP-ART_01' });
    expect(normalizeCustomVoucherCode('../VIP')).toMatchObject({ ok: false });
    expect(normalizeCustomVoucherCode('A/B')).toMatchObject({ ok: false });
  });

  test('generates unique unambiguous prefixed codes', () => {
    const request = normalizeRandomCodeRequest({ count: 100, randomLength: 8 }, 'MSTR-');
    expect(request).toMatchObject({ ok: true, count: 100, randomLength: 8, prefix: 'MSTR-' });
    const codes = generateVoucherCodes(request);
    expect(codes).toHaveLength(100);
    expect(new Set(codes).size).toBe(100);
    expect(codes.every(code => /^MSTR-[A-HJ-NP-Z2-9]{8}$/.test(code))).toBe(true);
  });

  test('caps bulk generation and code length', () => {
    expect(normalizeRandomCodeRequest({ count: 101, randomLength: 8 }, 'MSTR-')).toMatchObject({ ok: false });
    expect(normalizeRandomCodeRequest({ count: 1, randomLength: 17 }, 'MSTR-')).toMatchObject({ ok: false });
  });
});

describe('Voucher Manager security surface', () => {
  test('hides the tab unless the authenticated session is Art owner', () => {
    expect(adminHtml).toContain('id="voucherTabBtn"');
    expect(adminHtml).toContain('currentAdminName==="Art"&&currentAdminRole==="owner"');
  });

  test('uses the existing multiplexed API and repeats the Art-owner gate server-side', () => {
    expect(adminHtml).toContain('fetch("/api/admin-user-action"');
    expect(adminApi).toContain("adminName !== 'Art' || !requireRole(session, 'owner')");
    for (const action of ['voucher_list', 'voucher_save_campaign', 'voucher_set_campaign_active', 'voucher_create_codes', 'voucher_set_code_active']) {
      expect(adminApi).toContain(`'${action}'`);
    }
  });
});
