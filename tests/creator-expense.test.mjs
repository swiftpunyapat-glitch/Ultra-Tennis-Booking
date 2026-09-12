import { describe, expect, test } from 'vitest';
import { readFileSync } from 'node:fs';
import {
  CREATOR_EXPENSE_HOURLY_FALLBACK,
  buildCreatorExpenseDelete,
  buildCreatorExpenseDoc,
  buildCreatorExpenseNote,
  buildCreatorExpenseUpdate,
  resolveCreatorExpenseAmount,
} from '../api/_lib/creator-expense.js';
import { applyVoucherToQuote, evaluateVoucher } from '../api/_lib/voucher-engine.js';
import { normalizeCampaignInput } from '../api/_lib/voucher-admin.js';

const adminHtml = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');
const adminApi = readFileSync(new URL('../api/admin-user-action.js', import.meta.url), 'utf8');

const TS = { __serverTimestamp: true };

describe('creator expense amount', () => {
  test('an explicit admin amount wins over every fallback', () => {
    expect(resolveCreatorExpenseAmount({
      explicitAmount: 500, storedValue: 330, durationHours: 2,
    })).toBe(500);
  });

  test('an explicit zero is floored to 1 rather than booking a free giveaway', () => {
    expect(resolveCreatorExpenseAmount({ explicitAmount: 0, storedValue: 330 })).toBe(1);
  });

  test('a slot is costed at what it would have sold for', () => {
    expect(resolveCreatorExpenseAmount({ storedValue: 330, durationHours: 1 })).toBe(330);
    expect(resolveCreatorExpenseAmount({ storedValue: 320, durationHours: 1 })).toBe(320);
    expect(resolveCreatorExpenseAmount({ storedValue: 450, durationHours: 1 })).toBe(450);
  });

  test('a fractional stored value rounds up, never down', () => {
    expect(resolveCreatorExpenseAmount({ storedValue: 330.2 })).toBe(331);
  });

  test('a campaign rate prices the hours when no stored value is given', () => {
    expect(resolveCreatorExpenseAmount({ durationHours: 2, hourlyRate: 400 })).toBe(800);
    expect(resolveCreatorExpenseAmount({ durationHours: 1.5, hourlyRate: 300 })).toBe(450);
  });

  test('the legacy flat rate is the last resort', () => {
    expect(resolveCreatorExpenseAmount({ durationHours: 1 })).toBe(CREATOR_EXPENSE_HOURLY_FALLBACK);
    expect(resolveCreatorExpenseAmount({})).toBe(CREATOR_EXPENSE_HOURLY_FALLBACK);
  });

  test('a booking with no usable price or duration still costs something', () => {
    // The pre-refactor expression produced a 1 baht expense here, which reads
    // as a data-entry error rather than a giveaway.
    expect(resolveCreatorExpenseAmount({ storedValue: 0, durationHours: 0 }))
      .toBe(CREATOR_EXPENSE_HOURLY_FALLBACK);
  });

  test('a non-numeric amount falls through instead of producing NaN', () => {
    expect(resolveCreatorExpenseAmount({ explicitAmount: 'abc', storedValue: 330 })).toBe(330);
  });
});

describe('creator expense note', () => {
  test('carries the booking code, customer and slot', () => {
    expect(buildCreatorExpenseNote({
      bookingCode: 'UT-1234', customerName: 'Ploy', customerPhone: '0812345678',
      date: '2026-09-14', startTime: '13:00', endTime: '14:00',
    })).toBe('Auto: UT-1234 - Ploy (0812345678) plays 2026-09-14 13:00–14:00');
  });

  test('falls back to the document id when no booking code exists', () => {
    expect(buildCreatorExpenseNote({ bookingId: 'abc123' })).toBe('Auto: abc123');
  });

  test('omits missing parts without leaving double spaces', () => {
    expect(buildCreatorExpenseNote({ bookingCode: 'UT-1', date: '2026-09-14' }))
      .toBe('Auto: UT-1 plays 2026-09-14');
  });

  test('is capped so one long name cannot overflow the field', () => {
    expect(buildCreatorExpenseNote({
      bookingCode: 'UT-1', customerName: 'x'.repeat(900),
    }).length).toBe(400);
  });
});

describe('creator expense document', () => {
  const doc = buildCreatorExpenseDoc({
    amount: 330, note: 'Auto: UT-1234', date: '2026-09-14', bookingId: 'bk1',
    createdBy: 'Art', timestamp: TS,
  });

  test('lands in Marketing so it reduces net profit', () => {
    expect(doc.category).toBe('Marketing');
    expect(doc.amount).toBe(330);
    expect(doc.deleted).toBe(false);
  });

  test('stays traceable back to its booking', () => {
    expect(doc).toMatchObject({
      autoCreated: true,
      sourceType: 'influencer_free_slot',
      sourceBookingId: 'bk1',
      businessUnit: 'ultra_tennis',
      paymentMethod: 'Other',
    });
  });

  test('defaults the vendor when a campaign names none', () => {
    expect(doc.vendor).toBe('Influencer Free Slot');
  });

  test('groups spend under the campaign when one is named', () => {
    const campaignDoc = buildCreatorExpenseDoc({
      amount: 330, note: 'n', date: '2026-09-14', bookingId: 'bk1',
      vendor: 'MONSTR Sponsor 2026', campaignId: 'monstr-2026',
      voucherCode: 'MSTR-ABCDE', createdBy: 'system:voucher', timestamp: TS,
    });
    expect(campaignDoc).toMatchObject({
      vendor: 'MONSTR Sponsor 2026',
      sourceCampaignId: 'monstr-2026',
      sourceVoucherCode: 'MSTR-ABCDE',
      addedByAdmin: 'system:voucher',
    });
  });

  test('omits campaign fields entirely for a manual admin reclassification', () => {
    expect(doc).not.toHaveProperty('sourceCampaignId');
    expect(doc).not.toHaveProperty('sourceVoucherCode');
  });

  test('defaults the date rather than writing an undated expense', () => {
    const dated = buildCreatorExpenseDoc({ amount: 1, note: 'n', bookingId: 'bk1', timestamp: TS });
    expect(dated.date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });
});

describe('creator expense reversal', () => {
  test('an update re-points the existing row instead of stacking a second one', () => {
    expect(buildCreatorExpenseUpdate({ amount: 450, note: 'n2', adminName: 'Art', timestamp: TS }))
      .toEqual({ amount: 450, note: 'n2', updatedByAdmin: 'Art', updatedAt: TS });
  });

  test('a delete is soft, so the row survives for audit', () => {
    const del = buildCreatorExpenseDelete({ adminName: 'Art', timestamp: TS });
    expect(del).toEqual({ deleted: true, deletedAt: TS, deletedBy: 'Art' });
  });
});

// Campaign policy through the voucher engine
const nowMs = Date.parse('2026-09-14T10:00:00+07:00');
const baseQuote = {
  pricingType: 'morning_weekday', originalPrice: 330, finalPrice: 330,
  price: 330, amount: 330, qrAmount: 330, qrType: 'normal',
};
const freeCampaign = {
  id: 'creator-2026', schemaVersion: 2, campaignId: 'creator-2026',
  name: 'Creator Program 2026', active: true, voucherType: 'free_booking',
  allowedDays: [1, 2, 3, 4, 5], startTime: '06:00', endTime: '24:00',
  exactDurationMinutes: 60, maxUsesPerCode: 1,
};
const freeVoucher = {
  schemaVersion: 2, campaignId: 'creator-2026', active: true,
  state: 'available', usedCount: 0, maxUses: 1,
};
const evaluate = (campaign = freeCampaign, voucher = freeVoucher) => evaluateVoucher({
  voucher, campaign, code: 'CRTR-ABCDE', nowMs, lineUserId: 'U1',
  date: '2026-09-14', startTime: '13:00', durationMinutes: 60,
  isHoliday: false, baseQuote,
});

describe('campaign marketing-expense policy', () => {
  test('is off unless a campaign opts in, so existing books do not move', () => {
    const result = evaluate();
    expect(result.ok).toBe(true);
    expect(result.marketingExpense).toBe(false);
    expect(applyVoucherToQuote(baseQuote, result).voucherMarketingExpense).toBe(false);
  });

  test('an opted-in campaign flags the booking for an expense', () => {
    const result = evaluate({ ...freeCampaign, marketingExpense: true });
    const quote = applyVoucherToQuote(baseQuote, result);
    expect(quote.voucherMarketingExpense).toBe(true);
    expect(quote.isFreeVoucher).toBe(true);
  });

  test('the vendor falls back to the campaign name for per-campaign grouping', () => {
    const quote = applyVoucherToQuote(baseQuote, evaluate({ ...freeCampaign, marketingExpense: true }));
    expect(quote.voucherExpenseVendor).toBe('Creator Program 2026');
  });

  test('an explicit vendor overrides the campaign name', () => {
    const quote = applyVoucherToQuote(baseQuote,
      evaluate({ ...freeCampaign, marketingExpense: true, expenseVendor: 'Creator - Ploy' }));
    expect(quote.voucherExpenseVendor).toBe('Creator - Ploy');
  });

  test('a campaign rate reaches the booking route', () => {
    const quote = applyVoucherToQuote(baseQuote,
      evaluate({ ...freeCampaign, marketingExpense: true, expenseHourlyRate: 250 }));
    expect(quote.voucherExpenseHourlyRate).toBe(250);
    expect(resolveCreatorExpenseAmount({
      storedValue: quote.voucherExpenseHourlyRate > 0 ? null : quote.originalPrice,
      durationHours: 1, hourlyRate: quote.voucherExpenseHourlyRate,
    })).toBe(250);
  });

  test('with no campaign rate the slot is costed at its own price', () => {
    const quote = applyVoucherToQuote(baseQuote, evaluate({ ...freeCampaign, marketingExpense: true }));
    expect(resolveCreatorExpenseAmount({
      storedValue: quote.voucherExpenseHourlyRate > 0 ? null : quote.originalPrice,
      durationHours: 1, hourlyRate: quote.voucherExpenseHourlyRate,
    })).toBe(330);
  });

  test('a discount voucher never books a giveaway expense, no court is given away', () => {
    const discountCampaign = {
      ...freeCampaign, voucherType: 'discount_amount',
      discountAmount: 100, marketingExpense: true,
    };
    const result = evaluate(discountCampaign, { ...freeVoucher, voucherType: 'discount_amount' });
    expect(result.ok).toBe(true);
    expect(result.isFree).toBe(false);
    expect(applyVoucherToQuote(baseQuote, result).voucherMarketingExpense).toBe(false);
  });
});

// Campaigns are saved with { merge: true }, so an absent field is preserved
// rather than cleared. The Voucher tab does not send these yet.
const validCampaign = (extra = {}) => ({
  campaignId: 'creator-2027', name: 'Creator Program 2027',
  keyword: 'CREATOR', codePrefix: 'CRTR-', active: true,
  voucherType: 'free_booking',
  validFrom: '2027-01-01T00:00:00+07:00', expiresAt: '2027-12-31T23:59:59+07:00',
  allowedDays: [1, 2, 3, 4, 5], startTime: '06:00', endTime: '24:00',
  exactDurationMinutes: 60, excludeHolidays: true, requiresLineLogin: true,
  transferable: false, maxCancellationRestores: 2, allowedPricingTypes: [],
  minFinalPrice: 0,
  ...extra,
});

describe('campaign admin accepts the marketing-expense policy', () => {
  test('a client that omits the policy does not clear an opt-in set elsewhere', () => {
    const { data } = normalizeCampaignInput(validCampaign());
    expect(data).not.toHaveProperty('marketingExpense');
    expect(data).not.toHaveProperty('expenseVendor');
    expect(data).not.toHaveProperty('expenseHourlyRate');
  });

  test('an opt-in round-trips', () => {
    const { data } = normalizeCampaignInput(validCampaign({ marketingExpense: true }));
    expect(data.marketingExpense).toBe(true);
  });

  test('an explicit opt-out is written, not omitted', () => {
    const { data } = normalizeCampaignInput(validCampaign({ marketingExpense: false }));
    expect(data.marketingExpense).toBe(false);
  });

  test('vendor and rate round-trip', () => {
    const { data } = normalizeCampaignInput(validCampaign({
      marketingExpense: true, expenseVendor: 'Creator - Ploy', expenseHourlyRate: 250,
    }));
    expect(data).toMatchObject({ expenseVendor: 'Creator - Ploy', expenseHourlyRate: 250 });
  });

  test('a negative or junk rate cannot produce a negative cost', () => {
    expect(normalizeCampaignInput(validCampaign({ expenseHourlyRate: -50 })).data.expenseHourlyRate).toBe(0);
    expect(normalizeCampaignInput(validCampaign({ expenseHourlyRate: 'abc' })).data.expenseHourlyRate).toBe(0);
  });

  test('an empty vendor stores null rather than an empty string', () => {
    expect(normalizeCampaignInput(validCampaign({ expenseVendor: '  ' })).data.expenseVendor).toBeNull();
  });
});

describe('Voucher tab exposes the policy', () => {
  test('the form carries a control for each field', () => {
    for (const id of ['vcMarketingExpense', 'vcExpenseVendor', 'vcExpenseRate']) {
      expect(adminHtml).toContain(`id="${id}"`);
    }
  });

  test('a save sends all three', () => {
    expect(adminHtml).toContain('marketingExpense:$("vcMarketingExpense").checked');
    expect(adminHtml).toContain('expenseVendor:$("vcExpenseVendor").value.trim()');
    expect(adminHtml).toContain('expenseHourlyRate:Number($("vcExpenseRate").value)');
  });

  test('opening a campaign loads the stored policy back into the form', () => {
    // Without this the form would read false and the next save would
    // silently clear an opt-in, because campaigns save with { merge: true }.
    expect(adminHtml).toContain('$("vcMarketingExpense").checked=c.marketingExpense===true');
    expect(adminHtml).toContain('$("vcExpenseVendor").value=c.expenseVendor||""');
    expect(adminHtml).toContain('$("vcExpenseRate").value=Number(c.expenseHourlyRate)||0');
  });

  test('the API returns the policy so the form has something to load', () => {
    expect(adminApi).toContain('marketingExpense: data.marketingExpense === true');
    expect(adminApi).toContain('expenseVendor: data.expenseVendor');
    expect(adminApi).toContain('expenseHourlyRate: Number(data.expenseHourlyRate)');
  });

  test('the section shows only for the type that gives a court away', () => {
    expect(adminHtml).toContain('$("vcMarketingFields").style.display=type==="free_booking"?"block":"none"');
  });

  test('resetting the form clears the policy', () => {
    expect(adminHtml).toContain('$("vcMarketingExpense").checked=false');
  });
});
