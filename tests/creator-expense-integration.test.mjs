// ════════════════════════════════════════════════════════════════════
// Creator free slots book a Marketing expense — against the real handlers
// ════════════════════════════════════════════════════════════════════
// The unit tests cover the document builders. These drive the actual
// booking and accounting endpoints, because the invariant that matters is
// transactional: a giveaway must never reach the books without its cost,
// and a cancelled giveaway must not leave the cost behind.
// ════════════════════════════════════════════════════════════════════

import { beforeAll, beforeEach, describe, expect, test } from 'vitest';

const DATE   = '2027-11-10';
const START  = '10:00';
const SLOT   = `room1_${DATE}_1000`;
const CODE   = 'CRTR-TEST1';
const CAMPAIGN = 'creator-test';

let db, booking, accounting, adminAction, cookie;

beforeAll(async () => {
  if (!/^127\.0\.0\.1:\d+$/.test(process.env.FIRESTORE_EMULATOR_HOST || '')) {
    throw new Error('Refusing to run without the local Firestore emulator');
  }
  process.env.ADMIN_SESSION_SECRET = 'creator-expense-test-secret';
  process.env.ADMIN_USERS_JSON = JSON.stringify({ Art: { pin: '0000', role: 'owner', branches: '*' } });
  db = (await import('../api/_lib/firebase-admin.js')).getAdminDb();
  booking = (await import('../api/booking.js')).default;
  accounting = (await import('../api/admin-edit-booking-accounting.js')).default;
  adminAction = (await import('../api/admin-user-action.js')).default;
  cookie = (await import('../api/_lib/admin-auth.js')).createSessionCookie('Art').split(';')[0];
});

async function call(handler, body, headers = {}) {
  const res = {
    statusCode: 0, body: null,
    status(c) { this.statusCode = c; return this; },
    json(b) { this.body = b; return this; },
    setHeader() {},
  };
  await handler({ method: 'POST', body, headers, socket: {} }, res);
  return res;
}

const createBooking = (overrides = {}) => call(booking, {
  action: 'create',
  date: DATE, startTime: START, durationMinutes: 60,
  customerName: 'Creator Ploy', customerPhone: '0812345678',
  voucherCode: CODE,
  ...overrides,
});

async function wipe(collection) {
  const snap = await db.collection(collection).get();
  await Promise.all(snap.docs.map(d => d.ref.delete()));
}

// `marketingExpense` is the opt-in under test; everything else is the
// minimum a free-booking campaign needs to evaluate.
async function seedCampaign(extra = {}) {
  await db.collection('voucher_campaigns').doc(CAMPAIGN).set({
    schemaVersion: 2, campaignId: CAMPAIGN, name: 'Creator Program 2027',
    active: true, voucherType: 'free_booking',
    exactDurationMinutes: 60, maxUsesPerCode: 1, maxCancellationRestores: 2,
    ...extra,
  });
  await db.collection('vouchers').doc(CODE).set({
    schemaVersion: 2, campaignId: CAMPAIGN, active: true,
    state: 'available', usedCount: 0, maxUses: 1,
  });
}

beforeEach(async () => {
  for (const c of ['bookings', 'booking_slots', 'booking_slot_claims',
                   'finance_expenses', 'vouchers', 'voucher_campaigns',
                   'guest_access', 'holidays']) {
    await wipe(c);
  }
  await db.collection('available_slots').doc(SLOT).set({
    resourceId: 'room1', branchId: 'ladprao1', date: DATE,
    startTime: START, endTime: '11:00', status: 'open',
  });
});

const expenses = async () => (await db.collection('finance_expenses').get()).docs.map(d => ({ id: d.id, ...d.data() }));
const bookings = async () => (await db.collection('bookings').get()).docs.map(d => ({ id: d.id, ...d.data() }));

describe('an opted-in campaign books the giveaway as a cost', () => {
  beforeEach(() => seedCampaign({ marketingExpense: true }));

  test('the booking confirms free and one Marketing expense lands with it', async () => {
    const res = await createBooking();
    expect(res.statusCode).toBe(200);
    expect(res.body.ok).toBe(true);
    expect(res.body.booking.finalPrice).toBe(0);
    expect(res.body.booking.paymentStatus).toBe('package');

    const rows = await expenses();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      category: 'Marketing',
      deleted: false,
      autoCreated: true,
      sourceType: 'influencer_free_slot',
      sourceCampaignId: CAMPAIGN,
      sourceVoucherCode: CODE,
      addedByAdmin: 'system:voucher',
    });
  });

  test('the cost equals what the slot would have sold for', async () => {
    await createBooking();
    const [bk] = await bookings();
    const [exp] = await expenses();
    expect(bk.originalPrice).toBeGreaterThan(0);
    expect(exp.amount).toBe(bk.originalPrice);
    // The booking itself still earns nothing.
    expect(bk.price).toBe(0);
  });

  test('the expense points at its booking and the booking back at the expense', async () => {
    await createBooking();
    const [bk] = await bookings();
    const [exp] = await expenses();
    expect(exp.sourceBookingId).toBe(bk.id);
    expect(bk.influencerExpenseId).toBe(exp.id);
    expect(bk.influencerExpenseAmount).toBe(exp.amount);
    expect(bk.isInfluencerBooking).toBe(true);
  });

  test('the vendor groups the spend under the campaign', async () => {
    await createBooking();
    const [exp] = await expenses();
    expect(exp.vendor).toBe('Creator Program 2027');
  });

  test('a campaign rate overrides the slot price', async () => {
    await seedCampaign({ marketingExpense: true, expenseHourlyRate: 250 });
    await createBooking();
    const [exp] = await expenses();
    expect(exp.amount).toBe(250);
  });

  test('an explicit vendor names the creator instead of the campaign', async () => {
    await seedCampaign({ marketingExpense: true, expenseVendor: 'Creator - Ploy' });
    await createBooking();
    const [exp] = await expenses();
    expect(exp.vendor).toBe('Creator - Ploy');
  });
});

describe('the opt-in is required', () => {
  test('a campaign that has not opted in writes no expense at all', async () => {
    await seedCampaign();                      // marketingExpense defaults false
    const res = await createBooking();
    expect(res.statusCode).toBe(200);
    expect(await expenses()).toHaveLength(0);
    const [bk] = await bookings();
    expect(bk.isInfluencerBooking).toBeUndefined();
    expect(bk.influencerExpenseId).toBeUndefined();
  });

  test('a discount campaign books no giveaway cost — the court is still sold', async () => {
    await seedCampaign({
      marketingExpense: true, voucherType: 'discount_amount', discountAmount: 100,
    });
    const res = await createBooking();
    expect(res.statusCode).toBe(200);
    expect(res.body.booking.finalPrice).toBeGreaterThan(0);
    expect(await expenses()).toHaveLength(0);
  });
});

describe('the cost commits with the booking, never on its own', () => {
  test('a lost slot race leaves no orphan expense', async () => {
    await seedCampaign({ marketingExpense: true });
    // Someone else already holds the hour.
    await db.collection('booking_slots').doc(SLOT).set({
      resourceId: 'room1', date: DATE, hour: START,
      bookingStatus: 'confirmed', paymentStatus: 'paid',
    });

    const res = await createBooking();
    expect(res.statusCode).toBe(409);
    expect(await expenses()).toHaveLength(0);
    expect(await bookings()).toHaveLength(0);
    // The code is still spendable.
    expect((await db.collection('vouchers').doc(CODE).get()).data().state).toBe('available');
  });
});

describe('cancelling gives the court back and the cost with it', () => {
  test('the expense is soft-deleted and the booking releases its pointer', async () => {
    await seedCampaign({ marketingExpense: true });
    await createBooking();
    const [bk] = await bookings();
    const [expBefore] = await expenses();
    expect(expBefore.deleted).toBe(false);

    const res = await call(accounting, {
      operation: 'reject_payment', bookingId: bk.id, reason: 'creator did not show',
    }, { cookie });
    expect(res.statusCode).toBe(200);

    const expAfter = (await db.collection('finance_expenses').doc(expBefore.id).get()).data();
    expect(expAfter.deleted).toBe(true);
    expect(expAfter.deletedBy).toBe('Art');
    // The row survives for audit rather than disappearing.
    expect(expAfter.amount).toBe(expBefore.amount);

    const bkAfter = (await db.collection('bookings').doc(bk.id).get()).data();
    expect(bkAfter.bookingStatus).toBe('cancelled');
    expect(bkAfter.influencerExpenseId).toBeNull();
  });

  test('a missing expense row does not block giving the court back', async () => {
    await seedCampaign({ marketingExpense: true });
    await createBooking();
    const [bk] = await bookings();
    // Someone cleared the expense by hand in Finance.
    await db.collection('finance_expenses').doc(bk.influencerExpenseId).delete();

    const res = await call(accounting, {
      operation: 'reject_payment', bookingId: bk.id, reason: 'creator did not show',
    }, { cookie });
    expect(res.statusCode).toBe(200);
    const bkAfter = (await db.collection('bookings').doc(bk.id).get()).data();
    expect(bkAfter.bookingStatus).toBe('cancelled');
  });
});

// The Voucher tab saves campaigns with { merge: true }. If the policy did not
// survive save → list → save, an owner editing anything else on the campaign
// would silently switch the giveaway cost back off.
describe('the policy survives a Voucher tab round trip', () => {
  const saveCampaign = (campaign) => call(adminAction, {
    action: 'voucher_save_campaign', campaign,
  }, { cookie });

  const listCampaign = async () => {
    const res = await call(adminAction, { action: 'voucher_list' }, { cookie });
    expect(res.statusCode).toBe(200);
    return res.body.campaigns.find(c => c.id === CAMPAIGN);
  };

  const formPayload = (extra = {}) => ({
    campaignId: CAMPAIGN, name: 'Creator Program 2027',
    keyword: 'CREATOR', codePrefix: 'CRTR-', active: true,
    voucherType: 'free_booking',
    // validFrom/expiresAt are checked against wall-clock now, not the booked
    // date, so the window has to be open today for a redemption to evaluate.
    validFrom: '2020-01-01T00:00:00+07:00', expiresAt: '2099-12-31T23:59:59+07:00',
    allowedDays: [1, 2, 3, 4, 5], startTime: '06:00', endTime: '24:00',
    exactDurationMinutes: 60, excludeHolidays: true,
    // The guest booking below is about the expense policy, not the login rule.
    requiresLineLogin: false,
    transferable: false, maxCancellationRestores: 2, allowedPricingTypes: [],
    discountAmount: 50, discountPercent: 10, maxDiscountAmount: 0, minFinalPrice: 0,
    ...extra,
  });

  test('an opt-in saved from the form reads back as an opt-in', async () => {
    expect((await saveCampaign(formPayload({
      marketingExpense: true, expenseVendor: 'Creator - Ploy', expenseHourlyRate: 250,
    }))).statusCode).toBe(200);

    expect(await listCampaign()).toMatchObject({
      marketingExpense: true, expenseVendor: 'Creator - Ploy', expenseHourlyRate: 250,
    });
  });

  test('editing the name again does not switch the cost back off', async () => {
    await saveCampaign(formPayload({ marketingExpense: true, expenseHourlyRate: 250 }));
    const loaded = await listCampaign();

    // What the form does on save: it sends back whatever it loaded.
    await saveCampaign(formPayload({
      name: 'Creator Program 2027 (renamed)',
      marketingExpense: loaded.marketingExpense,
      expenseVendor: loaded.expenseVendor,
      expenseHourlyRate: loaded.expenseHourlyRate,
    }));

    expect(await listCampaign()).toMatchObject({
      name: 'Creator Program 2027 (renamed)',
      marketingExpense: true, expenseHourlyRate: 250,
    });
  });

  test('an opt-out saved from the form actually turns the cost off', async () => {
    await saveCampaign(formPayload({ marketingExpense: true }));
    await saveCampaign(formPayload({ marketingExpense: false }));
    expect((await listCampaign()).marketingExpense).toBe(false);
  });

  test('a saved opt-in reaches a real booking', async () => {
    await saveCampaign(formPayload({ marketingExpense: true }));
    await db.collection('vouchers').doc(CODE).set({
      schemaVersion: 2, campaignId: CAMPAIGN, active: true,
      state: 'available', usedCount: 0, maxUses: 1,
    });
    const res = await createBooking();
    expect(res.statusCode).toBe(200);
    const rows = await expenses();
    expect(rows).toHaveLength(1);
    expect(rows[0].category).toBe('Marketing');
  });
});
