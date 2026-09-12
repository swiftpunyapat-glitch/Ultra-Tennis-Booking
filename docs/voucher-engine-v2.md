# Voucher Engine v2

Voucher v2 separates the commercial rules from individual codes:

- `voucher_campaigns/{campaignId}` contains the campaign, eligibility, value, and restore policy.
- `vouchers/{CODE}` is the exact bearer/assigned entitlement and its lifecycle state.
- A `keyword` or `codePrefix` is routing/reporting metadata only. It never grants value without an exact voucher document.
- Voucher types are `free_booking`, `discount_amount`, and `discount_percent`.
- Legacy voucher documents without `schemaVersion: 2` continue to use the original standard-rate amount-discount behavior.

## MONSTR campaign example

```json
{
  "schemaVersion": 2,
  "campaignId": "monstr-2026",
  "name": "MONSTR Sponsor 2026",
  "keyword": "MONSTR",
  "codePrefix": "MSTR-",
  "active": true,
  "voucherType": "free_booking",
  "allowedDays": [1, 2, 3, 4, 5],
  "startTime": "06:00",
  "endTime": "24:00",
  "excludeHolidays": true,
  "exactDurationMinutes": 60,
  "requiresLineLogin": true,
  "transferable": true,
  "maxUsesPerCode": 1,
  "maxCancellationRestores": 2,
  "branchId": "ladprao1",
  "resourceId": "room1"
}
```

Each spreadsheet row becomes an exact code document:

```json
{
  "schemaVersion": 2,
  "campaignId": "monstr-2026",
  "active": true,
  "state": "available",
  "usedCount": 0,
  "maxUses": 1,
  "source": "MONSTR_Voucher_Codes.xlsx"
}
```

The workbook's start/end dates must be corrected before import because its current Config date formulas evaluate to `#VALUE!`. Campaign expiry should never be guessed by an importer.

## Lifecycle

| Voucher | Booking created | Payment confirmed | Unpaid cancellation | Confirmed cancellation |
|---|---|---|---|---|
| Discount v2 | `reserved` | `redeemed` | back to `available` | follows the normal paid refund policy |
| Free booking | immediately `redeemed` and booking confirmed | not applicable | not applicable | back to `available` until `maxCancellationRestores` is reached |
| Legacy discount | `usedCount + 1` (unchanged behavior) | unchanged | unchanged | unchanged |

Expired discount reservations are considered reclaimable by the engine, so an abandoned payment hold does not permanently burn a v2 code.

## Deep links

Use `https://liff.line.me/2010034901-ClPr9N5v?voucher=MSTR-ABCDE`. The customer page preserves the code across LIFF login and prefills the voucher field. The API still validates the exact code and all campaign rules during both quote and booking creation.

## Admin Voucher Manager

The Admin `Voucher` tab is visible only to the authenticated `Art` owner session. Every read and mutation repeats the same authorization check in `admin-user-action`; the browser never writes voucher collections directly.

The first release supports:

- creating and editing campaign rules;
- free booking, fixed amount, and percentage voucher types;
- dates, days, time windows, holiday exclusion, pricing-rate scope, and cancellation restore limits;
- custom codes and bulk random generation of up to 100 codes;
- code search, status inspection, link copying, and safe enable/disable controls;
- campaign and code audit-log entries.

Voucher duration remains fixed at 60 minutes because the current customer booking route intentionally rejects vouchers on other durations. Reserved codes cannot be disabled, and redeemed codes cannot be enabled again.

## Approval-based Event Pass

Campaigns with `voucherType: event_pass` use a separate entitlement flow and never act as a price discount:

1. A signed-in LINE customer submits an exact code. The code moves from `available` to `pending_approval` and an `event_pass_requests` record is created.
2. The Art owner compares the customer identity with the organizer-assigned name and approves or rejects the request.
3. Approval issues one `monstr_event_pass` package with 60 minutes and the campaign expiry. Rejection returns the code to `available` by default.
4. The customer selects an eligible slot and the normal server pass-booking transaction confirms it immediately.

Event Pass bookings are exactly 60 minutes, Monday-Friday, exclude holidays, and the service date must not exceed the pass expiry. They cannot be rescheduled. Cancellation releases the court but forfeits the pass. The owner-only test reset may return a code after its booking is terminal; it refuses to reset a code with an active booking.

## Empty restriction lists mean no restriction

`allowedPricingTypes`, `allowedDays` and `allowedDurations` each restrict a
redemption only when they have entries. An empty array means no restriction.

This is easy to get wrong because `[]` is truthy in JavaScript while
`[].includes(anything)` is always false, so a bare `if (list && !list.includes(x))`
guard rejects every redemption instead of allowing every one. All three
guards test `list?.length` for that reason.

It matters most for `allowedPricingTypes`, which the Voucher tab sends as
`[]` whenever no pricing box is ticked — the form's default, labelled "none
selected = every 1-hour rate". A campaign stored without the field is
projected to the form as `[]`, so opening a working campaign and pressing
Save was enough to write `[]` onto it. The customer then saw a message that
reads like a bad code rather than a broken setting.

`scripts/audit-voucher-restriction-lists.js` reports which campaigns and
codes hold an empty list. It is read-only; no stored document needs
repairing, because `[]` now means what it was always meant to mean.

## Marketing expense for free slots

Free court time given to a creator is barter, not a sale. No cash arrives,
but a sellable hour is spent, and recording it only as `price: 0` makes it
vanish from the books: it is not revenue (that filter wants
`paymentStatus: "paid"`), and it is not package usage (that filter wants an
Ultra Pass `packageType`), so nothing in the P&L ever sees it.

A campaign opts in and the booking route books the slot's notional value to
`finance_expenses` as a `Marketing` expense, inside the same transaction that
confirms the booking. Three optional campaign fields control it:

| Field | Default | Meaning |
|---|---|---|
| `marketingExpense` | `false` | Opt in. Only ever acts on `free_booking` redemptions. |
| `expenseVendor` | campaign name | Groups the spend in Finance. Set it to the creator's name to see spend per creator. |
| `expenseHourlyRate` | `0` | `0` values the slot at what it would have sold for, so an off-peak giveaway is not costed at a peak rate. |

`api/_lib/creator-expense.js` owns the document shape. Both writers use it —
the free-voucher booking route and the Art-only `influencer_free` accounting
edit — so a giveaway looks identical in Finance however it was recorded. The
booking carries `isInfluencerBooking`, `influencerExpenseAmount` and
`influencerExpenseId`, which is what makes the existing accounting editor
operate on voucher bookings unchanged and makes a later manual edit update
that expense instead of stacking a second one.

Cancelling a confirmed free-voucher booking soft-deletes the expense and
clears the booking's pointer: the court comes back, so the cost does too. A
missing expense row never blocks the cancellation.

The Voucher tab carries the controls under **Marketing Cost**, shown only for
`free_booking` campaigns because they are the only ones that give a court
away. The form loads the stored policy and sends it back on save, and the
campaign list marks a campaign that spends money.

Campaigns save with `{ merge: true }`, so the admin normalizer emits these
three only when a caller actually supplies them, and `projectVoucherCampaign`
returns them so the form has something to load. Both halves are required: if
the read side dropped them the form would show `false` and the next save of
an unrelated field would silently switch the cost back off.
