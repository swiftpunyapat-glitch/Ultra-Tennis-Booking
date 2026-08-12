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
