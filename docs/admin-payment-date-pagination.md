# Admin payment dates and complete booking pagination

The Paid Summary now uses **paidAt in Asia/Bangkok**, matching Finance's payment-date basis. Review, confirmation, creation and play dates do not substitute for missing payment evidence. Paid rows without a valid paidAt are excluded and reported explicitly. Paid cancellations remain visible with their booking/refund status; refunds are not represented as new receipts. Existing stored timestamps and amounts are unchanged.

Admin booking reads now consume every server cursor, with 500 as a page size rather than a total cap. The new opt-in `bookingOrder: "id"` mode traverses immutable document IDs so legacy records missing createdAt are included. Existing API consumers retain their createdAt cursor contract. Admin sorts the completed data by creation time for display, while monthly/daily/status/court filters and global pending queues keep working on the full set. Authorization, branch filtering, test exclusion and field projection remain server-side. Empty branch-filtered pages still advance using the raw server cursor. Results are deduplicated by canonical document ID, never by customer or booking code. Malformed/cyclic cursors or a failed page surface an error instead of publishing a partial history.

This is a paginated full-history read, not a transactionally frozen analytics export. Concurrent inserts may require Refresh. Reading the full history uses more requests than the old truncated load; month filtering itself remains local and does not issue another history read. Packages/registered-user pagination is outside this PR.

## Validation

Run against a loopback Firestore emulator and a demo project only:

```powershell
$env:FIRESTORE_EMULATOR_HOST='127.0.0.1:8087'
$env:GCLOUD_PROJECT='demo-ultra-recovery'
node node_modules/vitest/vitest.mjs run tests/admin-booking-data.test.mjs tests/admin-booking-pagination-api.test.mjs tests/admin-cutover.test.mjs tests/finance-company.test.mjs tests/admin-ui-hubs.test.mjs tests/admin-customer-profile.test.mjs tests/ai-report.test.mjs tests/ai-report-admin.test.mjs
```

Coverage includes real Admin projection versus Finance, Bangkok midnight/month/year boundaries, missing/conflicting payment timestamps, paid cancellation/refund display, >500 history records, equal createdAt timestamps, missing createdAt, empty filtered pages, duplicate IDs, split booking codes, spoofed stored IDs, failed/cyclic cursors, existing authorization/mutation boundaries and the actual Paid Summary renderer. No production data or credentials are used.

## Deployment boundary

Vercel production was verified in its dashboard as deployment `GaSpgsmsUAENMwtWzs33CRHrrMpR`, source `1797b185aaadbd5c09903967915a229af9ef56b9`, Node.js 24.x. The exact running Node patch version was not exposed and is not inferred from package.json or local tests.

`vercel.json` disables Git-triggered deployment **only for this development branch** (`codex/booking-payment-date-pagination-20261022`), following Vercel's documented `git.deploymentEnabled` branch map. Production/main configuration is not disabled. No merge, deployment, price/credit/account change or customer contact is included. Dependency/runtime/security changes need separate review and explicit deployment approval.
