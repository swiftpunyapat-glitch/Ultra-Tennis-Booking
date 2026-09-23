# Retrospective test bookings

Art can open **Owner View → Bookings → เปลี่ยนเป็น Test**, enter a reason,
and close an existing booking as a test. Other owners, managers, staff and
public booking clients cannot use this action. The server enforces both the
authenticated name `Art` and role `owner` independently of button visibility.

The action commits these changes in one Firestore transaction:

- Keep the booking, its original price/payment evidence and an audit entry;
  set `isTest`, `testSource` and `testConversion` with the previous statuses.
- Close the booking, payment and pending-reschedule workflow, and release
  court/coach claims still owned by that booking. Replacement bookings are safe.
- Restore verified package usage once, including Off-Peak quotas and Event Pass
  consumption. Restore v2 coupon reservations/redemptions without consuming
  the customer's cancellation-restoration allowance.
- Exclude directly linked refund, influencer, coach-payout and manual-income
  records, and void their issued finance documents plus the booking receipt.
  Document numbers and original amounts remain available for audit.
- Revoke any guest capability token. Ordinary editing, payment approval,
  rescheduling and deletion are blocked for the reclassified record.

Normal Admin reads and financial reports exclude test bookings. **ประวัติ Test**
opens a separate Art-only history without adding those rows to normal counts.
Changing to Staff View hides the buttons. No Firestore security-rule changes
or new public endpoint are required.

## Calendar and money

Calendar deletion runs after the local commit with `sendUpdates=none`. Failure
does not reoccupy slots or restore revenue: `testCalendarCleanup=pending`
persists and the history offers a retry button. Retrying never restores a
package or coupon twice. No LINE notification or bank refund is triggered.
Previously sent notifications cannot be recalled.

This changes the application's records only. If a real bank transfer happened,
any transfer back must be handled separately. Unlinked manual ledger entries
cannot be inferred from their text and are not modified.

## Data that needs review

The operation fails without committing if package ownership/balance or an
active coupon cannot be verified. Active legacy coupons without the v2
ownership fields require review rather than a guessed decrement. An expense
pointer naming another booking also blocks the operation. An already released
coupon or replacement slot belonging to another booking is left untouched.
There is deliberately no “restore to live” button: slots or coupon rights may
already have been used by another customer.

## Verification

`tests/booking-mark-test.test.mjs` uses real transactions against the local
Firestore emulator. It covers owner authorization, new booking/payment flows,
immediate slot reuse, report exclusion, receipts and finance records, coupon
restoration, package quotas, coach claims, failed-transaction rollback,
concurrent duplicate clicks, stale coach payouts and calendar retry.

Manual check before production release:

1. Log in as Art, select Owner View and create a test booking normally.
2. Mark it as paid, then use **เปลี่ยนเป็น Test** and enter a reason.
3. Confirm that the normal booking list/count and Finance exclude the record,
   that the slot is available again, and that history retains the reason.
4. Refresh/retry and confirm package/coupon balances have not increased again.
5. Log in as another admin and verify that neither the action nor history is
   accessible. Inspect the real Calendar integration if it is configured.

Run automated checks only against a local emulator, with
`FIRESTORE_EMULATOR_HOST=127.0.0.1:8185` and `GCLOUD_PROJECT=demo-emulator`.
