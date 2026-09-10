# Availability incident patch — 2026-09-10

Base: GitHub main `21eef0595ea9faf20d5386898a73b9539a272404`.
Client build: `2026-09-10-availability-incident-1`.

## Findings and limits

The existing clients already distinguish read failures from a closed schedule,
reject Firestore cache snapshots, retry and retain the last result. Therefore,
production's reported “slow load then everything closed” is not established as
a read-error rendering bug by this source review. Production deployment, logs,
and affected dates/bookings still need correlation.

Confirmed gaps: cached results could still be submitted after a failed refresh;
empty schedule queries looked like closed hours; older requests could overwrite
the per-date cache; room close operations were not atomic with booking; and
accounting/refund writes could apply an earlier ownership decision to newer data.

## Changes

- Booking requires a complete result for the selected date, at most 60 seconds
  old, with no refresh in progress or failed refresh. A replaced/blocked selection
  is cleared. Submission stays disabled while a booking request is in progress.
- Availability reads have two bounded attempts, 4 and 5 seconds with a 450 ms
  gap. Empty schedules have their own message and Retry. Last results remain
  visible but are not actionable during a refresh or after failure.
- Each tab retains its last 20 diagnostics in session storage under
  `ut_availability_diagnostics`. Slow/error/empty reads are also sent to the
  existing booking API and logged with `[availability_diagnostic]`.
  Payloads include date, build, duration, counts and error code, not customer
  details or booking tokens. Client and per-server-instance throttles limit logs;
  this is best-effort diagnostics, not durable or globally rate-limited monitoring.
- All room close paths read hourly availability and both :00/:30 occupancies
  inside a transaction. Occupied hours are skipped and Admin reports counts.
- Accounting/refund commit in a transaction that rechecks every document version
  used for the decision. Concurrent changes return `409 ADMIN_DATA_CHANGED`.
  Read errors abort the operation. An old booking cannot be confirmed without
  its slot; completed bookings retain historical accounting corrections.
- Pending room holds without expiry remain blocking for Admin, matching the
  booking server's conservative handling of this malformed state.

## Validation

Selected suites: availability-incident, availability-incident-integration,
admin-cutover, client-cutover, pass-booking-concurrency,
coach-addon-v2-integration and slot-contract. Integration tests use local
Firestore emulator only. Browser loader/render tests use an isolated VM harness.
Inline scripts were syntax-checked; diagnostic sanitization and throttling were
exercised directly.

The pre-existing cross-type Event Pass test also failed against the original
GitHub handler: its pass expired before its fixed booking date. Its fixture now
expires 90 days after the test booking date; production expiry policy is unchanged.

No production data, rules, indexes or deployment were modified. Server-side
availability migration remains deferred. After deployment approval, verify the
build marker and the affected date in both LINE WebView and a regular browser,
and correlate diagnostics with the actual schedule and booking ownership.
