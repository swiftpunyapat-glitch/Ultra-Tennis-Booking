# Customer Identity V1 — Reversible Migration Contract

## Scope

The first release is a read-only dry-run. It reads `registered_users`, `bookings`, and `customer_packages`, derives a report, and performs zero writes. No customer, booking, package, counter, or report document is modified.

## Sources of truth

- Booking totals, first/last booking dates, confirmed counts, and cancellation counts are derived from booking documents.
- Legacy customer counters are not added together and are not trusted by the new report.
- A package is identified only by its `customer_packages` document ID.
- Package name, remaining minutes, validity, and expiry are never used to deduplicate packages.
- Identity matching loads each source collection once and aggregates in memory. Per-customer booking queries are prohibited.

## Identity strength

1. A real LINE user ID matching `U` plus 32 hexadecimal characters is the strongest identity. Placeholders and invalid/non-LINE identifiers are not identities.
2. A validated normalized Thai mobile phone (`06`, `08`, or `09`, ten digits) is secondary evidence. Landlines, foreign/unknown formats, and invalid values are unlinkable.
3. Names are display evidence only and never trigger an automatic merge.
4. A phone owned by more than one registered LINE account is always manual review.
5. A record whose LINE identity disagrees with the registered owner of its phone is always manual review.
6. A LINE identity found with two distinct valid mobile phones is suspicious/manual review. Three or more is a hard block. Every suspicious LINE is excluded from auto-link proposals while its explicit records remain intact.

## Canonical IDs

- Registered LINE customer: `line:{lineUserId}`
- Manual customer with a usable phone and no unique registered owner: `phone:{normalizedPhone}`
- Unresolved record: `record:{source}:{documentId}`

Future soft-links must point directly to one canonical ID. Link chains such as `A → B → C` are invalid.

## Dry-run proposals

A phone alias may be proposed for `phone:{phone} → line:{lineUserId}` only when exactly one registered LINE account owns that phone and that LINE identity is not suspicious. The proposal includes booking IDs, package IDs, names, rule version, and evidence count for owner review.

The dry-run never approves its own proposals.

## Suspicious LINE safety gate

The report includes `suspiciousLineIdentities` with valid phones, diagnostic names, booking IDs, registered-user document IDs, package IDs, severity, and reason. Names are display-only evidence and never participate in matching.

- Two distinct valid phones: `manual_review`
- Three or more distinct valid phones: `hard_block`

Both severities block phone aliases from resolving or proposing a link into that LINE identity. Explicit booking/package records carrying the LINE ID remain under the LINE-derived profile, and existing conflicts such as `line_phone_disagreement` continue to be reported independently.

The summary includes `suspiciousLineIdentities`, `hardBlockedLineIdentities`, and the number of explicit `affectedBookings`. The report always returns `dryRun: true` and `writesPerformed: 0`.

## Pass review

- A booking reference is checked using `packageId` and `usedPackageId` only.
- Missing package documents and package-owner identity mismatches are manual review.
- If any canonical customer has multiple distinct package documents, every package remains distinct and the set is manually reviewed.
- Similar minutes, names, products, or expiry dates never justify package deduplication.

## Reversible write phase (not implemented in the dry-run)

The later write phase must:

- require explicit Art/Owner approval for each proposal;
- create direct canonical soft-links without deleting legacy documents;
- run in Firestore transactions with a migration Run ID;
- store before snapshots and audit entries;
- reject cycles, link chains, conflicting LINE IDs, shared-phone conflicts, and unreviewed package conflicts;
- support rollback by deactivating approved links and restoring the previous link snapshot.

## `party_size`

Legacy bookings remain `null`/unknown. They must never be backfilled as `1` without evidence. A later phase will require `party_size` on new User and Manual bookings.
