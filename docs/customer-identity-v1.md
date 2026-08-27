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

1. A real LINE user ID is the strongest identity. `guest` and `manual` are placeholders, not identities.
2. A validated normalized Thai phone is secondary evidence.
3. Names are display evidence only and never trigger an automatic merge.
4. A phone owned by more than one registered LINE account is always manual review.
5. A record whose LINE identity disagrees with the registered owner of its phone is always manual review.

## Canonical IDs

- Registered LINE customer: `line:{lineUserId}`
- Manual customer with a usable phone and no unique registered owner: `phone:{normalizedPhone}`
- Unresolved record: `record:{source}:{documentId}`

Future soft-links must point directly to one canonical ID. Link chains such as `A → B → C` are invalid.

## Dry-run proposals

A phone alias may be proposed for `phone:{phone} → line:{lineUserId}` only when exactly one registered LINE account owns that phone. The proposal includes booking IDs, package IDs, names, rule version, and evidence count for owner review.

The dry-run never approves its own proposals.

## Pass review

- A booking reference is checked using `packageId` and `usedPackageId` only.
- Missing package documents and package-owner identity mismatches are manual review.
- If a proposed canonical customer has multiple distinct package documents, every package remains distinct and the pair is manually reviewed.
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
