# Firestore TTL Policies — Deployment Prerequisite

Two collections written by the security hotfix grow without bound unless
Firestore reaps them. Both carry an `expiresAt` timestamp for that purpose.
**Creating these policies is a prerequisite for Deployment A**, not a
follow-up: without them the collections accumulate one document per unique
(bucket, caller) and per idempotency key, forever.

Firestore TTL deletes a document some time after the instant in its TTL
field — usually within 24 hours, not immediately. Every value below is
therefore chosen so the document is already irrelevant well before deletion
becomes possible.

## Required policies

| Collection | TTL field | Written by | Value |
|---|---|---|---|
| `rate_limits` | `expiresAt` | `checkRateLimit()` | `max(windowEnd, blockedUntil) + 24h` |
| `idempotency_records` | `expiresAt` | `writeIdempotencyInTx()` | `createdAt + 90 days` |

### Why these values

**`rate_limits`** — a document only influences a decision until its window
closes, or until its block expires if it is in lockout. The extra 24 hours
means TTL can never remove a document that is still capable of denying a
request. `blockedUntil` is included in the maximum so a 60-minute lockout is
never cut short by an earlier window end.

**`idempotency_records`** — a record must outlive any retry a client could
plausibly make. 90 days also covers the dispute window for a payment-bearing
mutation, so a replay can still be demonstrated during an investigation.

## Creating them

Console → Firestore → TTL → Create policy, once per collection. Or:

```bash
gcloud firestore fields ttls update expiresAt \
  --collection-group=rate_limits \
  --enable-ttl \
  --project=ultra-tennis-booking
```

```bash
gcloud firestore fields ttls update expiresAt \
  --collection-group=idempotency_records \
  --enable-ttl \
  --project=ultra-tennis-booking
```

## Verifying

```bash
gcloud firestore fields ttls list --project=ultra-tennis-booking
```

Both collection groups must appear with state `ACTIVE`. A policy that is
still `CREATING` has not started reaping yet.

## Notes

- `guest_booking_access` deliberately has **no** TTL. One document per
  booking is bounded by booking volume, and the revoked document is the
  record that access was withdrawn — deleting it would lose that.
- Enabling TTL on a collection does not touch documents lacking the field;
  documents written before this hotfix simply never expire.
- TTL deletion counts as a normal delete for billing.
