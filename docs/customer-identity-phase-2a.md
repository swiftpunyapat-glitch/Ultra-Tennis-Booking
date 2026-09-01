# Customer Identity v1 — Phase 2A

Phase 2A is a read-only planning tool. It creates a reversible migration plan from one immutable source snapshot and performs an in-memory simulation. It cannot apply the plan.

## Safety contract

- Reads `bookings`, `registered_users`, and `customer_packages` once each.
- Plans only new canonical documents, new alias documents, and additive booking soft-link fields.
- Never changes historical name, phone, LINE, customer ID, booking counters, or package data.
- Never merges, deduplicates, recalculates, or changes a pass/package balance.
- Rejects alias conflicts, duplicate targets, unexpected approved evidence, hash changes, and stale snapshots.
- Applies the simulated plan twice to prove idempotency, then rolls it back to prove exact reversibility.
- `--apply` is hard-disabled.

## Local plan command

Use a previously exported snapshot:

```powershell
node scripts/customer-identity-migrate.mjs --plan --snapshot=C:\secure\customer-identity-snapshot.json
```

Or set `FIREBASE_SERVICE_ACCOUNT` only in the local terminal and read Production directly:

```powershell
node scripts/customer-identity-migrate.mjs --plan
```

Never paste the credential into chat and never save it in the repository. Generated plan and private canonical assignment files are stored under `migration-plans/`, which is ignored by Git.

## Future apply guard (not implemented)

Any later apply tool must re-read the source collections, compare the source digest, validate the plan hash, check that target documents are absent or exactly idempotent, and abort on any alias conflict or canonical redirect cycle. Phase 2A provides no write path.
