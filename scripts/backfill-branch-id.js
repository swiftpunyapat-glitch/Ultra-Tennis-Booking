// ════════════════════════════════════════════════════════════════════
// scripts/backfill-branch-id.js
//
// Stamp branchId on legacy docs that don't have one.
// Collections: bookings, booking_slots, available_slots.
//
// This is an OPTIMIZATION, not a prerequisite: all readers resolve a
// missing branchId to DEFAULT_BRANCH_ID at read time. Safe to re-run.
//
// Usage:
//   node scripts/backfill-branch-id.js            # dry-run (safe, no writes)
//   node scripts/backfill-branch-id.js --write    # apply updates
//
// Requires: FIREBASE_SERVICE_ACCOUNT env var (same JSON the API routes use).
// Example:
//   $env:FIREBASE_SERVICE_ACCOUNT = Get-Content path\to\sa.json -Raw
//   node scripts/backfill-branch-id.js
//   node scripts/backfill-branch-id.js --write
// ════════════════════════════════════════════════════════════════════

import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore }                  from 'firebase-admin/firestore';

const WRITE_MODE = process.argv.includes('--write');

// Must match DEFAULT_BRANCH_ID in api/_lib/admin-auth.js, index.html, admin.html.
const DEFAULT_BRANCH_ID = 'ladprao1';

const COLLECTIONS = ['bookings', 'booking_slots', 'available_slots'];
const BATCH_SIZE  = 400;   // Firestore batch limit is 500; stay under it

function getDb() {
  if (!getApps().length) {
    const raw = process.env.FIREBASE_SERVICE_ACCOUNT;
    if (!raw) throw new Error('FIREBASE_SERVICE_ACCOUNT env var not set');
    let sa;
    try { sa = JSON.parse(raw); }
    catch { throw new Error('FIREBASE_SERVICE_ACCOUNT is not valid JSON'); }
    initializeApp({ credential: cert(sa) });
  }
  return getFirestore();
}

async function backfillCollection(db, name) {
  // Full scan: a where('branchId','==',...) filter would EXCLUDE docs
  // missing the field, which is exactly the set we want.
  const snap    = await db.collection(name).get();
  const missing = snap.docs.filter(d => !d.data().branchId);

  console.log(`${name.padEnd(16)} total: ${String(snap.size).padStart(6)}   missing branchId: ${String(missing.length).padStart(6)}`);

  if (!WRITE_MODE || !missing.length) return { scanned: snap.size, updated: 0, pending: missing.length };

  let updated = 0;
  for (let i = 0; i < missing.length; i += BATCH_SIZE) {
    const chunk = missing.slice(i, i + BATCH_SIZE);
    const batch = db.batch();
    chunk.forEach(d => batch.update(d.ref, { branchId: DEFAULT_BRANCH_ID }));
    await batch.commit();
    updated += chunk.length;
    console.log(`  ✓ ${name}: ${updated}/${missing.length} stamped`);
  }
  return { scanned: snap.size, updated, pending: 0 };
}

async function run() {
  console.log('\n══ Backfill branchId ══');
  console.log(`Mode    : ${WRITE_MODE ? '⚠️  WRITE — Firestore will be updated' : '✅ DRY RUN — read-only, no changes'}`);
  console.log(`Stamp   : branchId = "${DEFAULT_BRANCH_ID}"`);
  console.log(`Targets : ${COLLECTIONS.join(', ')}\n`);

  const db = getDb();

  let totalUpdated = 0, totalPending = 0;
  for (const name of COLLECTIONS) {
    const r = await backfillCollection(db, name);
    totalUpdated += r.updated;
    totalPending += r.pending;
  }

  if (WRITE_MODE) {
    console.log(`\n══ Done: ${totalUpdated} doc(s) stamped ══\n`);
  } else {
    console.log(`\nDry run complete — ${totalPending} doc(s) would be stamped.`);
    console.log('Re-run with --write to apply.\n');
  }
}

run().catch(e => {
  console.error('\nFatal error:', e.message);
  process.exit(1);
});
