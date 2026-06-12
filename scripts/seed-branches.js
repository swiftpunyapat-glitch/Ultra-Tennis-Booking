// ════════════════════════════════════════════════════════════════════
// scripts/seed-branches.js
//
// Seed the branches/{branchId} config doc for the default branch.
// Idempotent: skips if the doc already exists (use --force to overwrite).
//
// Usage:
//   node scripts/seed-branches.js            # dry-run (safe, no writes)
//   node scripts/seed-branches.js --write    # create the doc
//   node scripts/seed-branches.js --write --force   # overwrite if it exists
//
// Requires: FIREBASE_SERVICE_ACCOUNT env var (same JSON the API routes use).
// Example:
//   $env:FIREBASE_SERVICE_ACCOUNT = Get-Content path\to\sa.json -Raw
//   node scripts/seed-branches.js
//   node scripts/seed-branches.js --write
// ════════════════════════════════════════════════════════════════════

import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, FieldValue }      from 'firebase-admin/firestore';

const WRITE_MODE = process.argv.includes('--write');
const FORCE_MODE = process.argv.includes('--force');

// Must match DEFAULT_BRANCH_ID in api/_lib/admin-auth.js, index.html, admin.html.
const DEFAULT_BRANCH_ID = 'ladprao1';

const BRANCH_DOC = {
  branchId:         DEFAULT_BRANCH_ID,
  name:             DEFAULT_BRANCH_ID,
  displayName:      'Ultra Tennis Ladprao 1',
  status:           'active',   // active | soft_locked | hard_locked | customer_protection
  allowNewBookings: true,
  allowStaffAccess: true,
  allowCustomerView: true,
  operatingHours:   { open: 6, close: 24 },
  address:          'Floor G, Omni Business Mall',
  contact:          '',
  // One court today; adding court 2 later = append a new entry here
  // with a NEW resourceId (slot doc IDs are keyed by resourceId).
  resources:        [{ resourceId: 'room1', displayName: 'Court 1', active: true }],
  isDefault:        true,
};

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

async function run() {
  console.log('\n══ Seed Branch Config ══');
  console.log(`Mode    : ${WRITE_MODE ? '⚠️  WRITE — Firestore will be updated' : '✅ DRY RUN — read-only, no changes'}`);
  console.log(`Branch  : branches/${DEFAULT_BRANCH_ID}\n`);

  const db  = getDb();
  const ref = db.collection('branches').doc(DEFAULT_BRANCH_ID);
  const cur = await ref.get();

  if (cur.exists && !FORCE_MODE) {
    console.log('Doc already exists — nothing to do (use --force to overwrite):\n');
    console.log(JSON.stringify(cur.data(), null, 2), '\n');
    return;
  }

  console.log(`${cur.exists ? 'Would OVERWRITE existing doc with' : 'Would create'}:\n`);
  console.log(JSON.stringify(BRANCH_DOC, null, 2), '\n');

  if (!WRITE_MODE) {
    console.log('Dry run complete — re-run with --write to apply.\n');
    return;
  }

  await ref.set({
    ...BRANCH_DOC,
    createdAt: cur.exists ? (cur.data().createdAt ?? FieldValue.serverTimestamp()) : FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  });

  console.log(`✓ branches/${DEFAULT_BRANCH_ID} written.\n`);
}

run().catch(e => {
  console.error('\nFatal error:', e.message);
  process.exit(1);
});
