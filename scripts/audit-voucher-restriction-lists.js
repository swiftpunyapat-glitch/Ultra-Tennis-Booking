// ════════════════════════════════════════════════════════════════════
// scripts/audit-voucher-restriction-lists.js
//
// Report campaigns and codes whose "allowed X" restriction lists are empty
// arrays.
//
// An empty array is truthy, so the engine used to read [] as "nothing is
// allowed" and reject every redemption with not_applicable /
// day_not_allowed / duration_not_allowed — the opposite of what the Voucher
// tab promises ("none selected = every 1-hour rate"), and what it sends when
// no box is ticked.
//
// The engine now treats [] as no restriction, so nothing here needs
// repairing: this script only says how much was affected while the bug was
// live, i.e. where customers were being told a valid code did not apply.
//
// READ ONLY. It never writes.
//
// Usage:
//   node scripts/audit-voucher-restriction-lists.js
//
// Requires: FIREBASE_SERVICE_ACCOUNT env var (same JSON the API routes use).
// Example:
//   $env:FIREBASE_SERVICE_ACCOUNT = Get-Content path\to\sa.json -Raw
//   node scripts/audit-voucher-restriction-lists.js
// ════════════════════════════════════════════════════════════════════

import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore }                  from 'firebase-admin/firestore';

// Campaign field name → the restriction it feeds in the engine.
const CAMPAIGN_LISTS = {
  allowedPricingTypes: 'pricing type (not_applicable)',
  allowedDays:         'day of week (day_not_allowed)',
  durationMinutes:     'duration (duration_not_allowed)',
};
// Per-code overrides carry the engine's own names.
const VOUCHER_LISTS = {
  allowedPricingTypes: 'pricing type (not_applicable)',
  allowedDays:         'day of week (day_not_allowed)',
  allowedDurations:    'duration (duration_not_allowed)',
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

// An absent field is fine — it always meant "no restriction". Only a stored
// empty array was misread.
const emptyLists = (data, fields) =>
  Object.keys(fields).filter(f => Array.isArray(data[f]) && data[f].length === 0);

async function main() {
  const db = getDb();

  const campaignSnap = await db.collection('voucher_campaigns').get();
  const affected = [];
  for (const doc of campaignSnap.docs) {
    const data = doc.data();
    const empty = emptyLists(data, CAMPAIGN_LISTS);
    if (empty.length) {
      affected.push({ id: doc.id, name: data.name || doc.id, active: data.active === true, empty });
    }
  }

  console.log(`\nCampaigns scanned: ${campaignSnap.size}`);
  console.log(`Campaigns with an empty restriction list: ${affected.length}\n`);

  if (!affected.length) {
    console.log('No campaign was affected. Any failed redemption had another cause.');
  }

  let blockedCodes = 0;
  for (const c of affected) {
    // Codes are what customers actually hold, so count them per campaign.
    const codeSnap = await db.collection('vouchers').where('campaignId', '==', c.id).get();
    const unused = codeSnap.docs.filter(d => {
      const v = d.data();
      return v.active !== false && !['redeemed', 'disabled', 'expired'].includes(String(v.state || 'available'));
    }).length;
    blockedCodes += unused;

    console.log(`  ${c.active ? '[ACTIVE]  ' : '[inactive]'} ${c.id} — ${c.name}`);
    for (const f of c.empty) console.log(`      empty: ${f}  → blocked on ${CAMPAIGN_LISTS[f]}`);
    console.log(`      codes: ${codeSnap.size} total, ${unused} still spendable\n`);
  }

  // A per-code override hits only its own holder, but the symptom is the same.
  const voucherSnap = await db.collection('vouchers').get();
  const codeLevel = voucherSnap.docs
    .map(d => ({ code: d.id, empty: emptyLists(d.data(), VOUCHER_LISTS) }))
    .filter(r => r.empty.length);

  console.log(`Codes scanned: ${voucherSnap.size}`);
  console.log(`Codes with their own empty restriction list: ${codeLevel.length}`);
  for (const r of codeLevel.slice(0, 50)) console.log(`  ${r.code} — empty: ${r.empty.join(', ')}`);
  if (codeLevel.length > 50) console.log(`  ...and ${codeLevel.length - 50} more`);

  if (affected.length || codeLevel.length) {
    console.log(`\nWhile the bug was live these redemptions were refused with a message`);
    console.log(`that reads like a bad code rather than a broken setting.`);
    console.log(`Roughly ${blockedCodes} unused code(s) sit under an affected campaign.`);
    console.log(`Deploying the engine fix restores them — no data change needed.`);
  }
}

main()
  .then(() => process.exit(0))
  .catch(e => { console.error('\nFailed:', e.message); process.exit(1); });
