// ════════════════════════════════════════════════════════════════════
// scripts/first-day-cleanup.js
//
// One-time patch: mark first-day unpaid manual bookings as paid.
//
// Usage:
//   node scripts/first-day-cleanup.js            # dry-run (safe, no writes)
//   node scripts/first-day-cleanup.js --write    # write to Firestore
//
// Requires: FIREBASE_SERVICE_ACCOUNT env var (same JSON the API routes use).
// Example:
//   $env:FIREBASE_SERVICE_ACCOUNT = Get-Content path\to\sa.json -Raw
//   node scripts/first-day-cleanup.js
//   node scripts/first-day-cleanup.js --write
//
// Filter applied (in memory after querying by date):
//   • paymentStatus === "unpaid"
//   • price === 350
//   • bookingType is NOT "Ultra Pass Manual" or "Off-Peak Manual"
//   • date in TARGET_DATES
//
// Write mode updates each matching booking + its booking_slots doc:
//   paymentStatus → "paid", bookingStatus → "confirmed",
//   paidBy: "system:first-day-manual-cleanup",
//   paymentMethod: "manual_confirmed",
//   paymentNote, adminReviewedAt, confirmedAt, updatedAt.
// ════════════════════════════════════════════════════════════════════

import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore, FieldValue }      from 'firebase-admin/firestore';

const WRITE_MODE    = process.argv.includes('--write');
const TARGET_DATES  = ['2026-05-22', '2026-05-23'];
const PACKAGE_TYPES = new Set(['Ultra Pass Manual', 'Off-Peak Manual']);
const RESOURCE_ID   = 'room1';

const PAID_BY    = 'system:first-day-manual-cleanup';
const PAY_METHOD = 'manual_confirmed';
const PAY_NOTE   = 'First-day manual booking was paid before finance/payment status control existed';

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
  console.log('\n══ First-Day Manual Booking Cleanup ══');
  console.log(`Mode    : ${WRITE_MODE ? '⚠️  WRITE — Firestore will be updated' : '✅ DRY RUN — read-only, no changes'}`);
  console.log(`Dates   : ${TARGET_DATES.join(', ')}`);
  console.log(`Filter  : paymentStatus=unpaid, price=350, not a package type\n`);

  const db = getDb();

  // Query each target date separately (Firestore can't combine date IN + paymentStatus cheaply).
  const snaps = await Promise.all(
    TARGET_DATES.map(date =>
      db.collection('bookings')
        .where('date', '==', date)
        .where('paymentStatus', '==', 'unpaid')
        .get()
    )
  );

  const candidates = [];
  snaps.forEach(snap => {
    snap.docs.forEach(d => {
      const b = { id: d.id, ...d.data() };
      if (Number(b.price) !== 350)           return;  // not a manual single-use price
      if (PACKAGE_TYPES.has(b.bookingType))  return;  // package booking — skip
      candidates.push(b);
    });
  });

  if (!candidates.length) {
    console.log('No matching bookings found. Nothing to do.\n');
    return;
  }

  console.log(`Found ${candidates.length} matching booking(s):\n`);
  candidates.forEach(b => {
    console.log(
      `  • ${(b.bookingCode || b.id).padEnd(24)}` +
      `  ${(b.customerName || '—').padEnd(20)}` +
      `  ${b.date}  ${(b.startTime || '—')}–${(b.endTime || '—')}` +
      `  ${b.bookingType || '—'}  ฿${b.price}`
    );
  });

  if (!WRITE_MODE) {
    console.log(`\nDry run complete — ${candidates.length} booking(s) would be updated.`);
    console.log('Re-run with --write to apply.\n');
    return;
  }

  console.log('\n⚠️  Applying updates…\n');

  let success = 0, fail = 0;

  for (const b of candidates) {
    const bookingRef = db.collection('bookings').doc(b.id);
    const slotId     = `${RESOURCE_ID}_${b.date}_${(b.startTime || '').replace(':', '')}`;
    const slotRef    = db.collection('booking_slots').doc(slotId);

    try {
      const batch = db.batch();

      batch.update(bookingRef, {
        paymentStatus:   'paid',
        bookingStatus:   'confirmed',
        status:          'confirmed',
        paidAt:          FieldValue.serverTimestamp(),
        paidBy:          PAID_BY,
        paymentMethod:   PAY_METHOD,
        paymentNote:     PAY_NOTE,
        adminReviewedAt: FieldValue.serverTimestamp(),
        confirmedAt:     FieldValue.serverTimestamp(),
        updatedAt:       FieldValue.serverTimestamp(),
      });

      batch.update(slotRef, {
        paymentStatus: 'paid',
        bookingStatus: 'confirmed',
      });

      await batch.commit();
      console.log(`  ✓ ${b.bookingCode || b.id} — updated`);
      success++;
    } catch (e) {
      console.error(`  ✗ ${b.bookingCode || b.id} — FAILED: ${e.message}`);
      fail++;
    }
  }

  console.log(`\n══ Done: ${success} updated, ${fail} failed ══\n`);
}

run().catch(e => {
  console.error('\nFatal error:', e.message);
  process.exit(1);
});
