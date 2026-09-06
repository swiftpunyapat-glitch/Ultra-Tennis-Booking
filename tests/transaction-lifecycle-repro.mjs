// Standalone SDK-only reproduction. No application imports or test mocks.
// FIRESTORE_EMULATOR_HOST=127.0.0.1:18085 node tests/transaction-lifecycle-repro.mjs split 3
import { initializeApp } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';
import { traceTransactions } from './helpers/transaction-trace.mjs';

if (!/^127\.0\.0\.1:\d+$/.test(process.env.FIRESTORE_EMULATOR_HOST || '')) throw new Error('Local emulator required');
const db = getFirestore(initializeApp({ projectId: 'demo-audit-patches' }));
const mode = process.argv[2] || 'split';
const count = Number(process.argv[3] || 3);
const booking = db.doc('transaction_repro/booking');
// Like booking_slot_claims vs bookings, this lock sorts before the booking.
const pkg = db.doc('transaction_repro/aaa_resource');
let failures = 0;
try {
  for (let iteration = 1; iteration <= count; iteration++) {
    await booking.set({ state: 'held' });
    await pkg.set({ remaining: 0 });
    const events = [];
    const transition = () => db.runTransaction(async t => {
      // Payment after the deadline and expiry both perform this release.
      const [b, p] = mode === 'batch' ? await t.getAll(booking, pkg) :
        [await t.get(booking), ...(await t.getAll(pkg))];
      if (b.data().state !== 'held') return;
      t.update(pkg, { remaining: p.data().remaining + 60 });
      t.update(booking, { state: 'expired' });
    });
    const results = await traceTransactions(db, events, () => Promise.allSettled([
      transition(),
      // Start skew only; no barrier or transaction reference escapes a callback.
      (async () => { await new Promise(resolve => setTimeout(resolve, 2)); return transition(); })(),
    ]));
    const errors = results.filter(r => r.status === 'rejected').map(r => ({ code: r.reason.code, message: r.reason.message }));
    failures += errors.length;
    console.log(JSON.stringify({ mode, iteration, errors, remaining: (await pkg.get()).data().remaining, events }));
  }
} finally { await booking.delete(); await pkg.delete(); await db.terminate(); }
process.exitCode = failures ? 1 : 0;
