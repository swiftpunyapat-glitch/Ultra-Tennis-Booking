// Shared Firebase Admin SDK initializer for Ultra Tennis API routes.
// getAdminDb() returns a Firestore instance. Safe to call on every lambda
// invocation — getApps() prevents re-initialization on warm reuse.
// Also hosts writeAuditLog — kept here (not a new _lib file) because every
// .js file under api/ counts toward the Vercel function limit.

import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getFirestore, FieldValue } from 'firebase-admin/firestore';

export function getAdminDb() {
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

// Audit log writer. Fire-and-forget safe: NEVER throws — a failed audit
// write must not break the operation it documents.
export async function writeAuditLog(db, { actor, actorRole, branchId, action, targetId, before, after, source, note }) {
  try {
    await db.collection('audit_logs').add({
      actor:     actor     ?? 'unknown',
      actorRole: actorRole ?? 'unknown',
      branchId:  branchId  ?? null,
      action:    action    ?? 'unknown',
      targetId:  targetId  ?? null,
      before:    before    ?? null,   // keep small: only the fields that changed
      after:     after     ?? null,
      source:    source    ?? 'api',
      note:      note      ?? null,
      createdAt: FieldValue.serverTimestamp(),
    });
  } catch (e) {
    console.error('[audit] writeAuditLog failed:', e.message);
  }
}

// Convert a Firestore Admin SDK document data object to a JSON-safe plain object.
// Firestore Timestamps (have .toDate()) are converted to ISO-8601 strings.
export function serializeFsDoc(data) {
  const out = {};
  for (const [k, v] of Object.entries(data)) {
    if (v === null || v === undefined) {
      out[k] = v;
    } else if (typeof v.toDate === 'function') {
      out[k] = v.toDate().toISOString();
    } else {
      out[k] = v;
    }
  }
  return out;
}
