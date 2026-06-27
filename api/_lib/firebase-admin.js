// Shared Firebase Admin SDK initializer for Ultra Tennis API routes.
// getAdminDb() returns a Firestore instance. Safe to call on every lambda
// invocation — getApps() prevents re-initialization on warm reuse.
// Also hosts writeAuditLog — kept here (not a new _lib file) because every
// .js file under api/ counts toward the Vercel function limit.

import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getFirestore, FieldValue } from 'firebase-admin/firestore';
import { getAuth } from 'firebase-admin/auth';

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

// Firebase Auth admin instance. getAdminDb() ensures the app is initialized.
// Because init uses a service-account cert, createCustomToken() signs locally
// with the SA private key — no "Service Account Token Creator" IAM role needed.
export function getAdminAuth() {
  getAdminDb();
  return getAuth();
}

// ── Branch helpers ───────────────────────────────────────────────────
// Additive and fail-open by design: a missing branch doc or a failed
// read must NEVER block an operation (backward compatibility with the
// pre-branch system). Kept here, not in a new _lib file — every .js
// under api/ counts toward the Vercel function limit on this project.

export const BRANCH_STATUSES = ['active', 'soft_locked', 'hard_locked', 'customer_protection'];

// Read a branch config doc. Never throws; missing doc → null (= implicitly active).
export async function getBranch(db, branchId) {
  try {
    const snap = await db.collection('branches').doc(branchId).get();
    return snap.exists ? snap.data() : null;
  } catch (e) {
    console.error('[branch] getBranch failed:', e.message);
    return null;
  }
}

// Capability flags derived from a branch status.
export function statusFlags(status) {
  switch (status) {
    case 'soft_locked':
      return { allowNewBookings: false, allowStaffAccess: true,  allowCustomerView: true, showProtectionBanner: false };
    case 'hard_locked':
      return { allowNewBookings: false, allowStaffAccess: false, allowCustomerView: true, showProtectionBanner: false };
    case 'customer_protection':
      return { allowNewBookings: false, allowStaffAccess: false, allowCustomerView: true, showProtectionBanner: true };
    case 'active':
    default:
      return { allowNewBookings: true,  allowStaffAccess: true,  allowCustomerView: true, showProtectionBanner: false };
  }
}

// capability: 'new_bookings' | 'staff_access' | 'customer_view'
// Explicit flags on the doc win over status-derived defaults.
// branch null (missing doc / read failure) → always ok.
export function assertBranchAllows(branch, capability) {
  if (!branch) return { ok: true };
  const flags = statusFlags(branch.status);
  const map = {
    new_bookings:  branch.allowNewBookings  ?? flags.allowNewBookings,
    staff_access:  branch.allowStaffAccess  ?? flags.allowStaffAccess,
    customer_view: branch.allowCustomerView ?? flags.allowCustomerView,
  };
  const allowed = map[capability];
  if (allowed === undefined) return { ok: true };
  return allowed
    ? { ok: true }
    : { ok: false, error: `branch_${branch.status || 'locked'}` };
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
