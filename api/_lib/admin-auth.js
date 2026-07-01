import { createHmac, timingSafeEqual, scryptSync, randomBytes, createHash } from 'crypto';

const COOKIE = 'adminSession';

// 'coach' is a non-admin operational role (coach.html terminal). It is NOT in
// the admin privilege chain — requireRole is an allowlist, so coach endpoints
// must opt it in explicitly and admin endpoints simply omit it.
export const ROLES = ['owner', 'ultra_admin', 'branch_manager', 'branch_staff', 'viewer', 'coach'];
export const ALL_BRANCHES = '*';

// Branch helpers live here (not in their own _lib file) because every .js
// file under api/ counts toward the Vercel function limit.
// DEFAULT_BRANCH_ID must match the constants in index.html and admin.html.
export const DEFAULT_BRANCH_ID = 'ladprao1';

// Legacy docs have no branchId — treat them as the default branch.
export function resolveBranchId(docData) {
  return docData?.branchId || DEFAULT_BRANCH_ID;
}

// Legacy role mapping: before roles existed, Art was the implicit owner and
// every other admin had full access. Applied to legacy ADMIN_USERS_JSON
// entries AND to cookies issued before the role field existed.
function legacyRole(name) {
  return name === 'Art' ? 'owner' : 'ultra_admin';
}

function sign(payload) {
  const secret = process.env.ADMIN_SESSION_SECRET;
  if (!secret) throw new Error('ADMIN_SESSION_SECRET is not configured');
  return createHmac('sha256', secret).update(payload).digest('hex');
}

// Resolve one admin entry from ADMIN_USERS_JSON. Two formats per user (may be mixed):
//   legacy : "Art": "1234"                  → role derived via legacyRole, all branches
//   object : "Pim": { "pin": "1234", "role": "branch_staff", "branches": ["hatyai"] }
// Returns { pin, role, branches } or null if the user doesn't exist / is malformed.
export function getAdminUser(name) {
  const raw = process.env.ADMIN_USERS_JSON;
  if (!raw || !name) return null;
  let users;
  try { users = JSON.parse(raw); } catch { return null; }
  const entry = users[name];
  if (typeof entry === 'string') {
    return { pin: entry, role: legacyRole(name), branches: ALL_BRANCHES };
  }
  if (entry && typeof entry === 'object' && typeof entry.pin === 'string') {
    const role = ROLES.includes(entry.role) ? entry.role : 'viewer';
    const branches = entry.branches === ALL_BRANCHES
      ? ALL_BRANCHES
      : (Array.isArray(entry.branches) && entry.branches.length ? entry.branches : [DEFAULT_BRANCH_ID]);
    return { pin: entry.pin, role, branches };
  }
  return null;
}

// Validate name + PIN against ADMIN_USERS_JSON env var.
// Uses timing-safe comparison to resist brute-force timing attacks.
export function validateAdminPin(name, pin) {
  const user = getAdminUser(name);
  if (!user || typeof pin !== 'string') return false;
  const storedBuf = Buffer.from(user.pin);
  const pinBuf    = Buffer.from(pin);
  if (storedBuf.length !== pinBuf.length) return false;
  try { return timingSafeEqual(storedBuf, pinBuf); } catch { return false; }
}

// Build an HttpOnly signed session cookie.
// Payload carries role/branches so per-request checks don't re-read env.
export function createSessionCookie(name) {
  const hours = Math.max(1, parseInt(process.env.ADMIN_SESSION_HOURS ?? '24', 10));
  const exp   = Date.now() + hours * 3_600_000;
  const user  = getAdminUser(name);
  const role     = user?.role     ?? legacyRole(name);
  const branches = user?.branches ?? ALL_BRANCHES;
  const payload = Buffer.from(JSON.stringify({ name, role, branches, exp })).toString('base64url');
  const sig     = sign(payload);
  return `${COOKIE}=${payload}.${sig}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=${hours * 3600}`;
}

// Verify the signed session cookie from the incoming request.
// Returns { name, role, branches } on success, null on any failure.
// Cookies issued before roles existed lack role/branches — derive the same
// legacy mapping so existing sessions keep working across the deploy.
export function verifySession(req) {
  const raw  = req.headers.cookie ?? '';
  const pair = raw.split(';').map(s => s.trim()).find(s => s.startsWith(`${COOKIE}=`));
  if (!pair) return null;

  const value = pair.slice(COOKIE.length + 1);
  const dot   = value.lastIndexOf('.');
  if (dot < 0) return null;

  const payload = value.slice(0, dot);
  const sig     = value.slice(dot + 1);

  // HMAC-SHA256 hex output is always exactly 64 characters.
  if (sig.length !== 64) return null;

  let expected;
  try { expected = sign(payload); } catch { return null; }

  try {
    if (!timingSafeEqual(Buffer.from(expected, 'hex'), Buffer.from(sig, 'hex'))) return null;
  } catch { return null; }

  let data;
  try { data = JSON.parse(Buffer.from(payload, 'base64url').toString()); } catch { return null; }
  if (!data?.name || typeof data.exp !== 'number' || data.exp < Date.now()) return null;

  return {
    name:     data.name,
    role:     ROLES.includes(data.role) ? data.role : legacyRole(data.name),
    branches: data.branches === ALL_BRANCHES || Array.isArray(data.branches) ? data.branches : ALL_BRANCHES,
    sv:       typeof data.sv === 'number' ? data.sv : null,   // coach session version (Coach 2A)
  };
}

// ── Coach credential crypto (Coach 2A) ─────────────────────────────
// PINs are low-entropy → hash with scrypt + per-PIN salt + a server pepper so
// a DB leak alone can't brute-force them. No plaintext PIN is ever stored.
function pepper() {
  return process.env.COACH_PIN_PEPPER || process.env.ADMIN_SESSION_SECRET || '';
}

// Returns "saltHex:hashHex". Verify with verifyPin.
export function hashPin(pin) {
  const salt = randomBytes(16).toString('hex');
  const hash = scryptSync(String(pin), salt + pepper(), 64).toString('hex');
  return `${salt}:${hash}`;
}
export function verifyPin(pin, stored) {
  if (typeof stored !== 'string' || !stored.includes(':')) return false;
  const [salt, hash] = stored.split(':');
  if (!salt || !hash) return false;
  let derived;
  try { derived = scryptSync(String(pin), salt + pepper(), 64).toString('hex'); }
  catch { return false; }
  const a = Buffer.from(derived, 'hex'), b = Buffer.from(hash, 'hex');
  if (a.length !== b.length) return false;
  try { return timingSafeEqual(a, b); } catch { return false; }
}

// One-time setup/reset code — 8 chars from an unambiguous alphabet.
export function randomResetCode() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no I/O/0/1
  const bytes = randomBytes(8);
  let out = '';
  for (let i = 0; i < 8; i++) out += alphabet[bytes[i] % alphabet.length];
  return out;
}
export function hashResetCode(code) {
  return createHash('sha256').update(String(code).toUpperCase() + pepper()).digest('hex');
}
export function verifyResetCode(code, storedHash) {
  if (typeof storedHash !== 'string' || !storedHash) return false;
  const a = Buffer.from(hashResetCode(code), 'hex'), b = Buffer.from(storedHash, 'hex');
  if (a.length !== b.length) return false;
  try { return timingSafeEqual(a, b); } catch { return false; }
}

// Coach session cookie — role fixed 'coach', carries branches + sessionVersion (sv).
export function createCoachSessionCookie(coachId, branches, sessionVersion) {
  const hours = Math.max(1, parseInt(process.env.ADMIN_SESSION_HOURS ?? '24', 10));
  const exp   = Date.now() + hours * 3_600_000;
  const br    = branches === ALL_BRANCHES || (Array.isArray(branches) && branches.length)
    ? branches : [DEFAULT_BRANCH_ID];
  const payload = Buffer.from(JSON.stringify({
    name: coachId, role: 'coach', branches: br, sv: Number(sessionVersion) || 1, exp,
  })).toString('base64url');
  const sig = sign(payload);
  return `${COOKIE}=${payload}.${sig}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=${hours * 3600}`;
}

// Back-compat wrapper: returns the admin name string (or null).
// Existing call sites write this value straight into Firestore fields
// (paidBy, refundedBy, updatedBy, …) — the string return type must not change.
export function verifySessionCookie(req) {
  return verifySession(req)?.name ?? null;
}

// True if the session's role is one of the allowed roles.
export function requireRole(session, ...roles) {
  return !!session && roles.includes(session.role);
}

// True if the session can act on the given branch ('*' = all branches).
// branchId null/undefined → DEFAULT_BRANCH_ID (legacy data).
export function hasBranchAccess(session, branchId) {
  if (!session) return false;
  if (session.branches === ALL_BRANCHES) return true;
  const id = branchId || DEFAULT_BRANCH_ID;
  return Array.isArray(session.branches) && session.branches.includes(id);
}

// Return a cookie header string that immediately expires the session cookie.
export function clearSessionCookie() {
  return `${COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0`;
}
