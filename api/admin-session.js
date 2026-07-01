// ════════════════════════════════════════════════════════════════════
// /api/admin-session — consolidated session route
// ════════════════════════════════════════════════════════════════════
// Merged from admin-login.js + admin-logout.js + admin-check.js to keep
// the Vercel function count down. Request/response shapes are identical
// to the old routes — only the URL changed.
//
//   GET  /api/admin-session
//        → check session: 200 { ok:true, name, role, branches } | 401 { ok:false }
//
//   POST /api/admin-session  { action:"login", name, pin }
//        → validates against ADMIN_USERS_JSON, sets HttpOnly signed cookie
//          200 { ok:true, name, role, branches } | 400/401/500 { ok:false, error }
//
//   POST /api/admin-session  { action:"logout" }
//        → clears cookie; always 200 { ok:true }
//
// PINs are never exposed to the frontend.
// ════════════════════════════════════════════════════════════════════

import {
  validateAdminPin, createSessionCookie, clearSessionCookie,
  verifySession, getAdminUser,
  verifyPin, createCoachSessionCookie, DEFAULT_BRANCH_ID,
} from './_lib/admin-auth.js';
import { getAdminDb } from './_lib/firebase-admin.js';
import { FieldValue, Timestamp } from 'firebase-admin/firestore';

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

export default async function handler(req, res) {
  // ── check ───────────────────────────────────────────────────────
  if (req.method === 'GET') {
    const session = verifySession(req);
    if (!session) return res.status(401).json({ ok: false });
    return res.status(200).json({ ok: true, name: session.name, role: session.role, branches: session.branches });
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  const body = parseBody(req);
  const action = body?.action;

  // ── logout ──────────────────────────────────────────────────────
  // Always succeeds — safe to call even if no session is active.
  if (action === 'logout') {
    res.setHeader('Set-Cookie', clearSessionCookie());
    return res.status(200).json({ ok: true });
  }

  // ── login ───────────────────────────────────────────────────────
  if (action === 'login') {
    const name = typeof body?.name === 'string' ? body.name.trim() : '';
    const pin  = typeof body?.pin  === 'string' ? body.pin        : '';

    if (!name || !pin) {
      return res.status(400).json({ ok: false, error: 'Invalid request' });
    }

    // ── Owner/admin via ADMIN_USERS_JSON (unchanged path) ───────────
    if (getAdminUser(name)) {
      if (!validateAdminPin(name, pin)) {
        return res.status(401).json({ ok: false, error: 'Invalid name or PIN' });
      }
      let cookie;
      try {
        cookie = createSessionCookie(name);
      } catch (e) {
        console.error('[admin-session] createSessionCookie failed:', e.message);
        return res.status(500).json({ ok: false, error: 'Server configuration error' });
      }
      res.setHeader('Set-Cookie', cookie);
      const user = getAdminUser(name);
      return res.status(200).json({ ok: true, name, role: user?.role, branches: user?.branches });
    }

    // ── Coach via coach_auth (Coach 2A) ─────────────────────────────
    return handleCoachLogin(res, name, pin);
  }

  return res.status(400).json({ ok: false, error: `Invalid action: ${action}` });
}

// Coach login — verifies coach_auth pinHash with brute-force lockout, then
// issues a coach session cookie carrying branches + sessionVersion (sv).
async function handleCoachLogin(res, name, pin) {
  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[coach-login] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  const authRef = db.collection('coach_auth').doc(name);
  let auth;
  try { const s = await authRef.get(); auth = s.exists ? s.data() : null; }
  catch (e) { console.error('[coach-login] read:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }

  // Generic error for "no such coach / no PIN set" — don't reveal which.
  if (!auth || !auth.pinHash) {
    return res.status(401).json({ ok: false, error: 'Invalid name or PIN' });
  }

  const now = Date.now();
  const lockedUntil = auth.lockedUntil?.toMillis?.() ?? 0;
  if (lockedUntil > now) {
    return res.status(429).json({ ok: false, error: 'Too many attempts. Try again later.' });
  }

  if (!verifyPin(pin, auth.pinHash)) {
    const attempts = (Number(auth.failedAttempts) || 0) + 1;
    const upd = { failedAttempts: attempts, updatedAt: FieldValue.serverTimestamp() };
    if (attempts >= 5) upd.lockedUntil = Timestamp.fromMillis(now + 15 * 60 * 1000);
    try { await authRef.update(upd); } catch { /* non-fatal */ }
    return res.status(401).json({ ok: false, error: 'Invalid name or PIN' });
  }

  // PIN ok — coach must exist and be active.
  let coach;
  try { const cs = await db.collection('coaches').doc(name).get(); coach = cs.exists ? cs.data() : null; }
  catch (e) { console.error('[coach-login] read coach:', e.message); return res.status(500).json({ ok: false, error: 'Server error' }); }
  if (!coach || coach.active === false) {
    return res.status(403).json({ ok: false, error: 'Coach account is inactive' });
  }

  const branches = [coach.branchId || DEFAULT_BRANCH_ID];
  const sv = Number(auth.sessionVersion) || 1;
  try { await authRef.update({ failedAttempts: 0, lockedUntil: null, lastLoginAt: FieldValue.serverTimestamp() }); } catch { /* non-fatal */ }

  let cookie;
  try { cookie = createCoachSessionCookie(name, branches, sv); }
  catch (e) { console.error('[coach-login] cookie:', e.message); return res.status(500).json({ ok: false, error: 'Server configuration error' }); }
  res.setHeader('Set-Cookie', cookie);
  return res.status(200).json({ ok: true, name, role: 'coach', branches });
}
