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
} from './_lib/admin-auth.js';

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

  return res.status(400).json({ ok: false, error: `Invalid action: ${action}` });
}
