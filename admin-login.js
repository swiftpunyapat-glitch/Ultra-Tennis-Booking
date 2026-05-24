// POST /api/admin-login
// Body: { name: string, pin: string }
// On success: sets HttpOnly signed session cookie, returns { ok: true, name }
// On failure: returns { ok: false, error } with 401
//
// Credentials are validated against ADMIN_USERS_JSON env var.
// PINs are never exposed to the frontend.

import { validateAdminPin, createSessionCookie } from './_lib/admin-auth.js';

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  const body = parseBody(req);
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
    console.error('[admin-login] createSessionCookie failed:', e.message);
    return res.status(500).json({ ok: false, error: 'Server configuration error' });
  }

  res.setHeader('Set-Cookie', cookie);
  return res.status(200).json({ ok: true, name });
}
