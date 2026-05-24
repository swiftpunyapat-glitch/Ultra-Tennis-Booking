// POST /api/admin-logout
// Clears the HttpOnly session cookie by setting Max-Age=0.
// Always succeeds — safe to call even if no session is active.

import { clearSessionCookie } from './_lib/admin-auth.js';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }
  res.setHeader('Set-Cookie', clearSessionCookie());
  return res.status(200).json({ ok: true });
}
