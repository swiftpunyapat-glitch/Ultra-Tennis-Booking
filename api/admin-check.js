// GET /api/admin-check
// Verifies the HttpOnly session cookie set by /api/admin-login.
// Returns { ok: true, name } if the session is valid and unexpired.
// Returns 401 { ok: false } otherwise — caller should redirect to login.

import { verifySessionCookie } from './_lib/admin-auth.js';

export default async function handler(req, res) {
  const name = verifySessionCookie(req);
  if (!name) return res.status(401).json({ ok: false });
  return res.status(200).json({ ok: true, name });
}
