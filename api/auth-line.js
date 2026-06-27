// ════════════════════════════════════════════════════════════════════
// POST /api/auth-line — Phase 2 / Stage 0: customer custom-token sign-in
// ════════════════════════════════════════════════════════════════════
// Verifies a LIFF id_token against LINE, then mints a Firebase custom token
// whose uid IS the verified LINE userId. The client exchanges it via
// signInWithCustomToken() so request.auth.uid === lineUserId.
//
// PUBLIC (no admin session): safe because a token is only issued for an
// identity the caller can already prove (a valid LINE id_token). The client's
// self-stated lineUserId is NEVER trusted — `sub` from the verified token is
// the only source of identity.
//
// Additive (Stage 0): firestore.rules are unchanged, so a failure here must
// never block the customer — the client falls back to the existing flow.
// ════════════════════════════════════════════════════════════════════

import { getAdminAuth } from './_lib/firebase-admin.js';

// LINE Login channel id that the LIFF app belongs to (the id_token `aud`).
// Not a secret — the LIFF id is already shipped in the client. Env overrides
// the default so the value can be rotated without a redeploy.
const LINE_LOGIN_CHANNEL_ID = process.env.LINE_LOGIN_CHANNEL_ID || '2010034901';

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  const body = parseBody(req);
  const idToken = body && typeof body.idToken === 'string' ? body.idToken.trim() : '';
  if (!idToken) {
    return res.status(400).json({ ok: false, error: 'Missing idToken' });
  }

  // ── Verify the LIFF id_token against LINE ───────────────────────────
  let payload;
  try {
    const r = await fetch('https://api.line.me/oauth2/v2.1/verify', {
      method:  'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body:    new URLSearchParams({ id_token: idToken, client_id: LINE_LOGIN_CHANNEL_ID }).toString(),
    });
    payload = await r.json().catch(() => null);
    if (!r.ok || !payload) {
      console.warn('[auth-line] LINE verify rejected:', r.status, payload?.error_description || payload?.error || '');
      return res.status(401).json({ ok: false, error: 'LINE token invalid' });
    }
  } catch (e) {
    console.error('[auth-line] LINE verify threw:', e.message);
    return res.status(502).json({ ok: false, error: 'Could not reach LINE verification' });
  }

  // ── Defensive re-validation of security-critical claims ─────────────
  // (LINE's endpoint already checks signature/exp/aud given client_id, but we
  //  re-check the fields we depend on so a future LINE change can't slip past.)
  const lineUserId = typeof payload.sub === 'string' ? payload.sub : '';
  if (!lineUserId) {
    return res.status(401).json({ ok: false, error: 'Token has no subject' });
  }
  if (payload.aud !== LINE_LOGIN_CHANNEL_ID) {
    console.warn('[auth-line] aud mismatch:', payload.aud);
    return res.status(401).json({ ok: false, error: 'Token audience mismatch' });
  }
  if (typeof payload.exp === 'number' && payload.exp * 1000 <= Date.now()) {
    return res.status(401).json({ ok: false, error: 'Token expired' });
  }

  // ── Mint Firebase custom token (uid = verified lineUserId) ──────────
  let customToken;
  try {
    customToken = await getAdminAuth().createCustomToken(lineUserId, { line: true });
  } catch (e) {
    console.error('[auth-line] createCustomToken failed:', e.message);
    return res.status(500).json({ ok: false, error: 'Could not mint auth token' });
  }

  console.log(`[auth-line] minted token for ${lineUserId}`);
  return res.status(200).json({
    ok:          true,
    customToken,
    uid:         lineUserId,
    displayName: typeof payload.name === 'string' ? payload.name : null,
  });
}
