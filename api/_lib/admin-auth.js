import { createHmac, timingSafeEqual } from 'crypto';

const COOKIE = 'adminSession';

function sign(payload) {
  const secret = process.env.ADMIN_SESSION_SECRET;
  if (!secret) throw new Error('ADMIN_SESSION_SECRET is not configured');
  return createHmac('sha256', secret).update(payload).digest('hex');
}

// Validate name + PIN against ADMIN_USERS_JSON env var.
// Uses timing-safe comparison to resist brute-force timing attacks.
export function validateAdminPin(name, pin) {
  const raw = process.env.ADMIN_USERS_JSON;
  if (!raw) return false;
  let users;
  try { users = JSON.parse(raw); } catch { return false; }
  const stored = users[name];
  if (typeof stored !== 'string') return false;
  const storedBuf = Buffer.from(stored);
  const pinBuf    = Buffer.from(pin);
  if (storedBuf.length !== pinBuf.length) return false;
  try { return timingSafeEqual(storedBuf, pinBuf); } catch { return false; }
}

// Build an HttpOnly signed session cookie.
export function createSessionCookie(name) {
  const hours = Math.max(1, parseInt(process.env.ADMIN_SESSION_HOURS ?? '24', 10));
  const exp     = Date.now() + hours * 3_600_000;
  const payload = Buffer.from(JSON.stringify({ name, exp })).toString('base64url');
  const sig     = sign(payload);
  return `${COOKIE}=${payload}.${sig}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=${hours * 3600}`;
}

// Verify the signed session cookie from the incoming request.
// Returns the admin name on success, null on any failure.
export function verifySessionCookie(req) {
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
  return data.name;
}

// Return a cookie header string that immediately expires the session cookie.
export function clearSessionCookie() {
  return `${COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0`;
}
