// GET /api/ai-report — read-only, token-authenticated booking analytics.
// Tokens are issued/revoked by Art from Admin. The response deliberately
// excludes customer names, phone numbers, LINE IDs and payment-slip URLs.

import {
  getAdminDb, checkRateLimit, clientIp, RATE_LIMITS,
} from './_lib/firebase-admin.js';
import { FieldValue } from 'firebase-admin/firestore';
import {
  buildAiBookingReport, hashAiReportToken, normalizeAiReportToken,
  resolveAiReportRange, AI_REPORT_MAX_RANGE_DAYS,
} from './_lib/ai-report.js';

function singleQueryValue(value) {
  return Array.isArray(value) ? value[0] : value;
}

function bearerToken(req) {
  const authorization = String(req.headers?.authorization || '');
  const match = authorization.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

function requestedToken(req) {
  return normalizeAiReportToken(bearerToken(req) || singleQueryValue(req.query?.token));
}

function isExpired(timestamp) {
  const millis = timestamp?.toMillis?.();
  return !Number.isFinite(millis) || millis <= Date.now();
}

function boolQuery(value) {
  return ['1', 'true', 'yes'].includes(String(singleQueryValue(value) || '').toLowerCase());
}

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 'private, no-store, max-age=0');
  res.setHeader('X-Robots-Tag', 'noindex, nofollow, noarchive');

  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ ok: false, error: 'Method Not Allowed' });
  }

  const token = requestedToken(req);
  if (!token) return res.status(401).json({ ok: false, error: 'Valid AI Report access token required' });
  const tokenHash = hashAiReportToken(token);

  let db;
  try { db = getAdminDb(); }
  catch (e) {
    console.error('[ai-report] DB init:', e.message);
    return res.status(500).json({ ok: false, error: 'Report database unavailable' });
  }

  const ip = clientIp(req);
  const rateKey = ip === 'unknown' ? `token:${tokenHash}` : `ip:${ip}`;
  const rate = await checkRateLimit(db, {
    bucket: 'ai_report_read', key: rateKey, ...RATE_LIMITS.aiReportRead,
  });
  if (!rate.allowed) {
    res.setHeader('Retry-After', String(rate.retryAfterSec || 60));
    return res.status(429).json({ ok: false, error: 'Too many report requests; try again later' });
  }

  let accessSnap;
  try { accessSnap = await db.collection('ai_report_access').doc(tokenHash).get(); }
  catch (e) {
    console.error('[ai-report] Access lookup:', e.message);
    return res.status(500).json({ ok: false, error: 'Report access check failed' });
  }
  if (!accessSnap.exists || accessSnap.data().active !== true) {
    return res.status(401).json({ ok: false, error: 'AI Report link is invalid or revoked' });
  }
  const access = accessSnap.data();
  if (isExpired(access.expiresAt)) {
    return res.status(410).json({ ok: false, error: 'AI Report link has expired' });
  }

  const query = {
    month: singleQueryValue(req.query?.month),
    from: singleQueryValue(req.query?.from),
    to: singleQueryValue(req.query?.to),
  };
  const range = resolveAiReportRange(query);
  if (!range.ok) return res.status(400).json({ ok: false, error: range.error });

  const details = boolQuery(req.query?.details);
  if (details && (!Array.isArray(access.scopes) || !access.scopes.includes('booking_details_sanitized'))) {
    return res.status(403).json({ ok: false, error: 'This AI Report link cannot read booking rows' });
  }
  const page = Number(singleQueryValue(req.query?.page) || 1);
  const limit = Number(singleQueryValue(req.query?.limit) || 200);
  try {
    const bookingSnap = await db.collection('bookings')
      .where('date', '>=', range.from)
      .where('date', '<=', range.to)
      .get();
    const records = bookingSnap.docs.map(doc => ({ id: doc.id, data: doc.data() }));
    const report = buildAiBookingReport(records, range, { details, page, limit });

    try {
      await accessSnap.ref.set({
        lastUsedAt: FieldValue.serverTimestamp(),
        requestCount: FieldValue.increment(1),
      }, { merge: true });
    } catch (e) {
      console.error('[ai-report] Usage metadata update:', e.message);
    }

    return res.status(200).json({
      ok: true,
      reportVersion: '1',
      generatedAt: new Date().toISOString(),
      timezone: 'Asia/Bangkok',
      access: { label: access.label || 'AI Booking Report', expiresAt: access.expiresAt.toDate().toISOString() },
      privacy: {
        piiIncluded: false,
        excludedFields: ['customerName', 'customerPhone', 'lineUserId', 'slipUrl'],
      },
      filters: { mode: range.mode, month: range.month, from: range.from, to: range.to, details },
      ...report,
      usage: {
        currentMonth: '?month=YYYY-MM',
        dateRange: `?from=YYYY-MM-DD&to=YYYY-MM-DD (maximum ${AI_REPORT_MAX_RANGE_DAYS} days per request)`,
        sanitizedBookings: '?details=1&page=1&limit=200 (limit maximum 500)',
        authentication: 'Use this link token or Authorization: Bearer <token>',
      },
    });
  } catch (e) {
    console.error('[ai-report] Build report:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to build booking report' });
  }
}
