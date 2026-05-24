// ════════════════════════════════════════════════════════════════════
// POST /api/gcal  — create a Google Calendar event for a paid booking
// ════════════════════════════════════════════════════════════════════
// Auth:   requires valid admin session cookie (set by /api/admin-login).
// Body:   { action: "create", booking: { bookingCode, date, startTime, endTime, ... } }
// Return: { ok: true,  eventId, htmlLink }
//         { ok: false, error: "<safe message>" }
//
// Phase 1 scope: action "create" only.
// Phase 2 will add "update" (reschedule) and "delete" (cancel of paid booking).
//
// Privacy rules enforced here:
//   • Event title contains NO customer name or phone number.
//   • Description contains only: booking code, date, time, payment status, type, source.
//   • No LINE userId, no slip URL, no Firestore document paths in the event.
// ════════════════════════════════════════════════════════════════════

import { verifySessionCookie } from './_lib/admin-auth.js';

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

// Exchange refresh token for a short-lived access token.
async function getAccessToken() {
  const { GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN } = process.env;
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !GOOGLE_REFRESH_TOKEN) {
    throw new Error('Google OAuth env vars not configured');
  }
  const params = new URLSearchParams({
    grant_type:    'refresh_token',
    client_id:     GOOGLE_CLIENT_ID,
    client_secret: GOOGLE_CLIENT_SECRET,
    refresh_token: GOOGLE_REFRESH_TOKEN,
  });
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body:    params.toString(),
  });
  if (!res.ok) {
    const preview = await res.text().catch(() => '');
    console.error('[gcal] token exchange failed — status:', res.status, preview.slice(0, 120));
    throw new Error('token_exchange_failed');
  }
  const data = await res.json();
  if (!data.access_token) throw new Error('token_exchange_empty');
  return data.access_token;
}

// Build the Calendar event object from booking fields.
// Only privacy-safe fields are included (no customer name, phone, or LINE ID).
function buildEvent(booking) {
  const { date, startTime, endTime, bookingCode, bookingType, source } = booking;

  // Privacy-safe title: slot time only, no PII
  const summary = `Ultra Tennis Booking | ${startTime}-${endTime}`;

  // RFC 3339 with Bangkok offset — avoids UTC conversion entirely
  const startDT = `${date}T${startTime}:00+07:00`;
  const endDT   = `${date}T${endTime}:00+07:00`;

  const descLines = [
    `Booking Code: ${bookingCode || '—'}`,
    `Date: ${date || '—'}`,
    `Time: ${startTime || '—'}–${endTime || '—'}`,
    `Payment: Paid`,
  ];
  if (bookingType) descLines.push(`Type: ${bookingType}`);
  if (source)      descLines.push(`Source: ${source}`);

  const event = {
    summary,
    description: descLines.join('\n'),
    start:    { dateTime: startDT, timeZone: 'Asia/Bangkok' },
    end:      { dateTime: endDT,   timeZone: 'Asia/Bangkok' },
    location: 'Ultra Tennis',
  };

  // Attendees: comma-separated email list from env var
  const attendeesRaw = (process.env.GOOGLE_CALENDAR_ATTENDEES || '').trim();
  if (attendeesRaw) {
    event.attendees = attendeesRaw
      .split(',')
      .map(e => e.trim())
      .filter(Boolean)
      .map(email => ({ email }));
  }

  return event;
}

async function createCalendarEvent(booking) {
  const calendarId = process.env.GOOGLE_CALENDAR_ID;
  if (!calendarId) throw new Error('GOOGLE_CALENDAR_ID not configured');

  const accessToken = await getAccessToken();
  const event       = buildEvent(booking);

  // sendUpdates=all — ensures attendees (e.g. BaiMon) receive an email/calendar invite.
  // Without this param Google defaults to no notification for internal Gmail attendees.
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events?sendUpdates=all`;
  const res = await fetch(url, {
    method:  'POST',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type':  'application/json',
    },
    body: JSON.stringify(event),
  });

  if (!res.ok) {
    const preview = await res.text().catch(() => '');
    console.error('[gcal] event creation failed — status:', res.status, preview.slice(0, 200));
    throw new Error(`calendar_api_${res.status}`);
  }

  const data = await res.json();
  return { eventId: data.id, htmlLink: data.htmlLink };
}

function validateBooking(b) {
  if (!b || typeof b !== 'object')                  return 'Missing booking data';
  if (!b.bookingCode)                               return 'Missing bookingCode';
  if (!b.date || !/^\d{4}-\d{2}-\d{2}$/.test(b.date))       return 'Invalid or missing date';
  if (!b.startTime || !/^\d{2}:\d{2}$/.test(b.startTime))    return 'Invalid or missing startTime';
  if (!b.endTime   || !/^\d{2}:\d{2}$/.test(b.endTime))      return 'Invalid or missing endTime';
  return null;
}

// Translate internal error codes to user-safe messages.
// Never expose OAuth tokens, raw Google responses, or server internals.
function safeError(internalMsg) {
  if (internalMsg?.includes('token_exchange')) return 'Google Calendar auth failed — check OAuth credentials';
  if (internalMsg?.includes('calendar_api_403')) return 'Calendar access denied — check calendar sharing settings';
  if (internalMsg?.includes('calendar_api_404')) return 'Calendar not found — check GOOGLE_CALENDAR_ID';
  if (internalMsg?.includes('calendar_api_'))    return 'Google Calendar returned an error — please retry';
  if (internalMsg?.includes('not configured'))   return 'Google Calendar is not configured — contact admin';
  return 'Google Calendar sync unavailable — please retry';
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  // Auth: require valid admin session cookie — no x-internal-secret for this route
  const adminName = verifySessionCookie(req);
  if (!adminName) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }

  const body           = parseBody(req);
  const { action, booking } = body ?? {};

  if (action !== 'create') {
    return res.status(400).json({ ok: false, error: `Unsupported action "${action}"` });
  }

  const validationError = validateBooking(booking);
  if (validationError) {
    return res.status(400).json({ ok: false, error: validationError });
  }

  try {
    const { eventId, htmlLink } = await createCalendarEvent(booking);
    console.log(`[gcal] event created — admin: ${adminName}, booking: ${booking.bookingCode}, eventId: ${eventId}`);
    return res.status(200).json({ ok: true, eventId, htmlLink });
  } catch (e) {
    // Always return HTTP 200 so the caller can read the JSON body.
    // The caller is responsible for writing syncStatus: "failed" to Firestore.
    console.error('[gcal] createCalendarEvent error:', e.message);
    return res.status(200).json({ ok: false, error: safeError(e.message) });
  }
}
