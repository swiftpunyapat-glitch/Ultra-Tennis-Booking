// ════════════════════════════════════════════════════════════════════
// POST /api/admin-delete-booking  — permanently delete a booking record
// ════════════════════════════════════════════════════════════════════
// Auth:   requires valid admin session cookie AND admin name must be "Art".
// Body:   { bookingId: "<Firestore doc ID>" }
// Return: { ok: true }
//         { ok: false, error: "<safe message>" }
//
// What is deleted:
//   1. Google Calendar event (if googleCalendarEventId is set on the booking)
//   2. booking_slots/{resourceId}_{date}_{startTime} — only if bookingCode matches
//   3. bookings/{bookingId}
//
// What is NOT deleted:
//   • customer profile, LINE user data
//   • customer_packages (pass/package records)
//   • notification_logs
//   • unrelated bookings or slots
//   • available_slots (slot open/closed status is unchanged)
//
// Use case: test data cleanup / mistaken booking records.
// ════════════════════════════════════════════════════════════════════

import { verifySessionCookie } from './_lib/admin-auth.js';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';

// ── Firebase Admin singleton (survives warm lambda invocations) ──────
function getDb() {
  if (!getApps().length) {
    const raw = process.env.FIREBASE_SERVICE_ACCOUNT;
    if (!raw) throw new Error('FIREBASE_SERVICE_ACCOUNT env var not set');
    let sa;
    try { sa = JSON.parse(raw); }
    catch (e) { throw new Error('FIREBASE_SERVICE_ACCOUNT is not valid JSON'); }
    initializeApp({ credential: cert(sa) });
  }
  return getFirestore();
}

// ── Google Calendar OAuth — exchange refresh token for access token ──
// Mirrors the same logic as api/gcal.js (that function is not exported).
// Returns null on failure — caller treats null as "skip Calendar delete".
async function getCalendarAccessToken() {
  const { GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN } = process.env;
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !GOOGLE_REFRESH_TOKEN) return null;
  const params = new URLSearchParams({
    grant_type:    'refresh_token',
    client_id:     GOOGLE_CLIENT_ID,
    client_secret: GOOGLE_CLIENT_SECRET,
    refresh_token: GOOGLE_REFRESH_TOKEN,
  });
  try {
    const res = await fetch('https://oauth2.googleapis.com/token', {
      method:  'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body:    params.toString(),
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data.access_token || null;
  } catch {
    return null;
  }
}

// ── Delete a Google Calendar event by ID ────────────────────────────
// Returns true on success (204) or already-gone (404).
// Returns false on token failure or any other API error.
// Caller treats false as a hard stop — see Step 1 in the handler.
// sendUpdates=all — notifies BaiMon of cancellation.
async function deleteCalendarEvent(eventId) {
  const calendarId = process.env.GOOGLE_CALENDAR_ID;
  if (!calendarId || !eventId) return true; // nothing to delete

  const accessToken = await getCalendarAccessToken();
  if (!accessToken) {
    console.error('[admin-delete-booking] calendar: could not obtain access token');
    return false;
  }

  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}?sendUpdates=all`;
  try {
    const res = await fetch(url, {
      method:  'DELETE',
      headers: { 'Authorization': `Bearer ${accessToken}` },
    });
    if (res.status === 204 || res.status === 404) return true;
    const preview = await res.text().catch(() => '');
    console.error(`[admin-delete-booking] calendar delete failed — status:${res.status}`, preview.slice(0, 200));
    return false;
  } catch (e) {
    console.error('[admin-delete-booking] calendar delete threw:', e.message);
    return false;
  }
}

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  // ── Auth: valid admin session cookie required ──────────────────────
  const adminName = verifySessionCookie(req);
  if (!adminName) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }

  // ── Art-only: enforce server-side regardless of frontend hiding ────
  if (adminName !== 'Art') {
    return res.status(403).json({ ok: false, error: 'Only Art can permanently delete bookings.' });
  }

  const body = parseBody(req);
  const { bookingId } = body ?? {};
  if (!bookingId || typeof bookingId !== 'string' || !bookingId.trim()) {
    return res.status(400).json({ ok: false, error: 'Missing bookingId' });
  }

  let db;
  try { db = getDb(); } catch (e) {
    console.error('[admin-delete-booking] DB init failed:', e.message);
    return res.status(500).json({ ok: false, error: 'Database not available' });
  }

  // ── Read booking from Firestore — never trust frontend fields ──────
  const bookingRef = db.collection('bookings').doc(bookingId);
  let booking;
  try {
    const snap = await bookingRef.get();
    if (!snap.exists) {
      return res.status(404).json({ ok: false, error: 'Booking not found' });
    }
    booking = snap.data();
  } catch (e) {
    console.error('[admin-delete-booking] failed to read booking:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to read booking' });
  }

  const { resourceId, date, startTime, bookingCode, googleCalendarEventId } = booking;
  console.log(`[admin-delete-booking] START — admin:${adminName} id:${bookingId} code:${bookingCode}`);

  // ── Step 1: delete Google Calendar event (BLOCKING) ─────────────
  // If the booking has a Calendar event, it MUST be deleted before any
  // Firestore writes. A failure here stops the entire operation so we
  // never create an orphan Calendar event with no admin UI record to
  // retry or clean up from.
  // 404 from Google = event already gone → treat as success and continue.
  if (googleCalendarEventId) {
    const calOk = await deleteCalendarEvent(googleCalendarEventId);
    if (!calOk) {
      console.error(`[admin-delete-booking] calendar delete failed for ${googleCalendarEventId} — booking NOT deleted`);
      return res.status(200).json({
        ok: false,
        error: 'Calendar delete failed. Booking was not deleted. Please retry.',
      });
    }
    console.log(`[admin-delete-booking] calendar event removed: ${googleCalendarEventId}`);
  }

  // ── Step 2: delete booking_slot doc if it belongs to this booking ──
  // Slot ID pattern: {resourceId}_{date}_{startTime without colon}
  // Example: room1_2026-05-25_2300
  //
  // Safety: only delete if slotData.bookingCode === bookingCode.
  // Protects against accidentally releasing a slot reassigned to another booking
  // (possible after data drift or race conditions).
  if (resourceId && date && startTime) {
    const slotId = `${resourceId}_${date}_${startTime.replace(':', '')}`;
    const slotRef = db.collection('booking_slots').doc(slotId);
    try {
      const slotSnap = await slotRef.get();
      if (!slotSnap.exists) {
        console.log(`[admin-delete-booking] booking_slot ${slotId} — not found, skipped`);
      } else {
        const slotData = slotSnap.data();
        const ownsSlot = slotData.bookingId === bookingId || slotData.bookingCode === bookingCode;
        if (ownsSlot) {
          await slotRef.delete();
          console.log(`[admin-delete-booking] booking_slot deleted: ${slotId}`);
        } else {
          // Slot belongs to a different booking — do NOT touch it.
          console.log(`[admin-delete-booking] booking_slot ${slotId} belongs to ${slotData.bookingCode} — skipped`);
        }
      }
    } catch (e) {
      // Non-fatal — log and continue to delete the booking document.
      console.error('[admin-delete-booking] booking_slot delete error:', e.message);
    }
  } else {
    console.log(`[admin-delete-booking] missing resourceId/date/startTime — slot cleanup skipped`);
  }

  // ── Step 3: delete the booking document ───────────────────────────
  try {
    await bookingRef.delete();
    console.log(`[admin-delete-booking] DONE — booking deleted: ${bookingId} code:${bookingCode}`);
  } catch (e) {
    console.error('[admin-delete-booking] failed to delete booking doc:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to delete booking record' });
  }

  return res.status(200).json({ ok: true });
}
