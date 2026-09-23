// Test Mode — the pure part, shared by the pages and the server.
//
// A test booking goes through the real booking, transaction and slot-claim
// path, so it is a booking record like any other and every report would pick
// it up. Two fields mark it, written only by the server from a verified test
// session or Art's owner-only retrospective action, never public client flags:
//
//   isTest: true          — the flag the reports filter on
//   testSessionId: "..."  — which session owns it, so a purge knows its scope
//
// Reading treats the two as independent: a record carrying either one is a
// test record. Exclusion has to fail toward excluding, because a test booking
// counted as real corrupts revenue figures, while a real booking wrongly
// excluded is visible immediately and cannot silently inflate anything.
//
// The signing and session lookup live in api/_lib/test-session.js — that half
// is server-only and must never be reachable from a page.

export const TEST_BOOKING_FIELDS = Object.freeze(['isTest', 'testSessionId']);

export function isTestBooking(b) {
  if (b?.isTest === true) return true;
  return typeof b?.testSessionId === 'string' && b.testSessionId.trim() !== '';
}

export function isLiveBooking(b) {
  return !isTestBooking(b);
}

// Drop test records from anything a report is about to total up. Reports call
// this instead of filtering inline so a new report cannot forget to.
export function excludeTestBookings(bookings) {
  return (Array.isArray(bookings) ? bookings : []).filter(isLiveBooking);
}

// Same, for the { id, data } record shape the AI report and admin-ops iterate.
export function excludeTestRecords(records) {
  return (Array.isArray(records) ? records : []).filter(r => isLiveBooking(r?.data ?? r));
}

// The scope a purge is allowed to touch. A record belongs to a session only
// when it names that session — isTest alone is never enough to delete
// something, so a stray flag cannot widen a purge.
export function belongsToTestSession(record, testSessionId) {
  const id = String(testSessionId || '').trim();
  if (!id) return false;
  return String(record?.testSessionId || '').trim() === id;
}
