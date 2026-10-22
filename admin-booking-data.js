// Read-only Admin data helpers. Finance's payment basis is paidAt, never the
// review, confirmation, creation or play date. No stored dates are rewritten.
export function paymentMillis(value) {
  if (value == null) return null;
  let ms;
  if (typeof value.toMillis === 'function') ms = value.toMillis();
  else if (typeof value.toDate === 'function') ms = value.toDate()?.getTime();
  else if (value instanceof Date) ms = value.getTime();
  else if (typeof value === 'string' && /T.*(?:Z|[+-]\d{2}:\d{2})$/.test(value)) ms = Date.parse(value);
  else if (typeof value === 'object' && Number.isFinite(value.seconds)) ms = value.seconds * 1000 + (value.nanoseconds || 0) / 1e6;
  return Number.isFinite(ms) ? ms : null;
}

function bangkokParts(value) {
  const ms = paymentMillis(value);
  if (ms === null) return null;
  return Object.fromEntries(new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Bangkok', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(new Date(ms)).map(part => [part.type, part.value]));
}

export function paymentDate(value) {
  const p = bangkokParts(value);
  return p ? `${p.year}-${p.month}-${p.day}` : '';
}

export function paymentTime(value) {
  const p = bangkokParts(value);
  return p ? `${p.hour}:${p.minute}` : '—';
}

export function paidSummary(bookings, date, resourceId) {
  const seen = new Set();
  const paid = bookings.filter(booking => {
    if (booking.id && seen.has(booking.id)) return false;
    if (booking.id) seen.add(booking.id);
    return booking.paymentStatus === 'paid' && (!resourceId || (booking.resourceId || 'room1') === resourceId);
  });
  return {
    // Like Finance, cancellation does not erase an earlier payment. Refunds
    // are displayed separately; an old payment must not look like new cash.
    rows: paid.filter(b => paymentDate(b.paidAt) === date)
      .sort((a, b) => paymentMillis(a.paidAt) - paymentMillis(b.paidAt) || String(a.id).localeCompare(String(b.id))),
    missingPaidAt: paid.filter(b => paymentMillis(b.paidAt) === null).length,
  };
}

// `limit` is the SERVER PAGE SIZE, not a cap on the complete Admin history.
// Follow the raw-page cursor even if branch/test filtering leaves items empty.
// Publish a result only after every page succeeds, so failures cannot be shown
// as an apparently complete (but truncated) month or pending-reschedule queue.
export async function collectAdminBookingPages(readPage, options = {}) {
  const pageSize = Number(options.limit ?? 500);
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 500) throw new Error('Invalid booking page size');
  const { cursorBookingId: ignored, ...filters } = options;
  const items = new Map(), visited = new Set();
  let cursor;
  do {
    const data = await readPage({ ...filters, bookingOrder: 'id', limit: pageSize, ...(cursor ? { cursorBookingId: cursor } : {}) });
    if (!data || !Array.isArray(data.items)) throw new Error('Invalid booking page');
    for (const item of data.items) {
      if (!item || typeof item.id !== 'string' || !item.id) throw new Error('Booking page is missing an ID');
      // A repeated document from a concurrent refresh is one record, not two.
      // Distinct IDs (including split bookings) are never merged by code/name.
      if (!items.has(item.id)) items.set(item.id, item);
    }
    const next = data.nextCursorId;
    if (next == null) {
      if (data.nextCursor) throw new Error('Booking pagination cursor missing; refresh and retry');
      break;
    }
    if (typeof next !== 'string' || !next || next === cursor || visited.has(next)) throw new Error('Booking pagination did not advance; refresh and retry');
    visited.add(next);
    cursor = next;
  } while (true);
  return [...items.values()];
}
