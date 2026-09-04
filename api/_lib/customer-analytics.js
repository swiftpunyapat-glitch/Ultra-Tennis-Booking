// Read-only customer analytics resolver.
//
// Customer identity is resolved in memory for reporting only. This module
// never imports Firestore, never writes data, never returns names/phones/LINE
// IDs, and never reads or combines package balances.

import { createHash, createHmac } from 'node:crypto';
import { normalizeIdentityPhone, usableIdentityLineId } from './customer-identity.js';

export const CUSTOMER_ANALYTICS_VERSION = 'customer-analytics-read-only-v1';

// Human-reviewed decisions are stored as one-way fingerprints so the API
// bundle does not contain the reviewed customer identifiers in plaintext.
const APPROVED_AUTO_FINGERPRINTS = new Set([
  'ca8f317e7fda7e4485660d72b97f29edc7bb6ff48f5febdd19828d95e60d0a3d',
  '10599641059dec7e419083de8270765ace158756bd2a6fd3dea89d2a943261c1',
  '50be39245b18eef46137c501a07f09c9ca52f44b2efae7cc8a72bc9c18980782',
  '72249579f23f0c46e6cc02f09de192c19624ea2e8dfcd5df38407dd1cc0dfd0e',
  '35d428fbf3c5f891e529c51f788acd2087d7935c9eb83ae5f44187d72bbcaf5f',
  'a744eada4a6d92fa36e800dc109bcfee71f3284f7f7423e94a98285619e5e571',
  '356896f24a8b92d1182fe0bf05e30311c4989d199956e0d1eca89f95ce404bf5',
  'd1f6fdb505bff933faec5c77bcdf9a5ac747175a50a0dd5f1f977f088316ed39',
  '0ab997a4e27d281c1d05e7b7d5c5db4cb71e1b271278b6df397ae711c5513486',
  '53dfc415af9a32afd37f2d12abf617dd7a1d1549935ade33794994383e87dc50',
  '2ad7d2925cafd3555ba7aab653325d46a5c4301302a0d612318eabda8ae0f2ed',
]);

const HUMAN_LINE_POLICIES = new Map([
  ['9fb41335861ef25e59be7c59d0b09d637f7467e898eb852d825e2c9a5e1d758a', 'approved_same_customer'], // Art
  ['b8cf63cde9f50a06e58e2692c286d5dc10d4963bd79681445d12327b781eefd0', 'approved_same_customer'], // Noppon
  ['e9c8d14996709f54f29054926b2aeb2866dc5aed6eba8b21010ab7992c2cf318', 'approved_same_customer'], // PAKSADA
  ['03547adaabf159197c457ccbd5f52d86725488e143e10f576bb0fe60220ad215', 'manual_review'], // Anuthat
  ['53cca8a01bcf985087373fcbde7330a5d291b0d41581632a6a432d8690f1ee68', 'contaminated'], // U609 legacy LINE
]);

const TERMINAL_NON_PLAYED = new Set(['cancelled', 'expired', 'no_show', 'rescheduled']);

function sha256(value) {
  return createHash('sha256').update(String(value)).digest('hex');
}

function lineFingerprint(lineUserId) {
  return sha256(`line|${lineUserId}`);
}

function autoFingerprint(lineUserId, phone) {
  return sha256(`auto|${lineUserId}|${phone}`);
}

function phoneOf(record) {
  return [record?.phoneNormalized, record?.customerPhoneNormalized, record?.phone, record?.customerPhone]
    .map(normalizeIdentityPhone).find(Boolean) || '';
}

function lineOf(record, source = '') {
  return usableIdentityLineId(record?.lineUserId)
    || (source === 'registered_user' ? usableIdentityLineId(record?.id) : '');
}

function validDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
}

function minutesOf(record) {
  const explicit = Number(record.durationMinutes);
  if (Number.isFinite(explicit) && explicit > 0) return explicit;
  const hours = Number(record.durationHours);
  if (Number.isFinite(hours) && hours > 0) return Math.round(hours * 60);
  const asMinutes = value => {
    const match = String(value || '').match(/^(\d{2}):(\d{2})$/);
    return match ? Number(match[1]) * 60 + Number(match[2]) : null;
  };
  const start = asMinutes(record.startTime);
  const end = asMinutes(record.endTime);
  return start !== null && end !== null && end > start ? end - start : 60;
}

function isPlayed(record) {
  const status = String(record.bookingStatus || record.status || '');
  if (TERMINAL_NON_PLAYED.has(status)) return false;
  return ['confirmed', 'completed'].includes(status)
    || ['paid', 'package'].includes(String(record.paymentStatus || ''));
}

function categoryOf(record) {
  const packageType = String(record.packageType || record.usedPackageType || '');
  if (record.isEventBooking === true || packageType === 'monstr_event_pass') return 'event_pass';
  if (record.voucherCode) return 'voucher';
  if (record.paymentStatus === 'package' || record.packageId || record.usedPackageId) return 'package';
  if (record.coachId || record.serviceType === 'coach') return 'coaching';
  return 'single_use';
}

function increment(target, key) {
  const normalized = String(key || 'unknown');
  target[normalized] = (target[normalized] || 0) + 1;
}

function rounded(value) {
  return Math.round(Number(value || 0) * 100) / 100;
}

function customerReference(identityKey, referenceSalt) {
  if (!referenceSalt || String(referenceSalt).length < 32) throw new Error('A private report reference salt is required');
  return `cust_${createHmac('sha256', String(referenceSalt)).update(`${CUSTOMER_ANALYTICS_VERSION}|${identityKey}`).digest('hex').slice(0, 16)}`;
}

function identityContext(bookings, users) {
  const registeredOwners = new Map();
  const phonesByLine = new Map();
  const addLinePhone = (line, phone) => {
    if (!line || !phone) return;
    if (!phonesByLine.has(line)) phonesByLine.set(line, new Set());
    phonesByLine.get(line).add(phone);
  };
  for (const user of users) {
    const line = lineOf(user, 'registered_user');
    const phone = phoneOf(user);
    addLinePhone(line, phone);
    if (line && phone) {
      if (!registeredOwners.has(phone)) registeredOwners.set(phone, new Set());
      registeredOwners.get(phone).add(line);
    }
  }
  for (const booking of bookings) addLinePhone(lineOf(booking), phoneOf(booking));
  const suspiciousLines = new Set([...phonesByLine].filter(([, phones]) => phones.size > 1).map(([line]) => line));
  return { registeredOwners, suspiciousLines };
}

function resolveIdentity(booking, context) {
  const existingCanonical = String(booking.canonicalCustomerId || '').trim();
  if (/^cc_[0-9a-f]{32}$/i.test(existingCanonical)) {
    return { key: `canonical:${existingCanonical}`, resolution: 'stored_canonical', reviewRequired: false };
  }
  const line = lineOf(booking);
  const phone = phoneOf(booking);
  if (line) {
    const policy = HUMAN_LINE_POLICIES.get(lineFingerprint(line));
    if (policy === 'contaminated') return { key: `isolated:${booking.id}`, resolution: 'contaminated_isolated', reviewRequired: true };
    if (policy === 'manual_review') return { key: `isolated:${booking.id}`, resolution: 'manual_review_isolated', reviewRequired: true };
    if (policy === 'approved_same_customer') return { key: `line:${line}`, resolution: 'approved_human', reviewRequired: false };
    if (context.suspiciousLines.has(line)) return { key: `isolated:${booking.id}`, resolution: 'suspicious_line_isolated', reviewRequired: true };
    return { key: `line:${line}`, resolution: 'safe_line', reviewRequired: false };
  }
  if (phone) {
    const owners = [...(context.registeredOwners.get(phone) || [])];
    if (owners.length === 1 && APPROVED_AUTO_FINGERPRINTS.has(autoFingerprint(owners[0], phone))) {
      return { key: `line:${owners[0]}`, resolution: 'approved_phone_to_line', reviewRequired: false };
    }
    return { key: `phone:${phone}`, resolution: owners.length > 1 ? 'shared_phone_manual' : 'safe_phone', reviewRequired: owners.length > 1 };
  }
  return { key: `isolated:${booking.id}`, resolution: 'unresolved_isolated', reviewRequired: true };
}

function emptyProfile(identityKey, resolved, referenceSalt) {
  return {
    identityKey,
    customerRef: customerReference(identityKey, referenceSalt),
    resolution: resolved.resolution,
    reviewRequired: resolved.reviewRequired,
    lifetimeRecords: 0,
    lifetimePlayed: 0,
    lifetimeFirstBookingDate: null,
    lifetimeLastBookingDate: null,
    periodRecords: 0,
    periodPlayed: 0,
    periodCancelled: 0,
    periodPaidRevenue: 0,
    periodBookedMinutes: 0,
    periodCategories: {},
  };
}

export function buildReadOnlyCustomerAnalytics(bookingRecords = [], userRecords = [], range, { referenceSalt } = {}) {
  if (!range?.from || !range?.to) throw new Error('Customer analytics requires an explicit date range');
  const bookings = bookingRecords.map(record => ({ ...(record.data || record), id: String(record.id) }));
  const users = userRecords.map(record => ({ ...(record.data || record), id: String(record.id) }));
  const context = identityContext(bookings, users);
  const profiles = new Map();
  const resolutionCounts = {};

  for (const booking of bookings) {
    if (!validDate(booking.date)) continue;
    const resolved = resolveIdentity(booking, context);
    if (!profiles.has(resolved.key)) profiles.set(resolved.key, emptyProfile(resolved.key, resolved, referenceSalt));
    const profile = profiles.get(resolved.key);
    profile.lifetimeRecords++;
    if (isPlayed(booking)) {
      profile.lifetimePlayed++;
      if (!profile.lifetimeFirstBookingDate || booking.date < profile.lifetimeFirstBookingDate) profile.lifetimeFirstBookingDate = booking.date;
      if (!profile.lifetimeLastBookingDate || booking.date > profile.lifetimeLastBookingDate) profile.lifetimeLastBookingDate = booking.date;
    }
    if (booking.date < range.from || booking.date > range.to) continue;
    profile.periodRecords++;
    increment(resolutionCounts, resolved.resolution);
    if (!isPlayed(booking)) {
      if (String(booking.bookingStatus || booking.status) === 'cancelled') profile.periodCancelled++;
      continue;
    }
    profile.periodPlayed++;
    profile.periodBookedMinutes += minutesOf(booking);
    increment(profile.periodCategories, categoryOf(booking));
    if (booking.paymentStatus === 'paid') profile.periodPaidRevenue += Number(booking.price ?? booking.amount) || 0;
  }

  const activeProfiles = [...profiles.values()]
    .filter(profile => profile.periodRecords > 0)
    .map(profile => {
      const output = { ...profile };
      delete output.identityKey;
      output.periodPaidRevenue = rounded(output.periodPaidRevenue);
      output.periodBookedHours = rounded(output.periodBookedMinutes / 60);
      output.isNewCustomerInPeriod = Boolean(output.lifetimeFirstBookingDate && output.lifetimeFirstBookingDate >= range.from);
      output.hadPlayedBeforePeriod = Boolean(output.lifetimeFirstBookingDate && output.lifetimeFirstBookingDate < range.from);
      return output;
    })
    .sort((a, b) => b.periodPlayed - a.periodPlayed || b.periodPaidRevenue - a.periodPaidRevenue || a.customerRef.localeCompare(b.customerRef));

  const playedProfiles = activeProfiles.filter(profile => profile.periodPlayed > 0);
  const summary = {
    periodRecords: activeProfiles.reduce((sum, profile) => sum + profile.periodRecords, 0),
    periodPlayedBookings: playedProfiles.reduce((sum, profile) => sum + profile.periodPlayed, 0),
    periodCancelledBookings: activeProfiles.reduce((sum, profile) => sum + profile.periodCancelled, 0),
    customersWithRecords: activeProfiles.length,
    activeCustomers: playedProfiles.length,
    newCustomers: playedProfiles.filter(profile => profile.isNewCustomerInPeriod).length,
    returningCustomers: playedProfiles.filter(profile => profile.hadPlayedBeforePeriod).length,
    repeatCustomersInPeriod: playedProfiles.filter(profile => profile.periodPlayed >= 2).length,
    oneVisitCustomersInPeriod: playedProfiles.filter(profile => profile.periodPlayed === 1).length,
    reviewRequiredCustomers: activeProfiles.filter(profile => profile.reviewRequired).length,
    reviewRequiredBookings: activeProfiles.filter(profile => profile.reviewRequired).reduce((sum, profile) => sum + profile.periodRecords, 0),
    totalCustomerPaidRevenue: rounded(playedProfiles.reduce((sum, profile) => sum + profile.periodPaidRevenue, 0)),
    totalCustomerBookedHours: rounded(playedProfiles.reduce((sum, profile) => sum + profile.periodBookedMinutes, 0) / 60),
  };
  return {
    analyticsVersion: CUSTOMER_ANALYTICS_VERSION,
    readOnly: true,
    writesPerformed: 0,
    piiIncluded: false,
    packageDocumentsRead: 0,
    packageBalancesCombined: false,
    range: { from: range.from, to: range.to },
    sourceCounts: { bookingsLoadedOnce: bookings.length, registeredUsersLoadedOnce: users.length },
    summary,
    resolutionCounts,
    customers: activeProfiles,
  };
}
