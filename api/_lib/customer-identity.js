// Customer identity dry-run engine.
//
// This module is intentionally pure: it receives already-read documents and
// returns a proposal/report. It never imports Firestore and never writes data.

export const CUSTOMER_IDENTITY_RULE_VERSION = 'customer-identity-v1-safety-2';

const THAI_MOBILE_RE = /^0[689]\d{8}$/;
const LINE_USER_ID_RE = /^U[0-9a-f]{32}$/i;

export function normalizeIdentityPhone(value) {
  let digits = String(value || '').replace(/\D/g, '');
  if (digits.startsWith('66') && digits.length === 11) digits = `0${digits.slice(2)}`;
  if (!THAI_MOBILE_RE.test(digits)) return '';
  return digits;
}

export function usableIdentityLineId(value) {
  const id = String(value || '').trim();
  return LINE_USER_ID_RE.test(id) ? id : '';
}

function sorted(values) {
  return [...values].sort((a, b) => String(a).localeCompare(String(b)));
}

function sourceRecord(record, source) {
  const id = String(record?.id || 'unknown');
  const lineUserId = usableIdentityLineId(record?.lineUserId)
    || (source === 'registered_user' ? usableIdentityLineId(id) : '');
  const phone = [record?.phoneNormalized, record?.customerPhoneNormalized, record?.phone, record?.customerPhone]
    .map(normalizeIdentityPhone).find(Boolean) || '';
  const name = String(record?.name || record?.customerName || record?.lineDisplayName || '').trim();
  return { id, source, lineUserId, phone, name, raw: record || {} };
}

function bookingStatusStats(bookings) {
  const dates = bookings.map(item => String(item.raw.date || '')).filter(value => /^\d{4}-\d{2}-\d{2}$/.test(value)).sort();
  return {
    totalBookings: bookings.length,
    confirmedBookings: bookings.filter(item => ['confirmed', 'completed'].includes(item.raw.bookingStatus)).length,
    cancelledBookings: bookings.filter(item => item.raw.bookingStatus === 'cancelled').length,
    firstBookingDate: dates[0] || null,
    lastBookingDate: dates.at(-1) || null,
  };
}

export function buildCustomerIdentityDryRun({ bookings = [], users = [], packages = [], generatedAt = null } = {}) {
  const userRecords = users.map(item => sourceRecord(item, 'registered_user'));
  const bookingRecords = bookings.map(item => sourceRecord(item, 'booking'));
  const packageRecords = packages.map(item => sourceRecord(item, 'package'));
  const allRecords = [...userRecords, ...bookingRecords, ...packageRecords];

  const usersByLine = new Map();
  const linesByPhone = new Map();
  for (const user of userRecords) {
    if (user.lineUserId) {
      if (!usersByLine.has(user.lineUserId)) usersByLine.set(user.lineUserId, []);
      usersByLine.get(user.lineUserId).push(user);
    }
    if (user.phone && user.lineUserId) {
      if (!linesByPhone.has(user.phone)) linesByPhone.set(user.phone, new Set());
      linesByPhone.get(user.phone).add(user.lineUserId);
    }
  }

  const recordsByLine = new Map();
  for (const record of allRecords) {
    if (!record.lineUserId) continue;
    if (!recordsByLine.has(record.lineUserId)) recordsByLine.set(record.lineUserId, []);
    recordsByLine.get(record.lineUserId).push(record);
  }
  const suspiciousLineIdentities = [...recordsByLine.entries()].flatMap(([lineUserId, records]) => {
    const phones = new Set(records.map(record => record.phone).filter(Boolean));
    if (phones.size < 2) return [];
    const severity = phones.size >= 3 ? 'hard_block' : 'manual_review';
    return [{
      type: 'suspicious_line_identity', lineUserId,
      phones: sorted(phones),
      names: sorted(new Set(records.map(record => record.name).filter(Boolean))),
      bookingIds: sorted(records.filter(record => record.source === 'booking').map(record => record.id)),
      registeredUserDocumentIds: sorted(records.filter(record => record.source === 'registered_user').map(record => record.id)),
      packageIds: sorted(records.filter(record => record.source === 'package').map(record => record.id)),
      severity,
      reason: severity === 'hard_block'
        ? 'One LINE identity is attached to three or more distinct valid Thai mobile phones. Auto-linking is hard-blocked.'
        : 'One LINE identity is attached to two distinct valid Thai mobile phones. Auto-linking requires manual review.',
    }];
  }).sort((a, b) => b.phones.length - a.phones.length || a.lineUserId.localeCompare(b.lineUserId));
  const suspiciousLineIds = new Set(suspiciousLineIdentities.map(item => item.lineUserId));

  const conflicts = new Map();
  const addConflict = (key, value) => { if (!conflicts.has(key)) conflicts.set(key, value); };

  for (const [phone, lineIds] of linesByPhone) {
    if (lineIds.size > 1) {
      addConflict(`shared_phone:${phone}`, {
        type: 'shared_phone_multiple_line_accounts', phone,
        lineUserIds: sorted(lineIds), severity: 'manual_review',
        reason: 'One normalized phone belongs to more than one registered LINE account.',
      });
    }
  }
  for (const [lineUserId, records] of usersByLine) {
    if (records.length > 1) {
      addConflict(`duplicate_line:${lineUserId}`, {
        type: 'duplicate_registered_line_documents', lineUserId,
        registeredUserDocumentIds: sorted(records.map(record => record.id)), severity: 'manual_review',
        reason: 'More than one registered-user document resolves to the same LINE identity.',
      });
    }
  }

  const resolveCanonical = record => {
    if (record.lineUserId) {
      if (record.phone) {
        const owners = linesByPhone.get(record.phone) || new Set();
        if (owners.size === 1 && !owners.has(record.lineUserId)) {
          addConflict(`line_phone:${record.source}:${record.id}`, {
            type: 'line_phone_disagreement', source: record.source, recordId: record.id,
            lineUserId: record.lineUserId, phone: record.phone, registeredPhoneOwner: sorted(owners)[0],
            severity: 'manual_review', reason: 'Record LINE identity disagrees with the sole registered owner of its phone.',
          });
        }
      }
      return `line:${record.lineUserId}`;
    }
    if (record.phone) {
      const owners = linesByPhone.get(record.phone) || new Set();
      if (owners.size === 1) {
        const owner = sorted(owners)[0];
        if (!suspiciousLineIds.has(owner)) return `line:${owner}`;
      }
      return `phone:${record.phone}`;
    }
    return `record:${record.source}:${record.id}`;
  };

  const canonicalByRecord = new Map();
  for (const record of allRecords) canonicalByRecord.set(`${record.source}:${record.id}`, resolveCanonical(record));

  const proposalMap = new Map();
  for (const record of [...bookingRecords, ...packageRecords]) {
    if (record.lineUserId || !record.phone) continue;
    const owners = linesByPhone.get(record.phone) || new Set();
    if (owners.size !== 1) continue;
    const owner = sorted(owners)[0];
    if (suspiciousLineIds.has(owner)) continue;
    const canonicalCustomerId = `line:${owner}`;
    const key = `phone:${record.phone}->${canonicalCustomerId}`;
    if (!proposalMap.has(key)) proposalMap.set(key, {
      aliasCustomerId: `phone:${record.phone}`, canonicalCustomerId,
      phone: record.phone, confidence: 'high', rule: 'phone_matches_single_registered_line',
      bookingIds: new Set(), packageIds: new Set(), names: new Set(),
    });
    const proposal = proposalMap.get(key);
    if (record.source === 'booking') proposal.bookingIds.add(record.id);
    if (record.source === 'package') proposal.packageIds.add(record.id);
    if (record.name) proposal.names.add(record.name);
  }
  const proposedLinks = [...proposalMap.values()].map(item => ({
    ...item,
    bookingIds: sorted(item.bookingIds), packageIds: sorted(item.packageIds), names: sorted(item.names),
    evidenceCount: item.bookingIds.size + item.packageIds.size,
  })).sort((a, b) => b.evidenceCount - a.evidenceCount || a.aliasCustomerId.localeCompare(b.aliasCustomerId));

  const profiles = new Map();
  const ensureProfile = canonicalCustomerId => {
    if (!profiles.has(canonicalCustomerId)) profiles.set(canonicalCustomerId, {
      canonicalCustomerId, names: new Set(), phones: new Set(), lineUserIds: new Set(),
      registeredUserDocumentIds: new Set(), bookingRecords: [], packageIds: new Set(), sourceIdentities: new Set(),
    });
    return profiles.get(canonicalCustomerId);
  };
  for (const record of allRecords) {
    const canonicalCustomerId = canonicalByRecord.get(`${record.source}:${record.id}`);
    const profile = ensureProfile(canonicalCustomerId);
    if (record.name) profile.names.add(record.name);
    if (record.phone) profile.phones.add(record.phone);
    if (record.lineUserId) profile.lineUserIds.add(record.lineUserId);
    profile.sourceIdentities.add(record.lineUserId ? `line:${record.lineUserId}` : record.phone ? `phone:${record.phone}` : `record:${record.source}:${record.id}`);
    if (record.source === 'registered_user') profile.registeredUserDocumentIds.add(record.id);
    if (record.source === 'booking') profile.bookingRecords.push(record);
    if (record.source === 'package') profile.packageIds.add(record.id);
  }

  const packageById = new Map(packageRecords.map(record => [record.id, record]));
  const packageConflicts = [];
  for (const booking of bookingRecords) {
    const bookingCanonical = canonicalByRecord.get(`booking:${booking.id}`);
    const packageIds = new Set([booking.raw.packageId, booking.raw.usedPackageId].filter(Boolean).map(String));
    for (const packageId of packageIds) {
      const packageRecord = packageById.get(packageId);
      if (!packageRecord) {
        packageConflicts.push({
          type: 'missing_package_document', bookingId: booking.id, packageId,
          severity: 'manual_review', reason: 'Booking references a package document that does not exist in the loaded source.',
        });
        continue;
      }
      const packageCanonical = canonicalByRecord.get(`package:${packageId}`);
      if (bookingCanonical !== packageCanonical) {
        packageConflicts.push({
          type: 'package_owner_identity_mismatch', bookingId: booking.id, packageId,
          bookingCanonicalCustomerId: bookingCanonical, packageCanonicalCustomerId: packageCanonical,
          severity: 'manual_review', reason: 'Booking and referenced package resolve to different canonical customers.',
        });
      }
    }
  }

  const passReview = [...profiles.values()]
    .filter(profile => profile.packageIds.size > 1)
    .map(profile => ({
      type: 'multiple_distinct_package_documents', canonicalCustomerId: profile.canonicalCustomerId,
      packageIds: sorted(profile.packageIds), severity: 'manual_review',
      reason: 'Distinct package documents remain distinct; never deduplicate them by name, minutes, or expiry.',
    }));

  const unresolvedRecords = allRecords
    .filter(record => !record.lineUserId && !record.phone)
    .map(record => ({ source: record.source, recordId: record.id, name: record.name || null, reason: 'No usable LINE ID or normalized phone.' }));

  const derivedProfiles = [...profiles.values()].map(profile => ({
    canonicalCustomerId: profile.canonicalCustomerId,
    names: sorted(profile.names), phones: sorted(profile.phones), lineUserIds: sorted(profile.lineUserIds),
    registeredUserDocumentIds: sorted(profile.registeredUserDocumentIds),
    sourceIdentities: sorted(profile.sourceIdentities), packageIds: sorted(profile.packageIds),
    ...bookingStatusStats(profile.bookingRecords),
  })).sort((a, b) => (b.lastBookingDate || '').localeCompare(a.lastBookingDate || '') || a.canonicalCustomerId.localeCompare(b.canonicalCustomerId));

  const conflictList = [...conflicts.values()];
  const affectedBookingIds = new Set(suspiciousLineIdentities.flatMap(item => item.bookingIds));
  return {
    ok: true, dryRun: true, writesPerformed: 0,
    ruleVersion: CUSTOMER_IDENTITY_RULE_VERSION,
    generatedAt: generatedAt || new Date().toISOString(),
    sourceCounts: { bookings: bookingRecords.length, registeredUsers: userRecords.length, packages: packageRecords.length },
    summary: {
      canonicalProfiles: derivedProfiles.length,
      proposedLinks: proposedLinks.length,
      identityConflicts: conflictList.length,
      suspiciousLineIdentities: suspiciousLineIdentities.length,
      hardBlockedLineIdentities: suspiciousLineIdentities.filter(item => item.severity === 'hard_block').length,
      affectedBookings: affectedBookingIds.size,
      packageConflicts: packageConflicts.length,
      passReviews: passReview.length,
      unresolvedRecords: unresolvedRecords.length,
    },
    proposedLinks, conflicts: conflictList, suspiciousLineIdentities, packageConflicts, passReview, unresolvedRecords,
    derivedProfiles,
  };
}
