// Customer identity dry-run engine.
//
// This module is intentionally pure: it receives already-read documents and
// returns a proposal/report. It never imports Firestore and never writes data.

export const CUSTOMER_IDENTITY_RULE_VERSION = 'customer-identity-v1';

export function normalizeIdentityPhone(value) {
  let digits = String(value || '').replace(/\D/g, '');
  if (digits.startsWith('66') && digits.length === 11) digits = `0${digits.slice(2)}`;
  if (!/^0\d{8,9}$/.test(digits)) return '';
  return digits;
}

export function usableIdentityLineId(value) {
  const id = String(value || '').trim();
  return id && id !== 'guest' && id !== 'manual' ? id : '';
}

function sorted(values) {
  return [...values].sort((a, b) => String(a).localeCompare(String(b)));
}

function sourceRecord(record, source) {
  const id = String(record?.id || 'unknown');
  const lineUserId = usableIdentityLineId(record?.lineUserId)
    || (source === 'registered_user' ? usableIdentityLineId(id) : '');
  const phone = normalizeIdentityPhone(
    record?.phoneNormalized || record?.customerPhoneNormalized || record?.phone || record?.customerPhone,
  );
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
      if (owners.size === 1) return `line:${sorted(owners)[0]}`;
      return `phone:${record.phone}`;
    }
    return `record:${record.source}:${record.id}`;
  };

  const allRecords = [...userRecords, ...bookingRecords, ...packageRecords];
  const canonicalByRecord = new Map();
  for (const record of allRecords) canonicalByRecord.set(`${record.source}:${record.id}`, resolveCanonical(record));

  const proposalMap = new Map();
  for (const record of [...bookingRecords, ...packageRecords]) {
    if (record.lineUserId || !record.phone) continue;
    const owners = linesByPhone.get(record.phone) || new Set();
    if (owners.size !== 1) continue;
    const canonicalCustomerId = `line:${sorted(owners)[0]}`;
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

  const proposedCanonicalIds = new Set(proposedLinks.map(item => item.canonicalCustomerId));
  const passReview = [...profiles.values()]
    .filter(profile => proposedCanonicalIds.has(profile.canonicalCustomerId) && profile.packageIds.size > 1)
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
  return {
    ok: true, dryRun: true, writesPerformed: 0,
    ruleVersion: CUSTOMER_IDENTITY_RULE_VERSION,
    generatedAt: generatedAt || new Date().toISOString(),
    sourceCounts: { bookings: bookingRecords.length, registeredUsers: userRecords.length, packages: packageRecords.length },
    summary: {
      canonicalProfiles: derivedProfiles.length,
      proposedLinks: proposedLinks.length,
      identityConflicts: conflictList.length,
      packageConflicts: packageConflicts.length,
      passReviews: passReview.length,
      unresolvedRecords: unresolvedRecords.length,
    },
    proposedLinks, conflicts: conflictList, packageConflicts, passReview, unresolvedRecords,
    derivedProfiles,
  };
}
