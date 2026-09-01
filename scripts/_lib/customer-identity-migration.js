import { createHash, randomUUID } from 'node:crypto';
import {
  buildCustomerIdentityDryRun,
  normalizeIdentityPhone,
  usableIdentityLineId,
} from '../../api/_lib/customer-identity.js';

export const MIGRATION_VERSION = 'customer-identity-v1-phase-2a';
export const CANONICAL_COLLECTION = 'canonical_customers';
export const ALIAS_COLLECTION = 'customer_identity_aliases';
export const BOOKING_COLLECTION = 'bookings';

const FORBIDDEN_BOOKING_FIELDS = new Set([
  'customerId', 'lineUserId', 'lineDisplayName', 'name', 'customerName',
  'phone', 'customerPhone', 'phoneNormalized', 'customerPhoneNormalized',
]);

export class MigrationPlanError extends Error {
  constructor(code, message, details = null) {
    super(message);
    this.name = 'MigrationPlanError';
    this.code = code;
    this.details = details;
  }
}

function fail(code, message, details) {
  throw new MigrationPlanError(code, message, details);
}

export function stableValue(value) {
  if (Array.isArray(value)) return value.map(stableValue);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, stableValue(value[key])]));
  }
  return value;
}

export function stableStringify(value) {
  return JSON.stringify(stableValue(value));
}

export function sha256(value) {
  return createHash('sha256').update(typeof value === 'string' ? value : stableStringify(value)).digest('hex');
}

export function sourceSnapshotDigest(snapshot) {
  return sha256({
    branchId: String(snapshot?.branchId || ''),
    bookings: [...(snapshot?.bookings || [])].sort(byId),
    users: [...(snapshot?.users || [])].sort(byId),
    packages: [...(snapshot?.packages || [])].sort(byId),
  });
}

function byId(a, b) {
  return String(a?.id || '').localeCompare(String(b?.id || ''));
}

function sortedUnique(values) {
  return [...new Set(values.filter(Boolean).map(String))].sort();
}

function phoneOf(record) {
  return [record?.phoneNormalized, record?.customerPhoneNormalized, record?.phone, record?.customerPhone]
    .map(normalizeIdentityPhone).find(Boolean) || '';
}

function lineOf(record, source = '') {
  return usableIdentityLineId(record?.lineUserId)
    || (source === 'registered_user' ? usableIdentityLineId(record?.id) : '');
}

function aliasDocumentId(aliasType, value) {
  return `ia_${sha256(`${aliasType}:${value}`).slice(0, 32)}`;
}

function makeCanonicalId() {
  return `cc_${randomUUID().replaceAll('-', '')}`;
}

function makeAssignmentKey(kind, lineUserId = '', phones = [], bookingId = '') {
  return sha256({ kind, lineUserId, phones: sortedUnique(phones), bookingId });
}

function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}

function assertSnapshot(snapshot) {
  if (!snapshot || !Array.isArray(snapshot.bookings) || !Array.isArray(snapshot.users) || !Array.isArray(snapshot.packages)) {
    fail('INVALID_SOURCE_SNAPSHOT', 'Snapshot must contain bookings, users, and packages arrays.');
  }
  for (const [collection, records] of Object.entries({ bookings: snapshot.bookings, users: snapshot.users, packages: snapshot.packages })) {
    const ids = new Set();
    for (const record of records) {
      const id = String(record?.id || '');
      if (!id) fail('MISSING_SOURCE_DOCUMENT_ID', `${collection} contains a record without an id.`);
      if (ids.has(id)) fail('DUPLICATE_SOURCE_DOCUMENT_ID', `${collection}/${id} appears more than once.`);
      ids.add(id);
    }
  }
}

function buildRegisteredPhoneOwners(users) {
  const owners = new Map();
  for (const user of users) {
    const phone = phoneOf(user);
    const line = lineOf(user, 'registered_user');
    if (!phone || !line) continue;
    if (!owners.has(phone)) owners.set(phone, new Set());
    owners.get(phone).add(line);
  }
  return owners;
}

function assertApprovedLink(link, bookingById, owners) {
  const phone = normalizeIdentityPhone(link.phone);
  const lineUserId = usableIdentityLineId(link.lineUserId);
  if (!phone || !lineUserId) fail('INVALID_APPROVED_ALIAS', 'Approved phone/LINE mapping is invalid.', link);
  const registeredOwners = [...(owners.get(phone) || [])];
  if (registeredOwners.length > 1 || (registeredOwners.length === 1 && registeredOwners[0] !== lineUserId)) {
    fail('APPROVED_ALIAS_OWNER_CONFLICT', `Approved phone ${phone} conflicts with its registered owner.`, { link, registeredOwners });
  }
  for (const bookingId of link.bookingIds || []) {
    const booking = bookingById.get(String(bookingId));
    if (!booking) fail('APPROVED_BOOKING_MISSING', `Approved booking ${bookingId} is absent from the snapshot.`, link);
    const bookingPhone = phoneOf(booking);
    const bookingLine = lineOf(booking);
    if (bookingPhone && bookingPhone !== phone) {
      fail('APPROVED_BOOKING_PHONE_CONFLICT', `Booking ${bookingId} has an unexpected phone.`, { expected: phone, actual: bookingPhone });
    }
    if (bookingLine && bookingLine !== lineUserId) {
      fail('APPROVED_BOOKING_LINE_CONFLICT', `Booking ${bookingId} has an unexpected LINE identity.`, { expected: lineUserId, actual: bookingLine });
    }
  }
  return { phone, lineUserId };
}

function assertPhoneOwnerAllows(phone, lineUserId, owners, subject) {
  const registeredOwners = [...(owners.get(phone) || [])];
  if (registeredOwners.length > 1 || (registeredOwners.length === 1 && registeredOwners[0] !== lineUserId)) {
    fail('APPROVED_ALIAS_OWNER_CONFLICT', `${subject} phone ${phone} conflicts with its registered owner.`, { registeredOwners, lineUserId });
  }
}

function createAssignment(assignments, key, idFactory) {
  const existing = assignments[key];
  if (existing) {
    if (!/^cc_[0-9a-f]{32}$/i.test(existing)) fail('INVALID_CANONICAL_ASSIGNMENT', `Invalid canonical id stored for ${key}.`);
    return existing;
  }
  const canonicalCustomerId = idFactory();
  if (!/^cc_[0-9a-f]{32}$/i.test(canonicalCustomerId)) fail('INVALID_GENERATED_CANONICAL_ID', 'Canonical id factory returned an invalid id.');
  if (Object.values(assignments).includes(canonicalCustomerId)) fail('DUPLICATE_CANONICAL_ASSIGNMENT', 'Canonical id factory returned an existing id.');
  assignments[key] = canonicalCustomerId;
  return canonicalCustomerId;
}

function canonicalMutation(canonicalCustomerId, logicalKey, evidence, context) {
  const after = {
    canonicalCustomerId,
    status: 'active',
    identityModel: 'alias_soft_link_v1',
    migrationVersion: context.migrationVersion,
    migrationRunId: context.migrationRunId,
    migrationPlanHash: '__PLAN_HASH__',
    createdAt: '__SERVER_TIMESTAMP__',
    evidence,
  };
  return {
    targetCollection: CANONICAL_COLLECTION,
    targetDocumentId: canonicalCustomerId,
    operation: 'create',
    before: null,
    after,
    reason: `Create immutable random canonical identity for ${logicalKey}.`,
    rollback: { operation: 'delete_created_document', onlyIfMigrationRunId: context.migrationRunId },
  };
}

function aliasMutation({ aliasType, value, canonicalCustomerId, usableForMatching, usableForContact, reason }, context) {
  const targetDocumentId = aliasDocumentId(aliasType, value);
  return {
    targetCollection: ALIAS_COLLECTION,
    targetDocumentId,
    operation: 'create',
    before: null,
    after: {
      aliasType,
      value,
      canonicalCustomerId,
      source: aliasType === 'contaminated_phone_evidence' ? 'phase_2a_contaminated_record_evidence' : 'phase_2a_approved_mapping',
      usableForMatching: Boolean(usableForMatching),
      usableForContact: Boolean(usableForContact),
      migrationVersion: context.migrationVersion,
      migrationRunId: context.migrationRunId,
      migrationPlanHash: '__PLAN_HASH__',
      createdAt: '__SERVER_TIMESTAMP__',
    },
    reason,
    rollback: { operation: 'delete_created_document', onlyIfMigrationRunId: context.migrationRunId },
  };
}

function bookingMutation(booking, canonicalCustomerId, reason, context) {
  const before = {};
  const after = {
    canonicalCustomerId,
    canonicalLinkedBy: context.migrationVersion,
    canonicalMigrationRunId: context.migrationRunId,
    canonicalMigrationPlanHash: '__PLAN_HASH__',
  };
  for (const field of Object.keys(after)) before[field] = Object.hasOwn(booking, field) ? clone(booking[field]) : '__ABSENT__';
  return {
    targetCollection: BOOKING_COLLECTION,
    targetDocumentId: String(booking.id),
    operation: 'update_fields',
    before,
    after,
    reason,
    rollback: {
      operation: 'restore_fields',
      restore: Object.fromEntries(Object.entries(before).filter(([, value]) => value !== '__ABSENT__')),
      remove: Object.entries(before).filter(([, value]) => value === '__ABSENT__').map(([field]) => field),
      onlyIfMigrationRunId: context.migrationRunId,
    },
  };
}

function addAlias(list, seenValues, alias, context) {
  const key = `${alias.aliasType}:${alias.value}`;
  const previous = seenValues.get(key);
  if (previous && previous !== alias.canonicalCustomerId) {
    fail('ALIAS_TARGET_CONFLICT', `Alias ${key} points at more than one canonical identity.`);
  }
  if (!previous) {
    seenValues.set(key, alias.canonicalCustomerId);
    list.push(aliasMutation(alias, context));
  }
}

function addBookingLink(list, linkedBookings, booking, canonicalCustomerId, reason, context) {
  const bookingId = String(booking.id);
  const previous = linkedBookings.get(bookingId);
  if (previous && previous !== canonicalCustomerId) {
    fail('BOOKING_TARGET_CONFLICT', `Booking ${bookingId} is planned for two canonical identities.`);
  }
  if (!previous) {
    linkedBookings.set(bookingId, canonicalCustomerId);
    list.push(bookingMutation(booking, canonicalCustomerId, reason, context));
  }
}

function matchingBookings(snapshot, lineUserId, phones, explicitBookingIds = []) {
  const explicit = new Set(explicitBookingIds.map(String));
  const phoneSet = new Set(phones.map(normalizeIdentityPhone).filter(Boolean));
  return snapshot.bookings.filter(booking => explicit.has(String(booking.id)) || lineOf(booking) === lineUserId || phoneSet.has(phoneOf(booking)));
}

function assertKnownPackages(packageIds, packageById, subject) {
  for (const packageId of packageIds || []) {
    if (!packageById.has(String(packageId))) {
      fail('APPROVED_PACKAGE_MISSING', `Expected untouched package ${packageId} for ${subject} is absent from the snapshot.`);
    }
  }
}

function assertKnownBookings(bookingIds, bookingById, subject) {
  for (const bookingId of bookingIds || []) {
    if (!bookingById.has(String(bookingId))) {
      fail('APPROVED_BOOKING_MISSING', `Expected booking ${bookingId} for ${subject} is absent from the snapshot.`);
    }
  }
}

function decoratePlanHash(plan, planHash) {
  const result = clone(plan);
  result.planHash = planHash;
  for (const mutation of [...result.canonicalCustomersToCreate, ...result.aliasesToCreate, ...result.bookingSoftLinksToAdd]) {
    for (const [key, value] of Object.entries(mutation.after || {})) {
      if (value === '__PLAN_HASH__') mutation.after[key] = planHash;
    }
  }
  return result;
}

function materialForPlanHash(plan) {
  const copy = clone(plan);
  delete copy.planHash;
  for (const mutation of [...(copy.canonicalCustomersToCreate || []), ...(copy.aliasesToCreate || []), ...(copy.bookingSoftLinksToAdd || [])]) {
    for (const key of Object.keys(mutation.after || {})) {
      if (/PlanHash$/i.test(key)) mutation.after[key] = '__PLAN_HASH__';
    }
  }
  return copy;
}

export function calculatePlanHash(plan) {
  return sha256(materialForPlanHash(plan));
}

export function validateApprovalsAgainstDryRunReport(approvals, report) {
  if (!report?.dryRun || Number(report.writesPerformed) !== 0) fail('INVALID_DRY_RUN_REPORT', 'Evidence report must be a zero-write Production dry-run.');
  if (report.ruleVersion !== 'customer-identity-v1-safety-2') fail('STALE_SAFETY_REPORT', 'Evidence report must use Customer Identity safety patch #2.');
  if (approvals?.migrationVersion !== MIGRATION_VERSION) fail('MIGRATION_VERSION_MISMATCH', `Approvals must use ${MIGRATION_VERSION}.`);
  if (approvals.sourceEvidence) {
    if (sha256(report) !== approvals.sourceEvidence.reportDigest) fail('APPROVAL_EVIDENCE_REPORT_CHANGED', 'The dry-run report does not match the report Art reviewed.');
    if (stableStringify(report.sourceCounts) !== stableStringify(approvals.sourceEvidence.sourceCounts)) {
      fail('APPROVAL_EVIDENCE_COUNTS_CHANGED', 'The dry-run source counts do not match the reviewed evidence.');
    }
  }
  const proposals = new Map((report.proposedLinks || []).map(item => {
    const phone = normalizeIdentityPhone(item.phone);
    const lineUserId = usableIdentityLineId(String(item.canonicalCustomerId || '').replace(/^line:/, ''));
    return [`${phone}|${lineUserId}`, item];
  }));
  const approvedKeys = new Set();
  for (const approved of approvals.approvedAutoLinks || []) {
    const key = `${normalizeIdentityPhone(approved.phone)}|${usableIdentityLineId(approved.lineUserId)}`;
    const proposal = proposals.get(key);
    if (!proposal) fail('APPROVED_EVIDENCE_MISSING', `Current dry-run no longer proposes ${key}.`);
    const missingBookings = (approved.bookingIds || []).filter(id => !(proposal.bookingIds || []).includes(id));
    if (missingBookings.length) fail('APPROVED_BOOKING_MISSING', `Approved proposal ${key} lost booking evidence.`, { missingBookings });
    approvedKeys.add(key);
  }
  const unexpectedProposals = [...proposals.keys()].filter(key => !approvedKeys.has(key));
  if (unexpectedProposals.length) fail('UNREVIEWED_AUTO_LINK_PROPOSAL', 'Current dry-run contains new proposals that have not been approved.', { unexpectedProposals });

  const suspiciousByLine = new Map((report.suspiciousLineIdentities || []).map(item => [item.lineUserId, item]));
  const assertSuspicious = (subject, mapping, expectedPhones, expectedSeverity = 'manual_review') => {
    const evidence = suspiciousByLine.get(mapping.lineUserId);
    if (!evidence) fail('HUMAN_REVIEW_EVIDENCE_MISSING', `${subject} is absent from current manual-review evidence.`);
    if (evidence.severity !== expectedSeverity) fail('HUMAN_REVIEW_SEVERITY_CHANGED', `${subject} review severity changed.`, { expectedSeverity, actual: evidence.severity });
    if (stableStringify(sortedUnique(evidence.phones)) !== stableStringify(sortedUnique(expectedPhones))) {
      fail('HUMAN_REVIEW_PHONE_SET_CHANGED', `${subject} phone evidence changed.`, { expected: sortedUnique(expectedPhones), actual: sortedUnique(evidence.phones) });
    }
    const missingBookings = (mapping.bookingIds || []).filter(id => !(evidence.bookingIds || []).includes(id));
    if (missingBookings.length) fail('APPROVED_BOOKING_MISSING', `${subject} lost reviewed booking evidence.`, { missingBookings });
    const missingPackages = (mapping.packageIds || []).filter(id => !(evidence.packageIds || []).includes(id));
    if (missingPackages.length) fail('APPROVED_PACKAGE_MISSING', `${subject} lost reviewed package evidence.`, { missingPackages });
    return evidence;
  };

  const human = approvals.approvedHumanMappings || {};
  if (human.art) assertSuspicious('art', human.art, [human.art.correctPhone, human.art.typoPhone]);
  if (human.noppon) assertSuspicious('noppon', human.noppon, human.noppon.phones);
  for (const [subject, mapping] of Object.entries(approvals.manualReviewPreserved || {})) {
    assertSuspicious(subject, mapping, mapping.phones);
  }
  const contaminated = approvals.contaminatedLegacyLine;
  if (contaminated) {
    const evidence = assertSuspicious('contaminated legacy LINE', contaminated, [contaminated.adminPhone, ...(contaminated.isolatedPhones || [])], 'hard_block');
    if (stableStringify(sortedUnique(evidence.bookingIds)) !== stableStringify(sortedUnique(contaminated.bookingIds || []))) {
      fail('CONTAMINATED_BOOKING_SET_CHANGED', 'The current contaminated booking set differs from the reviewed set.');
    }
  }

  const allPackageIds = new Set((report.derivedProfiles || []).flatMap(profile => profile.packageIds || []).map(String));
  for (const [subject, mapping] of Object.entries(human)) {
    for (const packageId of mapping.packageIds || []) {
      if (!allPackageIds.has(String(packageId))) fail('APPROVED_PACKAGE_MISSING', `${subject} package ${packageId} is absent from the current report.`);
    }
  }
  return {
    ok: true,
    reportDigest: sha256(report),
    generatedAt: report.generatedAt,
    branchId: report.branchId,
    sourceCounts: clone(report.sourceCounts),
    approvedAutoLinksVerified: approvedKeys.size,
    suspiciousDecisionsVerified: (human.art ? 1 : 0) + (human.noppon ? 1 : 0) + Object.keys(approvals.manualReviewPreserved || {}).length + (contaminated ? 1 : 0),
    packageDocumentsObserved: allPackageIds.size,
    writesPerformed: 0,
  };
}

export function buildMigrationPlan({ snapshot, approvals, assignments = {}, idFactory = makeCanonicalId } = {}) {
  assertSnapshot(snapshot);
  if (approvals?.migrationVersion !== MIGRATION_VERSION) {
    fail('MIGRATION_VERSION_MISMATCH', `Approvals must use ${MIGRATION_VERSION}.`);
  }
  if (approvals.sourceEvidence) {
    const actualCounts = { bookings: snapshot.bookings.length, registeredUsers: snapshot.users.length, packages: snapshot.packages.length };
    if (stableStringify(actualCounts) !== stableStringify(approvals.sourceEvidence.sourceCounts)) {
      fail('SOURCE_COUNTS_CHANGED_AFTER_REVIEW', 'Production counts changed after the approved dry-run report; generate and review a new report first.', { expected: approvals.sourceEvidence.sourceCounts, actual: actualCounts });
    }
  }
  const updatedAssignments = { ...assignments };
  const snapshotDigest = sourceSnapshotDigest(snapshot);
  const generatedAt = snapshot.capturedAt || new Date().toISOString();
  const migrationRunId = `mir_${snapshotDigest.slice(0, 24)}`;
  const context = { migrationVersion: MIGRATION_VERSION, migrationRunId };
  const bookingById = new Map(snapshot.bookings.map(record => [String(record.id), record]));
  const packageById = new Map(snapshot.packages.map(record => [String(record.id), record]));
  const owners = buildRegisteredPhoneOwners(snapshot.users);
  const safetyReport = buildCustomerIdentityDryRun({
    bookings: snapshot.bookings,
    users: snapshot.users,
    packages: snapshot.packages,
    generatedAt,
  });

  const canonicalCustomersToCreate = [];
  const aliasesToCreate = [];
  const bookingSoftLinksToAdd = [];
  const aliasTargets = new Map();
  const linkedBookings = new Map();
  const canonicalByLine = new Map();
  const approvedAutoLinks = [];
  const approvedHumanMappings = [];
  const contaminatedLineSplits = [];
  const typoCorrections = [];
  const recordsSkipped = [];

  const createGroup = ({ kind, lineUserId, phones, names = [], bookingIds = [], evidence = {}, reason, explicitOnly = false }) => {
    const key = makeAssignmentKey(kind, lineUserId, phones, kind === 'isolated_booking' ? bookingIds[0] : '');
    const canonicalCustomerId = createAssignment(updatedAssignments, key, idFactory);
    if (lineUserId) {
      const prior = canonicalByLine.get(lineUserId);
      if (prior && prior !== canonicalCustomerId) fail('LINE_TARGET_CONFLICT', `LINE ${lineUserId} is assigned twice.`);
      canonicalByLine.set(lineUserId, canonicalCustomerId);
    }
    canonicalCustomersToCreate.push(canonicalMutation(canonicalCustomerId, kind, {
      lineUserId: lineUserId || null,
      phones: sortedUnique(phones),
      names: sortedUnique(names),
      ...evidence,
    }, context));
    const groupBookings = explicitOnly
      ? bookingIds.map(id => bookingById.get(String(id))).filter(Boolean)
      : matchingBookings(snapshot, lineUserId, phones, bookingIds);
    for (const booking of groupBookings) {
      const actualLine = lineOf(booking);
      if (lineUserId && actualLine && actualLine !== lineUserId) {
        fail('GROUP_BOOKING_LINE_CONFLICT', `Booking ${booking.id} has a different usable LINE identity.`, { expected: lineUserId, actual: actualLine });
      }
      addBookingLink(bookingSoftLinksToAdd, linkedBookings, booking, canonicalCustomerId, reason, context);
    }
    return canonicalCustomerId;
  };

  for (const approved of approvals.approvedAutoLinks || []) {
    const { phone, lineUserId } = assertApprovedLink(approved, bookingById, owners);
    const suspicious = safetyReport.suspiciousLineIdentities.find(item => item.lineUserId === lineUserId);
    if (suspicious?.severity === 'hard_block') {
      fail('AUTO_LINK_HARD_BLOCKED', `Approved auto-link ${lineUserId} remains hard-blocked and needs an explicit human mapping.`);
    }
    const canonicalCustomerId = createGroup({
      kind: 'approved_auto_link', lineUserId, phones: [phone], names: approved.names,
      bookingIds: approved.bookingIds, evidence: { approvalType: 'human_approved_auto_link' },
      reason: 'Human-approved phone-to-LINE identity soft-link.',
    });
    addAlias(aliasesToCreate, aliasTargets, {
      aliasType: 'line_user_id', value: lineUserId, canonicalCustomerId,
      usableForMatching: true, usableForContact: true, reason: 'Approved LINE identity alias.',
    }, context);
    addAlias(aliasesToCreate, aliasTargets, {
      aliasType: 'phone', value: phone, canonicalCustomerId,
      usableForMatching: true, usableForContact: true, reason: 'Approved normalized phone alias.',
    }, context);
    approvedAutoLinks.push({ phone, lineUserId, canonicalCustomerId, bookingIds: sortedUnique(approved.bookingIds || []) });
  }

  const human = approvals.approvedHumanMappings || {};
  const art = human.art;
  if (art) {
    assertKnownBookings(art.bookingIds, bookingById, 'art');
    assertKnownPackages(art.packageIds, packageById, 'art');
    const correctPhone = normalizeIdentityPhone(art.correctPhone);
    const typoPhone = normalizeIdentityPhone(art.typoPhone);
    const lineUserId = usableIdentityLineId(art.lineUserId);
    if (!correctPhone || !typoPhone || !lineUserId || correctPhone === typoPhone) fail('INVALID_ART_MAPPING', 'Art mapping is incomplete.');
    assertPhoneOwnerAllows(correctPhone, lineUserId, owners, 'Art');
    const canonicalCustomerId = createGroup({
      kind: 'approved_human_art_typo', lineUserId, phones: [correctPhone, typoPhone], names: art.names,
      bookingIds: art.bookingIds, evidence: { contactPhone: correctPhone, typoPhone, packageIdsUntouched: art.packageIds || [] }, reason: 'Explicit human approval joins Art records while preserving the typo as non-usable evidence.',
    });
    addAlias(aliasesToCreate, aliasTargets, { aliasType: 'line_user_id', value: lineUserId, canonicalCustomerId, usableForMatching: true, usableForContact: true, reason: 'Approved Art LINE identity.' }, context);
    addAlias(aliasesToCreate, aliasTargets, { aliasType: 'phone', value: correctPhone, canonicalCustomerId, usableForMatching: true, usableForContact: true, reason: 'Approved corrected Art phone.' }, context);
    addAlias(aliasesToCreate, aliasTargets, { aliasType: 'typo_phone', value: typoPhone, canonicalCustomerId, usableForMatching: false, usableForContact: false, reason: 'Historical typo retained only for audit; never match or contact.' }, context);
    typoCorrections.push({ subject: 'art', typoPhone, correctPhone, canonicalCustomerId, destructiveOverwrite: false });
    approvedHumanMappings.push({ subject: 'art', canonicalCustomerId, contactPhone: correctPhone, decision: 'same_person_with_nonusable_typo_alias', packageIdsUntouched: clone(art.packageIds || []) });
  }

  const noppon = human.noppon;
  if (noppon) {
    assertKnownBookings(noppon.bookingIds, bookingById, 'noppon');
    assertKnownPackages(noppon.packageIds, packageById, 'noppon');
    const phones = sortedUnique((noppon.phones || []).map(normalizeIdentityPhone));
    const lineUserId = usableIdentityLineId(noppon.lineUserId);
    if (phones.length !== 2 || !lineUserId) fail('INVALID_NOPPON_MAPPING', 'Noppon mapping must contain one LINE identity and two phones.');
    for (const phone of phones) assertPhoneOwnerAllows(phone, lineUserId, owners, 'Noppon');
    const canonicalCustomerId = createGroup({
      kind: 'approved_human_noppon_two_phones', lineUserId, phones, names: noppon.names,
      bookingIds: noppon.bookingIds, evidence: { primaryPhone: null, packageIdsUntouched: noppon.packageIds },
      reason: 'Explicit human approval joins both Noppon phone histories without selecting a primary phone.',
    });
    addAlias(aliasesToCreate, aliasTargets, { aliasType: 'line_user_id', value: lineUserId, canonicalCustomerId, usableForMatching: true, usableForContact: true, reason: 'Approved Noppon LINE identity.' }, context);
    for (const phone of phones) addAlias(aliasesToCreate, aliasTargets, { aliasType: 'phone', value: phone, canonicalCustomerId, usableForMatching: true, usableForContact: true, reason: 'Approved Noppon phone; no primary phone is selected.' }, context);
    approvedHumanMappings.push({ subject: 'noppon', canonicalCustomerId, decision: 'same_person_two_phones_no_primary', packageIdsUntouched: clone(noppon.packageIds) });
  }

  const paksada = human.paksada;
  if (paksada) {
    assertKnownPackages(paksada.packageIds, packageById, 'paksada');
    const phones = sortedUnique((paksada.phones || []).map(normalizeIdentityPhone));
    const lineUserId = usableIdentityLineId(paksada.lineUserId);
    if (phones.length !== 1 || !lineUserId) fail('INVALID_PAKSADA_MAPPING', 'PAKSADA mapping is incomplete.');
    assertPhoneOwnerAllows(phones[0], lineUserId, owners, 'PAKSADA');
    const canonicalCustomerId = createGroup({
      kind: 'approved_human_paksada', lineUserId, phones, names: paksada.names,
      evidence: { packageIdsUntouched: paksada.packageIds }, reason: 'Explicit human approval links PAKSADA identity while keeping package documents distinct.',
    });
    addAlias(aliasesToCreate, aliasTargets, { aliasType: 'line_user_id', value: lineUserId, canonicalCustomerId, usableForMatching: true, usableForContact: true, reason: 'Approved PAKSADA LINE identity.' }, context);
    addAlias(aliasesToCreate, aliasTargets, { aliasType: 'phone', value: phones[0], canonicalCustomerId, usableForMatching: true, usableForContact: true, reason: 'Approved PAKSADA phone.' }, context);
    approvedHumanMappings.push({ subject: 'paksada', canonicalCustomerId, decision: 'same_person_distinct_packages', packageIdsUntouched: clone(paksada.packageIds) });
  }

  const contaminated = approvals.contaminatedLegacyLine;
  if (contaminated) {
    assertKnownBookings(contaminated.bookingIds, bookingById, 'contaminated legacy LINE');
    assertKnownPackages(contaminated.packageIds, packageById, 'contaminated legacy LINE');
    const contaminatedLine = usableIdentityLineId(contaminated.lineUserId);
    const adminPhone = normalizeIdentityPhone(contaminated.adminPhone);
    const isolatedPhones = new Set((contaminated.isolatedPhones || []).map(normalizeIdentityPhone));
    const affected = snapshot.bookings.filter(booking => lineOf(booking) === contaminatedLine);
    const expectedAffected = sortedUnique(contaminated.bookingIds || []);
    const actualAffected = sortedUnique(affected.map(booking => booking.id));
    if (stableStringify(expectedAffected) !== stableStringify(actualAffected)) {
      fail('CONTAMINATED_BOOKING_SET_CHANGED', 'The contaminated LINE booking set changed after human review.', { expectedAffected, actualAffected });
    }
    for (const booking of affected) {
      const phone = phoneOf(booking);
      const canonicalCustomerId = createGroup({
        kind: 'isolated_booking', lineUserId: '', phones: phone ? [phone] : [], names: [booking.customerName || booking.name],
        bookingIds: [String(booking.id)], evidence: { contaminatedLine, isolatedRecord: true },
        reason: 'Contaminated shared LINE: isolate this booking into its own manual-review canonical record.', explicitOnly: true,
      });
      if (phone && phone !== adminPhone) {
        addAlias(aliasesToCreate, aliasTargets, {
          aliasType: 'contaminated_phone_evidence', value: phone, canonicalCustomerId,
          usableForMatching: false, usableForContact: isolatedPhones.has(phone),
          reason: 'Record-local evidence only; never eligible for automatic identity matching.',
        }, context);
      }
      if (phone === adminPhone) recordsSkipped.push({ source: 'alias', recordId: String(booking.id), reason: 'Contaminated admin phone is isolated but never made an alias.' });
      contaminatedLineSplits.push({ bookingId: String(booking.id), phone: phone || null, canonicalCustomerId, contaminatedLineAliasCreated: false });
    }
  }

  const preserved = approvals.manualReviewPreserved || {};
  for (const [subject, value] of Object.entries(preserved)) assertKnownBookings(value.bookingIds, bookingById, subject);
  const manualReviewPreserved = Object.entries(preserved).map(([subject, value]) => ({ subject, ...clone(value), migrationAction: 'none' }));
  const preservedLines = new Set(manualReviewPreserved.map(item => usableIdentityLineId(item.lineUserId)).filter(Boolean));
  for (const booking of snapshot.bookings) {
    if (preservedLines.has(lineOf(booking))) recordsSkipped.push({ source: 'booking', recordId: String(booking.id), reason: 'Explicitly preserved for manual review; no canonical link planned.' });
  }

  const hardBlockedRecords = safetyReport.suspiciousLineIdentities
    .filter(item => item.severity === 'hard_block')
    .map(item => ({ ...clone(item), migrationAction: item.lineUserId === contaminated?.lineUserId ? 'isolated_record_split' : 'none' }));
  const packageDocumentsUntouched = [...snapshot.packages].sort(byId).map(pkg => ({
    packageId: String(pkg.id), operation: 'none', reason: 'Phase 2A never mutates, merges, deduplicates, or recalculates package documents.',
  }));

  const sourceSnapshot = {
    capturedAt: snapshot.capturedAt || null,
    branchId: snapshot.branchId || approvals.branchId || null,
    digest: snapshotDigest,
    counts: { bookings: snapshot.bookings.length, registeredUsers: snapshot.users.length, packages: snapshot.packages.length },
  };
  const planBase = {
    ok: true,
    mode: 'plan',
    dryRun: true,
    migrationVersion: MIGRATION_VERSION,
    generatedAt,
    migrationRunId,
    sourceSnapshot,
    planHash: null,
    canonicalCustomersToCreate,
    aliasesToCreate,
    bookingSoftLinksToAdd,
    recordsSkipped,
    manualReviewPreserved,
    hardBlockedRecords,
    packageDocumentsUntouched,
    approvedAutoLinks,
    approvedHumanMappings,
    contaminatedLineSplits,
    typoCorrections,
    safetyReportSummary: clone(safetyReport.summary),
    canonicalAssignmentRegistryDigest: sha256(updatedAssignments),
    writeCountPlanned: canonicalCustomersToCreate.length + aliasesToCreate.length + bookingSoftLinksToAdd.length,
    writesPerformed: 0,
  };
  const planHash = calculatePlanHash(planBase);
  const plan = decoratePlanHash(planBase, planHash);
  validateMigrationPlan(plan, { snapshot });
  return { plan, assignments: updatedAssignments };
}

export function validateMigrationPlan(plan, { snapshot = null } = {}) {
  if (plan?.migrationVersion !== MIGRATION_VERSION) fail('INVALID_PLAN_VERSION', 'Unexpected migration plan version.');
  if (plan.writesPerformed !== 0 || plan.mode !== 'plan' || plan.dryRun !== true) fail('PLAN_NOT_READ_ONLY', 'Phase 2A plan must be a zero-write dry run.');
  if (calculatePlanHash(plan) !== plan.planHash) fail('PLAN_HASH_MISMATCH', 'Migration plan hash does not match its contents.');
  if (snapshot && sourceSnapshotDigest(snapshot) !== plan.sourceSnapshot?.digest) fail('STALE_MIGRATION_PLAN', 'Current source data no longer matches the planned snapshot.');

  const mutations = [...(plan.canonicalCustomersToCreate || []), ...(plan.aliasesToCreate || []), ...(plan.bookingSoftLinksToAdd || [])];
  const targets = new Set();
  for (const mutation of mutations) {
    const key = `${mutation.targetCollection}/${mutation.targetDocumentId}`;
    if (targets.has(key)) fail('DUPLICATE_MUTATION_TARGET', `Plan mutates ${key} more than once.`);
    targets.add(key);
    if (mutation.targetCollection === 'customer_packages') fail('PACKAGE_MUTATION_FORBIDDEN', 'Package mutation is forbidden in Phase 2A.');
    if (!['create', 'update_fields'].includes(mutation.operation)) fail('DESTRUCTIVE_OPERATION_FORBIDDEN', `Operation ${mutation.operation} is forbidden.`);
    if (mutation.targetCollection === BOOKING_COLLECTION) {
      if (mutation.operation !== 'update_fields') fail('BOOKING_CREATE_FORBIDDEN', 'Existing bookings may only receive additive soft-link fields.');
      for (const field of Object.keys(mutation.after || {})) {
        if (FORBIDDEN_BOOKING_FIELDS.has(field)) fail('HISTORICAL_IDENTITY_OVERWRITE', `Plan attempts to overwrite booking.${field}.`);
      }
    }
  }
  const aliasTargets = new Map();
  for (const mutation of plan.aliasesToCreate || []) {
    const key = `${mutation.after.aliasType}:${mutation.after.value}`;
    const previous = aliasTargets.get(key);
    if (previous && previous !== mutation.after.canonicalCustomerId) fail('ALIAS_TARGET_CONFLICT', `Alias ${key} has multiple targets.`);
    aliasTargets.set(key, mutation.after.canonicalCustomerId);
  }
  const canonicalIds = new Set((plan.canonicalCustomersToCreate || []).map(item => item.targetDocumentId));
  for (const mutation of plan.canonicalCustomersToCreate || []) {
    if (mutation.after?.redirectTo || mutation.after?.canonicalCustomerId !== mutation.targetDocumentId) {
      fail('CANONICAL_REDIRECT_FORBIDDEN', 'Phase 2A canonical identities cannot redirect or form cycles.');
    }
  }
  for (const mutation of plan.aliasesToCreate || []) {
    if (!canonicalIds.has(mutation.after.canonicalCustomerId)) fail('ALIAS_TARGET_MISSING', 'Alias targets a canonical identity outside this plan.');
  }
  if (mutations.length !== plan.writeCountPlanned) fail('PLANNED_WRITE_COUNT_MISMATCH', 'writeCountPlanned is inaccurate.');
  return true;
}

export function simulateMigration(plan, snapshot) {
  validateMigrationPlan(plan, { snapshot });
  const state = {
    canonicalCustomers: new Map(),
    aliases: new Map(),
    bookings: new Map(snapshot.bookings.map(item => [String(item.id), clone(item)])),
    packages: new Map(snapshot.packages.map(item => [String(item.id), clone(item)])),
  };
  const packageDigestBefore = sha256([...state.packages.values()].sort(byId));

  const applyOnce = () => {
    for (const mutation of plan.canonicalCustomersToCreate) {
      const existing = state.canonicalCustomers.get(mutation.targetDocumentId);
      if (existing && stableStringify(existing) !== stableStringify(mutation.after)) fail('SIMULATION_CREATE_CONFLICT', 'Canonical create conflicts with existing state.');
      state.canonicalCustomers.set(mutation.targetDocumentId, clone(mutation.after));
    }
    for (const mutation of plan.aliasesToCreate) {
      const existing = state.aliases.get(mutation.targetDocumentId);
      if (existing && stableStringify(existing) !== stableStringify(mutation.after)) fail('SIMULATION_CREATE_CONFLICT', 'Alias create conflicts with existing state.');
      state.aliases.set(mutation.targetDocumentId, clone(mutation.after));
    }
    for (const mutation of plan.bookingSoftLinksToAdd) {
      const booking = state.bookings.get(mutation.targetDocumentId);
      if (!booking) fail('SIMULATION_BOOKING_MISSING', `Booking ${mutation.targetDocumentId} is missing.`);
      Object.assign(booking, clone(mutation.after));
    }
  };

  applyOnce();
  const afterFirst = sha256({
    canonicalCustomers: [...state.canonicalCustomers.entries()].sort(),
    aliases: [...state.aliases.entries()].sort(),
    bookings: [...state.bookings.values()].sort(byId),
  });
  applyOnce();
  const afterSecond = sha256({
    canonicalCustomers: [...state.canonicalCustomers.entries()].sort(),
    aliases: [...state.aliases.entries()].sort(),
    bookings: [...state.bookings.values()].sort(byId),
  });

  for (const mutation of [...plan.bookingSoftLinksToAdd].reverse()) {
    const booking = state.bookings.get(mutation.targetDocumentId);
    Object.assign(booking, clone(mutation.rollback.restore));
    for (const field of mutation.rollback.remove) delete booking[field];
  }
  for (const mutation of plan.aliasesToCreate) state.aliases.delete(mutation.targetDocumentId);
  for (const mutation of plan.canonicalCustomersToCreate) state.canonicalCustomers.delete(mutation.targetDocumentId);
  const bookingDigestAfterRollback = sha256([...state.bookings.values()].sort(byId));
  const sourceBookingDigest = sha256([...snapshot.bookings].sort(byId));
  const packageDigestAfter = sha256([...state.packages.values()].sort(byId));
  return {
    ok: true,
    idempotent: afterFirst === afterSecond,
    rollbackExact: bookingDigestAfterRollback === sourceBookingDigest && state.canonicalCustomers.size === 0 && state.aliases.size === 0,
    packagesUntouched: packageDigestBefore === packageDigestAfter,
    writesPerformed: 0,
    simulatedWriteCount: plan.writeCountPlanned,
  };
}
