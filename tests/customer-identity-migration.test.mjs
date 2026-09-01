import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { describe, expect, test } from 'vitest';
import { buildCustomerIdentityDryRun } from '../api/_lib/customer-identity.js';
import {
  MIGRATION_VERSION,
  MigrationPlanError,
  buildMigrationPlan,
  calculatePlanHash,
  sha256,
  simulateMigration,
  sourceSnapshotDigest,
  validateApprovalsAgainstDryRunReport,
  validateMigrationPlan,
} from '../scripts/_lib/customer-identity-migration.js';

const AUTO = `U${'1'.repeat(32)}`;
const ART = 'U8ef026374d200e21bcf615a3bbc6bb2d';
const NOPPON = 'U6741a143a06b5a63cab0a840d9bb8197';
const PAKSADA = 'U283f715a63f878c05fb3902a4cbef2c0';
const ANUTHAT = 'U1652aadd4d9e2c5da0ae93f0d8af0fb6';
const CONTAMINATED = 'U60929ad30400dcb9b8ab70527f104c3f';

function fixture() {
  const booking = (id, lineUserId, customerPhone, customerName = id) => ({
    id, branchId: 'ladprao1', date: '2026-08-24', bookingStatus: 'confirmed',
    lineUserId, customerPhone, customerName, totalPrice: 500,
  });
  const snapshot = {
    capturedAt: '2026-09-01T00:00:00.000Z',
    branchId: 'ladprao1',
    users: [
      { id: AUTO, phone: '0811111111', name: 'Auto' },
      { id: ART, phone: '0962825392', name: 'Art' },
      { id: NOPPON, phone: '0809660850', name: 'Noppon' },
      { id: PAKSADA, phone: '0837824549', name: 'PAKSADA' },
      { id: ANUTHAT, phone: '0935822219', name: 'Anuthat' },
    ],
    bookings: [
      booking('auto-1', 'manual', '0811111111', 'Auto'),
      booking('art-1', ART, '0962825392', 'Art'),
      booking('art-2', ART, '0962828392', 'Art typo'),
      booking('nop-1', NOPPON, '0809660850', 'Noppon'),
      booking('nop-2', NOPPON, '0923924850', 'นพพล'),
      booking('anu-1', ANUTHAT, '0935822219', 'Anuthat'),
      booking('anu-2', ANUTHAT, '0935822249', 'Anuthat'),
      booking('u609-admin', CONTAMINATED, '0649649222', 'BaiMon'),
      booking('u609-1', CONTAMINATED, '0829646369', 'K.ปิ่น'),
      booking('u609-2', CONTAMINATED, '0830177815', 'K.ภัทร'),
      booking('u609-3', CONTAMINATED, '0890177815', 'ก้องภพ'),
      booking('u609-4', CONTAMINATED, '0993248353', 'Unknown'),
    ],
    packages: [
      { id: 'pkg-nop', lineUserId: NOPPON, phone: '0809660850', remainingMinutes: 600 },
      { id: 'pkg-pak-1', lineUserId: PAKSADA, phone: '0837824549', remainingMinutes: 300 },
      { id: 'pkg-pak-2', lineUserId: PAKSADA, phone: '0837824549', remainingMinutes: 300 },
      { id: 'pkg-u609', lineUserId: CONTAMINATED, phone: '0649649222', remainingMinutes: 120 },
    ],
  };
  const approvals = {
    migrationVersion: MIGRATION_VERSION,
    branchId: 'ladprao1',
    approvedAutoLinks: [{ phone: '0811111111', lineUserId: AUTO, names: ['Auto'], bookingIds: ['auto-1'] }],
    approvedHumanMappings: {
      art: { lineUserId: ART, correctPhone: '0962825392', typoPhone: '0962828392', names: ['Art'], bookingIds: ['art-1', 'art-2'], packageIds: [] },
      noppon: { lineUserId: NOPPON, phones: ['0809660850', '0923924850'], names: ['Noppon'], bookingIds: ['nop-1', 'nop-2'], packageIds: ['pkg-nop'] },
      paksada: { lineUserId: PAKSADA, phones: ['0837824549'], names: ['PAKSADA'], packageIds: ['pkg-pak-1', 'pkg-pak-2'] },
    },
    manualReviewPreserved: {
      anuthat: { lineUserId: ANUTHAT, phones: ['0935822219', '0935822249'], decision: 'do_not_merge' },
    },
    contaminatedLegacyLine: {
      lineUserId: CONTAMINATED,
      adminPhone: '0649649222',
      isolatedPhones: ['0829646369', '0830177815', '0890177815', '0993248353'],
      bookingIds: ['u609-admin', 'u609-1', 'u609-2', 'u609-3', 'u609-4'],
      packageIds: ['pkg-u609'],
      registeredOwnerLinkDeniedPhones: ['0993248353'],
    },
  };
  return { snapshot, approvals };
}

function deterministicFactory() {
  let counter = 0;
  return () => `cc_${(++counter).toString(16).padStart(32, '0')}`;
}

function built(overrides = {}) {
  const base = fixture();
  return buildMigrationPlan({ ...base, idFactory: deterministicFactory(), ...overrides });
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function expectCode(callback, code) {
  try {
    callback();
    throw new Error('Expected callback to fail.');
  } catch (error) {
    expect(error).toBeInstanceOf(MigrationPlanError);
    expect(error.code).toBe(code);
  }
}

function rehash(plan) {
  plan.planHash = calculatePlanHash(plan);
  for (const mutation of [...plan.canonicalCustomersToCreate, ...plan.aliasesToCreate, ...plan.bookingSoftLinksToAdd]) {
    for (const key of Object.keys(mutation.after || {})) if (/PlanHash$/i.test(key)) mutation.after[key] = plan.planHash;
  }
  plan.planHash = calculatePlanHash(plan);
  return plan;
}

function evidenceReport() {
  const { snapshot } = fixture();
  return {
    ...buildCustomerIdentityDryRun({ bookings: snapshot.bookings, users: snapshot.users, packages: snapshot.packages, generatedAt: snapshot.capturedAt }),
    branchId: snapshot.branchId,
  };
}

describe('Customer Identity v1 Phase 2A migration planner', () => {
  test('uses the locked Phase 2A migration version', () => {
    expect(built().plan.migrationVersion).toBe(MIGRATION_VERSION);
  });

  test('produces a zero-write plan only', () => {
    expect(built().plan).toMatchObject({ mode: 'plan', dryRun: true, writesPerformed: 0 });
  });

  test('creates opaque random-shaped canonical identifiers without embedding PII', () => {
    const ids = built().plan.canonicalCustomersToCreate.map(item => item.targetDocumentId);
    expect(ids.every(id => /^cc_[0-9a-f]{32}$/.test(id))).toBe(true);
    expect(ids.join(' ')).not.toMatch(/0811111111|U1111|Art|Noppon/);
  });

  test('reuses canonical IDs from the private assignment registry', () => {
    const first = built();
    const second = built({ assignments: first.assignments, idFactory: () => { throw new Error('must not allocate'); } });
    expect(second.assignments).toEqual(first.assignments);
    expect(second.plan.canonicalCustomersToCreate.map(item => item.targetDocumentId))
      .toEqual(first.plan.canonicalCustomersToCreate.map(item => item.targetDocumentId));
  });

  test('source digest is stable when source array order changes', () => {
    const { snapshot } = fixture();
    const reversed = clone(snapshot);
    reversed.bookings.reverse(); reversed.users.reverse(); reversed.packages.reverse();
    expect(sourceSnapshotDigest(reversed)).toBe(sourceSnapshotDigest(snapshot));
  });

  test('current safety #2 evidence report verifies all approved decisions', () => {
    const { approvals } = fixture();
    expect(validateApprovalsAgainstDryRunReport(approvals, evidenceReport())).toMatchObject({
      ok: true, approvedAutoLinksVerified: 1, suspiciousDecisionsVerified: 4,
      packageDocumentsObserved: 4, writesPerformed: 0,
    });
  });

  test('new unreviewed auto-link proposal aborts evidence preflight', () => {
    const { approvals } = fixture();
    approvals.approvedAutoLinks = [];
    expectCode(() => validateApprovalsAgainstDryRunReport(approvals, evidenceReport()), 'UNREVIEWED_AUTO_LINK_PROPOSAL');
  });

  test('changed human-review phone evidence aborts preflight', () => {
    const { approvals } = fixture();
    approvals.approvedHumanMappings.noppon.phones[1] = '0899999999';
    expectCode(() => validateApprovalsAgainstDryRunReport(approvals, evidenceReport()), 'HUMAN_REVIEW_PHONE_SET_CHANGED');
  });

  test('reviewed evidence report digest is immutable', () => {
    const { approvals } = fixture();
    const report = evidenceReport();
    approvals.sourceEvidence = { reportDigest: sha256(report), sourceCounts: report.sourceCounts };
    report.summary.canonicalProfiles += 1;
    expectCode(() => validateApprovalsAgainstDryRunReport(approvals, report), 'APPROVAL_EVIDENCE_REPORT_CHANGED');
  });

  test('raw source counts changing after review aborts plan generation', () => {
    const { snapshot, approvals } = fixture();
    approvals.sourceEvidence = { sourceCounts: { bookings: 999, registeredUsers: 5, packages: 4 } };
    expectCode(() => buildMigrationPlan({ snapshot, approvals, idFactory: deterministicFactory() }), 'SOURCE_COUNTS_CHANGED_AFTER_REVIEW');
  });

  test('plan hash validates without mutation', () => {
    const { plan } = built();
    expect(validateMigrationPlan(plan, { snapshot: fixture().snapshot })).toBe(true);
  });

  test('tampered plan fails its hash guard', () => {
    const { plan } = built();
    plan.generatedAt = 'changed';
    expectCode(() => validateMigrationPlan(plan), 'PLAN_HASH_MISMATCH');
  });

  test('changed Production snapshot fails the stale-plan guard', () => {
    const { plan } = built();
    const changed = fixture().snapshot;
    changed.bookings[0].totalPrice = 999;
    expectCode(() => validateMigrationPlan(plan, { snapshot: changed }), 'STALE_MIGRATION_PLAN');
  });

  test('every planned mutation includes before, after, reason, and rollback', () => {
    const { plan } = built();
    const mutations = [...plan.canonicalCustomersToCreate, ...plan.aliasesToCreate, ...plan.bookingSoftLinksToAdd];
    expect(mutations.length).toBe(plan.writeCountPlanned);
    for (const item of mutations) {
      expect(item).toEqual(expect.objectContaining({ targetCollection: expect.any(String), targetDocumentId: expect.any(String), operation: expect.any(String), reason: expect.any(String), rollback: expect.any(Object) }));
      expect(item).toHaveProperty('before');
      expect(item).toHaveProperty('after');
    }
  });

  test('simulation is idempotent when run twice', () => {
    const { plan } = built();
    expect(simulateMigration(plan, fixture().snapshot).idempotent).toBe(true);
  });

  test('simulation rolls back exactly to the source bookings', () => {
    const { plan } = built();
    expect(simulateMigration(plan, fixture().snapshot).rollbackExact).toBe(true);
  });

  test('simulation proves all package documents are untouched', () => {
    const { plan } = built();
    expect(simulateMigration(plan, fixture().snapshot).packagesUntouched).toBe(true);
  });

  test('never plans a package mutation or balance calculation', () => {
    const { plan } = built();
    const text = JSON.stringify([...plan.canonicalCustomersToCreate, ...plan.aliasesToCreate, ...plan.bookingSoftLinksToAdd]);
    expect(text).not.toContain('customer_packages');
    expect(text).not.toContain('remainingMinutes');
    expect(plan.packageDocumentsUntouched).toHaveLength(4);
  });

  test('approved auto-link creates LINE and normalized phone aliases', () => {
    const { plan } = built();
    const approved = plan.approvedAutoLinks[0];
    const aliases = plan.aliasesToCreate.filter(item => item.after.canonicalCustomerId === approved.canonicalCustomerId);
    expect(aliases.map(item => item.after.aliasType).sort()).toEqual(['line_user_id', 'phone']);
    expect(plan.bookingSoftLinksToAdd.find(item => item.targetDocumentId === 'auto-1').after.canonicalCustomerId).toBe(approved.canonicalCustomerId);
  });

  test('Art typo is preserved as a non-matching, non-contact alias', () => {
    const { plan } = built();
    const typo = plan.aliasesToCreate.find(item => item.after.aliasType === 'typo_phone');
    expect(typo.after).toMatchObject({ value: '0962828392', usableForMatching: false, usableForContact: false });
    expect(plan.typoCorrections[0]).toMatchObject({ correctPhone: '0962825392', destructiveOverwrite: false });
  });

  test('Noppon joins two phones without selecting a primary phone', () => {
    const { plan } = built();
    const mapping = plan.approvedHumanMappings.find(item => item.subject === 'noppon');
    const canonical = plan.canonicalCustomersToCreate.find(item => item.targetDocumentId === mapping.canonicalCustomerId);
    expect(canonical.after.evidence.primaryPhone).toBeNull();
    expect(canonical.after.evidence.phones).toEqual(['0809660850', '0923924850']);
  });

  test('PAKSADA keeps two distinct package IDs', () => {
    const { plan } = built();
    const mapping = plan.approvedHumanMappings.find(item => item.subject === 'paksada');
    expect(mapping.packageIdsUntouched).toEqual(['pkg-pak-1', 'pkg-pak-2']);
    expect(plan.packageDocumentsUntouched.filter(item => mapping.packageIdsUntouched.includes(item.packageId))).toHaveLength(2);
  });

  test('Anuthat stays in manual review with no booking soft-links', () => {
    const { plan } = built();
    expect(plan.manualReviewPreserved).toContainEqual(expect.objectContaining({ subject: 'anuthat', migrationAction: 'none' }));
    expect(plan.bookingSoftLinksToAdd.some(item => ['anu-1', 'anu-2'].includes(item.targetDocumentId))).toBe(false);
  });

  test('each U609 booking receives a separate isolated canonical ID', () => {
    const { plan } = built();
    expect(plan.contaminatedLineSplits).toHaveLength(5);
    expect(new Set(plan.contaminatedLineSplits.map(item => item.canonicalCustomerId)).size).toBe(5);
  });

  test('U609 contaminated LINE never becomes an alias', () => {
    const { plan } = built();
    expect(plan.aliasesToCreate.some(item => item.after.value === CONTAMINATED)).toBe(false);
  });

  test('U609 admin phone is isolated but never becomes an alias', () => {
    const { plan } = built();
    expect(plan.contaminatedLineSplits.some(item => item.phone === '0649649222')).toBe(true);
    expect(plan.aliasesToCreate.some(item => item.after.value === '0649649222')).toBe(false);
  });

  test('U609 record-local phone evidence can never auto-match', () => {
    const { plan } = built();
    const aliases = plan.aliasesToCreate.filter(item => item.after.aliasType === 'contaminated_phone_evidence');
    expect(aliases).toHaveLength(4);
    expect(aliases.every(item => item.after.usableForMatching === false)).toBe(true);
  });

  test('U609 denied registered owner phone is not linked to a normal canonical group', () => {
    const { plan } = built();
    const alias = plan.aliasesToCreate.find(item => item.after.value === '0993248353');
    expect(alias.after.aliasType).toBe('contaminated_phone_evidence');
    expect(alias.after.usableForMatching).toBe(false);
  });

  test('missing approved booking aborts planning', () => {
    const { snapshot, approvals } = fixture();
    approvals.approvedAutoLinks[0].bookingIds.push('missing');
    expectCode(() => buildMigrationPlan({ snapshot, approvals, idFactory: deterministicFactory() }), 'APPROVED_BOOKING_MISSING');
  });

  test('unexpected approved booking phone aborts planning', () => {
    const { snapshot, approvals } = fixture();
    snapshot.bookings.find(item => item.id === 'auto-1').customerPhone = '0899999999';
    expectCode(() => buildMigrationPlan({ snapshot, approvals, idFactory: deterministicFactory() }), 'APPROVED_BOOKING_PHONE_CONFLICT');
  });

  test('unexpected approved booking LINE aborts planning', () => {
    const { snapshot, approvals } = fixture();
    snapshot.bookings.find(item => item.id === 'auto-1').lineUserId = `U${'9'.repeat(32)}`;
    expectCode(() => buildMigrationPlan({ snapshot, approvals, idFactory: deterministicFactory() }), 'APPROVED_BOOKING_LINE_CONFLICT');
  });

  test('registered phone owner conflict aborts planning', () => {
    const { snapshot, approvals } = fixture();
    snapshot.users[0].id = `U${'9'.repeat(32)}`;
    expectCode(() => buildMigrationPlan({ snapshot, approvals, idFactory: deterministicFactory() }), 'APPROVED_ALIAS_OWNER_CONFLICT');
  });

  test('missing reviewed package aborts planning', () => {
    const { snapshot, approvals } = fixture();
    snapshot.packages = snapshot.packages.filter(item => item.id !== 'pkg-nop');
    expectCode(() => buildMigrationPlan({ snapshot, approvals, idFactory: deterministicFactory() }), 'APPROVED_PACKAGE_MISSING');
  });

  test('historical identity field overwrite is rejected', () => {
    const { plan } = built();
    plan.bookingSoftLinksToAdd[0].after.customerPhone = '000';
    rehash(plan);
    expectCode(() => validateMigrationPlan(plan), 'HISTORICAL_IDENTITY_OVERWRITE');
  });

  test('package target mutation is rejected', () => {
    const { plan } = built();
    plan.bookingSoftLinksToAdd[0].targetCollection = 'customer_packages';
    rehash(plan);
    expectCode(() => validateMigrationPlan(plan), 'PACKAGE_MUTATION_FORBIDDEN');
  });

  test('destructive operation is rejected', () => {
    const { plan } = built();
    plan.bookingSoftLinksToAdd[0].operation = 'delete';
    rehash(plan);
    expectCode(() => validateMigrationPlan(plan), 'DESTRUCTIVE_OPERATION_FORBIDDEN');
  });

  test('canonical redirect and cycle material are rejected', () => {
    const { plan } = built();
    plan.canonicalCustomersToCreate[0].after.redirectTo = plan.canonicalCustomersToCreate[1].targetDocumentId;
    rehash(plan);
    expectCode(() => validateMigrationPlan(plan), 'CANONICAL_REDIRECT_FORBIDDEN');
  });

  test('invalid saved canonical assignment aborts planning', () => {
    const first = built();
    const key = Object.keys(first.assignments)[0];
    const assignments = { ...first.assignments, [key]: 'phone:0811111111' };
    expectCode(() => built({ assignments }), 'INVALID_CANONICAL_ASSIGNMENT');
  });

  test('planned booking writes contain no legacy counters or derived analytics', () => {
    const { plan } = built();
    const fields = plan.bookingSoftLinksToAdd.flatMap(item => Object.keys(item.after));
    expect(fields).toEqual(expect.not.arrayContaining(['totalBookings', 'confirmedBookings', 'cancelledBookings', 'lastBookingDate', 'firstBookingDate']));
  });

  test('CLI hard-disables apply before attempting credentials or reads', () => {
    const script = path.resolve('scripts/customer-identity-migrate.mjs');
    const result = spawnSync(process.execPath, [script, '--apply'], { cwd: process.cwd(), encoding: 'utf8' });
    expect(result.status).toBe(1);
    expect(result.stderr).toContain('PHASE_2A_APPLY_DISABLED');
    expect(result.stderr).toContain('"writesPerformed": 0');
  });
});
