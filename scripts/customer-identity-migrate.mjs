#!/usr/bin/env node

// Trusted local Phase 2A planner. This file deliberately contains no apply path.

import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { initializeApp, cert, getApps } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';
import {
  MIGRATION_VERSION,
  MigrationPlanError,
  buildMigrationPlan,
  simulateMigration,
} from './_lib/customer-identity-migration.js';

const ROOT = path.resolve(import.meta.dirname, '..');
const APPROVALS_PATH = path.join(ROOT, 'scripts', 'customer-identity-approvals-v1.json');
const PLAN_DIR = path.join(ROOT, 'migration-plans');
const ASSIGNMENTS_PATH = path.join(PLAN_DIR, 'customer-identity-v1-assignments.private.json');

function argument(name) {
  const prefix = `--${name}=`;
  const item = process.argv.slice(2).find(value => value.startsWith(prefix));
  return item ? item.slice(prefix.length) : '';
}

function hasFlag(name) {
  return process.argv.slice(2).includes(`--${name}`) || process.argv.slice(2).some(value => value.startsWith(`--${name}=`));
}

function serializeFirestore(value) {
  if (value == null || ['string', 'number', 'boolean'].includes(typeof value)) return value;
  if (Array.isArray(value)) return value.map(serializeFirestore);
  if (typeof value.toDate === 'function') return value.toDate().toISOString();
  if (typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, serializeFirestore(child)]));
  }
  return String(value);
}

async function readJson(filePath, fallback = null) {
  try {
    return JSON.parse(await fs.readFile(filePath, 'utf8'));
  } catch (error) {
    if (error.code === 'ENOENT' && fallback !== null) return fallback;
    throw error;
  }
}

async function writeJsonAtomic(filePath, value) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const temporary = `${filePath}.${process.pid}.tmp`;
  await fs.writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
  await fs.rename(temporary, filePath);
}

async function loadProductionSnapshot(branchId) {
  const credentialJson = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (!credentialJson) {
    throw new MigrationPlanError(
      'READ_CREDENTIAL_REQUIRED',
      'Set FIREBASE_SERVICE_ACCOUNT in this local terminal to read Production. Do not paste the credential into chat or save it in the repository.',
    );
  }
  let serviceAccount;
  try {
    serviceAccount = JSON.parse(credentialJson);
  } catch {
    throw new MigrationPlanError('INVALID_READ_CREDENTIAL', 'FIREBASE_SERVICE_ACCOUNT is not valid JSON.');
  }
  if (!getApps().length) initializeApp({ credential: cert(serviceAccount) });
  const db = getFirestore();
  const [bookingsSnapshot, usersSnapshot, packagesSnapshot] = await Promise.all([
    db.collection('bookings').get(),
    db.collection('registered_users').get(),
    db.collection('customer_packages').get(),
  ]);
  const documents = querySnapshot => querySnapshot.docs.map(doc => ({ id: doc.id, ...serializeFirestore(doc.data()) }));
  const belongsToBranch = record => !record.branchId || record.branchId === branchId;
  return {
    capturedAt: new Date().toISOString(),
    branchId,
    bookings: documents(bookingsSnapshot).filter(belongsToBranch),
    users: documents(usersSnapshot),
    packages: documents(packagesSnapshot).filter(belongsToBranch),
  };
}

async function main() {
  if (hasFlag('apply')) {
    throw new MigrationPlanError('PHASE_2A_APPLY_DISABLED', 'Apply is hard-disabled in Customer Identity Phase 2A. This script can only produce a plan.');
  }
  if (!hasFlag('plan')) {
    throw new MigrationPlanError('PLAN_FLAG_REQUIRED', 'Use: node scripts/customer-identity-migrate.mjs --plan [--snapshot=path]');
  }
  const unknownFlags = process.argv.slice(2).filter(value => !value.startsWith('--plan') && !value.startsWith('--snapshot=') && !value.startsWith('--out=') && !value.startsWith('--assignments='));
  if (unknownFlags.length) throw new MigrationPlanError('UNKNOWN_ARGUMENT', `Unknown argument(s): ${unknownFlags.join(', ')}`);

  const approvals = await readJson(APPROVALS_PATH);
  const snapshotPath = argument('snapshot');
  const snapshot = snapshotPath
    ? await readJson(path.resolve(process.cwd(), snapshotPath))
    : await loadProductionSnapshot(approvals.branchId);
  const assignmentsPath = path.resolve(process.cwd(), argument('assignments') || ASSIGNMENTS_PATH);
  const assignments = await readJson(assignmentsPath, {});
  const { plan, assignments: updatedAssignments } = buildMigrationPlan({ snapshot, approvals, assignments });
  const simulation = simulateMigration(plan, snapshot);
  if (!simulation.idempotent || !simulation.rollbackExact || !simulation.packagesUntouched) {
    throw new MigrationPlanError('SIMULATION_FAILED', 'Plan failed idempotency, rollback, or package-integrity simulation.', simulation);
  }

  const timestamp = String(plan.generatedAt).replaceAll(':', '-').replaceAll('.', '-');
  const outputPath = path.resolve(process.cwd(), argument('out') || path.join(PLAN_DIR, `customer-identity-plan-${timestamp}.json`));
  await writeJsonAtomic(assignmentsPath, updatedAssignments);
  await writeJsonAtomic(outputPath, { ...plan, simulation });
  process.stdout.write(`${JSON.stringify({
    ok: true,
    migrationVersion: MIGRATION_VERSION,
    outputPath,
    assignmentsPath,
    planHash: plan.planHash,
    sourceSnapshot: plan.sourceSnapshot,
    canonicalCustomersToCreate: plan.canonicalCustomersToCreate.length,
    aliasesToCreate: plan.aliasesToCreate.length,
    bookingSoftLinksToAdd: plan.bookingSoftLinksToAdd.length,
    recordsSkipped: plan.recordsSkipped.length,
    manualReviewPreserved: plan.manualReviewPreserved.length,
    hardBlockedRecords: plan.hardBlockedRecords.length,
    packagesUntouched: plan.packageDocumentsUntouched.length,
    writeCountPlanned: plan.writeCountPlanned,
    writesPerformed: 0,
    simulation,
  }, null, 2)}\n`);
}

main().catch(error => {
  process.stderr.write(`${JSON.stringify({
    ok: false,
    code: error.code || 'UNEXPECTED_ERROR',
    message: error.message,
    details: error.details || null,
    writesPerformed: 0,
  }, null, 2)}\n`);
  process.exitCode = 1;
});
