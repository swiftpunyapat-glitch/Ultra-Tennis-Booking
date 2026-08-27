import { describe, expect, test } from 'vitest';
import { readdirSync, readFileSync } from 'node:fs';

const source = readFileSync(new URL('../api/admin-ops.js', import.meta.url), 'utf8');
const apiFiles = readdirSync(new URL('../api/', import.meta.url), { withFileTypes:true })
  .filter(entry => entry.isFile() && entry.name.endsWith('.js'))
  .map(entry => entry.name);

describe('customer identity admin endpoint', () => {
  test('multiplexes into admin-ops without consuming another top-level API function', () => {
    expect(source).toContain("'admin_read', 'identity_dry_run'");
    expect(source).toContain("case 'identity_dry_run':       return handleIdentityDryRun");
    expect(apiFiles).not.toContain('customer-identity.js');
  });

  test('is restricted to Art with the owner role and branch access', () => {
    expect(source).toContain("session.name !== 'Art' || !requireRole(session, 'owner')");
    expect(source).toContain("if (!hasBranchAccess(session, branchId))");
  });

  test('reads each source once and delegates to a pure zero-write engine', () => {
    expect(source).toContain("db.collection('bookings').get()");
    expect(source).toContain("db.collection('registered_users').get()");
    expect(source).toContain("db.collection('customer_packages').get()");
    expect(source).toContain('buildCustomerIdentityDryRun({');
    const handlerStart = source.indexOf('async function handleIdentityDryRun');
    const handlerEnd = source.indexOf('// ════════════════════════════════════════════════════════════════════', handlerStart);
    const handlerSource = source.slice(handlerStart, handlerEnd);
    expect(handlerSource).not.toMatch(/\.set\(|\.update\(|\.delete\(|\.add\(|runTransaction|batch\(/);
  });
});
