import { beforeEach, describe, expect, test, vi } from 'vitest';

const state = vi.hoisted(() => ({
  session: { name: 'Art', role: 'owner', branches: '*' },
  records: new Map(),
  audit: vi.fn(),
}));

vi.mock('../api/_lib/admin-auth.js', () => ({
  verifySession: () => state.session,
  requireRole: (session, ...roles) => roles.includes(session?.role),
  hasBranchAccess: () => true,
  resolveBranchId: value => value?.branchId || 'ladprao1',
  DEFAULT_BRANCH_ID: 'ladprao1',
}));

vi.mock('../api/_lib/firebase-admin.js', () => ({
  getAdminDb: () => ({
    collection(name) {
      if (name !== 'ai_report_access') throw new Error(`Unexpected collection ${name}`);
      const snapshot = (id, ref) => ({ exists: state.records.has(id), id, ref, data: () => state.records.get(id) });
      return {
        limit: () => ({
          get: async () => ({ docs: [...state.records.keys()].map(id => snapshot(id, null)) }),
        }),
        doc(id) {
          const ref = {
            create: async data => {
              if (state.records.has(id)) throw new Error('already exists');
              state.records.set(id, data);
            },
            get: async () => snapshot(id, ref),
            update: async data => state.records.set(id, { ...state.records.get(id), ...data }),
          };
          return ref;
        },
      };
    },
  }),
  writeAuditLog: (...args) => state.audit(...args),
}));

vi.mock('../api/_lib/notify.js', () => ({
  sendAndLog: vi.fn(),
  loadActiveAdmins: vi.fn(),
}));

const { default: handler } = await import('../api/admin-user-action.js');

function call(body) {
  let statusCode = 200;
  let payload;
  const req = { method: 'POST', body, headers: {}, socket: {} };
  const res = {
    status(code) { statusCode = code; return this; },
    json(value) { payload = value; return this; },
  };
  return Promise.resolve(handler(req, res)).then(() => ({ statusCode, payload }));
}

beforeEach(() => {
  state.session = { name: 'Art', role: 'owner', branches: '*' };
  state.records.clear();
  state.audit.mockReset();
});

describe('Art-owned AI Report access management', () => {
  test('denies another owner even when the role itself is valid', async () => {
    state.session = { name: 'Boss', role: 'owner', branches: '*' };
    expect((await call({ action: 'ai_report_access_list' })).statusCode).toBe(403);
    expect((await call({ action: 'ai_report_access_create', label: 'Boss Link', expiresDays: 30 })).statusCode).toBe(403);
  });

  test('creates once, lists without the raw token, audits, and revokes immediately', async () => {
    const created = await call({ action: 'ai_report_access_create', label: 'Ace Monthly', expiresDays: 30 });
    expect(created.statusCode).toBe(200);
    expect(created.payload.token).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(created.payload.tokenShownOnce).toBe(true);
    expect(state.records.has(created.payload.access.id)).toBe(true);
    expect(state.records.get(created.payload.access.id).scopes).toContain('customer_analytics_anonymous');
    expect(JSON.stringify([...state.records.values()])).not.toContain(created.payload.token);

    const listed = await call({ action: 'ai_report_access_list' });
    expect(listed.statusCode).toBe(200);
    expect(listed.payload.links).toHaveLength(1);
    expect(JSON.stringify(listed.payload)).not.toContain(created.payload.token);

    const revoked = await call({ action: 'ai_report_access_revoke', id: created.payload.access.id });
    expect(revoked.statusCode).toBe(200);
    expect(state.records.get(created.payload.access.id).active).toBe(false);
    expect(state.audit).toHaveBeenCalledTimes(2);
  });
});
