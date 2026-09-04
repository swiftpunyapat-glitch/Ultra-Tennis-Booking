import { beforeAll, beforeEach, describe, expect, test, vi } from 'vitest';

const mocked = vi.hoisted(() => ({
  coach: {},
  saved: [],
  deleted: [],
  objectPath: '',
}));

vi.mock('../api/_lib/admin-auth.js', async importOriginal => ({
  ...(await importOriginal()),
  coachSessionFromToken: async idToken => idToken === 'coach-token'
    ? { name: 'coach-a', role: 'coach', branches: ['ladprao1'], coach: { ...mocked.coach } }
    : null,
}));

vi.mock('../api/_lib/firebase-admin.js', async importOriginal => {
  const actual = await importOriginal();
  const db = {
    collection(name) {
      if (name !== 'coaches') throw new Error(`Unexpected collection ${name}`);
      return {
        doc(coachId) {
          return {
            async get() {
              return coachId === 'coach-a'
                ? { exists: true, data: () => ({ ...mocked.coach }) }
                : { exists: false, data: () => null };
            },
            async update(values) { Object.assign(mocked.coach, values); },
          };
        },
      };
    },
  };
  return {
    ...actual,
    getAdminDb: () => db,
    getAdminAuth: () => ({}),
    getAdminBucket: () => ({
      file(path) {
        mocked.objectPath = path;
        return {
          async save(buffer, options) { mocked.saved.push({ buffer, options }); },
          async delete(options) { mocked.deleted.push(options); },
        };
      },
    }),
    writeAuditLog: async () => {},
  };
});

let handler, cookies;

beforeAll(async () => {
  process.env.ADMIN_SESSION_SECRET = 'coach-profile-api-test-secret';
  process.env.ADMIN_USERS_JSON = JSON.stringify({
    Manager: { pin: '0000', role: 'branch_manager', branches: ['ladprao1'] },
    Staff: { pin: '0000', role: 'branch_staff', branches: ['ladprao1'] },
  });
  handler = (await import('../api/admin-ops.js')).default;
  const { createSessionCookie } = await import('../api/_lib/admin-auth.js');
  cookies = Object.fromEntries(['Manager', 'Staff'].map(name => [name, createSessionCookie(name).split(';')[0]]));
});

beforeEach(() => {
  mocked.coach = { name: 'coach-a', displayName: 'Old Name', bio: '', photoUrl: null, branchId: 'ladprao1', active: true };
  mocked.saved.length = 0;
  mocked.deleted.length = 0;
  mocked.objectPath = '';
});

function request(body, who) {
  return { method: 'POST', body, headers: { cookie: cookies[who] }, socket: {} };
}

function response() {
  const value = { statusCode: null, body: null };
  value.status = code => { value.statusCode = code; return value; };
  value.json = body => { value.body = body; return value; };
  return value;
}

async function call(body, who) {
  const out = response();
  await handler(request(body, who), out);
  return out;
}

const jpegDataUrl = `data:image/jpeg;base64,${Buffer.from([0xff, 0xd8, 0xff, 0x00]).toString('base64')}`;

describe('Admin Coach Profile API', () => {
  test('branch manager uploads a validated image and saves the public profile', async () => {
    const result = await call({
      action: 'coach_update_profile', coachId: 'coach-a',
      displayName: 'Coach Art', bio: 'Beginner-friendly coach', photoDataUrl: jpegDataUrl,
    }, 'Manager');

    expect(result.statusCode).toBe(200);
    expect(mocked.saved).toHaveLength(1);
    expect(mocked.saved[0].options.metadata.contentType).toBe('image/jpeg');
    expect(mocked.saved[0].options.metadata.metadata.firebaseStorageDownloadTokens).toBeTruthy();
    expect(mocked.objectPath).toMatch(/^coach_profiles\/[a-f0-9]{24}\/.+\.jpg$/);
    expect(mocked.coach).toMatchObject({ displayName: 'Coach Art', bio: 'Beginner-friendly coach' });
    expect(mocked.coach.photoUrl).toMatch(/^https:\/\/firebasestorage\.googleapis\.com\//);
    expect(result.body.profile.photoUrl).toBe(mocked.coach.photoUrl);
  });

  test('invalid image bytes fail before any storage write', async () => {
    const bad = `data:image/png;base64,${Buffer.from([0xff, 0xd8, 0xff]).toString('base64')}`;
    const result = await call({ action: 'coach_update_profile', coachId: 'coach-a', displayName: 'Coach Art', photoDataUrl: bad }, 'Manager');
    expect(result.statusCode).toBe(400);
    expect(result.body.code).toBe('COACH_PHOTO_INVALID_CONTENT');
    expect(mocked.saved).toHaveLength(0);
  });

  test('branch staff cannot edit or upload coach profiles', async () => {
    const result = await call({ action: 'coach_update_profile', coachId: 'coach-a', displayName: 'Changed', photoDataUrl: jpegDataUrl }, 'Staff');
    expect(result.statusCode).toBe(403);
    expect(mocked.saved).toHaveLength(0);
    expect(mocked.coach.displayName).toBe('Old Name');
  });

  test('coach sessions cannot use the Admin-managed photo upload field', async () => {
    const out = response();
    await handler({
      method: 'POST',
      body: { action: 'coach_update_profile', idToken: 'coach-token', photoDataUrl: jpegDataUrl },
      headers: {},
      socket: {},
    }, out);
    expect(out.statusCode).toBe(403);
    expect(mocked.saved).toHaveLength(0);
  });
});
