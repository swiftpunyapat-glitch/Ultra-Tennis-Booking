import { afterEach, describe, expect, test, vi } from 'vitest';

const authMocks = vi.hoisted(() => ({
  createCustomToken: vi.fn(),
}));

vi.mock('../api/_lib/firebase-admin.js', () => ({
  getAdminAuth: () => ({ createCustomToken: authMocks.createCustomToken }),
}));

import handler from '../api/auth-line.js';

const originalFetch = global.fetch;

function call(body) {
  let statusCode = 200;
  let payload = null;
  const req = { method: 'POST', body };
  const res = {
    status(code) { statusCode = code; return this; },
    json(value) { payload = value; return this; },
  };
  return Promise.resolve(handler(req, res)).then(() => ({ statusCode, payload }));
}

afterEach(() => {
  vi.useRealTimers();
  vi.clearAllMocks();
  global.fetch = originalFetch;
});

describe('LINE authentication timeout boundaries', () => {
  test('aborts a stalled LINE profile verification after five seconds', async () => {
    vi.useFakeTimers();
    global.fetch = vi.fn((_url, options) => new Promise((_, reject) => {
      options.signal.addEventListener('abort', () => {
        const error = new Error('aborted');
        error.name = 'AbortError';
        reject(error);
      }, { once: true });
    }));

    const pending = call({ accessToken: 'line-access-token' });
    await vi.advanceTimersByTimeAsync(5001);

    expect(await pending).toMatchObject({
      statusCode: 504,
      payload: { ok: false, error: 'LINE profile verification timed out' },
    });
    expect(authMocks.createCustomToken).not.toHaveBeenCalled();
  });

  test('stops waiting when Firebase custom-token minting stalls', async () => {
    vi.useFakeTimers();
    global.fetch = vi.fn(async () => ({
      ok: true,
      json: async () => ({ userId: 'U-test', displayName: 'Test User' }),
    }));
    authMocks.createCustomToken.mockReturnValue(new Promise(() => {}));

    const pending = call({ accessToken: 'line-access-token' });
    await vi.advanceTimersByTimeAsync(5001);

    expect(await pending).toMatchObject({
      statusCode: 504,
      payload: { ok: false, error: 'Firebase token mint timed out' },
    });
  });
});
