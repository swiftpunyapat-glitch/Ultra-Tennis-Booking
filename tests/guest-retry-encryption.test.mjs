import { afterEach, describe, expect, test, vi } from 'vitest';
import { protectGuestRetryResponse, restoreGuestRetryResponse } from '../api/_lib/firebase-admin.js';

afterEach(() => vi.unstubAllEnvs());
describe('encrypted guest retry response', () => {
  test('round trips, uses unique nonces, and never mutates or stores the raw response', () => {
    vi.stubEnv('GUEST_RETRY_SECRET', 'test-only-guest-retry-secret');
    const response = { booking: { id: 'b1' }, guestAccessToken: 'opaque-test-capability' };
    const a = protectGuestRetryResponse(response, 'record:b1');
    const b = protectGuestRetryResponse(response, 'record:b1');
    expect(JSON.stringify(a).includes(response.guestAccessToken)).toBe(false);
    expect(a.guestRetryEnvelope.iv).not.toBe(b.guestRetryEnvelope.iv);
    expect(restoreGuestRetryResponse(a, 'record:b1')).toEqual(response);
    expect(response.guestAccessToken).toBe('opaque-test-capability');
  });
  test('wrong context, changed ciphertext and wrong key fail closed', () => {
    vi.stubEnv('GUEST_RETRY_SECRET', 'test-only-guest-retry-secret');
    const a = protectGuestRetryResponse({ guestAccessToken: 'opaque-test-capability' }, 'record:b1');
    expect(() => restoreGuestRetryResponse(a, 'record:b2')).toThrow('GUEST_RETRY_UNAVAILABLE');
    const altered = structuredClone(a);
    const bytes = Buffer.from(altered.guestRetryEnvelope.ciphertext, 'base64url');
    bytes[0] ^= 1;
    altered.guestRetryEnvelope.ciphertext = bytes.toString('base64url');
    expect(() => restoreGuestRetryResponse(altered, 'record:b1')).toThrow('GUEST_RETRY_UNAVAILABLE');
    vi.stubEnv('GUEST_RETRY_SECRET', 'different-server-secret');
    expect(() => restoreGuestRetryResponse(a, 'record:b1')).toThrow('GUEST_RETRY_UNAVAILABLE');
  });
  test('existing server secret is supported, no secret fails closed, signed responses need no encryption', () => {
    vi.stubEnv('GUEST_RETRY_SECRET', '');
    vi.stubEnv('ADMIN_SESSION_SECRET', 'existing-server-secret');
    const response = { guestAccessToken: 'opaque-test-capability' };
    expect(restoreGuestRetryResponse(protectGuestRetryResponse(response, 'id'), 'id')).toEqual(response);
    vi.stubEnv('ADMIN_SESSION_SECRET', '');
    expect(() => protectGuestRetryResponse(response, 'id')).toThrow('GUEST_RETRY_UNAVAILABLE');
    expect(protectGuestRetryResponse({ ok: true }, 'id')).toEqual({ ok: true });
  });
});
