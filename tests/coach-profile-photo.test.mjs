import { describe, expect, test } from 'vitest';
import {
  COACH_PROFILE_PHOTO_MAX_BYTES,
  parseCoachProfilePhotoDataUrl,
  coachProfilePhotoObjectPath,
  coachProfilePhotoDownloadUrl,
} from '../api/_lib/coach-profile-photo.js';

const dataUrl = (type, bytes) => `data:${type};base64,${Buffer.from(bytes).toString('base64')}`;

describe('Coach profile photo validation', () => {
  test('accepts JPEG, PNG, and WebP only when the bytes match the declared type', () => {
    expect(parseCoachProfilePhotoDataUrl(dataUrl('image/jpeg', [0xff, 0xd8, 0xff, 0x00]))).toMatchObject({ contentType: 'image/jpeg', extension: 'jpg' });
    expect(parseCoachProfilePhotoDataUrl(dataUrl('image/png', [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))).toMatchObject({ contentType: 'image/png', extension: 'png' });
    expect(parseCoachProfilePhotoDataUrl(dataUrl('image/webp', Buffer.from('RIFF0000WEBP')))).toMatchObject({ contentType: 'image/webp', extension: 'webp' });
    expect(() => parseCoachProfilePhotoDataUrl(dataUrl('image/png', [0xff, 0xd8, 0xff]))).toThrow('COACH_PHOTO_INVALID_CONTENT');
    expect(() => parseCoachProfilePhotoDataUrl(dataUrl('image/gif', Buffer.from('GIF89a')))).toThrow('COACH_PHOTO_INVALID_TYPE');
  });

  test('rejects decoded images above the server limit', () => {
    const oversized = Buffer.alloc(COACH_PROFILE_PHOTO_MAX_BYTES + 1, 0);
    oversized[0] = 0xff; oversized[1] = 0xd8; oversized[2] = 0xff;
    expect(() => parseCoachProfilePhotoDataUrl(dataUrl('image/jpeg', oversized))).toThrow('COACH_PHOTO_TOO_LARGE');
  });

  test('builds versioned private object paths and tokenized Firebase download URLs', () => {
    const path = coachProfilePhotoObjectPath('Coach Art', '123-photo.jpg');
    expect(path).toMatch(/^coach_profiles\/[a-f0-9]{24}\/123-photo\.jpg$/);
    expect(coachProfilePhotoDownloadUrl('example.firebasestorage.app', path, 'token value')).toBe(
      `https://firebasestorage.googleapis.com/v0/b/example.firebasestorage.app/o/${encodeURIComponent(path)}?alt=media&token=token%20value`,
    );
  });
});
