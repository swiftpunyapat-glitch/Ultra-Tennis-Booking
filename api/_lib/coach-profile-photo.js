import { createHash } from 'node:crypto';

export const COACH_PROFILE_PHOTO_MAX_BYTES = 1_500_000;
export const COACH_PROFILE_PHOTO_MAX_DATA_URL_CHARS = 2_100_000;

const PHOTO_TYPES = Object.freeze({
  'image/jpeg': 'jpg',
  'image/png': 'png',
  'image/webp': 'webp',
});

function hasExpectedSignature(buffer, contentType) {
  if (contentType === 'image/jpeg') {
    return buffer.length >= 3 && buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff;
  }
  if (contentType === 'image/png') {
    return buffer.length >= 8 && buffer.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]));
  }
  if (contentType === 'image/webp') {
    return buffer.length >= 12 && buffer.subarray(0, 4).toString('ascii') === 'RIFF' &&
      buffer.subarray(8, 12).toString('ascii') === 'WEBP';
  }
  return false;
}

export function parseCoachProfilePhotoDataUrl(value) {
  if (typeof value !== 'string' || !value || value.length > COACH_PROFILE_PHOTO_MAX_DATA_URL_CHARS) {
    throw new Error('COACH_PHOTO_TOO_LARGE');
  }
  const match = value.match(/^data:(image\/(?:jpeg|png|webp));base64,([A-Za-z0-9+/]+={0,2})$/);
  if (!match) throw new Error('COACH_PHOTO_INVALID_TYPE');
  const [, contentType, payload] = match;
  const buffer = Buffer.from(payload, 'base64');
  if (!buffer.length || buffer.length > COACH_PROFILE_PHOTO_MAX_BYTES) {
    throw new Error('COACH_PHOTO_TOO_LARGE');
  }
  if (!hasExpectedSignature(buffer, contentType)) throw new Error('COACH_PHOTO_INVALID_CONTENT');
  return { buffer, contentType, extension: PHOTO_TYPES[contentType] };
}

export function coachProfilePhotoObjectPath(coachId, uniquePart) {
  const coachKey = createHash('sha256').update(String(coachId)).digest('hex').slice(0, 24);
  const suffix = String(uniquePart || '').replace(/[^a-zA-Z0-9.-]/g, '').slice(0, 80);
  if (!suffix) throw new Error('COACH_PHOTO_PATH_REQUIRED');
  return `coach_profiles/${coachKey}/${suffix}`;
}

export function coachProfilePhotoDownloadUrl(bucketName, objectPath, token) {
  if (!bucketName || !objectPath || !token) throw new Error('COACH_PHOTO_URL_REQUIRED');
  return `https://firebasestorage.googleapis.com/v0/b/${encodeURIComponent(bucketName)}/o/${encodeURIComponent(objectPath)}?alt=media&token=${encodeURIComponent(token)}`;
}
