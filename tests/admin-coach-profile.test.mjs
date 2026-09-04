import { readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';

const adminHtml = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');
const adminOps = readFileSync(new URL('../api/admin-ops.js', import.meta.url), 'utf8');

describe('Admin Coach Profile editor', () => {
  test('edits the existing public profile fields and previews the customer card', () => {
    expect(adminHtml).toContain('id="coachProfileModal"');
    expect(adminHtml).toContain('id="coachProfileName"');
    expect(adminHtml).toContain('id="coachProfileBio"');
    expect(adminHtml).toContain('id="coachProfilePreview"');
    expect(adminHtml).toContain('ตัวอย่างที่ลูกค้าจะเห็น');
    expect(adminHtml).toContain('renderCoachProfileAdminPreview');
    expect(adminHtml).toContain('data-coach-profile=');
  });

  test('accepts only supported image files, resizes locally, and uploads on save', () => {
    expect(adminHtml).toContain('accept="image/jpeg,image/png,image/webp"');
    expect(adminHtml).toContain('const maxSide=900');
    expect(adminHtml).toContain('canvas.toDataURL("image/jpeg",quality)');
    expect(adminHtml).toContain('payload.photoDataUrl=coachProfileDraft.photoDataUrl');
    expect(adminHtml).toContain('action:"coach_update_profile"');
    expect(adminHtml).toContain('if(coachProfileDraft!==draft) return;');
  });

  test('server validates image content and stores it through the authenticated admin route', () => {
    expect(adminOps).toContain('parseCoachProfilePhotoDataUrl(body.photoDataUrl)');
    expect(adminOps).toContain("getAdminBucket(bucketName).file(objectPath)");
    expect(adminOps).toContain("firebaseStorageDownloadTokens: token");
    expect(adminOps).toContain("requireRole(session, 'owner', 'ultra_admin', 'branch_manager')");
    expect(adminOps).toContain("hasPhotoUpload && session.role === 'coach'");
  });
});
