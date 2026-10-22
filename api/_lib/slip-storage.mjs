// Payment documents are private. Only this server module touches Storage.
import crypto from 'node:crypto';
import sharp from 'sharp';
import { Timestamp } from 'firebase-admin/firestore';
import { getAdminAuth, getAdminBucket, checkRateLimit, readRateLimitGate, RATE_LIMITS, clientIp, verifyGuestToken, isValidIdempotencyKey } from './firebase-admin.js';
import { verifySession, requireRole, hasBranchAccess, resolveBranchId } from './admin-auth.js';

export const MAX_UPLOAD_BYTES = 2 * 1024 * 1024; // base64 JSON stays below Vercel's 4.5 MB request limit
export const SLIP_BUCKET = 'ultra-tennis-booking.firebasestorage.app';
const LEGACY_BUCKETS = [SLIP_BUCKET, 'ultra-tennis-booking.appspot.com'];
const TYPES = { booking: 'bookings', pass_purchase: 'pass_purchases' };
const ID = /^[A-Za-z0-9_-]{1,128}$/;
const RECEIPT = /^private:([a-f0-9]{64})$/;
const fail = (status, code, message = code) => { throw Object.assign(new Error(message), { status, code }); };
export function slipError(res, error) {
  const status = error.status || 503;
  if (status === 429) res.setHeader('Retry-After', '900');
  return res.status(status).json({ ok: false, code: error.code || 'SLIP_UNAVAILABLE', error: error.status ? error.message : 'รับ/เปิดสลิปไม่ได้ชั่วคราว กรุณาติดต่อแอดมินผ่านช่องทางเดิม' });
}
async function rate(db, bucket, key, limit, windowMs = 900_000) {
  const gate = await checkRateLimit(db, { bucket, key, limit, windowMs });
  if (!gate.allowed) fail(429, 'SLIP_RATE_LIMIT', 'กรุณารอสักครู่ก่อนลองใหม่ หรือติดต่อแอดมิน');
}
export async function authorizeSlipTarget(req, body, db, operation = 'read') {
  const type = body.targetType === 'pass_purchase' || body.action === 'submit_pass_slip' ? 'pass_purchase' : 'booking';
  const id = type === 'booking' ? body.bookingId : body.purchaseId;
  if (!ID.test(id || '') || (body.targetType && !['booking', 'pass_purchase'].includes(body.targetType))) fail(400, 'SLIP_TARGET');
  // Authenticate before looking up a supplied document ID. Booking codes are never credentials.
  const session = verifySession(req);
  let actor, guest = false, admin = false;
  if (operation !== 'upload' && session) {
    if (!requireRole(session, 'owner', 'ultra_admin', 'branch_manager')) fail(403, 'SLIP_ROLE');
    actor = `admin:${session.name}`; admin = true;
  } else if (body.idToken) {
    await rate(db, 'slipIp', clientIp(req), operation === 'upload' ? 30 : 120);
    if (typeof body.idToken !== 'string' || body.idToken.length > 8192) fail(401, 'SLIP_AUTH');
    try { actor = (await getAdminAuth().verifyIdToken(body.idToken, true)).uid; }
    catch { fail(401, 'SLIP_AUTH', 'กรุณาเข้าสู่ระบบ LINE อีกครั้ง'); }
  } else if (type === 'booking') {
    const globalGate = await readRateLimitGate(db, { bucket: 'guestInvalid', key: clientIp(req) });
    if (!globalGate.allowed) fail(429, 'SLIP_RATE_LIMIT');
    if (typeof body.guestToken !== 'string' || body.guestToken.length > 128) fail(401, 'SLIP_AUTH');
    const capability = await verifyGuestToken(db, id, body.guestToken, operation === 'read' ? 'booking:read' : 'slip:submit');
    if (!capability.ok) {
      const gate = await checkRateLimit(db, { bucket: 'guestInvalid', key: clientIp(req), ...RATE_LIMITS.guestInvalid });
      if (!gate.allowed) fail(429, 'SLIP_RATE_LIMIT');
      fail(403, 'SLIP_CAPABILITY', 'สิทธิ์เข้าถึงหมดอายุหรือไม่ถูกต้อง กรุณาติดต่อแอดมิน');
    }
    await rate(db, 'slipIp', clientIp(req), operation === 'upload' ? 30 : 120);
    actor = `guest:${id}`; guest = true;
  } else { await rate(db, 'slipIp', clientIp(req), 30); fail(401, 'SLIP_AUTH'); }
  if (admin) await rate(db, 'slipIp', clientIp(req), 120);
  const ref = db.collection(TYPES[type]).doc(id);
  const snap = await ref.get();
  if (!snap.exists) fail(404, 'SLIP_NOT_FOUND');
  const record = snap.data();
  if (admin) {
    if (!hasBranchAccess(session, resolveBranchId(record))) fail(403, 'SLIP_BRANCH');
  } else if (guest) {
    if (record.lineUserId && record.lineUserId !== 'guest') fail(403, 'SLIP_OWNER');
  } else if (!actor || record.lineUserId !== actor) fail(403, 'SLIP_OWNER');
  if (operation === 'upload') {
    assertUploadState(record, type);
    await rate(db, 'slipOwner', actor, 12, 3_600_000);
    await rate(db, 'slipTarget', `${type}:${id}`, 6, 3_600_000);
  }
  return { type, id, ref, record, actor };
}
// Firestore emulator/SDK can surface a closed transaction after lock contention
// as INVALID_ARGUMENT instead of ABORTED. Retry the WHOLE transaction, never a
// read/write using the closed transaction handle. Logical 4xx errors are not retried.
export async function runSlipTransaction(db, work) {
  for (let attempt = 0; ; attempt++) {
    try { return await db.runTransaction(work); }
    catch (error) {
      if (attempt >= 2 || error.code !== 3 || !/Transaction is invalid or closed/.test(error.message || '')) throw error;
    }
  }
}
export function assertUploadState(record, type) {
  if (['paid', 'package', 'rejected'].includes(record.paymentStatus) || record.issuedPackageId ||
      ['cancelled', 'rejected'].includes(record.bookingStatus || record.status)) fail(409, 'SLIP_SETTLED');
  if (!['unpaid', 'pending_review'].includes(record.paymentStatus)) fail(409, 'SLIP_STATE');
  const expiry = record.paymentExpiresAt?.toMillis?.();
  if (record.paymentStatus === 'unpaid' && expiry && expiry <= Date.now()) fail(409, 'SLIP_HOLD_EXPIRED');
}
function rasterFormat(bytes) {
  if (bytes.subarray(0,3).equals(Buffer.from([255,216,255]))) return 'jpeg';
  if (bytes.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10]))) return 'png';
  if (bytes.subarray(0,4).toString() === 'RIFF' && bytes.subarray(8,12).toString() === 'WEBP') return 'webp';
  fail(415, 'SLIP_TYPE');
}
export async function normalizeSlip(bytes, declaredType) {
  if (!Buffer.isBuffer(bytes) || !bytes.length || bytes.length > MAX_UPLOAD_BYTES) fail(413, 'SLIP_SIZE', 'ใช้ภาพ JPG, PNG หรือ WebP ขนาดไม่เกิน 2 MB');
  const formats = { 'image/jpeg': 'jpeg', 'image/png': 'png', 'image/webp': 'webp' };
  if (!formats[declaredType]) fail(415, 'SLIP_TYPE', 'ใช้ภาพ JPG, PNG หรือ WebP เท่านั้น');
  if (rasterFormat(bytes) !== formats[declaredType]) fail(415, 'SLIP_TYPE');
  try {
    const image = sharp(bytes, { limitInputPixels: 16_000_000, failOn: 'warning', animated: false });
    const meta = await image.metadata();
    if (meta.format !== formats[declaredType] || (meta.pages || 1) !== 1) fail(415, 'SLIP_TYPE');
    // Full decode/re-encode strips metadata and appended payloads; never serve original untrusted bytes.
    const output = await image.rotate().jpeg({ quality: 95 }).toBuffer();
    if (output.length > MAX_UPLOAD_BYTES) fail(413, 'SLIP_SIZE', 'ภาพหลังประมวลผลเกิน 2 MB กรุณาลดขนาดภาพหรือติดต่อแอดมิน');
    return output;
  } catch (error) {
    if (error.status) throw error;
    fail(415, 'SLIP_IMAGE', 'อ่านภาพไม่ได้ กรุณาเลือกภาพสลิปใหม่');
  }
}
export async function uploadPrivateSlip(req, body, db) {
  const target = await authorizeSlipTarget(req, body, db, 'upload');
  if (!isValidIdempotencyKey(body.idempotencyKey)) fail(400, 'SLIP_IDEMPOTENCY');
  if (typeof body.fileBase64 !== 'string' || body.fileBase64.length > Math.ceil(MAX_UPLOAD_BYTES / 3) * 4 ||
      body.fileBase64.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(body.fileBase64)) fail(413, 'SLIP_SIZE');
  const bytes = Buffer.from(body.fileBase64, 'base64');
  if (bytes.toString('base64') !== body.fileBase64) fail(400, 'SLIP_ENCODING');
  const inputHash = crypto.createHash('sha256').update(bytes).update(String(body.contentType)).digest('hex');
  const receiptId = crypto.createHash('sha256').update(`${target.type}|${target.id}|${target.actor}|${body.idempotencyKey}`).digest('hex');
  const receiptRef = db.collection('private_slip_uploads').doc(receiptId);
  let replay;
  await runSlipTransaction(db, async t => {
    const snap = await t.get(receiptRef);
    if (snap.exists) {
      const existing = snap.data();
      if (existing.inputHash !== inputHash) fail(409, 'SLIP_REPLAY_CONFLICT');
      if (existing.expiresAt.toMillis() <= Date.now()) fail(410, 'SLIP_UPLOAD_EXPIRED');
      if (!['ready', 'consumed'].includes(existing.state)) fail(409, 'SLIP_UPLOAD_IN_PROGRESS');
      replay = true; return;
    }
    t.create(receiptRef, { type: target.type, targetId: target.id, actor: target.actor, inputHash,
      state: 'uploading', expiresAt: Timestamp.fromMillis(Date.now() + 600_000), createdAt: Timestamp.now() });
  });
  const handle = `private:${receiptId}`;
  if (replay) return { ok: true, slipReceipt: handle, replayed: true };
  try {
    const output = await normalizeSlip(bytes, body.contentType);
    const path = `private_slips/${target.type}/${target.id}/${receiptId}.jpg`;
    const file = getAdminBucket(SLIP_BUCKET).file(path);
    await file.save(output, { resumable: false, validation: 'crc32c', preconditionOpts: { ifGenerationMatch: 0 },
      metadata: { contentType: 'image/jpeg', cacheControl: 'private, no-store', contentDisposition: 'attachment; filename="payment-slip.jpg"', metadata: {} } });
    const [metadata] = await file.getMetadata();
    // Admin SDK writes must never mint Firebase download tokens.
    if (metadata.metadata?.firebaseStorageDownloadTokens) fail(503, 'SLIP_TOKEN_UNEXPECTED');
    await receiptRef.update({ state: 'ready', object: { bucket: SLIP_BUCKET, path, generation: String(metadata.generation),
      size: output.length, contentType: 'image/jpeg', sha256: crypto.createHash('sha256').update(output).digest('hex'),
      sourceSha256: crypto.createHash('sha256').update(bytes).digest('hex') } });
    return { ok: true, slipReceipt: handle };
  } catch (error) {
    await receiptRef.update({ state: 'failed' }).catch(() => {});
    throw error;
  }
}
export async function readReceiptInTransaction(t, db, handle, type, targetId, actor) {
  const id = RECEIPT.exec(handle || '')?.[1];
  if (!id) fail(400, 'SLIP_RECEIPT_REQUIRED', 'กรุณารีเฟรชหน้าแล้วอัปโหลดผ่านระบบใหม่');
  const ref = db.collection('private_slip_uploads').doc(id);
  const snap = await t.get(ref);
  const receipt = snap.data();
  if (!receipt || receipt.type !== type || receipt.targetId !== targetId || receipt.actor !== actor) fail(403, 'SLIP_RECEIPT_OWNER');
  if (receipt.state !== 'ready') fail(409, 'SLIP_RECEIPT_USED');
  if (receipt.expiresAt.toMillis() <= Date.now()) fail(410, 'SLIP_UPLOAD_EXPIRED');
  return { ref, object: receipt.object };
}
export async function storedSlipLocation(db, record, type, id) {
  const receiptId = RECEIPT.exec(record.slipUrl || '')?.[1];
  if (receiptId) {
    const receipt = (await db.collection('private_slip_uploads').doc(receiptId).get()).data();
    const object = receipt?.object;
    if (receipt?.state !== 'consumed' || receipt.type !== type || receipt.targetId !== id ||
        object?.bucket !== SLIP_BUCKET || object.path !== `private_slips/${type}/${id}/${receiptId}.jpg` || !/^\d+$/.test(object.generation)) fail(409, 'SLIP_REFERENCE');
    return object;
  }
  // Legacy links are read ONLY from the owned record, never a caller-supplied URL/path.
  try {
    const url = new URL(record.slipUrl);
    const match = url.pathname.match(/^\/v0\/b\/([^/]+)\/o\/(.+)$/);
    const path = match && decodeURIComponent(match[2]);
    const code = type === 'booking' ? record.bookingCode : record.purchaseCode;
    const prefix = type === 'booking' ? 'payment_slips' : 'pass_slips';
    if (url.protocol !== 'https:' || url.hostname !== 'firebasestorage.googleapis.com' || !LEGACY_BUCKETS.includes(match?.[1]) ||
        !ID.test(code || '') || !path?.startsWith(`${prefix}/${code}/`) || path.split('/').some(p => p === '.' || p === '..')) fail(409, 'SLIP_LEGACY_REFERENCE');
    return { bucket: match[1], path };
  } catch (error) { if (error.status) throw error; fail(404, 'SLIP_MISSING'); }
}
export async function readStoredSlip(db, record, type, id) {
  const object = await storedSlipLocation(db, record, type, id);
  const file = getAdminBucket(object.bucket).file(object.path, object.generation ? { generation: object.generation } : {});
  const [metadata] = await file.getMetadata();
  const max = object.generation ? MAX_UPLOAD_BYTES : 6 * 1024 * 1024;
  if (!Number(metadata.size) || Number(metadata.size) > max) fail(413, 'SLIP_SIZE');
  // Bound the stream too: legacy objects are not generation-bound and metadata alone is insufficient.
  const chunks = []; let size = 0;
  for await (const chunk of file.createReadStream()) {
    size += chunk.length; if (size > max) fail(413, 'SLIP_SIZE'); chunks.push(chunk);
  }
  const bytes = Buffer.concat(chunks);
  if (object.sha256 && crypto.createHash('sha256').update(bytes).digest('hex') !== object.sha256) fail(409, 'SLIP_INTEGRITY');
  return { bytes, contentType: metadata.contentType, legacy: !object.generation, sourceSha256: object.sourceSha256 || null };
}
export async function servePrivateSlip(req, res, body, db) {
  const target = await authorizeSlipTarget(req, body, db, 'read');
  const stored = await readStoredSlip(db, target.record, target.type, target.id);
  rasterFormat(stored.bytes); // Historical SVG/GIF/HTML is held for a restricted manual review.
  // Legacy images can be up to 5MB: downscale/re-encode for the bounded proxy response.
  const bytes = stored.legacy ? await sharp(stored.bytes, { limitInputPixels: 16_000_000, failOn: 'warning' }).rotate().resize({ width: 1800, height: 2400, fit: 'inside', withoutEnlargement: true }).jpeg({ quality: 90 }).toBuffer() : stored.bytes;
  if (bytes.length > MAX_UPLOAD_BYTES) fail(413, 'SLIP_SIZE');
  res.setHeader('Content-Type', 'image/jpeg');
  res.setHeader('Content-Disposition', 'attachment; filename="payment-slip.jpg"');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Content-Security-Policy', "default-src 'none'; sandbox");
  return res.status(200).end(bytes);
}
