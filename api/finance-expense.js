// ════════════════════════════════════════════════════════════════════
// POST  /api/finance-expense  — create a new expense
// PATCH /api/finance-expense  — update or soft-delete an existing expense
// ════════════════════════════════════════════════════════════════════
// Auth: requires valid adminSession cookie (set by /api/admin-login).
//
// POST body:
//   { date: "YYYY-MM-DD", category, amount, paymentMethod?, vendor?, note? }
//
// PATCH body (update):
//   { id, date: "YYYY-MM-DD", category, amount, paymentMethod?, vendor?, note? }
//
// PATCH body (soft delete):
//   { id, deleted: true }
//
// On success: { ok: true }  (POST create also returns { id })
// On failure: { ok: false, error: "..." }
//
// Server-side validation rejects invalid dates, unknown categories,
// and non-positive amounts before any Firestore write.
// businessUnit is set server-side — never trusted from the client.
// ════════════════════════════════════════════════════════════════════

import { verifySessionCookie } from './_lib/admin-auth.js';
import { getAdminDb } from './_lib/firebase-admin.js';
import { FieldValue } from 'firebase-admin/firestore';

const BUSINESS_UNIT    = 'ultra_tennis';
const VALID_CATEGORIES = ['Rent','Staff','Utility','Equipment','Maintenance','Marketing','Software','Supplies','Refund','Misc'];
const VALID_METHODS    = ['Transfer','Cash','Credit Card','Online','Other'];

function parseBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body); } catch { return null; }
}
function isValidDate(v) {
  return typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v);
}
function isPositiveNumber(v) {
  return Number.isFinite(Number(v)) && Number(v) > 0;
}

export default async function handler(req, res) {
  const adminName = verifySessionCookie(req);
  if (!adminName) return res.status(401).json({ ok: false, error: 'Unauthorized' });

  if (req.method !== 'POST' && req.method !== 'PATCH') {
    return res.status(405).json({ ok: false, error: 'Method not allowed' });
  }

  let db;
  try { db = getAdminDb(); }
  catch (e) {
    console.error('[finance-expense] DB init:', e.message);
    return res.status(500).json({ ok: false, error: 'Database not available' });
  }

  const body = parseBody(req);
  if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

  // ── CREATE ────────────────────────────────────────────────────────
  if (req.method === 'POST') {
    const { date, category, amount, paymentMethod, vendor = '', note = '' } = body;

    if (!isValidDate(date))
      return res.status(400).json({ ok: false, error: 'Invalid date — expected YYYY-MM-DD' });
    if (!VALID_CATEGORIES.includes(category))
      return res.status(400).json({ ok: false, error: `Invalid category. Must be one of: ${VALID_CATEGORIES.join(', ')}` });
    if (!isPositiveNumber(amount))
      return res.status(400).json({ ok: false, error: 'Amount must be a positive number' });
    if (paymentMethod && !VALID_METHODS.includes(paymentMethod))
      return res.status(400).json({ ok: false, error: `Invalid payment method. Must be one of: ${VALID_METHODS.join(', ')}` });

    const ref = await db.collection('finance_expenses').add({
      businessUnit:  BUSINESS_UNIT,
      date:          String(date),
      category:      String(category),
      amount:        Number(amount),
      paymentMethod: String(paymentMethod || 'Transfer'),
      vendor:        String(vendor).slice(0, 200),
      note:          String(note).slice(0, 400),
      deleted:       false,
      addedByAdmin:  adminName,
      createdAt:     FieldValue.serverTimestamp(),
      updatedAt:     FieldValue.serverTimestamp(),
    });

    console.log(`[finance-expense] created id:${ref.id} admin:${adminName}`);
    return res.status(200).json({ ok: true, id: ref.id });
  }

  // ── UPDATE / SOFT-DELETE ──────────────────────────────────────────
  const { id, deleted: softDelete, date, category, amount, paymentMethod, vendor = '', note = '' } = body;

  if (!id || typeof id !== 'string' || !id.trim()) {
    return res.status(400).json({ ok: false, error: 'Missing expense id' });
  }

  // Read document server-side — never trust client-supplied businessUnit.
  const ref = db.collection('finance_expenses').doc(id.trim());
  let existing;
  try {
    const snap = await ref.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Expense not found' });
    existing = snap.data();
  } catch (e) {
    console.error('[finance-expense] read for PATCH:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to read expense' });
  }

  if (existing.businessUnit !== BUSINESS_UNIT) {
    return res.status(403).json({ ok: false, error: 'Cannot modify this record' });
  }
  if (existing.deleted) {
    return res.status(409).json({ ok: false, error: 'Expense is already deleted' });
  }

  // Soft delete
  if (softDelete === true) {
    await ref.update({
      deleted:   true,
      deletedAt: FieldValue.serverTimestamp(),
      deletedBy: adminName,
    });
    console.log(`[finance-expense] soft-deleted id:${id} admin:${adminName}`);
    return res.status(200).json({ ok: true });
  }

  // Regular field update
  if (!isValidDate(date))
    return res.status(400).json({ ok: false, error: 'Invalid date — expected YYYY-MM-DD' });
  if (!VALID_CATEGORIES.includes(category))
    return res.status(400).json({ ok: false, error: `Invalid category. Must be one of: ${VALID_CATEGORIES.join(', ')}` });
  if (!isPositiveNumber(amount))
    return res.status(400).json({ ok: false, error: 'Amount must be a positive number' });
  if (paymentMethod && !VALID_METHODS.includes(paymentMethod))
    return res.status(400).json({ ok: false, error: `Invalid payment method. Must be one of: ${VALID_METHODS.join(', ')}` });

  await ref.update({
    date:           String(date),
    category:       String(category),
    amount:         Number(amount),
    paymentMethod:  String(paymentMethod || 'Transfer'),
    vendor:         String(vendor).slice(0, 200),
    note:           String(note).slice(0, 400),
    updatedByAdmin: adminName,
    updatedAt:      FieldValue.serverTimestamp(),
  });

  console.log(`[finance-expense] updated id:${id} admin:${adminName}`);
  return res.status(200).json({ ok: true });
}
