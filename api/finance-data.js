// ════════════════════════════════════════════════════════════════════
// GET /api/finance-data?month=YYYY-MM
// POST/PATCH /api/finance-data — write handling for expense and income
// ════════════════════════════════════════════════════════════════════

import { verifySession, requireRole } from './_lib/admin-auth.js';
import { getAdminDb, serializeFsDoc } from './_lib/firebase-admin.js';
import { FieldValue } from 'firebase-admin/firestore';

const BUSINESS_UNIT = 'ultra_tennis';

// Expense Categories and Methods
const VALID_EXPENSE_CATEGORIES = ['Rent','Staff','Utility','Equipment','Maintenance','Marketing','Software','Supplies','Refund','Misc'];
const VALID_EXPENSE_METHODS    = ['Transfer','Cash','Credit Card','Online','Other'];

// Income Categories
const VALID_INCOME_CATEGORIES = ['Pass Sale','Lesson','Event','Membership','Misc'];

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
  // GET behavior remains unchanged
  if (req.method === 'GET') {
    const session = verifySession(req);
    if (!session) {
      return res.status(401).json({ ok: false, error: 'Unauthorized' });
    }
    if (!requireRole(session, 'owner', 'ultra_admin')) {
      return res.status(403).json({ ok: false, error: 'Finance access denied' });
    }

    const { month } = req.query;
    if (!month || !/^\d{4}-\d{2}$/.test(month)) {
      return res.status(400).json({ ok: false, error: 'Invalid month — expected YYYY-MM' });
    }

    const [year, m] = month.split('-');
    const startDate = `${year}-${m}-01`;
    const endDate   = `${year}-${m}-31`;

    let db;
    try { db = getAdminDb(); }
    catch (e) {
      console.error('[finance-data] DB init failed:', e.message);
      return res.status(500).json({ ok: false, error: 'Database not available' });
    }

    try {
      const [bookSnap, expSnap, incSnap] = await Promise.all([
        db.collection('bookings')
          .where('date', '>=', startDate)
          .where('date', '<=', endDate)
          .orderBy('date', 'asc')
          .get(),
        db.collection('finance_expenses')
          .where('date', '>=', startDate)
          .where('date', '<=', endDate)
          .orderBy('date', 'asc')
          .get(),
        db.collection('finance_income_manual')
          .where('date', '>=', startDate)
          .where('date', '<=', endDate)
          .orderBy('date', 'asc')
          .get(),
      ]);

      const bookings = bookSnap.docs.map(d => {
        const b = d.data();
        return {
          id:                       d.id,
          date:                     b.date                     ?? '',
          startTime:                b.startTime                ?? '',
          endTime:                  b.endTime                  ?? '',
          customerName:             b.customerName             ?? '',
          paymentStatus:            b.paymentStatus            ?? '',
          bookingStatus:            b.bookingStatus            ?? '',
          price:                    Number(b.price)            || 0,
          bookingType:              b.bookingType              ?? '',
          bookingCode:              b.bookingCode              ?? '',
          packageType:              b.packageType              ?? null,
          packageUsageValuePerHour: b.packageUsageValuePerHour ?? null,
          packageUsageValueTotal:   b.packageUsageValueTotal   ?? null,
          isInfluencerBooking:      b.isInfluencerBooking      ?? false,
        };
      });

      const expenses = expSnap.docs
        .map(d => ({ id: d.id, ...serializeFsDoc(d.data()) }))
        .filter(e => !e.deleted);

      const income = incSnap.docs
        .map(d => ({ id: d.id, ...serializeFsDoc(d.data()) }))
        .filter(i => !i.deleted);

      return res.status(200).json({ ok: true, bookings, expenses, income });
    } catch (e) {
      console.error('[finance-data] query error:', e.message);
      return res.status(500).json({ ok: false, error: 'Failed to load finance data' });
    }
  }

  // POST and PATCH for expense/income write consolidation
  if (req.method === 'POST' || req.method === 'PATCH') {
    const session = verifySession(req);
    if (!session) return res.status(401).json({ ok: false, error: 'Unauthorized' });
    if (!requireRole(session, 'owner', 'ultra_admin')) {
      return res.status(403).json({ ok: false, error: 'Finance access denied' });
    }
    const adminName = session.name;

    let db;
    try { db = getAdminDb(); }
    catch (e) {
      console.error('[finance-data] DB init failed:', e.message);
      return res.status(500).json({ ok: false, error: 'Database not available' });
    }

    const body = parseBody(req);
    if (!body) return res.status(400).json({ ok: false, error: 'Invalid request body' });

    const { type } = body;
    if (type !== 'expense' && type !== 'income') {
      return res.status(400).json({ ok: false, error: 'Invalid type. Must be "expense" or "income"' });
    }

    if (type === 'expense') {
      return handleExpense({ req, res, db, body, adminName });
    } else {
      return handleIncome({ req, res, db, body, adminName });
    }
  }

  return res.status(405).json({ ok: false, error: 'Method not allowed' });
}

async function handleExpense({ req, res, db, body, adminName }) {
  // CREATE (POST)
  if (req.method === 'POST') {
    const { date, category, amount, paymentMethod, vendor = '', note = '' } = body;

    if (!isValidDate(date))
      return res.status(400).json({ ok: false, error: 'Invalid date — expected YYYY-MM-DD' });
    if (!VALID_EXPENSE_CATEGORIES.includes(category))
      return res.status(400).json({ ok: false, error: `Invalid category. Must be one of: ${VALID_EXPENSE_CATEGORIES.join(', ')}` });
    if (!isPositiveNumber(amount))
      return res.status(400).json({ ok: false, error: 'Amount must be a positive number' });
    if (paymentMethod && !VALID_EXPENSE_METHODS.includes(paymentMethod))
      return res.status(400).json({ ok: false, error: `Invalid payment method. Must be one of: ${VALID_EXPENSE_METHODS.join(', ')}` });

    try {
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
    } catch (e) {
      console.error('[finance-expense] create error:', e.message);
      return res.status(500).json({ ok: false, error: 'Failed to create expense' });
    }
  }

  // UPDATE / SOFT-DELETE (PATCH)
  if (req.method === 'PATCH') {
    const { id, deleted: softDelete, date, category, amount, paymentMethod, vendor = '', note = '' } = body;

    if (!id || typeof id !== 'string' || !id.trim()) {
      return res.status(400).json({ ok: false, error: 'Missing expense id' });
    }

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
      try {
        await ref.update({
          deleted:   true,
          deletedAt: FieldValue.serverTimestamp(),
          deletedBy: adminName,
        });
        console.log(`[finance-expense] soft-deleted id:${id} admin:${adminName}`);
        return res.status(200).json({ ok: true });
      } catch (e) {
        console.error('[finance-expense] soft delete error:', e.message);
        return res.status(500).json({ ok: false, error: 'Failed to delete expense' });
      }
    }

    // Regular field update
    if (!isValidDate(date))
      return res.status(400).json({ ok: false, error: 'Invalid date — expected YYYY-MM-DD' });
    if (!VALID_EXPENSE_CATEGORIES.includes(category))
      return res.status(400).json({ ok: false, error: `Invalid category. Must be one of: ${VALID_EXPENSE_CATEGORIES.join(', ')}` });
    if (!isPositiveNumber(amount))
      return res.status(400).json({ ok: false, error: 'Amount must be a positive number' });
    if (paymentMethod && !VALID_EXPENSE_METHODS.includes(paymentMethod))
      return res.status(400).json({ ok: false, error: `Invalid payment method. Must be one of: ${VALID_EXPENSE_METHODS.join(', ')}` });

    try {
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
    } catch (e) {
      console.error('[finance-expense] update error:', e.message);
      return res.status(500).json({ ok: false, error: 'Failed to update expense' });
    }
  }
}

async function handleIncome({ req, res, db, body, adminName }) {
  // CREATE (POST)
  if (req.method === 'POST') {
    const { date, category, amount, description = '' } = body;

    if (!isValidDate(date))
      return res.status(400).json({ ok: false, error: 'Invalid date — expected YYYY-MM-DD' });
    if (!VALID_INCOME_CATEGORIES.includes(category))
      return res.status(400).json({ ok: false, error: `Invalid category. Must be one of: ${VALID_INCOME_CATEGORIES.join(', ')}` });
    if (!isPositiveNumber(amount))
      return res.status(400).json({ ok: false, error: 'Amount must be a positive number' });

    try {
      const ref = await db.collection('finance_income_manual').add({
        businessUnit: BUSINESS_UNIT,
        date:         String(date),
        category:     String(category),
        amount:       Number(amount),
        description:  String(description).slice(0, 300),
        deleted:      false,
        addedByAdmin: adminName,
        createdAt:    FieldValue.serverTimestamp(),
        updatedAt:    FieldValue.serverTimestamp(),
      });

      console.log(`[finance-income] created id:${ref.id} admin:${adminName}`);
      return res.status(200).json({ ok: true, id: ref.id });
    } catch (e) {
      console.error('[finance-income] create error:', e.message);
      return res.status(500).json({ ok: false, error: 'Failed to create income entry' });
    }
  }

  // UPDATE / SOFT-DELETE (PATCH)
  if (req.method === 'PATCH') {
    const { id, deleted: softDelete, date, category, amount, description = '' } = body;

    if (!id || typeof id !== 'string' || !id.trim()) {
      return res.status(400).json({ ok: false, error: 'Missing income id' });
    }

    const ref = db.collection('finance_income_manual').doc(id.trim());
    let existing;
    try {
      const snap = await ref.get();
      if (!snap.exists) return res.status(404).json({ ok: false, error: 'Income entry not found' });
      existing = snap.data();
    } catch (e) {
      console.error('[finance-income] read for PATCH:', e.message);
      return res.status(500).json({ ok: false, error: 'Failed to read income entry' });
    }

    if (existing.businessUnit !== BUSINESS_UNIT) {
      return res.status(403).json({ ok: false, error: 'Cannot modify this record' });
    }
    if (existing.deleted) {
      return res.status(409).json({ ok: false, error: 'Income entry is already deleted' });
    }

    // Soft delete
    if (softDelete === true) {
      try {
        await ref.update({
          deleted:   true,
          deletedAt: FieldValue.serverTimestamp(),
          deletedBy: adminName,
        });
        console.log(`[finance-income] soft-deleted id:${id} admin:${adminName}`);
        return res.status(200).json({ ok: true });
      } catch (e) {
        console.error('[finance-income] soft delete error:', e.message);
        return res.status(500).json({ ok: false, error: 'Failed to delete income entry' });
      }
    }

    // Regular field update
    if (!isValidDate(date))
      return res.status(400).json({ ok: false, error: 'Invalid date — expected YYYY-MM-DD' });
    if (!VALID_INCOME_CATEGORIES.includes(category))
      return res.status(400).json({ ok: false, error: `Invalid category. Must be one of: ${VALID_INCOME_CATEGORIES.join(', ')}` });
    if (!isPositiveNumber(amount))
      return res.status(400).json({ ok: false, error: 'Amount must be a positive number' });

    try {
      await ref.update({
        date:           String(date),
        category:       String(category),
        amount:         Number(amount),
        description:    String(description).slice(0, 300),
        updatedByAdmin: adminName,
        updatedAt:      FieldValue.serverTimestamp(),
      });

      console.log(`[finance-income] updated id:${id} admin:${adminName}`);
      return res.status(200).json({ ok: true });
    } catch (e) {
      console.error('[finance-income] update error:', e.message);
      return res.status(500).json({ ok: false, error: 'Failed to update income entry' });
    }
  }
}
