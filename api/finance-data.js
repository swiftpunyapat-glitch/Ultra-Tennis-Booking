// ════════════════════════════════════════════════════════════════════
// GET /api/finance-data?month=YYYY-MM
// POST/PATCH /api/finance-data — write handling for expense and income
// ════════════════════════════════════════════════════════════════════

import { verifySession, requireRole, DEFAULT_BRANCH_ID } from './_lib/admin-auth.js';
import { getAdminDb, serializeFsDoc } from './_lib/firebase-admin.js';
import { FieldValue } from 'firebase-admin/firestore';

const BUSINESS_UNIT = 'ultra_tennis';
const COMPANY_PROFILE = {
  legalNameTh:       'บริษัท สวิฟท์ สปอร์ตส์ กรุ๊ป จำกัด',
  legalNameEn:       'Swift Sports Group Co., Ltd.',
  brandName:         'Ultra Tennis',
  taxId:             '',
  branch:            'สำนักงานใหญ่',
  address:           '',
  phone:             '',
  email:             '',
  vatRegistered:     false,
  vatRate:           0.07,
  taxInvoiceEnabled: false,
};
const DOCUMENT_TYPES = {
  receipt:         { prefix: 'RC', linkedTypes: ['booking', 'manual_income'] },
  payment_voucher: { prefix: 'PV', linkedTypes: ['expense'] },
};
const SOURCE_COLLECTIONS = {
  booking:       'bookings',
  manual_income: 'finance_income_manual',
  expense:       'finance_expenses',
};

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

function httpError(status, message, extra = {}) {
  return Object.assign(new Error(message), { status, extra });
}

function validDocumentId(v) {
  return typeof v === 'string' && v.length > 0 && v.length <= 200 && !v.includes('/');
}

function documentLockId(docType, linkedType, linkedId) {
  return `active_${docType}_${linkedType}_${encodeURIComponent(linkedId)}`;
}

function buildDocumentSource(docType, linkedType, linkedId, source) {
  const amount = Number(source.price ?? source.amount);
  if (!isPositiveNumber(amount)) {
    throw httpError(409, 'Source record must have a positive amount');
  }
  if (!isValidDate(source.date)) {
    throw httpError(409, 'Source record has an invalid date');
  }

  if (linkedType === 'booking') {
    if (docType !== 'receipt' || source.paymentStatus !== 'paid') {
      throw httpError(409, 'Receipts can only be issued for paid bookings');
    }
    const bookingRef = source.bookingCode || linkedId;
    const time = source.startTime && source.endTime ? ` ${source.startTime}-${source.endTime}` : '';
    const type = source.bookingType ? ` (${source.bookingType})` : '';
    return {
      counterpartyName: String(source.customerName || 'Customer').slice(0, 200),
      counterpartyType: 'customer',
      description:      `Booking ${bookingRef} - ${source.date}${time}${type}`.slice(0, 400),
      paymentMethod:    String(source.paymentMethod || '').slice(0, 100),
      amount,
    };
  }

  if (linkedType === 'manual_income') {
    if (source.deleted) throw httpError(409, 'Cannot issue a receipt for a deleted income record');
    const description = String(source.description || '').trim();
    const category = String(source.category || 'Manual Income').trim();
    return {
      counterpartyName: (description || category || 'Customer').slice(0, 200),
      counterpartyType: 'customer',
      description:      `${category}${description ? ` - ${description}` : ''}`.slice(0, 400),
      paymentMethod:    String(source.paymentMethod || '').slice(0, 100),
      amount,
    };
  }

  if (linkedType === 'expense') {
    if (source.deleted) throw httpError(409, 'Cannot create a voucher for a deleted expense');
    const category = String(source.category || 'Expense').trim();
    const note = String(source.note || '').trim();
    return {
      counterpartyName: String(source.vendor || category || 'Vendor').slice(0, 200),
      counterpartyType: 'vendor',
      description:      `${category}${note ? ` - ${note}` : ''}`.slice(0, 400),
      paymentMethod:    String(source.paymentMethod || '').slice(0, 100),
      amount,
    };
  }

  throw httpError(400, 'Unsupported linkedType');
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

    const { month, action } = req.query;
    if (!month || !/^\d{4}-\d{2}$/.test(month)) {
      return res.status(400).json({ ok: false, error: 'Invalid month — expected YYYY-MM' });
    }
    if (action && action !== 'documents:list') {
      return res.status(400).json({ ok: false, error: 'Unsupported finance action' });
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

    if (action === 'documents:list') {
      return handleDocumentList({ res, db, month });
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

    if (body.action === 'documents:issue') {
      if (req.method !== 'POST') {
        return res.status(400).json({ ok: false, error: 'Document issuance requires POST' });
      }
      return handleDocumentIssue({ res, db, body, session });
    }
    if (body.action === 'documents:void') {
      if (req.method !== 'PATCH') {
        return res.status(400).json({ ok: false, error: 'Document voiding requires PATCH' });
      }
      return handleDocumentVoid({ res, db, body, session });
    }
    if (body.action) {
      return res.status(400).json({ ok: false, error: 'Unsupported finance action' });
    }

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

async function handleDocumentList({ res, db, month }) {
  try {
    const snap = await db.collection('finance_documents')
      .where('month', '==', month)
      .get();
    const documents = snap.docs
      .map(d => ({ id: d.id, ...serializeFsDoc(d.data()) }))
      .filter(d => d.businessUnit === BUSINESS_UNIT)
      .sort((a, b) => String(b.docDate).localeCompare(String(a.docDate)) || String(b.docNo).localeCompare(String(a.docNo)));
    return res.status(200).json({ ok: true, documents });
  } catch (e) {
    console.error('[finance-documents] list error:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to load documents' });
  }
}

async function handleDocumentIssue({ res, db, body, session }) {
  const { docType, linkedType, linkedId } = body;
  const config = DOCUMENT_TYPES[docType];
  if (!config) {
    return res.status(400).json({ ok: false, error: 'docType must be "receipt" or "payment_voucher"' });
  }
  if (!config.linkedTypes.includes(linkedType)) {
    return res.status(400).json({ ok: false, error: `Invalid linkedType for ${docType}` });
  }
  if (!validDocumentId(linkedId)) {
    return res.status(400).json({ ok: false, error: 'Invalid linkedId' });
  }

  const sourceRef = db.collection(SOURCE_COLLECTIONS[linkedType]).doc(linkedId);
  const documentRef = db.collection('finance_documents').doc();
  const lockRef = db.collection('finance_document_counters').doc(documentLockId(docType, linkedType, linkedId));

  try {
    const result = await db.runTransaction(async transaction => {
      const [sourceSnap, lockSnap] = await Promise.all([
        transaction.get(sourceRef),
        transaction.get(lockRef),
      ]);
      if (!sourceSnap.exists) throw httpError(404, 'Linked finance record not found');
      if (lockSnap.exists) {
        const lock = lockSnap.data();
        throw httpError(409, 'An active document already exists for this record', {
          documentId: lock.documentId || null,
          docNo: lock.docNo || null,
        });
      }

      const source = sourceSnap.data();
      if (linkedType !== 'booking' && source.businessUnit && source.businessUnit !== BUSINESS_UNIT) {
        throw httpError(403, 'Cannot issue a document for this record');
      }
      const derived = buildDocumentSource(docType, linkedType, linkedId, source);
      const year = source.date.slice(0, 4);
      const counterRef = db.collection('finance_document_counters').doc(`${docType}_${year}`);
      const counterSnap = await transaction.get(counterRef);
      const previousNumber = counterSnap.exists ? Number(counterSnap.data().lastNumber) || 0 : 0;
      const nextNumber = previousNumber + 1;
      const docNo = `${config.prefix}-${year}-${String(nextNumber).padStart(6, '0')}`;
      const now = FieldValue.serverTimestamp();
      const document = {
        docNo,
        docType,
        docDate:                source.date,
        month:                  source.date.slice(0, 7),
        status:                 'issued',
        counterpartyName:       derived.counterpartyName,
        counterpartyType:       derived.counterpartyType,
        items:                  [{ description: derived.description, quantity: 1, unitPrice: derived.amount, amount: derived.amount }],
        subtotal:               derived.amount,
        discount:               0,
        vatMode:                'non_vat',
        vatAmount:              0,
        total:                  derived.amount,
        paymentMethod:          derived.paymentMethod,
        linkedType,
        linkedId,
        branchId:               source.branchId || DEFAULT_BRANCH_ID,
        businessUnit:           BUSINESS_UNIT,
        createdBy:              session.name,
        createdAt:              now,
        voidedAt:               null,
        voidedBy:               null,
        voidReason:             null,
        companyProfileSnapshot: { ...COMPANY_PROFILE },
      };

      transaction.set(counterRef, {
        docType,
        year,
        lastNumber: nextNumber,
        updatedAt: now,
      }, { merge: true });
      transaction.create(documentRef, document);
      transaction.create(lockRef, {
        kind:       'active_document_lock',
        docType,
        linkedType,
        linkedId,
        documentId: documentRef.id,
        docNo,
        createdAt:  now,
      });
      return { id: documentRef.id, docNo };
    });

    console.log(`[finance-documents] issued ${result.docNo} admin:${session.name}`);
    return res.status(200).json({ ok: true, document: result });
  } catch (e) {
    const status = Number(e.status) || 500;
    if (status >= 500) console.error('[finance-documents] issue error:', e.message);
    return res.status(status).json({ ok: false, error: e.message || 'Failed to issue document', ...(e.extra || {}) });
  }
}

async function handleDocumentVoid({ res, db, body, session }) {
  const { docId } = body;
  const voidReason = typeof body.voidReason === 'string' ? body.voidReason.trim() : '';
  if (!validDocumentId(docId)) {
    return res.status(400).json({ ok: false, error: 'Invalid docId' });
  }
  if (!voidReason) {
    return res.status(400).json({ ok: false, error: 'voidReason is required' });
  }

  const documentRef = db.collection('finance_documents').doc(docId);
  try {
    await db.runTransaction(async transaction => {
      const documentSnap = await transaction.get(documentRef);
      if (!documentSnap.exists) throw httpError(404, 'Document not found');
      const document = documentSnap.data();
      if (document.businessUnit !== BUSINESS_UNIT) throw httpError(403, 'Cannot void this document');
      if (document.status === 'void') throw httpError(409, 'Document is already void');
      if (document.status !== 'issued') throw httpError(409, 'Only issued documents can be voided');

      const lockRef = db.collection('finance_document_counters').doc(
        documentLockId(document.docType, document.linkedType, document.linkedId)
      );
      const lockSnap = await transaction.get(lockRef);
      transaction.update(documentRef, {
        status:     'void',
        voidedAt:   FieldValue.serverTimestamp(),
        voidedBy:   session.name,
        voidReason: voidReason.slice(0, 400),
      });
      if (lockSnap.exists && lockSnap.data().documentId === docId) {
        transaction.delete(lockRef);
      }
    });

    console.log(`[finance-documents] voided id:${docId} admin:${session.name}`);
    return res.status(200).json({ ok: true });
  } catch (e) {
    const status = Number(e.status) || 500;
    if (status >= 500) console.error('[finance-documents] void error:', e.message);
    return res.status(status).json({ ok: false, error: e.message || 'Failed to void document' });
  }
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
