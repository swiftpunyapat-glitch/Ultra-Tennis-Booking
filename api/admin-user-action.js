// ════════════════════════════════════════════════════════════════════
// POST /api/admin-user-action — Admin user management, package and
// pricing actions
// ════════════════════════════════════════════════════════════════════
// Auth: requires valid admin session cookie.
// Actions:
//   add_pass_to_registered_user            (branch_manager or above)
//   save_store_pricing                      (Art owner only)
//   save_special_promotion                 (owner-only — merged in from
//   deactivate_special_promotion            the former /api/admin-pricing-action
//                                           route to keep the Vercel function
//                                           count down; same request/response)
// ════════════════════════════════════════════════════════════════════

import { verifySession, requireRole, hasBranchAccess, resolveBranchId, DEFAULT_BRANCH_ID } from './_lib/admin-auth.js';
import { getAdminDb, writeAuditLog } from './_lib/firebase-admin.js';
import { sendAndLog, loadActiveAdmins } from './_lib/notify.js';
import { FieldValue, Timestamp } from 'firebase-admin/firestore';
import {
  generateVoucherCodes, normalizeCampaignInput,
  normalizeCustomVoucherCode, normalizeRandomCodeRequest, normalizeVoucherImportRecords,
} from './_lib/voucher-admin.js';

const ACTIVE_PACKAGES = {
  ultra_starter_3: {
    packageType: "ultra_starter_3",
    packageName: "Ultra Starter",
    price: 999,
    totalMinutes: 180,
    validityDays: 30,
    ownerRole: "customer"
  },
  ultra_pass_10: {
    packageType: "ultra_pass_10",
    packageName: "Ultra Pass 10 Hours",
    price: 3100,
    totalMinutes: 600,
    validityDays: 90,
    ownerRole: "customer"
  },
  ultra_pass_20: {
    packageType: "ultra_pass_20",
    packageName: "Ultra Pass 20 Hours",
    price: 5900,
    totalMinutes: 1200,
    validityDays: 180,
    ownerRole: "customer"
  },
  beginner_coaching_5: {
    packageType: "beginner_coaching_5",
    packageName: "Beginner Coaching",
    price: 4000,               // "เริ่มต้น 4,000" — actual price may vary per coach
    totalMinutes: 300,
    validityDays: 60,
    ownerRole: "customer",
    requiresCoachOrAdminBooking: true
  },
  // Off-Peak Pass (Stage C, 2026-07 rules): ฿3,600 · 16 hours total within 30
  // days of purchase · max 4 hrs per ISO week (Mon–Sun) · Mon–Fri 09:00–15:00
  // excl. holidays. Unused hours expire with the pass — no carry-over/refund.
  offpeak: {
    packageType: "offpeak",
    packageName: "Off-Peak Pass",
    price: 3600,
    totalMinutes: 960,          // 16 hours hard total (deducted per booking)
    validityDays: 30,
    ownerRole: "customer",
    weeklyLimitHours: 4,
    monthlyLimitHours: 16       // safety net; real total cap = remainingMinutes
  },
  coach_at_ultra_10: {
    packageType: "coach_at_ultra_10",
    packageName: "Coaching at Ultra",
    price: 3000,
    // Legacy key retained for compatibility; product entitlement is 5 hours.
    totalMinutes: 300,
    validityDays: 60,
    ownerRole: "coach",
    requireStudentInfo: true
  },
  monstr_event_pass: {
    packageType: "monstr_event_pass",
    packageName: "MONSTR Event Pass",
    price: 0,
    totalMinutes: 60,        // single 1-hour use (deduct 60 → 0)
    validityDays: 30,        // default; admin may override with eventEndDate
    ownerRole: "customer",
    isEventPass: true,
    restrictDays: [1, 2, 3, 4, 5]   // Mon-Fri (booking also rejects holidays)
  }
};

const normalizePhone = p => String(p || "").replace(/\D/g, "");

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method Not Allowed' });
  }

  const session = verifySession(req);
  if (!session) {
    return res.status(401).json({ ok: false, error: 'Unauthorized admin session' });
  }
  const adminName = session.name;

  const { action, targetUserId, packageType, validFrom, note, eventEndDate, eventName } = req.body || {};

  // Voucher Manager is deliberately pinned to the same single operator as
  // store pricing. The UI is hidden for everyone else, but this server gate is
  // the authority and protects reads as well as writes.
  const voucherActions = new Set([
    'voucher_list', 'voucher_save_campaign', 'voucher_set_campaign_active',
    'voucher_create_codes', 'voucher_import_codes', 'voucher_set_code_active',
    'event_pass_list_requests', 'event_pass_approve_request',
    'event_pass_reject_request', 'event_pass_reset_code',
  ]);
  if (voucherActions.has(action)) {
    if (adminName !== 'Art' || !requireRole(session, 'owner')) {
      return res.status(403).json({ ok: false, error: 'Access denied: Art owner only.' });
    }
    return handleVoucherAction({ req, res, adminName, session, action });
  }

  // ── Pricing actions (owner-only) ─────────────────────────────────
  if (action === 'save_store_pricing' || action === 'save_special_promotion' || action === 'deactivate_special_promotion') {
    // Pricing is intentionally pinned to one authenticated operator. The UI
    // also hides these controls, but this server check is the security boundary.
    if (adminName !== 'Art' || !requireRole(session, 'owner')) {
      return res.status(403).json({ ok: false, error: 'Access denied: Art owner only.' });
    }
    return handlePricingAction({ req, res, adminName, session, action });
  }

  const passFinancialActions = new Set([
    'add_pass_to_registered_user', 'adjust_pass_minutes', 'deactivate_pass',
    'list_pending_pass_purchases', 'approve_pass_purchase', 'reject_pass_purchase',
  ]);
  if (passFinancialActions.has(action)) {
    if (!requireRole(session, 'owner', 'ultra_admin', 'branch_manager')) {
      return res.status(403).json({ ok: false, error: 'Role cannot perform pass financial actions' });
    }
    if (!hasBranchAccess(session, DEFAULT_BRANCH_ID)) {
      return res.status(403).json({ ok: false, error: 'No access to this branch' });
    }
  }

  // ── Pass actions (any valid admin — consistent with add_pass) ────
  if (action === 'adjust_pass_minutes') {
    return handleAdjustPassMinutes({ req, res, adminName, session });
  }
  if (action === 'deactivate_pass') {
    return handleDeactivatePass({ req, res, adminName, session });
  }

  // ── Pass self-purchase approval (Stage D — any valid admin) ──────
  if (action === 'list_pending_pass_purchases') {
    return handleListPendingPassPurchases({ res });
  }
  if (action === 'approve_pass_purchase') {
    return handleApprovePassPurchase({ req, res, adminName, session });
  }
  if (action === 'reject_pass_purchase') {
    return handleRejectPassPurchase({ req, res, adminName, session });
  }

  if (action !== 'add_pass_to_registered_user') {
    return res.status(400).json({ ok: false, error: `Unsupported action: ${action}` });
  }

  if (!targetUserId) {
    return res.status(400).json({ ok: false, error: 'targetUserId is required' });
  }

  if (!packageType) {
    return res.status(400).json({ ok: false, error: 'packageType is required' });
  }

  const pkg = ACTIVE_PACKAGES[packageType];
  if (!pkg) {
    return res.status(400).json({
      ok: false,
      error: `Invalid or deprecated packageType: ${packageType}. New flow only issues: ultra_starter_3, ultra_pass_10, ultra_pass_20, beginner_coaching_5, coach_at_ultra_10, offpeak.`
    });
  }

  try {
    const db = getAdminDb();
    
    // 1. Load target registered user document
    const userRef = db.collection('registered_users').doc(targetUserId);
    const userSnap = await userRef.get();
    if (!userSnap.exists) {
      return res.status(404).json({ ok: false, error: `Registered user not found for ID: ${targetUserId}` });
    }

    const userData = userSnap.data();

    // 2. Parse validFrom/validUntil in Bangkok timezone (UTC+7)
    let vFrom;
    if (validFrom) {
      if (/^\d{4}-\d{2}-\d{2}$/.test(validFrom)) {
        vFrom = new Date(`${validFrom}T00:00:00+07:00`);
      } else {
        vFrom = new Date(validFrom);
      }
      if (isNaN(vFrom.getTime())) {
        return res.status(400).json({ ok: false, error: `Invalid date format for validFrom: ${validFrom}` });
      }
    } else {
      vFrom = new Date();
    }

    // Event passes may carry an explicit expiry = end of the event day (Bangkok);
    // otherwise fall back to validityDays from validFrom.
    let vUntil;
    if (pkg.isEventPass && eventEndDate) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(eventEndDate)) {
        return res.status(400).json({ ok: false, error: `Invalid eventEndDate (expected YYYY-MM-DD): ${eventEndDate}` });
      }
      vUntil = new Date(`${eventEndDate}T23:59:59+07:00`);
      if (isNaN(vUntil.getTime())) {
        return res.status(400).json({ ok: false, error: `Invalid eventEndDate: ${eventEndDate}` });
      }
    } else {
      vUntil = new Date(vFrom.getTime() + pkg.validityDays * 24 * 60 * 60 * 1000);
    }

    // 3. Prepare package payload
    const packagePayload = {
      lineUserId: targetUserId,
      customerName: userData.name || "",
      customerPhone: userData.phone || "",
      customerPhoneNormalized: userData.phoneNormalized || normalizePhone(userData.phone),
      lineDisplayName: userData.lineDisplayName || "",
      packageType: pkg.packageType,
      packageName: pkg.packageName,
      price: pkg.price,
      ownerRole: pkg.ownerRole,
      totalMinutes: pkg.totalMinutes,
      remainingMinutes: pkg.totalMinutes,
      validityDays: pkg.validityDays,
      validFrom: Timestamp.fromDate(vFrom),
      validUntil: Timestamp.fromDate(vUntil),
      status: "active",
      addedByAdmin: adminName,
      createdFromPurchaseCode: null,
      createdAt: FieldValue.serverTimestamp(),
      source: "admin_registered_user_add_pass",
      note: note || "",
      weeklyLimitHours: pkg.weeklyLimitHours ?? null,
      monthlyLimitHours: pkg.monthlyLimitHours ?? null,
      weeklyUsage: {},
      monthlyUsage: {}
    };

    // Conditional requirement fields based on package configuration
    if (pkg.requireStudentInfo) {
      packagePayload.requireStudentInfo = true;
    }
    if (pkg.requiresCoachOrAdminBooking) {
      packagePayload.requiresCoachOrAdminBooking = true;
    }
    // Event-pass fields (MONSTR Event Pass): single-use, Mon-Fri + non-holiday,
    // hard-scoped to ladprao1/room1. Booking flow enforces the restrictions.
    if (pkg.isEventPass) {
      packagePayload.isEventPass  = true;
      packagePayload.restrictDays = pkg.restrictDays || [1, 2, 3, 4, 5];
      packagePayload.branchId     = "ladprao1";
      packagePayload.resourceId   = "room1";
      packagePayload.eventUsedAt  = null;
      if (typeof eventName === "string" && eventName.trim()) {
        packagePayload.eventName = eventName.trim().slice(0, 120);
      }
    }

    // 4. Create new document in customer_packages
    const pkgRef = await db.collection('customer_packages').add(packagePayload);

    console.log(`[admin-user-action] Created pass ${pkgRef.id} for user ${targetUserId} by admin ${adminName}`);
    await writeAuditLog(db, {
      actor: adminName,
      actorRole: session.role,
      branchId: DEFAULT_BRANCH_ID,
      action: 'add_pass',
      targetId: pkgRef.id,
      after: {
        status: 'active',
        packageType: pkg.packageType,
        packageName: pkg.packageName,
        customerName: userData.name || '',
      },
      note: `เพิ่มแพ็กเกจ ${pkg.packageName} ให้ ${userData.name || targetUserId}`,
    });

    return res.status(200).json({ ok: true, packageId: pkgRef.id });
  } catch (err) {
    console.error('[admin-user-action] Error adding pass to registered user:', err);
    return res.status(500).json({ ok: false, error: err.message || 'Internal Server Error' });
  }
}

// ════════════════════════════════════════════════════════════════════
// handlePricingAction — manage special promotions (owner-only)
// (moved verbatim from the former /api/admin-pricing-action route)
// ════════════════════════════════════════════════════════════════════
async function handlePricingAction({ req, res, adminName, session, action }) {
  const {
    promoActive, promoName, promoPrice, promoLabel, startsAt, endsAt,
    morningPromoActive, halfHourPrice, normalSingleUsePrice,
    morningPrice, morningAdvancePrice, morningAdvanceHours, lateNightPrice,
  } = req.body || {};

  try {
    const db = getAdminDb();
    const docRef = db.collection('system_settings').doc('pricing');

    if (action === 'deactivate_special_promotion') {
      const beforeSnap = await docRef.get();
      await docRef.set({
        specialPromoActive: false,
        updatedAt: FieldValue.serverTimestamp(),
        updatedBy: adminName
      }, { merge: true });

      console.log(`[admin-user-action] Deactivated special promotion by ${adminName}`);
      await writeAuditLog(db, {
        actor: adminName, actorRole: session.role,
        action: 'deactivate_special_promotion', targetId: 'system_settings/pricing',
        before: beforeSnap.exists ? { specialPromoActive: beforeSnap.data().specialPromoActive === true } : null,
        after: { specialPromoActive: false },
      });
      return res.status(200).json({ ok: true });
    }

    // Promotion details may stay blank while inactive, allowing Art to update
    // only the store's base rates. Activating a promotion requires both fields.
    const promoIsActive = Boolean(promoActive);
    const promoNameClean = String(promoName || '').trim();
    const hasPromoPrice = promoPrice !== undefined && promoPrice !== null && promoPrice !== '';
    const price = hasPromoPrice ? Number(promoPrice) : undefined;
    if (hasPromoPrice && (!Number.isInteger(price) || price < 100 || price > 5000)) {
      return res.status(400).json({ ok: false, error: 'Promotion price must be an integer 100-5000 THB' });
    }
    if (promoIsActive && (!promoNameClean || price === undefined)) {
      return res.status(400).json({ ok: false, error: 'Active promotion requires a name and price' });
    }

    // All store-rate fields are optional for backward compatibility with a
    // previously cached admin.html. Only values actually sent are merged.
    const optionalInteger = (raw, label, min, max) => {
      if (raw === undefined || raw === null || raw === '') return { value: undefined };
      const value = Number(raw);
      return Number.isInteger(value) && value >= min && value <= max
        ? { value }
        : { error: `${label} must be an integer ${min}-${max}` };
    };
    const rateInputs = {
      normalSingleUsePrice: optionalInteger(normalSingleUsePrice, 'Standard price', 100, 5000),
      halfHourPrice: optionalInteger(halfHourPrice, 'Half-hour price', 100, 1000),
      morningPrice: optionalInteger(morningPrice, 'Morning price', 100, 5000),
      morningAdvancePrice: optionalInteger(morningAdvancePrice, 'Morning advance price', 100, 5000),
      morningAdvanceHours: optionalInteger(morningAdvanceHours, 'Morning advance hours', 1, 720),
      lateNightPrice: optionalInteger(lateNightPrice, 'Late-night price', 100, 5000),
    };
    const invalidRate = Object.values(rateInputs).find(x => x.error);
    if (invalidRate) return res.status(400).json({ ok: false, error: invalidRate.error });
    const rateUpdate = Object.fromEntries(
      Object.entries(rateInputs).filter(([, result]) => result.value !== undefined).map(([key, result]) => [key, result.value])
    );

    let startTS = null;
    let startDate = null;
    if (startsAt) {
      // StartsAt datetime-local is interpreted as Bangkok local time
      // If startsAt is "YYYY-MM-DDTHH:mm", Vercel parses it as "YYYY-MM-DDTHH:mm:00+07:00"
      let startsStr = startsAt;
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(startsStr)) {
        startsStr = `${startsStr}:00+07:00`;
      }
      const d = new Date(startsStr);
      if (isNaN(d.getTime())) {
        return res.status(400).json({ ok: false, error: `Invalid startsAt date format: ${startsAt}` });
      }
      startDate = d;
      startTS = Timestamp.fromDate(d);
    }

    let endTS = null;
    let endDate = null;
    if (endsAt) {
      // EndsAt datetime-local is interpreted as Bangkok local time
      let endsStr = endsAt;
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(endsStr)) {
        endsStr = `${endsStr}:00+07:00`;
      }
      const d = new Date(endsStr);
      if (isNaN(d.getTime())) {
        return res.status(400).json({ ok: false, error: `Invalid endsAt date format: ${endsAt}` });
      }
      endDate = d;
      endTS = Timestamp.fromDate(d);
    }

    if (startDate && endDate && endDate.getTime() <= startDate.getTime()) {
      return res.status(400).json({ ok: false, error: 'Ends At date must be after Starts At date' });
    }

    const beforeSnap = await docRef.get();
    const beforeData = beforeSnap.exists ? beforeSnap.data() : {};
    const settingsUpdate = {
      ...rateUpdate,
      pricingSchemaVersion: 3,
      // Morning 330/320 kill-switch. Written only when the client sends it, so
      // an older admin.html can't silently flip the promo off.
      ...(typeof morningPromoActive === 'boolean' ? { morningPromoActive } : {}),
      specialPromoActive: promoIsActive,
      specialPromoName: promoNameClean,
      ...(price !== undefined ? { specialPromoPrice: price } : {}),
      specialPromoLabel: String(promoLabel || "").trim(),
      specialPromoStartsAt: startTS,
      specialPromoEndsAt: endTS,
      normalQrUrl: "/payment-qr.png",
      specialQrUrl: "/payment-qr-special.png",
      lateNightQrUrl: "/late-night-qr.png",
      updatedAt: FieldValue.serverTimestamp(),
      updatedBy: adminName
    };
    await docRef.set(settingsUpdate, { merge: true });

    console.log(`[admin-user-action] Saved store pricing and promotion settings by ${adminName}`);
    await writeAuditLog(db, {
      actor: adminName, actorRole: session.role,
      action: action === 'save_store_pricing' ? 'save_store_pricing' : 'save_special_promotion',
      targetId: 'system_settings/pricing',
      before: {
        ...Object.fromEntries(Object.keys(rateUpdate).map(key => [key, beforeData[key] ?? null])),
        morningPromoActive: beforeData.morningPromoActive !== false,
        specialPromoActive: beforeData.specialPromoActive === true,
        specialPromoName: beforeData.specialPromoName || '',
        specialPromoPrice: beforeData.specialPromoPrice ?? null,
      },
      after: {
        ...rateUpdate,
        morningPromoActive: typeof morningPromoActive === 'boolean' ? morningPromoActive : beforeData.morningPromoActive !== false,
        specialPromoActive: promoIsActive,
        specialPromoName: promoNameClean,
        specialPromoPrice: price ?? beforeData.specialPromoPrice ?? null,
      },
    });
    return res.status(200).json({ ok: true });
  } catch (err) {
    console.error('[admin-user-action] pricing error:', err);
    return res.status(500).json({ ok: false, error: err.message || 'Internal Server Error' });
  }
}

// ════════════════════════════════════════════════════════════════════
// Pass self-purchase approval (Stage D)
// ════════════════════════════════════════════════════════════════════
// GUARDRAIL: a package is issued ONLY here, after an admin explicitly
// approves — never from a slip upload. Idempotency: issuedPackageId on the
// purchase doc is set inside the same transaction that creates the package,
// so double-clicks / duplicate slips / retries can never issue twice.

async function handleListPendingPassPurchases({ res }) {
  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[list_pending_pass_purchases] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }
  try {
    const snap = await db.collection('pass_purchases')
      .where('paymentStatus', '==', 'pending_review').get();
    const items = snap.docs.map(d => {
      const p = d.data();
      return {
        id: d.id,
        purchaseCode: p.purchaseCode ?? null,
        packageType: p.packageType ?? null,
        packageName: p.packageName ?? null,
        price: p.price ?? null,
        customerName: p.customerName ?? '',
        customerPhone: p.customerPhone ?? '',
        lineUserId: p.lineUserId ?? null,
        slipUrl: (typeof p.slipUrl === 'string' && /^https?:\/\//.test(p.slipUrl)) ? p.slipUrl : null,
        slipUploadedAt: p.slipUploadedAt?.toMillis?.() ?? null,
        createdAt: p.createdAt?.toMillis?.() ?? null,
        paymentVerification: p.paymentVerification
          ? { status: p.paymentVerification.status ?? null, reason: p.paymentVerification.reason ?? null }
          : null,
      };
    }).sort((a, b) => (a.slipUploadedAt || 0) - (b.slipUploadedAt || 0));
    return res.status(200).json({ ok: true, purchases: items });
  } catch (e) {
    console.error('[list_pending_pass_purchases]', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to load pending purchases' });
  }
}

async function handleApprovePassPurchase({ req, res, adminName, session }) {
  const { purchaseId } = req.body || {};
  if (!purchaseId || typeof purchaseId !== 'string' || !purchaseId.trim()) {
    return res.status(400).json({ ok: false, error: 'purchaseId is required' });
  }
  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[approve_pass_purchase] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  const purchaseRef = db.collection('pass_purchases').doc(purchaseId.trim());
  let issued;
  try {
    issued = await db.runTransaction(async (t) => {
      const snap = await t.get(purchaseRef);
      if (!snap.exists) throw new Error('NOT_FOUND');
      const p = snap.data();
      if (p.issuedPackageId) throw new Error('ALREADY_ISSUED');
      if (p.status === 'rejected' || p.paymentStatus === 'rejected') throw new Error('REJECTED');
      if (p.paymentStatus !== 'pending_review') throw new Error('BAD_STATE');
      const pkg = ACTIVE_PACKAGES[p.packageType];
      if (!pkg) throw new Error('UNSUPPORTED_TYPE');

      const vFrom = new Date();
      const vUntil = new Date(vFrom.getTime() + pkg.validityDays * 24 * 60 * 60 * 1000);
      const pkgRef = db.collection('customer_packages').doc();
      t.set(pkgRef, {
        lineUserId: p.lineUserId,
        customerName: p.customerName || '',
        customerPhone: p.customerPhone || '',
        customerPhoneNormalized: p.customerPhoneNormalized || normalizePhone(p.customerPhone),
        lineDisplayName: p.lineDisplayName || '',
        packageType: pkg.packageType,
        packageName: pkg.packageName,
        price: Number(p.price) || pkg.price,      // purchase-time snapshot wins
        ownerRole: pkg.ownerRole,
        totalMinutes: pkg.totalMinutes,
        remainingMinutes: pkg.totalMinutes,
        validityDays: pkg.validityDays,
        validFrom: Timestamp.fromDate(vFrom),
        validUntil: Timestamp.fromDate(vUntil),
        status: 'active',
        addedByAdmin: adminName,
        createdFromPurchaseCode: p.purchaseCode || null,
        createdAt: FieldValue.serverTimestamp(),
        source: 'self_purchase_approved',
        note: '',
        weeklyLimitHours: pkg.weeklyLimitHours ?? null,
        monthlyLimitHours: pkg.monthlyLimitHours ?? null,
        weeklyUsage: {},
        monthlyUsage: {},
      });
      t.update(purchaseRef, {
        status: 'completed',
        paymentStatus: 'paid',
        issuedPackageId: pkgRef.id,
        approvedBy: adminName,
        approvedAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
      });
      return { packageId: pkgRef.id, purchase: p, pkg, vUntil };
    });
  } catch (e) {
    const map = {
      NOT_FOUND:        [404, 'Purchase not found'],
      ALREADY_ISSUED:   [409, 'Pass already issued for this purchase'],
      REJECTED:         [409, 'Purchase was rejected'],
      BAD_STATE:        [409, 'Purchase is not awaiting review'],
      UNSUPPORTED_TYPE: [409, 'Unsupported packageType on this purchase'],
    };
    const [code, msg] = map[e.message] || [500, 'Failed to approve purchase'];
    if (code === 500) console.error('[approve_pass_purchase] tx:', e.message);
    return res.status(code).json({ ok: false, error: msg });
  }

  const p = issued.purchase;
  console.log(`[approve_pass_purchase] ${p.purchaseCode} → package ${issued.packageId} by ${adminName}`);
  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role, branchId: DEFAULT_BRANCH_ID,
    action: 'approve_pass_purchase', targetId: purchaseId.trim(),
    before: { paymentStatus: 'pending_review' },
    after:  { paymentStatus: 'paid', issuedPackageId: issued.packageId },
    note: `อนุมัติซื้อ ${p.packageName} ของ ${p.customerName || p.lineUserId}`,
  });

  // Notify customer + all admins — never fails the request.
  try {
    const validUntilStr = issued.vUntil.toLocaleDateString('en-GB', { timeZone: 'Asia/Bangkok' });
    const sends = [];
    sends.push(sendAndLog({
      eventId: `${p.purchaseCode}_pass_activated_customer`,
      type: 'pass_activated_customer', targetType: 'customer',
      lineUserId: p.lineUserId, bookingCode: p.purchaseCode,
      payload: { packageName: p.packageName, remainingMinutes: issued.pkg.totalMinutes, validUntil: validUntilStr },
    }).catch(e => ({ ok: false, error: e.message })));
    const admins = await loadActiveAdmins();
    admins.forEach(a => sends.push(sendAndLog({
      eventId: `${p.purchaseCode}_pass_issued_${a.lineUserId}`,
      type: 'pass_issued_admin', targetType: 'admin',
      lineUserId: a.lineUserId, bookingCode: p.purchaseCode,
      payload: {
        purchaseCode: p.purchaseCode, customerName: p.customerName, customerPhone: p.customerPhone,
        packageName: p.packageName, price: p.price, actionBy: adminName,
      },
    }).catch(e => ({ ok: false, error: e.message }))));
    await Promise.all(sends);
  } catch (e) {
    console.error('[approve_pass_purchase] notify (non-fatal):', e.message);
  }

  return res.status(200).json({ ok: true, packageId: issued.packageId });
}

async function handleRejectPassPurchase({ req, res, adminName, session }) {
  const { purchaseId, reason = '' } = req.body || {};
  if (!purchaseId || typeof purchaseId !== 'string' || !purchaseId.trim()) {
    return res.status(400).json({ ok: false, error: 'purchaseId is required' });
  }
  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[reject_pass_purchase] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  const purchaseRef = db.collection('pass_purchases').doc(purchaseId.trim());
  let p;
  try {
    p = await db.runTransaction(async (t) => {
      const snap = await t.get(purchaseRef);
      if (!snap.exists) throw new Error('NOT_FOUND');
      const cur = snap.data();
      if (cur.issuedPackageId) throw new Error('ALREADY_ISSUED');
      if (cur.paymentStatus !== 'pending_review') throw new Error('BAD_STATE');
      t.update(purchaseRef, {
        status: 'rejected',
        paymentStatus: 'rejected',
        rejectReason: String(reason || '').slice(0, 400),
        rejectedBy: adminName,
        rejectedAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
      });
      return cur;
    });
  } catch (e) {
    const map = {
      NOT_FOUND:      [404, 'Purchase not found'],
      ALREADY_ISSUED: [409, 'Pass already issued — cannot reject (use pass controls instead)'],
      BAD_STATE:      [409, 'Purchase is not awaiting review'],
    };
    const [code, msg] = map[e.message] || [500, 'Failed to reject purchase'];
    if (code === 500) console.error('[reject_pass_purchase] tx:', e.message);
    return res.status(code).json({ ok: false, error: msg });
  }

  console.log(`[reject_pass_purchase] ${p.purchaseCode} by ${adminName}`);
  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role, branchId: DEFAULT_BRANCH_ID,
    action: 'reject_pass_purchase', targetId: purchaseId.trim(),
    before: { paymentStatus: 'pending_review' },
    after:  { paymentStatus: 'rejected' },
    note: `ปฏิเสธการซื้อ ${p.packageName} ของ ${p.customerName || p.lineUserId}${reason ? ` · ${reason}` : ''}`,
  });
  try {
    await sendAndLog({
      eventId: `${p.purchaseCode}_pass_purchase_rejected`,
      type: 'pass_purchase_rejected_customer', targetType: 'customer',
      lineUserId: p.lineUserId, bookingCode: p.purchaseCode,
      payload: { purchaseCode: p.purchaseCode, packageName: p.packageName, reason: String(reason || '').slice(0, 200) },
    });
  } catch (e) {
    console.error('[reject_pass_purchase] notify (non-fatal):', e.message);
  }
  return res.status(200).json({ ok: true });
}

// ════════════════════════════════════════════════════════════════════
// handleAdjustPassMinutes — add / deduct / set remaining minutes on an
// Ultra (minute-based) pass. Runs in a transaction that recomputes from the
// CURRENT DB value (not a client-supplied old value) so concurrent admin
// edits never lose an update. Writes an immutable customer_package_logs row.
// (Server-side via Admin SDK bypasses Firestore rules, which only permit the
//  ultra_10/ultra_20 enum for that log collection — this is intentional.)
// ════════════════════════════════════════════════════════════════════
async function handleAdjustPassMinutes({ req, res, adminName, session }) {
  const { passId, adjustAction, value: rawValue, reason = '' } = req.body || {};

  const VALID_ADJUST = ['add_minutes', 'deduct_minutes', 'set_minutes'];
  if (!passId || typeof passId !== 'string' || !passId.trim()) {
    return res.status(400).json({ ok: false, error: 'passId is required' });
  }
  if (!VALID_ADJUST.includes(adjustAction)) {
    return res.status(400).json({ ok: false, error: `Invalid adjustAction. Must be one of: ${VALID_ADJUST.join(', ')}` });
  }
  const value = Number(rawValue);
  if (!Number.isInteger(value) || value <= 0 || value > 100000) {
    return res.status(400).json({ ok: false, error: 'value must be a positive whole number of minutes up to 100000' });
  }

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[adjust_pass_minutes] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  const pkgRef = db.collection('customer_packages').doc(passId.trim());

  let result;
  try {
    result = await db.runTransaction(async (t) => {
      const snap = await t.get(pkgRef);
      if (!snap.exists) throw new Error('NOT_FOUND');
      const pkg = snap.data();
      if (!hasBranchAccess(session, resolveBranchId(pkg))) throw new Error('NO_BRANCH');

      // Minute controls apply to Ultra (minute-based) passes only.
      // Off-Peak passes track usage maps, not a minute counter.
      if (!String(pkg.packageType || '').includes('ultra')) throw new Error('NOT_ULTRA');

      const oldRemaining = Number(pkg.remainingMinutes) || 0;
      let newRemaining;
      if (adjustAction === 'add_minutes')         newRemaining = oldRemaining + value;
      else if (adjustAction === 'deduct_minutes') newRemaining = oldRemaining - value;
      else                                        newRemaining = value; // set_minutes
      if (newRemaining < 0) throw new Error('BELOW_ZERO');

      const deltaMinutes = newRemaining - oldRemaining;

      t.update(pkgRef, { remainingMinutes: newRemaining, updatedAt: FieldValue.serverTimestamp() });
      const logRef = db.collection('customer_package_logs').doc();
      t.set(logRef, {
        packageId:           passId.trim(),
        lineUserId:          pkg.lineUserId || '',
        customerName:        pkg.customerName || '',
        customerPhone:       pkg.customerPhone || '',
        packageType:         pkg.packageType || '',
        packageName:         pkg.packageName || '',
        action:              adjustAction,
        oldRemainingMinutes: oldRemaining,
        newRemainingMinutes: newRemaining,
        deltaMinutes,
        reason:              String(reason || '').slice(0, 400),
        adminName,
        createdAt:           FieldValue.serverTimestamp(),
      });
      return { oldRemaining, newRemaining, deltaMinutes, pkg };
    });
  } catch (e) {
    const map = {
      NOT_FOUND:  [404, 'Pass not found'],
      NOT_ULTRA:  [409, 'Minute controls apply to Ultra Pass only'],
      BELOW_ZERO: [400, 'Remaining minutes cannot go below 0'],
      NO_BRANCH:   [403, 'No access to this branch'],
    };
    const [code, msg] = map[e.message] || [500, 'Failed to adjust minutes'];
    if (code === 500) console.error('[adjust_pass_minutes] tx:', e.message);
    return res.status(code).json({ ok: false, error: msg });
  }

  console.log(`[adjust_pass_minutes] pass:${passId.trim()} ${result.oldRemaining}→${result.newRemaining} by ${adminName}`);
  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role,
    branchId: result.pkg.branchId || DEFAULT_BRANCH_ID,
    action: 'adjust_pass_minutes', targetId: passId.trim(),
    before: { remainingMinutes: result.oldRemaining },
    after:  { remainingMinutes: result.newRemaining },
    note: `ปรับ ${result.pkg.packageName || 'แพ็กเกจ'} ของ ${result.pkg.customerName || 'ลูกค้า'} ${result.oldRemaining} → ${result.newRemaining} นาที${reason ? ` · ${reason}` : ''}`,
  });

  return res.status(200).json({
    ok: true,
    oldRemainingMinutes: result.oldRemaining,
    newRemainingMinutes: result.newRemaining,
    deltaMinutes:        result.deltaMinutes,
  });
}

// ════════════════════════════════════════════════════════════════════
// handleDeactivatePass — set a pass status to "inactive" (any pass type).
// Idempotent guard: already-inactive returns 409.
// ════════════════════════════════════════════════════════════════════
async function handleDeactivatePass({ req, res, adminName, session }) {
  const { passId } = req.body || {};
  if (!passId || typeof passId !== 'string' || !passId.trim()) {
    return res.status(400).json({ ok: false, error: 'passId is required' });
  }

  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[deactivate_pass] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  const pkgRef = db.collection('customer_packages').doc(passId.trim());
  let pkg;
  try {
    const snap = await pkgRef.get();
    if (!snap.exists) return res.status(404).json({ ok: false, error: 'Pass not found' });
    pkg = snap.data();
  } catch (e) {
    console.error('[deactivate_pass] read:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to read pass' });
  }

  if (!hasBranchAccess(session, resolveBranchId(pkg))) {
    return res.status(403).json({ ok:false, error:'No access to this branch' });
  }

  if (pkg.status === 'inactive') {
    return res.status(409).json({ ok: false, error: 'Pass is already inactive' });
  }

  try {
    await pkgRef.update({ status: 'inactive', updatedAt: FieldValue.serverTimestamp() });
  } catch (e) {
    console.error('[deactivate_pass] write:', e.message);
    return res.status(500).json({ ok: false, error: 'Failed to deactivate pass' });
  }

  console.log(`[deactivate_pass] pass:${passId.trim()} by ${adminName}`);
  await writeAuditLog(db, {
    actor: adminName, actorRole: session.role,
    branchId: pkg.branchId || DEFAULT_BRANCH_ID,
    action: 'deactivate_pass', targetId: passId.trim(),
    before: { status: pkg.status || 'active' },
    after:  { status: 'inactive' },
    note: `ปิดใช้งาน ${pkg.packageName || 'แพ็กเกจ'} ของ ${pkg.customerName || 'ลูกค้า'}`,
  });

  return res.status(200).json({ ok: true });
}

// ════════════════════════════════════════════════════════════════════
// Voucher Manager v1 — owner-only campaign and code administration.
// Kept in this multiplexed function to stay below the Vercel Hobby limit.
// ════════════════════════════════════════════════════════════════════
const voucherTimestampIso = value => value?.toDate?.()?.toISOString?.() ?? null;

function projectVoucherCampaign(doc) {
  const data = doc.data();
  return {
    id: doc.id,
    schemaVersion: data.schemaVersion ?? 2,
    campaignId: data.campaignId || doc.id,
    name: data.name || doc.id,
    keyword: data.keyword || '',
    codePrefix: data.codePrefix || '',
    active: data.active === true,
    voucherType: data.voucherType || 'discount_amount',
    validFrom: voucherTimestampIso(data.validFrom),
    expiresAt: voucherTimestampIso(data.expiresAt),
    allowedDays: Array.isArray(data.allowedDays) ? data.allowedDays : [],
    startTime: data.startTime || '06:00',
    endTime: data.endTime || '24:00',
    excludeHolidays: data.excludeHolidays === true,
    exactDurationMinutes: Number(data.exactDurationMinutes) || 60,
    requiresLineLogin: data.requiresLineLogin !== false,
    transferable: data.transferable === true,
    maxUsesPerCode: Number(data.maxUsesPerCode) || 1,
    maxCancellationRestores: Number(data.maxCancellationRestores) || 0,
    branchId: data.branchId || DEFAULT_BRANCH_ID,
    resourceId: data.resourceId || 'room1',
    allowedPricingTypes: Array.isArray(data.allowedPricingTypes) ? data.allowedPricingTypes : [],
    discountAmount: data.discountAmount ?? null,
    discountPercent: data.discountPercent ?? null,
    maxDiscountAmount: data.maxDiscountAmount ?? null,
    minFinalPrice: data.minFinalPrice ?? 0,
    createdAt: voucherTimestampIso(data.createdAt),
    updatedAt: voucherTimestampIso(data.updatedAt),
  };
}

function projectVoucherCode(doc) {
  const data = doc.data();
  return {
    code: doc.id,
    campaignId: data.campaignId || '',
    active: data.active === true,
    state: data.state || 'available',
    usedCount: Number(data.usedCount) || 0,
    maxUses: Number(data.maxUses) || 1,
    cancellationRestoreCount: Number(data.cancellationRestoreCount) || 0,
    maxCancellationRestores: Number(data.maxCancellationRestores) || 0,
    reservedBookingCode: data.reservedBookingCode || null,
    reservedUntil: voucherTimestampIso(data.reservedUntil),
    redeemedBookingCode: data.redeemedBookingCode || null,
    redeemedAt: voucherTimestampIso(data.redeemedAt),
    pendingRequestId: data.pendingRequestId || null,
    issuedTo: data.issuedTo || null,
    assignedName: data.assignedName || '',
    assignedDraw: data.assignedDraw || '',
    assignedNickname: data.assignedNickname || '',
    redeemedPackageId: data.redeemedPackageId || null,
    createdAt: voucherTimestampIso(data.createdAt),
    updatedAt: voucherTimestampIso(data.updatedAt),
  };
}

function projectEventPassRequest(doc) {
  const data = doc.data();
  return {
    id: doc.id,
    code: data.code || '',
    campaignId: data.campaignId || '',
    status: data.status || 'pending',
    lineUserId: data.lineUserId || '',
    lineDisplayName: data.lineDisplayName || '',
    customerName: data.customerName || '',
    customerPhone: data.customerPhone || '',
    assignedName: data.assignedName || '',
    assignedDraw: data.assignedDraw || '',
    assignedNickname: data.assignedNickname || '',
    issuedPackageId: data.issuedPackageId || null,
    codeReturned: data.codeReturned === true,
    createdAt: voucherTimestampIso(data.createdAt),
    reviewedAt: voucherTimestampIso(data.reviewedAt),
    reviewedBy: data.reviewedBy || null,
  };
}

async function handleVoucherAction({ req, res, adminName, session, action }) {
  let db;
  try { db = getAdminDb(); }
  catch (e) { console.error('[voucher_manager] DB init:', e.message); return res.status(500).json({ ok: false, error: 'Database not available' }); }

  if (action === 'voucher_list') {
    const campaignId = typeof req.body?.campaignId === 'string' ? req.body.campaignId.trim() : '';
    try {
      const campaignSnap = await db.collection('voucher_campaigns').limit(100).get();
      const campaigns = campaignSnap.docs.map(projectVoucherCampaign)
        .sort((a, b) => a.name.localeCompare(b.name, 'en'));
      let codes = [];
      if (campaignId) {
        const codeSnap = await db.collection('vouchers').where('campaignId', '==', campaignId).limit(500).get();
        codes = codeSnap.docs.map(projectVoucherCode)
          .sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || '') || a.code.localeCompare(b.code));
      }
      return res.status(200).json({ ok: true, campaigns, codes, codeLimit: 500 });
    } catch (e) {
      console.error('[voucher_list]', e.message);
      return res.status(500).json({ ok: false, error: 'Failed to load Voucher Manager' });
    }
  }

  if (action === 'voucher_save_campaign') {
    const normalized = normalizeCampaignInput(req.body?.campaign || {});
    if (!normalized.ok) return res.status(400).json({ ok: false, error: normalized.error });
    const ref = db.collection('voucher_campaigns').doc(normalized.campaignId);
    try {
      const before = await ref.get();
      const value = normalized.data;
      await ref.set({
        ...value,
        validFrom: value.validFromMs === null ? null : Timestamp.fromMillis(value.validFromMs),
        expiresAt: value.expiresAtMs === null ? null : Timestamp.fromMillis(value.expiresAtMs),
        validFromMs: FieldValue.delete(),
        expiresAtMs: FieldValue.delete(),
        ...(before.exists ? {} : { createdAt: FieldValue.serverTimestamp(), createdBy: adminName }),
        updatedAt: FieldValue.serverTimestamp(),
        updatedBy: adminName,
      }, { merge: true });
      await writeAuditLog(db, {
        actor: adminName, actorRole: session.role, branchId: DEFAULT_BRANCH_ID,
        action: before.exists ? 'voucher_campaign_update' : 'voucher_campaign_create',
        targetId: normalized.campaignId,
        before: before.exists ? { name: before.data().name || '', active: before.data().active === true, voucherType: before.data().voucherType || null } : null,
        after: { name: value.name, active: value.active, voucherType: value.voucherType },
      });
      return res.status(200).json({ ok: true, campaignId: normalized.campaignId });
    } catch (e) {
      console.error('[voucher_save_campaign]', e.message);
      return res.status(500).json({ ok: false, error: 'Failed to save campaign' });
    }
  }

  if (action === 'voucher_set_campaign_active') {
    const campaignId = String(req.body?.campaignId || '').trim();
    const active = req.body?.active === true;
    if (!campaignId) return res.status(400).json({ ok: false, error: 'campaignId is required' });
    const ref = db.collection('voucher_campaigns').doc(campaignId);
    try {
      const snap = await ref.get();
      if (!snap.exists) return res.status(404).json({ ok: false, error: 'Campaign not found' });
      await ref.update({ active, updatedAt: FieldValue.serverTimestamp(), updatedBy: adminName });
      await writeAuditLog(db, {
        actor: adminName, actorRole: session.role, branchId: DEFAULT_BRANCH_ID,
        action: 'voucher_campaign_status', targetId: campaignId,
        before: { active: snap.data().active === true }, after: { active },
      });
      return res.status(200).json({ ok: true, active });
    } catch (e) {
      console.error('[voucher_set_campaign_active]', e.message);
      return res.status(500).json({ ok: false, error: 'Failed to update campaign status' });
    }
  }

  if (action === 'voucher_create_codes') {
    const campaignId = String(req.body?.campaignId || '').trim();
    if (!campaignId) return res.status(400).json({ ok: false, error: 'Select a campaign' });
    const campaignRef = db.collection('voucher_campaigns').doc(campaignId);
    try {
      const campaignSnap = await campaignRef.get();
      if (!campaignSnap.exists) return res.status(404).json({ ok: false, error: 'Campaign not found' });
      const campaign = campaignSnap.data();
      const mode = req.body?.mode === 'custom' ? 'custom' : 'random';
      let codes;
      if (mode === 'custom') {
        const custom = normalizeCustomVoucherCode(req.body?.customCode);
        if (!custom.ok) return res.status(400).json({ ok: false, error: custom.error });
        codes = [custom.code];
      } else {
        const request = normalizeRandomCodeRequest(req.body, campaign.codePrefix || '');
        if (!request.ok) return res.status(400).json({ ok: false, error: request.error });
        codes = [];
        for (let attempt = 0; attempt < 5 && codes.length < request.count; attempt++) {
          const candidates = generateVoucherCodes({
            count: request.count - codes.length,
            randomLength: request.randomLength,
            prefix: request.prefix,
          }).filter(code => !codes.includes(code));
          const refs = candidates.map(code => db.collection('vouchers').doc(code));
          const snaps = refs.length ? await db.getAll(...refs) : [];
          snaps.forEach((snap, index) => { if (!snap.exists) codes.push(candidates[index]); });
        }
        if (codes.length !== request.count) return res.status(409).json({ ok: false, error: 'Could not generate enough unique codes; try again' });
      }

      const refs = codes.map(code => db.collection('vouchers').doc(code));
      const existing = refs.length ? await db.getAll(...refs) : [];
      if (existing.some(snap => snap.exists)) return res.status(409).json({ ok: false, error: 'One or more Voucher codes already exist' });

      const batch = db.batch();
      refs.forEach((ref, index) => batch.create(ref, {
        schemaVersion: 2,
        code: codes[index],
        campaignId,
        active: true,
        state: 'available',
        usedCount: 0,
        maxUses: 1,
        cancellationRestoreCount: 0,
        maxCancellationRestores: Number(campaign.maxCancellationRestores) || 0,
        source: mode === 'custom' ? 'admin_custom' : 'admin_generated',
        createdBy: adminName,
        createdAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
      }));
      await batch.commit();
      await writeAuditLog(db, {
        actor: adminName, actorRole: session.role, branchId: DEFAULT_BRANCH_ID,
        action: 'voucher_codes_create', targetId: campaignId,
        before: null, after: { count: codes.length, mode },
        note: `Created ${codes.length} Voucher code(s)`,
      });
      return res.status(200).json({ ok: true, campaignId, codes });
    } catch (e) {
      const conflict = e.code === 6 || e.code === 'already-exists';
      if (!conflict) console.error('[voucher_create_codes]', e.message);
      return res.status(conflict ? 409 : 500).json({ ok: false, error: conflict ? 'Voucher code already exists' : 'Failed to create Voucher codes' });
    }
  }

  if (action === 'voucher_import_codes') {
    const campaignId = String(req.body?.campaignId || '').trim();
    if (!campaignId) return res.status(400).json({ ok: false, error: 'Select a campaign' });
    const normalized = normalizeVoucherImportRecords(req.body?.records);
    if (!normalized.ok) return res.status(400).json({ ok: false, error: normalized.error });
    try {
      const campaignSnap = await db.collection('voucher_campaigns').doc(campaignId).get();
      if (!campaignSnap.exists) return res.status(404).json({ ok: false, error: 'Campaign not found' });
      const refs = normalized.records.map(record => db.collection('vouchers').doc(record.code));
      const existing = await db.getAll(...refs);
      const duplicates = existing.filter(snap => snap.exists).map(snap => snap.id);
      if (duplicates.length) {
        return res.status(409).json({ ok: false, error: `${duplicates.length} code(s) already exist`, duplicates });
      }
      const campaign = campaignSnap.data();
      const batch = db.batch();
      normalized.records.forEach((record, index) => batch.create(refs[index], {
        schemaVersion: 2,
        code: record.code,
        campaignId,
        active: true,
        state: 'available',
        usedCount: 0,
        maxUses: 1,
        cancellationRestoreCount: 0,
        maxCancellationRestores: Number(campaign.maxCancellationRestores) || 0,
        assignedName: record.assignedName,
        assignedDraw: record.assignedDraw,
        assignedNickname: record.assignedNickname,
        source: 'admin_exact_import',
        createdBy: adminName,
        createdAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
      }));
      await batch.commit();
      await writeAuditLog(db, {
        actor: adminName, actorRole: session.role, branchId: DEFAULT_BRANCH_ID,
        action: 'voucher_codes_import', targetId: campaignId,
        before: null, after: { count: normalized.records.length, mode: 'exact_import' },
        note: `Imported ${normalized.records.length} exact Voucher code(s)`,
      });
      return res.status(200).json({ ok: true, campaignId, imported: normalized.records.length });
    } catch (e) {
      const conflict = e.code === 6 || e.code === 'already-exists';
      if (!conflict) console.error('[voucher_import_codes]', e.message);
      return res.status(conflict ? 409 : 500).json({ ok: false, error: conflict ? 'Voucher code already exists' : 'Failed to import Voucher codes' });
    }
  }

  if (action === 'event_pass_list_requests') {
    try {
      const snap = await db.collection('event_pass_requests').limit(500).get();
      const requests = snap.docs.map(projectEventPassRequest)
        .sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
      return res.status(200).json({ ok: true, requests });
    } catch (e) {
      console.error('[event_pass_list_requests]', e.message);
      return res.status(500).json({ ok: false, error: 'Failed to load Event Pass requests' });
    }
  }

  if (action === 'event_pass_approve_request') {
    const requestId = String(req.body?.requestId || '').trim();
    if (!requestId) return res.status(400).json({ ok: false, error: 'requestId is required' });
    const requestRef = db.collection('event_pass_requests').doc(requestId);
    const packageRef = db.collection('customer_packages').doc();
    try {
      let result;
      await db.runTransaction(async transaction => {
        const requestSnap = await transaction.get(requestRef);
        if (!requestSnap.exists) throw new Error('NOT_FOUND');
        const request = requestSnap.data();
        if (request.status !== 'pending') throw new Error('BAD_STATE');
        const voucherRef = db.collection('vouchers').doc(String(request.code));
        const campaignRef = db.collection('voucher_campaigns').doc(String(request.campaignId));
        const userRef = db.collection('registered_users').doc(String(request.lineUserId));
        const [voucherSnap, campaignSnap, userSnap] = await Promise.all([
          transaction.get(voucherRef), transaction.get(campaignRef), transaction.get(userRef),
        ]);
        if (!voucherSnap.exists || !campaignSnap.exists) throw new Error('ENTITLEMENT_MISSING');
        if (!userSnap.exists) throw new Error('USER_MISSING');
        const voucher = voucherSnap.data();
        const campaign = campaignSnap.data();
        if (campaign.voucherType !== 'event_pass' || campaign.active !== true) throw new Error('CAMPAIGN_INACTIVE');
        if (voucher.state !== 'pending_approval' || voucher.pendingRequestId !== requestId) throw new Error('CODE_CONFLICT');
        const expiresAtMs = campaign.expiresAt?.toMillis?.() ?? 0;
        if (!expiresAtMs || expiresAtMs <= Date.now()) throw new Error('EXPIRED');
        const user = userSnap.data();
        transaction.create(packageRef, {
          lineUserId: request.lineUserId,
          customerName: request.customerName || user.name || '',
          customerPhone: request.customerPhone || user.phone || '',
          customerPhoneNormalized: normalizePhone(request.customerPhone || user.phone),
          lineDisplayName: request.lineDisplayName || user.lineDisplayName || '',
          packageType: 'monstr_event_pass', packageName: campaign.name || 'Event Pass',
          price: 0, ownerRole: 'customer', totalMinutes: 60, remainingMinutes: 60,
          validityDays: null, validFrom: FieldValue.serverTimestamp(), validUntil: campaign.expiresAt,
          status: 'active', isEventPass: true,
          restrictDays: Array.isArray(campaign.allowedDays) ? campaign.allowedDays : [1, 2, 3, 4, 5],
          branchId: campaign.branchId || DEFAULT_BRANCH_ID,
          resourceId: campaign.resourceId || 'room1',
          excludeHolidays: campaign.excludeHolidays === true,
          exactDurationMinutes: 60, eventUsedAt: null,
          eventName: campaign.name || 'Event Pass',
          sourceVoucherCode: request.code, sourceEventPassRequestId: requestId,
          addedByAdmin: adminName, source: 'event_code_approved',
          createdAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
          weeklyUsage: {}, monthlyUsage: {}, note: '',
        });
        transaction.update(voucherRef, {
          state: 'redeemed', usedCount: 1, issuedTo: request.lineUserId,
          redeemedBy: request.lineUserId, redeemedAt: FieldValue.serverTimestamp(),
          redeemedPackageId: packageRef.id, redeemedRequestId: requestId,
          updatedAt: FieldValue.serverTimestamp(), updatedBy: adminName,
        });
        transaction.update(requestRef, {
          status: 'approved', issuedPackageId: packageRef.id,
          reviewedAt: FieldValue.serverTimestamp(), reviewedBy: adminName,
          updatedAt: FieldValue.serverTimestamp(),
        });
        result = { packageId: packageRef.id, code: request.code, lineUserId: request.lineUserId, packageName: campaign.name || 'Event Pass', expiresAtMs };
      });
      await writeAuditLog(db, {
        actor: adminName, actorRole: session.role, branchId: DEFAULT_BRANCH_ID,
        action: 'event_pass_approve', targetId: requestId,
        before: { status: 'pending' }, after: { status: 'approved', packageId: result.packageId, code: result.code },
      });
      try {
        await sendAndLog({
          eventId: `${result.code}_event_pass_activated_customer`,
          type: 'pass_activated_customer', targetType: 'customer',
          lineUserId: result.lineUserId, bookingCode: result.code,
          payload: {
            packageName: result.packageName, remainingMinutes: 60,
            validUntil: new Date(result.expiresAtMs).toLocaleDateString('en-GB', { timeZone: 'Asia/Bangkok' }),
          },
        });
      } catch (e) { console.error('[event_pass_approve_request] notify (non-fatal):', e.message); }
      return res.status(200).json({ ok: true, ...result });
    } catch (e) {
      const map = {
        NOT_FOUND: [404, 'Event Pass request not found'], BAD_STATE: [409, 'Request was already reviewed'],
        ENTITLEMENT_MISSING: [409, 'Code or campaign is missing'], USER_MISSING: [409, 'Customer must register before approval'],
        CAMPAIGN_INACTIVE: [409, 'Event Pass campaign is inactive'], CODE_CONFLICT: [409, 'Code is no longer reserved for this request'],
        EXPIRED: [409, 'Event Pass campaign has expired'],
      };
      const [status, error] = map[e.message] || [500, 'Failed to approve Event Pass'];
      if (status === 500) console.error('[event_pass_approve_request]', e.message);
      return res.status(status).json({ ok: false, error });
    }
  }

  if (action === 'event_pass_reject_request') {
    const requestId = String(req.body?.requestId || '').trim();
    const returnCode = req.body?.returnCode !== false;
    if (!requestId) return res.status(400).json({ ok: false, error: 'requestId is required' });
    const requestRef = db.collection('event_pass_requests').doc(requestId);
    try {
      let code;
      await db.runTransaction(async transaction => {
        const requestSnap = await transaction.get(requestRef);
        if (!requestSnap.exists) throw new Error('NOT_FOUND');
        const request = requestSnap.data();
        if (request.status !== 'pending') throw new Error('BAD_STATE');
        code = String(request.code || '');
        const voucherRef = db.collection('vouchers').doc(code);
        const voucherSnap = await transaction.get(voucherRef);
        if (!voucherSnap.exists) throw new Error('CODE_MISSING');
        const voucher = voucherSnap.data();
        if (voucher.state !== 'pending_approval' || voucher.pendingRequestId !== requestId) throw new Error('CODE_CONFLICT');
        transaction.update(voucherRef, returnCode ? {
          state: 'available', active: true, issuedTo: FieldValue.delete(),
          pendingRequestId: FieldValue.delete(), pendingAt: FieldValue.delete(),
          updatedAt: FieldValue.serverTimestamp(), updatedBy: adminName,
        } : {
          state: 'disabled', active: false, updatedAt: FieldValue.serverTimestamp(), updatedBy: adminName,
        });
        transaction.update(requestRef, {
          status: 'rejected', codeReturned: returnCode,
          reviewedAt: FieldValue.serverTimestamp(), reviewedBy: adminName,
          updatedAt: FieldValue.serverTimestamp(),
        });
      });
      await writeAuditLog(db, {
        actor: adminName, actorRole: session.role, branchId: DEFAULT_BRANCH_ID,
        action: 'event_pass_reject', targetId: requestId,
        before: { status: 'pending' }, after: { status: 'rejected', codeReturned: returnCode, code },
      });
      return res.status(200).json({ ok: true, code, codeReturned: returnCode });
    } catch (e) {
      const map = { NOT_FOUND: [404, 'Event Pass request not found'], BAD_STATE: [409, 'Request was already reviewed'], CODE_MISSING: [409, 'Code is missing'], CODE_CONFLICT: [409, 'Code is no longer reserved for this request'] };
      const [status, error] = map[e.message] || [500, 'Failed to reject Event Pass'];
      if (status === 500) console.error('[event_pass_reject_request]', e.message);
      return res.status(status).json({ ok: false, error });
    }
  }

  if (action === 'event_pass_reset_code') {
    const normalized = normalizeCustomVoucherCode(req.body?.code);
    if (!normalized.ok) return res.status(400).json({ ok: false, error: normalized.error });
    const voucherRef = db.collection('vouchers').doc(normalized.code);
    try {
      const initial = await voucherRef.get();
      if (!initial.exists) return res.status(404).json({ ok: false, error: 'Code not found' });
      const initialData = initial.data();
      const packageId = String(initialData.redeemedPackageId || '');
      const requestId = String(initialData.pendingRequestId || initialData.redeemedRequestId || '');
      const packageRef = packageId ? db.collection('customer_packages').doc(packageId) : null;
      const requestRef = requestId ? db.collection('event_pass_requests').doc(requestId) : null;
      let bookingRefs = [];
      if (packageRef) {
        const bookingSnap = await db.collection('bookings').where('packageId', '==', packageId).limit(10).get();
        bookingRefs = bookingSnap.docs.map(doc => doc.ref);
      }
      await db.runTransaction(async transaction => {
        const reads = await Promise.all([
          transaction.get(voucherRef),
          ...(packageRef ? [transaction.get(packageRef)] : []),
          ...(requestRef ? [transaction.get(requestRef)] : []),
          ...bookingRefs.map(ref => transaction.get(ref)),
        ]);
        if (!reads[0].exists) throw new Error('NOT_FOUND');
        const voucher = reads[0].data();
        const state = voucher.state || 'available';
        if (state === 'redeemed' && !packageRef) throw new Error('PACKAGE_MISSING');
        const bookingOffset = 1 + (packageRef ? 1 : 0) + (requestRef ? 1 : 0);
        const bookings = reads.slice(bookingOffset).filter(snap => snap.exists).map(snap => snap.data());
        if (state === 'redeemed' && packageRef) {
          const pkgSnap = reads[1];
          if (!pkgSnap.exists) throw new Error('PACKAGE_MISSING');
          const pkg = pkgSnap.data();
          if (pkg.lastUsedBooking && !bookings.length) throw new Error('BOOKING_UNKNOWN');
          if (bookings.some(booking => !['cancelled', 'expired', 'completed', 'no_show'].includes(booking.bookingStatus))) {
            throw new Error('ACTIVE_BOOKING');
          }
          transaction.update(packageRef, {
            status: 'inactive', resetBy: adminName, resetAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(),
          });
        }
        transaction.update(voucherRef, {
          state: 'available', active: true, usedCount: 0,
          issuedTo: FieldValue.delete(), pendingRequestId: FieldValue.delete(), pendingAt: FieldValue.delete(),
          redeemedBy: FieldValue.delete(), redeemedAt: FieldValue.delete(), redeemedPackageId: FieldValue.delete(), redeemedRequestId: FieldValue.delete(),
          resetBy: adminName, resetAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp(), updatedBy: adminName,
        });
        if (requestRef) {
          const requestSnap = reads[1 + (packageRef ? 1 : 0)];
          if (requestSnap.exists) transaction.update(requestRef, {
            status: 'reset', codeReturned: true, reviewedAt: FieldValue.serverTimestamp(), reviewedBy: adminName, updatedAt: FieldValue.serverTimestamp(),
          });
        }
      });
      await writeAuditLog(db, {
        actor: adminName, actorRole: session.role, branchId: DEFAULT_BRANCH_ID,
        action: 'event_pass_reset_code', targetId: normalized.code,
        before: { state: initialData.state || 'available', packageId: packageId || null },
        after: { state: 'available', usedCount: 0 }, note: 'Owner test reset / return code',
      });
      return res.status(200).json({ ok: true, code: normalized.code, state: 'available' });
    } catch (e) {
      const map = {
        NOT_FOUND: [404, 'Code not found'], PACKAGE_MISSING: [409, 'Issued Event Pass is missing'],
        BOOKING_UNKNOWN: [409, 'Cannot verify the Event Pass booking safely'],
        ACTIVE_BOOKING: [409, 'Cancel the active Event Pass booking before resetting this code'],
      };
      const [status, error] = map[e.message] || [500, 'Failed to reset Event Pass code'];
      if (status === 500) console.error('[event_pass_reset_code]', e.message);
      return res.status(status).json({ ok: false, error });
    }
  }

  if (action === 'voucher_set_code_active') {
    const normalized = normalizeCustomVoucherCode(req.body?.code);
    if (!normalized.ok) return res.status(400).json({ ok: false, error: normalized.error });
    const active = req.body?.active === true;
    const ref = db.collection('vouchers').doc(normalized.code);
    try {
      let afterState;
      await db.runTransaction(async transaction => {
        const snap = await transaction.get(ref);
        if (!snap.exists) throw new Error('NOT_FOUND');
        const voucher = snap.data();
        if (voucher.state === 'reserved' || voucher.state === 'pending_approval') throw new Error('RESERVED');
        if (active && (voucher.state === 'redeemed' || (Number(voucher.usedCount) || 0) >= (Number(voucher.maxUses) || 1))) {
          throw new Error('REDEEMED');
        }
        afterState = active ? 'available' : (voucher.state === 'redeemed' ? 'redeemed' : 'disabled');
        transaction.update(ref, {
          active,
          state: afterState,
          updatedAt: FieldValue.serverTimestamp(),
          updatedBy: adminName,
        });
      });
      await writeAuditLog(db, {
        actor: adminName, actorRole: session.role, branchId: DEFAULT_BRANCH_ID,
        action: 'voucher_code_status', targetId: normalized.code,
        before: null, after: { active, state: afterState },
      });
      return res.status(200).json({ ok: true, code: normalized.code, active, state: afterState });
    } catch (e) {
      const map = {
        NOT_FOUND: [404, 'Voucher code not found'],
        RESERVED: [409, 'Reserved Voucher cannot be disabled or enabled'],
        REDEEMED: [409, 'Redeemed Voucher cannot be enabled again'],
      };
      const [status, error] = map[e.message] || [500, 'Failed to update Voucher code'];
      if (status === 500) console.error('[voucher_set_code_active]', e.message);
      return res.status(status).json({ ok: false, error });
    }
  }

  return res.status(400).json({ ok: false, error: `Unsupported Voucher action: ${action}` });
}
