// ════════════════════════════════════════════════════════════════════
// POST /api/admin-user-action — Admin user management, package and
// pricing actions
// ════════════════════════════════════════════════════════════════════
// Auth: requires valid admin session cookie.
// Actions:
//   add_pass_to_registered_user            (any logged-in admin)
//   save_special_promotion                 (owner-only — merged in from
//   deactivate_special_promotion            the former /api/admin-pricing-action
//                                           route to keep the Vercel function
//                                           count down; same request/response)
// ════════════════════════════════════════════════════════════════════

import { verifySession, requireRole, DEFAULT_BRANCH_ID } from './_lib/admin-auth.js';
import { getAdminDb, writeAuditLog } from './_lib/firebase-admin.js';
import { sendAndLog, loadActiveAdmins } from './_lib/notify.js';
import { FieldValue, Timestamp } from 'firebase-admin/firestore';

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

  // ── Pricing actions (owner-only) ─────────────────────────────────
  if (action === 'save_special_promotion' || action === 'deactivate_special_promotion') {
    if (!requireRole(session, 'owner')) {
      return res.status(403).json({ ok: false, error: 'Access denied: owner only.' });
    }
    return handlePricingAction({ req, res, adminName, session, action });
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
  const { promoActive, promoName, promoPrice, promoLabel, startsAt, endsAt, morningPromoActive, halfHourPrice } = req.body || {};

  try {
    const db = getAdminDb();
    const docRef = db.collection('system_settings').doc('pricing');

    if (action === 'deactivate_special_promotion') {
      await docRef.set({
        specialPromoActive: false,
        updatedAt: FieldValue.serverTimestamp(),
        updatedBy: adminName
      }, { merge: true });

      console.log(`[admin-user-action] Deactivated special promotion by ${adminName}`);
      await writeAuditLog(db, {
        actor: adminName, actorRole: session.role,
        action: 'deactivate_special_promotion', targetId: 'system_settings/pricing',
        after: { specialPromoActive: false },
      });
      return res.status(200).json({ ok: true });
    }

    // action === 'save_special_promotion'
    const price = Number(promoPrice);
    if (!Number.isInteger(price) || price <= 0) {
      return res.status(400).json({ ok: false, error: 'Price must be a valid positive integer' });
    }

    // Half-hour price (Phase B): optional — written only when the client sends
    // it, so an older admin.html can't silently reset it. Bounds keep a typo
    // from selling half hours at ฿1 or ฿9999; server fallback stays ฿200.
    let halfPrice;
    if (halfHourPrice !== undefined && halfHourPrice !== null && halfHourPrice !== '') {
      halfPrice = Number(halfHourPrice);
      if (!Number.isInteger(halfPrice) || halfPrice < 100 || halfPrice > 1000) {
        return res.status(400).json({ ok: false, error: 'Half-hour price must be an integer 100-1000 THB' });
      }
    }

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

    await docRef.set({
      normalSingleUsePrice: 350,
      // Morning 330/320 kill-switch. Written only when the client sends it, so
      // an older admin.html can't silently flip the promo off.
      ...(typeof morningPromoActive === 'boolean' ? { morningPromoActive } : {}),
      ...(halfPrice !== undefined ? { halfHourPrice: halfPrice } : {}),
      specialPromoActive: Boolean(promoActive),
      specialPromoName: String(promoName || "").trim(),
      specialPromoPrice: price,
      specialPromoLabel: String(promoLabel || "").trim(),
      specialPromoStartsAt: startTS,
      specialPromoEndsAt: endTS,
      normalQrUrl: "/payment-qr.png",
      specialQrUrl: "/payment-qr-special.png",
      lateNightQrUrl: "/late-night-qr.png",
      updatedAt: FieldValue.serverTimestamp(),
      updatedBy: adminName
    }, { merge: true });

    console.log(`[admin-user-action] Saved special promotion: ${promoName} (Price: ${price}) by ${adminName}`);
    await writeAuditLog(db, {
      actor: adminName, actorRole: session.role,
      action: 'save_special_promotion', targetId: 'system_settings/pricing',
      after: { specialPromoActive: Boolean(promoActive), specialPromoName: String(promoName || '').trim(), specialPromoPrice: price, ...(halfPrice !== undefined ? { halfHourPrice: halfPrice } : {}) },
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
  if (!Number.isInteger(value) || value <= 0) {
    return res.status(400).json({ ok: false, error: 'value must be a positive whole number of minutes' });
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
