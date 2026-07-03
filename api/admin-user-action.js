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
    price: 3500,
    totalMinutes: 300,
    validityDays: 60,
    ownerRole: "customer",
    requiresCoachOrAdminBooking: true
  },
  coach_at_ultra_10: {
    packageType: "coach_at_ultra_10",
    packageName: "Coaching at Ultra",
    price: 3000,
    totalMinutes: 600,
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
      error: `Invalid or deprecated packageType: ${packageType}. New flow only issues: ultra_starter_3, ultra_pass_10, ultra_pass_20, beginner_coaching_5, coach_at_ultra_10.`
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
      weeklyLimitHours: null,
      monthlyLimitHours: null,
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
  const { promoActive, promoName, promoPrice, promoLabel, startsAt, endsAt, morningPromoActive } = req.body || {};

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
      after: { specialPromoActive: Boolean(promoActive), specialPromoName: String(promoName || '').trim(), specialPromoPrice: price },
    });
    return res.status(200).json({ ok: true });
  } catch (err) {
    console.error('[admin-user-action] pricing error:', err);
    return res.status(500).json({ ok: false, error: err.message || 'Internal Server Error' });
  }
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
