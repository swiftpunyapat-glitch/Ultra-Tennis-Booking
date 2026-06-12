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

import { verifySession, requireRole } from './_lib/admin-auth.js';
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

  const { action, targetUserId, packageType, validFrom, note } = req.body || {};

  // ── Pricing actions (owner-only) ─────────────────────────────────
  if (action === 'save_special_promotion' || action === 'deactivate_special_promotion') {
    if (!requireRole(session, 'owner')) {
      return res.status(403).json({ ok: false, error: 'Access denied: owner only.' });
    }
    return handlePricingAction({ req, res, adminName, session, action });
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

    const vUntil = new Date(vFrom.getTime() + pkg.validityDays * 24 * 60 * 60 * 1000);

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

    // 4. Create new document in customer_packages
    const pkgRef = await db.collection('customer_packages').add(packagePayload);

    console.log(`[admin-user-action] Created pass ${pkgRef.id} for user ${targetUserId} by admin ${adminName}`);

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
  const { promoActive, promoName, promoPrice, promoLabel, startsAt, endsAt } = req.body || {};

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
