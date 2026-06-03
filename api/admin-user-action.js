// ════════════════════════════════════════════════════════════════════
// POST /api/admin-user-action — Admin user management and package actions
// ════════════════════════════════════════════════════════════════════
// Auth: requires valid admin session cookie.
// ════════════════════════════════════════════════════════════════════

import { verifySessionCookie } from './_lib/admin-auth.js';
import { getAdminDb } from './_lib/firebase-admin.js';
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

  const adminName = verifySessionCookie(req);
  if (!adminName) {
    return res.status(401).json({ ok: false, error: 'Unauthorized admin session' });
  }

  const { action, targetUserId, packageType, validFrom, note } = req.body || {};

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
