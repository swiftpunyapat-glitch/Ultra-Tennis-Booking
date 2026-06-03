// ════════════════════════════════════════════════════════════════════
// POST /api/admin-pricing-action — Manage special promotions
// ════════════════════════════════════════════════════════════════════
// Auth: requires valid admin session cookie AND admin name must be "Art".
// ════════════════════════════════════════════════════════════════════

import { verifySessionCookie } from './_lib/admin-auth.js';
import { getAdminDb } from './_lib/firebase-admin.js';
import { FieldValue, Timestamp } from 'firebase-admin/firestore';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'Method Not Allowed' });
  }

  const adminName = verifySessionCookie(req);
  if (!adminName) {
    return res.status(401).json({ ok: false, error: 'Unauthorized admin session' });
  }

  if (adminName !== 'Art') {
    return res.status(403).json({ ok: false, error: 'Access denied: Art only.' });
  }

  const { action, promoActive, promoName, promoPrice, promoLabel, startsAt, endsAt, specialQrUrl } = req.body || {};

  try {
    const db = getAdminDb();
    const docRef = db.collection('system_settings').doc('pricing');

    if (action === 'deactivate_special_promotion') {
      await docRef.set({
        specialPromoActive: false,
        updatedAt: FieldValue.serverTimestamp(),
        updatedBy: adminName
      }, { merge: true });

      console.log(`[admin-pricing-action] Deactivated special promotion by Art`);
      return res.status(200).json({ ok: true });
    }

    if (action === 'save_special_promotion') {
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

      console.log(`[admin-pricing-action] Saved special promotion: ${promoName} (Price: ${price}) by Art`);
      return res.status(200).json({ ok: true });
    }

    return res.status(400).json({ ok: false, error: `Invalid action: ${action}` });
  } catch (err) {
    console.error('[admin-pricing-action] Error:', err);
    return res.status(500).json({ ok: false, error: err.message || 'Internal Server Error' });
  }
}
