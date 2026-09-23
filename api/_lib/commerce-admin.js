import { evaluateVoucher, applyVoucherToQuote } from './voucher-engine.js';
import { validateCommerce, storeConfig, resourceError } from '../../commerce.js';
import { readPricing, readStore, commerceSettingsRef, pricingFromSnapshots } from './store-settings.js';
import { courtQuote } from './court-quote.js';
import { FieldValue } from 'firebase-admin/firestore';
import { writeAuditLog } from './firebase-admin.js';

export async function handleCommerceSettings({ db, body, res, session }) {
  const ref = commerceSettingsRef(db);
  const publicRef = db.collection('system_settings').doc('pricing');
  try {
    if (body.action === 'commerce_get') {
      return res.status(200).json({ ok: true, commerce: await readStore(db) });
    }
    const config = validateCommerce(body.commerce);
    if (body.action === 'commerce_preview') {
      const { date, startTime, durationMinutes, resourceId } = body;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !/^(?:[01]\d|2[0-3]):[03]0$/.test(startTime) || ![30,60,90,120,150,180].includes(durationMinutes)) throw new Error('Invalid preview date/time/duration');
      const error = resourceError(config, resourceId, startTime, durationMinutes);
      if (error) throw new Error(error);
      const [pricing, holiday] = await Promise.all([readPricing(db), db.collection('holidays').doc(date).get()]);
      const segments = [];
      let minute = Number(startTime.slice(0,2)) * 60 + Number(startTime.slice(3));
      if (durationMinutes % 60 === 0 && minute % 60 !== 0) throw new Error('Whole-hour bookings must start on the hour');
      for (let left = durationMinutes; left > 0;) {
        const span = minute % 60 === 0 && left >= 60 ? 60 : 30;
        segments.push({ start: `${String(Math.floor(minute/60)).padStart(2,'0')}:${String(minute%60).padStart(2,'0')}`, span });
        minute += span; left -= span;
      }
      if (segments.some(s=>s.span===30) && (Number(startTime.slice(0,2))<6 || segments.some(s=>s.span===30 && Number(s.start.slice(0,2))>=23))) throw new Error('ช่วงครึ่งชั่วโมงเปิดเฉพาะ 06:00–23:00');
      let { quote } = courtQuote({ segments, date, startTime, durationMinutes, resourceId,
        pricing: { ...pricing, commerce: config }, isHoliday: holiday.exists && holiday.data().isHoliday === true });
      if (body.voucherCode) {
        const code = String(body.voucherCode).trim().toUpperCase();
        if (!/^[A-Z0-9_-]{1,80}$/.test(code)) throw new Error('Invalid Coupon code');
        const voucherSnap = await db.collection('vouchers').doc(code).get();
        const voucher = voucherSnap.exists ? voucherSnap.data() : null;
        const campaignSnap = voucher?.campaignId ? await db.collection('voucher_campaigns').doc(voucher.campaignId).get() : null;
        quote = applyVoucherToQuote(quote, evaluateVoucher({ voucher, campaign: campaignSnap?.exists ? campaignSnap.data() : null, code,
          date, startTime, durationMinutes, resourceId, branchId:'ladprao1', lineUserId:body.lineUserId || null,
          isHoliday:holiday.exists && holiday.data().isHoliday === true, baseQuote:quote }));
      }
      return res.status(200).json({ ok: true, quote });
    }
    const revision = await db.runTransaction(async t => {
      const snap = await t.get(ref);
      const publicSnap = await t.get(publicRef);
      const before = storeConfig(pricingFromSnapshots(publicSnap, snap));
      if (Number(body.revision) !== Number(before.revision || 0)) throw Object.assign(new Error('Settings changed. Reload before saving.'), { status: 409 });
      // Keep historical court IDs resolvable; an unused court can be disabled.
      if (before.resources.some(r => !config.resources.some(next => next.id === r.id))) throw new Error('Existing courts cannot be removed. Disable them instead.');
      const nextRevision = Number(before.revision || 0) + 1;
      const after = { ...config, revision: nextRevision };
      t.set(ref, { commerce: after, updatedAt: FieldValue.serverTimestamp(), updatedBy: session.name });
      // Atomically remove the provisional inline config on the first save.
      if (publicSnap.exists && publicSnap.data().commerce !== undefined) t.update(publicRef, { commerce: FieldValue.delete() });
      t.create(db.collection('commerce_settings_history').doc(), { before, after, actor: session.name, createdAt: FieldValue.serverTimestamp() });
      return nextRevision;
    });
    await writeAuditLog(db, { actor: session.name, actorRole: session.role, action: 'commerce_save', targetId: 'commerce_settings/current', after: { revision } });
    return res.status(200).json({ ok: true, commerce: { ...config, revision } });
  } catch (e) {
    return res.status(e.status || 400).json({ ok: false, error: e.message });
  }
}
