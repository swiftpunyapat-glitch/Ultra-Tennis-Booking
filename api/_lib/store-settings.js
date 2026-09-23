import { storeConfig, resourceError } from '../../commerce.js';

// Keep commercial plans out of system_settings/pricing, which the legacy
// browser reads directly. Client access to this collection must be denied.
export const commerceSettingsRef = db => db.collection('commerce_settings').doc('current');
export function pricingFromSnapshots(base, commercial) {
  const pricing = base.exists ? base.data() : null;
  return commercial.exists ? { ...pricing, commerce: commercial.data().commerce } : pricing;
}
export async function readPricing(db, transaction = null) {
  const baseRef = db.collection('system_settings').doc('pricing');
  const privateRef = commerceSettingsRef(db);
  const [base, commercial] = transaction
    ? [await transaction.get(baseRef), await transaction.get(privateRef)]
    : await Promise.all([baseRef.get(), privateRef.get()]);
  return pricingFromSnapshots(base, commercial);
}
export async function readStore(db, transaction = null) {
  return storeConfig(await readPricing(db, transaction));
}
export async function validateResourceRequest(db, body, { checkHours = true } = {}) {
  const config = await readStore(db);
  const id = body.resourceId || 'room1';
  const duration = body.durationMinutes ?? (Number(body.durationHours || 1) * 60);
  const error = resourceError(config, id, checkHours ? body.startTime : null, duration);
  if (error) throw Object.assign(new Error(error), { status: 400 });
  return id;
}
export const resourceSlotId = (resourceId, date, startTime) => `${resourceId}_${date}_${String(startTime).replace(':', '')}`;
