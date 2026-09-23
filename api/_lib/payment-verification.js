// Provider seam. No API key, external request, or automatic approval is enabled.
export const PAYMENT_VERIFICATION_MODE = 'manual';
export function manualPaymentApproval(actor, timestamp) {
  return { paymentApproval: { mode: 'manual', provider: null, approvedBy: actor, approvedAt: timestamp } };
}
export function verificationCapabilities() {
  return { mode: PAYMENT_VERIFICATION_MODE, automaticAvailable: false, localPrecheck: true };
}
// A future adapter must provide trusted bank reference, receiver, amount and
// transfer time. Persist/deduplicate that reference transactionally before
// invoking the same payment transition as manual approval. Never accept a
// browser-supplied verification result as payment evidence.
