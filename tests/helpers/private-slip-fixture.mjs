// Submission-transaction fixtures only. End-to-end upload/storage is exercised
// separately in slip-storage-security.test.mjs against all three emulators.
import { createHash } from 'node:crypto';
import { Timestamp } from 'firebase-admin/firestore';
export async function privateSlipFixture(db, { id, actor, key }) {
  const receiptId=createHash('sha256').update(`${id}|${key}`).digest('hex');
  await db.collection('private_slip_uploads').doc(receiptId).set({ type:'booking',targetId:id,actor,
    state:'ready',expiresAt:Timestamp.fromMillis(Date.now()+600_000),
    object:{bucket:'ultra-tennis-booking.firebasestorage.app',path:`private_slips/booking/${id}/${receiptId}.jpg`,generation:'1',size:10,contentType:'image/jpeg',sha256:'0'.repeat(64)} });
  return `private:${receiptId}`;
}
