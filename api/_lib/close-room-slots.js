// Every room booking transaction reads this same hourly availability document.
// Reading both half-hour occupancies here makes closing serialize with booking.
export async function closeRoomSlots(db, refs, isLive, fields, createMissing = null) {
  let closed = 0, skipped = 0, missing = 0;
  for (let offset = 0; offset < refs.length; offset += 100) {
    const group = refs.slice(offset, offset + 100);
    const result = await db.runTransaction(async t => {
      const dependencies = group.flatMap(ref => {
        const halfId = ref.id.slice(0, -2) + '30';
        return [ref, db.collection('booking_slots').doc(ref.id), db.collection('booking_slots').doc(halfId)];
      });
      const snapshots = await t.getAll(...dependencies);
      const rows = group.map((ref,i) => ({ref,av:snapshots[i*3],full:snapshots[i*3+1],half:snapshots[i*3+2]}));
      let count = 0, busy = 0, absent = 0;
      const now = Date.now();
      for (const {ref, av, full, half} of rows) {
        if ((full.exists && isLive(full.data(), now)) || (half.exists && isLive(half.data(), now))) {
          busy++; continue;
        }
        if (!av.exists) {
          if (createMissing) { t.set(ref,{...createMissing(ref),status:'closed',...fields}); count++; }
          else absent++;
          continue;
        }
        if (av.data().status === 'closed') continue;
        t.update(ref, {status:'closed', ...fields});
        count++;
      }
      return {count, busy, absent};
    });
    closed += result.count; skipped += result.busy; missing += result.absent;
  }
  return {closed, skipped, missing};
}
