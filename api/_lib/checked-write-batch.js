// Preserve the versions used to make an administrative decision. At commit,
// re-read every dependency inside a transaction before applying any writes.
// A concurrent change requires a fresh admin decision, never a stale replay.
export function checkedWriteBatch(db, initialSnapshots = []) {
  const reads = new Map(initialSnapshots.map(s => [s.ref.path, s]));
  const writes = [];
  const batch = {
    async get(ref) {
      if (!reads.has(ref.path)) reads.set(ref.path, await ref.get());
      return reads.get(ref.path);
    },
    async commit() {
      return db.runTransaction(async t => {
        const expected = [...reads.values()];
        const current = expected.length ? await t.getAll(...expected.map(s => s.ref)) : [];
        for (let i = 0; i < expected.length; i++) {
          const before = expected[i], after = current[i];
          if (before.exists !== after.exists ||
              (before.exists && !before.updateTime.isEqual(after.updateTime))) {
            throw Object.assign(new Error('Data changed. Refresh and try again.'), { code: 'ADMIN_DATA_CHANGED' });
          }
        }
        for (const [method, args] of writes) t[method](...args);
      });
    },
  };
  for (const method of ['set', 'update', 'delete', 'create']) {
    batch[method] = (...args) => { writes.push([method, args]); return batch; };
  }
  return batch;
}
