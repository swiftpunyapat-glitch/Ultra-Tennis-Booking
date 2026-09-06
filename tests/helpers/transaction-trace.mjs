import { AsyncLocalStorage } from 'node:async_hooks';

// Test-only instrumentation: no barriers, retries, timers or error translation.
// Each wrapper returns/awaits the original operation and is restored on exit.
export async function traceTransactions(db, events, work) {
  const context = new AsyncLocalStorage();
  const runTransaction = db.runTransaction;
  const request = db.request;
  const retry = db._retry;
  let sequence = 0, runs = 0;
  const record = (phase, extra = {}) => events.push({ sequence: ++sequence, ...context.getStore(), phase, ...extra });
  // Observe RPC retries below the transaction runner, without changing policy.
  db._retry = function (method, tag, fn) {
    return retry.call(this, method, tag, async () => {
      record('rpc:start', { method });
      try { return await fn(); }
      catch (error) { record('rpc:error', { method, code: error.code, message: error.message }); throw error; }
    });
  };
  db.request = async function (method, ...args) {
    const tracked = ['commit', 'rollback'].includes(method);
    if (tracked) record(`${method}:start`);
    try {
      const result = await request.call(this, method, ...args);
      if (tracked) record(`${method}:end`);
      return result;
    } catch (error) {
      if (tracked) record(`${method}:error`, { code: error.code, message: error.message });
      throw error;
    }
  };
  db.runTransaction = function (callback, options) {
    if (context.getStore()?.inCallback) record('nested-transaction');
    const run = ++runs;
    return context.run({ run }, async () => {
      let attempt = 0;
      record('run:start');
      try {
        const result = await runTransaction.call(this, t => context.run({ run, attempt: ++attempt, inCallback: true }, async () => {
          let active = true, pending = 0;
          const originals = {};
          record('callback:start');
          for (const method of ['get', 'getAll']) {
            originals[method] = t[method];
            t[method] = async function (...args) {
              if (!active) record('use-after-callback', { method });
              ++pending;
              record(`${method}:start`);
              try { return await originals[method].apply(this, args); }
              catch (error) { record(`${method}:error`, { code: error.code, message: error.message }); throw error; }
              finally { --pending; record(`${method}:end`, { active, pending }); }
            };
          }
          for (const method of ['create', 'set', 'update', 'delete']) {
            originals[method] = t[method];
            t[method] = function (...args) {
              if (!active) record('use-after-callback', { method });
              record(`${method}:queued`);
              return originals[method].apply(this, args);
            };
          }
          try { return await callback(t); }
          catch (error) { record('callback:error', { code: error.code, message: error.message }); throw error; }
          finally {
            active = false;
            record('callback:end', { pending });
            for (const [method, original] of Object.entries(originals)) t[method] = original;
          }
        }), options);
        record('run:end');
        return result;
      } catch (error) { record('run:error', { code: error.code, message: error.message }); throw error; }
    });
  };
  try { return await work(); }
  finally { db.runTransaction = runTransaction; db.request = request; db._retry = retry; context.disable(); }
}
