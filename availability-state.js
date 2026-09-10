export const AVAILABILITY_BUILD = '2026-09-10-availability-incident-1';
export const AVAILABILITY_MAX_AGE = 60_000;
export function canUseAvailability(state, date, now = Date.now()) {
  const age = now - (state.availabilityLoadedAt || 0);
  return state.availabilityDate === date && state.availabilityStatus === 'ready' &&
    !state.availabilityStale && !state.availabilityRefreshing &&
    state.availabilityLoadedAt > 0 && age >= 0 && age <= AVAILABILITY_MAX_AGE;
}

// Only diagnostics about the grid; never customer names, tokens or booking IDs.
export function availabilityDiagnostic(input) {
  const result = {};
  for (const key of ['page','buildVersion','selectedDate','availabilityError','event']) {
    if (typeof input[key] === 'string') result[key] = input[key].replace(/[^a-zA-Z0-9_.:/-]/g,'').slice(0,80);
  }
  for (const key of ['loadMs','rawSlotCount','bookedSlotCount','availableSlotCount','requestId']) {
    if (Number.isFinite(input[key])) result[key] = Math.max(0,Math.min(1e7,Math.round(input[key])));
  }
  return result;
}

let lastReportAt = 0;
export function reportAvailability(input) {
  const diagnostic = availabilityDiagnostic({...input,buildVersion:AVAILABILITY_BUILD});
  try {
    const history = JSON.parse(sessionStorage.getItem('ut_availability_diagnostics') || '[]');
    sessionStorage.setItem('ut_availability_diagnostics',JSON.stringify([...history.slice(-19),{...diagnostic,at:new Date().toISOString()}]));
  } catch (_) {}
  // Report failures, slow reads and empty schedules; healthy reads stay local.
  if (!diagnostic.availabilityError && diagnostic.loadMs < 5000 && diagnostic.rawSlotCount !== 0) return;
  if (Date.now() - lastReportAt < 30_000) return;
  lastReportAt = Date.now();
  try { fetch('/api/booking',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({action:'availability_diagnostic',diagnostic}),keepalive:true,
    signal:AbortSignal.timeout(4000)}).catch(()=>{}); } catch (_) {}
}
