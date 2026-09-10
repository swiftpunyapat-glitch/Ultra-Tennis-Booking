import { availabilityDiagnostic } from '../../availability-state.js';
// Diagnostics must still work when Firestore is unavailable. This bounded,
// per-instance throttle avoids adding a database dependency to error reporting.
const windows = new Map();
export function handleAvailabilityDiagnostic(req,res,body) {
  const input = body.diagnostic;
  if (!input || typeof input !== 'object' || Array.isArray(input) || JSON.stringify(input).length > 2000) {
    return res.status(400).json({ok:false});
  }
  const key = String(req.headers?.['x-forwarded-for'] || req.socket?.remoteAddress || 'unknown').split(',')[0];
  const now = Date.now();
  for(const [ip,value] of windows) if(now-value.at>60_000) windows.delete(ip);
  const entry = windows.get(key) || {at:now,count:0};
  if(entry.count >= 6 || (!windows.has(key) && windows.size >= 1000)) return res.status(429).json({ok:false});
  entry.count++; windows.set(key,entry);
  console.info('[availability_diagnostic]',JSON.stringify(availabilityDiagnostic(input)));
  return res.status(200).json({ok:true});
}
