export function courtRequest(resourceId, url, options = {}) {
  const body = JSON.parse(options.body || '{}');
  return fetch(url, { ...options, body: JSON.stringify({ resourceId, ...body }) });
}
export async function loadCourtSelector(select, selectedId, onChange) {
  const response = await fetch('/api/booking', { method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'features'}) });
  const result = await response.json();
  if (!response.ok || !result.ok) throw new Error('โหลดรายชื่อคอร์ตไม่ได้');
  const courts = result.commerce.resources.filter(r=>r.active);
  select.replaceChildren(...courts.map(c=>new Option(c.name,c.id)));
  select.value = courts.some(c=>c.id===selectedId) ? selectedId : courts[0].id;
  select.onchange = () => onChange(select.value);
  if (select.value !== selectedId) await onChange(select.value);
  return result.commerce;
}
