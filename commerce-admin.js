// Owner editor for courts and commercial rules. All mutations go through the
// authenticated server endpoint; the browser cannot select automatic payment.
const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const input = (key, label, value, type = 'text') => `<label>${label}<input data-field="${key}" type="${type}" value="${escape(value)}" ${type === 'number' ? 'min="0" step="0.01"' : ''}></label>`;
const check = (key, label, value) => `<label class="commerce-check"><input data-field="${key}" type="checkbox" ${value ? 'checked' : ''}>${label}</label>`;
async function request(action, body = {}) {
  const response = await fetch('/api/admin-user-action', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action, ...body }) });
  const result = await response.json();
  if (!response.ok || !result.ok) throw new Error(result.error || 'Request failed');
  return result;
}
let editor;
export async function mountCommerceEditor(root, owner) {
  root.hidden = !owner;
  if (!owner) { root.replaceChildren(); editor = null; return; }
  if (editor?.root === root) return;
  root.textContent = 'กำลังโหลดการตั้งค่า…';
  try {
    const { commerce } = await request('commerce_get');
    editor = { root, config: commerce };
    render();
  } catch (e) { root.textContent = e.message; }
}
function render() {
  const { root, config } = editor;
  root.innerHTML = `<style>
    .commerce-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:12px;margin:12px 0}
    #commerceEditor label{font-size:12px;display:flex;flex-direction:column;gap:5px;color:var(--m)}
    #commerceEditor input,#commerceEditor select{padding:9px;background:var(--bg,#10151d);color:var(--t,#fff);border:1px solid #45505b;border-radius:7px;width:100%;box-sizing:border-box}
    #commerceEditor .commerce-check{flex-direction:row;align-items:center}#commerceEditor input[type=checkbox]{width:auto}
    .commerce-rule{border:1px solid #45505b;border-radius:10px;padding:14px;margin:12px 0}
    #commerceEditor button{margin:4px;padding:9px 13px;border-radius:7px;cursor:pointer}
    .commerce-days{display:flex;flex-wrap:wrap;gap:10px}.commerce-note{font-size:12px;line-height:1.6;color:var(--m)}
  </style><h2>คอร์ต ราคา และ Promotion</h2>
  <p class="commerce-note">ราคาเฉพาะจะใช้แทนราคาพื้นฐานด้านล่างเมื่อเงื่อนไขตรงกัน เลือกกฎที่มีลำดับสูงสุด (ถ้าเท่ากันเรียงตามรหัส) แล้วใช้ Promotion ได้หนึ่งรายการต่อการจอง และ Coupon ได้หนึ่งโค้ดเมื่ออนุญาต ส่วนลดบาท/ราคาสุทธิคิดต่อการจอง วันและเวลาอ้างอิงวันใช้สนาม ใช้กับการเช่าคอร์ต ส่วนคาบโค้ชและแพ็กเกจยังใช้เงื่อนไขเดิม</p>
  <p class="commerce-note">การตรวจสลิป: แอดมินตรวจและอนุมัติ · ระบบตรวจอัตโนมัติยังไม่เชื่อมต่อ</p>
  <h3>คอร์ตและเวลาเปิด</h3><div id="commerceCourts"></div><button type="button" data-add="resources">+ เพิ่มคอร์ต</button>
  <h3>ราคาเฉพาะคอร์ต / วัน / เวลา</h3><div id="commerceRates"></div><button type="button" data-add="rateRules">+ เพิ่มกฎราคา</button>
  <h3>Promotion อัตโนมัติ</h3><div id="commercePromos"></div><button type="button" data-add="promotions">+ เพิ่ม Promotion</button>
  <div class="commerce-rule"><h3>ทดลองราคาก่อนบันทึก</h3><p class="commerce-note">ใช้กฎที่แก้ไขในฟอร์มนี้ และราคาพื้นฐานที่บันทึกแล้ว</p><div class="commerce-grid" id="commercePreview">
    ${input('date','วันที่ใช้สนาม',new Date().toLocaleDateString('en-CA',{timeZone:'Asia/Bangkok'}),'date')}
    ${input('startTime','เวลาเริ่ม','12:00','time')}
    <label>ระยะเวลา<select data-field="durationMinutes">${[30,60,90,120,150,180].map(n=>`<option value="${n}" ${n===60?'selected':''}>${n} นาที</option>`).join('')}</select></label>
    <label>คอร์ต<select data-field="resourceId">${config.resources.map(r=>`<option value="${escape(r.id)}">${escape(r.name)}</option>`).join('')}</select></label>
    ${input('voucherCode','Coupon (ถ้ามี)','')}${input('lineUserId','LINE User ID สำหรับคูปองระบุเจ้าของ','')}
  </div><button type="button" data-preview>คำนวณตัวอย่าง</button><div id="commercePreviewResult" role="status"></div></div>
  <button type="button" data-save>บันทึกคอร์ตและกฎทั้งหมด</button><button type="button" data-reload>โหลดค่าที่บันทึกใหม่</button>
  <p id="commerceMessage" role="status" class="commerce-note">รุ่นการตั้งค่า ${config.revision || 0} · การจองเดิมคงราคาเดิม</p>`;
  root.querySelector('#commerceCourts').innerHTML = config.resources.map(r => `<div class="commerce-rule" data-kind="resources" data-id="${escape(r.id)}"><div class="commerce-grid">${input('name','ชื่อคอร์ต',r.name)}${input('openTime','เปิด (00:00–23:30)',r.openTime)}${input('closeTime','ปิด (ถึง 24:00)',r.closeTime)}${check('active','เปิดใช้งาน',r.active)}</div><small>${escape(r.id)}</small></div>`).join('');
  for (const [kind, target] of [['rateRules','commerceRates'],['promotions','commercePromos']]) {
    root.querySelector(`#${target}`).innerHTML = config[kind].map(r => ruleHTML(r, kind, config.resources)).join('');
  }
  root.onclick = async event => {
    const button = event.target.closest('button'); if (!button) return;
    const message = root.querySelector('#commerceMessage');
    try {
      if (button.hasAttribute('data-reload')) {
        editor.config = (await request('commerce_get')).commerce; render(); return;
      }
      const draft = readConfig();
      if (button.dataset.add) {
        const kind = button.dataset.add;
        const id = `${kind === 'resources' ? 'court' : 'rule'}-${crypto.randomUUID().slice(0,8)}`;
        draft[kind].push(kind === 'resources' ? { id, name: `คอร์ต ${draft.resources.length+1}`, active:true, openTime:'06:00',closeTime:'24:00' }
          : { id, name: kind === 'rateRules' ? 'ราคาใหม่' : 'โปรใหม่', active:false, resourceIds:[],days:[],startTime:'00:00',endTime:'24:00',startDate:'',endDate:'',priority:10,
            hourlyPrice:350,halfHourPrice:200,type:'amount',value:50,maxDiscount:0,minSubtotal:0,minDuration:60,allowCoupon:false });
        editor.config=draft; render(); return;
      }
      if (button.dataset.remove) {
        const row=button.closest('[data-kind]'); draft[row.dataset.kind]=draft[row.dataset.kind].filter(r=>r.id!==row.dataset.id); editor.config=draft; render(); return;
      }
      button.disabled=true;
      if (button.hasAttribute('data-preview')) {
        const fields = readFields(root.querySelector('#commercePreview'));
        const { quote } = await request('commerce_preview', { commerce:draft,...fields,durationMinutes:Number(fields.durationMinutes) });
        root.querySelector('#commercePreviewResult').textContent = `ราคา ${quote.subtotal} − โปร ${quote.promotionDiscount || 0} − Coupon ${quote.couponDiscount || 0} = ${quote.finalPrice} บาท${quote.voucherReason ? ` · Coupon: ${quote.voucherReason}` : ''}`;
      }
      if (button.hasAttribute('data-save')) {
        editor.config=(await request('commerce_save',{commerce:draft,revision:editor.config.revision || 0})).commerce;
        render(); root.querySelector('#commerceMessage').textContent='บันทึกแล้ว ใช้กับการจองใหม่';
        window.dispatchEvent(new Event('commerce-saved'));
      }
    } catch(e) { message.textContent=e.message; }
    finally { button.disabled=false; }
  };
}
function ruleHTML(r, kind, courts) {
  const promo = kind === 'promotions';
  return `<div class="commerce-rule" data-kind="${kind}" data-id="${escape(r.id)}"><div class="commerce-grid">
    ${input('name','ชื่อ',r.name)}${input('priority','ลำดับ (มากใช้ก่อน)',r.priority,'number')}
    ${input('startDate','ตั้งแต่วันที่',r.startDate,'date')}${input('endDate','ถึงวันที่',r.endDate,'date')}
    ${input('startTime','เริ่มเวลา',r.startTime)}${input('endTime','สิ้นสุดเวลา',r.endTime)}
    ${promo ? `<label>รูปแบบ<select data-field="type">${[['amount','ลดบาท / การจอง'],['percent','ลดเปอร์เซ็นต์'],['fixed','ราคาสุทธิ / การจอง']].map(([v,n])=>`<option value="${v}" ${r.type===v?'selected':''}>${n}</option>`).join('')}</select></label>${input('value','จำนวน / เปอร์เซ็นต์',r.value,'number')}${input('maxDiscount','เพดานส่วนลด (0 = ไม่จำกัด)',r.maxDiscount,'number')}${input('minSubtotal','ยอดขั้นต่ำ',r.minSubtotal,'number')}${input('minDuration','นาทีขั้นต่ำ',r.minDuration,'number')}${check('allowCoupon','ใช้ Coupon ร่วมได้',r.allowCoupon)}` : `${input('hourlyPrice','ราคาชั่วโมง',r.hourlyPrice,'number')}${input('halfHourPrice','ราคาครึ่งชั่วโมง',r.halfHourPrice,'number')}`}
    ${check('active','เปิดใช้กฎนี้',r.active)}${check('excludeHolidays','ไม่รวมวันหยุดพิเศษ',r.excludeHolidays)}</div>
    <p class="commerce-note">วัน (ไม่เลือก = ทุกวัน)</p><div class="commerce-days">${['อา','จ','อ','พ','พฤ','ศ','ส'].map((d,i)=>`<label class="commerce-check"><input type="checkbox" data-day="${i}" ${r.days.includes(i)?'checked':''}>${d}</label>`).join('')}</div>
    <p class="commerce-note">คอร์ต (ไม่เลือก = ทุกคอร์ต)</p><div class="commerce-days">${courts.map(c=>`<label class="commerce-check"><input type="checkbox" data-court="${escape(c.id)}" ${r.resourceIds.includes(c.id)?'checked':''}>${escape(c.name)}</label>`).join('')}</div>
    <button type="button" data-remove="true">ลบกฎ</button><small>${escape(r.id)}</small></div>`;
}
function readFields(root) {
  return Object.fromEntries([...root.querySelectorAll('[data-field]')].map(el=>[el.dataset.field,el.type==='checkbox'?el.checked:el.type==='number'?Number(el.value):el.value]));
}
function readConfig() {
  const { root,config }=editor;
  const draft={...config,paymentVerificationMode:'manual'};
  for(const kind of ['resources','rateRules','promotions']) {
    draft[kind]=[...root.querySelectorAll(`[data-kind="${kind}"]`)].map(row=>({id:row.dataset.id,...readFields(row),
      ...(kind==='resources'?{}:{days:[...row.querySelectorAll('[data-day]:checked')].map(el=>Number(el.dataset.day)),resourceIds:[...row.querySelectorAll('[data-court]:checked')].map(el=>el.dataset.court)})}));
  }
  return draft;
}
