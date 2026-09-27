// Customer-only UI; all ownership, price and availability checks are server-side.
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
export function createPendingRescheduleUI({ api, signedIn, onAssigned, english = () => false }) {
  const tr = (th, en) => english() ? en : th;
  const dialog = document.createElement('dialog');
  dialog.className = 'pending-reschedule-dialog';
  dialog.setAttribute('aria-label', 'เลือกเวลาใช้สิทธิ์รอเลื่อน / Reschedule booking');
  document.body.append(dialog);
  let selected = null, timesVersion = 0, submitting = false;
  const panels = new Map();
  const today = () => new Date(Date.now() + 7 * 3600000).toISOString().slice(0, 10);

  async function load(container) {
    if (!container) return;
    const version = (panels.get(container) || 0) + 1;
    panels.set(container, version);
    if (!signedIn()) { container.replaceChildren(); return; }
    container.innerHTML = `<div class="card">${tr('กำลังตรวจสอบชั่วโมงรอเลื่อน…', 'Checking pending hours…')}</div>`;
    try {
      const data = await api({ action: 'pending_reschedules' });
      if (panels.get(container) !== version) return;
      if (!data.bookings.length) { container.replaceChildren(); return; }
      container.innerHTML = `<div class="card"><strong>🕒 ${tr('คุณมีเวลารอเลื่อน', 'Pending reschedule')}: ${Number(data.totalMinutes) / 60} ${tr('ชั่วโมง', 'hours')}</strong>
        <p>${tr('เลือกใช้ทีละการจอง ไม่คิดเงินหรือตัด Pass ซ้ำ', 'Use one booking at a time. No new payment or Pass deduction.')}</p>
        ${data.bookings.map((b, i) => `<div class="pending-reschedule-item"><div><strong>${esc(b.bookingCode)}</strong> · ${Number(b.durationMinutes) / 60} ${tr('ชม.', 'hrs')}<br><small>${esc(b.originalDate)} ${esc(b.originalStartTime)} · ${esc(b.resourceId)}</small></div>
          ${b.restriction ? `<p>${esc(english() ? 'Please contact admin to arrange this booking.' : b.restriction)}</p>` : `<button class="btn-empty" data-pending-index="${i}">${tr('เลือกวันใช้สิทธิ์', 'Choose new time')}</button>`}</div>`).join('')}
        </div>`;
      container.querySelectorAll('[data-pending-index]').forEach(button => button.addEventListener('click', () => open(data.bookings[Number(button.dataset.pendingIndex)])));
    } catch {
      if (panels.get(container) !== version) return;
      container.innerHTML = `<div class="card">${tr('ยังตรวจสอบชั่วโมงรอเลื่อนไม่ได้', 'Unable to check pending hours')} <button class="btn-empty">${tr('ลองใหม่', 'Retry')}</button></div>`;
      container.querySelector('button').addEventListener('click', () => load(container));
    }
  }

  function open(booking) {
    selected = booking; timesVersion++;
    dialog.innerHTML = `<h2>${tr('เลือกเวลาใช้สิทธิ์รอเลื่อน', 'Use pending booking')}</h2>
      <p><strong>${esc(booking.bookingCode)}</strong> · ${Number(booking.durationMinutes) / 60} ${tr('ชั่วโมง', 'hours')}</p>
      <p>${tr('ใช้คอร์ตเดิม เฉพาะเวลาที่สิทธิ์เดิมครอบคลุม หากเลือกเวลาราคาต่ำกว่า ยอดเงินเดิมยังคงเดิม', 'Same court, within the original entitlement. A cheaper time does not change the original payment.')}</p>
      <label>${tr('จำนวนชั่วโมง', 'Duration')}<select data-duration>${Array.from({ length: booking.durationMinutes / 30 }, (_, i) => (i + 1) * 30).map(m => `<option value="${m}" ${m === booking.durationMinutes ? 'selected' : ''}>${m / 60} ${tr('ชม.', 'hrs')}${m === booking.durationMinutes ? tr(' · เต็มจำนวน', ' · Full booking') : ''}</option>`).join('')}</select></label>
      <label>${tr('วันใหม่', 'New date')}<input data-date type="date" min="${today()}" value="${today()}"></label>
      <div data-times class="tgrid"></div><p data-message role="status" aria-live="polite"></p>
      <div class="pending-reschedule-actions"><button data-close class="btn-empty">${tr('ปิด', 'Close')}</button><button data-confirm class="btn-empty" disabled>${tr('ยืนยันใช้สิทธิ์', 'Confirm booking')}</button></div>`;
    dialog.querySelector('[data-close]').addEventListener('click', () => dialog.close());
    dialog.querySelector('[data-date]').addEventListener('change', loadTimes);
    dialog.querySelector('[data-duration]').addEventListener('change', loadTimes);
    dialog.querySelector('[data-confirm]').addEventListener('click', assign);
    dialog.showModal();
    loadTimes();
  }
  dialog.addEventListener('cancel', event => { if (submitting) event.preventDefault(); });
  dialog.addEventListener('close', () => { timesVersion++; selected = null; });

  async function loadTimes() {
    const version = ++timesVersion;
    const booking = selected;
    const date = dialog.querySelector('[data-date]').value;
    const duration = Number(dialog.querySelector('[data-duration]').value);
    const confirm = dialog.querySelector('[data-confirm]');
    confirm.disabled = true; delete confirm.dataset.time;
    const grid = dialog.querySelector('[data-times]'), message = dialog.querySelector('[data-message]');
    grid.replaceChildren();
    message.textContent = tr('กำลังค้นหาเวลาที่ใช้สิทธิ์ได้…', 'Finding eligible times…');
    try {
      const data = await api({ action: 'pending_reschedule_options', bookingId: booking.id, newDate: date, assignDurationMinutes: duration });
      if (version !== timesVersion || selected !== booking) return;
      if (data.fullDurationOnly && duration !== booking.durationMinutes) {
        dialog.querySelector('[data-duration]').value = String(booking.durationMinutes);
        return loadTimes();
      }
      if (data.fullDurationOnly) dialog.querySelector('[data-duration]').disabled = true;
      message.textContent = data.times.length ? (data.fullDurationOnly ? tr('รายการมีใบเสร็จแล้ว ใช้ได้เต็มจำนวน หากต้องการแบ่งชั่วโมงกรุณาติดต่อแอดมิน', 'A receipt has been issued. Use the full booking or contact admin to split it.') : tr('เลือกเวลาใหม่ แล้วกดยืนยัน ชั่วโมงที่เหลือจะยังรอเลื่อน', 'Select a time and confirm. Unused hours remain pending.')) : tr('ไม่มีเวลาว่างที่ตรงสิทธิ์ในวันนี้ ลองวันอื่นหรือติดต่อแอดมิน', 'No eligible times on this date. Try another date or contact admin.');
      for (const time of data.times) {
        const button = document.createElement('button');
        button.className = 'tbtn'; button.textContent = time;
        button.addEventListener('click', () => {
          grid.querySelectorAll('button').forEach(b => b.classList.remove('on'));
          button.classList.add('on'); confirm.dataset.time = time; confirm.disabled = false;
        });
        grid.append(button);
      }
    } catch (error) {
      if (version === timesVersion) message.textContent = error.message;
    }
  }

  async function assign() {
    if (submitting || !selected) return;
    const button = dialog.querySelector('[data-confirm]');
    if (!button.dataset.time) return;
    const booking = selected;
    const body = { action: 'assign_pending_reschedule', bookingId: booking.id, newDate: dialog.querySelector('[data-date]').value,
      newStartTime: button.dataset.time, assignDurationMinutes: Number(dialog.querySelector('[data-duration]').value) };
    const message = dialog.querySelector('[data-message]');
    submitting = true;
    dialog.querySelectorAll('button, input, select').forEach(el => { el.disabled = true; });
    message.textContent = tr('กำลังยืนยัน…', 'Confirming…');
    try {
      const data = await api(body);
      message.textContent = `${tr('ยืนยันแล้ว', 'Confirmed')}: ${body.newDate} ${body.newStartTime}–${data.newEndTime} · ${tr('เหลือรอเลื่อน', 'Remaining pending')} ${Number(data.remainderDurationMinutes) / 60} ${tr('ชม.', 'hrs')}`;
      dialog.querySelector('[data-times]').replaceChildren();
      button.hidden = true;
      message.tabIndex = -1; message.focus();
      selected = null; timesVersion++;
      await Promise.all([...panels.keys()].filter(el => el.isConnected).map(load));
      try { await onAssigned(data); } catch { /* booking already committed; allow closing */ }
    } catch (error) {
      message.textContent = `${error.message} ${tr('ปิดแล้วโหลดรายการใหม่เพื่อตรวจสอบก่อนลองอีกครั้ง', 'Close and reload bookings to check the result before retrying.')}`;
      // An ambiguous network failure may already have committed. Force a fresh
      // list instead of resubmitting a stale entitlement from this dialog.
      selected = null;
      await Promise.all([...panels.keys()].filter(el => el.isConnected).map(load));
    } finally {
      submitting = false;
      dialog.querySelector('[data-close]').disabled = false;
    }
  }
  return { load };
}
