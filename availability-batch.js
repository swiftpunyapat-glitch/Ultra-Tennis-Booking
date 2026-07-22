const STYLE_ID = 'ut-availability-batch-styles';

export function isAvailabilityActionable(item, mode) {
  return Boolean(item && (mode === 'open' ? item.canOpen : item.canClose));
}

function installStyles() {
  if (document.getElementById(STYLE_ID)) return;
  const style = document.createElement('style');
  style.id = STYLE_ID;
  style.textContent = `
    .ut-batch{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:12px 0;padding:10px;border:1px solid var(--bd,var(--line,#dbe3ec));border-radius:12px;background:var(--s1,var(--card,#f8fafc))}
    .ut-batch__modes{display:flex;gap:6px;padding:3px;background:#e8eef5;border-radius:9px}
    .ut-batch button{border:1px solid var(--bd,var(--line,#c8d2df));background:var(--s2,#fff);color:var(--t,var(--text,#172033));border-radius:8px;padding:8px 11px;font:inherit;font-weight:700;cursor:pointer}
    .ut-batch button:hover:not(:disabled){border-color:#6084aa}
    .ut-batch button:disabled{opacity:.5;cursor:not-allowed}
    .ut-batch__mode.is-active{background:#17324d;color:#fff;border-color:#17324d}
    .ut-batch__mode[data-mode="close"].is-active{background:#8b2d2d;border-color:#8b2d2d}
    .ut-batch__count{margin-left:auto;font-size:.92rem;font-weight:700;color:var(--m,var(--muted,#334155))}
    .ut-batch__save{background:#147d64!important;border-color:#147d64!important;color:#fff!important;min-width:120px}
    .ut-batch__save[data-mode="close"]{background:#a43838!important;border-color:#a43838!important}
    .ut-batch__status{margin:0 2px 12px;font-size:.82rem;line-height:1.45;color:#147d64}
    .ut-batch__status.is-error{color:#b42318}
    .ut-batch__status:empty{display:none}
    @media(max-width:640px){.ut-batch__count{order:3;width:100%;margin-left:0}.ut-batch__save{margin-left:auto}}
  `;
  document.head.appendChild(style);
}

function button(label, className) {
  const el = document.createElement('button');
  el.type = 'button';
  el.className = className;
  el.textContent = label;
  return el;
}

export function createAvailabilityBatchSelector({ mount, onSave, onSelectionChange } = {}) {
  const root = typeof mount === 'string' ? document.querySelector(mount) : mount;
  if (!root) throw new Error('Batch selector mount element not found');
  if (typeof onSave !== 'function') throw new Error('Batch selector onSave callback is required');
  installStyles();

  let mode = 'open';
  let busy = false;
  let items = new Map();
  let selected = new Set();

  root.replaceChildren();
  const bar = document.createElement('div');
  bar.className = 'ut-batch';
  const modes = document.createElement('div');
  modes.className = 'ut-batch__modes';
  const openBtn = button('เปิดหลายช่วง', 'ut-batch__mode');
  openBtn.dataset.mode = 'open';
  const closeBtn = button('ปิดหลายช่วง', 'ut-batch__mode');
  closeBtn.dataset.mode = 'close';
  modes.append(openBtn, closeBtn);
  const allBtn = button('เลือกทั้งหมด', 'ut-batch__all');
  const clearBtn = button('ล้างรายการ', 'ut-batch__clear');
  const count = document.createElement('span');
  count.className = 'ut-batch__count';
  count.setAttribute('aria-live', 'polite');
  const saveBtn = button('บันทึก', 'ut-batch__save');
  const status = document.createElement('p');
  status.className = 'ut-batch__status';
  status.setAttribute('aria-live', 'polite');
  bar.append(modes, allBtn, clearBtn, count, saveBtn);
  root.append(bar, status);

  function setStatus(message, isError = false) {
    status.textContent = message || '';
    status.classList.toggle('is-error', Boolean(message && isError));
  }

  const isActionable = (item) => isAvailabilityActionable(item, mode);
  const actionableIds = () => [...items.values()].filter(isActionable).map(item => item.id);

  function notify() {
    if (typeof onSelectionChange === 'function') {
      onSelectionChange({ mode, open: mode === 'open', selectedIds: [...selected] });
    }
  }

  function render(notifyChange = true) {
    const actionable = actionableIds();
    openBtn.classList.toggle('is-active', mode === 'open');
    closeBtn.classList.toggle('is-active', mode === 'close');
    openBtn.setAttribute('aria-pressed', String(mode === 'open'));
    closeBtn.setAttribute('aria-pressed', String(mode === 'close'));
    count.textContent = `เลือกแล้ว ${selected.size} ช่วงเวลา`;
    saveBtn.textContent = busy ? 'กำลังบันทึก…' : `บันทึก ${selected.size} ช่วง`;
    saveBtn.dataset.mode = mode;
    saveBtn.disabled = busy || selected.size === 0;
    openBtn.disabled = busy;
    closeBtn.disabled = busy;
    allBtn.disabled = busy || actionable.length === 0 || actionable.every(id => selected.has(id));
    clearBtn.disabled = busy || selected.size === 0;
    if (notifyChange) notify();
  }

  function setMode(nextMode) {
    if (busy || nextMode === mode || !['open', 'close'].includes(nextMode)) return;
    mode = nextMode;
    selected.clear();
    render();
  }

  openBtn.addEventListener('click', () => setMode('open'));
  closeBtn.addEventListener('click', () => setMode('close'));
  allBtn.addEventListener('click', () => {
    if (busy) return;
    selected = new Set(actionableIds());
    render();
  });
  clearBtn.addEventListener('click', () => {
    if (busy) return;
    selected.clear();
    render();
  });
  saveBtn.addEventListener('click', async () => {
    if (busy || selected.size === 0) return;
    busy = true;
    render(false);
    const submittedIds = [...selected];
    try {
      const result = await onSave({
        mode,
        open: mode === 'open',
        items: submittedIds.map(id => items.get(id)).filter(Boolean),
      });
      const failedIds = new Set(Array.isArray(result?.failedIds) ? result.failedIds : []);
      selected = new Set(submittedIds.filter(id => failedIds.has(id) && isActionable(items.get(id))));
      if (result?.message) setStatus(result.message, result.isError);
    } catch (error) {
      console.error('[availability batch save]', error);
    } finally {
      busy = false;
      render();
    }
  });

  render(false);

  return {
    setItems(nextItems, { preserveSelection = true } = {}) {
      items = new Map((Array.isArray(nextItems) ? nextItems : []).map(item => [item.id, item]));
      selected = preserveSelection
        ? new Set([...selected].filter(id => isActionable(items.get(id))))
        : new Set();
      render();
    },
    toggle(id) {
      if (busy || !isActionable(items.get(id))) return false;
      if (selected.has(id)) selected.delete(id); else selected.add(id);
      render();
      return selected.has(id);
    },
    clear() {
      if (busy) return;
      selected.clear();
      render();
    },
    isSelected: id => selected.has(id),
    isActionable: id => isActionable(items.get(id)),
    getMode: () => mode,
    getSelectedIds: () => [...selected],
    setStatus,
  };
}
