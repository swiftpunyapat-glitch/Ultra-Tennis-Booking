// Shared defaults for legacy documents and the company-header editor.
export const DEFAULT_COMPANY_PROFILE = Object.freeze({
  legalNameTh: 'บริษัท สวิฟท์ สปอร์ตส์ กรุ๊ป จำกัด',
  legalNameEn: 'Swift Sports Group Co., Ltd.', brandName: 'Ultra Tennis',
  taxId: '', branch: 'สำนักงานใหญ่', address: '', phone: '', email: '',
  vatRegistered: false, vatRate: 0.07, taxInvoiceEnabled: false,
});
export const COMPANY_FIELDS = Object.freeze({
  legalNameTh: 200, legalNameEn: 200, brandName: 100, branch: 100,
  address: 1000, taxId: 13, phone: 50, email: 254,
});

export function validateCompanyProfile(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('ข้อมูลหัวเอกสารไม่ถูกต้อง');
  if (Object.keys(input).some(k => !Object.hasOwn(COMPANY_FIELDS, k))) throw new Error('มีข้อมูลที่ไม่รองรับในหัวเอกสาร');
  const profile = {};
  for (const [key, max] of Object.entries(COMPANY_FIELDS)) {
    if (typeof input[key] !== 'string') throw new Error(`กรุณาระบุ ${key} เป็นข้อความ`);
    const value = input[key].trim();
    if (value.length > max) throw new Error(`${key} ยาวเกิน ${max} ตัวอักษร`);
    if (/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(value) ||
        (key !== 'address' && /[\r\n\t]/.test(value))) throw new Error(`${key} มีอักขระที่ไม่รองรับ`);
    profile[key] = value;
  }
  if (!profile.legalNameTh) throw new Error('กรุณาระบุชื่อผู้ออกเอกสารภาษาไทย');
  if (profile.taxId && !/^\d{13}$/.test(profile.taxId)) throw new Error('เลขประจำตัวผู้เสียภาษีต้องเป็นตัวเลข 13 หลัก หรือเว้นว่าง');
  if (profile.email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(profile.email)) throw new Error('รูปแบบอีเมลไม่ถูกต้อง');
  // Editing the header never changes the accounting/tax mode.
  return { ...DEFAULT_COMPANY_PROFILE, ...profile };
}

export function documentCompanyProfile(snapshot) {
  // Only missing legacy fields fall back. Intentional empty values stay empty.
  return { ...DEFAULT_COMPANY_PROFILE, ...(snapshot || {}) };
}
const esc = value => String(value ?? '').replace(/[&<>"']/g, c =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
export function companyHeaderHTML(snapshot) {
  const p = documentCompanyProfile(snapshot);
  const line = (value, label = '') => value ? `<p>${label}${esc(value)}</p>` : '';
  return `<h2>${esc(p.legalNameTh)}</h2>${line(p.legalNameEn)}${line(p.brandName, 'Brand: ')}${line(p.branch, 'Branch: ')}${
    p.address ? `<p style="white-space:pre-wrap">${esc(p.address)}</p>` : ''
  }${line(p.taxId, 'Tax ID: ')}${line(p.phone, 'Phone: ')}${line(p.email, 'Email: ')}`;
}
export function companyFooterHTML(snapshot, isReceipt) {
  const p = documentCompanyProfile(snapshot);
  return `<p>เอกสารนี้ออกโดย ${esc(p.legalNameTh)}</p>${isReceipt ? '<p><strong>This document is not a tax invoice.</strong></p>' : ''}`;
}
