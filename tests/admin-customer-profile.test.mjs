import { describe, expect, test } from 'vitest';
import { readFileSync } from 'node:fs';

const adminHtml = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');

describe('Admin unified customer profile Hub', () => {
  test('provides one searchable customer workspace with explicit states', () => {
    for (const id of [
      'customerProfileSearch',
      'customerProfileSearchBtn',
      'customerProfileClearBtn',
      'customersList',
    ]) expect(adminHtml).toContain(`id="${id}"`);

    expect(adminHtml).toContain('UNIFIED CUSTOMER PROFILE HUB');
    expect(adminHtml).toContain('customerProfileState="idle"');
    expect(adminHtml).toContain('customerProfileState==="loading"');
    expect(adminHtml).toContain('customerProfileState==="error"');
    expect(adminHtml).toContain('ไม่พบลูกค้าที่ตรงกับ');
  });

  test('joins existing booking, registered-user and package data without schema changes', () => {
    expect(adminHtml).toContain('adminRead("registered_users",{limit:500})');
    expect(adminHtml).toContain('adminRead("packages",{limit:500})');
    expect(adminHtml).toContain('allBookings.forEach(booking=>');
    expect(adminHtml).toContain('customerPackageData.filter(pass=>pass.status==="active")');
    expect(adminHtml).toContain('ประวัติการจองทั้งหมด');
    expect(adminHtml).toContain('Pass ที่ถืออยู่');
  });

  test('reuses the existing issue-Pass and minute-adjust flows', () => {
    expect(adminHtml).toContain('selectRegisteredUser(selected.registeredUser)');
    expect(adminHtml).toContain('activateAdminTab("passes",{subKey:"passes",focusId:"selectedUserPanel"})');
    expect(adminHtml).toContain('openMinModal(button.dataset.customerAdjust)');
    expect(adminHtml).toContain('add_pass_to_registered_user');
    expect(adminHtml).toContain('adjust_pass_minutes');
  });

  test('mirrors the server role gate for Pass mutations', () => {
    expect(adminHtml).toContain('["owner","ultra_admin","branch_manager"].includes(currentAdminRole)');
    expect(adminHtml).toContain('mayManage&&registered?"":"disabled"');
  });

  test('collapses the profile layout cleanly on mobile', () => {
    expect(adminHtml).toContain('.customer-hub-grid{grid-template-columns:1fr;}');
    expect(adminHtml).toContain('.customer-kpis{grid-template-columns:repeat(2,minmax(0,1fr));}');
  });
});
