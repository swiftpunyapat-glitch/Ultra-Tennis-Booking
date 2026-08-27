import { describe, expect, test } from 'vitest';
import { readFileSync } from 'node:fs';

const adminHtml = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');

describe('Admin unified customer profile Hub', () => {
  test('provides one searchable customer workspace with explicit states', () => {
    for (const id of [
      'customerProfileSearch',
      'customerProfileSearchBtn',
      'customerProfileClearBtn',
      'customerProfileSort',
      'customerProfilePassFilter',
      'customersList',
    ]) expect(adminHtml).toContain(`id="${id}"`);

    expect(adminHtml).toContain('UNIFIED CUSTOMER PROFILE HUB');
    expect(adminHtml).toContain('customerProfileState="idle"');
    expect(adminHtml).toContain('customerProfileState==="loading"');
    expect(adminHtml).toContain('customerProfileState==="error"');
    expect(adminHtml).toContain('ไม่พบลูกค้าที่ตรงกับ');
  });

  test('sorts by real visits and supports practical customer filters', () => {
    for (const value of ['last_played', 'most_played', 'upcoming', 'pass_minutes', 'name']) {
      expect(adminHtml).toContain(`value="${value}"`);
    }
    for (const value of ['all', 'with_pass', 'without_pass']) {
      expect(adminHtml).toContain(`value="${value}"`);
    }
    expect(adminHtml).toContain('function filterAndSortCustomerProfiles(profiles)');
    expect(adminHtml).toContain('profile.lastPlayedAt=playedBookings.reduce');
    expect(adminHtml).toContain('booking.bookingStatus==="completed"');
    expect(adminHtml).toContain('booking.bookingStatus!=="confirmed"');
    expect(adminHtml).toContain('return (b.lastPlayedAt||"").localeCompare(a.lastPlayedAt||"")');
    expect(adminHtml).toContain('b.playedCount-a.playedCount');
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

  test('exposes an Art/Owner-only zero-write identity dry-run without merge controls', () => {
    for (const id of ['customerIdentityPanel', 'identityDryRunBtn', 'identityDownloadBtn', 'identityDryRunOutput']) {
      expect(adminHtml).toContain(`id="${id}"`);
    }
    expect(adminHtml).toContain('currentAdminName==="Art"&&currentAdminRole==="owner"&&adminViewMode==="owner"');
    expect(adminHtml).toContain('action:"identity_dry_run"');
    expect(adminHtml).toContain('report.dryRun!==true||Number(report.writesPerformed)!==0');
    expect(adminHtml).toContain('ดาวน์โหลด JSON');
    expect(adminHtml).not.toContain('id="identityMergeBtn"');
    expect(adminHtml).not.toContain('id="identityApplyBtn"');
  });
});
