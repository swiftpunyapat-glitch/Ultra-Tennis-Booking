import { describe, expect, test } from 'vitest';
import { readFileSync } from 'node:fs';

const adminHtml = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');
const legacyTabs = ['bookings','daily','slots','passes','slips','pending','customers','promo','vouchers','coach','activity'];
const hubMap = {
  bookings: 'bookings', daily: 'ops', slots: 'ops', passes: 'customers',
  slips: 'bookings', pending: 'bookings', customers: 'customers',
  promo: 'management', vouchers: 'management', coach: 'management', activity: 'management',
};

describe('Admin four-Hub feature parity', () => {
  test('keeps every original panel and hidden legacy tab lifecycle', () => {
    for (const tab of legacyTabs) {
      expect(adminHtml).toContain(`data-tab="${tab}"`);
      const panel = `tab${tab[0].toUpperCase()}${tab.slice(1)}`;
      expect(adminHtml).toContain(`id="${panel}"`);
    }
    expect((adminHtml.match(/<button class="tab(?: on)?"/g) || [])).toHaveLength(11);
    expect(adminHtml).toContain('class="tabbar legacy-tabbar"');
    expect(adminHtml).toContain('LEGACY PANEL LIFECYCLE (feature-parity layer)');
  });

  test('maps all eleven destinations to exactly one of four Hubs', () => {
    expect((adminHtml.match(/data-admin-hub="/g) || [])).toHaveLength(4);
    for (const [tab, hub] of Object.entries(hubMap)) {
      expect(adminHtml).toContain(`{key:"${tab}",tab:"${tab}"`);
      expect(adminHtml).toMatch(new RegExp(`${hub}:\\{[\\s\\S]*?tab:\"${tab}\"`));
    }
    expect(adminHtml).toContain('Object.fromEntries(Object.entries(ADMIN_HUBS)');
  });

  test('provides four quick actions by reusing existing flows', () => {
    for (const id of ['quickManualBooking','quickSlipQueue','quickRescheduleQueue','quickCopySlotAlert']) {
      expect(adminHtml).toContain(`id="${id}"`);
      expect(adminHtml).toContain(`$("${id}").addEventListener`);
    }
    expect(adminHtml).toContain('openManualModal(todayISOString(),null)');
    expect(adminHtml).toContain('copyDailySlotAlert()');
    expect(adminHtml).toContain('activateAdminTab("pending")');
  });
});

describe('Admin Staff / Owner view safety', () => {
  test('derives Owner capability from authenticated role and defaults everyone else to Staff', () => {
    expect(adminHtml).toContain('currentAdminRole==="owner"||currentAdminRole==="ultra_admin"');
    expect(adminHtml).toContain('if(!canSwitchOwnerView())adminViewMode="staff"');
    expect(adminHtml).toContain('hub!=="management"||(adminViewMode==="owner"&&canSwitchOwnerView())');
    expect(adminHtml).toContain('if(!isAdminTabAllowed(btn.dataset.tab))');
  });

  test('persists only view preference and retains existing privileged gates', () => {
    expect(adminHtml).toContain('localStorage.setItem(adminViewPreferenceKey(),adminViewMode)');
    expect(adminHtml).not.toMatch(/localStorage\.setItem\([^,]*(?:role|owner|permission)/i);
    expect(adminHtml).toContain('currentAdminName==="Art"&&currentAdminRole==="owner"');
    expect(adminHtml).toContain('permission:"vouchers"');
    expect(adminHtml).toContain('permission:"activity"');
  });

  test('shows explicit queue loading, count, and error states', () => {
    expect(adminHtml).toContain('updateAdminQueueBadges("loading")');
    expect(adminHtml).toContain('updateAdminQueueBadges("error")');
    for (const id of ['quickSlipBadge','quickRescheduleBadge','bookingHubBadge']) expect(adminHtml).toContain(`id="${id}"`);
  });
});

describe('Admin responsive navigation', () => {
  test('uses compact two-column Hub and Quick Action grids on mobile', () => {
    expect(adminHtml).toContain('.quickbar{grid-template-columns:repeat(2,minmax(0,1fr))');
    expect(adminHtml).toContain('.hubbar{grid-template-columns:repeat(2,minmax(0,1fr))');
    expect(adminHtml).toContain('.hub-subnav{justify-content:flex-start');
  });
});
