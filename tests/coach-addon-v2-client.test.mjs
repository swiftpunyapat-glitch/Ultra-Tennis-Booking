import { readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');

describe('Coach Add-on v2 customer cutover', () => {
  test('add-on UI is hidden by default and requires an explicit true flag', () => {
    expect(html).toContain('id="coachAddonV2Card" style="display:none"');
    expect(html).toContain('state.coachAddonV2.enabled=d.enableCoachAddonV2===true');
  });

  test('customer flow calls only the additive v2 actions', () => {
    expect(html).toContain('action:"coach_addon_v2_options"');
    expect(html).toContain('action:"coach_addon_v2_quote"');
    expect(html).toContain('action:"create_coach_addon_v2"');
    expect(html).toContain('action:"expire_coach_addon_v2"');
  });

  test('available coaches render as accessible profile cards with existing profile fields', () => {
    expect(html).toContain('class="coach-profile-list"');
    expect(html).toContain('coach-profile-card');
    expect(html).toContain('btn.setAttribute("aria-pressed"');
    expect(html).toContain('coach-profile-photo');
    expect(html).toContain('coach-profile-bio');
    expect(html).toContain('ว่างช่วงนี้');
    expect(html).toContain('พร้อมสอนครบ ${need} นาที');
    expect(html).toContain('<span class="coach-profile-price-label">ราคาคาบต่อชั่วโมง<br>รวมค่าคอร์ทแล้ว</span>');
  });

  test('only Beginner Coaching is exposed as a coaching package', () => {
    expect(html).toContain('p.packageType === "beginner_coaching_5"');
    expect(html).toContain('coach_at_ultra_10 is intentionally excluded');
  });
});
