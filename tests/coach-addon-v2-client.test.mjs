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

  test('coach card explains that the lesson price already includes court', () => {
    expect(html).toContain('(รวมค่าคอร์ทแล้ว)');
  });

  test('only Beginner Coaching is exposed as a coaching package', () => {
    expect(html).toContain('p.packageType === "beginner_coaching_5"');
    expect(html).toContain('coach_at_ultra_10 is intentionally excluded');
  });
});
