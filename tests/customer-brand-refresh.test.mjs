import { existsSync, readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';

const customerHtml = readFileSync(new URL('../index.html', import.meta.url), 'utf8');

describe('Ultra Tennis Studio customer brand refresh', () => {
  test('uses the approved master logo and locked working palette', () => {
    expect(existsSync(new URL('../assets/brand/ultra-logo-master.png', import.meta.url))).toBe(true);
    expect(customerHtml).toContain('/assets/brand/ultra-logo-master.png');
    expect(customerHtml).toContain('--ut-yellow:#F6ED1F');
    expect(customerHtml).toContain('--ut-blue:#0A3EC1');
    expect(customerHtml).toContain('--ut-text:#14161C');
    expect(customerHtml).toContain('IBM Plex Sans Thai');
    expect(customerHtml).toContain('PLAY YOUR WAY');
  });

  test('keeps coach selection inside the standard booking flow', () => {
    expect(customerHtml).not.toContain('id="btnCoach"');
    expect(customerHtml).toContain('id="coachAddonV2Card"');
    expect(customerHtml).toContain('id="coachAddonV2Toggle"');
    expect(customerHtml).toContain('d.enableCoachAddonV2===true');
  });

  test('renames the customer redemption entry without changing its internal action', () => {
    expect(customerHtml).not.toContain('Redeem Event Code');
    expect(customerHtml).not.toContain('>Event Code<');
    expect(customerHtml).toContain('data-i18n="redeemCode">Redeem Code');
    expect(customerHtml).toContain('action:"event_pass_redeem"');
    expect(customerHtml).toContain('action:"event_pass_status"');
  });

  test('keeps price communication dynamic and equipment-inclusive', () => {
    expect(customerHtml).toContain('id="homeSinglePrice"');
    expect(customerHtml).toContain('รวมไม้ ลูก บอลแมชชีน และน้ำ');
    expect(customerHtml).toContain('setPrice(prices.normal)');
    expect(customerHtml).toContain('setPrice(promo.price)');
  });
});
