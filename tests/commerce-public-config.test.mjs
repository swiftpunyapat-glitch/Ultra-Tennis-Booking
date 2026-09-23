import { readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';
import { publicStoreConfig, storeConfig, validateCommerce } from '../commerce.js';

const bookingJs = readFileSync(new URL('../api/booking.js', import.meta.url), 'utf8');
const indexHtml = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const courtSelector = readFileSync(new URL('../court-selector.js', import.meta.url), 'utf8');

// A realistic configuration: a retired court, a price rise that starts next
// month, and a promotion that has not launched yet.
const pricing = {
  commerce: validateCommerce({
    brandName: 'Ultra Tennis',
    resources: [
      { id: 'room1', name: 'Court 1', active: true, openTime: '06:00', closeTime: '22:00' },
      { id: 'court2', name: 'Court 2', active: true, openTime: '06:00', closeTime: '22:00' },
      { id: 'old', name: 'Retired Court', active: false, openTime: '06:00', closeTime: '22:00' },
    ],
    rateRules: [{
      id: 'rise', name: 'New year rate', resourceIds: [], days: [], priority: 10,
      startDate: '2027-01-01', endDate: '', startTime: '00:00', endTime: '24:00',
      hourlyPrice: 500, halfHourPrice: 300,
    }],
    promotions: [{
      id: 'songkran', name: 'Songkran 30% off', resourceIds: [], days: [], priority: 5,
      startDate: '2027-04-10', endDate: '2027-04-15', startTime: '00:00', endTime: '24:00',
      type: 'percent', value: 30, allowCoupon: false,
    }],
  }),
};

const publicPayload = publicStoreConfig(pricing);
const serialized = JSON.stringify(publicPayload);

describe('the unauthenticated features payload carries no commercial plans', () => {
  test('rate rules never leave the server', () => {
    expect(publicPayload).not.toHaveProperty('rateRules');
    expect(serialized).not.toContain('rise');
    expect(serialized).not.toContain('500');
  });

  test('promotions never leave the server, including ones not yet launched', () => {
    expect(publicPayload).not.toHaveProperty('promotions');
    expect(serialized).not.toContain('songkran');
    expect(serialized).not.toContain('Songkran');
    expect(serialized).not.toContain('2027-04-10');
  });

  test('a future price change is not readable before it starts', () => {
    // The whole point: storeConfig would have published both dated rules.
    const full = JSON.stringify(storeConfig(pricing));
    expect(full).toContain('2027-01-01');
    expect(serialized).not.toContain('2027-01-01');
  });

  test('the payload is exactly two keys, so a new config field cannot leak by default', () => {
    expect(Object.keys(publicPayload).sort()).toEqual(['brandName', 'resources']);
  });

  test('a court row carries only what a page draws with', () => {
    expect(Object.keys(publicPayload.resources[0]).sort()).toEqual(['active', 'closeTime', 'id', 'name', 'openTime']);
  });
});

describe('retired courts are withheld rather than flagged', () => {
  test('an inactive court is absent, not marked', () => {
    expect(publicPayload.resources.map(r => r.id)).toEqual(['room1', 'court2']);
    expect(serialized).not.toContain('Retired Court');
  });

  test('every row that survives says active, so existing client filters still match', () => {
    expect(publicPayload.resources.every(r => r.active === true)).toBe(true);
  });

  test('the two client filters keep working against this shape', () => {
    // court-selector.js: resources.filter(r => r.active)
    expect(courtSelector).toContain('result.commerce.resources.filter(r=>r.active)');
    expect(publicPayload.resources.filter(r => r.active)).toHaveLength(2);
    // index.html: resources.find(r => r.id === RESOURCE_ID && r.active)
    expect(indexHtml).toContain('state.commerce?.resources?.find(r=>r.id===RESOURCE_ID && r.active)');
    expect(publicPayload.resources.find(r => r.id === 'court2' && r.active)).toBeTruthy();
    expect(publicPayload.resources.find(r => r.id === 'old' && r.active)).toBeUndefined();
  });
});

describe('defaults and edge cases', () => {
  test('no stored config still yields a usable court list', () => {
    const fallback = publicStoreConfig(null);
    expect(fallback.resources).toHaveLength(1);
    expect(fallback.resources[0].id).toBe('room1');
    expect(fallback).not.toHaveProperty('promotions');
  });

  test.each([undefined, {}, { commerce: {} }, { commerce: { resources: [] } }])('%o does not throw', input => {
    expect(() => publicStoreConfig(input)).not.toThrow();
  });
});

describe('wiring', () => {
  test('the features route uses the public projection, not the full config', () => {
    const fn = bookingJs.slice(bookingJs.indexOf('async function handleFeatures'), bookingJs.indexOf('async function handleCoachOptions'));
    expect(fn).toContain('publicStoreConfig(null)');
    expect(fn).toContain('publicStoreConfig(pricing)');
    expect(fn).not.toMatch(/commerce = storeConfig\(/);
  });

  test('the admin editor still reads the full config through its own authenticated action', () => {
    const adminLib = readFileSync(new URL('../api/_lib/commerce-admin.js', import.meta.url), 'utf8');
    expect(adminLib).toContain("body.action === 'commerce_get'");
    expect(adminLib).toContain('commerce: await readStore(db)');
  });

  test('pricing itself still sees every rule — this narrows the response, not the engine', () => {
    expect(bookingJs).toContain('storeConfig(');
    expect(storeConfig(pricing).promotions).toHaveLength(1);
    expect(storeConfig(pricing).rateRules).toHaveLength(1);
  });
});
