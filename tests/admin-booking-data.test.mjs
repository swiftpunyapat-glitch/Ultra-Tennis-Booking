import { describe, test, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { runInNewContext } from 'node:vm';
import { collectAdminBookingPages, paidSummary, paymentDate, paymentTime, paymentMillis } from '../admin-booking-data.js';

const paid = (id, paidAt, extra = {}) => ({ id, paidAt, resourceId:'room1', paymentStatus:'paid', bookingStatus:'confirmed', ...extra });

describe('Finance-compatible payment date semantics', () => {
  test.each([
    ['2026-10-21T16:59:59.999Z', '2026-10-21', '23:59'],
    ['2026-10-21T17:00:00.000Z', '2026-10-22', '00:00'],
    ['2026-10-31T16:59:59.999Z', '2026-10-31', '23:59'],
    ['2026-10-31T17:00:00.000Z', '2026-11-01', '00:00'],
    ['2026-12-31T17:00:00.000Z', '2027-01-01', '00:00'],
    ['2026-10-22T11:49:00+07:00', '2026-10-22', '11:49'],
  ])('uses Bangkok boundaries for %s', (timestamp, day, time) => {
    expect(paymentDate(timestamp)).toBe(day); expect(paymentTime(timestamp)).toBe(time);
  });

  test('accepts the actual API ISO/revived/Firestore timestamp shapes', () => {
    const ms=Date.parse('2026-10-22T04:49:00Z');
    for (const timestamp of [new Date(ms), {toDate:()=>new Date(ms)}, {toMillis:()=>ms}, {seconds:ms/1000,nanoseconds:0}]) {
      expect(paymentDate(timestamp)).toBe('2026-10-22');
    }
    for (const timestamp of [null, undefined, 'invalid', '2026-10-22 11:49', {}, new Date(NaN)]) expect(paymentMillis(timestamp)).toBeNull();
  });

  test('uses paidAt despite missing/conflicting review, confirm, creation and play dates', () => {
    const out=paidSummary([
      paid('cash','2026-10-22T04:49:00Z',{date:'2026-10-23',adminReviewedAt:'2026-10-23T01:00:00Z'}),
      paid('old',undefined,{adminReviewedAt:'2026-10-22T04:49:00Z',confirmedAt:'2026-10-22T04:49:00Z',createdAt:'2026-10-22T04:49:00Z'}),
    ],'2026-10-22','room1');
    expect(out.rows.map(b=>b.id)).toEqual(['cash']); expect(out.missingPaidAt).toBe(1);
  });

  test('keeps paid cancellations/refunds visible, excludes unpaid and package usage, scopes court', () => {
    const at='2026-10-22T01:00:00Z';
    const out=paidSummary([
      paid('cancel',at,{bookingStatus:'cancelled',refundStatus:'refunded',refundAmount:350}),
      paid('package',at,{paymentStatus:'package'}),paid('unpaid',at,{paymentStatus:'unpaid'}),
      paid('other',at,{resourceId:'room2'}),paid('legacy',at,{resourceId:undefined}),
    ],'2026-10-22','room1');
    expect(out.rows.map(b=>b.id)).toEqual(['cancel','legacy']); expect(out.missingPaidAt).toBe(0);
  });

  test('deduplicates document IDs without merging split bookings that share a code', () => {
    const a=paid('a','2026-10-22T01:00:00Z',{bookingCode:'SAME'}),b={...a,id:'b'};
    expect(paidSummary([a,a,b],'2026-10-22','room1').rows.map(x=>x.id)).toEqual(['a','b']);
  });
});

describe('complete server-cursor pagination', () => {
  test('loads a July record beyond 1,000 recent records before applying monthly/daily/status views', async () => {
    const recent=Array.from({length:1000},(_,i)=>({id:`new${i}`,date:'2026-10-22',bookingStatus:'confirmed'}));
    const old={id:'old-july',date:'2026-07-12',bookingStatus:'cancelled'};
    const requests=[];
    const pages=[{items:recent.slice(0,500),nextCursorId:'cursor1'}, {items:recent.slice(500),nextCursorId:'cursor2'}, {items:[old],nextCursorId:null}];
    const out=await collectAdminBookingPages(async options=>{requests.push(options);return pages.shift();},{limit:500,testsOnly:false});
    expect(out).toHaveLength(1001);
    expect(out.filter(b=>b.date.startsWith('2026-07'))).toEqual([old]);
    expect(out.filter(b=>b.date==='2026-07-12'&&b.bookingStatus==='cancelled')).toEqual([old]);
    expect(requests.map(r=>r.cursorBookingId)).toEqual([undefined,'cursor1','cursor2']);
    expect(requests.every(r=>r.testsOnly===false&&r.limit===500)).toBe(true);
  });

  test('continues through an empty filtered page and preserves global pending queues', async () => {
    const pages=[{items:[],nextCursorId:'raw-page'}, {items:[{id:'pending',date:'2026-08-01',bookingStatus:'pending_reschedule'}],nextCursorId:null}];
    expect(await collectAdminBookingPages(async()=>pages.shift())).toEqual([{id:'pending',date:'2026-08-01',bookingStatus:'pending_reschedule'}]);
  });

  test('deduplicates repeated pages by ID and keeps cancelled/split rows', async () => {
    const a={id:'a',bookingCode:'same',bookingStatus:'cancelled'}, b={...a,id:'b'};
    const pages=[{items:[a],nextCursorId:'a'}, {items:[a,b],nextCursorId:null}];
    expect(await collectAdminBookingPages(async()=>pages.shift())).toEqual([a,b]);
  });

  test.each([
    [{items:[],nextCursorId:'a'},{items:[],nextCursorId:'a'}],
    [{items:[],nextCursorId:'a'},{items:[],nextCursorId:'b'},{items:[],nextCursorId:'a'}],
    [{items:[],nextCursor:'legacy-only'}],
    [{items:[{}],nextCursorId:null}],
  ])('fails closed on malformed/nonadvancing pagination %#', async (...pages) => {
    await expect(collectAdminBookingPages(async()=>pages.shift())).rejects.toThrow();
  });

  test('does not turn a later-page failure into a successful partial history', async () => {
    let page=0;
    await expect(collectAdminBookingPages(async()=>{if(page++)throw new Error('page unavailable');return{items:[{id:'one'}],nextCursorId:'one'};})).rejects.toThrow('page unavailable');
  });
});

test('actual Admin renderer shows paidAt-only payment, cancellation/refund and missing-date warning', () => {
  const html=readFileSync(new URL('../admin.html',import.meta.url),'utf8');
  const start=html.indexOf('function renderPaidSummary(){');
  const code=html.slice(start,html.indexOf('// ──',start));
  const output={innerHTML:''};
  const context={
    $:id=>id==='paidSummaryDate'?{value:'2026-10-22'}:output,
    allBookings:[paid('cash','2026-10-22T04:49:00Z',{bookingCode:'PAYMENT-ONLY',date:'2026-10-23',bookingStatus:'cancelled',refundStatus:'refunded',refundAmount:350}),paid('legacy',null)],
    RESOURCE_ID:'room1',paidSummary,paymentTime,bookingStatusDisplay:b=>b.bookingStatus,esc:s=>String(s),
  };
  runInNewContext(code+'\nrenderPaidSummary();',context);
  expect(output.innerHTML).toContain('PAYMENT-ONLY'); expect(output.innerHTML).toContain('11:49');
  expect(output.innerHTML).toContain('cancelled'); expect(output.innerHTML).toContain('refunded');
  expect(output.innerHTML).toContain('1 paid record(s)');
});
