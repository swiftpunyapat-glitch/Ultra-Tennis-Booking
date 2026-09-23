import {describe,test,expect,vi,afterEach} from 'vitest';
import {canUseAvailability,availabilityDiagnostic,reportAvailability} from '../availability-state.js';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const now=100_000;
const fresh={availabilityDate:'2026-09-10',availabilityStatus:'ready',availabilityLoadedAt:now-100};
test('only complete, fresh data for the selected date can be used',()=>{
  expect(canUseAvailability(fresh,'2026-09-10',now)).toBe(true);
  for(const change of [{availabilityStatus:'error'},{availabilityStatus:'loading'},{availabilityStale:true},{availabilityRefreshing:true},{availabilityLoadedAt:1},{availabilityLoadedAt:now+1},{availabilityDate:'2026-09-11'}]){
    expect(canUseAvailability({...fresh,...change},'2026-09-10',now)).toBe(false);
  }
});
test('diagnostics exclude customer identifiers and bound values',()=>{
  expect(availabilityDiagnostic({page:'customer',rawSlotCount:5,loadMs:Infinity,phone:'0812345678',idToken:'secret',bookingId:'private'})).toEqual({page:'customer',rawSlotCount:5});
});
test('diagnostics never break availability on an unsupported browser',()=>{
  vi.stubGlobal('AbortSignal',{});
  expect(()=>reportAvailability({page:'customer',availabilityError:'unavailable'})).not.toThrow();
});
afterEach(()=>vi.unstubAllGlobals());

// Exercise the real loader and renderer without Firebase or a live browser.
const html=readFileSync(new URL('../index.html',import.meta.url),'utf8');
function harness({cached,fail=false,empty=false,blocked=false}={}){
  const nodes=new Map();
  function node(){return {innerHTML:'',textContent:'',style:{},append(){},prepend(){},appendChild(){},addEventListener(){}};}
  const $=id=>{if(!nodes.has(id))nodes.set(id,node());return nodes.get(id);};
  const state={time:'10:00',availabilityStatus:'idle',durationMinutes:60};
  const snaps=[{size:empty?0:1,forEach:f=>{if(!empty)f({data:()=>({status:'open',startTime:'10:00'})});}}, {size:0,forEach(){}}, {exists:()=>false}];
  const context=vm.createContext({state,$,document:{createElement:node},currentLang:'en',console:{error(){}},Date,Set,Map,
    RESOURCE_ID:'room1',bookingAvailabilityRequestId:0,bookingAvailabilityCache:new Map(cached?[['room1:2026-09-10',cached]]:[]),
    getSelectedDateISO:()=> '2026-09-10',fetchFreshAvailability:async()=>{if(fail)throw {code:'unavailable'};return snaps;},
    updateBookingConfirmBtn:vi.fn(),reportAvailability:vi.fn(),updateDebugTelemetry:vi.fn(),refreshQuote:vi.fn(),
    getDisabledReasonRange:()=>blocked?'booked':'',toMinB:()=>600,isLateNightHour:()=>false,toHHMMB:()=> '10:00',
    canUseAvailability,bangkokDateParts:()=>({hour:0}),bangkokTodayKey:()=> '2026-09-09',slotDateTime:()=>new Date(),
    OPEN_HOUR:10,CLOSE_HOUR:11,t:k=>k,hardRetryBookingAvailability:vi.fn(),
  });
  vm.runInContext(html.slice(html.indexOf('async function loadSlotsAndRender('),html.indexOf('\nfunction reloadBookingWithCacheBust(')),context);
  vm.runInContext(html.slice(html.indexOf('function renderTimes()'),html.indexOf('// Safari/WebView may restore')),context);
  return {context,state,$};
}
describe('customer availability recovery',()=>{
  test('failed first read shows retry, never a closed grid',async()=>{
    const h=harness({fail:true});await h.context.loadSlotsAndRender('2026-09-10');
    expect(h.state.availabilityStatus).toBe('error');expect(h.$('slotsText').textContent).toBe('Could not load availability');
  });
  test('failed refresh retains last result but cannot book it',async()=>{
    const h=harness({fail:true,cached:{openSlots:['10:00'],blockedCells:[],loadedAt:Date.now(),rawSlotCount:1}});
    await h.context.loadSlotsAndRender('2026-09-10');
    expect(h.state.openSlots.has('10:00')).toBe(true);expect(canUseAvailability(h.state,'2026-09-10')).toBe(false);
  });
  test('an empty successful read is an absent schedule',async()=>{
    const h=harness({empty:true,blocked:true});await h.context.loadSlotsAndRender('2026-09-10');
    expect(h.$('slotsText').textContent).toBe('No schedule for this date');expect(h.state.time).toBe(null);
  });
  test('a newly occupied selection is cleared on refresh',async()=>{
    const h=harness({blocked:true});await h.context.loadSlotsAndRender('2026-09-10');
    expect(h.state.time).toBe(null);expect(h.state.availabilityStatus).toBe('ready');
  });
  test('an older response cannot overwrite the current result or its cache',async()=>{
    const h=harness({empty:true,blocked:true});
    const fetch=h.context.fetchFreshAvailability;
    let resolve;
    h.context.fetchFreshAvailability=()=>new Promise(r=>{resolve=r;});
    const old=h.context.loadSlotsAndRender('2026-09-10');
    h.context.fetchFreshAvailability=fetch;
    await h.context.loadSlotsAndRender('2026-09-10');
    resolve([{size:1,forEach:f=>f({data:()=>({status:'open',startTime:'10:00'})})},{size:0,forEach(){}},{exists:()=>false}]);
    await old;
    expect(h.state.availabilityRawCount).toBe(0);
    expect(h.context.bookingAvailabilityCache.get('room1:2026-09-10').rawSlotCount).toBe(0);
  });
});

test('switching courts never shows another court cached availability',async()=>{
  const h=harness({fail:true,cached:{openSlots:['10:00'],blockedCells:[],loadedAt:Date.now(),rawSlotCount:1}});
  h.context.RESOURCE_ID='court2';
  await h.context.loadSlotsAndRender('2026-09-10');
  expect(h.state.openSlots.has('10:00')).toBe(false);
  expect(h.state.availabilityStatus).toBe('error');
});
test('switching courts while loading discards the old court response',async()=>{
  const h=harness({empty:true,blocked:true});
  const fetch=h.context.fetchFreshAvailability;
  let resolve;
  h.context.fetchFreshAvailability=()=>new Promise(r=>{resolve=r;});
  const old=h.context.loadSlotsAndRender('2026-09-10');
  h.context.RESOURCE_ID='court2';h.context.fetchFreshAvailability=fetch;
  await h.context.loadSlotsAndRender('2026-09-10');
  resolve([{size:1,forEach:f=>f({data:()=>({status:'open',startTime:'10:00'})})},{size:0,forEach(){}},{exists:()=>false}]);
  await old;
  expect(h.state.availabilityRawCount).toBe(0);
  expect(h.context.bookingAvailabilityCache.get('court2:2026-09-10').rawSlotCount).toBe(0);
  expect(h.context.bookingAvailabilityCache.has('room1:2026-09-10')).toBe(false);
});
