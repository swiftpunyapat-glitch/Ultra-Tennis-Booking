// Local-only editor preview. Uses its own emulator project and no production
// credentials. Run with FIRESTORE_EMULATOR_HOST=127.0.0.1:8185.
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
if (!/^127\.0\.0\.1:\d+$/.test(process.env.FIRESTORE_EMULATOR_HOST || '')) throw new Error('Start a local Firestore emulator first');
process.env.GCLOUD_PROJECT = 'demo-commerce-preview';
process.env.ADMIN_SESSION_SECRET = randomBytes(32).toString('hex');
process.env.ADMIN_USERS_JSON = JSON.stringify({ DemoOwner: { pin:'0000',role:'owner',branches:'*' } });
const { createSessionCookie } = await import('../api/_lib/admin-auth.js');
const admin = (await import('../api/admin-user-action.js')).default;
const booking = (await import('../api/booking.js')).default;
const allowedFiles = new Set(['commerce.js','commerce-admin.js','court-selector.js']);
const shell = `<!doctype html><html lang="th"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Local Commerce Preview</title>
<style>body{background:#0d1724;color:#edf3fa;font:15px/1.5 system-ui,sans-serif;--m:#b1bfce;--t:#edf3fa;--bg:#142334;margin:0;padding:24px}main{max-width:1080px;margin:auto}button{background:#ffd45c;border:0;color:#152030}h2,h3{color:#f5d67b}</style>
<main><p>Local preview · ข้อมูลจำลองในเครื่อง</p><div id="commerceEditor"></div></main><script type="module">import {mountCommerceEditor} from '/commerce-admin.js';await mountCommerceEditor(document.querySelector('#commerceEditor'),true);</script></html>`;
http.createServer(async (req,res)=>{
  const url=new URL(req.url,'http://127.0.0.1');
  try {
    if(req.method==='GET' && url.pathname==='/'){
      res.setHeader('Set-Cookie',createSessionCookie('DemoOwner').replace(/;\s*Secure/gi,''));
      res.setHeader('Content-Type','text/html; charset=utf-8');res.end(shell);return;
    }
    if(req.method==='GET' && allowedFiles.has(url.pathname.slice(1))){res.setHeader('Content-Type','text/javascript; charset=utf-8');res.end(await readFile(new URL(`../${url.pathname.slice(1)}`,import.meta.url)));return;}
    if(req.method==='POST' && ['/api/admin-user-action','/api/booking'].includes(url.pathname)){
      let raw='';for await(const chunk of req){raw+=chunk;if(raw.length>200000)throw new Error('Request too large');}
      req.body=JSON.parse(raw);req.query=Object.fromEntries(url.searchParams);
      res.status=n=>{res.statusCode=n;return res;};res.json=value=>{res.setHeader('Content-Type','application/json');res.end(JSON.stringify(value));return res;};
      return await (url.pathname==='/api/booking'?booking:admin)(req,res);
    }
    res.writeHead(404);res.end('Not found');
  } catch(e){res.writeHead(500);res.end(e.message);}
}).listen(8787,'127.0.0.1',()=>console.log('Local editor: http://127.0.0.1:8787 (emulator project demo-commerce-preview)'));
