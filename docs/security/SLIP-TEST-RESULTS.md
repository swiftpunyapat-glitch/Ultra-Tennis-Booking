# Slip hotfix — ผลทดสอบและข้อจำกัด

22 ต.ค. 2026: **198 ผ่าน / 0 ไม่ผ่าน / 9 test files** บน Windows, **Node 22.23.4**, Java 17.0.19, Firebase CLI 13.35.1, Vitest 2.1.9, sharp 0.35.5 ใช้ Auth/Firestore/Storage emulators ที่ loopback กับ project `demo-slip-security` เท่านั้น ระยะทดสอบประมาณ 94 วินาที ไม่ใช้ production credentials และไม่ใช้ข้อมูลลูกค้า

| Test file | ผ่าน |
|---|---:|
| slip-storage-security.test.mjs | 20 |
| admin-cutover.test.mjs | 33 |
| api-budget.test.mjs | 3 |
| client-cutover.test.mjs | 11 |
| coach-addon-v2-integration.test.mjs | 59 |
| commerce-integration.test.mjs | 12 |
| guest-access.test.mjs | 18 |
| owner-review-flows.test.mjs | 9 |
| test-mode-propagation.test.mjs | 33 |
| **รวม** | **198** |

ชุดใหม่ตรวจ anonymous และ authenticated client get/list/create/overwrite/metadata-update/delete ใน 3 prefixes; owner/คนอื่น/guest capability/Pass LINE-only; actual guest booking → capability → upload → submit → read; expired/revoked/disabled identity; MIME/signature mismatch, HTML/SVG, truncated image, >2 MiB, >16M pixels; retry/conflict/concurrent upload/one-use receipt; expired receipt; cancelled/paid/expired booking; owner/branch/staff/expired admin; legacy record-bound read; legacy/new duplicate hashes; explicit Pass approval และออกเครดิตเพียงครั้งเดียว; rate limit/maintenance/feature-off/CSRF

ภาพเป็นข้อมูลจำลองสร้างใน test ด้วย sharp ไม่ใช้สลิปธนาคารจริง การตรวจ duplicate เป็น exact-byte SHA-256 ไม่ตรวจการแต่งรูปหรือยืนยันธุรกรรมกับธนาคาร รูปที่ re-encode เก็บ hash bytes ต้นฉบับฝั่ง trusted uploader เพื่อเทียบ registry เก่าได้

Regression transaction tests เดิมเปลี่ยน fixture จาก external public URL เป็น server receipt เท่านั้น มีชุดใหม่ทดสอบ upload/read กับ Storage emulator จริงแยกต่างหาก ไม่เปิด production access เพื่อให้ test ผ่าน

## วิธีรันซ้ำ

ใช้ patched Node ที่รองรับ sharp (>=20.9; แนะนำรุ่น patched ใน supported LTS) + Java 17 ตั้ง PATH ให้คำสั่งลูกใช้ Node ตัวเดียวกัน ติดตั้งจาก lock ด้วย `npm ci --ignore-scripts` optional native packages ต้องตรง OS/architecture:

```powershell
$env:GCLOUD_PROJECT = 'demo-slip-security'
npx --no-install firebase emulators:exec --project demo-slip-security --config firebase.slip-security.json --only "firestore,storage,auth" "npx --no-install vitest run tests/slip-storage-security.test.mjs tests/admin-cutover.test.mjs tests/api-budget.test.mjs tests/client-cutover.test.mjs tests/coach-addon-v2-integration.test.mjs tests/commerce-integration.test.mjs tests/guest-access.test.mjs tests/owner-review-flows.test.mjs tests/test-mode-propagation.test.mjs"
```

`firebase.slip-security.json` ใช้ Firestore deny rules สำหรับ emulator เท่านั้น production deploy ใช้ **อีกไฟล์** `firebase.slip-security.deploy.json` ซึ่งมีเฉพาะ Storage target ห้ามนำ emulator deny rules ไป deploy Firestore

ตรวจ syntax ของ `api/slip-verify.js`, `api/_lib/slip-storage.mjs` และ inline JavaScript ใน index/admin ผ่าน; `git diff --check` ผ่าน ผล JSON แบบเต็มเก็บใน workspace `ultra-tennis-recovery/support/slip-security-release-tests.json` ไม่ใส่ absolute workstation paths ใน public PR

## ความเสี่ยง/สิ่งที่ยังไม่ทดสอบ

- **Critical / ยังไม่แก้ใน production:** กฎที่เปิด anonymous read/list/create ยัง active จนเจ้าของอนุมัติ cutover ยังไม่พบหลักฐานว่าถูกนำไปใช้โจมตี
- **High / แยกอนุมัติ:** bearer URL เก่าต้อง inventory และ revoke แบบ batch แยก Rules ใหม่ไม่เพิกถอน token เก่า ไม่มีการ revoke หรือแก้ production record ในงานนี้
- **Unverified / release gates:** Vercel Linux packaging ของ native sharp, production service-account IAM/Auth permission, public bucket/object IAM, bucket alias อื่น, LINE/mobile/browser E2E และอัตราสำเร็จหลังเปิดจริง ยังไม่ทดสอบกับ production ดู [cutover](SLIP-CUTOVER.md)
- **High/Moderate advisories ที่มีอยู่เดิม:** `npm audit --omit=dev` ของ lock ที่รวม sharp รายงาน 11 entries (4 high, 7 moderate, 0 critical) ใน Firebase Admin/transitive tree; sharp ไม่อยู่ในผล advisory นี้ ไม่สรุปว่า exploit ได้จริง ไม่ทำ dependency migration กว้างใน P0 slip patch ต้องประสานงาน remediation เดิมและ rerun audit ก่อน release ไม่ใช้ผล emulator เป็นหลักฐานว่าปัญหาพวกนี้หมดไป
- การ proxy ใช้ CPU/Firestore/egress ในบริการเดิม มี rate limit แต่ไม่ใช่การรับประกันไม่มี billing abuse หรือค่าใช้งานเพิ่ม ไม่มี subscription หรือการจ่ายเงินใหม่
- Storage กับ Firestore ไม่ใช่ transaction เดียวกัน ความล้มเหลวหลัง save อาจมี orphan ที่ยัง private ต้อง cleanup หลังอนุมัติ/ตรวจ references ห้ามเปิด TTL ลบ consumed receipts
- Hash/precheck ไม่ใช่ bank verification; การชำระเงินจริงและการออกสิทธิ์ยังต้องให้แอดมินอนุมัติ; การ re-encode อาจทำ QR/ตัวอักษรเล็กอ่านยาก ต้องตรวจภาพตัวอย่างใน staging
- Session admin เดิมใช้ role/branch claims ใน cookie จนหมดอายุ ไม่เพิ่มระบบ revoke admin session ในงานนี้ Blob ที่เปิดแล้ว/รูปที่เคยดาวน์โหลด/ภาพหน้าจอเรียกคืนไม่ได้

Git branch ปิด automatic Vercel deployment ใน `vercel.json` ตั้งแต่ commit แรก ไม่ใช่การอนุมัติให้ deploy หรือ merge PR #4 ไม่เกี่ยวข้องกับ patch นี้
