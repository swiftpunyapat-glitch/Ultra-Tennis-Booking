# Payment slip hotfix — ชุดอนุมัติและคำสั่ง cutover

สถานะ: **Draft / ยังไม่ merge หรือ deploy** — 22 ตุลาคม 2026  
Branch: `codex/payment-slip-security-20261022` จาก main `1797b185aaadbd5c09903967915a229af9ef56b9`  
PR #4 แยกจากงานนี้และไม่ได้แก้ไข

## หลักฐานที่ตรวจจริง

เปิดหน้า **Rules** ของ bucket `ultra-tennis-booking.firebasestorage.app` ใน Firebase Console ที่เข้าสู่ระบบแล้ว ตรวจซ้ำวันที่ 22 ต.ค. พบ `allow read: if true` และ create ที่ตรวจเพียง MIME `image/.*` / ขนาดต่ำกว่า 5 MB ทั้ง `payment_slips` และ `pass_slips` กฎไม่มี ownership/auth check ส่วน prefix อื่น deny อยู่เดิม

เก็บข้อความที่แสดงใน editor ที่ [production-storage-rules-20261022.txt](production-storage-rules-20261022.txt) SHA-256 ของไฟล์หลักฐานเดิมก่อน Git จัดการ line endings: `5ecf23eb138d1742915ac9cede1cb289446b79940dfef1e4c0cab658473a1bc9` ภาพหน้าจอเก็บใน workspace ของเจ้าของ ไม่แนบภาพที่มีบัญชีผู้ใช้เข้า public PR ไม่ได้เปิด/ดาวน์โหลด/ทดลองสร้างไฟล์ลูกค้า ไม่ได้รัน Rules Playground หรือกด Publish

Firestore Rules รุ่น active ที่ console แสดง **Aug 5, 2026 • 6:42 PM** ยังมี catch-all `allow read, write: if false` ตาม [excerpt](production-firestore-catchall-20261022.txt) ใบรับใหม่ `private_slip_uploads` ต้องคงเป็น server-only ห้ามเพิ่ม client exception หรือเปิด TTL ที่จะลบใบรับ consumed

**ช่องโหว่ production ยังอยู่จนกว่าจะอนุมัติและทำ cutover** ไม่มีหลักฐานยืนยันว่ามีผู้โจมตีดาวน์โหลดไฟล์ การเปลี่ยน Rules อย่างเดียวไม่ใช่การเพิกถอน bearer download URL เก่า

## สิ่งที่โค้ดเปลี่ยน

- ใช้ route เดิม `/api/slip-verify` เพิ่ม `upload_slip` และ `read_slip` ไม่เพิ่ม direct Vercel function (ยัง 11)
- Booking: Firebase ID token ของ LINE UID ต้องตรงกับเจ้าของ หรือ guest capability ที่ตรวจ hash/scope/expiry/revocation ผูก booking นั้น Pass ใช้ LINE เท่านั้น รหัสจอง/รหัสซื้ออย่างเดียวทำอะไรไม่ได้
- รับเฉพาะ JPEG/PNG/WebP ไม่เกิน **2 MiB**, ตรวจ signature และชนิดจาก decoder, จำกัด 16 ล้านพิกเซล, ปฏิเสธภาพหลายเฟรม, decode/re-encode ด้วย pinned `sharp@0.35.5` ตัด metadata/ข้อมูลต่อท้าย ผลลัพธ์ JPEG ไม่เกิน 2 MiB ไฟล์ต้นฉบับไม่ถูกเก็บ
- เซิร์ฟเวอร์กำหนด bucket/path เอง: `private_slips/{booking|pass_purchase}/{documentId}/{receiptId}.jpg` ไม่ใช้ชื่อไฟล์/รหัสจองเป็นสิทธิ์ ใช้ generation-create precondition `ifGenerationMatch: 0` ไม่ออก signed URL/download token
- ใบรับอัปโหลดอายุ 10 นาที ผูก actor/ชนิด/record ใช้ได้ครั้งเดียวใน transaction เดียวกับ pending_review และ idempotency record คีย์เดิมกับข้อมูลเดิม retry ได้ ข้อมูลต่างกัน conflict การชำระ/ยกเลิกระหว่าง upload และ submit ทำให้ submit ไม่ผ่าน
- Rate limit แบบ durable/fail-closed: upload สูงสุด 6 ครั้ง/record/ชั่วโมง, 12 ครั้ง/actor/ชั่วโมง, IP upload 30 ครั้ง/15 นาที; read/verify 120 ครั้ง/IP/15 นาที แชร์ global invalid-guest gate เดิม ไม่เก็บ raw token ใน limiter
- Admin อ่านได้เฉพาะ owner/ultra_admin/branch_manager ที่มีสิทธิ์สาขา Staff/viewer อ่านไม่ได้ Customer อ่านได้เฉพาะของตนทุก request ใช้ POST JSON, same-origin, no-store/nosniff ไม่มี credential ใน URL รูปเปิดใน dialog ผ่าน Blob และ revoke Blob URL เมื่อปิด
- Admin APIs ส่งเพียง `hasSlip` และตัด URL ใน verification projection หน้าเก่าที่ cache อยู่ต้อง refresh การ submit external URL เก่าปฏิเสธทั้งหมด
- รูปเก่าอ่านผ่าน Admin SDK จาก reference ที่บันทึกใน owned record เท่านั้น ตรวจ bucket และ prefix/รหัสให้ตรง record ไม่ตาม URL ที่ caller ส่งมา รูปเก่า SVG/GIF/ภาพเกินขีดจำกัดต้องใช้ manual review
- Hash สลิปซ้ำใช้ SHA-256 ของ bytes ต้นฉบับที่เซิร์ฟเวอร์คำนวณก่อน re-encode จึงเทียบ registry เก่าได้ อีก hash ตรวจ integrity ของ JPEG ที่เก็บ การ precheck ไม่ยืนยันเงิน ไม่ออก Pass และไม่ mark paid อัตโนมัติ

## Gate ก่อนอนุมัติ production

1. Reviewer อ่าน diff + ผลทดสอบ และยืนยัน SHA ของ release artifact ไม่ใช้ branch ที่อัปโหลดไฟล์ไม่ครบ
2. ยืนยัน service account เดิมอ่าน/สร้าง object และ metadata ใน bucket ที่ระบุได้ และอ่าน Firebase Auth user เพื่อเช็ก revoked/disabled token ได้ โดยไม่เพิ่ม IAM กว้างเกินจำเป็น ยังไม่ได้ทดสอบ IAM ใน production
3. ทดสอบ Linux native `sharp` ใน build artifact และ preview/staging ที่แยกจาก production ด้วย synthetic data หลังเจ้าของอนุมัติ ห้าม preview ใช้ production Firebase credentials/LINE notification การทดสอบปัจจุบันเป็น local Windows emulator
4. ทดสอบหน้าลูกค้าใน LINE/mobile, guest resume, การเปิด dialog แอดมิน/ลูกค้า และความคมชัด/QR ของภาพตัวอย่าง ยังไม่มี browser E2E หรือ live upload test
5. คง `useServerSlipSubmit=true`; หากไม่พร้อม endpoint จะตอบ maintenance ก่อนรับไฟล์ `SLIP_UPLOADS_DISABLED=true` เป็น emergency switch สำหรับปิด upload ใหม่ (ต้อง deploy env หลังอนุมัติ) read ที่ตรวจสิทธิ์ยังใช้ได้
6. ตรวจ Rules ไม่เปลี่ยนจากหลักฐาน, ยืนยัน catch-all Firestore ปิด receipt/limiter และไม่มี bucket/object IAM public grant ที่ข้าม Rules การตรวจ IAM/public grant และ bucket alias อื่นยังเป็น release gate ที่ไม่ยืนยัน

## ลำดับ cutover ที่เสนอ

แนะนำช่วงดูแลได้ 15–30 นาที โดยเจ้าของอนุมัติ window และ manual workflow ก่อน ไม่รับประกัน zero downtime

1. เตรียม build backend/frontend ชุดเดียวกันที่ผ่าน staging และ deploy rollback artifact ที่ยังใช้ private path ได้ เก็บ rule version เดิมเป็นหลักฐานเท่านั้น ไม่เป็น rollback default
2. **Containment ก่อน:** deploy Storage deny rules ตามคำสั่งด้านล่าง ช่วงนี้ client เก่าจะ upload/read ทาง Storage ไม่ได้ ให้เจ้าหน้าที่รับหลักฐานผ่านช่องทางเดิมชั่วคราว การเก็บความลับสำคัญกว่าการย้อนเปิด anonymous
3. Deploy/promotion code ที่รีวิวแล้วทั้ง frontend/backend อัปเดตพร้อมกัน ตรวจ deployment commit จริง ให้ลูกค้า/admin refresh หน้า ไม่มี service worker migration ใน patch นี้
4. หลังอนุมัติ smoke test ให้ใช้ synthetic booking/purchase ที่แยกและมีแผนล้างรายการ: LINE/guest/Pass upload, pending_review, admin view/approve, cross-customer denial, no token metadata, no anonymous get/list/create **ห้ามใช้ไฟล์ลูกค้าจริงเป็น test**
5. ตรวจ 401/403/409/413/415/429/503 และอัตราส่งสลิปสำเร็จแบบ aggregate หลัง 15 นาทีและ 1 ชั่วโมง ห้ามบันทึก body/base64/ID token/guest capability/รูป/URL tokens ลง logs
6. ประกาศจบ maintenance เมื่อ smoke gate ผ่าน ถ้าไม่ผ่าน ให้คง deny และ manual workflow แล้ว forward-fix เฉพาะจุด
7. ค่อยทำ migration/revocation URL เก่าตามขอบเขตอนุมัติแยก ไม่ถือว่า step 2 ปิด bearer URL ทั้งหมดแล้ว

การ deploy โค้ดก่อน Rules จะทำให้รูปใหม่อยู่ใน private prefix ได้ แต่รูปเก่ายัง public ระหว่างรอ จึงไม่ใช่ containment ที่เสร็จสมบูรณ์ ไม่เสนอให้ปล่อยหน้าต่างนี้โดยไม่กำหนดเวลา

## คำสั่ง deploy Storage Rules แบบเจาะ bucket

**คำสั่งนี้ยังไม่ได้รัน ใช้หลังอนุมัติเท่านั้น** เปิด PowerShell ใน checkout ของ release ที่รีวิวแล้ว ใช้ Node รุ่น patched และ Firebase CLI จาก lockfile ห้ามใช้ emulator environment กับ production CLI:

```powershell
# ให้ admin ที่มีสิทธิ์ลงชื่อเข้า Firebase CLI ผ่าน browser ตามปกติ
# ไม่ส่งรหัสผ่าน/token/service-account JSON ในแชต
npx --no-install firebase login
npx --no-install firebase projects:list

# หยุดถ้าเห็น project/bucket ต่างจากนี้ หรือ process ยังมี emulator env
Get-ChildItem Env:FIRESTORE_EMULATOR_HOST,Env:FIREBASE_STORAGE_EMULATOR_HOST,Env:FIREBASE_AUTH_EMULATOR_HOST -ErrorAction SilentlyContinue

# สร้าง target mapping ใน .firebaserc ของ checkout นี้เท่านั้น
npx --no-install firebase target:apply storage payment-slips ultra-tennis-booking.firebasestorage.app --project ultra-tennis-booking
Get-Content .firebaserc
Get-FileHash storage.rules -Algorithm SHA256

# Reviewer ต้องตรวจ mapping และ rule diff ก่อนคำสั่งที่เปลี่ยน production นี้
npx --no-install firebase deploy --only "storage:payment-slips" --project ultra-tennis-booking --config firebase.slip-security.deploy.json
```

ถ้าพบ emulator env ให้เปิด terminal สะอาดใหม่ก่อนดำเนินการ ห้ามสลับ project เป็น production เพื่อรันทดสอบ ห้ามใช้ `firebase deploy` เปล่า ๆ และห้าม deploy Firestore emulator deny rules สังเกต version/time ที่ Firebase Storage → Rules หลัง deploy และเก็บ output ที่ไม่มี credentials ไม่ต้องเปิด customer Files

## Migration / revoke URL เก่า — เตรียมแผน ยังไม่ดำเนินการ

1. หลังได้รับอนุมัติขอบเขต ให้ trusted server inventory **metadata + record reference** ของทั้งสอง legacy prefixes/bucket ที่มีจริง เก็บ object path, generation/metageneration, reference record IDs, token-present boolean, จำนวน orphan ไม่ดึงรูป ไม่พิมพ์ token values ไม่ส่งข้อมูลไปภายนอก
2. สร้าง restricted manifest แยก missing file, invalid path/code, reference ซ้ำ และ URL ใน `slipUrl`, `paymentVerification.slipUrlChecked`, legacy aliases/ข้อความที่เคยส่ง ไม่มี manifest ของลูกค้าจริงอยู่ใน PR นี้
3. ทดลองกับ synthetic legacy object ใน isolated bucket: authorized proxy อ่านได้ → ลบ custom metadata `firebaseStorageDownloadTokens` โดยใช้ metageneration precondition → URL เก่าของ synthetic object ใช้ไม่ได้ → proxy ยังอ่านได้ ห้ามตั้ง token ใหม่/ใช้ signed URL แทนแบบถาวร ตรวจ IAM public access ด้วย
4. เริ่ม canary ขนาดเล็กที่อนุมัติ: ให้ backend อ้าง private object/legacy location ที่ไม่มี token แล้ว scrub token-bearing fields ใน record/verification อย่างมี precondition จาก manifest พร้อม audit ไม่มีการแก้ราคา/เครดิต/สถานะเงิน
5. เพิกถอน tokens ของ object ที่ canary ผ่านแล้วด้วย metadata update และ precondition เท่านั้น ค่า token ไม่เข้า logs จากนั้นขยายเป็น batch ตาม manifest เก็บ counts/errors; orphan ที่ไม่มี reference ต้องอยู่ในขอบเขตการเพิกถอนด้วย อย่าลบเอกสารหลักฐานเพื่อแก้ปัญหา URL
6. ตรวจทั้ง references ใน Firestore และลิงก์ที่เคยแชร์ URL เก่าอาจอยู่ใน chat/history/browser cache ไม่มีวิธีเรียกคืนสำเนารูปที่ถูกดาวน์โหลดไปแล้ว อย่าอ้างว่ากฎใหม่ลบสำเนาเหล่านั้น
7. การเพิกถอนควร forward-fix ผ่าน authorized proxy ไม่คืน token เดิม การเข้าถึงข้อมูลในอดีต/retention เป็นการตัดสินใจเจ้าของแยกจาก patch

ไฟล์ใหม่ไม่มี bearer URL การเปลี่ยน Rules **ไม่รับประกัน** ว่า Firebase download URL เดิมหมดสิทธิ์ ต้องผ่านการทดลอง revocation ของ synthetic token และอนุมัติ production batch ก่อน

## ผลกระทบและ manual workflow

ลูกค้า: เลือก JPG/PNG/WebP ≤2 MiB (เดิม <5 MB) ภาพถูกสร้างใหม่เป็น JPEG; ต้องลดขนาดภาพหรือส่งผ่านช่องทางแอดมินเดิมหากไฟล์ใหญ่/อ่านไม่ได้ ไม่อัปโหลดซ้ำหลายรอบเมื่อขึ้นรอ review กรณี guest token หาย/หมดอายุให้แอดมินยืนยันตัวตนตามขั้นตอนเดิม รหัสจองอย่างเดียวใช้กู้สิทธิ์ไม่ได้

Admin: กด **เปิดสลิป (ตรวจสิทธิ์)** แทน thumbnail/public link ต้องมี session และสิทธิ์สาขา ข้อมูลราคา/ยอดเงิน/สิทธิ์ Pass ไม่เปลี่ยนจากการ upload/precheck การยืนยันเงินจริงยังใช้การตรวจและ approve เดิม

ข้อความพร้อมใช้ระหว่าง maintenance (ยังไม่ส่ง/เผยแพร่):

> ระบบแนบสลิปกำลังปรับปรุงความปลอดภัยชั่วคราว หากโอนแล้ว กรุณาส่งหลักฐานในแชตส่วนตัวช่องทางเดิมของ Ultra Tennis พร้อมรหัสรายการ ไม่ต้องโอนซ้ำค่ะ/ครับ ทีมงานจะตรวจยอดและแจ้งยืนยันให้ หากใกล้หมดเวลาชำระ กรุณาแจ้งทีมงานก่อนเพื่อเช็กคิวให้ค่ะ/ครับ

ใช้ทีมเดิม รับใน official/private channel เดิมเท่านั้น ตรวจตัวตน/รายการ/ยอดเข้าธนาคารและเวลาอย่างน้อยสองแหล่งก่อน approve ผู้มีสิทธิ์เดิมเป็นคนยืนยัน เก็บ checklist ภายในว่าได้รับหลักฐาน/ตรวจธนาคารแล้ว/ใครอนุมัติ ไม่สร้าง public URL ไม่เปลี่ยนเครดิตเอง และไม่ถือว่าการแจ้งว่าโอนแล้วต่ออายุ hold/รับรองคิวอัตโนมัติ ต้องตรวจความว่างก่อนยืนยัน หากคิวหมดเวลาแล้วอย่ารับประกันว่าจะได้คิวเดิม

ไม่มี subscription/บริการใหม่ ไม่มีการใช้เงินหรือส่งข้อความจริง การ proxy เพิ่ม CPU/Firestore/egress ในบริการเดิมจึงต้องติดตาม usage; ไม่รับประกันต้นทุนดำเนินการเป็นศูนย์หลังเปิดจริง

## Rollback / forward-fix

- ปัญหา upload: คง deny rules, เปิด manual workflow, ปิด upload ใหม่ด้วย emergency switch หลังอนุมัติ deploy แล้วแก้แบบ forward หรือกลับไป **private-capable** artifact ที่ทดสอบแล้ว
- ห้าม rollback ไป frontend direct `uploadBytes/getDownloadURL` หรือคืน anonymous rules เพื่อให้หน้าเก่าทำงาน การ rollback main เก่าโดยคง deny จะทำให้ upload ใช้ไม่ได้ ต้องแจ้ง maintenance ต่อ
- ปัญหา view เก่า/URL ถูก revoke: แก้ reference/permission ให้ proxy อ่านได้ ไม่สร้าง bearer token ใหม่ ไม่ลบภาพหรือแก้เงินเพื่อ workaround
- Storage สำเร็จแต่ Firestore fail อาจมี orphan แบบ private ตรวจ receipts `uploading/failed/ready` ที่หมดอายุ >24 ชม. กับ record references แล้วเตรียม cleanup แบบมี precondition หลังอนุมัติ อย่าเปิด Firestore TTL ที่ลบ consumed receipts เพราะใช้เปิดรูปย้อนหลัง

## เจ้าของอนุมัติเพียงชุดนี้

- [ ] ยอมรับขนาด 2 MiB/ชนิดภาพ/การสร้าง JPEG ใหม่ และทีมเดิมรับหลักฐานชั่วคราวช่วง cutover
- [ ] อนุมัติ isolated Linux build + LINE/mobile/admin staging ด้วย synthetic data ก่อน production
- [ ] หลัง gate ผ่าน อนุมัติ commit ที่ระบุ + window สำหรับ deploy **Storage deny ก่อน แล้ว code** และ synthetic production smoke test ที่กำหนดขอบเขต/ผู้ดูแลชัดเจน
- [ ] ระบุผู้รับผิดชอบเปิด/ปิด maintenance และ forward-fix หาก upload หยุด (ห้ามคืน public rules)
- [ ] อนุมัติ inventory/canary/batch revoke URLs เก่าเป็นขอบเขตแยก พร้อม retention/ผู้ดูแล manifest — ยังไม่อนุมัติถือว่ายังไม่ทำ

## แหล่งอ้างอิงสำหรับผู้ deploy

[Storage rule operations](https://firebase.google.com/docs/storage/security/core-syntax), [Admin SDK Storage / IAM](https://firebase.google.com/docs/storage/admin/start), [Firebase deploy targets](https://firebase.google.com/docs/cli/targets), [GCS generation preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions), [Vercel 4.5 MB payload limit](https://vercel.com/docs/functions/limitations), [sharp decode limits](https://sharp.pixelplumbing.com/api-constructor/), [Vercel branch deployment configuration](https://vercel.com/docs/project-configuration/git-configuration)
