# Retail Data Warehouse ETL Pipeline

ระบบ **Data Pipeline & Data Warehouse** แบบจำลองสำหรับธุรกิจค้าปลีก (Retail) พัฒนาด้วย **Python**, **Pandas** และ **SQLite3** ออกแบบสถาปัตยกรรมข้อมูลแบบ **Star Schema** พร้อมระบบตรวจสอบคุณภาพข้อมูล (Data Validation), การจัดการข้อมูลผิดพลาด (Data Quarantine) และการบันทึกประวัติการทำงาน (Pipeline Audit Logging)

---

## Architecture & Data Warehouse Schema

ฐานข้อมูลหลักใช้ออกแบบในลักษณะ **Star Schema** เพื่อรองรับการนำไปทำ Analytics / Dashboard ต่อไป ประกอบด้วยตารางต่าง ๆ ดังนี้:

```
                  ┌─────────────────┐
                  │  dim_customer   │
                  ├─────────────────┤
                  │ customer_key(PK)│
                  │ customer_id     │
                  │ customer_name   │
                  │ province        │
                  │ segment         │
                  └────────┬────────┘
                           │
                           │ 1:N
┌───────────────┐          ▼          ┌────────────────┐
│   dim_date    │    ┌───────────┐    │  dim_product   │
├───────────────┤    │fact_sales │    ├────────────────┤
│ date_key (PK) │◄───┼───────────┼───►│ product_key(PK)│
│ full_date     │1:N │ order_id  │N:1 │ product_id     │
│ day           │    │ ...       │    │ product_name   │
│ month/qtr/year│    └───────────┘    │ category       │
└───────────────┘                     └────────────────┘
```

### 1. Dimension Tables (ตารางมิติข้อมูล)
* **`dim_customer`**: เก็บข้อมูลลูกค้า (`customer_key`, `customer_id`, `customer_name`, `province`, `segment`)
* **`dim_product`**: เก็บข้อมูลสินค้า (`product_key`, `product_id`, `product_name`, `category`)
* **`dim_date`**: เก็บมิติด้านเวลาเพื่อวิเคราะห์ตามวัน/เดือน/ไตรมาส/ปี (`date_key`, `full_date`, `day`, `month`, `quarter`, `year`)

### 2. Fact Table (ตารางข้อเท็จจริง)
* **`fact_sales`**: เก็บธุรกรรมการสั่งซื้อที่ผ่านการตรวจสอบแล้ว (`order_id`, `date_key`, `customer_key`, `product_key`, `quantity`, `unit_price`, `discount_pct`, `gross_amount`, `net_amount`, `payment_method`, `sales_channel`, `updated_at`)

### 3. Monitoring & Quality Tables
* **`quarantine`**: เก็บรายการข้อมูลที่ไม่ผ่านการตรวจสอบ (Data Validation Errors) เพื่อรอการตรวจสอบและแก้ไข (`quarantine_id`, `order_id`, `customer_id`, `product_id`, `reason_code`, `source_batch`)
* **`pipeline_run_log`**: บันทึก Log การรัน Pipeline แต่ละ Batch (`run_id`, `batch_name`, `started_at`, `ended_at`, `rows_read`, `rows_valid`, `rows_rejected`, `rows_loaded`, `status`)

---

## ETL Pipeline Workflow

```
[ Excel File ] ──► ( Extract ) ──► ( Validate & Transform ) ┬──► [ Valid ] ────► [ fact_sales & dim_date ]
                                                           │
                                                           └──► [ Rejected ] ─► [ quarantine ]
```

### 1. **Extract**
* ดึงข้อมูลคำสั่งซื้อจากไฟล์ Excel (`Python_Data_Pipeline_Lab_Dataset.xlsx`) ทีละ Sheet/Batch (`orders_batch_1`, `orders_batch_2`, `orders_batch_3`)

### 2. **Transform & Validate**
ตรวจสอบความถูกต้องของข้อมูลตามกฎ (Business Rules):
* **Foreign Key Check**: ตรวจสอบว่า `customer_id` และ `product_id` มีอยู่ใน `dim_customer` และ `dim_product` หรือไม่ (`INVALID_CUSTOMER_FK`, `INVALID_PRODUCT_FK`)
* **Date Validation**: ตรวจสอบรูปแบบวันที่สั่งซื้อ (`INVALID_ORDER_DATE`)
* **Metrics Validation**: ตรวจสอบค่านวนเชิงตัวเลข (`INVALID_METRICS`) เช่น `quantity` > 0, `unit_price` > 0 และ `discount_pct` อยู่ระหว่าง 0 - 100%
* **Data Transformation**:
  * คำนวณ `gross_amount` = `quantity` × `unit_price`
  * คำนวณ `net_amount` = `gross_amount` × (1 - `discount_pct` / 100)
  * Standardize ข้อความ: `payment_method` เป็นตัวพิมพ์ใหญ่ (UPPERCASE) และ `sales_channel` เป็นตัวแรกพิมพ์ใหญ่ (Capitalize)

### 3. **Load**
* **Deduplication**: หากมี `order_id` ซ้ำใน Batch เดียวกัน จะเลือกข้อมูลอัปเดตล่าสุดอ้างอิงตาม `updated_at`
* Map ข้อมูลรหัสสั่งซื้อและวันที่เข้ากับ Surrogate Keys (`customer_key`, `product_key`, `date_key`)
* บันทึกรายการที่ผ่านเข้าตาราง `fact_sales` และรายการที่ผิดพลาดเข้าตาราง `quarantine`
* บันทึกสถานะการรันสคริปต์ลงใน `pipeline_run_log`
* ส่งออก (Export) ตาราง `quarantine` และ `pipeline_run_log` ออกมาเป็นไฟล์ `.csv` โดยอัตโนมัติ

---

## โครงสร้างโปรเจกต์ (Project Structure)

```text
.
├── Python_Data_Pipeline_Lab_Dataset.xlsx  # ไฟล์ข้อมูลต้นทาง (Source Excel)
├── pipeline.py                            # สคริปต์ ETL หลัก
├── retail_dw.db                           # SQLite Data Warehouse (สร้างอัตโนมัติ)
├── quarantine.csv                         # ไฟล์สรุปรายการข้อมูลที่ติด Quarantine (สร้างอัตโนมัติ)
├── pipeline_run_log.csv                   # ไฟล์บันทึกประวัติการรัน Pipeline (สร้างอัตโนมัติ)
└── README.md                              # เอกสารอธิบายโครงการ
```

---

## ข้อกำหนดและการติดตั้ง (Requirements & Setup)

### Prerequisites
* **Python 3.8+**
* ไลบรารี Python ที่จำเป็น:
  * `pandas`
  * `openpyxl` (ใช้สำหรับอ่านไฟล์ Excel)

### การติดตั้ง Dependencies
```bash
pip install pandas openpyxl
```

---

## การใช้งาน (How to Run)

วางไฟล์ข้อมูล `Python_Data_Pipeline_Lab_Dataset.xlsx` ไว้ในโฟลเดอร์เดียวกับสคริปต์ จากนั้นสั่งรันด้วยคำสั่ง:

```bash
python pipeline.py
```

### สิ่งที่จะเกิดขึ้นหลังการรัน:
1. ระบบจะสร้างฐานข้อมูล `retail_dw.db` (หากยังไม่มี)
2. ทำการ Seed ข้อมูลตั้งต้นเข้า `dim_customer` และ `dim_product` จาก Sheet `customers` และ `products`
3. ประมวลผล Batch 1, 2 และ 3 ตามลำดับ
4. แสดง Log สถานะการทำงานบน Terminal
5. บันทึกไฟล์ `quarantine.csv` และ `pipeline_run_log.csv` ออกมาในไดเรกทอรีปัจจุบัน

---

## ตัวอย่าง Output Logging

```text
2026-08-19 13:40:30 [INFO] Seeding dim_customer and dim_product...
2026-08-19 13:40:30 [INFO] Extracting batch: orders_batch_1
2026-08-19 13:40:30 [INFO] Batch orders_batch_1 complete: Loaded 280 new records.
2026-08-19 13:40:30 [INFO] Extracting batch: orders_batch_2
2026-08-19 13:40:30 [INFO] Batch orders_batch_2 complete: Loaded 255 new records.
2026-08-19 13:40:30 [INFO] Extracting batch: orders_batch_3
2026-08-19 13:40:30 [INFO] Batch orders_batch_3 complete: Loaded 261 new records.
2026-08-19 13:40:30 [INFO] Exported quarantine.csv and pipeline_run_log.csv successfully!
