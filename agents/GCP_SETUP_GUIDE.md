# 🚀 Hướng dẫn Setup Google Cloud Platform (GCP) cho HAIAN DWH ETL Agent

> Làm theo từng bước. Mỗi bước đều có ảnh chụp màn hình mô tả vị trí cần click.

---

## Bước 1 — Tạo tài khoản Google Cloud

1. Vào **https://console.cloud.google.com**
2. Đăng nhập bằng tài khoản Google của bạn
3. Nếu lần đầu, Google sẽ hỏi kích hoạt trial $300 credit (dùng thử miễn phí)
   - Click **"Bắt đầu dùng thử miễn phí"**
   - Điền thông tin thanh toán (không bị tính phí trong trial)

---

## Bước 2 — Tạo GCP Project mới

1. Ở góc trên trái, click dropdown tên project → **"New Project"**
2. Điền thông tin:
   - **Project name**: `haian-dwh-project` (hoặc tên bạn thích)
   - **Project ID**: ghi lại ID này, ví dụ `haian-dwh-project-123456`
   - **Location**: No organization (hoặc chọn org nếu có)
3. Click **"Create"**
4. Chờ 30-60 giây, sau đó chọn project vừa tạo

> ⚠️ **Ghi lại Project ID!** Bạn sẽ cần điền vào file `.env`:
> ```
> GCP_PROJECT_ID=haian-dwh-project-123456
> ```

---

## Bước 3 — Kích hoạt BigQuery API

1. Trong Console, vào menu **"APIs & Services"** → **"Library"**
2. Tìm kiếm **"BigQuery API"**
3. Click **"Enable"** (Kích hoạt)
4. Chờ 1-2 phút

---

## Bước 4 — Tạo Service Account

Service Account là tài khoản robot để agent Python kết nối với BigQuery.

1. Vào **"IAM & Admin"** → **"Service Accounts"**
2. Click **"+ Create Service Account"**
3. Điền thông tin:
   - **Name**: `haian-etl-agent`
   - **Description**: `ETL Agent for HAIAN DWH`
4. Click **"Create and Continue"**
5. Phần **"Grant this service account access"**:
   - Role 1: **BigQuery Data Editor** ← quan trọng, cho phép đọc/ghi data
   - Role 2: **BigQuery Job User** ← cho phép chạy query
6. Click **"Continue"** → **"Done"**

---

## Bước 5 — Tải JSON Key về máy

1. Vào lại danh sách Service Accounts
2. Click vào service account `haian-etl-agent` vừa tạo
3. Tab **"Keys"** → **"Add Key"** → **"Create new key"**
4. Chọn **JSON** → Click **"Create"**
5. File `credentials.json` sẽ tự download về máy

**Đặt file vào thư mục dự án:**
```
D:\Dự Án\haian_dwh_project\agents\credentials.json
```

> 🔐 **QUAN TRỌNG**: Không commit file này lên Git!
> Thêm vào `.gitignore`:
> ```
> agents/credentials.json
> agents/.env
> agents/logs/
> ```

---

## Bước 6 — Tạo BigQuery Dataset

1. Trong Console, vào **"BigQuery"**
2. Click dấu **"+"** bên cạnh tên project của bạn
3. **"Create dataset"**:
   - **Dataset ID**: `haian_dwh`
   - **Location type**: Multi-region → **US** (hoặc chọn region gần Việt Nam: `asia-southeast1`)
4. Click **"Create dataset"**

> (Agent cũng có thể tự tạo dataset — xem bước 8)

---

## Bước 7 — Cấu hình file `.env`

```bash
# Vào thư mục agents/
cd "D:\Dự Án\haian_dwh_project\agents"

# Copy template
copy .env.example .env
```

Mở file `.env` và điền thông tin:

```env
GCP_PROJECT_ID=haian-dwh-project-123456    # ← Project ID của bạn (từ Bước 2)
BQ_DATASET_ID=haian_dwh                    # ← Tên dataset (từ Bước 6)
GOOGLE_CREDENTIALS_PATH=D:/Du An/haian_dwh_project/agents/credentials.json

FULL_SYNC_TIME=02:00
CSV_DATA_DIR=D:/Du An/haian_dwh_project/Data/Raw

LOG_LEVEL=INFO
LOG_FILE=D:/Du An/haian_dwh_project/agents/logs/etl_agent.log
```

---

## Bước 8 — Cài thư viện Python

```bash
# Mở Terminal/Command Prompt, vào thư mục dự án
cd "D:\Dự Án\haian_dwh_project"

# Cài thư viện
pip install -r agents/requirements.txt
```

---

## Bước 9 — Kiểm tra kết nối

```bash
cd "D:\Dự Án\haian_dwh_project\agents"

# Test kết nối BigQuery
python etl_agent.py --test-connection
```

**Output kỳ vọng:**
```
✅ Kết nối BigQuery thành công — project: haian-dwh-project-123456
✅ Kết nối BigQuery OK!
```

---

## Bước 10 — Chạy Dry Run (không upload thật)

```bash
python etl_agent.py --dry-run
```

Xem log để đảm bảo tất cả file CSV được đọc đúng và không có lỗi schema.

---

## Bước 11 — Chạy Full Sync lần đầu

```bash
python etl_agent.py --run-now
```

Sau khi chạy xong, vào **BigQuery Console** để xem bảng đã tạo chưa.

---

## Bước 12 — Chạy Daemon (chế độ nền, scheduler + file watcher)

```bash
python etl_agent.py --daemon
```

Agent sẽ:
- ✅ Chạy ngay một lần full sync lúc khởi động
- ⏰ Tự động full sync lúc 02:00 AM hằng đêm
- 👁️ Theo dõi thư mục CSV — nếu có file thay đổi, sync ngay trong 30 giây

---

## Các lệnh CLI hữu ích

```bash
# Kiểm tra kết nối
python etl_agent.py --test-connection

# Full sync khô (không upload)
python etl_agent.py --dry-run

# Full sync thật
python etl_agent.py --run-now

# Sync một file cụ thể
python etl_agent.py --file fact_booking.csv

# Chạy daemon (khuyến nghị cho production)
python etl_agent.py --daemon
```

---

## Xử lý lỗi thường gặp

| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `Could not automatically determine credentials` | Chưa có credentials | Kiểm tra `GOOGLE_CREDENTIALS_PATH` trong `.env` |
| `Project not found` | Project ID sai | Kiểm tra `GCP_PROJECT_ID` trong `.env` |
| `BigQuery API has not been used` | Chưa enable API | Làm Bước 3 |
| `Access Denied` | Service account thiếu quyền | Kiểm tra roles ở Bước 4 |
| `CSV_DATA_DIR không tồn tại` | Đường dẫn sai | Kiểm tra `CSV_DATA_DIR` trong `.env` |

---

## Chi phí BigQuery (ước tính cho dự án này)

| Hoạt động | Chi phí |
|-----------|---------|
| Storage (~50MB data) | ~$0.001/tháng (gần như miễn phí) |
| Query (1TB miễn phí/tháng) | $0 trong trial |
| Load jobs (upload CSV) | **Miễn phí** |

> 📌 Dự án này đủ nhỏ để dùng trong **free tier** của Google Cloud.
