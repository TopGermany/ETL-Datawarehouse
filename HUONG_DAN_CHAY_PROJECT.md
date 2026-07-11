# Hướng dẫn chạy project HAIAN ETL Data Warehouse

Tài liệu này hướng dẫn cài đặt và chạy project theo hai cách:

- **Docker + Airflow (khuyến nghị):** chạy đầy đủ pipeline theo lịch.
- **Python trực tiếp:** phù hợp để phát triển, kiểm tra từng thành phần hoặc chạy dry-run.

## 1. Tổng quan luồng chạy

Pipeline hiện tại chạy theo thứ tự tổng quát:

```text
Airflow DAG
  -> HAIAN DWH Agent (Google ADK/Gemini)
  -> Scrape giá phòng đối thủ
  -> Kiểm tra chất lượng dữ liệu
  -> Đồng bộ CSV lên BigQuery
  -> Tạo các Data Mart phục vụ dashboard
```

DAG chính là `haian_competitor_price_pipeline`, được cấu hình chạy lúc **02:00 hằng ngày**.

## 2. Yêu cầu trước khi chạy

### Cách 1: Docker + Airflow

- Docker Desktop (Windows/macOS) hoặc Docker Engine + Docker Compose (Linux).
- Tài khoản Google Cloud đã bật BigQuery API.
- BigQuery dataset, mặc định là `haian_dwh`.
- Service Account có tối thiểu các quyền:
  - BigQuery Data Editor
  - BigQuery Job User
- Gemini API key để chạy Google ADK agent.

Hướng dẫn tạo project, dataset và Service Account chi tiết nằm tại `agents/GCP_SETUP_GUIDE.md`.

### Cách 2: Chạy Python trực tiếp

- Python **3.10** được khuyến nghị vì Docker image hiện dùng Python 3.10.
- `pip` và môi trường ảo Python.
- Các cấu hình Google Cloud tương tự cách Docker nếu muốn đồng bộ thật lên BigQuery.

## 3. Chuẩn bị biến môi trường

Project Docker đọc cấu hình từ file `agents/.env`.

Từ thư mục gốc của project, tạo file cấu hình:

### macOS/Linux

```bash
cp agents/.env.example agents/.env
```

### Windows PowerShell

```powershell
Copy-Item agents/.env.example agents/.env
```

Mở `agents/.env` và cập nhật các giá trị:

```env
# Google Cloud / BigQuery
GCP_PROJECT_ID=your-gcp-project-id
BQ_DATASET_ID=haian_dwh
GOOGLE_CREDENTIALS_PATH=/absolute/path/to/service-account.json

# Gemini / Google ADK
GOOGLE_API_KEY=your-gemini-api-key
AGENT_MODEL=gemini-2.0-flash
AGENT_MAX_ACTIONS=8
AGENT_DEFAULT_DRY_RUN=true

# Dữ liệu và log khi chạy Python trực tiếp
CSV_DATA_DIR=/absolute/path/to/ETL-Datawarehouse/Data/Raw
LOG_LEVEL=INFO
LOG_FILE=/absolute/path/to/ETL-Datawarehouse/agents/logs/etl_agent.log

# Lịch chạy script ETL truyền thống
FULL_SYNC_TIME=02:00

# Email cảnh báo (có thể để trống nếu chưa dùng)
SMTP_USER=
SMTP_PASSWORD=
ALERT_EMAIL=
```

> Không commit `agents/.env`, API key hoặc file Service Account JSON lên Git.

## 4. Chạy đầy đủ bằng Docker + Airflow

### 4.1. Chuẩn bị Service Account cho container

Trong `docker-compose.yml`, đường dẫn credentials trong container hiện được cố định là:

```text
/opt/airflow/agents/haian-dwh-project-3ded89ea6dc8.json
```

Do thư mục `agents/` được mount vào container, hãy đặt file Service Account tại:

```text
agents/haian-dwh-project-3ded89ea6dc8.json
```

Nếu file của bạn có tên khác, đổi tên file hoặc cập nhật biến `GOOGLE_CREDENTIALS_PATH` trong `docker-compose.yml` cho khớp.

### 4.2. Kiểm tra cấu hình Docker Compose

```bash
docker compose config --quiet
```

Lệnh không in lỗi nghĩa là cấu hình hợp lệ. Cảnh báo thuộc tính `version` đã cũ không ngăn project chạy.

### 4.3. Build và khởi động hệ thống

```bash
docker compose up -d --build
```

Lần build đầu có thể mất vài phút vì image phải cài Airflow, thư viện Python và Chromium cho Playwright.

Kiểm tra container:

```bash
docker compose ps
```

Theo dõi log khởi động:

```bash
docker compose logs -f airflow-standalone
```

Nhấn `Ctrl + C` để thoát chế độ xem log; container vẫn tiếp tục chạy nền.

### 4.4. Mở Airflow

Truy cập:

```text
http://localhost:8080
```

Thông tin đăng nhập mặc định:

```text
Username: admin
Password: admin
```

Trong Airflow:

1. Tìm DAG `haian_competitor_price_pipeline`.
2. Bật DAG sang trạng thái **ON** để chạy theo lịch.
3. Muốn chạy ngay, chọn DAG rồi bấm **Trigger DAG**.
4. Mở task `run_haian_dwh_agent` để xem log chi tiết.

> Chạy DAG ở chế độ hiện tại dùng cờ `--live`: scraper có thể ghi CSV, ETL có thể ghi BigQuery và bước Data Mart có thể thay đổi object trên BigQuery.

### 4.5. Dừng hệ thống

```bash
docker compose down
```

Nếu dùng Windows và muốn giải phóng thêm RAM của WSL sau khi tắt Docker Desktop:

```powershell
wsl --shutdown
```

## 5. Chạy trực tiếp bằng Python

Thực hiện các lệnh sau tại **thư mục gốc project**.

### 5.1. Tạo môi trường ảo

### macOS/Linux

```bash
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r agents/requirements.txt
playwright install chromium
```

### Windows PowerShell

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r agents/requirements.txt
playwright install chromium
```

### 5.2. Chạy test tự động

```bash
pytest -q
```

### 5.3. Chạy HAIAN DWH Agent

Dry-run an toàn, không ghi dữ liệu thật:

```bash
python -m agents.haian_dwh_agent.cli \
  --goal daily_competitor_pipeline \
  --dry-run
```

Chạy live:

```bash
python -m agents.haian_dwh_agent.cli \
  --goal daily_competitor_pipeline \
  --live
```

> Chỉ dùng `--live` sau khi đã kiểm tra đúng GCP project, dataset, credentials và dữ liệu CSV.

### 5.4. Chạy riêng scraper

Test nhanh một ngày:

```bash
python agents/competitor_scraper_agent.py --test
```

Hiện cửa sổ Chromium để debug:

```bash
python agents/competitor_scraper_agent.py --test --no-headless
```

Chạy scrape đầy đủ:

```bash
python agents/competitor_scraper_agent.py --run-now
```

Chạy nhưng không ghi CSV:

```bash
python agents/competitor_scraper_agent.py --dry-run
```

### 5.5. Chạy riêng ETL

Kiểm tra kết nối BigQuery:

```bash
python agents/etl_agent.py --test-connection
```

Validate toàn bộ dữ liệu nhưng không upload:

```bash
python agents/etl_agent.py --dry-run
```

Đồng bộ toàn bộ dữ liệu lên BigQuery:

```bash
python agents/etl_agent.py --run-now
```

Đồng bộ một file cụ thể:

```bash
python agents/etl_agent.py --file fact_booking.csv
```

### 5.6. Mở dashboard theo dõi flow cục bộ

```bash
python tools/flow_status_server.py
```

Sau đó truy cập:

```text
http://127.0.0.1:8090/docs/flow-dashboard.html
```

## 6. Kiểm tra sau khi chạy

- Airflow UI mở được tại `http://localhost:8080`.
- DAG `haian_competitor_price_pipeline` không có task thất bại.
- Log task có final summary từ HAIAN DWH Agent.
- CSV scrape được ghi trong `Data/Raw/` khi chạy live.
- BigQuery dataset có các bảng Silver sau khi ETL live thành công.
- Các view Data Mart xuất hiện sau bước tạo mart.

## 7. Lỗi thường gặp

### Docker báo không tìm thấy `agents/.env`

Tạo file từ template:

```bash
cp agents/.env.example agents/.env
```

### `Could not automatically determine credentials`

- Kiểm tra file Service Account có tồn tại.
- Khi chạy Docker, kiểm tra đúng tên file được mô tả ở mục 4.1.
- Khi chạy Python trực tiếp, kiểm tra `GOOGLE_CREDENTIALS_PATH` là đường dẫn tuyệt đối hợp lệ.

### `Project not found` hoặc `Access Denied`

- Kiểm tra `GCP_PROJECT_ID`.
- Bật BigQuery API.
- Kiểm tra Service Account có quyền BigQuery Data Editor và BigQuery Job User.

### Agent báo thiếu API key hoặc không gọi được model

- Kiểm tra `GOOGLE_API_KEY` trong `agents/.env`.
- Kiểm tra API key đã được phép dùng Gemini API.
- Kiểm tra `AGENT_MODEL` là model mà tài khoản hiện có quyền truy cập.

### `CSV_DATA_DIR không tồn tại`

- Dùng đường dẫn tuyệt đối khi chạy Python trực tiếp.
- Khi chạy Docker, Compose tự đặt đường dẫn thành `/opt/airflow/Data/Raw`.

### Port 8080 đã được sử dụng

Tìm process/container đang dùng port 8080 hoặc đổi mapping trong `docker-compose.yml`, ví dụ:

```yaml
ports:
  - "8081:8080"
```

Sau đó truy cập `http://localhost:8081`.

### Xem lại lỗi container

```bash
docker compose ps
docker compose logs --tail=200 airflow-standalone
```

## 8. Lệnh nhanh

```bash
# Khởi động đầy đủ
docker compose up -d --build

# Xem trạng thái và log
docker compose ps
docker compose logs -f airflow-standalone

# Test project
pytest -q

# Dry-run agent
python -m agents.haian_dwh_agent.cli --goal daily_competitor_pipeline --dry-run

# Tắt hệ thống
docker compose down
```
