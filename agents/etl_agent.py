"""
HAIAN DWH — ETL Agent
=====================
Tự động đồng bộ dữ liệu từ CSV (Data/Raw/) lên Google BigQuery.

Chức năng:
  - Chạy full sync hằng đêm theo lịch (mặc định 02:00 AM)
  - File watcher realtime: phát hiện CSV thay đổi → sync ngay
  - UPSERT: chỉ thêm/cập nhật dòng mới, không xóa dữ liệu cũ
  - Load theo thứ tự ERD: Dimension → Fact → Mart

Cách dùng:
  python etl_agent.py --run-now        # Full sync ngay lập tức
  python etl_agent.py --daemon         # Chạy nền + scheduler + file watcher
  python etl_agent.py --test-connection # Kiểm tra kết nối BigQuery
  python etl_agent.py --dry-run        # Validate dữ liệu, không upload thật
  python etl_agent.py --file <tên.csv> # Sync một file cụ thể
"""

import os
import sys

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

import time
import hashlib
import logging
import argparse
import threading
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import schedule
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ── Google Cloud ──────────────────────────────────────────────────────────────
try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from google.api_core.exceptions import NotFound, GoogleAPIError
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

# ══════════════════════════════════════════════════════════════════════════════
# 0. BOOTSTRAP
# ══════════════════════════════════════════════════════════════════════════════

# Tìm .env trong thư mục agents/
_AGENT_DIR = Path(__file__).parent
load_dotenv(_AGENT_DIR / ".env")

# Tạo thư mục logs nếu chưa có
_LOG_DIR = _AGENT_DIR / "logs"
_LOG_DIR.mkdir(exist_ok=True)

# ── Logger ────────────────────────────────────────────────────────────────────
_LOG_FILE = os.getenv("LOG_FILE", str(_LOG_DIR / "etl_agent.log"))
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("etl_agent")


# ══════════════════════════════════════════════════════════════════════════════
# 1. SCHEMA REGISTRY  — định nghĩa toàn bộ schema, PK, FK, load order
# ══════════════════════════════════════════════════════════════════════════════

class SchemaRegistry:
    """
    Trung tâm mô tả schema BigQuery, khoá chính và quan hệ FK cho mỗi bảng.
    Cũng định nghĩa thứ tự load để đảm bảo FK integrity (Dim → Fact → Mart).
    """

    # ── Thứ tự load (dependency order) ───────────────────────────────────────
    LOAD_ORDER: list[str] = [
        # Layer 0 — Calendar / Reference (không FK)
        "dim_date",
        # Layer 1 — Dimension không phụ thuộc nhau
        "dim_hotel",
        "dim_room_type",
        "dim_channel",
        "dim_guest_segment",
        "dim_cost_category",
        "dim_promotion",
        # Layer 2 — Fact Tables (phụ thuộc Layer 1)
        "fact_booking",
        "fact_room_inventory_daily",
        "fact_room_cost_daily",
        "fact_room_revenue_daily",
        "fact_distribution_cost_daily",
        "fact_guest_review",
        # Layer 3 — Competitor snapshot template (rỗng, chờ cào)
        "fact_competitor_price_snapshot_template",
        # Layer 4 — Mart & Summary
        "kpi_monthly_summary",
        "qa_checks_summary",
    ]

    # ── Map: csv file name (không extension) → tên bảng BQ ───────────────────
    CSV_TO_TABLE: dict[str, str] = {
        "dim_date":                                "dim_date",
        "dim_hotel":                               "dim_hotel",
        "dim_room_type":                           "dim_room_type",
        "dim_channel":                             "dim_channel",
        "dim_guest_segment":                       "dim_guest_segment",
        "dim_cost_category":                       "dim_cost_category",
        "dim_promotion":                           "dim_promotion",
        "fact_booking":                            "fact_booking",
        "fact_room_inventory_daily":               "fact_room_inventory_daily",
        "fact_room_cost_daily":                    "fact_room_cost_daily",
        "fact_room_revenue_daily":                 "fact_room_revenue_daily",
        "fact_distribution_cost_daily":            "fact_distribution_cost_daily",
        "fact_guest_review":                       "fact_guest_review",
        "fact_competitor_price_snapshot_template": "fact_competitor_price_snapshot_template",
        "kpi_monthly_summary":                     "kpi_monthly_summary",
        "qa_checks_summary":                       "qa_checks_summary",
    }

    # ── Primary keys cho mỗi bảng ────────────────────────────────────────────
    PRIMARY_KEYS: dict[str, list[str]] = {
        "dim_date":                                ["date_key"],
        "dim_hotel":                               ["hotel_id"],
        "dim_room_type":                           ["room_type_id"],
        "dim_channel":                             ["channel_id"],
        "dim_guest_segment":                       ["segment_id"],
        "dim_cost_category":                       ["cost_category_id"],
        "dim_promotion":                           ["promotion_id"],
        "fact_booking":                            ["booking_id"],
        "fact_room_inventory_daily":               ["date_key", "room_type_id"],
        "fact_room_cost_daily":                    ["date_key", "cost_category_id"],
        "fact_room_revenue_daily":                 ["room_revenue_id"],
        "fact_distribution_cost_daily":            ["distribution_cost_id"],
        "fact_guest_review":                       ["booking_id", "review_date"],
        "fact_competitor_price_snapshot_template": ["snapshot_id"],
        "kpi_monthly_summary":                     ["month_number"],
        "qa_checks_summary":                       ["check_name"],
    }

    # ── Foreign key relationships (để validate trước khi load) ───────────────
    FOREIGN_KEYS: dict[str, list[dict]] = {
        "fact_booking": [
            {"fk_col": "room_type_id",  "ref_table": "dim_room_type",     "ref_col": "room_type_id"},
            {"fk_col": "channel_id",    "ref_table": "dim_channel",        "ref_col": "channel_id"},
            {"fk_col": "segment_id",    "ref_table": "dim_guest_segment",  "ref_col": "segment_id"},
            {"fk_col": "promotion_id",  "ref_table": "dim_promotion",      "ref_col": "promotion_id"},
        ],
        "fact_room_inventory_daily": [
            {"fk_col": "date_key",      "ref_table": "dim_date",           "ref_col": "date_key"},
            {"fk_col": "room_type_id",  "ref_table": "dim_room_type",      "ref_col": "room_type_id"},
        ],
        "fact_room_cost_daily": [
            {"fk_col": "date_key",          "ref_table": "dim_date",           "ref_col": "date_key"},
            {"fk_col": "cost_category_id",  "ref_table": "dim_cost_category",  "ref_col": "cost_category_id"},
        ],
        "fact_room_revenue_daily": [
            {"fk_col": "date_key",      "ref_table": "dim_date",           "ref_col": "date_key"},
            {"fk_col": "room_type_id",  "ref_table": "dim_room_type",      "ref_col": "room_type_id"},
            {"fk_col": "channel_id",    "ref_table": "dim_channel",        "ref_col": "channel_id"},
            {"fk_col": "segment_id",    "ref_table": "dim_guest_segment",  "ref_col": "segment_id"},
            {"fk_col": "promotion_id",  "ref_table": "dim_promotion",      "ref_col": "promotion_id"},
        ],
        "fact_distribution_cost_daily": [
            {"fk_col": "date_key",      "ref_table": "dim_date",           "ref_col": "date_key"},
            {"fk_col": "channel_id",    "ref_table": "dim_channel",        "ref_col": "channel_id"},
        ],
        "fact_guest_review": [
            {"fk_col": "booking_id",    "ref_table": "fact_booking",       "ref_col": "booking_id"},
        ],
    }

    # ── BigQuery schema definitions (field name → BQ type) ───────────────────
    BQ_SCHEMAS: dict[str, list[bigquery.SchemaField]] = {}  # Lazy-built bên dưới

    @classmethod
    def get_pk(cls, table_name: str) -> list[str]:
        return cls.PRIMARY_KEYS.get(table_name, [])

    @classmethod
    def get_fks(cls, table_name: str) -> list[dict]:
        return cls.FOREIGN_KEYS.get(table_name, [])

    @classmethod
    def csv_to_table(cls, csv_stem: str) -> str | None:
        """Chuyển tên file CSV (không .csv) sang tên bảng BigQuery."""
        return cls.CSV_TO_TABLE.get(csv_stem)

    @classmethod
    def infer_bq_schema(cls, df: pd.DataFrame) -> list:
        """
        Tự động suy diễn BigQuery schema từ pandas DataFrame.
        Fallback về STRING nếu không nhận dạng được kiểu dữ liệu.
        """
        if not GCP_AVAILABLE:
            return []

        type_map = {
            "int64":   "INTEGER",
            "int32":   "INTEGER",
            "float64": "FLOAT",
            "float32": "FLOAT",
            "bool":    "BOOLEAN",
            "object":  "STRING",
        }
        schema = []
        for col, dtype in df.dtypes.items():
            dtype_str = str(dtype)
            if "datetime" in dtype_str:
                bq_type = "DATETIME"
            elif "date" in dtype_str:
                bq_type = "DATE"
            else:
                bq_type = type_map.get(dtype_str, "STRING")
            schema.append(bigquery.SchemaField(col, bq_type, mode="NULLABLE"))
        return schema


# ══════════════════════════════════════════════════════════════════════════════
# 2. BIGQUERY UPLOADER
# ══════════════════════════════════════════════════════════════════════════════

class BigQueryUploader:
    """
    Quản lý toàn bộ thao tác với Google BigQuery:
    - Tạo bảng nếu chưa có
    - UPSERT dựa trên primary key
    - Validate foreign key integrity trước khi load
    """

    def __init__(self, project_id: str, dataset_id: str, credentials_path: str | None = None):
        if not GCP_AVAILABLE:
            raise ImportError(
                "Thư viện google-cloud-bigquery chưa được cài đặt.\n"
                "Chạy: pip install -r agents/requirements.txt"
            )

        self.project_id = project_id
        self.dataset_id = dataset_id
        self._client: bigquery.Client | None = None
        self.credentials_path = credentials_path

        # Cache row counts của dimension tables đã load (dùng cho FK validation)
        self._loaded_pk_cache: dict[str, set] = {}

    def _get_client(self) -> bigquery.Client:
        """Khởi tạo BigQuery client (lazy initialization)."""
        if self._client is None:
            if self.credentials_path and Path(self.credentials_path).exists():
                creds = service_account.Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
                self._client = bigquery.Client(
                    project=self.project_id,
                    credentials=creds,
                )
            else:
                # Dùng Application Default Credentials nếu không có file JSON
                self._client = bigquery.Client(project=self.project_id)
            log.info(f"✅ Kết nối BigQuery thành công — project: {self.project_id}")
        return self._client

    def test_connection(self) -> bool:
        """Kiểm tra kết nối BigQuery."""
        try:
            client = self._get_client()
            # Thử list datasets để xác nhận kết nối
            list(client.list_datasets())
            log.info("✅ Kết nối BigQuery OK!")
            return True
        except Exception as e:
            log.error(f"❌ Kết nối BigQuery thất bại: {e}")
            return False

    def ensure_dataset(self) -> None:
        """Tạo dataset nếu chưa tồn tại."""
        client = self._get_client()
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        try:
            client.get_dataset(dataset_ref)
            log.debug(f"Dataset '{self.dataset_id}' đã tồn tại.")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            client.create_dataset(dataset, timeout=30)
            log.info(f"✅ Đã tạo dataset '{self.dataset_id}'")

    def get_table_ref(self, table_name: str) -> str:
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    def table_exists(self, table_name: str) -> bool:
        try:
            self._get_client().get_table(self.get_table_ref(table_name))
            return True
        except NotFound:
            return False

    def get_existing_pks(self, table_name: str, pk_cols: list[str]) -> set[tuple]:
        """
        Lấy tập hợp các primary key đã có trong BigQuery để so sánh với CSV.
        Trả về set of tuples (pk_val1, pk_val2, ...).
        """
        if not pk_cols or not self.table_exists(table_name):
            return set()

        client = self._get_client()
        cols_sql = ", ".join(pk_cols)
        query = f"SELECT {cols_sql} FROM `{self.get_table_ref(table_name)}`"
        try:
            df = client.query(query).to_dataframe()
            if df.empty:
                return set()
            if len(pk_cols) == 1:
                return set(df[pk_cols[0]].astype(str).tolist())
            return set(tuple(str(v) for v in row) for row in df[pk_cols].itertuples(index=False))
        except Exception as e:
            log.warning(f"⚠️  Không lấy được PK từ {table_name}: {e}")
            return set()

    def validate_fk(
        self,
        df: pd.DataFrame,
        table_name: str,
        dry_run: bool = False,
    ) -> tuple[pd.DataFrame, list[str]]:
        """
        Kiểm tra FK integrity:
        - Với mỗi FK relationship, xác nhận giá trị trong df có tồn tại
          trong bảng tham chiếu đã load vào BQ hoặc trong cache.
        - Các dòng vi phạm FK sẽ bị loại (nếu không phải dry_run) hoặc chỉ log.
        Trả về (df_cleaned, danh sách lỗi).
        """
        fk_defs = SchemaRegistry.get_fks(table_name)
        if not fk_defs:
            return df, []

        errors = []
        for fk in fk_defs:
            fk_col = fk["fk_col"]
            ref_table = fk["ref_table"]
            ref_col = fk["ref_col"]

            if fk_col not in df.columns:
                continue

            # Lấy valid values từ cache hoặc query BQ
            if ref_table not in self._loaded_pk_cache:
                self._loaded_pk_cache[ref_table] = self.get_existing_pks(ref_table, [ref_col])

            valid_vals = self._loaded_pk_cache[ref_table]
            if not valid_vals:
                log.warning(
                    f"⚠️  Bảng tham chiếu '{ref_table}' rỗng hoặc chưa load — "
                    f"bỏ qua FK check cho {table_name}.{fk_col}"
                )
                continue

            csv_vals = set(df[fk_col].dropna().astype(str).tolist())
            orphan_vals = csv_vals - valid_vals
            if orphan_vals:
                msg = (
                    f"FK violation {table_name}.{fk_col} → {ref_table}.{ref_col}: "
                    f"{len(orphan_vals)} giá trị không tồn tại: "
                    f"{list(orphan_vals)[:5]}{'...' if len(orphan_vals) > 5 else ''}"
                )
                errors.append(msg)
                log.error(f"❌ {msg}")

                if not dry_run:
                    # Loại các dòng vi phạm FK
                    before = len(df)
                    df = df[~df[fk_col].astype(str).isin(orphan_vals)]
                    after = len(df)
                    log.warning(f"   ↳ Đã loại {before - after} dòng vi phạm FK")

        return df, errors

    def upsert_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        pk_cols: list[str],
        dry_run: bool = False,
    ) -> dict:
        """
        UPSERT dữ liệu vào BigQuery:
        1. Nếu bảng chưa tồn tại → tạo mới và insert toàn bộ
        2. Nếu đã tồn tại → so sánh PK, chỉ insert dòng mới (không có PK trong BQ)

        Trả về dict với thống kê: inserted, skipped, errors.
        """
        result = {"table": table_name, "inserted": 0, "skipped": 0, "errors": []}

        if df.empty:
            log.warning(f"⚠️  [{table_name}] DataFrame rỗng, bỏ qua.")
            return result

        client = self._get_client()
        table_ref = self.get_table_ref(table_name)

        # Infer schema từ df
        schema = SchemaRegistry.infer_bq_schema(df)

        # ── Tạo bảng nếu chưa tồn tại ────────────────────────────────────────
        if not self.table_exists(table_name):
            if dry_run:
                log.info(f"  [DRY-RUN] Sẽ tạo bảng '{table_name}' ({len(df)} dòng)")
                result["inserted"] = len(df)
                return result

            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            log.info(f"✅ Đã tạo bảng '{table_name}'")

        # ── Tìm dòng mới (so sánh PK) ────────────────────────────────────────
        existing_pks = self.get_existing_pks(table_name, pk_cols)

        if pk_cols and existing_pks:
            if len(pk_cols) == 1:
                pk_col = pk_cols[0]
                csv_pks = df[pk_col].astype(str)
                new_rows = df[~csv_pks.isin(existing_pks)]
            else:
                def pk_tuple(row):
                    return tuple(str(row[c]) for c in pk_cols)
                mask = df.apply(lambda r: pk_tuple(r) not in existing_pks, axis=1)
                new_rows = df[mask]

            skipped = len(df) - len(new_rows)
            result["skipped"] = skipped
            if skipped > 0:
                log.debug(f"  [{table_name}] {skipped} dòng đã tồn tại → bỏ qua")
        else:
            new_rows = df  # Bảng rỗng hoặc không có PK → insert hết

        if new_rows.empty:
            log.info(f"  [{table_name}] Không có dòng mới.")
            return result

        if dry_run:
            log.info(f"  [DRY-RUN] Sẽ insert {len(new_rows)} dòng mới vào '{table_name}'")
            result["inserted"] = len(new_rows)
            return result

        # ── Insert dòng mới ───────────────────────────────────────────────────
        try:
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_APPEND",
            )
            job = client.load_table_from_dataframe(new_rows, table_ref, job_config=job_config)
            job.result()  # Chờ job hoàn thành
            result["inserted"] = len(new_rows)
            log.info(f"  ✅ [{table_name}] Đã insert {len(new_rows)} dòng mới")

            # Cập nhật cache PK sau khi insert
            if table_name not in self._loaded_pk_cache:
                self._loaded_pk_cache[table_name] = existing_pks
            if pk_cols and len(pk_cols) == 1:
                pk_col = pk_cols[0]
                self._loaded_pk_cache[table_name].update(
                    new_rows[pk_col].astype(str).tolist()
                )

        except GoogleAPIError as e:
            err_msg = f"BigQuery API error khi insert '{table_name}': {e}"
            log.error(f"❌ {err_msg}")
            result["errors"].append(err_msg)

        return result


# ══════════════════════════════════════════════════════════════════════════════
# 3. CSV FILE WATCHER
# ══════════════════════════════════════════════════════════════════════════════

class CSVChangeHandler(FileSystemEventHandler):
    """
    Watchdog event handler: phát hiện CSV bị modified trong thư mục Data/Raw/
    và gọi callback để trigger incremental sync.
    """

    def __init__(self, on_csv_changed: callable, debounce_seconds: float = 30.0):
        super().__init__()
        self._callback = on_csv_changed
        self._debounce = debounce_seconds
        self._pending: dict[str, threading.Timer] = {}
        self._lock = threading.Lock()

    def on_modified(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() != ".csv":
            return
        self._schedule_callback(path)

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() != ".csv":
            return
        self._schedule_callback(path)

    def _schedule_callback(self, path: Path):
        """Debounce: chờ X giây sau khi file ngừng thay đổi mới trigger."""
        key = str(path)
        with self._lock:
            if key in self._pending:
                self._pending[key].cancel()
            timer = threading.Timer(
                self._debounce,
                self._fire,
                args=[path],
            )
            self._pending[key] = timer
            timer.start()

    def _fire(self, path: Path):
        with self._lock:
            self._pending.pop(str(path), None)
        log.info(f"📄 File thay đổi detected: {path.name}")
        self._callback(path)


# ══════════════════════════════════════════════════════════════════════════════
# 4. ETL AGENT  — Orchestrator chính
# ══════════════════════════════════════════════════════════════════════════════

class ETLAgent:
    """
    Orchestrator điều phối toàn bộ pipeline:
    - Full sync: load tất cả CSV theo thứ tự ERD
    - Incremental sync: chỉ load file vừa thay đổi
    - Scheduler: chạy full sync hàng đêm
    - File watcher: trigger incremental sync realtime
    """

    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID", "")
        self.dataset_id = os.getenv("BQ_DATASET_ID", "haian_dwh")
        self.credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
        self.csv_dir = Path(os.getenv("CSV_DATA_DIR", ""))
        self.full_sync_time = os.getenv("FULL_SYNC_TIME", "02:00")

        self._uploader: BigQueryUploader | None = None
        self._observer: Observer | None = None
        self._run_stats: list[dict] = []

        self._validate_config()

    def _validate_config(self):
        """Kiểm tra cấu hình trước khi chạy."""
        errors = []
        if not self.project_id:
            errors.append("GCP_PROJECT_ID chưa được thiết lập trong .env")
        if not self.csv_dir or not self.csv_dir.exists():
            errors.append(f"CSV_DATA_DIR không tồn tại: '{self.csv_dir}'")
        if errors:
            log.error("❌ Lỗi cấu hình:\n  " + "\n  ".join(errors))
            log.error(
                "💡 Hãy copy .env.example thành .env và điền thông tin:\n"
                f"   cp {_AGENT_DIR / '.env.example'} {_AGENT_DIR / '.env'}"
            )
            # Không raise — để --test-connection vẫn chạy được

    def _get_uploader(self) -> BigQueryUploader:
        if self._uploader is None:
            self._uploader = BigQueryUploader(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                credentials_path=self.credentials_path if self.credentials_path else None,
            )
        return self._uploader

    # ── CSV Loader ─────────────────────────────────────────────────────────────

    def _load_csv(self, csv_path: Path) -> pd.DataFrame | None:
        """Đọc CSV, xử lý encoding, trả về DataFrame sạch."""
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig", low_memory=False)
            # Xoá dòng hoàn toàn rỗng
            df = df.dropna(how="all")
            
            # Khắc phục lỗi PyArrow "Expected bytes, got a 'bool' object"
            if "is_sold_out" in df.columns:
                df["is_sold_out"] = df["is_sold_out"].astype(str).replace("nan", "")
                # Không lọc bỏ nữa, lấy cả dữ liệu sold_out = true theo yêu cầu mới
            if "breakfast_included" in df.columns:
                df["breakfast_included"] = df["breakfast_included"].astype(str).replace("nan", "")

            log.debug(f"  Đọc {csv_path.name}: {len(df)} dòng, {len(df.columns)} cột")
            return df
        except Exception as e:
            log.error(f"❌ Không đọc được '{csv_path.name}': {e}")
            return None
    def send_alert_email(self, subject: str, body: str):
        smtp_user = os.getenv("SMTP_USER")
        smtp_password = os.getenv("SMTP_PASSWORD")
        alert_email = os.getenv("ALERT_EMAIL")
        if not smtp_user or not smtp_password or not alert_email:
            log.warning("Chưa cấu hình Email, bỏ qua gửi cảnh báo.")
            return
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = alert_email
            msg['Subject'] = f"[HAIAN DWH] ⚠️ CẢNH BÁO DATA QUALITY - {subject}"
            msg.attach(MIMEText(body, 'html'))
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
            server.quit()
            log.info(f"📧 Đã gửi email cảnh báo lỗi tới {alert_email}")
        except Exception as e:
            log.error(f"Lỗi khi gửi email cảnh báo: {e}")

    def apply_business_rules(self, table_name: str, df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
        """Kiểm tra và lọc các lỗi theo Business Rules. Trả về df đã lọc và danh sách lỗi."""
        errors = []
        if df.empty:
            return df, errors

        bad_indices = set()

        # 1. Chống số Âm (Negative Values)
        money_cols = [c for c in df.columns if any(x in c.lower() for x in ['revenue', 'cost', 'price', 'amount', 'profit'])]
        for col in money_cols:
            if pd.api.types.is_numeric_dtype(df[col]):
                negatives = df[df[col] < 0]
                if not negatives.empty:
                    errors.append(f"[{table_name}] Cột '{col}' có {len(negatives)} dòng bị số âm (Hệ thống đã tự động ép về 0).")
                    df.loc[df[col] < 0, col] = 0

        # 2. Logic Doanh thu: rooms_sold = 0 -> revenue = 0
        if 'rooms_sold' in df.columns:
            zero_sold = df[df['rooms_sold'] == 0]
            if not zero_sold.empty:
                for rev_col in ['gross_room_revenue', 'listed_room_revenue', 'net_room_revenue']:
                    if rev_col in df.columns:
                        wrong_rev = zero_sold[zero_sold[rev_col] > 0]
                        if not wrong_rev.empty:
                            errors.append(f"[{table_name}] Bán 0 phòng nhưng '{rev_col}' > 0 ở {len(wrong_rev)} dòng.")
                            bad_indices.update(wrong_rev.index)

        # 3. Giới hạn vật lý Khách sạn
        if 'sellable_rooms' in df.columns:
            over_physical = df[df['sellable_rooms'] > 212]
            if not over_physical.empty:
                errors.append(f"[{table_name}] Phát hiện {len(over_physical)} dòng có sellable_rooms > 212 (Vượt giới hạn vật lý Hải An).")
                bad_indices.update(over_physical.index)

        # 4. Phòng bán <= Phòng khả dụng
        if 'rooms_sold' in df.columns and 'total_rooms_available' in df.columns:
            over_sold = df[df['rooms_sold'] > df['total_rooms_available']]
            if not over_sold.empty:
                errors.append(f"[{table_name}] Phát hiện {len(over_sold)} dòng có rooms_sold > total_rooms_available (Lỗi Overbooking/Sai số liệu).")
                bad_indices.update(over_sold.index)

        if bad_indices:
            bad_df = df.loc[list(bad_indices)]
            err_details = "<br>".join(errors)
            html_body = f"<h2>Phát hiện dữ liệu bất thường tại bảng {table_name}</h2><p>Hệ thống ETL Agent đã tự động <b>chặn</b> các dòng lỗi này không cho tải lên BigQuery để bảo vệ Lớp Vàng.</p><h3>Chi tiết lỗi:</h3><p style='color:red;'>{err_details}</p><h3>Dữ liệu lỗi (vui lòng kiểm tra lại file CSV gốc và sửa lại):</h3>{bad_df.head(10).to_html()}<p><em>Chỉ hiển thị tối đa 10 dòng đầu tiên bị lỗi.</em></p>"
            self.send_alert_email(f"Lỗi tại {table_name}", html_body)
            df = df.drop(index=list(bad_indices)).reset_index(drop=True)
            log.warning(f"  [DQ CẢNH BÁO] Đã chặn {len(bad_indices)} dòng vi phạm Business Rules (Đã gửi Email).")

        return df, errors

    # ── Sync một bảng ─────────────────────────────────────────────────────────

    def _sync_one_table(
        self,
        table_name: str,
        csv_path: Path,
        dry_run: bool = False,
    ) -> dict:
        """Pipeline cho một bảng: read → validate FK → upsert BQ."""
        log.info(f"  ▶ {table_name} ({csv_path.name})")
        result = {"table": table_name, "inserted": 0, "skipped": 0, "errors": [], "status": "ok"}

        # 1. Đọc CSV
        df = self._load_csv(csv_path)
        if df is None or df.empty:
            result["status"] = "empty"
            log.warning(f"    ↳ File rỗng hoặc chỉ có header, bỏ qua.")
            return result

        # 1.5 Áp dụng Business Rules & Data Quality Checks
        df, rule_errors = self.apply_business_rules(table_name, df)
        if rule_errors:
            result["errors"].extend(rule_errors)
            if df.empty:
                result["status"] = "business_rule_violation_all"
                log.error(f"    ↳ Toàn bộ dòng vi phạm Business Rules, không insert.")
                return result

        uploader = self._get_uploader()

        # 2. Validate FK integrity
        df, fk_errors = uploader.validate_fk(df, table_name, dry_run=dry_run)
        result["errors"].extend(fk_errors)

        if df.empty:
            result["status"] = "fk_violation_all"
            log.error(f"    ↳ Toàn bộ dòng vi phạm FK, không insert.")
            return result

        # 3. UPSERT lên BigQuery
        pk_cols = SchemaRegistry.get_pk(table_name)
        upsert_result = uploader.upsert_table(table_name, df, pk_cols, dry_run=dry_run)
        result["inserted"] = upsert_result["inserted"]
        result["skipped"] = upsert_result["skipped"]
        result["errors"].extend(upsert_result["errors"])

        if upsert_result["errors"]:
            result["status"] = "error"

        return result

    # ── Full Sync ──────────────────────────────────────────────────────────────

    def run_full_sync(self, dry_run: bool = False) -> list[dict]:
        """
        Chạy toàn bộ pipeline theo thứ tự load_order ERD.
        Bỏ qua file không có trong thư mục CSV.
        """
        mode = "[DRY-RUN] " if dry_run else ""
        log.info(f"{'='*60}")
        log.info(f"🚀 {mode}Full Sync bắt đầu lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info(f"{'='*60}")

        start_time = time.perf_counter()
        all_results = []

        if not dry_run:
            try:
                uploader = self._get_uploader()
                uploader.ensure_dataset()
            except Exception as e:
                log.error(f"❌ Không thể kết nối BigQuery: {e}")
                return []

        # Xây map: table_name → csv_path
        csv_map: dict[str, Path] = {}
        for f in self.csv_dir.glob("*.csv"):
            tname = SchemaRegistry.csv_to_table(f.stem)
            if tname:
                csv_map[tname] = f

        total_inserted = 0
        total_skipped = 0
        total_errors = 0

        for table_name in SchemaRegistry.LOAD_ORDER:
            csv_path = csv_map.get(table_name)
            if csv_path is None:
                log.debug(f"  ⏭  {table_name}: file không tìm thấy trong {self.csv_dir.name}/")
                continue

            result = self._sync_one_table(table_name, csv_path, dry_run=dry_run)
            all_results.append(result)
            total_inserted += result.get("inserted", 0)
            total_skipped += result.get("skipped", 0)
            total_errors += len(result.get("errors", []))

        elapsed = time.perf_counter() - start_time
        log.info(f"{'='*60}")
        log.info(
            f"✅ {mode}Full Sync hoàn thành trong {elapsed:.1f}s | "
            f"Inserted: {total_inserted} | Skipped: {total_skipped} | Errors: {total_errors}"
        )
        log.info(f"{'='*60}")

        self._run_stats.append({
            "run_type": "full_sync",
            "dry_run": dry_run,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elapsed_s": round(elapsed, 2),
            "total_inserted": total_inserted,
            "total_skipped": total_skipped,
            "total_errors": total_errors,
        })
        return all_results

    # ── Incremental Sync ───────────────────────────────────────────────────────

    def run_incremental_sync(self, csv_path: Path, dry_run: bool = False):
        """Sync một file CSV đơn lẻ (trigger từ file watcher)."""
        table_name = SchemaRegistry.csv_to_table(csv_path.stem)
        if table_name is None:
            log.warning(f"⚠️  '{csv_path.name}' không có trong schema registry, bỏ qua.")
            return

        log.info(f"⚡ Incremental Sync: {csv_path.name} → {table_name}")
        try:
            uploader = self._get_uploader()
            uploader.ensure_dataset()
        except Exception as e:
            log.error(f"❌ Không thể kết nối BigQuery: {e}")
            return

        result = self._sync_one_table(table_name, csv_path, dry_run=dry_run)
        log.info(
            f"⚡ Incremental done: {table_name} | "
            f"Inserted: {result['inserted']} | Skipped: {result['skipped']} | "
            f"Errors: {len(result['errors'])}"
        )

    # Chức năng chạy nền (daemon), scheduler và file watcher đã bị xóa
    # Vui lòng sử dụng Apache Airflow để lên lịch trình (Schedule)



# ══════════════════════════════════════════════════════════════════════════════
# 5. ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def _print_banner():
    banner = r"""
  _   _    _    ___ _   _   ____  __        __ _   _
 | | | |  / \  |_ _| \ | | |  _ \ \ \      / /| | | |
 | |_| | / _ \  | ||  \| | | | | | \ \ /\ / / | |_| |
 |  _  |/ ___ \ | || |\  | | |_| |  \ V  V /  |  _  |
 |_| |_/_/   \_\___|_| \_| |____/    \_/\_/   |_| |_|

 HAIAN DWH — ETL Agent  |  BigQuery Sync
    """
    print(banner)


def main():
    _print_banner()

    parser = argparse.ArgumentParser(
        description="HAIAN DWH ETL Agent — Đồng bộ CSV → Google BigQuery"
    )

    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Chạy full sync ngay lập tức một lần rồi thoát",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate dữ liệu, không upload thật lên BigQuery",
    )
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Kiểm tra kết nối Google BigQuery",
    )
    parser.add_argument(
        "--file",
        metavar="CSV_FILENAME",
        help="Sync một file CSV cụ thể, VD: fact_booking.csv",
    )
    args = parser.parse_args()

    agent = ETLAgent()

    if args.test_connection:
        log.info("🔌 Kiểm tra kết nối BigQuery...")
        uploader = BigQueryUploader(
            project_id=agent.project_id,
            dataset_id=agent.dataset_id,
            credentials_path=agent.credentials_path or None,
        )
        ok = uploader.test_connection()
        sys.exit(0 if ok else 1)

    elif args.file:
        csv_path = agent.csv_dir / args.file
        if not csv_path.exists():
            log.error(f"❌ File không tồn tại: {csv_path}")
            sys.exit(1)
        agent.run_incremental_sync(csv_path, dry_run=args.dry_run)

    elif args.run_now:
        agent.run_full_sync(dry_run=args.dry_run)

    elif args.dry_run:
        # Dry run không kèm --run-now → chạy full dry run
        agent.run_full_sync(dry_run=True)


    else:
        parser.print_help()
        print("\n💡 Gợi ý lần đầu:")
        print("   1. Copy .env.example → .env và điền GCP_PROJECT_ID, GOOGLE_CREDENTIALS_PATH")
        print("   2. python etl_agent.py --test-connection")
        print("   3. python etl_agent.py --dry-run")
        print("   4. python etl_agent.py --daemon\n")


if __name__ == "__main__":
    main()
