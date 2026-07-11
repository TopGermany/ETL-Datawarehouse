"""
scrapers/base.py
================
Tiện ích dùng chung cho tất cả scrapers.
Import từ đây thay vì copy-paste vào từng file.
"""

import os
import csv
import time
import uuid
import random
import logging
from pathlib import Path
from datetime import datetime, date, timedelta, timezone

from dotenv import load_dotenv

# ── Paths ──────────────────────────────────────────────────────────────────────
_SCRAPERS_DIR  = Path(__file__).parent          # haian_dwh_project/scrapers/
_PROJECT_DIR   = _SCRAPERS_DIR.parent           # haian_dwh_project/
_AGENTS_DIR    = _PROJECT_DIR / "agents"        # haian_dwh_project/agents/

# Load .env từ agents/
load_dotenv(_AGENTS_DIR / ".env")

log = logging.getLogger("scraper")

# ── Playwright (optional import) ───────────────────────────────────────────────
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    PLAYWRIGHT_OK = True
except ImportError:
    PLAYWRIGHT_OK = False

try:
    from playwright_stealth import stealth_sync
    STEALTH_OK = True
except ImportError:
    STEALTH_OK = False

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

# Tìm kiếm khu vực Sơn Trà, Đà Nẵng
# offset sẽ tăng dần 0, 25, 50... cho mỗi trang
BOOKING_SEARCH_URL_TEMPLATE = (
    "https://www.booking.com/searchresults.vi.html"
    "?ss=Đà+Nẵng&dest_id=-3712125&dest_type=city"
    "&checkin={checkin}&checkout={checkout}&offset={offset}"
)
MAX_PAGES_PER_DAY = 10  # Tối đa 10 trang (tương đương 250 khách sạn) mỗi ngày để tiết kiệm thời gian


ROOM_TYPE_MAPPING: dict[str, str] = {
    "penthouse":  "RT09",
    "suite":      "RT08",
    "villa":      "RT07",
    "family":     "RT06",
    "connecting": "RT05",
    "deluxe":     "RT04",
    "superior":   "RT03",
    "oasis":      "RT02",
    "standard":   "RT01",
}

# Tháng cao điểm mùa hè 2026 cần cào
PEAK_MONTHS: list[tuple[int, int]] = [(2026, 6), (2026, 7), (2026, 8)]

# Đường dẫn file CSV đầu ra
CSV_OUTPUT: Path = Path(os.getenv(
    "CSV_DATA_DIR",
    str(_PROJECT_DIR / "Data" / "Raw"),
)) / "fact_competitor_price_snapshot_template.csv"

# Cột của file CSV — KHÔNG thay đổi thứ tự (khớp với BigQuery schema)
CSV_HEADERS: list[str] = [
    "snapshot_id", "snapshot_datetime", "checkin_date", "checkin_date_key", "checkout_date",
    "search_los", "source_platform", "hotel_id", "competitor_hotel_name", "hotel_link",
    "location_area", "room_type_raw", "mapped_room_type_id",
    "listed_price_vnd", "discounted_price_vnd",
    "is_sold_out", "rating_score", "scrape_status"
]


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

class HotelDimensionManager:
    def __init__(self):
        self.dim_file = Path(os.getenv("CSV_DATA_DIR", str(_PROJECT_DIR / "Data" / "Raw"))) / "dim_hotel.csv"
        self.hotel_map = {}
        self.max_id = 0
        self._load()

    def _load(self):
        if not self.dim_file.exists():
            return
        with open(self.dim_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    hid = int(row['hotel_id'])
                    if hid > self.max_id:
                        self.max_id = hid
                    self.hotel_map[row['hotel_name'].strip().lower()] = hid
                except (ValueError, KeyError):
                    continue

    def get_or_create(self, hotel_name: str) -> int:
        name_clean = hotel_name.strip()
        name_lower = name_clean.lower()
        
        # Hardcode 1 số từ khóa phổ biến để không bị lệch do booking.com thêm suffix
        if "muong thanh" in name_lower or "mường thanh" in name_lower:
            return 2
        if "sala" in name_lower:
            return 3
        if "stella maris" in name_lower:
            return 4
        if "novotel" in name_lower:
            return 5
        if "haian" in name_lower or "hải an" in name_lower:
            return 1

        if name_lower in self.hotel_map:
            return self.hotel_map[name_lower]

        # Khách sạn mới hoàn toàn -> tự động đăng ký
        self.max_id += 1
        new_id = self.max_id
        
        with open(self.dim_file, 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            # Cột: hotel_id,hotel_code,hotel_name,hotel_type,star_rating,total_rooms,address,location_area,distance_to_beach_m,has_pool,has_spa,has_restaurant,data_source
            writer.writerow([
                new_id, 'AUTO_GEN', name_clean, 'Competitor', 0, 0, 
                'Unknown', 'Unknown', 0, 0, 0, 0, 'Scraper Auto-Discovery'
            ])
            
        self.hotel_map[name_lower] = new_id
        log.info(f"🆕 Phát hiện khách sạn mới: '{name_clean}'. Đã cấp ID: {new_id} vào dim_hotel.csv")
        return new_id

# Khởi tạo instance global
hotel_manager = HotelDimensionManager()


def make_record(
    platform: str,
    hotel_name: str,
    checkin: date,
    checkout: date,
    status: str = "success",
) -> dict:
    """Tạo một dòng record với các giá trị mặc định."""
    now = datetime.now(timezone.utc)
    # Lấy hoặc tạo tự động hotel_id
    hotel_id = hotel_manager.get_or_create(hotel_name)

    return {
        "snapshot_id":           str(uuid.uuid4()),
        "snapshot_datetime":     now.strftime("%Y-%m-%d %H:%M:%S"),
        "checkin_date":          checkin.strftime("%Y-%m-%d"),
        "checkin_date_key":      int(checkin.strftime("%Y%m%d")),
        "checkout_date":         checkout.strftime("%Y-%m-%d"),
        "search_los":            (checkout - checkin).days,
        "source_platform":       platform,
        "hotel_id":              hotel_id,
        "competitor_hotel_name": hotel_name,
        "hotel_link":            "",
        "location_area":         "My Khe Beach - Da Nang",
        "room_type_raw":         "",
        "mapped_room_type_id":   "",
        "listed_price_vnd":      "",
        "discounted_price_vnd":  "",
        "is_sold_out":           "",
        "rating_score":          "",
        "scrape_status":         status,
    }


def parse_price(text: str) -> str:
    """Tách số từ chuỗi giá. VD: 'VND 2,500,000' → '2500000'."""
    return "".join(c for c in text if c.isdigit()) if text else ""


def map_room_type(room_name_raw: str) -> str:
    """Ánh xạ tên phòng thô → room_type_id nội bộ."""
    if not room_name_raw:
        return ""
    name_lower = room_name_raw.lower()
    for keyword, rt_id in ROOM_TYPE_MAPPING.items():
        if keyword in name_lower:
            return rt_id
    return "RT01"


def random_delay(min_s: float = 3.0, max_s: float = 7.0) -> None:
    """Nghỉ ngẫu nhiên để tránh rate-limit / bot detection."""
    time.sleep(random.uniform(min_s, max_s))


def setup_browser(playwright, headless: bool = True):
    """
    Khởi tạo Playwright browser với cấu hình stealth.
    Trả về (browser, context).
    """
    browser = playwright.chromium.launch(
        headless=headless,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
        ],
    )
    ctx = browser.new_context(
        viewport={"width": 1366, "height": 768},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="vi-VN",
        timezone_id="Asia/Ho_Chi_Minh",
    )
    ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        window.chrome = {runtime: {}};
    """)
    return browser, ctx


def build_peak_dates() -> list[date]:
    """Trả về danh sách tất cả các ngày trong PEAK_MONTHS."""
    dates = []
    for year, month in PEAK_MONTHS:
        d = date(year, month, 1)
        while d.month == month:
            dates.append(d)
            d += timedelta(days=1)
    return dates


def get_scraped_dates(platform: str) -> set[date]:
    """Trả về danh sách các ngày check-in đã có dữ liệu thành công trong NGÀY HÔM NAY (UTC)."""
    scraped = set()
    if not CSV_OUTPUT.exists():
        return scraped
        
    # Chỉ lấy dữ liệu cào trong ngày hôm nay
    today_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    try:
        with open(CSV_OUTPUT, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Kiểm tra ngày cào có phải hôm nay không
                if row.get("snapshot_datetime", "").startswith(today_utc_str):
                    if row.get("source_platform", "").lower() == platform.lower():
                        if row.get("scrape_status") == "success":
                            try:
                                d = datetime.strptime(row["checkin_date"], "%Y-%m-%d").date()
                                scraped.add(d)
                            except ValueError:
                                pass
    except Exception as e:
        log.warning(f"Loi kiem tra lich su cao: {e}")
    return scraped


def write_records(records: list[dict], output_path: Path) -> int:
    """
    Ghi danh sách records vào file CSV.
    - Nếu file chưa tồn tại → tạo mới + ghi header.
    - Nếu file đã có dữ liệu → append (không trùng header).
    - Nếu file đang bị Excel khóa → ghi vào file tạm, thông báo cho user.
    Trả về số records đã ghi.
    """
    if not records:
        log.warning("[Writer] Khong co records de ghi.")
        return 0

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = output_path.exists()
    has_data = False
    if file_exists:
        try:
            with open(output_path, "r", encoding="utf-8-sig") as f:
                has_data = len(f.readlines()) > 1
        except Exception:
            has_data = False

    mode = "a" if (file_exists and has_data) else "w"

    # Thử ghi vào file chính — nếu bị khóa (Excel đang mở) thì ghi vào file tạm
    try:
        with open(output_path, mode, encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
            if mode == "w":
                writer.writeheader()
                log.info(f"[Writer] Tao moi file: {output_path.name}")
            writer.writerows(records)
        log.info(f"[Writer] Da ghi {len(records)} records vao {output_path.name}")
        return len(records)

    except PermissionError:
        # File đang bị khóa bởi Excel hoặc chương trình khác
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_path = output_path.parent / f"_temp_scraper_{timestamp}.csv"
        log.warning(
            f"[Writer] PermissionError: File '{output_path.name}' dang bi khoa "
            f"(co the Excel dang mo).\n"
            f"[Writer] Ghi tam vao: {temp_path.name}\n"
            f"[Writer] --> Dong Excel roi copy noi dung vao file chinh, hoac dung ETL Agent de merge."
        )
        with open(temp_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        log.info(f"[Writer] Da ghi {len(records)} records vao FILE TAM: {temp_path.name}")
        return len(records)
