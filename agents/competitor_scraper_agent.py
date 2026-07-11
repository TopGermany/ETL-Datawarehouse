"""
agents/competitor_scraper_agent.py
====================================
FILE CHÍNH — Điều phối (Orchestrator) Booking.com Scraper.

Vai trò:
  - Khởi chạy BookingScraper theo lịch hoặc thủ công
  - Gộp kết quả vào file CSV duy nhất
  - Chạy tự động hàng đêm lúc 03:00 AM

Cách dùng:
  python competitor_scraper_agent.py --run-now          # Cào ngay tất cả ngày
  python competitor_scraper_agent.py --daemon           # Chạy nền tự động hàng đêm
  python competitor_scraper_agent.py --test             # Test 1 ngày (2026-07-15)
  python competitor_scraper_agent.py --test --no-headless  # Test với browser hiện
  python competitor_scraper_agent.py --dry-run          # Cào nhưng không ghi file
"""

import os
import sys

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

import logging
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta

# ── Thêm project root vào sys.path để import scrapers/ ───────────────────────
_AGENTS_DIR  = Path(__file__).parent
_PROJECT_DIR = _AGENTS_DIR.parent
sys.path.insert(0, str(_PROJECT_DIR))

from dotenv import load_dotenv
import schedule
import time

load_dotenv(_AGENTS_DIR / ".env")

# ── Import scraper ────────────────────────────────────────────────────────
from scrapers import (
    BookingScraper,
    build_peak_dates,
    write_records,
    CSV_OUTPUT,
)

# ══════════════════════════════════════════════════════════════════════════════
# LOGGER
# ══════════════════════════════════════════════════════════════════════════════

_LOG_DIR = _AGENTS_DIR / "logs"
_LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            _LOG_DIR / "competitor_scraper.log", encoding="utf-8"
        ),
    ],
)
log = logging.getLogger("orchestrator")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

SCRAPER_SCHEDULE_TIME = os.getenv("SCRAPER_SCHEDULE_TIME", "03:00")
HEADLESS              = os.getenv("SCRAPER_HEADLESS", "true").lower() == "true"

# Ngày test cố định (dùng với --test)
TEST_DATE = date(2026, 7, 15)


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def run_all(
    checkin_dates: list[date] | None = None,
    dry_run: bool = False,
    headless: bool = True,
) -> list[dict]:
    """
    Hàm điều phối chính.

    Args:
        checkin_dates: Danh sách ngày check-in cần cào.
                       None → dùng build_peak_dates() (mùa hè 2026).
        dry_run:       True → không ghi file (chỉ in kết quả).
        headless:      True → browser ẩn, False → hiện browser (debug).
    """
    if checkin_dates is None:
        checkin_dates = build_peak_dates()

    start_time = datetime.now()
    all_records: list[dict] = []

    _print_banner()
    log.info("=" * 62)
    log.info(f"Bat dau scrape: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Ngay cao  : {len(checkin_dates)} ngay")
    log.info(f"Dry-run   : {dry_run}")
    log.info(f"Headless  : {headless}")
    log.info("=" * 62)

    log.info(f"\n{'─'*40}")
    log.info(f"Bat dau: BOOKING.COM")
    log.info(f"{'─'*40}")
    
    try:
        scraper = BookingScraper(headless=headless)
        records = scraper.scrape(checkin_dates)
        all_records.extend(records)
        log.info(f"[booking] Thu thap duoc: {len(records)} records")
    except Exception as e:
        log.error(f"[booking] Loi nghiem trong: {e}", exc_info=True)

    log.info(f"\n{'─'*40}")
    log.info(f"Bat dau: IVIVU.COM")
    try:
        from scrapers.ivivu_scraper import IvivuScraper
        ivivu = IvivuScraper(headless=headless)
        ivivu_records = ivivu.scrape(checkin_dates)
        all_records.extend(ivivu_records)
        log.info(f"[ivivu] Thu thap duoc: {len(ivivu_records)} records")

    except Exception as e:
        log.error(f"[ivivu] Loi nghiem trong: {e}", exc_info=True)



    elapsed = (datetime.now() - start_time).total_seconds()

    # ── Ghi kết quả ───────────────────────────────────────────────────────────
    log.info(f"\n{'='*62}")
    log.info(f"Ket qua tong: {len(all_records)} records | Thoi gian: {elapsed:.0f}s")

    if dry_run:
        log.info("[DRY-RUN] Khong ghi file.")
        _print_summary(all_records)
    elif all_records:
        # written = write_records(all_records, CSV_OUTPUT) # Đã ghi tăng dần ở scraper
        log.info(f"Da hoan thanh scrape, cac record da duoc ghi tung phan vao: {CSV_OUTPUT}")
    _print_summary(all_records)
    log.info("=" * 62)
    
    # Đã tắt chức năng tự động gọi ETL bằng subprocess vì giờ đã có Airflow điều phối
    log.info("✅ Da hoan tat scrape! Airflow se lo phan tiep theo.")
        
    return all_records


def _print_banner() -> None:
    print("""
  #############################################
  # DWH Competitor Price Scraper Agent        #
  # Platform: Booking.com & Trip.com          #
  #############################################
    """)


def _print_summary(records: list[dict]) -> None:
    """In thống kê ngắn gọn theo scrape_status."""
    from collections import Counter
    by_status   = Counter(r["scrape_status"]   for r in records)

    log.info("─" * 30)
    log.info("Thong ke theo trang thai:")
    for status, count in sorted(by_status.items()):
        icon = "✅" if status == "success" else "⚠️"
        log.info(f"  {icon} {status:<25}: {count}")
    log.info("─" * 30)


# Chức năng chạy nền (daemon) bằng thư viện schedule đã bị xóa
# Vui lòng sử dụng Apache Airflow để lên lịch trình (Schedule)


# ══════════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="HAIAN DWH — Competitor Price Scraper Orchestrator (Booking Only)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Vi du:
  python competitor_scraper_agent.py --test
  python competitor_scraper_agent.py --test --no-headless
  python competitor_scraper_agent.py --run-now
  python competitor_scraper_agent.py --dry-run
  python competitor_scraper_agent.py --daemon
        """,
    )

    # Che do chay
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--run-now",  action="store_true", help="Cao tat ca ngay lap tuc")
    mode.add_argument("--test",     action="store_true", help=f"Test 1 ngay ({TEST_DATE})")
    mode.add_argument("--dry-run",  action="store_true", help="Cao nhung khong ghi file")

    # Tuy chon them
    parser.add_argument("--no-headless",    action="store_true", help="Hien thi browser (debug)")

    args = parser.parse_args()

    headless = not args.no_headless and HEADLESS

    # Chay theo mode
    if args.test:
        run_all(
            checkin_dates=[TEST_DATE],
            dry_run=False,
            headless=headless,
        )
    elif args.dry_run:
        run_all(
            checkin_dates=build_peak_dates(),
            dry_run=True,
            headless=headless,
        )
    elif args.run_now:
        run_all(headless=headless)
    else:
        parser.print_help()
        print("\n💡 Goi y bat dau:")
        print("   python competitor_scraper_agent.py --test           # Test nhanh 1 ngay")
        print("   python competitor_scraper_agent.py --run-now        # Cao day du mua he")
        print("   python competitor_scraper_agent.py --daemon         # Chay hang dem\n")


if __name__ == "__main__":
    main()
