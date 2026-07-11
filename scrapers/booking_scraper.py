"""
scrapers/booking_scraper.py
============================
Cào dữ liệu giá phòng từ Booking.com bằng cách quét trang kết quả tìm kiếm (SRP).

Phương pháp: Playwright + Lật trang (Pagination).
Khu vực: Sơn Trà, Đà Nẵng
"""

import re
import time
import logging
from datetime import date, timedelta

from .base import (
    BOOKING_SEARCH_URL_TEMPLATE, MAX_PAGES_PER_DAY,
    PLAYWRIGHT_OK, STEALTH_OK,
    make_record, map_room_type, parse_price, random_delay, setup_browser,
)

log = logging.getLogger("scraper.booking")

class BookingScraper:
    PLATFORM = "booking"

    def __init__(self, headless: bool = True):
        self.headless = headless

    def scrape(self, checkin_dates: list[date]) -> list[dict]:
        if not PLAYWRIGHT_OK:
            log.error("[Booking] Playwright chua cai.")
            return []

        from .base import get_scraped_dates
        scraped = get_scraped_dates(self.PLATFORM)
        checkin_dates = [d for d in checkin_dates if d not in scraped]
        
        if not checkin_dates:
            log.info(f"[Booking] Tat ca cac ngay da duoc cao. Bo qua.")
            return []

        records = []
        log.info(f"[Booking] Bat dau cao {len(checkin_dates)} ngay check-in (Search Mode)")

        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as pw:
                browser, ctx = setup_browser(pw, self.headless)
                try:
                    for checkin in checkin_dates:
                        checkout = checkin + timedelta(days=1)
                        day_records = self._scrape_one_date(ctx, checkin, checkout)
                        records.extend(day_records)
                        from .base import write_records, CSV_OUTPUT
                        write_records(day_records, CSV_OUTPUT)
                        log.info(f"  [Booking] {checkin} → {len(day_records)} records (Đã lưu ngay)")
                finally:
                    browser.close()
        except Exception as e:
            log.error(f"[Booking] Loi nghiem trong: {e}")

        log.info(f"[Booking] Hoan thanh: {len(records)} records tong cong")
        return records

    def _scrape_one_date(self, ctx, checkin: date, checkout: date) -> list[dict]:
        """Quét tất cả các khách sạn trên trang Search cho 1 ngày check-in."""
        from playwright.sync_api import TimeoutError as PWTimeout
        if STEALTH_OK:
            from playwright_stealth import stealth_sync

        records = []
        page = ctx.new_page()
        if STEALTH_OK:
            stealth_sync(page)

        for page_num in range(MAX_PAGES_PER_DAY):
            offset = page_num * 25
            url = BOOKING_SEARCH_URL_TEMPLATE.format(
                checkin=checkin.strftime('%Y-%m-%d'),
                checkout=checkout.strftime('%Y-%m-%d'),
                offset=offset
            )
            
            log.info(f"  [Booking] {checkin} - Dang quet trang {page_num + 1} (Offset {offset})")

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                random_delay(2, 4)

                # Chờ các thẻ khách sạn xuất hiện
                try:
                    page.wait_for_selector('div[data-testid="property-card"]', timeout=10000)
                except PWTimeout:
                    log.warning(f"  [Booking] Khong tim thay the property-card o trang {page_num + 1}. Co the het ket qua.")
                    try:
                        # Chụp lại màn hình để gỡ lỗi
                        error_img_path = f"/opt/airflow/agents/logs/booking_error_{checkin.strftime('%Y%m%d')}_p{page_num+1}.png"
                        page.screenshot(path=error_img_path)
                        log.info(f"  [Booking] Da chup anh man hinh loi luu tai: {error_img_path}")
                    except Exception as e_snap:
                        log.warning(f"  [Booking] Khong the chup anh man hinh: {e_snap}")
                    break # Thoát vòng lặp trang

                cards = page.query_selector_all('div[data-testid="property-card"]')
                if not cards:
                    break

                for card in cards:
                    # Tên khách sạn
                    title_el = card.query_selector('[data-testid="title"]')
                    hotel_name = title_el.inner_text().strip() if title_el else "Unknown"

                    # Link
                    link_el = card.query_selector('a[data-testid="title-link"]')
                    hotel_link = link_el.get_attribute('href') if link_el else ""
                    if hotel_link and hotel_link.startswith("/"):
                        hotel_link = "https://www.booking.com" + hotel_link
                    
                    # Dọn dẹp query string trong link cho gọn
                    if hotel_link and "?" in hotel_link:
                        hotel_link = hotel_link.split("?")[0]

                    # Tạo record
                    rec = make_record(self.PLATFORM, hotel_name, checkin, checkout)
                    rec["hotel_link"] = hotel_link

                    # Loại phòng
                    room_el = card.query_selector('div[data-testid="recommended-units"] h4')
                    if room_el:
                        room_name = room_el.inner_text().strip()
                        rec["room_type_raw"] = room_name
                        rec["mapped_room_type_id"] = map_room_type(room_name)

                    # Điểm Rating
                    rating_el = card.query_selector('[data-testid="review-score"]')
                    if rating_el:
                        match = re.search(r'(\d+)[.,](\d+)', rating_el.inner_text())
                        if match:
                            rec["rating_score"] = f"{match.group(1)}.{match.group(2)}"
                    else:
                        # Fallback
                        alt_rating = card.query_selector('div.a3b8729ab1')
                        if alt_rating:
                            match = re.search(r'(\d+)[.,](\d+)', alt_rating.inner_text())
                            if match:
                                rec["rating_score"] = f"{match.group(1)}.{match.group(2)}"

                    # Giá
                    price_el = card.query_selector('[data-testid="price-and-discounted-price"]')
                    if price_el:
                        digits = parse_price(price_el.inner_text())
                        if digits:
                            rec["listed_price_vnd"] = digits
                            rec["discounted_price_vnd"] = digits
                            rec["scrape_status"] = "success"
                        else:
                            rec["is_sold_out"] = "true"
                            rec["scrape_status"] = "success"
                    else:
                        rec["is_sold_out"] = "true"
                        rec["scrape_status"] = "success"

                    records.append(rec)

            except PWTimeout:
                log.warning(f"  [Booking] Timeout tai trang {page_num + 1}")
                break
            except Exception as e:
                log.warning(f"  [Booking] Loi parse o trang {page_num + 1}: {e}")
                continue

        page.close()
        return records
