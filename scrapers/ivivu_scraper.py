import os
import logging
from datetime import datetime, date, timedelta
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from scrapers.base import (
    make_record,
    random_delay,
    write_records,
    CSV_OUTPUT,
    get_scraped_dates,
)

log = logging.getLogger("scraper.ivivu")

class IvivuScraper:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.platform = "Ivivu.com"
        self.base_url = "https://www.ivivu.com/khach-san-da-nang"
        self.scraped_dates = get_scraped_dates(self.platform)
        self.records_buffer = []
        self.batch_size = 50

    def scrape(self, checkin_dates: list[date], los_list: list[int] = None) -> list[dict]:
        if los_list is None:
            los_list = [1, 2]

        all_records = []
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=self.headless,
                    args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
                )
                
                for checkin in checkin_dates:
                    if checkin in self.scraped_dates:
                        log.info(f"[{self.platform}] Da cao {checkin}. Bo qua.")
                        continue
                        
                    for los in los_list:
                        recs = self._scrape_single_date(browser, checkin, los)
                        all_records.extend(recs)
                        self.records_buffer.extend(recs)
                        
                        if len(self.records_buffer) >= self.batch_size:
                            self._flush_buffer()
                            
                    random_delay(2.0, 5.0)

                browser.close()
                self._flush_buffer()

        except Exception as e:
            log.error(f"[{self.platform}] Loi scrape tong the: {e}", exc_info=True)
            self._flush_buffer()

        return all_records

    def _flush_buffer(self) -> None:
        if self.records_buffer:
            write_records(self.records_buffer, CSV_OUTPUT)
            self.records_buffer.clear()

    def _scrape_single_date(self, browser, checkin_date: date, los: int) -> list[dict]:
        checkout_date = checkin_date + timedelta(days=los)
        d_param = f"{checkin_date.strftime('%d%m%Y')}_{checkout_date.strftime('%d%m%Y')}"
        url = f"{self.base_url}?d={d_param}"
        
        log.info(f"[{self.platform}] Dang quet: {checkin_date} -> {checkout_date} | LOS={los}")
        records = []
        api_data_prices = []
        
        def handle_response(response):
            if response.request.method == 'POST' and 'SearchHotelPrices' in response.url:
                try:
                    api_data_prices.append(response.json())
                except:
                    pass

        try:
            page = browser.new_page(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page.on("response", handle_response)
            
            page.goto(url, timeout=60000, wait_until="networkidle")
            page.wait_for_timeout(8000) # Wait for APIs
            
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            hotel_elements = soup.find_all(class_='pdv__hotel--name')
            for el in hotel_elements:
                hotel_name = el.get_text(strip=True)
                
                # Check near DOM elements
                parent = el.parent
                price_text = None
                for _ in range(5):
                    if not parent:
                        break
                    price_el = parent.find(class_=lambda c: c and 'price' in str(c).lower())
                    if price_el:
                        price_text = price_el.get_text(strip=True)
                        break
                    parent = parent.parent
                
                price = self._parse_price(price_text) if price_text else 0.0
                
                # Default room type
                room_type = "RT01" # Standard
                
                rec = make_record(
                    platform=self.platform,
                    hotel_name=hotel_name,
                    checkin=checkin_date,
                    checkout=checkout_date,
                    status="success"
                )
                rec["mapped_room_type_id"] = room_type
                if price > 0:
                    rec["listed_price_vnd"] = price
                    rec["discounted_price_vnd"] = price
                    rec["is_sold_out"] = "false"
                else:
                    rec["is_sold_out"] = "true"
                
                records.append(rec)
            
            log.info(f"  [{self.platform}] Tim thay {len(records)} khach san.")
            page.close()
            
        except Exception as e:
            log.error(f"  [{self.platform}] Loi o {checkin_date}: {e}")
            
        return records

    def _parse_price(self, price_str: str) -> float:
        if not price_str:
            return 0.0
        digits = ''.join(c for c in price_str if c.isdigit())
        if digits:
            return float(digits)
        return 0.0
