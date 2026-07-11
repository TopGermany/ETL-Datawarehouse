"""
scrapers/
=========
Package chứa các module scraper. Hiện tại tập trung 100% vào Booking.com.
"""

from .base import (
    ROOM_TYPE_MAPPING,
    PEAK_MONTHS,
    CSV_OUTPUT,
    CSV_HEADERS,
    make_record,
    parse_price,
    map_room_type,
    build_peak_dates,
    write_records,
)

from .booking_scraper import BookingScraper

__all__ = [
    "BookingScraper",
    "ROOM_TYPE_MAPPING",
    "PEAK_MONTHS",
    "CSV_OUTPUT",
    "CSV_HEADERS",
    "make_record",
    "parse_price",
    "map_room_type",
    "build_peak_dates",
    "write_records",
]
