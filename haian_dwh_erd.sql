-- ==============================================================================
-- HAIAN DATA WAREHOUSE - ENTITY RELATIONSHIP DIAGRAM (ERD) SCRIPT
-- ==============================================================================
-- File SQL này dùng để mô tả cấu trúc ERD của dự án. 
-- Bạn có thể copy toàn bộ code này dán vào các trang web vẽ ERD tự động 
-- (như https://dbdiagram.io/ hoặc MySQL Workbench) để xem sơ đồ trực quan.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. DIMENSION TABLES (Bảng Chiều - Dữ liệu danh mục gốc)
-- ------------------------------------------------------------------------------

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    day_of_week VARCHAR(20),
    is_weekend BOOLEAN,
    month INT,
    quarter INT,
    year INT,
    season VARCHAR(50),
    is_holiday BOOLEAN,
    holiday_name VARCHAR(255)
);

CREATE TABLE dim_hotel (
    hotel_id INT PRIMARY KEY,
    hotel_name VARCHAR(255),
    star_rating INT,
    address VARCHAR(500),
    city VARCHAR(100),
    total_rooms INT
);

CREATE TABLE dim_room_type (
    room_type_id INT PRIMARY KEY,
    room_type_name VARCHAR(255),
    bed_type VARCHAR(100),
    view_type VARCHAR(100),
    max_occupancy INT,
    room_size_sqm DECIMAL(10,2)
);

CREATE TABLE dim_channel (
    channel_id INT PRIMARY KEY,
    channel_name VARCHAR(100),
    channel_category VARCHAR(50) -- Ví dụ: OTA, Direct, TA, Corporate
);

CREATE TABLE dim_guest_segment (
    segment_id INT PRIMARY KEY,
    segment_name VARCHAR(100),
    segment_group VARCHAR(100) -- Ví dụ: FIT, GIT, MICE
);

CREATE TABLE dim_cost_category (
    cost_category_id INT PRIMARY KEY,
    cost_name VARCHAR(255),
    is_fixed_cost BOOLEAN,
    department VARCHAR(100)
);

CREATE TABLE dim_promotion (
    promotion_id INT PRIMARY KEY,
    promotion_name VARCHAR(255),
    discount_type VARCHAR(50),
    discount_value DECIMAL(10,2)
);

-- ------------------------------------------------------------------------------
-- 2. FACT TABLES (Bảng Sự Kiện - Dữ liệu giao dịch/hằng ngày)
-- ------------------------------------------------------------------------------

CREATE TABLE fact_booking (
    booking_id VARCHAR(100) PRIMARY KEY,
    room_type_id INT,
    channel_id INT,
    segment_id INT,
    promotion_id INT,
    booking_date DATE,
    check_in_date DATE,
    check_out_date DATE,
    lead_time_days INT,
    length_of_stay INT,
    status VARCHAR(50)
);

CREATE TABLE fact_room_inventory_daily (
    date_key INT,
    room_type_id INT,
    total_rooms_available INT,
    out_of_order_rooms INT,
    sellable_rooms INT,
    PRIMARY KEY (date_key, room_type_id)
);

CREATE TABLE fact_room_cost_daily (
    date_key INT,
    cost_category_id INT,
    cost_amount DECIMAL(15,2),
    PRIMARY KEY (date_key, cost_category_id)
);

CREATE TABLE fact_distribution_cost_daily (
    distribution_cost_id VARCHAR(100) PRIMARY KEY,
    date_key INT,
    stay_date DATE,
    channel_id INT,
    rooms_sold_via_channel INT,
    gross_revenue_via_channel DECIMAL(15,2),
    commission_amount DECIMAL(15,2),
    promotion_cost DECIMAL(15,2),
    marketing_spend DECIMAL(15,2),
    total_distribution_cost DECIMAL(15,2),
    created_at TIMESTAMP
);

CREATE TABLE fact_room_revenue_daily (
    room_revenue_id VARCHAR(100) PRIMARY KEY,
    date_key INT,
    room_type_id INT,
    channel_id INT,
    segment_id INT,
    promotion_id INT,
    rooms_sold INT,
    gross_room_revenue DECIMAL(15,2),
    listed_room_revenue DECIMAL(15,2),
    net_room_revenue DECIMAL(15,2)
);

CREATE TABLE fact_guest_review (
    booking_id VARCHAR(100),
    review_date DATE,
    score DECIMAL(3,1),
    review_title VARCHAR(500),
    review_content TEXT,
    PRIMARY KEY (booking_id, review_date)
);

CREATE TABLE fact_competitor_price_snapshot_template (
    snapshot_id VARCHAR(100) PRIMARY KEY,
    snapshot_datetime TIMESTAMP,
    checkin_date DATE,
    checkin_date_key INT,
    checkout_date DATE,
    search_los INT,
    source_platform VARCHAR(100),
    hotel_id INT,
    competitor_hotel_name VARCHAR(255),
    hotel_link TEXT,
    location_area VARCHAR(255),
    room_type_raw VARCHAR(255),
    mapped_room_type_id VARCHAR(50),
    listed_price_vnd DECIMAL(15,2),
    discounted_price_vnd DECIMAL(15,2),
    is_sold_out BOOLEAN,
    rating_score VARCHAR(50),
    scrape_status VARCHAR(50)
);

-- ------------------------------------------------------------------------------
-- 3. FOREIGN KEY CONSTRAINTS (Các Ràng buộc Khoá Ngoại - Mối quan hệ)
-- ------------------------------------------------------------------------------

-- Ràng buộc cho fact_booking
ALTER TABLE fact_booking ADD FOREIGN KEY (room_type_id) REFERENCES dim_room_type (room_type_id);
ALTER TABLE fact_booking ADD FOREIGN KEY (channel_id) REFERENCES dim_channel (channel_id);
ALTER TABLE fact_booking ADD FOREIGN KEY (segment_id) REFERENCES dim_guest_segment (segment_id);
ALTER TABLE fact_booking ADD FOREIGN KEY (promotion_id) REFERENCES dim_promotion (promotion_id);

-- Ràng buộc cho fact_room_inventory_daily
ALTER TABLE fact_room_inventory_daily ADD FOREIGN KEY (date_key) REFERENCES dim_date (date_key);
ALTER TABLE fact_room_inventory_daily ADD FOREIGN KEY (room_type_id) REFERENCES dim_room_type (room_type_id);

-- Ràng buộc cho fact_room_cost_daily
ALTER TABLE fact_room_cost_daily ADD FOREIGN KEY (date_key) REFERENCES dim_date (date_key);
ALTER TABLE fact_room_cost_daily ADD FOREIGN KEY (cost_category_id) REFERENCES dim_cost_category (cost_category_id);

-- Ràng buộc cho fact_room_revenue_daily
ALTER TABLE fact_room_revenue_daily ADD FOREIGN KEY (date_key) REFERENCES dim_date (date_key);
ALTER TABLE fact_room_revenue_daily ADD FOREIGN KEY (room_type_id) REFERENCES dim_room_type (room_type_id);
ALTER TABLE fact_room_revenue_daily ADD FOREIGN KEY (channel_id) REFERENCES dim_channel (channel_id);
ALTER TABLE fact_room_revenue_daily ADD FOREIGN KEY (segment_id) REFERENCES dim_guest_segment (segment_id);
ALTER TABLE fact_room_revenue_daily ADD FOREIGN KEY (promotion_id) REFERENCES dim_promotion (promotion_id);

-- Ràng buộc cho fact_guest_review
ALTER TABLE fact_guest_review ADD FOREIGN KEY (booking_id) REFERENCES fact_booking (booking_id);

-- Ràng buộc cho fact_distribution_cost_daily
ALTER TABLE fact_distribution_cost_daily ADD FOREIGN KEY (date_key) REFERENCES dim_date (date_key);
ALTER TABLE fact_distribution_cost_daily ADD FOREIGN KEY (channel_id) REFERENCES dim_channel (channel_id);

-- Ràng buộc cho fact_competitor_price_snapshot_template
ALTER TABLE fact_competitor_price_snapshot_template ADD FOREIGN KEY (hotel_id) REFERENCES dim_hotel (hotel_id);
ALTER TABLE fact_competitor_price_snapshot_template ADD FOREIGN KEY (checkin_date_key) REFERENCES dim_date (date_key);
