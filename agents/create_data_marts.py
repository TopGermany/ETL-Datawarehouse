import os
import sys
import logging
from google.cloud import bigquery
from google.oauth2 import service_account

# Cấu hình log
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

PROJECT_ID = "haian-dwh-project"
DATASET_ID = "haian_dwh"
CREDENTIALS_PATH = "D:/Dự Án/haian_dwh_project/agents/haian-dwh-project-2254500c760c.json"

def create_marts():
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    client = bigquery.Client(credentials=creds, project=PROJECT_ID)

    # 1. View: mart_cost_analysis_daily (Dùng cho Biểu đồ Cột ngang - Phân tích hạng mục chi phí)
    sql_cost_analysis = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.mart_cost_analysis_daily` AS
    SELECT
        d.full_date AS stay_date,
        c.cost_name AS category_name,
        c.department,
        c.is_fixed_cost,
        f.cost_amount AS amount
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_room_cost_daily` f
    JOIN `{PROJECT_ID}.{DATASET_ID}.dim_date` d ON f.date_key = d.date_key
    JOIN `{PROJECT_ID}.{DATASET_ID}.dim_cost_category` c ON f.cost_category_id = c.cost_category_id
    """

    # 2. View: mart_profitpar_daily (Dùng cho Thẻ điểm KPI & Thác nước)
    sql_profitpar = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.mart_profitpar_daily` AS
    WITH daily_revenue AS (
        SELECT
            date_key,
            SUM(rooms_sold) AS total_rooms_sold,
            SUM(net_room_revenue) AS total_net_revenue,
            SUM(gross_room_revenue) AS total_gross_revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_room_revenue_daily`
        GROUP BY date_key
    ),
    daily_cost AS (
        SELECT
            date_key,
            SUM(cost_amount) AS total_cost
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_room_cost_daily`
        GROUP BY date_key
    ),
    daily_inventory AS (
        SELECT
            date_key,
            SUM(sellable_rooms) AS total_available_rooms
        FROM `{PROJECT_ID}.{DATASET_ID}.fact_room_inventory_daily`
        GROUP BY date_key
    )
    SELECT
        d.full_date AS stay_date,
        COALESCE(i.total_available_rooms, 0) AS total_available_rooms,
        COALESCE(r.total_rooms_sold, 0) AS total_rooms_sold,
        COALESCE(r.total_net_revenue, 0) AS total_net_revenue,
        COALESCE(r.total_gross_revenue, 0) AS total_gross_revenue,
        COALESCE(c.total_cost, 0) AS total_cost,
        (COALESCE(r.total_net_revenue, 0) - COALESCE(c.total_cost, 0)) AS net_profit,
        IF(COALESCE(r.total_net_revenue, 0) > 0, ROUND((COALESCE(r.total_net_revenue, 0) - COALESCE(c.total_cost, 0)) / r.total_net_revenue * 100, 2), 0) AS profit_margin_pct,
        IF(COALESCE(i.total_available_rooms, 0) > 0, ROUND((COALESCE(r.total_net_revenue, 0) - COALESCE(c.total_cost, 0)) / i.total_available_rooms, 2), 0) AS profitpar,
        IF(COALESCE(i.total_available_rooms, 0) > 0, ROUND(COALESCE(r.total_net_revenue, 0) / i.total_available_rooms, 2), 0) AS revpar,
        IF(COALESCE(r.total_rooms_sold, 0) > 0, ROUND(COALESCE(r.total_net_revenue, 0) / r.total_rooms_sold, 2), 0) AS adr
    FROM `{PROJECT_ID}.{DATASET_ID}.dim_date` d
    LEFT JOIN daily_revenue r ON d.date_key = r.date_key
    LEFT JOIN daily_cost c ON d.date_key = c.date_key
    LEFT JOIN daily_inventory i ON d.date_key = i.date_key
    WHERE i.total_available_rooms IS NOT NULL OR r.total_net_revenue IS NOT NULL
    """

    # 3. View: mart_competitor_pricing_summary
    sql_summary = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.mart_competitor_pricing_summary` AS
    WITH latest_snapshots AS (
        -- Chỉ lấy mức giá cào được gần nhất cho mỗi ngày check-in của mỗi đối thủ (vì ngày check-in càng gần thì giá càng biến động)
        SELECT *
        FROM (
            SELECT f.*,
                   h.star_rating,
                   ROW_NUMBER() OVER(
                       PARTITION BY f.checkin_date_key, f.competitor_hotel_name, f.source_platform, f.room_type_raw
                       ORDER BY f.snapshot_datetime DESC
                   ) as rn
            FROM `{PROJECT_ID}.{DATASET_ID}.fact_competitor_price_snapshot_template` f
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_hotel` h ON CAST(f.hotel_id AS STRING) = CAST(h.hotel_id AS STRING)
        )
        WHERE rn = 1
    )
    SELECT
        CAST(checkin_date AS DATE) as checkin_date,
        source_platform,
        hotel_id,
        competitor_hotel_name,
        star_rating,
        mapped_room_type_id,
        ROUND(AVG(discounted_price_vnd), 0) as avg_discounted_price_vnd,
        MIN(discounted_price_vnd) as min_price_vnd,
        MAX(discounted_price_vnd) as max_price_vnd,
        COUNTIF(is_sold_out = 'true') as sold_out_count,
        ROUND(AVG(rating_score), 1) as avg_rating
    FROM latest_snapshots
    GROUP BY 1, 2, 3, 4, 5, 6
    """

    # 2. View: mart_haian_vs_market_price
    sql_comparison = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.mart_haian_vs_market_price` AS
    WITH market_avg AS (
        SELECT 
            checkin_date,
            ROUND(AVG(avg_discounted_price_vnd), 0) as market_avg_price_vnd
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_competitor_pricing_summary`
        GROUP BY checkin_date
    ),
    haian_data AS (
        SELECT 
            CAST(stay_date AS DATE) as stay_date,
            adr as haian_adr
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_profitpar_daily`
    )
    SELECT 
        m.checkin_date as date,
        h.haian_adr,
        m.market_avg_price_vnd,
        -- Tránh chia cho 0
        IF(m.market_avg_price_vnd > 0, ROUND((h.haian_adr / m.market_avg_price_vnd) * 100, 2), NULL) as price_index
    FROM market_avg m
    LEFT JOIN haian_data h ON m.checkin_date = h.stay_date
    """

    logging.info("Tạo view mart_cost_analysis_daily...")
    client.query(sql_cost_analysis).result()
    logging.info("✅ Tạo thành công mart_cost_analysis_daily!")

    logging.info("Xóa bảng mart_profitpar_daily (nếu đang là physical table)...")
    try:
        client.query(f"DROP TABLE IF EXISTS `{PROJECT_ID}.{DATASET_ID}.mart_profitpar_daily`").result()
    except Exception as e:
        logging.warning(f"Bỏ qua DROP TABLE (có thể nó đã là VIEW): {e}")

    logging.info("Tạo view mart_profitpar_daily...")
    client.query(sql_profitpar).result()
    logging.info("✅ Tạo thành công mart_profitpar_daily!")

    logging.info("Tạo view mart_competitor_pricing_summary...")
    client.query(sql_summary).result()
    logging.info("✅ Tạo thành công mart_competitor_pricing_summary!")

    logging.info("Tạo view mart_haian_vs_market_price...")
    client.query(sql_comparison).result()
    logging.info("✅ Tạo thành công mart_haian_vs_market_price!")

if __name__ == "__main__":
    create_marts()
