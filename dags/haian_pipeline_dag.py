from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Định nghĩa các thông số cơ bản cho kịch bản
default_args = {
    'owner': 'Haian_IT',
    'retries': 3, # Thử lại tối đa 3 lần nếu có lỗi
    'retry_delay': timedelta(minutes=15), # Mỗi lần thử lại cách nhau 15 phút
    'email_on_failure': False, # Chưa setup email Airflow nên tắt tạm
}

with DAG(
    dag_id='haian_competitor_price_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *', # Chạy lúc 2h00 sáng mỗi ngày
    start_date=datetime(2026, 6, 25),
    catchup=False,
    tags=['haian', 'scraper', 'etl']
) as dag:

    # Task 1: Bật Scraper Cào dữ liệu
    task_scrape_data = BashOperator(
        task_id='run_competitor_scraper',
        bash_command='python /opt/airflow/agents/competitor_scraper_agent.py --run-now'
    )

    # Task 2: Bật ETL (Lọc rác & Đẩy lên BigQuery)
    task_etl_bq = BashOperator(
        task_id='run_etl_to_bigquery',
        bash_command='python /opt/airflow/agents/etl_agent.py --run-now'
    )

    # Thiết lập thứ tự: Cào thành công mới được ETL
    task_scrape_data >> task_etl_bq
