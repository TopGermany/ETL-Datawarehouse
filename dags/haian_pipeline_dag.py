from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Rollback commands (nếu ADK agent lỗi, khôi phục 2 task cũ):
# python /opt/airflow/agents/competitor_scraper_agent.py --run-now
# python /opt/airflow/agents/etl_agent.py --run-now

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
    tags=['haian', 'scraper', 'etl', 'agent']
) as dag:

    # Agent task: LLM tự lên kế hoạch scrape → DQ → sync → mart
    task_run_agent = BashOperator(
        task_id='run_haian_dwh_agent',
        bash_command='cd /opt/airflow && python -m agents.haian_dwh_agent.cli --goal daily_competitor_pipeline --live'
    )
