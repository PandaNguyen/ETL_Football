"""
Airflow DAG để chạy ETL Football vào thứ 4 hàng tuần
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import sys
from pathlib import Path

# Thêm thư mục scr vào Python path
BASE_DIR = Path(__file__).parent.parent.absolute()
SCR_DIR = BASE_DIR / "scr"
sys.path.insert(0, str(SCR_DIR))

# Import các module ETL
from Extract import main as extract_main
from Transform import (
    create_dim_player,
    create_dim_team,
    create_dim_stadium,
    create_fact_team_match,
    create_fact_player_match,
    create_fact_team_point
)
from Load import (
    load_config,
    connect,
    create_and_load_dim_stadium,
    create_and_load_dim_team,
    create_and_load_dim_match,
    create_and_load_dim_player,
    create_and_load_dim_season,
    create_and_load_fact_team_match,
    create_and_load_fact_player_match,
    create_and_load_fact_team_point
)

# Thiết lập BASE_DIR cho các script
os.environ["ETL_FOOTBALL_BASE_DIR"] = str(BASE_DIR)

# Default arguments cho DAG
default_args = {
    'owner': 'football_etl',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa DAG
dag = DAG(
    'football_etl_weekly',
    default_args=default_args,
    description='ETL Football Pipeline - Chạy vào thứ 4 hàng tuần',
    schedule_interval='0 2 * * 3',  # 2:00 AM mỗi thứ 4 (0 2 * * 3 = Wednesday 2 AM)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'football', 'postgresql'],
)


def extract_task():
    """Task để extract dữ liệu từ các nguồn"""
    print("Starting Extract process...")
    extract_main()
    print("Extract process completed!")


def transform_task():
    """Task để transform dữ liệu"""
    print("Starting Transform process...")
    
    # Tạo dimension tables
    create_dim_player()
    create_dim_team()
    create_dim_stadium()
    
    # Tạo fact tables (dựa vào dimension tables)
    create_fact_team_match()
    create_fact_player_match()
    create_fact_team_point()
    
    print("Transform process completed!")


def load_task():
    """Task để load dữ liệu vào PostgreSQL"""
    print("Starting Load process...")
    
    # Đảm bảo database.ini có đường dẫn đúng
    database_ini_path = os.path.join(SCR_DIR, 'database.ini')
    if not os.path.exists(database_ini_path):
        raise FileNotFoundError(f"Database config file not found: {database_ini_path}")
    
    # Load config và kết nối database
    # Tạm thời thay đổi working directory để load_config tìm được file
    original_cwd = os.getcwd()
    try:
        os.chdir(SCR_DIR)
        config = load_config()
    finally:
        os.chdir(original_cwd)
    
    conn = connect(config)
    
    if conn is None:
        raise Exception("Cannot connect to PostgreSQL database")
    
    conn.autocommit = True
    cursor = conn.cursor()
    
    DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data_processed")
    
    try:
        # Load dimension tables trước (vì fact tables có foreign keys)
        create_and_load_dim_stadium(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_stadium.csv'))
        create_and_load_dim_team(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
        create_and_load_dim_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_match.csv'))
        create_and_load_dim_player(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_player.csv'))
        create_and_load_dim_season(cursor, os.path.join(DATA_PROCESSED_DIR, 'dim_season.csv'))
        
        # Load fact tables sau
        create_and_load_fact_team_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_team_match_clean.csv'))
        create_and_load_fact_player_match(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_player_match_clean.csv'))
        create_and_load_fact_team_point(cursor, os.path.join(DATA_PROCESSED_DIR, 'fact_team_point.csv'))
        
        print("Load process completed!")
    finally:
        # Đóng kết nối
        cursor.close()
        conn.close()


# Định nghĩa các tasks
extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_task,
    dag=dag,
)

# Định nghĩa dependencies: extract -> transform -> load
extract >> transform >> load

