from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator




default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 5, 23,6,00),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('Entry_to_Postgres_DB',default_args=default_args,schedule_interval="0 6 * * *", template_searchpath=['/usr/local/airflow/sql_files'], catchup=False) as dag:

    
    t0 = PostgresOperator(task_id='create_table', postgres_conn_id ="postgres_conn", sql="create_table.sql")
    t1 = PostgresOperator(task_id='insert_to_table', postgres_conn_id ="postgres_conn", sql="insert_table.sql")

    t0 >> t1

    
