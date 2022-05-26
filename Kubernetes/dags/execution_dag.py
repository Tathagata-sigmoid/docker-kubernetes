from airflow import DAG
from datetime import datetime, timedelta
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


defalut_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 3, 30),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
with DAG("Execution_Dag",default_args=defalut_args, schedule_interval='* 6 * * *', template_searchpath=['/mnt/airflow/dags'],catchup=False) as dag:
    t1 = PostgresOperator(task_id="create_new_table",postgres_conn_id='postgres_conn',sql="create_new_table.sql")
    t1