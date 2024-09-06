from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.utils.dates import days_ago, timedelta

default_args = {
    'owner' : 'airflow',
    'depend_on_past' : False,
    'start_date' : days_ago(2),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

with DAG(
    'mysqlConnTest',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False) as dag:

    
    start_date = DummyOperator(task_id = "start_task")
   
    
    # [START howto_operator_mysql]
    select_table_mysql_task = MySqlOperator(
        task_id='select_table_mysql', mysql_conn_id='mysql', sql="SELECT * FROM country;", autocommit=True, parameters= {'ssl_mode': 'ENABLED'}
    )



    start_date >> select_table_mysql_task
