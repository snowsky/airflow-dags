from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_sftp(**kwargs):
    hook = SFTPHook(ssh_conn_id="my_sftp")
    print("Listing remote directory:")
    print(hook.list_directory("/"))

with DAG(
    "sftp_ed25519_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    PythonOperator(
        task_id="test_sftp_conn",
        python_callable=test_sftp,
    )
