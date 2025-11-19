from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime

def test_sftp_connection(**kwargs):
    logger = LoggingMixin().log
    params = kwargs.get('params', {})
    sftp_conn_id = params.get('SFTP_CONN_ID', 'default_conn_id')
    hook = SFTPHook(ssh_conn_id=sftp_conn_id)
    try:
        hook.get_conn()
        logger.info(f"✅ SFTP connection successful using conn_id: {sftp_conn_id}")
    except Exception as e:
        logger.error(f"❌ SFTP connection failed for conn_id {sftp_conn_id}: {e}", exc_info=True)
        raise

with DAG(
    dag_id='test_sftp_connection_from_conn_id',
    start_date=datetime(2025, 10, 8),
    catchup=False,
    params={
        'SFTP_CONN_ID': 'sftp' #default value
    }
) as dag:

    test_connection = PythonOperator(
        task_id='test_sftp_connection',
        python_callable=test_sftp_connection,
    )
