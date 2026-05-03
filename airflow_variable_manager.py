from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime


def get_and_update_variable():
    # Get the existing value of the variable
    var_name = 'my_variable_token'
    existing_value = Variable.get(var_name, default_var='default_value')
    
    # Update the variable value
    new_value = existing_value + '_updated'
    Variable.set(var_name, new_value)


# Define the DAG
with DAG(
    dag_id='airflow_variable_manager',
    schedule='@daily',
    start_date=datetime(2026, 5, 1),
) as dag:

    update_variable_task = PythonOperator(
        task_id='update_variable',
        python_callable=get_and_update_variable,
    )
