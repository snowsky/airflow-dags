#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.mysql_operator import MySqlOperator

with DAG(
    dag_id="example_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="echo 'Hello from the container, run_this_last!' >> /source/test.txt",
    )
    # [END howto_operator_bash]

    run_this >> run_this_last

    for i in range(3):
        task = BashOperator(
            task_id=f"runme_{i}",
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this

    # [START howto_operator_bash_template]
    also_run_this_1 = BashOperator(
        task_id="also_run_this_1",
        bash_command="echo 'Hello from the container, run1, also_run_this!' >> /source-s3-konzaandssigrouppipelines/test.txt; sleep 60",
    )
    # [END howto_operator_bash_template]
    also_run_this_1 >> run_this_last

    also_run_this_2 = BashOperator(
        task_id="also_run_this_2",
        bash_command="echo 'Hello from the container, run2, also_run_this!' >> /source-s3-konzaandssigroupqa/test.txt; sleep 60",
    )
    also_run_this_2 >> run_this_last

    also_run_this_3 = BashOperator(
        task_id="also_run_this_3",
        bash_command="echo 'Hello from the container, run3, also_run_this!' >> /source-s3-konzaandssigroup/test.txt; sleep 60",
    )
    also_run_this_3 >> run_this_last

    create_sql_query = """ CREATE TABLE dezyre.employee(empid int, empname VARCHAR(25), salary int); """
    test_mysql = MySqlOperator(sql=create_sql_query, 
        task_id="test_mysql",
        mysql_conn_id="prd_az1_connection"
    )
    test_mysql >> run_this_last

    also_run_this_email = EmailOperator(
       task_id="also_run_this_email",
       to='hao.1.wang@gmail.com',
       subject='Alert Mail',
       html_content=""" Mail Test """,
    )
    also_run_this_email >> run_this_last

# [START howto_operator_bash_skip]
this_will_skip = BashOperator(
    task_id="this_will_skip",
    bash_command='echo "hello world"; exit 99;',
    dag=dag,
)
# [END howto_operator_bash_skip]
this_will_skip >> run_this_last

if __name__ == "__main__":
    dag.test()
