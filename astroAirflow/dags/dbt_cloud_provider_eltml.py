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

from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

# https://cloud.getdbt.com/#/accounts/95357/projects/147695/jobs/118719/
# dbt cloud project id and job id 

with DAG(
    dag_id="dbt_cloud_provider_eltml",
    description='The DAG of project imba elt',
    start_date=datetime(2022, 8, 8),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    ml_training = DummyOperator(task_id="ml_training")

    trigger_dbt_cloud_job_imba_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_imba_run", # triggle DBT job: generate imba final data
        dbt_cloud_conn_id= "dbt_cloud", 
        account_id= 95357,
        job_id=118719,
        check_interval=10,
        timeout=300,
    )

    unload_snowflake_staging_data = SnowflakeOperator(
        task_id="unload_snowflake_to_s3_ext_staging",
        sql="./snowflake/unload_snowf_to_s3.sql",
        snowflake_conn_id = 'snowflake',
    )

    trigger_dbt_cloud_job_imba_run >> unload_snowflake_staging_data >> ml_training
