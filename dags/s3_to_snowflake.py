import csv
import logging
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.models import Variable


S3_CONN_ID = 'snowflake'
S3_BUCKET = Variable.get('s3_bucket_data')
AWS_KEY_ID = Variable.get('AWS_KEY_ID')
AWS_SECRET_KEY = Variable.get('AWS_SECRET_KEY')
DBT_CLOUD_CONN_ID = "dbt_cloud"
DBT_JOB_ID = Variable.get('DBT_JOB_ID')

# def upload_to_s3(endpoint, date):
#     # Instanstiate
#     s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

#     # Base URL
#     url = 'https://covidtracking.com/api/v1/states/'
    
#     # Grab data
#     res = requests.get(url+'{0}/{1}.csv'.format(endpoint, date))

#     # Take string, upload to S3 using predefined method
#     s3_hook.load_string(res.text, '{0}_{1}.csv'.format(endpoint, date), bucket_name=BUCKET, replace=True)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

endpoints_1 = ['products','orders']
endpoints_2 = ['prior', 'train']

with DAG('data_s3_to_snowflake',
         start_date=datetime(2022, 8, 7),
         schedule_interval='@once',
         default_args=default_args,
         template_searchpath='/usr/local/airflow/include',
         catchup=False
         ) as dag:

    begin = DummyOperator(task_id='begin')
    end = DummyOperator(task_id='end')

    create_source_tables = SnowflakeOperator(
        task_id='create_source_tables',
        snowflake_conn_id=S3_CONN_ID,
        sql='create_snowflake_table.sql',
    ) 

    create_file_format = SnowflakeOperator(
        task_id='create_file_format',
        snowflake_conn_id=S3_CONN_ID,
        sql='create_file_format.sql',
    )

    create_stage = SnowflakeOperator(
        task_id='create_stage',
        snowflake_conn_id=S3_CONN_ID,
        sql='create_snowflake_stage.sql',
        params={
            "s3_bucket": S3_BUCKET,
            "login": AWS_KEY_ID,
            "password": AWS_SECRET_KEY,
        },
    )

    # pivot_data = SnowflakeOperator(
    #     task_id='call_pivot_sproc',
    #     snowflake_conn_id='snowflake',
    #     sql='call pivot_state_data();',
    #     role='KENTEND',
    #     schema='SANDBOX_KENTEND'
    # ) 

    # load_to_snowflake = SnowflakeOperator(
    #     task_id='insert_into_table',
    #     snowflake_conn_id='snowflake',
    #     sql='load_to_snowflake.sql'
    # )

    task_list_1 = []
    for endpoint in endpoints_1:
        load_to_snowflake_1 = S3ToSnowflakeOperator(
            task_id='upload_{0}_snowflake'.format(endpoint),
            s3_keys=['{0}/{0}.csv'.format(endpoint)],
            stage='csv_stage',
            table='{0}'.format(endpoint),
            schema='rawdata',
            file_format='csv_format',
            role='SYSADMIN',
            snowflake_conn_id=S3_CONN_ID,
            )
        task_list_1.append(load_to_snowflake_1)    

    task_list_2 = []
    for endpoint in endpoints_2:
        load_to_snowflake_2 = S3ToSnowflakeOperator(
            task_id='upload_order_products_{0}_snowflake'.format(endpoint),
            s3_keys=['order_products/order_products__{0}.csv.gz'.format(endpoint)],
            stage='csv_stage',
            table='order_products_{0}'.format(endpoint),
            schema='rawdata',
            file_format='csv_format',
            role='SYSADMIN',
            snowflake_conn_id=S3_CONN_ID,
            )
        task_list_2.append(load_to_snowflake_2) 

    trigger_dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_run",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=DBT_JOB_ID,
        check_interval=10,
        timeout=300,
    )

    # for endpoint in endpoints:
    #     # generate_files = PythonOperator(
    #     #     task_id='generate_file_{0}'.format(endpoint),
    #     #     python_callable=upload_to_s3,
    #     #     op_kwargs={'endpoint': endpoint, 'date': date}
    #     # )
    
    #     load_to_snowflake = S3ToSnowflakeOperator(
    #         task_id='upload_{0}_snowflake'.format(endpoint[1]),
    #         s3_keys=['/{0}/{1}.csv'.format(endpoint[0], endpoint[1])],
    #         stage='csv_stage',
    #         table='{0}'.format(endpoint[1]),
    #         schema='rawdata',
    #         file_format='csv_format',
    #         role='SYSADMIN',
    #         snowflake_conn_id='snowflake',
    #     )


    # chain(
    #     begin,
    #     create_source_tables,
    #     create_file_format,
    #     create_stage,
    #     load_to_snowflake,
    #     end,
    # )

    chain(
        begin,
        create_source_tables,
        create_file_format,
        create_stage,
        *task_list_1,
        *task_list_2,
        trigger_dbt_cloud_job_run,
        end,
    )