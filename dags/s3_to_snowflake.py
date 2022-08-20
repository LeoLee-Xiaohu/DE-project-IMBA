from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.models import Variable


SNOWFLAKE_CONN_ID = 'snowflake'
S3_BUCKET = Variable.get('s3_bucket_data')
S3_BUCKET_NAME = Variable.get('s3_bucket_name')
AWS_KEY_ID = Variable.get('AWS_KEY_ID')
AWS_SECRET_KEY = Variable.get('AWS_SECRET_KEY')
DBT_CLOUD_CONN_ID = "dbt_cloud"
DBT_JOB_ID = Variable.get('DBT_JOB_ID')

today = date.today()

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
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='create_snowflake_table.sql',
    ) 

    create_file_format = SnowflakeOperator(
        task_id='create_file_format',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='create_file_format.sql',
    )

    create_stage = SnowflakeOperator(
        task_id='create_stage',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='create_snowflake_stage.sql',
        params={
            "s3_bucket": S3_BUCKET,
            "login": AWS_KEY_ID,
            "password": AWS_SECRET_KEY,
        },
    )

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
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
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
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            )
        task_list_2.append(load_to_snowflake_2) 

    trigger_dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_run",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=DBT_JOB_ID,
        check_interval=10,
        timeout=300,
    )

    unload_to_s3 = SnowflakeOperator(
        task_id='unload_to_s3',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql='unload_to_s3.sql',
        params={
            "s3_bucket": S3_BUCKET_NAME,
            "login": AWS_KEY_ID,
            "password": AWS_SECRET_KEY,
            "today_year": today.year,
            "today_month": today.month,
            "today_day": today.day,
        },
    )

    chain(
        begin,
        create_source_tables,
        create_file_format,
        create_stage,
        *task_list_1,
        *task_list_2,
        trigger_dbt_cloud_job_run,
        unload_to_s3,
        end,
    )