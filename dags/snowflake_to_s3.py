from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from contextlib import closing
from airflow.models import Variable
from airflow import models
import pandas as pd
import time
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator 

default_args = {
    'owner': 'Datexland',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 14),
    'email': ['alexbonella2806@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

# queries
query_test = """select current_date() as date;"""
query = """select * from PRUEBAS3.PUBLIC.RADIATION limit 1000;"""

def execute_snowflake(sql, snowflake_conn_id, with_cursor=False):
    """Execute snowflake query."""
    hook_connection = SnowflakeHook(
        snowflake_conn_id=snowflake_conn_id
    )

    with closing(hook_connection.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute(sql)
            res = cur.fetchall()
            if with_cursor:
                return (res, cur)
            else:
                return res

def snowflake_to_pandas(query, snowflake_conn_id,**kwargs): 
    """Convert snowflake list to df."""
    result, cursor = execute_snowflake(query, snowflake_conn_id, True)
    headers = list(map(lambda t: t[0], cursor.description))
    df = pd.DataFrame(result)
    df.columns = headers

    # save file before to send 
    
    # NOTE : This is not recommended in case of multi-worker deployments
    df.to_csv('/usr/local/airflow/data.csv',header=True,mode='w',sep=',')
    
   
    
    # Send to S3
    
    hook = S3Hook(aws_conn_id="s3_conn")
    hook.load_file(
        filename='/usr/local/airflow/data.csv',
        key='radiations_new.csv',
        bucket_name=Variable.get("BUCKET_NAME"),
        replace=True,
    )

    return 'This File Sent Successfully'

dag = DAG(
    'SNOWFLAKE_TO_S3',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False
)
 
# Connection Test
snowflake = SnowflakeOperator(
    task_id='test_snowflake_connection',
    sql=query_test,
    snowflake_conn_id='snow',
    dag=dag
)

# Data Upload  Task 
upload_stage = PythonOperator(task_id='UPLOAD_FILE_INTO_S3_BUCKET',     
                             python_callable=snowflake_to_pandas,
                             op_kwargs={"query":query,"snowflake_conn_id":'snow'},     
                             dag=dag )


snowflake >> upload_stage 