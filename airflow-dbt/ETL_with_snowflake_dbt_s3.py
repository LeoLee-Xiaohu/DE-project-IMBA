import csv
import logging
import pandas as pd
from datetime import datetime, timedelta, date
from pyspark.sql import SparkSession

from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import DAG, Variable


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

AWS_KEY_ID = Variable.get('AWS_KEY_ID')
AWS_SECRET_KEY = Variable.get('AWS_SECRET_KEY')

sql_stage = f"create or replace stage raw.csv_stage \
                file_format = csvformat \
                url = 's3://imba-johnny/data' \
                credentials=(aws_key_id='{AWS_KEY_ID}' aws_secret_key='{AWS_SECRET_KEY}');"

print(sql_stage)

today = date.today()

def snowflake_to_s3():
    # step 1: query data from snowflake and save into text file
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from final_data.data;")
    sql_data = pd.DataFrame(cursor.fetchmany(100), columns=[x[0] for x in cursor.description])
    cursor.close()
    conn.close()
    # sql_data.to_parquet('/imba-dbt/imba_airflow/final_data.parquet')
    # logging.info("Saved IMBA data in text file: final_data.parquet")

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(sql_data)
    df.repartition(1).write.mode('overwrite').parquet('/imba-dbt/imba_airflow/final_data_spark.parquet')

    # with open(f"/imba-dbt/imba_airflow/final_data.txt", "w") as f:
    #     csv_writer = csv.writer(f)
    #     csv_writer.writerow([i[0] for i in cursor.description])
    #     csv_writer.writerows(cursor)
    #     f.flush()
    #     cursor.close()
    #     conn.close()
    #     logging.info("Saved IMBA data in text file: final_data.txt")

    # step 2: upload text file into S3
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    s3_hook.load_file(
        "/imba-dbt/imba_airflow/final_data_spark.parquet/*.parquet",    # filename=f.name,
        key=f"output/airflow/{today.year}/{today.month}/{today.day}/final_data_spark.parquet",
        bucket_name="imba-johnny",
        replace=True
    )
    logging.info("IMBA data for machine learning has been pushed to S3!")


with DAG(
    dag_id='ETL_with_snowflake_dbt_s3',
    default_args=default_args,
    start_date=datetime(2021, 12, 19),
    schedule_interval='@once',
    catchup=False
) as dag:
    create_source_tables = SnowflakeOperator(
        task_id='create_source_tables',
        snowflake_conn_id='snowflake_conn',
        sql="""
            CREATE OR REPLACE SCHEMA raw;

            CREATE OR REPLACE TABLE raw.products (
	            product_id integer,
                product_name varchar,
                aisle_id integer,
                department_id integer
            );

            CREATE OR REPLACE TABLE raw.orders (
	            order_id integer,
                user_id integer,
                eval_set varchar(10),
                order_number integer,
                order_dow integer,
                order_hour_of_day integer,
                days_since_prior_order NUMERIC
            );

            CREATE OR REPLACE TABLE raw.order_products__prior(
	            order_id integer,
                product_id integer,
                add_to_cart_order integer,
                reordered integer
            );

            CREATE OR REPLACE TABLE raw.order_products__train(
	            order_id integer,
                product_id integer,
                add_to_cart_order integer,
                reordered integer
            );
        """
    )

    prepare_file_format = SnowflakeOperator(
        task_id='prepare_file_format',
        snowflake_conn_id='snowflake_conn',
        sql="""
            create or replace file format raw.csvformat
                type = 'CSV'
                field_delimiter = ','
                skip_header = 1
                error_on_column_count_mismatch = true
                trim_space = true
                FIELD_OPTIONALLY_ENCLOSED_BY = NONE
                null_if = ('NULL');
            """
    )

    prepare_stage = SnowflakeOperator(
        task_id='prepare_stage',
        snowflake_conn_id='snowflake_conn',
        sql=sql_stage
    )

    insert_into_table = SnowflakeOperator(
        task_id='insert_into_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
            USE SCHEMA raw;

            copy into raw.products 
                from @csv_stage/products/products.csv
                on_error = 'skip_file';

            copy into raw.orders
                from @csv_stage/orders/orders.csv
                on_error = 'skip_file';
            
            copy into raw.order_products__prior
                from @csv_stage/order_products/order_products__prior.csv.gz
                on_error = 'skip_file';

            copy into raw.order_products__train
                from @csv_stage/order_products/order_products__train.csv.gz
                on_error = 'skip_file';
        """
    )

    execute_dbt = BashOperator(
        task_id='execute_dbt',
        bash_command='cd /imba-dbt/imba_dbt && . .env && dbt run'
    )

    load_to_s3 = PythonOperator(
        task_id="snowflake_to_s3",
        python_callable=snowflake_to_s3
    )


    create_source_tables >> prepare_file_format >> prepare_stage >> insert_into_table >> execute_dbt >> load_to_s3
