from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator 
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
import datetime as dt
import os
# from airflow.utils.helpers import chain
from airflow.utils.task_group import TaskGroup

today = str(dt.date.today())
# today_var = { today }
# prepare sql for psql



def psql2s3_sql(table, col=[], date=today):

    if table=='order_products':
        sql = "select * from aws_s3.query_export_to_s3('select op.* from order_products op, orders o where o.order_id=op.order_id and "
    else:
        sql = "select * from aws_s3.query_export_to_s3('select * from {0} where ".format(table)

    if table== "order_products" or col[0] == "date_time":
        sql += "date_time between ''{0} 00:00:00'' and ''{1} 23:59:59'' ".format(date, date)
    else:
        sql += "{0}=''{1}'' or {2}=''{3}''".format(col[0], date, col[1], date)

    sql += "', aws_commons.create_s3_uri('imba-stage','psql_to_snowflake/{0}/{1}.csv', 'ap-southeast-2'), options := 'format csv, HEADER true');".format(date, table)

    return sql



    


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,8,1),
    'retries': 0
}


with DAG('imba_pipeline_chain', default_args=default_args, schedule_interval='@once') as dag_1:

    with TaskGroup(group_id='psql_to_s3') as task_1:
        task_1_list = []

        for i in [['users', ['reg_date', 'unreg_date']], ['products', ['on_shelf_date', 'off_shelf_date']],
                  ['orders', ['date_time']], ['order_products', []]]:

            sql_scp = psql2s3_sql(i[0], i[1], today)

            task_1_list.append(PostgresOperator(
                task_id="psql_to_s3_"+i[0],
                postgres_conn_id="postgres_conn",
                sql=sql_scp,
                dag=dag_1))

        task_1_list

    with TaskGroup(group_id='s3_to_ods') as task_2:
        task_2_list = []

        for i in ['users', 'products', 'orders', 'order_products']:
            
            sql_scp = "copy into imba.ods.{0}_ods from @imba.ods.IMBA_ODS_STAGE/psql_to_snowflake/{1}/{2}.csv file_format = imba.ods.imba_csv_format on_error = 'abort_statement' purge = false;".format(i, today, i)
            
            task_2_list.append(SnowflakeOperator(
                task_id="s3_to_ods_"+i,
                sql=sql_scp,
                snowflake_conn_id="snowflake_conn",
                dag=dag_1))

        task_2_list


    task_3 = BashOperator(
        task_id='ods_to_dwh',
        bash_command='cd /opt/dbt && dbt run --models to_dwh --profiles-dir . --vars "{ "current_date": { today } }" ',
        env={
            'snowflake_user': '{{ var.value.snowflake_user }}',
            'snowflake_password': '{{ var.value.snowflake_password }}',
            **os.environ
        },
        dag=dag_1
    )

    task_1 >> task_2 >> task_3
