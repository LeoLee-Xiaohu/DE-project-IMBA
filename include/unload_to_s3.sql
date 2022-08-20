create or replace file format "OUTPUT"."csv_format" 
    type = 'CSV'
    COMPRESSION = NONE;

CREATE OR REPLACE STAGE "OUTPUT"."csv_stage" url=s3://{{ params.s3_bucket }}/output/airflow/{{ params.today_year }}/{{ params.today_month }}/{{ params.today_day }}/
credentials=(aws_key_id='{{ params.login }}' aws_secret_key='{{ params.password }}')
file_format="OUTPUT"."csv_format";

COPY INTO @"OUTPUT"."csv_stage"/data.csv
FROM (SELECT *
	FROM "OUTPUT"."FINAL_DATA"
	) 
SINGLE = TRUE
OVERWRITE = TRUE
MAX_FILE_SIZE = 4900000000;