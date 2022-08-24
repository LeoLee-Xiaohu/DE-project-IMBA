use role accountadmin;
use database IMBA;

-- Method 1.create integration to unload snowflake data into s3
-- //create storage integration s3_int
-- //  type = external_stage
-- //  storage_provider = 'S3'
-- ////  storage_aws_role_arn = 'arn:aws:iam::001234567890:role/myrole'
-- //  enabled = true
-- //  storage_allowed_locations = ('s3://imba-dbt/results/');


-- method 2. 
-- create file format
create or replace file format "IMBA"."IMBA_PRODUCTION".imba_ext_upload_format
  type = 'csv'
  FIELD_DELIMITER = ','
  ;

-- Create an external s3 upload stage:
create or replace STAGE "IMBA"."IMBA_PRODUCTION".imba_ext_upload_stage
URL = 's3://imba-dbt/results/' 
CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '') 
comment = 'external stage for upload snowflake table into s3 (using secret_key)'
file_format = "IMBA"."IMBA_PRODUCTION".imba_ext_upload_format;

show stages;

-- upload final data into S3 bucket
copy into @"IMBA"."IMBA_PRODUCTION".imba_ext_upload_stage/non_daily_results_2022/ 
from "IMBA"."IMBA_PRODUCTION"."NON_DAILY_ORDERS_RESULTS"
OVERWRITE = TRUE 
HEADER=TRUE ;

ls @"IMBA"."IMBA_PRODUCTION".imba_ext_upload_stage; 