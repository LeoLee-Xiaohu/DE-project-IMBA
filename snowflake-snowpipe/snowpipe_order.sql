use role accountadmin;
use database IMBA;
create schema DEV;
create or replace table imba.DEV.src_orders
(
  order_id number,
  user_id number,
  eval_set text,
  order_number number,
  order_dow number,
  order_hour_of_day number,
  days_since_prior_order number
);

----------------------------------------------------DEV-------------------------------------------------
-- SNOW PIPE for SRC_CREDIT
-- Create file format
create or replace file format imba.DEV.formate_src_orders
  type = 'csv'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  ;
  
  
 -- 1. Create an external s3 stage:
create or replace STAGE "IMBA"."DEV".orders_staging 
URL = 's3://imba-dbt/data/orders/' 
CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '') 
comment = 'snowpipe for imba orders data';
;
-- Note: Stage location should be pointed to a folder location and not to a file.

-- 2. Verify the stage is created using:
show stages;

-- 3. Create a pipe using auto_ingest=true:
create or replace pipe "IMBA"."DEV".pipe_orders  auto_ingest = true 
as copy into imba.DEV.src_orders
from @"IMBA"."DEV".orders_staging 
file_format = (format_name = imba.DEV.formate_src_orders)
on_error = 'skip_file';

-- 4.Verify the pipe is created using:                                                                                                                                                        
show pipes;
-- copy ARN of the pipe into SQS 

-- 5. Run SHOW STAGES
-- to check for the NOTIFICATION_CHANNEL

-- 6. Setup SQS notification ( https://www.snowflake.net/your-first-steps-with-snowpipe/ )
-- s3 => property => event notification => SQS queue =》Specify SQS queue

-- 7. Upload the file CSV file to the static folder in S3

-- 8. Run the following to check if the uploaded file is in your stage location:
ls @IMBA.DEV.orders_staging; 

-- 9. Wait for 10-15seconds and check the result: 
select * from imba.DEV.src_orders;
