use role accountadmin;
use database IMBA;
//create schema DEV;
create or replace table imba.DEV.src_ordproducts
(
  order_id number,
  product_id number,
  add_to_cart_order number,
  reordered number
);

----------------------------------------------------DEV-------------------------------------------------
-- SNOW PIPE for SRC_CREDIT
-- Create file format
create or replace file format imba.DEV.formate_src_ordproducts
  type = 'csv'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  ;
  
  
 -- 1. Create an external s3 stage:
create or replace STAGE "IMBA"."DEV".staging_ordproducts
URL = 's3://imba-dbt/data/order_products/' 
CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '') 
comment = 'snowpipe for imba order-product data';

-- Note: Stage location should be pointed to a folder location and not to a file.

-- 2. Verify the stage is created using:
show stages;

-- 3. Create a pipe using auto_ingest=true:
create or replace pipe "IMBA"."DEV".pipe_ordproducts  auto_ingest = true 
as copy into imba.DEV.src_ordproducts
from @"IMBA"."DEV".staging_ordproducts
file_format = (format_name = imba.DEV.formate_src_ordproducts)
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
ls @IMBA.DEV.staging_ordproducts; 

-- 9. Wait for 10-15seconds and check the result: 
select * from imba.DEV.src_ordproducts;