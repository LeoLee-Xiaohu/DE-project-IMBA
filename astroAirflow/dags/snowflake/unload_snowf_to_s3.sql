use role accountadmin;
use database IMBA;

-- upload final data into S3 bucket
copy into @"IMBA"."IMBA_PRODUCTION".imba_ext_upload_stage/non_daily_results_202208/ 
from "IMBA"."IMBA_PRODUCTION"."NON_DAILY_ORDERS_RESULTS"
OVERWRITE = TRUE 
HEADER=TRUE ;

copy into @"IMBA"."IMBA_PRODUCTION".imba_ext_upload_stage/daily_order_results_202208/ 
from "IMBA"."IMBA_PRODUCTION"."DAILY_ORDER_RESULTS"
OVERWRITE = TRUE 
HEADER=TRUE ;

copy into @"IMBA"."IMBA_PRODUCTION".imba_ext_upload_stage/final_results_202208/ 
from "IMBA"."IMBA_PRODUCTION"."FINAL_DATA_FOR_ML"
OVERWRITE = TRUE 
HEADER=TRUE 

