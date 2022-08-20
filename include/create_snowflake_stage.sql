CREATE OR REPLACE STAGE rawdata.csv_stage url=s3://{{ params.s3_bucket }}
credentials=(aws_key_id='{{ params.login }}' aws_secret_key='{{ params.password }}')
file_format=csv_format;
