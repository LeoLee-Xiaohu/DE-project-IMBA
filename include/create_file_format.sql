create or replace file format rawdata.csv_format
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    error_on_column_count_mismatch = true
    trim_space = true
    FIELD_OPTIONALLY_ENCLOSED_BY = NONE
    null_if = ('NULL');