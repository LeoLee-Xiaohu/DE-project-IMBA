{{ config(
    materialized='table',
) }}

select 
    * 
from {{
    source(
        "raw",
        "order_products__prior"
    )
}}

union all

select 
    * 
from {{
    source(
        "raw",
        "order_products__train"
    )
}}
