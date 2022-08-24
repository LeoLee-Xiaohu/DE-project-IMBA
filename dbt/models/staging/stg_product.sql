
{{ 
config(
    materialized='view', 
      tags=["non-daily business data"]
    ) 
}}
with p as (
 
    select * from {{source('imba','src_products') }}
 
)

select * from p 