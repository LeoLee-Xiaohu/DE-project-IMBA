{{ 
config(
    materialized='view', 
      tags=["non-daily business data"]
    ) 
}}

with ais  as (
    select * from {{ref('departments')}}
)

select * from ais