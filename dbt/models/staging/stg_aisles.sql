{{ 
config(
    materialized='view', 
      tags=["non-daily business data"]
    ) 
}}

with aisles  as (
    select * from {{ref('aisles')}}
)

select * from aisles