{{ 
config(
    materialized='view', 
      tags=["non-daily business data"]
    ) 
}}

with p as (
    select * from {{ref('stg_product')}}
),
d as (
    select * from {{ref('stg_departments')}}
),

pd as (
    select p.product_id,
        p.department_id,
        p.aisle_id,
        p.product_name,
        d.department
    from p left join d on p.department_id = d.department_id
)

select * from pd