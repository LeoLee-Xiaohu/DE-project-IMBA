{{ 
config(
    materialized='table', 
      tags=["non-daily business data"]
    ) 
}}


with p as (
    select * from {{ ref('prd_dep') }}
),
a as (
    select * from {{ref('stg_aisles')}}
),

pa as (
    select p.product_id,
        p.department_id,
        p.aisle_id,
        p.product_name,
        p.department,
        a.aisle 
    from p left join a on p.aisle_id = a.aisle_id
)

select * from pa