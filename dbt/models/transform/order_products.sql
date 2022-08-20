{{ config(materialized='table', schema='transform') }}

with order_products as (

    SELECT * 

    FROM {{ source("rawdata", "order_products_prior") }}

    UNION ALL

    SELECT * 

    FROM {{ source("rawdata", "order_products_train") }}

)

SELECT * FROM order_products