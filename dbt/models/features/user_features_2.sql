{{ config(materialized='table', schema='features') }}

with user_features_2 as (

    SELECT user_id,
        Count(*) AS user_total_products, 
        Count(DISTINCT product_id) AS user_distinct_products,
        Sum(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) / Cast(Sum(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS DOUBLE) AS user_reorder_ratio
        
    FROM {{ ref("order_products_prior") }}

    GROUP BY user_id

)

SELECT * FROM user_features_2