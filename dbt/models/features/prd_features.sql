{{ config(materialized='table', schema='features') }}

with product_seq as (

    SELECT *, 
        Rank() OVER (partition BY user_id, product_id ORDER BY order_number) AS product_seq_time
    
    FROM {{ ref("order_products_prior") }}
),

prd_features as (

    SELECT product_id,
        Count(*) AS prod_orders,
        Sum(reordered) AS prod_reorders,
        Sum(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS prod_first_orders, 
        Sum(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS prod_second_orders
    
    FROM product_seq

    GROUP BY product_id

)

SELECT * FROM prd_features