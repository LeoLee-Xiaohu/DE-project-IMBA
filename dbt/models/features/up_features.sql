{{ config(materialized='table', schema='features') }}

with up_features as (

    SELECT user_id,
        product_id,
        Count(*) AS up_orders,
        Min(order_number) AS up_first_order, 
        Max(order_number) AS up_last_order, 
        Avg(add_to_cart_order) AS up_average_cart_position

    FROM {{ ref("order_products_prior") }}

    GROUP BY user_id, product_id

)

SELECT * FROM up_features