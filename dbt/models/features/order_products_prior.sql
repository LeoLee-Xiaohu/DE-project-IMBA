{{ config(materialized='table', schema='features') }}

with order_products_prior as (

    SELECT a.*,
        b.product_id,
        b.add_to_cart_order,
        b.reordered 

    FROM {{ source("rawdata", "orders") }} a

    JOIN {{ ref("order_products") }} b

    ON a.order_id = b.order_id

    WHERE a.eval_set = 'prior'

)

SELECT * FROM order_products_prior