{{ config(
    materialized='table',
) }}

SELECT a.*, 
    b.product_id,
    b.add_to_cart_order,
    b.reordered 
FROM {{
    source(
        "raw",
        "orders"
    )
}} a 
JOIN {{ ref("order_products") }} b 
ON a.order_id = b.order_id
WHERE a.eval_set = 'prior'
