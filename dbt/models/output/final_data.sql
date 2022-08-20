{{ config(materialized='table', schema='output') }}

with union_1 as (

    SELECT f1.user_id, 
        f1.user_orders, 
        f1.user_period, 
        f1.user_mean_days_since_prior,
        f2.user_total_products, 
        f2.user_distinct_products, 
        f2.user_reorder_ratio

    FROM {{ ref('user_features_1') }} f1

    INNER JOIN {{ ref('user_features_2') }} f2 

    ON f1.user_id = f2.user_id
),
    
union_2 as (

    SELECT u1.*, 
        uf.product_id, 
        uf.up_orders, 
        uf.up_first_order, 
        uf.up_last_order, 
        uf.up_average_cart_position

    FROM union_1 u1

    INNER JOIN {{ ref('up_features') }} uf 

    ON u1.user_id = uf.user_id

),

final_data as (

    SELECT u2.*,
        pf.prod_orders, 
        pf.prod_reorders, 
        pf.prod_first_orders, 
        pf.prod_second_orders
    FROM union_2 u2
    INNER JOIN {{ ref('prd_features') }} pf 
    ON u2.product_id = pf.product_id

)

SELECT * FROM final_data