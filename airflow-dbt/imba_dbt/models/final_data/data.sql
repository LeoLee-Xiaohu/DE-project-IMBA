{{ config(
    materialized='table',
) }}

with feature_1 as (
    select F1.user_id, 
        F1.user_orders, 
        F1.user_period, 
        F1.user_mean_days_since_prior,
        F2.user_total_products, 
        F2.user_distinct_products, 
        F2.user_reorder_ratio
    from {{ ref('user_features_1') }} F1
    INNER JOIN {{ ref('user_features_2') }} F2 
    on F1.user_id = F2.user_id
),
    
feature_2 as (
    select F3.*, 
        UF.product_id, 
        UF.up_orders, 
        UF.up_first_order, 
        UF.up_last_order, 
        UF.up_average_cart_position
    from feature_1 F3
    INNER JOIN {{ ref('up_features') }} UF 
    on F3.user_id = UF.user_id
),

feature_3 as (
    select F4.*,
        PF.prod_orders, 
        PF.prod_reorders, 
        PF.prod_first_orders, 
        PF.prod_second_orders
    from feature_2 F4
    INNER JOIN {{ ref('prd_features') }} PF 
    on F4.product_id = PF.product_id
)

select * from feature_3
