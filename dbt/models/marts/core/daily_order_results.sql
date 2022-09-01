
with 
PF as (
select * from {{ ref('prd_features' )}}
),

UF as (
    select * from {{ ref('up_features') }}
),

F1 as (
    select * from {{ ref('user_feature_1') }}
),

F2 as (
    select * from {{ ref('user_feature_2') }}
),

final as (
    select F1.user_id, 
        F1.user_orders, 
        F1.user_period, 
        F1.user_mean_days_since_prior,
        F2.user_total_products, 
        F2.user_distinct_products, 
        F2.user_reorder_ratio,
        UF.product_id, 
        UF.up_orders, 
        UF.up_first_order, 
        UF.up_last_order, 
        UF.up_average_cart_position,
        PF.prod_orders, 
        PF.prod_reorders, 
        PF.prod_first_orders, 
        PF.prod_second_orders
    
    from UF 
    inner join PF on PF.product_id = UF.product_id 
    inner join F1 on UF.user_id = F1.user_id 
    inner join F2 on F1.user_id = F2.user_id 
)

select * from final 
