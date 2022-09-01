
with 
d as (
select * from {{ ref('daily_order_results' )}}
),

nd as (
    select * from {{ ref('non_daily_orders_results') }}
),

final as (
    select d.user_id, 
        d.user_orders, 
        d.user_period, 
        d.user_mean_days_since_prior,
        d.user_total_products, 
        d.user_distinct_products, 
        d.user_reorder_ratio,
        d.product_id, 
        d.up_orders, 
        d.up_first_order, 
        d.up_last_order, 
        d.up_average_cart_position,
        d.prod_orders, 
        d.prod_reorders, 
        d.prod_first_orders, 
        d.prod_second_orders,
        nd.department_id,
        nd.aisle_id 
    
    from d 
    left join nd on d.product_id = nd.product_id
)

select * from final 