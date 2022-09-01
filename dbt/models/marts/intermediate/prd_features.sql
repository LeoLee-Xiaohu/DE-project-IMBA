with order_products_prior as (
    select * from {{ ref('stg_order_products_prior') }}
),

prd_f as(

    select 
    product_id,
    Count(*) AS prod_orders,
    Sum(reordered) AS prod_reorders,
    Sum(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS prod_first_orders,
    Sum(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS prod_second_orders
from 
    (select * , 
        rank() over (partition by user_id , product_id order by order_number ) as product_seq_time 
    from order_products_prior ) a
group by product_id

)

select * from prd_f