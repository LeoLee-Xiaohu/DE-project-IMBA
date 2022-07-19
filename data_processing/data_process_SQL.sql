create table order_products_prior with (external_location = 's3://imba-leo/features/order_products_prior/', format = 'parquet')
as (select a.*, 
    b.product_id,
    b.add_to_cart_order,
    b.reordered
    from orders a join order_products b on a.order_id = b.order_id
    where a.eval_set = 'prior');

# user features selection - order date
select user_id,
    max(order_number) as max_order_number,
    sum(days_since_prior_order) as totall_days_prior_order,
    avg(days_since_prior_order) as avg_days_priro_order
from orders 
group by user_id;

# user features selection - product reordered ratio
select user_id,
    count(product_id) as total_product_number,
    count(distinct product_id) as total_distinct_product_number,
    sum(case when reordered = 1 then 1 else 0 end) / Cast(Sum(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS DOUBLE) AS user_reorder_ratio
from order_products_prior
group by user_id ;

# product reordered features
select user_id,
    product_id,
    count(*) as total_order_number,
    max(order_number) as max_order_number,
    min(order_number) as min_order_number,
    avg(add_to_cart_order) as avg_add_to_cart_order
from order_products_prior
group by user_id, product_id;

# product features - reorder frequence
select 
    product_id,
    count(*) as prod_orders,
    sum(reordered) as prod_reorders,
    sum(case when product_seq_time = 1 then 1 else 0 end) count_product_seqtime1,
    sum(case when product_seq_time = 2 then 1 else 0 end) count_product_seqtime2
from 
    (select * , 
        rank() over (partition by user_id , product_id order by order_number ) as product_seq_time 
    from order_products_prior ) a
group by product_id