

with orders as (
 
    select * from {{source('imba','src_orders') }}
 
), 

order_products as (
    select * from {{source('imba', 'src_ordproducts')}}
),
 
order_products_prior as (
 
    select a.*, 
    b.product_id,
    b.add_to_cart_order,
    b.reordered
    from orders a join order_products b on a.order_id = b.order_id
    where a.eval_set = 'prior'
 
) 
 
select * from order_products_prior