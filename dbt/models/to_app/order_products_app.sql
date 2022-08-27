with T as (select op.product_id as product_id, op.order_id as order_id, o.user_id as user_id
	from dwh.order_products_dwh as op
	left join dwh.orders_dwh as o
	on op.order_id=o.order_id)

select op.product_id product_id, op.order_id order_id, op.add_to_cart_order add_to_cart_order, T3.reordered reordered
from dwh.order_products_dwh op
left join

(select T1.order_id order_id, T1.product_id product_id, case when count(*)>0 then 1 else 0 end as reordered
from T T1
right join T T2
on T1.user_id=T2.user_id and T1.product_id=T2.product_id and T2.order_id<T1.order_id
group by T1.order_id, T1.product_id) T3

on op.product_id=T3.product_id and op.order_id=T3.order_id