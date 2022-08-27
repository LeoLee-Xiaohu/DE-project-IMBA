select op.product_id product_id,
       op.order_id order_id,
       op.add_to_cart_order add_to_cart_order,
       o.user_id user_id,
       o.date_time date_time
from ods.order_products_ods op, ods.orders_ods o
where op.order_id=o.order_id and o.date_time='{{ var('current_date') }}'