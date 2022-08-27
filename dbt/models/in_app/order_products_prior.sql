SELECT a.*, b.product_id,
  b.add_to_cart_order,
  b.reordered
FROM app.orders a
JOIN app.order_products b
ON a.order_id = b.order_id
WHERE a.eval_set = 'prior'