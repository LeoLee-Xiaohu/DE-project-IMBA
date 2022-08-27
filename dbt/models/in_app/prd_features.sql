SELECT product_id,
Count(*) AS prod_orders,
Sum(reordered) AS prod_reorders,
Sum(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS prod_first_orders,
Sum(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS prod_second_orders
FROM (SELECT *,
      Rank() OVER (
        partition BY user_id, product_id
        ORDER BY order_number
      ) AS product_seq_time
FROM order_products_prior)
GROUP BY product_id