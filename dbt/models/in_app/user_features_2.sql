SELECT user_id,
Count(*) AS user_total_products,
Count(DISTINCT product_id) AS user_distinct_products,
Sum(reordered) / Cast(Sum(CASE WHEN order_number >= 1 THEN 1 ELSE 0 END) AS DOUBLE) AS user_reorder_ratio
FROM order_products_prior
GROUP BY user_id
