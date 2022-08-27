SELECT user_id,
Max(order_number) AS user_orders,
Sum(days_since_prior_order) AS user_period,
Avg(days_since_prior_order) AS user_mean_days_since_prior
FROM orders_app
GROUP BY user_id