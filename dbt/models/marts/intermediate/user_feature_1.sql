with orders as (
    select * from {{ source('imba','src_orders') }}
),

userf1 as (
    SELECT user_id,
        Max(order_number) AS user_orders, 
        Sum(days_since_prior_order) AS user_period, 
        Avg(days_since_prior_order) AS user_mean_days_since_prior
    FROM orders GROUP BY user_id
)

select * from userf1