{{ config(materialized='table', schema='features') }}

with user_features_1 as (

    SELECT user_id,
        Max(order_number) AS user_orders, 
        Sum(days_since_prior_order) AS user_period, 
        Avg(days_since_prior_order) AS user_mean_days_since_prior
        
    FROM {{ source("rawdata", "orders") }}

    GROUP BY user_id

)

SELECT * FROM user_features_1