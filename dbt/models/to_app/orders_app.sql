with T as (
  select unique(op.order_id) order_id, op.user_id user_id, op.date_time date_time,
  rank() over(partition by op.user_id order by op.order_id asc) order_number,
  dayofweek(op.date_time) order_dow,
  hour(op.date_time) order_hour_of_day
  from dwh.order_products_dwh op)

select T1.order_id order_id,
       T1.user_id user_id,
       T1.order_number order_number,
       T1.order_hour_of_day,
       datediff(day, T1.date_time, T2.date_time) days_since_prior_order,
       null as eval_set
from T T1
left join T T2
on T1.user_id=T2.user_id and T1.order_number - T2.order_number=1



