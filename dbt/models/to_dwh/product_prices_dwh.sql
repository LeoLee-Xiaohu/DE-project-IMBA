{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert', 
    unique_key = ['product_id', 'create_date']
) }}

with T as(select op.product_id product_id, op.price new_price, dpp.price old_price, 
		  op.price_effect_date price_effect_date, dpp.create_date old_create_date, 
		  dpp.modify_date modify_date
from ods.products_ods op
left join dwh.product_prices_dwh dpp
on op.product_id=dpp.product_id 
where dpp.modify_date='9999-12-31' and op.price_effect_date='{{ var('current_date') }}')

select t1.product_id product_id, t1.new_price price, t1.price_effect_date create_date, t1.modify_date modify_date
from T t1
-- new row

union

select t2.product_id product_id, t2.old_price price, t2.old_create_date create_date, t2.price_effect_date modify_date
from T t2
-- old row update
    -- incremental_strategy = 'merge', 
    -- 'delete+insert',