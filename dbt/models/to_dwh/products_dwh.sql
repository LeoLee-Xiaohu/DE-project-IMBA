select product_id, product_name, aisle_id, department_id, on_shelf_date, off_shelf_date
from ods.products_ods
where off_shelf_date='{{ var('current_date') }}' or on_shelf_date='{{ var('current_date') }}'