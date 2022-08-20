USE SCHEMA rawdata;
copy into rawdata.products 
    from @csv_stage/products/products.csv
    on_error = 'skip_file';
copy into rawdata.orders
    from @csv_stage/orders/orders.csv
    on_error = 'skip_file';
copy into rawdata.order_products_prior
    from @csv_stage/order_products/order_products__prior.csv.gz
    on_error = 'skip_file';
copy into rawdata.order_products_train
    from @csv_stage/order_products/order_products__train.csv.gz
    on_error = 'skip_file';