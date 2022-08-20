CREATE OR REPLACE SCHEMA rawdata;
CREATE OR REPLACE TABLE rawdata.products (
    product_id integer,
    product_name varchar,
    aisle_id integer,
    department_id integer
);
CREATE OR REPLACE TABLE rawdata.orders (
    order_id integer,
    user_id integer,
    eval_set varchar(10),
    order_number integer,
    order_dow integer,
    order_hour_of_day integer,
    days_since_prior_order NUMERIC
);
CREATE OR REPLACE TABLE rawdata.order_products_prior(
    order_id integer,
    product_id integer,
    add_to_cart_order integer,
    reordered integer
); 
CREATE OR REPLACE TABLE rawdata.order_products_train(
    order_id integer,
    product_id integer,
    add_to_cart_order integer,
    reordered integer
);