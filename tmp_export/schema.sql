CREATE TABLE fct_orders(order_id BIGINT, customer_id BIGINT, product_id BIGINT, quantity INTEGER, total_amount DECIMAL(12,2), status VARCHAR, created_at TIMESTAMP WITH TIME ZONE, updated_at TIMESTAMP WITH TIME ZONE);
CREATE VIEW dim_customers AS SELECT * FROM read_parquet('s3://warehouse/silver/customers/**/*.parquet') WHERE (_is_deleted = false);

