-- On ne crée PAS de base de données. On crée les tables directement avec un chemin explicite.
CREATE TABLE IF NOT EXISTS shopnova.customers (
  customer_id BIGINT, name STRING, email STRING, city STRING,
  created_at TIMESTAMP, updated_at TIMESTAMP, _lsn BIGINT,
  _op STRING, _source_ts TIMESTAMP, _is_deleted BOOLEAN, _silver_ts TIMESTAMP
) USING iceberg LOCATION 's3a://warehouse/silver/customers' PARTITIONED BY (bucket(16, customer_id));

CREATE TABLE IF NOT EXISTS shopnova.products (
  product_id BIGINT, name STRING, category STRING, price DECIMAL(12,2),
  created_at TIMESTAMP, updated_at TIMESTAMP, _lsn BIGINT,
  _op STRING, _source_ts TIMESTAMP, _is_deleted BOOLEAN, _silver_ts TIMESTAMP
) USING iceberg LOCATION 's3a://warehouse/silver/products' PARTITIONED BY (bucket(16, product_id));

CREATE TABLE IF NOT EXISTS shopnova.orders (
  id BIGINT, customer_id BIGINT, product_id BIGINT, quantity INT, amount DECIMAL(12,2),
  status STRING, created_at TIMESTAMP, updated_at TIMESTAMP, _lsn BIGINT,
  _op STRING, _source_ts TIMESTAMP, _is_deleted BOOLEAN, _silver_ts TIMESTAMP
) USING iceberg LOCATION 's3a://warehouse/silver/orders' PARTITIONED BY (bucket(16, id));

CREATE TABLE IF NOT EXISTS shopnova.customers_history (
  customer_id BIGINT, name STRING, email STRING, city STRING, created_at TIMESTAMP,
  updated_at TIMESTAMP, _lsn BIGINT, _op STRING, _before STRING, _after STRING,
  _source_ts TIMESTAMP, _silver_ts TIMESTAMP
) USING iceberg LOCATION 's3a://warehouse/silver/customers_history' PARTITIONED BY (days(_source_ts));

CREATE TABLE IF NOT EXISTS shopnova.products_history (
  product_id BIGINT, name STRING, category STRING, price DECIMAL(12,2), created_at TIMESTAMP,
  updated_at TIMESTAMP, _lsn BIGINT, _op STRING, _before STRING, _after STRING,
  _source_ts TIMESTAMP, _silver_ts TIMESTAMP
) USING iceberg LOCATION 's3a://warehouse/silver/products_history' PARTITIONED BY (days(_source_ts));

CREATE TABLE IF NOT EXISTS shopnova.orders_history (
  id BIGINT, customer_id BIGINT, product_id BIGINT, quantity INT, amount DECIMAL(12,2),
  status STRING, created_at TIMESTAMP, updated_at TIMESTAMP, _lsn BIGINT, _op STRING,
  _before STRING, _after STRING, _source_ts TIMESTAMP, _silver_ts TIMESTAMP
) USING iceberg LOCATION 's3a://warehouse/silver/orders_history' PARTITIONED BY (days(_source_ts));

CREATE TABLE IF NOT EXISTS shopnova._processed_partitions (
  table_name STRING, ingestion_date STRING, hour STRING, processed_at TIMESTAMP
) USING iceberg LOCATION 's3a://warehouse/silver/_processed_partitions';

CREATE TABLE IF NOT EXISTS shopnova._dq_results (
  run_ts TIMESTAMP, table_name STRING, check_name STRING,
  severity STRING, bad_rows BIGINT, status STRING, details STRING
) USING iceberg LOCATION 's3a://warehouse/silver/_dq_results';

