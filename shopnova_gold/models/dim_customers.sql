SELECT * FROM read_parquet('s3://warehouse/silver/customers/**/*.parquet') 
WHERE _is_deleted = false