SELECT * FROM read_parquet('s3://warehouse/silver/products/**/*.parquet') 
WHERE _is_deleted = false