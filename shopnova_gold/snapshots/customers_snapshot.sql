{% snapshot customers_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}
SELECT 
    customer_id, 
    name, 
    email, 
    city, 
    created_at,
    updated_at::timestamp AS updated_at  -- <-- Ajoute ce cast
FROM read_parquet('s3://warehouse/silver/customers/**/*.parquet')
{% endsnapshot %}