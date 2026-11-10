{{ config(materialized='incremental', unique_key='order_id') }}

WITH source_data AS (
    SELECT 
        id AS order_id, 
        customer_id, 
        product_id, 
        quantity, 
        amount AS total_amount, 
        status, 
        created_at, 
        updated_at,
        _lsn
    FROM read_parquet('s3://warehouse/silver/orders/**/*.parquet')
    WHERE _is_deleted = false
),
deduplicated AS (
    -- On garde uniquement la version la plus récente (LSN le plus élevé) pour chaque commande
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _lsn DESC) as rn
    FROM source_data
)
SELECT 
    order_id, 
    customer_id, 
    product_id, 
    quantity, 
    total_amount, 
    status, 
    created_at, 
    updated_at
FROM deduplicated
WHERE rn = 1

{% if is_incremental() %}
  AND updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}