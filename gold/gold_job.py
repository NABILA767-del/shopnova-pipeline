import logging
from pyspark.sql import SparkSession

log = logging.getLogger("gold")

def build_spark():
    return (SparkSession.builder.appName("shopnova-gold")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # 1. Catalogue SILVER (lecture) -> pointe vers s3a://warehouse/silver
        .config("spark.sql.catalog.shopnova", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.shopnova.type", "hadoop")
        .config("spark.sql.catalog.shopnova.warehouse", "s3a://warehouse/silver")
        
        # 2. Catalogue GOLD (écriture) -> pointe vers s3a://warehouse/gold
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gold.type", "hadoop")
        .config("spark.sql.catalog.gold.warehouse", "s3a://warehouse/gold")
        .config("spark.sql.defaultCatalog", "gold")

        # Configuration MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "ShopNova2026!")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())

def main():
    spark = build_spark()
    log.info("Création des tables Gold...")

    # Lire depuis shopnova.customers (qui pointe vers s3a://warehouse/silver)
    # Écrire dans gold.dim_customers (qui pointe vers s3a://warehouse/gold)
    spark.sql("""
        CREATE OR REPLACE TABLE gold.dim_customers AS
        SELECT customer_id, name, email, city, created_at, updated_at
        FROM shopnova.customers WHERE _is_deleted = false
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE gold.dim_products AS
        SELECT product_id, name, category, price, created_at, updated_at
        FROM shopnova.products WHERE _is_deleted = false
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE gold.fct_orders AS
        SELECT id AS order_id, customer_id, product_id, quantity, amount AS total_amount,
               status, created_at, updated_at
        FROM shopnova.orders WHERE _is_deleted = false
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE gold.agg_daily_sales AS
        SELECT date(created_at) AS order_date,
               sum(total_amount) AS total_revenue,
               count(order_id) AS nb_orders
        FROM gold.fct_orders
        WHERE status NOT IN ('annulee', 'remboursee')
        GROUP BY 1
    """)

    spark.sql("""
        CREATE OR REPLACE TABLE gold.agg_sales_by_status AS
        SELECT status,
               count(order_id) AS nb_orders,
               sum(total_amount) AS total_revenue
        FROM gold.fct_orders
        GROUP BY 1
    """)

    log.info("✅ Couche Gold terminée avec succès !")
    spark.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    main()