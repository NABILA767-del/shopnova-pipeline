from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import StringType, StructType
from pyspark.sql.functions import from_json, to_timestamp, from_unixtime

BUSINESS_COLUMNS = {
    "customers": ["customer_id", "name", "email", "city", "created_at", "updated_at"],
    "products":  ["product_id", "name", "category", "price", "created_at", "updated_at"],
    "orders":    ["id", "customer_id", "product_id", "quantity", "amount", "status", "created_at", "updated_at"],
}
PRIMARY_KEY = {"customers": "customer_id", "products": "product_id", "orders": "id"}

def parse_lsn(col):
    parts = F.split(F.col(col).cast("string"), "/")
    hi = F.conv(parts[0], 16, 10).cast("long")
    lo = F.conv(parts[1], 16, 10).cast("long")
    from_hex = hi * F.lit(4294967296) + lo
    return F.when(F.size(parts) == 2, from_hex).otherwise(F.col(col).cast("long"))

def flatten_envelope(df: DataFrame) -> DataFrame:
    return (df
        .withColumn("_payload", F.col("payload.after"))
        .withColumn("_before", F.col("payload.before").cast("string"))
        .withColumn("_after", F.col("payload.after").cast("string"))
        .withColumn("_lsn", parse_lsn("payload.source.lsn"))
        .withColumn("_op", F.col("payload.op"))
        .withColumn("_source_ts", F.to_timestamp(F.from_unixtime(F.col("payload.ts_ms") / 1000)))
        .withColumn("_is_deleted", F.col("payload.op") == "d")
        .select("_payload.*", "_before", "_after", "_lsn", "_op", "_source_ts", "_is_deleted"))

def normalize(df: DataFrame, table: str) -> DataFrame:
    df = df.withColumn("_silver_ts", F.current_timestamp())

    if table == "customers":
        df = (df
            .withColumnRenamed("id", "customer_id")
            .withColumnRenamed("nom", "name")
            .withColumnRenamed("ville", "city")
            .withColumn("customer_id", F.col("customer_id").cast("long"))
            .withColumn("name", F.trim("name"))
            .withColumn("email", F.lower(F.trim("email")))
            .withColumn("created_at", F.to_timestamp(F.from_unixtime(F.col("created_at") / 1000)))
            .withColumn("updated_at", F.to_timestamp(F.from_unixtime(F.col("updated_at") / 1000))))
    elif table == "products":
        # On suppose que les colonnes sont : id, nom, categorie, prix, created_at, updated_at
        df = (df
            .withColumnRenamed("id", "product_id")
            .withColumnRenamed("nom", "name")
            .withColumnRenamed("categorie", "category")
            .withColumnRenamed("prix", "price")
            .withColumn("product_id", F.col("product_id").cast("long"))
            .withColumn("name", F.trim("name"))
            .withColumn("price", F.col("price").cast("decimal(12,2)"))
            .withColumn("created_at", F.to_timestamp(F.from_unixtime(F.col("created_at") / 1000)))
            .withColumn("updated_at", F.to_timestamp(F.from_unixtime(F.col("updated_at") / 1000))))
    elif table == "orders":
        # On suppose que les colonnes sont : id, customer_id, product_id, quantite, montant, statut, created_at, updated_at
        df = (df
            .withColumnRenamed("quantite", "quantity")
            .withColumnRenamed("montant", "amount")
            .withColumnRenamed("statut", "status")
            .withColumn("id", F.col("id").cast("long"))
            .withColumn("customer_id", F.col("customer_id").cast("long"))
            .withColumn("product_id", F.col("product_id").cast("long"))
            .withColumn("quantity", F.col("quantity").cast("int"))
            .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
            .withColumn("status", F.lower(F.trim("status")))
            .withColumn("created_at", F.to_timestamp(F.from_unixtime(F.col("created_at") / 1000)))
            .withColumn("updated_at", F.to_timestamp(F.from_unixtime(F.col("updated_at") / 1000))))

    return df

def pick_latest_per_key(events: DataFrame, pk: str) -> DataFrame:
    w = Window.partitionBy(pk).orderBy(F.col("_lsn").desc_nulls_last())
    return events.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")