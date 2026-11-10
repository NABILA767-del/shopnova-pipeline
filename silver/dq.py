import logging
from datetime import datetime, timezone
from pyspark.sql import SparkSession
log = logging.getLogger("silver.dq")

CHECKS = [
    ("orders", "orders_pk_unique", "BLOCKING", "SELECT count(*) - count(DISTINCT id) AS bad FROM shopnova.orders"),
    ("orders", "orders_status_domain", "BLOCKING", "SELECT count(*) AS bad FROM shopnova.orders WHERE _is_deleted = false AND status NOT IN ('creee','payee','expediee','annulee','remboursee')"),
    ("orders", "orders_amount_positive", "WARNING", "SELECT count(*) AS bad FROM shopnova.orders WHERE amount < 0"),
    ("customers", "customers_pk_unique", "BLOCKING", "SELECT count(*) - count(DISTINCT customer_id) AS bad FROM shopnova.customers"),
    ("products", "products_pk_unique", "BLOCKING", "SELECT count(*) - count(DISTINCT product_id) AS bad FROM shopnova.products"),
]

def run_dq_checks(spark: SparkSession) -> bool:
    rows = []
    blocking_failed = False
    for table, name, severity, sql in CHECKS:
        try:
            bad = spark.sql(sql).collect()[0]["bad"] or 0
        except Exception as e:
            log.error("DQ %s : erreur %s", name, e)
            bad, severity = -1, "BLOCKING"
        
        status = "OK" if bad == 0 else "FAIL"
        if bad != 0 and severity == "BLOCKING":
            blocking_failed = True
            log.error("DQ %s : %s (%s)", name, status, bad)
        elif bad != 0:
            log.warning("DQ %s : %s (%s)", name, status, bad)
        else:
            log.info("DQ %s : OK", name)
            
        rows.append((datetime.now(timezone.utc), table, name, severity, int(bad), status, None))
    
    # On retire le .silver ici
    (spark.createDataFrame(rows, "run_ts timestamp, table_name string, check_name string, severity string, bad_rows long, status string, details string")
     .writeTo("shopnova._dq_results").append())
    
    if blocking_failed:
        raise RuntimeError("Contrôles DQ bloquants en échec")
    return True