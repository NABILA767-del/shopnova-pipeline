import pytest
from pyspark.sql import SparkSession
from transforms import flatten_envelope, normalize, pick_latest_per_key

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder.master("local[1]").appName("silver-tests")
        .config("spark.sql.shuffle.partitions", "1").getOrCreate())

def _envelope(op, before, after, lsn=100):
    return {"op": op, "before": before, "after": after, "source": {"lsn": lsn}, "ts_ms": 1_700_000_000_000}

def test_insert_uses_after(spark):
    df = spark.createDataFrame([_envelope("c", None, {"id": 1, "customer_id": 10, "product_id": 5, "quantity": 2, "amount": 99.9, "status": "creee"})])
    row = flatten_envelope(df).collect()[0]
    assert row["id"] == 1 and row["_op"] == "c" and row["_is_deleted"] is False

def test_delete_uses_before(spark):
    df = spark.createDataFrame([_envelope("d", {"id": 42, "status": "annulee"}, None)])
    row = flatten_envelope(df).collect()[0]
    assert row["id"] == 42 and row["_is_deleted"] is True

def test_lsn_hex_parsing(spark):
    df = spark.createDataFrame([_envelope("u", {"id": 1, "status": "creee"}, {"id": 1, "status": "payee"}, lsn="0/16B3740")])
    row = flatten_envelope(df).collect()[0]
    assert row["_lsn"] == 23_803_712

def test_normalize_orders(spark):
    df = spark.createDataFrame([_envelope("u", {"id": 1, "status": "creee"}, {"id": 1, "customer_id": 10, "product_id": 5, "quantity": "2", "amount": "99.9", "status": "  Payee "})])
    row = normalize(flatten_envelope(df), "orders").collect()[0]
    assert row["status"] == "payee" and str(row["amount"]) == "99.90" and row["quantity"] == 2

def test_pick_latest_per_key(spark):
    rows = [
        _envelope("c", None, {"id": 1, "status": "creee"}, lsn=100),
        _envelope("u", {"id": 1}, {"id": 1, "status": "payee"}, lsn=200),
        _envelope("u", {"id": 1}, {"id": 1, "status": "expediee"}, lsn=150),
    ]
    df = spark.createDataFrame(rows)
    latest = pick_latest_per_key(flatten_envelope(df), "id").collect()
    assert len(latest) == 1 and latest[0]["status"] == "payee" and latest[0]["_lsn"] == 200