import duckdb
import os
import boto3
import pandas as pd
import io
from botocore.client import Config

print("=== Création du fichier DuckDB pour Metabase ===\n")

if os.path.exists('shopnova_gold.duckdb'):
    os.remove('shopnova_gold.duckdb')
    print("Ancien fichier supprimé.")

con = duckdb.connect('shopnova_gold.duckdb')
print("Fichier 'shopnova_gold.duckdb' créé.\n")

s3 = boto3.client(
    's3',
    endpoint_url='http://127.0.0.1:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='ShopNova2026!',
    region_name='us-east-1',
    config=Config(signature_version='s3v4'),
)

def load_parquet_from_minio(bucket, prefix):
    print(f"Lecture de s3://{bucket}/{prefix}...")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        raise ValueError(f"Aucun objet trouvé dans s3://{bucket}/{prefix}")
    
    dfs = []
    for obj in response['Contents']:
        if obj['Key'].endswith('.parquet'):
            resp = s3.get_object(Bucket=bucket, Key=obj['Key'])
            df = pd.read_parquet(io.BytesIO(resp['Body'].read()))
            dfs.append(df)
    
    if not dfs:
        raise ValueError(f"Aucun fichier Parquet trouvé dans s3://{bucket}/{prefix}")
    return pd.concat(dfs, ignore_index=True)

def clean_dtypes(df):
    for col in df.columns:
        if str(df[col].dtype) in ('str', 'string'):
            df[col] = df[col].astype(object)
        elif str(df[col].dtype).startswith('datetime'):
            df[col] = pd.to_datetime(df[col])
    return df

# --- fct_orders ---
print("\nChargement de fct_orders depuis MinIO...")
orders_df = load_parquet_from_minio('warehouse', 'silver/orders/data/')
orders_df = orders_df.sort_values('_lsn', ascending=False).drop_duplicates(subset=['id'], keep='first')
orders_df = orders_df.rename(columns={'id': 'order_id', 'amount': 'total_amount'})
orders_final = orders_df[['order_id', 'customer_id', 'product_id', 'quantity', 'total_amount', 'status', 'created_at']]
orders_final = clean_dtypes(orders_final)

con.execute("CREATE TABLE fct_orders AS SELECT * FROM orders_final")
print(f"✅ fct_orders : {len(orders_final)} commandes")

# --- dim_customers ---
print("\nChargement de dim_customers depuis MinIO...")
cust_df = load_parquet_from_minio('warehouse', 'silver/customers/data/')
cust_df = cust_df[cust_df['_is_deleted'] == False]
cust_df = cust_df.sort_values('_lsn', ascending=False).drop_duplicates(subset=['customer_id'], keep='first')
cust_final = cust_df[['customer_id', 'name', 'email', 'city', 'created_at', 'updated_at']]
cust_final = clean_dtypes(cust_final)

con.execute("CREATE TABLE dim_customers AS SELECT * FROM cust_final")
print(f"✅ dim_customers : {len(cust_final)} clients")

# --- segments_rfm (Optionnel ECO-08) ---
print("\nCréation de segments_rfm...")
# On prépare un dataframe pour le calcul RFM
rfm_df = orders_final.copy()
rfm_df['created_at'] = pd.to_datetime(rfm_df['created_at']).dt.tz_localize(None)
today = pd.Timestamp.now()
rfm = rfm_df.groupby('customer_id').agg({
    'created_at': lambda x: (today - x.max()).days,
    'order_id': 'count',
    'total_amount': 'sum'
}).reset_index()
rfm.columns = ['customer_id', 'recency', 'frequency', 'monetary']

def segment(row):
    if row['frequency'] > 5 and row['monetary'] > 3000:
        return 'VIP'
    elif row['frequency'] > 2:
        return 'Fidèle'
    else:
        return 'Occasionnel'

rfm['segment'] = rfm.apply(segment, axis=1)
rfm = clean_dtypes(rfm)
con.execute("CREATE TABLE segments_rfm AS SELECT * FROM rfm")
print(f"✅ segments_rfm : {len(rfm)} clients segmentés")

print("\n🎉 Fichier 'shopnova_gold.duckdb' prêt pour Metabase !")
con.close()