import duckdb
import psycopg
from dotenv import load_dotenv

load_dotenv()

# 1. Récupérer les commandes Gold
con_duck = duckdb.connect('shopnova_gold.duckdb', read_only=True)
gold_df = con_duck.execute("SELECT order_id, total_amount FROM fct_orders").fetchdf()
con_duck.close()

order_ids = gold_df['order_id'].astype(int).tolist()
sum_gold = float(gold_df['total_amount'].sum())

# 2. Récupérer les montants correspondants dans Postgres
conn_pg = psycopg.connect(host="localhost", port=5435, dbname="shopnova", user="shopnova", password="ShopNova2026!")
cur = conn_pg.cursor()
cur.execute("SELECT id, montant FROM orders WHERE id = ANY(%s)", (order_ids,))
pg_rows = cur.fetchall()
conn_pg.close()

pg_dict = {int(oid): float(m) for oid, m in pg_rows}
sum_pg = sum(pg_dict.values())

print(f"Commandes dans Gold         : {len(gold_df)}")
print(f"Commandes trouvées en source : {len(pg_rows)}")
print()
print(f"CA Gold (somme des montants) : {sum_gold:.2f} €")
print(f"CA Source (mêmes commandes)  : {sum_pg:.2f} €")
print()

diff = abs(sum_gold - sum_pg)
if diff < 0.01:
    print("✅ RÉCONCILIATION RÉUSSIE : les montants sont identiques sur le même périmètre.")
else:
    print(f"❌ Écart total : {diff:.2f} €")
    mismatches = []
    for _, row in gold_df.iterrows():
        oid = int(row['order_id'])
        g = float(row['total_amount'])
        p = pg_dict.get(oid)
        if p is None or abs(g - p) > 0.01:
            mismatches.append((oid, g, p))
    print(f"Commandes avec écart : {len(mismatches)}")
    for oid, g, p in mismatches[:5]:
        print(f"  - id={oid} : Gold={g} | Source={p}")