import duckdb

# Connexion à la nouvelle base
con = duckdb.connect('shopnova_gold_v1.duckdb')

# 1. Installer et charger l'extension httpfs pour S3
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

# 2. Configurer les paramètres MinIO
con.execute("SET s3_region='us-east-1'")
con.execute("SET s3_endpoint='localhost:9000'")
con.execute("SET s3_access_key_id='minioadmin'")
con.execute("SET s3_secret_access_key='ShopNova2026!'")
con.execute("SET s3_use_ssl=false")
con.execute("SET s3_url_style='path'")

# 3. Importer les données et le schéma
con.execute("IMPORT DATABASE 'tmp_export'")

con.close()
print("✅ Nouveau fichier 'shopnova_gold_v1.duckdb' créé avec succès !")