import duckdb

# Connexion au fichier (format 999) avec DuckDB 2.0.0
con = duckdb.connect('shopnova_gold.duckdb', read_only=True)

# Export au format CSV + schema.sql (universel)
con.execute("EXPORT DATABASE 'tmp_export'")
con.close()

print("✅ Export terminé dans le dossier 'tmp_export'")