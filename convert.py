import duckdb

# On utilise la version 1.5.5, qui sait lire le format 999 (v2.0.0)
# On se connecte au fichier en lecture seule pour ne pas l'abîmer
con = duckdb.connect('shopnova_gold.duckdb', read_only=True)

# On exporte les données dans un dossier temporaire (format universel)
con.execute("EXPORT DATABASE 'tmp_export'")
con.close()

# On crée un nouveau fichier avec un format de stockage plus ancien (v1.0.0)
con2 = duckdb.connect('shopnova_gold_compat.duckdb')
con2.execute("IMPORT DATABASE 'tmp_export'")
con2.close()

print("✅ Fichier 'shopnova_gold_compat.duckdb' créé avec succès (format v1.0.0).")