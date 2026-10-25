# ShopNova — Pipeline de données (Sujet 2)

## Membres
- Nabila Imouzaz
- Jihane Bouazza
- Asmaa Aandadi

## Sujet
E-commerce ShopNova — capture de changements (CDC) depuis PostgreSQL.

## Architecture
(Schéma à insérer ici — voir `rapport/architecture.png`)

## Lancement (≤ 5 commandes)
```bash
cp .env.example .env
docker compose up -d
python source/simulateur.py
python ingestion/bronze_writer.py