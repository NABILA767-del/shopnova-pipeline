"""
Simulateur de données pour ShopNova (Sujet 2 - E-commerce)
-----------------------------------------------------------
Génère :
  - 5 000 clients (Faker)
  - 200 produits (Faker)
  - des commandes en continu
  - fait évoluer >= 20 % des commandes (payee, expediee, annulee, remboursee)

Exigences couvertes :
  - ECO-01 : >= 5 000 clients, 200 produits, commandes créées en continu
  - ECO-02 : >= 20 % des commandes changent de statut

Usage :
    python source/simulateur.py
"""

import os
import random
import time
import logging
from datetime import datetime

import psycopg as psycopg2   # psycopg v3, rétro-compatible avec l'API psycopg2
from faker import Faker
from dotenv import load_dotenv

# ============================================================
# Configuration
# ============================================================
load_dotenv()

DB_HOST = "localhost"
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "shopnova")
DB_USER = os.getenv("POSTGRES_USER", "shopnova")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ShopNova2026!")

NB_CLIENTS = 5000
NB_PRODUITS = 200
NB_COMMANDES_INITIALES = 500
DELAI_ENTRE_COMMANDES = 2.0      # secondes entre deux nouvelles commandes
PROBA_EVOLUTION = 0.25           # 25 % des commandes évoluent (>= 20 % exigé)
DUREE_SECONDES = 3600            # 1 heure de simulation (Ctrl+C pour arrêter)

STATUTS_POSSIBLES = ["payee", "expediee", "annulee", "remboursee"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("simulateur")

fake = Faker("fr_FR")


# ============================================================
# Connexion PostgreSQL
# ============================================================
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


# ============================================================
# 1. Initialisation : clients + produits + commandes de départ
# ============================================================
def seed_clients(cur):
    log.info("Insertion de %d clients...", NB_CLIENTS)
    for i in range(NB_CLIENTS):
        cur.execute(
            """
            INSERT INTO customers (nom, email, ville, date_inscription)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
            """,
            (
                fake.name(),
                f"client{i}@shopnova.fr",
                fake.city(),
                fake.date_between(start_date="-3y", end_date="today"),
            ),
        )
    log.info("✅ %d clients insérés", NB_CLIENTS)


def seed_products(cur):
    log.info("Insertion de %d produits...", NB_PRODUITS)
    categories = ["Électronique", "Vêtements", "Maison", "Sport", "Livres", "Jouets"]
    for i in range(NB_PRODUITS):
        cur.execute(
            """
            INSERT INTO products (nom, categorie, prix, stock)
            VALUES (%s, %s, %s, %s)
            """,
            (
                fake.catch_phrase(),
                random.choice(categories),
                round(random.uniform(5.0, 500.0), 2),
                random.randint(0, 1000),
            ),
        )
    log.info("✅ %d produits insérés", NB_PRODUITS)


def seed_orders(cur, nb):
    log.info("Insertion de %d commandes initiales...", nb)
    for _ in range(nb):
        customer_id = random.randint(1, NB_CLIENTS)
        product_id = random.randint(1, NB_PRODUITS)
        quantite = random.randint(1, 5)
        prix = random.uniform(5.0, 500.0)
        montant = round(prix * quantite, 2)
        cur.execute(
            """
            INSERT INTO orders (customer_id, product_id, quantite, montant, statut)
            VALUES (%s, %s, %s, %s, 'creee')
            """,
            (customer_id, product_id, quantite, montant),
        )
    log.info("✅ %d commandes initiales insérées", nb)


# ============================================================
# 2. Boucle : nouvelles commandes + évolution des statuts
# ============================================================
def creer_commande(cur):
    customer_id = random.randint(1, NB_CLIENTS)
    product_id = random.randint(1, NB_PRODUITS)
    quantite = random.randint(1, 5)
    prix = random.uniform(5.0, 500.0)
    montant = round(prix * quantite, 2)
    cur.execute(
        """
        INSERT INTO orders (customer_id, product_id, quantite, montant, statut)
        VALUES (%s, %s, %s, %s, 'creee')
        RETURNING id
        """,
        (customer_id, product_id, quantite, montant),
    )
    return cur.fetchone()[0]


def faire_evoluer_commande(cur):
    """
    Choisit une commande au hasard et la fait évoluer vers un statut final.
    """
    cur.execute(
        """
        SELECT id, statut FROM orders
        WHERE statut IN ('creee', 'payee')
        ORDER BY random()
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        return None
    order_id, _ = row
    nouveau_statut = random.choice(STATUTS_POSSIBLES)
    cur.execute(
        """
        UPDATE orders
        SET statut = %s, updated_at = NOW()
        WHERE id = %s
        """,
        (nouveau_statut, order_id),
    )
    return order_id, nouveau_statut


def boucle_principale(conn, cur):
    log.info("🚀 Démarrage de la boucle de simulation (Ctrl+C pour arrêter)")
    debut = time.time()
    nb_commandes_creees = 0
    nb_evolutions = 0

    try:
        while time.time() - debut < DUREE_SECONDES:
            # 1. Créer une nouvelle commande
            new_id = creer_commande(cur)
            nb_commandes_creees += 1

            # 2. Avec probabilité PROBA_EVOLUTION, faire évoluer une commande
            if random.random() < PROBA_EVOLUTION:
                evo = faire_evoluer_commande(cur)
                if evo:
                    nb_evolutions += 1
                    log.info("Commande %s → %s", evo[0], evo[1])

            conn.commit()

            # Log de progression toutes les 10 commandes
            if nb_commandes_creees % 10 == 0:
                taux = (nb_evolutions / nb_commandes_creees * 100) if nb_commandes_creees else 0
                log.info(
                    "Progression : %d commandes créées, %d évolutions (%.1f %%)",
                    nb_commandes_creees, nb_evolutions, taux,
                )

            time.sleep(DELAI_ENTRE_COMMANDES)

    except KeyboardInterrupt:
        log.info("⏹️  Interruption manuelle")
    finally:
        taux = (nb_evolutions / nb_commandes_creees * 100) if nb_commandes_creees else 0
        log.info(
            "📊 Bilan : %d commandes créées, %d évolutions (%.1f %%)",
            nb_commandes_creees, nb_evolutions, taux,
        )


# ============================================================
# Point d'entrée
# ============================================================
def main():
    log.info("Connexion à PostgreSQL %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # Vérifier si la base est déjà seedée
        cur.execute("SELECT COUNT(*) FROM customers")
        nb = cur.fetchone()[0]
        if nb < NB_CLIENTS:
            seed_clients(cur)
            seed_products(cur)
            conn.commit()
        else:
            log.info("✅ Base déjà seedée (%d clients)", nb)

        cur.execute("SELECT COUNT(*) FROM orders")
        nb_orders = cur.fetchone()[0]
        if nb_orders < NB_COMMANDES_INITIALES:
            seed_orders(cur, NB_COMMANDES_INITIALES - nb_orders)
            conn.commit()

        # --- AJOUT POUR ECO-02 ---
        cur.execute("SELECT COUNT(*) FROM orders WHERE updated_at > created_at")
        nb_evo = cur.fetchone()[0]
        if nb_evo < 100: # Si moins de 100 commandes ont évolué, on force l'initialisation
            log.info("🔄 Initialisation : Mise à jour de 30%% des commandes initiales...")
            cur.execute("""
                UPDATE orders 
                SET statut = (ARRAY['payee', 'expediee', 'annulee', 'remboursee'])[floor(random() * 4 + 1)],
                    updated_at = NOW()
                WHERE id IN (SELECT id FROM orders ORDER BY random() LIMIT 150)
            """)
            conn.commit()
        # --------------------------

        # Lancer la boucle
        boucle_principale(conn, cur)

    finally:
        cur.close()
        conn.close()
        log.info("Connexion fermée.")


if __name__ == "__main__":
    main()