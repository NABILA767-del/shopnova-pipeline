-- ============================================================
-- Initialisation de la base ShopNova (PostgreSQL)
-- Exécuté automatiquement au premier démarrage du conteneur
-- ============================================================

-- 1. Créer l'utilisateur Debezium (CDC)
CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'ShopNova2026!';

-- 2. Donner les droits de lecture sur le schéma public
GRANT USAGE ON SCHEMA public TO debezium;

-- ============================================================
-- TABLE customers (clients)
-- ============================================================
CREATE TABLE customers (
    id              SERIAL PRIMARY KEY,
    nom             VARCHAR(100) NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    ville           VARCHAR(100),
    date_inscription DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================================================
-- TABLE products (produits)
-- ============================================================
CREATE TABLE products (
    id          SERIAL PRIMARY KEY,
    nom         VARCHAR(150) NOT NULL,
    categorie   VARCHAR(80) NOT NULL,
    prix        NUMERIC(10, 2) NOT NULL CHECK (prix >= 0),
    stock       INTEGER NOT NULL DEFAULT 0 CHECK (stock >= 0),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================================================
-- TABLE orders (commandes)
-- ============================================================
CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    product_id  INTEGER NOT NULL REFERENCES products(id),
    quantite    INTEGER NOT NULL CHECK (quantite > 0),
    montant     NUMERIC(10, 2) NOT NULL CHECK (montant >= 0),
    statut      VARCHAR(20) NOT NULL DEFAULT 'creee'
                CHECK (statut IN ('creee', 'payee', 'expediee', 'annulee', 'remboursee')),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index pour accélérer les requêtes
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_statut ON orders(statut);
CREATE INDEX idx_orders_created_at ON orders(created_at);

-- ============================================================
-- PUBLICATION PostgreSQL pour Debezium (CDC)
-- ============================================================
CREATE PUBLICATION shopnova_pub
    FOR TABLE customers, products, orders;

-- ============================================================
-- DROITS pour Debezium
-- ============================================================
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO debezium;

-- ============================================================
-- DONNÉES DE TEST MINIMALES (seront complétées par simulateur.py)
-- ============================================================
INSERT INTO customers (nom, email, ville) VALUES
    ('Client Test 1', 'test1@shopnova.fr', 'Paris'),
    ('Client Test 2', 'test2@shopnova.fr', 'Lyon');

INSERT INTO products (nom, categorie, prix, stock) VALUES
    ('Produit Test 1', 'Électronique', 99.99, 100),
    ('Produit Test 2', 'Vêtements', 29.99, 50);