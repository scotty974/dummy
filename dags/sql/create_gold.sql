CREATE TABLE IF NOT EXISTS gold_sales_by_category (
    category           TEXT PRIMARY KEY,
    nb_produits        INTEGER,
    nb_paniers         INTEGER,
    quantite_vendue    BIGINT,
    ca_brut            NUMERIC(14,2),
    ca_net             NUMERIC(14,2),
    remise_moyenne_pct NUMERIC(5,2),
    panier_moyen       NUMERIC(12,2),
    computed_at        TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_top_products (
    rang            INTEGER PRIMARY KEY,
    product_id      BIGINT NOT NULL,
    title           TEXT NOT NULL,
    category        TEXT NOT NULL,
    brand           TEXT,
    quantite_vendue BIGINT,
    ca_net          NUMERIC(14,2),
    nb_paniers      INTEGER,
    computed_at     TIMESTAMPTZ DEFAULT now()
);