-- Controles qualite sur silver_products
-- Contrat : 0 ligne = OK. Sinon 1 ligne par violation.
-- Colonnes : rule_name, severity, entity_key, details

-- Volumetrie : la table ne doit pas etre vide
SELECT 'products_not_empty'::text           AS rule_name,
       'blocking'::text                     AS severity,
       NULL::text                           AS entity_key,
       'silver_products ne contient aucune ligne'::text AS details
WHERE NOT EXISTS (SELECT 1 FROM silver_products)

UNION ALL

-- Completude reelle : NOT NULL laisse passer la chaine vide
SELECT 'products_mandatory_not_blank', 'reject',
       product_id::text,
       format('title=%L, category=%L', title, category)
FROM silver_products
WHERE btrim(title) = '' OR btrim(category) = ''

UNION ALL

-- Unicite metier : la PK protege product_id, pas le sku
SELECT 'products_sku_unique', 'reject',
       sku,
       format('%s produits partagent ce sku', COUNT(*))
FROM silver_products
WHERE sku IS NOT NULL AND btrim(sku) <> ''
GROUP BY sku
HAVING COUNT(*) > 1

UNION ALL

-- Validite des domaines
SELECT 'products_domain_valid', 'reject',
       product_id::text,
       format('price=%s, discount=%s, rating=%s, stock=%s, min_order=%s',
              price, discount_percentage, rating, stock, minimum_order_quantity)
FROM silver_products
WHERE price < 0
   OR discount_percentage < 0 OR discount_percentage > 100
   OR rating < 0 OR rating > 5
   OR stock < 0
   OR minimum_order_quantity <= 0

UNION ALL

-- Coherence inter-colonnes : le statut doit refleter le stock
SELECT 'products_stock_status_coherent', 'reject',
       product_id::text,
       format('availability_status=%L mais stock=%s', availability_status, stock)
FROM silver_products
WHERE (availability_status = 'Out of Stock' AND stock > 0)
   OR (stock = 0 AND availability_status <> 'Out of Stock')

UNION ALL

-- Fraicheur : une date de mise a jour dans le futur signale un probleme de fuseau
SELECT 'products_updated_not_future', 'reject',
       product_id::text,
       format('source_updated_at=%s', source_updated_at)
FROM silver_products
WHERE source_updated_at > now()

UNION ALL

-- Coherence temporelle : on ne met pas a jour avant de creer
SELECT 'products_updated_after_created', 'reject',
       product_id::text,
       format('created=%s, updated=%s', source_created_at, source_updated_at)
FROM silver_products
WHERE source_updated_at < source_created_at

UNION ALL

-- Completude toleree : suivi du taux de marques absentes, en avertissement
SELECT 'products_brand_completeness', 'warning',
       NULL,
       format('taux de brand nuls = %s pourcent',
              ROUND(100.0 * COUNT(*) FILTER (WHERE brand IS NULL)
                    / NULLIF(COUNT(*), 0), 1))
FROM silver_products
HAVING COUNT(*) FILTER (WHERE brand IS NULL) > 0.30 * COUNT(*);