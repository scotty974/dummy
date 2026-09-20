-- Controles qualite sur silver_carts
-- Contrat : 0 ligne = OK. Sinon 1 ligne par violation.
-- Colonnes : rule_name, severity, entity_key, details

-- Volumetrie : la table ne doit pas etre vide
SELECT 'carts_not_empty'::text              AS rule_name,
       'blocking'::text                     AS severity,
       NULL::text                           AS entity_key,
       'silver_carts ne contient aucune ligne'::text AS details
WHERE NOT EXISTS (SELECT 1 FROM silver_carts)

UNION ALL

-- Validite : pas de montant negatif
SELECT 'carts_amounts_non_negative', 'reject',
       cart_id::text,
       format('total=%s, discounted_total=%s', total, discounted_total)
FROM silver_carts
WHERE total < 0 OR discounted_total < 0

UNION ALL

-- Coherence : une remise ne peut pas augmenter le montant
SELECT 'carts_discounted_le_total', 'reject',
       cart_id::text,
       format('discounted_total=%s superieur a total=%s', discounted_total, total)
FROM silver_carts
WHERE discounted_total > total

UNION ALL

-- Validite : les compteurs doivent etre strictement positifs
SELECT 'carts_counters_positive', 'reject',
       cart_id::text,
       format('total_products=%s, total_quantity=%s', total_products, total_quantity)
FROM silver_carts
WHERE total_products <= 0 OR total_quantity <= 0

UNION ALL

-- Coherence : chaque produit compte au moins une unite
SELECT 'carts_quantity_ge_products', 'reject',
       cart_id::text,
       format('total_quantity=%s inferieur a total_products=%s', total_quantity, total_products)
FROM silver_carts
WHERE total_quantity < total_products;