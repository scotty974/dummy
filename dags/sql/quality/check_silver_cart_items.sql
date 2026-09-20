-- Controles qualite sur silver_cart_items
-- Contrat : 0 ligne = OK. Sinon 1 ligne par violation.
-- Colonnes : rule_name, severity, entity_key, details

-- Integrite referentielle sur la donnee pivot : LE controle central
SELECT 'items_product_fk'::text AS rule_name,
       'reject'::text           AS severity,
       format('%s/%s', i.cart_id, i.product_id)::text AS entity_key,
       format('product_id=%s absent de silver_products', i.product_id)::text AS details
FROM silver_cart_items i
LEFT JOIN silver_products p USING (product_id)
WHERE p.product_id IS NULL

UNION ALL

-- Orphelins d'en-tete
SELECT 'items_cart_fk', 'reject',
       format('%s/%s', i.cart_id, i.product_id),
       format('cart_id=%s absent de silver_carts', i.cart_id)
FROM silver_cart_items i
LEFT JOIN silver_carts c USING (cart_id)
WHERE c.cart_id IS NULL

UNION ALL

-- Reconciliation des montants : LE controle qui valide l'eclatement
SELECT 'items_reconciliation', 'blocking',
       c.cart_id::text,
       format('total en-tete=%s, somme des lignes=%s, ecart=%s',
              c.total, SUM(i.line_total), ROUND(c.total - SUM(i.line_total), 2))
FROM silver_carts c
JOIN silver_cart_items i USING (cart_id)
GROUP BY c.cart_id, c.total
HAVING ABS(c.total - SUM(i.line_total)) > 0.01

UNION ALL

-- Reconciliation des quantites
SELECT 'items_quantity_matches_cart', 'blocking',
       c.cart_id::text,
       format('total_quantity en-tete=%s, somme des lignes=%s',
              c.total_quantity, SUM(i.quantity))
FROM silver_carts c
JOIN silver_cart_items i USING (cart_id)
GROUP BY c.cart_id, c.total_quantity
HAVING c.total_quantity <> SUM(i.quantity)

UNION ALL

-- Validite des domaines
SELECT 'items_domain_valid', 'reject',
       format('%s/%s', cart_id, product_id),
       format('quantity=%s, unit_price=%s, discount_percentage=%s',
              quantity, unit_price, discount_percentage)
FROM silver_cart_items
WHERE quantity <= 0
   OR unit_price < 0
   OR discount_percentage < 0
   OR discount_percentage > 100

UNION ALL

-- Coherence interne du calcul de ligne
SELECT 'items_line_total_consistent', 'reject',
       format('%s/%s', cart_id, product_id),
       format('line_total=%s, attendu=%s', line_total, ROUND(unit_price * quantity, 2))
FROM silver_cart_items
WHERE ABS(line_total - unit_price * quantity) > 0.01

UNION ALL

-- Coherence : la remise ne peut pas augmenter le montant de ligne
SELECT 'items_discounted_le_total', 'reject',
       format('%s/%s', cart_id, product_id),
       format('line_discounted_total=%s superieur a line_total=%s',
              line_discounted_total, line_total)
FROM silver_cart_items
WHERE line_discounted_total > line_total

UNION ALL

-- Propagation : le user_id de la ligne doit suivre celui de l'en-tete
SELECT 'items_user_matches_cart', 'reject',
       format('%s/%s', i.cart_id, i.product_id),
       format('user_id ligne=%s, user_id en-tete=%s', i.user_id, c.user_id)
FROM silver_cart_items i
JOIN silver_carts c USING (cart_id)
WHERE i.user_id <> c.user_id;