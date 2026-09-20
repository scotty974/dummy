TRUNCATE TABLE gold_sales_by_category;

INSERT INTO gold_sales_by_category (category, nb_produits, nb_paniers, quantite_vendue,
                                    ca_brut, ca_net, remise_moyenne_pct, panier_moyen)
SELECT p.category,
       COUNT(DISTINCT i.product_id)        AS nb_produits,
       COUNT(DISTINCT i.cart_id)           AS nb_paniers,
       SUM(i.quantity)                     AS quantite_vendue,
       SUM(i.line_total)                   AS ca_brut,
       SUM(i.line_discounted_total)        AS ca_net,
       ROUND(100.0 * (1 - SUM(i.line_discounted_total)
                          / NULLIF(SUM(i.line_total), 0)), 2) AS remise_moyenne_pct,
       ROUND(SUM(i.line_discounted_total)
             / NULLIF(COUNT(DISTINCT i.cart_id), 0), 2)       AS panier_moyen
FROM silver_cart_items i
JOIN silver_products p USING (product_id)      -- la jointure pivot
GROUP BY p.category;