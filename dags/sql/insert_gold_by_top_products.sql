TRUNCATE TABLE gold_top_products;

INSERT INTO gold_top_products (rang, product_id, title, category, brand,
                               quantite_vendue, ca_net, nb_paniers)
SELECT ROW_NUMBER() OVER (ORDER BY SUM(i.line_discounted_total) DESC) AS rang,
       p.product_id, p.title, p.category, p.brand,
       SUM(i.quantity)                AS quantite_vendue,
       SUM(i.line_discounted_total)   AS ca_net,
       COUNT(DISTINCT i.cart_id)      AS nb_paniers
FROM silver_cart_items i
JOIN silver_products p USING (product_id)
GROUP BY p.product_id, p.title, p.category, p.brand
ORDER BY ca_net DESC
LIMIT 20;