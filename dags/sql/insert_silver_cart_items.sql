WITH exploded AS (
    SELECT (c.content->>'id')::BIGINT                      AS cart_id,
           (item->>'id')::BIGINT                           AS product_id,
           (c.content->>'userId')::BIGINT                  AS user_id,
           (item->>'quantity')::INTEGER                    AS quantity,
           (item->>'price')::NUMERIC(10,2)                 AS unit_price,
           (item->>'discountPercentage')::NUMERIC(5,2)     AS discount_percentage,
           (item->>'total')::NUMERIC(12,2)                 AS line_total,
           (item->>'discountedTotal')::NUMERIC(12,2)       AS line_discounted_total
    FROM bronze_carts c,
         LATERAL jsonb_array_elements(c.content->'products') AS item
)
INSERT INTO silver_cart_items (cart_id, product_id, user_id, quantity, unit_price,
                               discount_percentage, line_total, line_discounted_total)
SELECT cart_id,
       product_id,
       MIN(user_id)               AS user_id,
       SUM(quantity)              AS quantity,
       MIN(unit_price)            AS unit_price,
       MIN(discount_percentage)   AS discount_percentage,
       SUM(line_total)            AS line_total,
       SUM(line_discounted_total) AS line_discounted_total
FROM exploded
GROUP BY cart_id, product_id
ON CONFLICT (cart_id, product_id) DO UPDATE
SET user_id               = EXCLUDED.user_id,
    quantity              = EXCLUDED.quantity,
    unit_price            = EXCLUDED.unit_price,
    discount_percentage   = EXCLUDED.discount_percentage,
    line_total            = EXCLUDED.line_total,
    line_discounted_total = EXCLUDED.line_discounted_total,
    transformed_at        = now();