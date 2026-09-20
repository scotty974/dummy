INSERT INTO silver_carts (cart_id, user_id, total, discounted_total,
                          total_products, total_quantity)
SELECT (content->>'id')::BIGINT,
       (content->>'userId')::BIGINT,
       (content->>'total')::NUMERIC(12,2),
       (content->>'discountedTotal')::NUMERIC(12,2),
       (content->>'totalProducts')::INTEGER,
       (content->>'totalQuantity')::INTEGER
FROM bronze_carts
ON CONFLICT (cart_id) DO UPDATE
SET user_id          = EXCLUDED.user_id,
    total            = EXCLUDED.total,
    discounted_total = EXCLUDED.discounted_total,
    total_products   = EXCLUDED.total_products,
    total_quantity   = EXCLUDED.total_quantity,
    transformed_at   = now();