INSERT INTO silver_products (product_id, title, category, brand, sku, price,
                             discount_percentage, rating, stock, availability_status,
                             minimum_order_quantity, source_created_at, source_updated_at)
SELECT (content->>'id')::BIGINT,
       content->>'title',
       content->>'category',
       content->>'brand',
       content->>'sku',
       (content->>'price')::NUMERIC(10,2),
       (content->>'discountPercentage')::NUMERIC(5,2),
       (content->>'rating')::NUMERIC(3,2),
       (content->>'stock')::INTEGER,
       content->>'availabilityStatus',
       (content->>'minimumOrderQuantity')::INTEGER,
       (content->'meta'->>'createdAt')::TIMESTAMPTZ,
       (content->'meta'->>'updatedAt')::TIMESTAMPTZ
FROM bronze_products
ON CONFLICT (product_id) DO UPDATE
SET title = EXCLUDED.title,
    price = EXCLUDED.price,
    stock = EXCLUDED.stock,
    source_updated_at = EXCLUDED.source_updated_at,
    transformed_at = now();