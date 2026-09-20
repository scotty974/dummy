CREATE TABLE IF NOT EXISTS bronze_products (
    product_id BIGINT PRIMARY KEY,
    content JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT now()
);