CREATE TABLE IF NOT EXISTS bronze_products (
    id SERIAL PRIMARY KEY,
    type TEXT,
    content JSONB
);