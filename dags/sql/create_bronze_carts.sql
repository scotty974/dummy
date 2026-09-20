CREATE TABLE IF NOT EXISTS bronze_carts (
    cart_id BIGINT PRIMARY KEY,
    content JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT now()
)