CREATE TABLE IF NOT EXISTS silver_products (
    product_id             BIGINT PRIMARY KEY,
    title                  TEXT NOT NULL,
    category               TEXT NOT NULL,
    brand                  TEXT,                    
    sku                    TEXT,
    price                  NUMERIC(10,2) NOT NULL,
    discount_percentage    NUMERIC(5,2),
    rating                 NUMERIC(3,2),
    stock                  INTEGER,
    availability_status    TEXT,
    minimum_order_quantity INTEGER,
    source_created_at      TIMESTAMPTZ,
    source_updated_at      TIMESTAMPTZ,             
    transformed_at         TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver_carts (
    cart_id          BIGINT PRIMARY KEY,
    user_id          BIGINT NOT NULL,
    total            NUMERIC(12,2),
    discounted_total NUMERIC(12,2),
    total_products   INTEGER,
    total_quantity   INTEGER,
    transformed_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver_cart_items (
    cart_id               BIGINT NOT NULL,
    product_id            BIGINT NOT NULL,          
    user_id               BIGINT NOT NULL,
    quantity              INTEGER NOT NULL,
    unit_price            NUMERIC(10,2) NOT NULL,
    discount_percentage   NUMERIC(5,2),
    line_total            NUMERIC(12,2),
    line_discounted_total NUMERIC(12,2),
    transformed_at        TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (cart_id, product_id)               
);