CREATE TABLE IF NOT EXISTS spfy_products (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    product_category TEXT,
    status BOOLEAN NOT NULL DEFAULT FALSE,
    sku TEXT NOT NULL
);