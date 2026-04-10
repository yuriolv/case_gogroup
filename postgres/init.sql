CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.smartphones (
    product_id BIGINT NOT NULL,
    record_hash TEXT NOT NULL,

    title TEXT,
    category TEXT,
    brand TEXT,

    price NUMERIC,
    original_price NUMERIC,
    discount_pct NUMERIC,
    price_final NUMERIC,

    stock INT,
    rating NUMERIC,
    reviews_count INT,

    free_shipping BOOLEAN,
    condition TEXT,

    seller_id BIGINT,
    seller_name TEXT,
    seller_state TEXT,

    collected_at TIMESTAMP NOT NULL,
    inserted_at TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (product_id, record_hash)
);