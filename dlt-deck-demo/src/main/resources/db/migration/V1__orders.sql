CREATE TABLE IF NOT EXISTS payment_orders (
    order_id      VARCHAR(64) PRIMARY KEY,
    customer_name VARCHAR(128) NOT NULL,
    currency      VARCHAR(8) NOT NULL,
    amount_cents  BIGINT NOT NULL,
    stage         VARCHAR(32) NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);
