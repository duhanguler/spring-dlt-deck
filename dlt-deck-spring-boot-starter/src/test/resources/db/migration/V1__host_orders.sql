CREATE TABLE IF NOT EXISTS host_orders (
    id          VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    amount_cents BIGINT NOT NULL
);
