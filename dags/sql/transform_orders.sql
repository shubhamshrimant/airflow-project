-- Rebuild the analytics table so repeated runs are deterministic.
CREATE TABLE IF NOT EXISTS analytics_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    order_date DATE NOT NULL,
    product TEXT NOT NULL,
    category TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    order_value NUMERIC(14, 2) NOT NULL,
    transformed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

TRUNCATE TABLE analytics_orders;

INSERT INTO analytics_orders (
    order_id, customer_id, order_date, product, category,
    quantity, unit_price, order_value, transformed_at
)
SELECT
    order_id,
    customer_id,
    order_date,
    product,
    category,
    quantity,
    unit_price,
    quantity * unit_price AS order_value,
    NOW()
FROM staging_orders;
