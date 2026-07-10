-- Runs automatically on first container start (docker-entrypoint-initdb.d).
-- Defines the OLTP source schema that the CDC simulator watches.

CREATE TABLE IF NOT EXISTS customers (
    customer_id     SERIAL PRIMARY KEY,
    full_name       TEXT NOT NULL,
    email           TEXT NOT NULL UNIQUE,
    segment         TEXT NOT NULL DEFAULT 'standard',
    address         TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    product_id      SERIAL PRIMARY KEY,
    sku             TEXT NOT NULL UNIQUE,
    name            TEXT,
    category        TEXT,
    unit_price      NUMERIC(10, 2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES customers(customer_id),
    order_date      TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT NOT NULL DEFAULT 'placed',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    campaign_id     INTEGER
    -- NOTE: discount_code column is added later via ALTER TABLE
    -- during the live schema-evolution demo.
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(order_id),
    product_id      INTEGER REFERENCES products(product_id),
    quantity        INTEGER,
    unit_price      NUMERIC(10, 2)
);

-- Watermark index the CDC simulator polls against.
CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON customers (updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders (updated_at);

-- FK indexes: Postgres does NOT auto-index the referencing side of a foreign
-- key. Without these, order lookups by customer and the order_items joins the
-- batch extract performs degrade to sequential scans as the seed volume grows,
-- and DELETEs/UPDATEs on the referenced tables trigger full scans here.
CREATE INDEX IF NOT EXISTS idx_orders_customer_id     ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_campaign_id     ON orders (campaign_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id   ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items (product_id);

-- Keep updated_at trustworthy at the database level. The CDC simulator's
-- watermark polling is only correct if every UPDATE bumps updated_at; before
-- this trigger, correctness silently depended on every writer remembering to
-- set the column manually — any ad-hoc UPDATE would be invisible to CDC.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_customers_set_updated_at ON customers;
CREATE TRIGGER trg_customers_set_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_orders_set_updated_at ON orders;
CREATE TRIGGER trg_orders_set_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
