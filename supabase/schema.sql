-- ─────────────────────────────────────────────────────────────────────────────
--  HEB Price Tracker — Supabase Schema
--
--  Run this in the Supabase SQL Editor (supabase.com → your project → SQL Editor)
--  before running the migration script.
-- ─────────────────────────────────────────────────────────────────────────────


-- ── Products table ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    id            SERIAL PRIMARY KEY,
    category_id   TEXT        NOT NULL,
    category_name TEXT        NOT NULL,
    product_id    TEXT        NOT NULL UNIQUE,
    product_name  TEXT        NOT NULL,
    brand_name    TEXT,
    is_own_brand  BOOLEAN     DEFAULT FALSE,
    sku_id        TEXT        NOT NULL,
    date_added    TIMESTAMPTZ DEFAULT NOW()
);

-- ── Price history table ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS price_history (
    id          SERIAL PRIMARY KEY,
    product_id  TEXT           NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
    price       DECIMAL(10, 2) NOT NULL,
    recorded_at TIMESTAMPTZ    DEFAULT NOW()
);

-- ── Indexes for fast querying ─────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_product_id       ON products(product_id);
CREATE INDEX IF NOT EXISTS idx_category         ON products(category_id);
CREATE INDEX IF NOT EXISTS idx_category_name    ON products(category_name);
CREATE INDEX IF NOT EXISTS idx_price_product    ON price_history(product_id);
CREATE INDEX IF NOT EXISTS idx_price_recorded   ON price_history(product_id, recorded_at DESC);

-- ── Row Level Security (RLS) — read-only public access ───────────────────────
-- This lets anyone read the data (for your public dashboard)
-- but nobody can write via the public API key.

ALTER TABLE products      ENABLE ROW LEVEL SECURITY;
ALTER TABLE price_history ENABLE ROW LEVEL SECURITY;

-- Allow public SELECT on both tables
CREATE POLICY "Public read products"
    ON products FOR SELECT
    USING (true);

CREATE POLICY "Public read price_history"
    ON price_history FOR SELECT
    USING (true);

-- ── Verify ───────────────────────────────────────────────────────────────────
SELECT 'Schema created successfully' AS status;
