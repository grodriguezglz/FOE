#!/usr/bin/env python3
"""
HEB Price Tracker — Local PostgreSQL → Supabase Migration
──────────────────────────────────────────────────────────
This script copies all data from your local heb_products database
to your new Supabase database.

BEFORE RUNNING:
  1. Run schema.sql in the Supabase SQL Editor first
  2. Fill in your Supabase connection details below (or set env vars)
  3. Make sure your local PostgreSQL is running
  4. Run: pip install psycopg2-binary python-dotenv

USAGE:
  python migrate_to_supabase.py
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv

load_dotenv(dotenv_path='../web-app-v2/.env')

# ── Local database (source) ───────────────────────────────────────────────────
LOCAL_DB = dict(
    host     = 'localhost',
    port     = 5432,
    database = 'heb_products',
    user     = os.getenv('LOCAL_DB_USER',     'memorodriguez'),
    password = os.getenv('LOCAL_DB_PASSWORD', ''),
)

# ── Supabase database (destination) ──────────────────────────────────────────
# Get these from: Supabase Dashboard → Settings → Database → Connection string
# Use the "URI" format and paste the values below, or set env vars.
SUPABASE_DB = dict(
    host     = os.getenv('SUPABASE_HOST'),
    port     = int(os.getenv('SUPABASE_PORT', '5432')),
    database = os.getenv('SUPABASE_DB',   'postgres'),
    user     = os.getenv('SUPABASE_USER'),
    password = os.getenv('SUPABASE_PASSWORD'),
    sslmode  = 'require',   # Supabase requires SSL
)

BATCH_SIZE = 500   # rows per batch insert


def connect(config, label):
    try:
        conn = psycopg2.connect(**config, cursor_factory=RealDictCursor)
        print(f"  ✓ Connected to {label}")
        return conn
    except psycopg2.Error as e:
        print(f"  ✗ Failed to connect to {label}: {e}")
        sys.exit(1)


def migrate_products(src_cur, dst_conn):
    print("\n── Migrating products ───────────────────────────────────")
    src_cur.execute("SELECT COUNT(*) AS n FROM products")
    total = src_cur.fetchone()['n']
    print(f"  Found {total:,} products in local DB")

    src_cur.execute("""
        SELECT category_id, category_name, product_id, product_name,
               brand_name, is_own_brand, sku_id, date_added
        FROM products
        ORDER BY id
    """)

    dst_cur  = dst_conn.cursor()
    inserted = 0
    skipped  = 0
    rows     = []

    for row in src_cur:
        rows.append((
            row['category_id'],
            row['category_name'],
            row['product_id'],
            row['product_name'],
            row['brand_name'],
            row['is_own_brand'],
            row['sku_id'],
            row['date_added'],
        ))

        if len(rows) >= BATCH_SIZE:
            n, s = _insert_products_batch(dst_cur, rows)
            inserted += n
            skipped  += s
            rows = []
            print(f"  Progress: {inserted:,} / {total:,} inserted, {skipped} skipped")

    if rows:
        n, s = _insert_products_batch(dst_cur, rows)
        inserted += n
        skipped  += s

    dst_conn.commit()
    print(f"  ✓ Products done: {inserted:,} inserted, {skipped} already existed")
    return inserted


def _insert_products_batch(dst_cur, rows):
    try:
        execute_values(dst_cur, """
            INSERT INTO products
                (category_id, category_name, product_id, product_name,
                 brand_name, is_own_brand, sku_id, date_added)
            VALUES %s
            ON CONFLICT (product_id) DO NOTHING
        """, rows)
        return len(rows), 0
    except psycopg2.Error as e:
        print(f"  ✗ Batch insert error: {e}")
        return 0, len(rows)


def migrate_price_history(src_cur, dst_conn):
    print("\n── Migrating price history ──────────────────────────────")
    src_cur.execute("SELECT COUNT(*) AS n FROM price_history")
    total = src_cur.fetchone()['n']
    print(f"  Found {total:,} price records in local DB")

    src_cur.execute("""
        SELECT product_id, price, recorded_at
        FROM price_history
        ORDER BY recorded_at ASC
    """)

    dst_cur  = dst_conn.cursor()
    inserted = 0
    rows     = []

    for row in src_cur:
        rows.append((
            row['product_id'],
            float(row['price']),
            row['recorded_at'],
        ))

        if len(rows) >= BATCH_SIZE:
            execute_values(dst_cur, """
                INSERT INTO price_history (product_id, price, recorded_at)
                VALUES %s
                ON CONFLICT DO NOTHING
            """, rows)
            inserted += len(rows)
            rows = []
            print(f"  Progress: {inserted:,} / {total:,}")

    if rows:
        execute_values(dst_cur, """
            INSERT INTO price_history (product_id, price, recorded_at)
            VALUES %s
            ON CONFLICT DO NOTHING
        """, rows)
        inserted += len(rows)

    dst_conn.commit()
    print(f"  ✓ Price history done: {inserted:,} records inserted")
    return inserted


def verify(dst_conn):
    print("\n── Verification ─────────────────────────────────────────")
    cur = dst_conn.cursor()
    cur.execute("SELECT COUNT(*) AS n FROM products")
    p = cur.fetchone()['n']
    cur.execute("SELECT COUNT(*) AS n FROM price_history")
    ph = cur.fetchone()['n']
    print(f"  Supabase now has: {p:,} products, {ph:,} price records")


def main():
    print("=" * 55)
    print("  HEB Price Tracker — Migration to Supabase")
    print("=" * 55)

    # Warn if Supabase creds look like placeholders
    if not SUPABASE_DB['password'] or not SUPABASE_DB['host']:
        print("\n  ⚠️  Please fill in your Supabase credentials!")
        print("  Edit the SUPABASE_DB block in this file,")
        print("  or set SUPABASE_HOST / SUPABASE_PASSWORD env vars in .env")
        sys.exit(1)

    print("\n  Connecting...")
    src_conn = connect(LOCAL_DB,    'local PostgreSQL')
    dst_conn = connect(SUPABASE_DB, 'Supabase')

    src_cur = src_conn.cursor()

    migrate_products(src_cur, dst_conn)
    migrate_price_history(src_cur, dst_conn)
    verify(dst_conn)

    src_conn.close()
    dst_conn.close()

    print("\n  ✅ Migration complete!")
    print("  Next: update your .env with Supabase DB credentials")
    print("        then deploy to Railway.\n")


if __name__ == '__main__':
    main()
