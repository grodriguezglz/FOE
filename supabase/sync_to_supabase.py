#!/usr/bin/env python3
"""
HEB Price Tracker — Incremental Sync to Supabase
─────────────────────────────────────────────────
Runs after the daily scrape. Copies only NEW records
(products and price_history) from local PostgreSQL to Supabase
since the last sync. Safe to run multiple times — skips duplicates.

USAGE:
  python sync_to_supabase.py
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv
from datetime import datetime

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../web-app-v2/.env'), override=True)

# ── Local database (source) ───────────────────────────────────────────────────
LOCAL_DB = dict(
    host     = os.getenv('LOCAL_DB_HOST',     'localhost'),
    port     = int(os.getenv('LOCAL_DB_PORT', '5432')),
    database = os.getenv('LOCAL_DB_NAME',     'heb_products'),
    user     = os.getenv('LOCAL_DB_USER',     'postgres'),
    password = os.getenv('LOCAL_DB_PASSWORD', ''),
)

# ── Supabase database (destination) ──────────────────────────────────────────
SUPABASE_DB = dict(
    host     = os.getenv('SUPABASE_HOST'),
    port     = int(os.getenv('SUPABASE_PORT', '5432')),
    database = os.getenv('SUPABASE_DB',       'postgres'),
    user     = os.getenv('SUPABASE_USER'),
    password = os.getenv('SUPABASE_PASSWORD'),
    sslmode  = 'require',
)

BATCH_SIZE = 500


def connect(config, label):
    try:
        conn = psycopg2.connect(**config, cursor_factory=RealDictCursor)
        print(f"  ✓ Connected to {label}")
        return conn
    except psycopg2.Error as e:
        print(f"  ✗ Failed to connect to {label}: {e}")
        sys.exit(1)


def get_last_synced_at(dst_conn):
    """Find the most recent recorded_at in Supabase price_history."""
    cur = dst_conn.cursor()
    cur.execute("SELECT MAX(recorded_at) AS last FROM price_history")
    row = cur.fetchone()
    return row['last'] if row and row['last'] else None


def sync_new_products(src_conn, dst_conn):
    """Sync any products that don't yet exist in Supabase."""
    print("\n── Syncing new products ─────────────────────────────────")
    src_cur = src_conn.cursor()
    dst_cur = dst_conn.cursor()

    # Get product_ids already in Supabase
    dst_cur.execute("SELECT product_id FROM products")
    existing = {r['product_id'] for r in dst_cur.fetchall()}

    src_cur.execute("""
        SELECT category_id, category_name, product_id, product_name,
               brand_name, is_own_brand, sku_id, date_added
        FROM products
        ORDER BY id
    """)
    all_products = src_cur.fetchall()

    new_rows = [
        (r['category_id'], r['category_name'], r['product_id'], r['product_name'],
         r['brand_name'], r['is_own_brand'], r['sku_id'], r['date_added'])
        for r in all_products
        if r['product_id'] not in existing
    ]

    if not new_rows:
        print("  ✓ No new products to sync")
        return 0

    inserted = 0
    for i in range(0, len(new_rows), BATCH_SIZE):
        batch = new_rows[i:i + BATCH_SIZE]
        execute_values(dst_cur, """
            INSERT INTO products
                (category_id, category_name, product_id, product_name,
                 brand_name, is_own_brand, sku_id, date_added)
            VALUES %s
            ON CONFLICT (product_id) DO NOTHING
        """, batch)
        inserted += len(batch)

    dst_conn.commit()
    print(f"  ✓ Inserted {inserted:,} new products")
    return inserted


def sync_new_prices(src_conn, dst_conn, since):
    """Sync price_history records newer than `since` timestamp."""
    print("\n── Syncing new price records ────────────────────────────")

    src_cur = src_conn.cursor()
    dst_cur = dst_conn.cursor()

    if since:
        print(f"  Last sync: {since.strftime('%Y-%m-%d %H:%M:%S')}")
        src_cur.execute("""
            SELECT product_id, price, recorded_at
            FROM price_history
            WHERE recorded_at > %s
            ORDER BY recorded_at ASC
        """, (since,))
    else:
        print("  No prior sync found — syncing all records")
        src_cur.execute("""
            SELECT product_id, price, recorded_at
            FROM price_history
            ORDER BY recorded_at ASC
        """)

    rows = []
    inserted = 0

    for row in src_cur:
        rows.append((row['product_id'], float(row['price']), row['recorded_at']))

        if len(rows) >= BATCH_SIZE:
            execute_values(dst_cur, """
                INSERT INTO price_history (product_id, price, recorded_at)
                VALUES %s
                ON CONFLICT DO NOTHING
            """, rows)
            inserted += len(rows)
            rows = []
            print(f"  Progress: {inserted:,} records synced...")

    if rows:
        execute_values(dst_cur, """
            INSERT INTO price_history (product_id, price, recorded_at)
            VALUES %s
            ON CONFLICT DO NOTHING
        """, rows)
        inserted += len(rows)

    dst_conn.commit()

    if inserted == 0:
        print("  ✓ No new price records to sync")
    else:
        print(f"  ✓ Synced {inserted:,} new price records")

    return inserted


def main():
    print("=" * 55)
    print("  HEB Price Tracker — Incremental Sync to Supabase")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    if not SUPABASE_DB['password'] or not SUPABASE_DB['host']:
        print("\n  ⚠️  Supabase credentials missing in .env")
        sys.exit(1)

    print("\n  Connecting...")
    src_conn = connect(LOCAL_DB,    'local PostgreSQL')
    dst_conn = connect(SUPABASE_DB, 'Supabase')

    # Find where we left off
    last_synced = get_last_synced_at(dst_conn)

    sync_new_products(src_conn, dst_conn)
    sync_new_prices(src_conn, dst_conn, last_synced)

    src_conn.close()
    dst_conn.close()

    print("\n  ✅ Sync complete!\n")


if __name__ == '__main__':
    main()
