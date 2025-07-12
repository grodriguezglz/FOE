#!/usr/bin/env python3
"""
Migrate SQLite database to PostgreSQL with exact schema matching
"""

import sqlite3
import psycopg2
from psycopg2.extras import execute_values
import sys
from datetime import datetime

def migrate_sqlite_to_postgres():
    """Migrate SQLite database to PostgreSQL"""
    
    # File paths
    sqlite_path = '/Users/memorodriguez/git/Groceries_data/heb_products.db'
    
    # PostgreSQL connection parameters
    pg_params = {
        'host': 'localhost',
        'port': 5432,
        'database': 'heb_products',
        'user': 'memorodriguez',  # Your system username
        'password': ''            # No password needed
    }
    
    print("=== SQLite to PostgreSQL Migration ===")
    print(f"Source: {sqlite_path}")
    print(f"Target: PostgreSQL {pg_params['database']}")
    print("-" * 50)
    
    try:
        # Connect to SQLite
        print("Connecting to SQLite database...")
        sqlite_conn = sqlite3.connect(sqlite_path)
        sqlite_cursor = sqlite_conn.cursor()
        
        # Connect to PostgreSQL
        print("Connecting to PostgreSQL database...")
        pg_conn = psycopg2.connect(**pg_params)
        pg_cursor = pg_conn.cursor()
        
        # Check existing data in SQLite
        sqlite_cursor.execute("SELECT COUNT(*) FROM products")
        sqlite_products_count = sqlite_cursor.fetchone()[0]
        
        sqlite_cursor.execute("SELECT COUNT(*) FROM price_history")
        sqlite_price_count = sqlite_cursor.fetchone()[0]
        
        print(f"SQLite data found:")
        print(f"  - Products: {sqlite_products_count:,}")
        print(f"  - Price history: {sqlite_price_count:,}")
        
        if sqlite_products_count == 0:
            print("No data found in SQLite database!")
            return
        
        # Create PostgreSQL tables with exact schema match
        print("\nCreating PostgreSQL tables...")
        
        # Drop existing tables if they exist (to start fresh)
        pg_cursor.execute("DROP TABLE IF EXISTS price_history CASCADE")
        pg_cursor.execute("DROP TABLE IF EXISTS products CASCADE")
        pg_cursor.execute("DROP TABLE IF EXISTS categories CASCADE")
        
        # Create products table (matching SQLite schema exactly)
        pg_cursor.execute('''
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                category_id TEXT NOT NULL,
                category_name TEXT NOT NULL,
                product_id TEXT NOT NULL UNIQUE,
                product_name TEXT NOT NULL,
                brand_name TEXT,
                is_own_brand BOOLEAN,
                sku_id TEXT NOT NULL,
                date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create price_history table (matching SQLite schema exactly)
        pg_cursor.execute('''
            CREATE TABLE price_history (
                id SERIAL PRIMARY KEY,
                product_id TEXT NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        ''')
        
        # Create indexes (matching SQLite indexes)
        pg_cursor.execute('CREATE INDEX idx_product_id ON products(product_id)')
        pg_cursor.execute('CREATE INDEX idx_category ON products(category_id)')
        pg_cursor.execute('CREATE INDEX idx_price_history ON price_history(product_id, recorded_at)')
        
        print("✅ Tables and indexes created successfully")
        
        # Migrate products data
        print("\nMigrating products data...")
        sqlite_cursor.execute("""
            SELECT category_id, category_name, product_id, product_name, 
                   brand_name, is_own_brand, sku_id, date_added 
            FROM products
        """)
        products_data_raw = sqlite_cursor.fetchall()
        
        if products_data_raw:
            # Convert boolean values from SQLite integers to PostgreSQL booleans
            products_data = []
            for row in products_data_raw:
                category_id, category_name, product_id, product_name, brand_name, is_own_brand, sku_id, date_added = row
                
                # Convert is_own_brand from integer to boolean
                is_own_brand_bool = bool(is_own_brand) if is_own_brand is not None else False
                
                products_data.append((
                    category_id, category_name, product_id, product_name, 
                    brand_name, is_own_brand_bool, sku_id, date_added
                ))
            
            # Batch insert products (excluding auto-increment id)
            execute_values(
                pg_cursor,
                """
                INSERT INTO products 
                (category_id, category_name, product_id, product_name, 
                 brand_name, is_own_brand, sku_id, date_added)
                VALUES %s
                """,
                products_data,
                template=None,
                page_size=1000
            )
            print(f"✅ Migrated {len(products_data):,} products")
        
        # Migrate price_history data
        print("Migrating price history data...")
        sqlite_cursor.execute("""
            SELECT product_id, price, recorded_at 
            FROM price_history
        """)
        price_data = sqlite_cursor.fetchall()
        
        if price_data:
            # Batch insert price history (excluding auto-increment id)
            execute_values(
                pg_cursor,
                """
                INSERT INTO price_history 
                (product_id, price, recorded_at)
                VALUES %s
                """,
                price_data,
                template=None,
                page_size=1000
            )
            print(f"✅ Migrated {len(price_data):,} price records")
        
        # Commit all changes
        pg_conn.commit()
        
        # Verify migration
        print("\nVerifying migration...")
        
        pg_cursor.execute("SELECT COUNT(*) FROM products")
        pg_products_count = pg_cursor.fetchone()[0]
        
        pg_cursor.execute("SELECT COUNT(*) FROM price_history")
        pg_price_count = pg_cursor.fetchone()[0]
        
        pg_cursor.execute("SELECT COUNT(DISTINCT product_id) FROM products")
        unique_products = pg_cursor.fetchone()[0]
        
        pg_cursor.execute("SELECT COUNT(DISTINCT category_id) FROM products")
        unique_categories = pg_cursor.fetchone()[0]
        
        # Check data integrity
        pg_cursor.execute("""
            SELECT COUNT(*) FROM price_history ph
            LEFT JOIN products p ON ph.product_id = p.product_id
            WHERE p.product_id IS NULL
        """)
        orphaned_prices = pg_cursor.fetchone()[0]
        
        # Get some sample data
        pg_cursor.execute("""
            SELECT p.product_name, p.brand_name, p.category_name, ph.price
            FROM products p
            LEFT JOIN price_history ph ON p.product_id = ph.product_id
            ORDER BY p.id LIMIT 5
        """)
        sample_data = pg_cursor.fetchall()
        
        print("\n=== Migration Results ===")
        print(f"✅ Products migrated: {pg_products_count:,} (Expected: {sqlite_products_count:,})")
        print(f"✅ Price records migrated: {pg_price_count:,} (Expected: {sqlite_price_count:,})")
        print(f"✅ Unique products: {unique_products:,}")
        print(f"✅ Unique categories: {unique_categories:,}")
        
        if orphaned_prices > 0:
            print(f"⚠️  Warning: {orphaned_prices} price records have no matching product")
        else:
            print("✅ All price records have matching products")
        
        print(f"\n=== Sample Data ===")
        for row in sample_data:
            product_name, brand_name, category_name, price = row
            price_str = f"${price}" if price else "No price"
            print(f"  {product_name} ({brand_name}) - {category_name} - {price_str}")
        
        # Success validation
        if pg_products_count == sqlite_products_count and pg_price_count == sqlite_price_count:
            print(f"\n🎉 Migration completed successfully!")
            print(f"All {sqlite_products_count:,} products and {sqlite_price_count:,} price records migrated.")
            
            # Create additional useful views for future web development
            print("\nCreating additional views for web development...")
            
            pg_cursor.execute('''
                CREATE OR REPLACE VIEW product_latest_prices AS
                SELECT DISTINCT ON (p.product_id) 
                    p.product_id,
                    p.product_name,
                    p.brand_name,
                    p.category_name,
                    ph.price,
                    ph.recorded_at
                FROM products p
                LEFT JOIN price_history ph ON p.product_id = ph.product_id
                ORDER BY p.product_id, ph.recorded_at DESC NULLS LAST
            ''')
            
            pg_cursor.execute('''
                CREATE OR REPLACE VIEW category_summary AS
                SELECT 
                    category_name,
                    COUNT(*) as product_count,
                    AVG(ph.price) as avg_price,
                    MIN(ph.price) as min_price,
                    MAX(ph.price) as max_price
                FROM products p
                LEFT JOIN price_history ph ON p.product_id = ph.product_id
                GROUP BY category_name
                ORDER BY product_count DESC
            ''')
            
            pg_conn.commit()
            print("✅ Created helpful views: product_latest_prices, category_summary")
            
        else:
            print(f"\n⚠️  Migration completed with discrepancies:")
            print(f"   Products: Expected {sqlite_products_count}, Got {pg_products_count}")
            print(f"   Price records: Expected {sqlite_price_count}, Got {pg_price_count}")
        
    except sqlite3.Error as e:
        print(f"❌ SQLite error: {e}")
        return False
        
    except psycopg2.Error as e:
        print(f"❌ PostgreSQL error: {e}")
        if 'pg_conn' in locals():
            pg_conn.rollback()
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
        
    finally:
        if 'sqlite_conn' in locals():
            sqlite_conn.close()
        if 'pg_conn' in locals():
            pg_conn.close()
    
    return True

def show_postgresql_quick_commands():
    """Show useful PostgreSQL commands for after migration"""
    print(f"\n=== Quick PostgreSQL Commands ===")
    print(f"Connect to database:")
    print(f"  psql heb_products")
    print(f"")
    print(f"Useful queries:")
    print(f"  \\dt                          -- List all tables")
    print(f"  \\d products                  -- Describe products table")
    print(f"  SELECT COUNT(*) FROM products;")
    print(f"  SELECT * FROM category_summary;")
    print(f"  SELECT * FROM product_latest_prices LIMIT 10;")
    print(f"")
    print(f"For web development, your tables are ready to use!")

if __name__ == "__main__":
    success = migrate_sqlite_to_postgres()
    if success:
        show_postgresql_quick_commands()
    else:
        print("\n❌ Migration failed. Please check the errors above.")
        sys.exit(1)