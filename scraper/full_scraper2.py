"""
HEB Grocery Scraper v2
Improvements:
- Batch commits per page (instead of per product) for better performance
- Fixed variable name collision (db_cursor vs pagination cursor)
- Removed debug print statements
- Safe nested dict access to prevent crashes
- Proper entry point guard
- Kept fresh session per category (intentional for anti-bot evasion)
"""

import os
import pandas as pd
import requests
from requests.sessions import Session
import time
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

# Load .env from web-app-v2 (shared config for whole project)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../web-app-v2/.env'))


def create_database():
    """Create local PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('LOCAL_DB_HOST', 'localhost'),
            port=int(os.getenv('LOCAL_DB_PORT', '5432')),
            database=os.getenv('LOCAL_DB_NAME', 'heb_products'),
            user=os.getenv('LOCAL_DB_USER', 'postgres'),
            password=os.getenv('LOCAL_DB_PASSWORD', '')
        )
        return conn
    except Exception as e:
        print(f"Database error: {e}")
        return None


def validate_price(price_str):
    """Validate and convert price string to decimal"""
    try:
        return float(price_str.replace('$', '').strip())
    except (ValueError, AttributeError):
        return None


def get_fresh_session():
    """Create a new session with fresh cookies for each request.
    This is intentional for anti-bot evasion - HEB tracks session patterns.
    """
    session = Session()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9'
    }

    session.get('https://www.heb.com/', headers=headers)
    time.sleep(2)
    return session


def safe_get_product_info(product, category_id, category_name):
    """Safely extract product info with fallbacks for missing data"""
    # Safe nested access for brand
    brand = product.get('brand') or {}
    brand_name = brand.get('name', 'N/A')
    is_own_brand = brand.get('isOwnBrand', False)

    # Safe nested access for SKU and price
    skus = product.get('SKUs') or []
    if skus:
        sku_id = skus[0].get('id', 'N/A')
        context_prices = skus[0].get('contextPrices') or []
        if context_prices:
            list_price = context_prices[0].get('listPrice') or {}
            price = list_price.get('formattedAmount', 'N/A')
        else:
            price = 'N/A'
    else:
        sku_id = 'N/A'
        price = 'N/A'

    return {
        'category_id': category_id,
        'category_name': category_name,
        'product_id': product.get('id', 'N/A'),
        'product_name': product.get('displayName', 'N/A'),
        'brand_name': brand_name,
        'is_own_brand': is_own_brand,
        'sku_id': sku_id,
        'price': price
    }


def batch_insert_products(conn, products_batch):
    """Insert a batch of products in a single transaction.
    Much faster than individual commits per product.
    """
    if not products_batch:
        return 0

    db_cursor = conn.cursor()
    successful = 0

    try:
        for product_info in products_batch:
            # Insert/update product
            db_cursor.execute('''
                INSERT INTO products
                (category_id, category_name, product_id, product_name,
                 brand_name, is_own_brand, sku_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    category_name = EXCLUDED.category_name,
                    product_name = EXCLUDED.product_name,
                    brand_name = EXCLUDED.brand_name,
                    is_own_brand = EXCLUDED.is_own_brand,
                    sku_id = EXCLUDED.sku_id
            ''', (
                product_info['category_id'],
                product_info['category_name'],
                product_info['product_id'],
                product_info['product_name'],
                product_info['brand_name'],
                product_info['is_own_brand'],
                product_info['sku_id']
            ))

            # Insert price history
            price = validate_price(product_info['price'])
            if price is not None:
                db_cursor.execute('''
                    INSERT INTO price_history (product_id, price)
                    VALUES (%s, %s)
                ''', (product_info['product_id'], price))

            successful += 1

        # Single commit for entire batch
        conn.commit()
        return successful

    except Exception as e:
        print(f"Error in batch insert: {e}")
        conn.rollback()
        return 0


def run_scraper():
    """Main scraper function"""
    start_time = datetime.now()

    # Initialize database
    conn = create_database()
    if conn is None:
        raise Exception("Failed to create database connection")

    # Read Excel - make sure this path is correct
    df = pd.read_excel('../data/categoryid.xlsx')
    total_categories = len(df)
    print(f"Loaded {total_categories} categories from Excel")

    # GraphQL request headers
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://www.heb.com',
        'Referer': 'https://www.heb.com/'
    }

    url = 'https://www.heb.com/graphql'
    successful = 0
    failed = 0
    products_processed = 0

    for index, row in df.iterrows():
        category_start_time = datetime.now()
        print(f"\nProcessing category {index + 1}/{total_categories}: {row.categoryID} - {row.CATEGORY}")

        # Fresh session per category (anti-bot evasion)
        session = get_fresh_session()

        query = """
        query {
            browseCategory(
                categoryId: "%s"
                storeId: 793
                shoppingContext: CURBSIDE_PICKUP
                limit: 50
                %s
            ) {
                pageTitle
                records {
                    id
                    displayName
                    brand {
                        name
                        isOwnBrand
                    }
                    SKUs {
                        id
                        contextPrices {
                            listPrice {
                                formattedAmount
                            }
                        }
                    }
                }
                total
                hasMoreRecords
                nextCursor
            }
        }
        """

        try:
            has_more = True
            page_cursor = ""  # Renamed to avoid collision with db cursor
            category_products = 0
            page = 1
            max_pages = 100

            while has_more and page <= max_pages:
                page_start_time = datetime.now()
                print(f"  Processing page {page}/{max_pages}")
                current_query = query % (str(row.categoryID), f'cursor: "{page_cursor}"' if page_cursor else '')

                response = session.post(url,
                                        json={'query': current_query},
                                        headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data and 'browseCategory' in data['data']:
                        browse_data = data['data']['browseCategory']
                        total_available = browse_data.get('total', 0)
                        print(f"  Total available products in category: {total_available}")

                        products = browse_data.get('records', [])

                        # Build batch of products for this page
                        products_batch = []
                        for product in products:
                            product_info = safe_get_product_info(
                                product,
                                row.categoryID,
                                row.CATEGORY
                            )
                            products_batch.append(product_info)

                        # Batch insert all products from this page
                        successful_inserts = batch_insert_products(conn, products_batch)

                        category_products += successful_inserts
                        products_processed += successful_inserts

                        has_more = browse_data.get('hasMoreRecords', False)
                        page_cursor = browse_data.get('nextCursor', '')

                        page_duration = datetime.now() - page_start_time
                        print(f"  Added {successful_inserts} products (Total in category: {category_products})")
                        print(f"  Page {page} processing time: {page_duration}")

                        page += 1
                        if page <= max_pages and has_more:
                            time.sleep(2)
                    else:
                        print("  No data in response")
                        has_more = False
                elif response.status_code == 429:
                    print("  Rate limited, waiting 30 seconds...")
                    time.sleep(30)
                    continue
                else:
                    print(f"  Error response: {response.status_code}")
                    has_more = False

            category_duration = datetime.now() - category_start_time
            if category_products > 0:
                successful += 1
                print(f"Completed category with {category_products} total products")
                print(f"Total category processing time: {category_duration}")
            else:
                failed += 1

        except Exception as e:
            failed += 1
            print(f"Error processing category: {str(e)}")

        time.sleep(3)

    # Print summary
    end_time = datetime.now()
    duration = end_time - start_time

    # Get some statistics from the database
    db_cursor = conn.cursor()
    db_cursor.execute("SELECT COUNT(DISTINCT product_id) FROM products")
    total_unique_products = db_cursor.fetchone()[0]

    db_cursor.execute("SELECT COUNT(*) FROM price_history")
    total_price_records = db_cursor.fetchone()[0]

    print("\n=== SUMMARY ===")
    print(f"Total categories processed: {total_categories}")
    print(f"Successful categories: {successful}")
    print(f"Failed categories: {failed}")
    print(f"Total products processed: {products_processed}")
    print(f"Unique products in database: {total_unique_products}")
    print(f"Total price history records: {total_price_records}")
    print(f"Total time taken: {duration}")

    # Close database connection
    conn.close()


if __name__ == "__main__":
    try:
        run_scraper()
    except Exception as e:
        print("Error:", str(e))
        import traceback
        print("Full error:", traceback.format_exc())
