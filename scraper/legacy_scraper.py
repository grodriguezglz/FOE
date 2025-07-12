print("Test 1: Starting")
import pandas as pd
import requests
from requests.sessions import Session
import time
from datetime import datetime
import sqlite3
from sqlite3 import Error
print("Test 2: All imports successful")



def create_database():
    """Create SQLite database and tables with proper indexes"""
    try:
        conn = sqlite3.connect('heb_products.db')
        c = conn.cursor()
        
        # Create main products table
        c.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        
        # Create price history table
        c.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        ''')
        
        # Create indexes for faster querying
        c.execute('CREATE INDEX IF NOT EXISTS idx_product_id ON products(product_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_category ON products(category_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_price_history ON price_history(product_id, recorded_at)')
        
        conn.commit()
        return conn
    except Error as e:
        print(f"Database error: {e}")
        return None

def validate_price(price_str):
    """Validate and convert price string to decimal"""
    try:
        return float(price_str.replace('$', '').strip())
    except (ValueError, AttributeError):
        return None

def get_fresh_session():
    """Create a new session with fresh cookies for each request"""
    session = Session()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    
    session.get('https://www.heb.com/', headers=headers)
    time.sleep(2)
    return session

def insert_or_update_product(conn, product_info):
    """Insert or update product and price information"""
    cursor = conn.cursor()
    try:
        # Try to insert new product
        cursor.execute('''
            INSERT OR IGNORE INTO products 
            (category_id, category_name, product_id, product_name, 
             brand_name, is_own_brand, sku_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
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
            cursor.execute('''
                INSERT INTO price_history (product_id, price)
                VALUES (?, ?)
            ''', (product_info['product_id'], price))
        
        conn.commit()
        return True
    except Error as e:
        print(f"Error inserting/updating product {product_info['product_id']}: {e}")
        conn.rollback()
        return False

try:
    start_time = datetime.now()
    
    # Initialize database
    conn = create_database()
    if conn is None:
        raise Exception("Failed to create database connection")
    
    # Read Excel
    df = pd.read_excel('categoryid.xlsx')
    total_categories = len(df)
    print(f"Test 3: Read Excel file with {total_categories} rows")
    
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
            cursor = ""
            category_products = 0
            page = 1
            max_pages = 100  # Increased from 20 to 100
            
            while has_more and page <= max_pages:
                page_start_time = datetime.now()
                print(f"  Processing page {page}/{max_pages}")
                current_query = query % (str(row.categoryID), f'cursor: "{cursor}"' if cursor else '')
                
                response = session.post(url,
                                      json={'query': current_query},
                                      headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data and 'browseCategory' in data['data']:
                        browse_data = data['data']['browseCategory']
                        total_available = browse_data.get('total', 0)
                        print(f"  Total available products in category: {total_available}")
                        
                        products = browse_data['records']
                        
                        successful_inserts = 0
                        for product in products:
                            product_info = {
                                'category_id': row.categoryID,
                                'category_name': row.CATEGORY,
                                'product_id': product['id'],
                                'product_name': product['displayName'],
                                'brand_name': product['brand']['name'] if product['brand'] else 'N/A',
                                'is_own_brand': product['brand']['isOwnBrand'] if product['brand'] else 'N/A',
                                'sku_id': product['SKUs'][0]['id'] if product['SKUs'] else 'N/A',
                                'price': product['SKUs'][0]['contextPrices'][0]['listPrice']['formattedAmount'] if product['SKUs'] else 'N/A'
                            }
                            
                            if insert_or_update_product(conn, product_info):
                                successful_inserts += 1
                                products_processed += 1
                        
                        category_products += successful_inserts
                        
                        has_more = browse_data['hasMoreRecords']
                        cursor = browse_data['nextCursor']
                        
                        page_duration = datetime.now() - page_start_time
                        print(f"  Added {successful_inserts} products (Total in category: {category_products})")
                        print(f"  Page {page} processing time: {page_duration}")
                        
                        page += 1
                        if page <= max_pages:
                            time.sleep(2)
                    else:
                        print("No data in response")
                        has_more = False
                elif response.status_code == 429:  # Rate limited
                    print("Rate limited, waiting 30 seconds...")
                    time.sleep(30)
                    continue
                else:
                    print(f"Error response: {response.status_code}")
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
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT product_id) FROM products")
    total_unique_products = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM price_history")
    total_price_records = cursor.fetchone()[0]
    
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
    
except Exception as e:
    print("Error:", str(e))
    import traceback
    print("Full error:", traceback.format_exc())
