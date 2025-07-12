import requests
from requests.sessions import Session
import time
import json

def get_fresh_session():
    """Create a new session with fresh cookies"""
    session = Session()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    
    session.get('https://www.heb.com/', headers=headers)
    time.sleep(2)
    return session

def explore_existing_structure():
    """Explore the structure of fields we know work"""
    session = get_fresh_session()
    
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://www.heb.com',
        'Referer': 'https://www.heb.com/'
    }
    
    url = 'https://www.heb.com/graphql'
    
    # Let's try expanding the price structure to see if there's more data
    query = """
    query {
        browseCategory(
            categoryId: "490020"
            storeId: 793
            shoppingContext: CURBSIDE_PICKUP
            limit: 3
        ) {
            pageTitle
            total
            hasMoreRecords
            nextCursor
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
                            value
                            formattedAmount
                        }
                    }
                }
            }
        }
    }
    """
    
    try:
        response = session.post(url, json={'query': query}, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'errors' not in data:
                print("=== EXPANDED PRICE STRUCTURE ===")
                print(json.dumps(data, indent=2))
                
                # Let's see what fields are actually populated
                if 'data' in data and data['data']['browseCategory']['records']:
                    products = data['data']['browseCategory']['records']
                    
                    print("\n=== FIELD ANALYSIS ===")
                    for i, product in enumerate(products):
                        print(f"\nProduct {i+1}: {product['displayName']}")
                        print(f"  ID: {product['id']}")
                        print(f"  Brand: {product['brand']}")
                        
                        if product['SKUs']:
                            sku = product['SKUs'][0]
                            print(f"  SKU ID: {sku['id']}")
                            
                            if sku['contextPrices']:
                                price = sku['contextPrices'][0]['listPrice']
                                print(f"  Price value: {price.get('value', 'N/A')}")
                                print(f"  Price formatted: {price.get('formattedAmount', 'N/A')}")
            else:
                print("❌ Errors found:")
                for error in data['errors']:
                    print(f"  {error['message']}")
        else:
            print(f"❌ HTTP {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"❌ Exception: {e}")
    
    finally:
        session.close()

if __name__ == "__main__":
    explore_existing_structure()