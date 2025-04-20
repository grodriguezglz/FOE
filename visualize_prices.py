#!/usr/bin/env python3
import sqlite3
import sys

def create_ascii_chart(data, width=50):
    """Create a simple ASCII chart from price data"""
    if not data:
        return "No data available"
    
    # Find min and max prices for scaling
    prices = [float(price) for _, price in data]
    max_price = max(prices)
    min_price = min(prices)
    price_range = max_price - min_price
    
    output = []
    output.append("Price History Chart")
    output.append("-" * width)
    
    # Create chart
    previous_price = None
    for date, price in data:
        price_float = float(price)
        # Calculate bar length
        if price_range == 0:
            bar_length = width // 2
        else:
            bar_length = int(((price_float - min_price) / price_range) * (width - 20))
        
        # Format the line with price aligned and show price change
        price_str = f"${price_float:6.2f}"
        date_str = f"{date[:10]}"
        
        # Add price change indicator
        if previous_price is not None:
            change = price_float - previous_price
            if change != 0:
                change_str = f" ({'+' if change > 0 else ''}{change:.2f})"
                price_str += change_str
        
        bar = "█" * bar_length
        output.append(f"{date_str} {price_str} |{bar}")
        previous_price = price_float
    
    output.append("-" * width)
    return "\n".join(output)

def main():
    # Connect to database
    conn = sqlite3.connect('heb_products.db')
    cursor = conn.cursor()
    
    # Get first and last prices for each product to properly detect increases and decreases
    price_variations_query = """
        WITH first_prices AS (
            SELECT ph1.product_id, ph1.price, ph1.recorded_at
            FROM price_history ph1
            WHERE ph1.recorded_at = (
                SELECT MIN(recorded_at) 
                FROM price_history ph2 
                WHERE ph2.product_id = ph1.product_id
            )
        ),
        last_prices AS (
            SELECT ph1.product_id, ph1.price, ph1.recorded_at
            FROM price_history ph1
            WHERE ph1.recorded_at = (
                SELECT MAX(recorded_at) 
                FROM price_history ph2 
                WHERE ph2.product_id = ph1.product_id
            )
        ),
        price_variations AS (
            SELECT 
                p.product_id,
                p.product_name,
                p.category_name,
                COUNT(DISTINCT ph.price) as unique_prices,
                MIN(ph.price) as min_price,
                MAX(ph.price) as max_price,
                date(fp.recorded_at) as first_price_date,
                date(lp.recorded_at) as last_price_date,
                fp.price as first_price,
                lp.price as last_price,
                COUNT(ph.id) as price_records,
                (lp.price - fp.price) as price_difference
            FROM products p 
            JOIN price_history ph ON p.product_id = ph.product_id
            JOIN first_prices fp ON p.product_id = fp.product_id
            JOIN last_prices lp ON p.product_id = lp.product_id
            GROUP BY p.product_id, p.product_name, p.category_name
            HAVING COUNT(ph.id) > 1
        )
    """
    
    # Get top 15 price increases
    print("\n=== TOP 15 PRICE INCREASES ===")
    cursor.execute(price_variations_query + """
        SELECT 
            product_id,
            product_name,
            category_name,
            unique_prices,
            min_price,
            max_price,
            first_price_date,
            last_price_date,
            first_price,
            last_price,
            price_records,
            price_difference
        FROM price_variations
        WHERE price_difference > 0
        ORDER BY price_difference DESC
        LIMIT 15
    """)
    
    increases = cursor.fetchall()
    
    # Get top 15 price decreases
    print("\n=== TOP 15 PRICE DECREASES ===")
    cursor.execute(price_variations_query + """
        SELECT 
            product_id,
            product_name,
            category_name,
            unique_prices,
            min_price,
            max_price,
            first_price_date,
            last_price_date,
            first_price,
            last_price,
            price_records,
            price_difference
        FROM price_variations
        WHERE price_difference < 0
        ORDER BY price_difference ASC
        LIMIT 15
    """)
    
    decreases = cursor.fetchall()
    
    # Combine and display results
    all_products = increases + decreases
    
    if not all_products:
        print("No products with price changes found.")
        return
    
    print("\n=== TOP 15 PRICE INCREASES ===")
    for i, product in enumerate(increases, 1):
        (pid, name, category, unique_prices, min_price, max_price, 
         first_date, last_date, first_price, last_price, records, diff) = product
        print(f"{i}. {name} ({category})")
        print(f"   Price range: ${first_price:.2f} ({first_date}) → ${last_price:.2f} ({last_date})")
        print(f"   Increase: ${diff:.2f} (+{(diff/first_price*100):.1f}%)")
        print(f"   Number of unique prices: {unique_prices}")
        print(f"   Total price records: {records}")
        print()
    
    print("\n=== TOP 15 PRICE DECREASES ===")
    for i, product in enumerate(decreases, 1):
        (pid, name, category, unique_prices, min_price, max_price, 
         first_date, last_date, first_price, last_price, records, diff) = product
        print(f"{i}. {name} ({category})")
        print(f"   Price range: ${first_price:.2f} ({first_date}) → ${last_price:.2f} ({last_date})")
        print(f"   Decrease: ${diff:.2f} ({(diff/first_price*100):.1f}%)")
        print(f"   Number of unique prices: {unique_prices}")
        print(f"   Total price records: {records}")
        print()
    
    # Get user input
    try:
        section = input("\nWhich section would you like to visualize? (I for Increases, D for Decreases): ").upper()
        if section not in ['I', 'D']:
            print("Invalid choice")
            return
            
        products = increases if section == 'I' else decreases
        if not products:
            print(f"No products found in the {'increases' if section == 'I' else 'decreases'} section.")
            return
            
        choice = int(input(f"Enter the number of the product to visualize (1-{len(products)}): "))
        if not 1 <= choice <= len(products):
            print("Invalid choice")
            return
    except ValueError:
        print("Please enter a valid number")
        return
    
    product_id = products[choice-1][0]
    
    # Get price history for selected product
    cursor.execute("""
        SELECT date(recorded_at), price 
        FROM price_history 
        WHERE product_id = ? 
        ORDER BY recorded_at
    """, (product_id,))
    
    data = cursor.fetchall()
    
    # Print product info
    print(f"\nPrice history for: {products[choice-1][1]}")
    print(create_ascii_chart(data))
    
    conn.close()

if __name__ == "__main__":
    main()