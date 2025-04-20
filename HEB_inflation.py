#!/usr/bin/env python3
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import numpy as np
import calendar

def calculate_inflation_metrics(db_path='heb_products.db'):
    """
    Calculate various inflation metrics from the price history database
    """
    conn = sqlite3.connect(db_path)
    
    # 1. Calculate overall average price change since tracking began
    overall_query = """
    WITH first_last_prices AS (
        SELECT 
            p.product_id,
            p.product_name,
            p.category_name,
            first_price.price AS first_price,
            first_price.recorded_at AS first_date,
            last_price.price AS last_price,
            last_price.recorded_at AS last_date
        FROM products p
        JOIN (
            SELECT product_id, price, recorded_at
            FROM price_history ph1
            WHERE recorded_at = (
                SELECT MIN(recorded_at) FROM price_history ph2 
                WHERE ph2.product_id = ph1.product_id
            )
        ) AS first_price ON p.product_id = first_price.product_id
        JOIN (
            SELECT product_id, price, recorded_at
            FROM price_history ph1
            WHERE recorded_at = (
                SELECT MAX(recorded_at) FROM price_history ph2 
                WHERE ph2.product_id = ph1.product_id
            )
        ) AS last_price ON p.product_id = last_price.product_id
        WHERE first_price.recorded_at != last_price.recorded_at
    )
    SELECT 
        COUNT(*) as total_products,
        AVG((last_price - first_price) / first_price * 100) as avg_percent_change,
        MIN(first_date) as earliest_date,
        MAX(last_date) as latest_date,
        SUM(CASE WHEN last_price > first_price THEN 1 ELSE 0 END) as num_increased,
        SUM(CASE WHEN last_price < first_price THEN 1 ELSE 0 END) as num_decreased,
        SUM(CASE WHEN last_price = first_price THEN 1 ELSE 0 END) as num_unchanged
    FROM first_last_prices
    """
    
    overall_df = pd.read_sql_query(overall_query, conn)
    
    # 2. Calculate monthly average prices (create a price index)
    monthly_query = """
    WITH monthly_avg AS (
        SELECT 
            strftime('%Y-%m', recorded_at) as year_month,
            product_id,
            AVG(price) as avg_price
        FROM price_history
        GROUP BY year_month, product_id
    ),
    baseline AS (
        SELECT 
            product_id,
            avg_price as baseline_price
        FROM monthly_avg
        WHERE year_month = (SELECT MIN(year_month) FROM monthly_avg)
    )
    SELECT 
        m.year_month,
        COUNT(m.product_id) as num_products,
        AVG(m.avg_price / b.baseline_price * 100) - 100 as avg_inflation_from_baseline,
        AVG(m.avg_price) as avg_product_price
    FROM monthly_avg m
    JOIN baseline b ON m.product_id = b.product_id
    GROUP BY m.year_month
    ORDER BY m.year_month
    """
    
    monthly_df = pd.read_sql_query(monthly_query, conn)
    
    # 3. Calculate inflation by category
    category_query = """
    WITH first_last_prices AS (
        SELECT 
            p.product_id,
            p.product_name,
            p.category_name,
            first_price.price AS first_price,
            first_price.recorded_at AS first_date,
            last_price.price AS last_price,
            last_price.recorded_at AS last_date
        FROM products p
        JOIN (
            SELECT product_id, price, recorded_at
            FROM price_history ph1
            WHERE recorded_at = (
                SELECT MIN(recorded_at) FROM price_history ph2 
                WHERE ph2.product_id = ph1.product_id
            )
        ) AS first_price ON p.product_id = first_price.product_id
        JOIN (
            SELECT product_id, price, recorded_at
            FROM price_history ph1
            WHERE recorded_at = (
                SELECT MAX(recorded_at) FROM price_history ph2 
                WHERE ph2.product_id = ph1.product_id
            )
        ) AS last_price ON p.product_id = last_price.product_id
        WHERE first_price.recorded_at != last_price.recorded_at
    )
    SELECT 
        category_name,
        COUNT(*) as num_products,
        AVG((last_price - first_price) / first_price * 100) as avg_percent_change,
        SUM(CASE WHEN last_price > first_price THEN 1 ELSE 0 END) as num_increased,
        SUM(CASE WHEN last_price < first_price THEN 1 ELSE 0 END) as num_decreased
    FROM first_last_prices
    GROUP BY category_name
    ORDER BY avg_percent_change DESC
    """
    
    category_df = pd.read_sql_query(category_query, conn)
    
    # 4. Create a shopping basket analysis
    # You could define your own basket of goods here
    basket_items = [
        "Milk, 1 Gallon",
        "Eggs, Large Dozen",
        "Bread, White Loaf",
        "Chicken Breast, 1 lb",
        "Ground Beef, 1 lb",
        "Apples, 1 lb",
        "Bananas, 1 lb",
        "Potato, Russet, 5 lb",
        "Rice, White, 2 lb",
        "Pasta, Spaghetti, 1 lb"
    ]
    
    # This is a placeholder, you'd need to map to your actual product IDs
    # Create a table with your basket items in the database and run this query:
    basket_query = """
    WITH monthly_prices AS (
        SELECT 
            strftime('%Y-%m', recorded_at) as year_month,
            p.product_id,
            p.product_name,
            AVG(ph.price) as avg_price
        FROM products p
        JOIN price_history ph ON p.product_id = ph.product_id
        WHERE p.product_name LIKE '%Milk%' OR p.product_name LIKE '%Egg%' OR p.product_name LIKE '%Bread%'
            OR p.product_name LIKE '%Chicken%' OR p.product_name LIKE '%Beef%' OR p.product_name LIKE '%Apple%'
            OR p.product_name LIKE '%Banana%' OR p.product_name LIKE '%Potato%' OR p.product_name LIKE '%Rice%'
            OR p.product_name LIKE '%Pasta%'
        GROUP BY year_month, p.product_id, p.product_name
    )
    SELECT 
        year_month,
        SUM(avg_price) as basket_cost,
        COUNT(DISTINCT product_id) as num_products
    FROM monthly_prices
    GROUP BY year_month
    ORDER BY year_month
    """
    
    try:
        basket_df = pd.read_sql_query(basket_query, conn)
    except:
        basket_df = pd.DataFrame(columns=['year_month', 'basket_cost', 'num_products'])
        print("Couldn't match basket items. Modify the query to match your actual product names.")
    
    conn.close()
    
    return {
        'overall': overall_df,
        'monthly': monthly_df,
        'category': category_df,
        'basket': basket_df
    }

def create_inflation_visualizations(metrics):
    """
    Create visualizations of the inflation metrics
    """
    # Create figure with subplots
    fig = plt.figure(figsize=(15, 12))
    
    # 1. Overall inflation metrics
    ax1 = plt.subplot(2, 2, 1)
    overall = metrics['overall'].iloc[0]
    labels = ['Increased', 'Decreased', 'Unchanged']
    sizes = [overall['num_increased'], overall['num_decreased'], overall['num_unchanged']]
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax1.set_title(f'Price Changes Since Tracking\nOverall Inflation: {overall["avg_percent_change"]:.2f}%')
    
    # 2. Monthly inflation trend
    monthly = metrics['monthly']
    ax2 = plt.subplot(2, 2, 2)
    ax2.plot(monthly['year_month'], monthly['avg_inflation_from_baseline'], 'o-', linewidth=2)
    ax2.set_title('Cumulative Inflation by Month')
    ax2.set_xlabel('Month')
    ax2.set_ylabel('Inflation % from Baseline')
    ax2.grid(True)
    plt.xticks(rotation=45)
    
    # 3. Category inflation
    category = metrics['category']
    ax3 = plt.subplot(2, 1, 2)
    bars = ax3.barh(category['category_name'], category['avg_percent_change'])
    ax3.set_title('Average Price Change by Category')
    ax3.set_xlabel('Average % Change')
    ax3.set_xlim(min(category['avg_percent_change']) - 1, max(category['avg_percent_change']) + 1)
    
    # Add data labels
    for bar in bars:
        width = bar.get_width()
        label_x_pos = width if width >= 0 else width - 1
        ax3.text(label_x_pos, bar.get_y() + bar.get_height()/2, f'{width:.1f}%', 
                 va='center', ha='left' if width >= 0 else 'right')
    
    plt.tight_layout()
    plt.savefig('grocery_inflation_metrics.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # 4. Shopping basket cost over time (if data available)
    if not metrics['basket'].empty and len(metrics['basket']) > 1:
        fig, ax = plt.subplots(figsize=(12, 6))
        basket = metrics['basket']
        
        if 'basket_cost' in basket.columns:
            ax.plot(basket['year_month'], basket['basket_cost'], 'o-', linewidth=2)
            
            # Calculate baseline and percentage increase
            baseline_cost = basket['basket_cost'].iloc[0]
            latest_cost = basket['basket_cost'].iloc[-1]
            percent_increase = (latest_cost - baseline_cost) / baseline_cost * 100
            
            ax.set_title(f'Cost of Standard Shopping Basket Over Time\nTotal Increase: {percent_increase:.1f}%')
            ax.set_xlabel('Month')
            ax.set_ylabel('Basket Cost ($)')
            ax.grid(True)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig('grocery_basket_cost.png', dpi=150, bbox_inches='tight')
            plt.close()

def format_inflation_report(metrics):
    """
    Create a text report summarizing the inflation metrics
    """
    overall = metrics['overall'].iloc[0]
    first_date = pd.to_datetime(overall['earliest_date']).strftime('%B %d, %Y')
    last_date = pd.to_datetime(overall['latest_date']).strftime('%B %d, %Y')
    monthly = metrics['monthly']
    category = metrics['category']
    
    report = []
    report.append("=" * 80)
    report.append(f"GROCERY PRICE INFLATION REPORT: {first_date} to {last_date}")
    report.append("=" * 80)
    
    # Overall summary
    report.append("\nOVERALL INFLATION METRICS:")
    report.append(f"- Average price change: {overall['avg_percent_change']:.2f}%")
    report.append(f"- Total products tracked: {overall['total_products']}")
    report.append(f"- Products with price increases: {overall['num_increased']} ({overall['num_increased']/overall['total_products']*100:.1f}%)")
    report.append(f"- Products with price decreases: {overall['num_decreased']} ({overall['num_decreased']/overall['total_products']*100:.1f}%)")
    report.append(f"- Products with unchanged prices: {overall['num_unchanged']} ({overall['num_unchanged']/overall['total_products']*100:.1f}%)")
    
    # Monthly trend
    report.append("\nMONTHLY INFLATION TREND:")
    for i, row in monthly.iterrows():
        year_month = row['year_month']
        year, month = year_month.split('-')
        month_name = calendar.month_name[int(month)]
        inflation = row['avg_inflation_from_baseline']
        report.append(f"- {month_name} {year}: {inflation:.2f}% (from baseline)")
    
    # Category breakdown
    report.append("\nINFLATION BY CATEGORY:")
    for i, row in category.iterrows():
        cat_name = row['category_name']
        pct_change = row['avg_percent_change']
        num_products = row['num_products']
        report.append(f"- {cat_name}: {pct_change:.2f}% across {num_products} products")
    
    # Shopping basket
    if not metrics['basket'].empty and len(metrics['basket']) > 1:
        basket = metrics['basket']
        if 'basket_cost' in basket.columns:
            report.append("\nSHOPPING BASKET ANALYSIS:")
            first_cost = basket['basket_cost'].iloc[0]
            last_cost = basket['basket_cost'].iloc[-1]
            percent_change = (last_cost - first_cost) / first_cost * 100
            
            first_month = basket['year_month'].iloc[0]
            last_month = basket['year_month'].iloc[-1]
            report.append(f"- Standard basket cost in {first_month}: ${first_cost:.2f}")
            report.append(f"- Standard basket cost in {last_month}: ${last_cost:.2f}")
            report.append(f"- Total increase: ${last_cost-first_cost:.2f} ({percent_change:.2f}%)")
    
    report.append("\nNOTE: This analysis is based on local grocery store pricing and may differ")
    report.append("from official inflation statistics. For comparison, the official food-at-home")
    report.append("CPI inflation rate from the Bureau of Labor Statistics is typically available")
    report.append("at https://www.bls.gov/cpi/")
    
    return "\n".join(report)

def main():
    # Calculate metrics
    print("Calculating inflation metrics...")
    metrics = calculate_inflation_metrics()
    
    # Create visualizations
    print("Creating visualizations...")
    create_inflation_visualizations(metrics)
    
    # Generate report
    print("Generating report...")
    report = format_inflation_report(metrics)
    
    # Save report to file
    with open("grocery_inflation_report.txt", "w") as f:
        f.write(report)
    
    # Print report to console
    print("\n" + report)
    
    print("\nReport and visualizations have been saved to:")
    print("- grocery_inflation_report.txt")
    print("- grocery_inflation_metrics.png")
    if not metrics['basket'].empty and len(metrics['basket']) > 1:
        print("- grocery_basket_cost.png")

if __name__ == "__main__":
    main()