#!/usr/bin/env python3
"""
HEB Product Dashboard - Fixed Version with Proper Encoding
No more weird character issues
"""

import http.server
import socketserver
import psycopg2
from psycopg2.extras import RealDictCursor
import urllib.parse as urlparse
from datetime import datetime
import html

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Handle requests in separate threads"""
    allow_reuse_address = True
    daemon_threads = True

class HEBHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        """Override to reduce log spam"""
        return
    
    def get_db_connection(self):
        try:
            return psycopg2.connect(
                host="localhost",
                port=5432,
                database="heb_products",
                user="memorodriguez",
                password="",
                cursor_factory=RealDictCursor
            )
        except Exception as e:
            print(f"Database connection error: {e}")
            return None
    
    def send_html_response(self, html_content):
        """Send HTML response with proper encoding"""
        try:
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Cache-Control', 'no-cache')
            self.end_headers()
            self.wfile.write(html_content.encode('utf-8'))
        except Exception as e:
            print(f"Error sending response: {e}")
    
    def escape_text(self, text):
        """Safely escape text for HTML"""
        if text is None:
            return ""
        return html.escape(str(text))
    
    def do_GET(self):
        try:
            if self.path == '/':
                self.serve_dashboard()
            elif self.path.startswith('/search'):
                self.serve_search()
            elif self.path.startswith('/product/'):
                self.serve_product_detail()
            else:
                self.send_error(404)
        except Exception as e:
            print(f"Error handling request: {e}")
            try:
                self.send_error(500, f"Server error: {str(e)}")
            except:
                pass
    
    def serve_dashboard(self):
        conn = self.get_db_connection()
        if not conn:
            self.send_error(500, "Database connection failed")
            return
            
        try:
            cursor = conn.cursor()
            
            # Get basic stats
            cursor.execute("SELECT COUNT(*) as total_products FROM products")
            total_products = cursor.fetchone()['total_products']
            
            cursor.execute("SELECT COUNT(DISTINCT category_name) as total_categories FROM products")
            total_categories = cursor.fetchone()['total_categories']
            
            cursor.execute("SELECT COUNT(*) as total_prices FROM price_history")
            total_prices = cursor.fetchone()['total_prices']
            
            cursor.execute("SELECT AVG(price) as avg_price FROM price_history")
            avg_price = round(float(cursor.fetchone()['avg_price'] or 0), 2)
            
            # Get categories
            cursor.execute("""
                SELECT category_name, COUNT(*) as product_count
                FROM products
                GROUP BY category_name
                ORDER BY product_count DESC
            """)
            categories = cursor.fetchall()
            
            html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HEB Product Dashboard</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            margin: 10px;
            background: #f5f5f5;
            line-height: 1.6;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        .header {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            color: #333;
        }}
        .header p {{
            margin: 0;
            color: #666;
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stat-number {{
            font-size: 2em;
            font-weight: bold;
            color: #e74c3c;
            margin-bottom: 5px;
        }}
        .stat-label {{
            color: #666;
            font-size: 0.9em;
        }}
        .search-section {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .search-form {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            align-items: center;
        }}
        .search-input {{
            flex: 1;
            min-width: 250px;
            padding: 12px;
            border: 2px solid #ddd;
            border-radius: 6px;
            font-size: 16px;
        }}
        .search-input:focus {{
            outline: none;
            border-color: #e74c3c;
        }}
        .search-btn {{
            background: #e74c3c;
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 16px;
            font-weight: 500;
        }}
        .search-btn:hover {{
            background: #c0392b;
        }}
        .categories {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 15px;
        }}
        .category-card {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            transition: transform 0.2s;
        }}
        .category-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        }}
        .category-card h4 {{
            margin: 0 0 10px 0;
            color: #333;
        }}
        .category-card p {{
            margin: 0 0 15px 0;
            color: #666;
        }}
        @media (max-width: 768px) {{
            .stats {{
                grid-template-columns: repeat(2, 1fr);
            }}
            .search-form {{
                flex-direction: column;
                align-items: stretch;
            }}
            .search-input {{
                min-width: auto;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>HEB Product Dashboard</h1>
            <p>Track products and prices from your HEB scraper</p>
        </div>

        <div class="stats">
            <div class="stat-card">
                <div class="stat-number">{total_products:,}</div>
                <div class="stat-label">Total Products</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{total_categories}</div>
                <div class="stat-label">Categories</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{total_prices:,}</div>
                <div class="stat-label">Price Records</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">${avg_price}</div>
                <div class="stat-label">Average Price</div>
            </div>
        </div>

        <div class="search-section">
            <div class="search-form">
                <input type="text" id="searchInput" placeholder="Search for products..." class="search-input" autocomplete="off">
                <button onclick="searchProducts()" class="search-btn">Search</button>
            </div>
        </div>

        <div class="categories">"""
            
            for category in categories:
                category_name = self.escape_text(category['category_name'])
                product_count = category['product_count']
                
                html_content += f"""
            <div class="category-card">
                <h4>{category_name}</h4>
                <p>{product_count:,} products available</p>
                <button onclick="searchByCategory('{category_name}')" class="search-btn">View Products</button>
            </div>"""
            
            html_content += """
        </div>
    </div>

    <script>
        function searchProducts() {
            const query = document.getElementById('searchInput').value.trim();
            if (query) {
                window.location.href = '/search?q=' + encodeURIComponent(query);
            } else {
                alert('Please enter a search term');
            }
        }
        
        function searchByCategory(category) {
            window.location.href = '/search?category=' + encodeURIComponent(category);
        }
        
        document.getElementById('searchInput').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                searchProducts();
            }
        });
        
        // Focus search input on page load
        document.getElementById('searchInput').focus();
    </script>
</body>
</html>"""
            
            self.send_html_response(html_content)
            
        except Exception as e:
            print(f"Error in serve_dashboard: {e}")
            self.send_error(500, f"Dashboard error: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def serve_search(self):
        conn = self.get_db_connection()
        if not conn:
            self.send_error(500, "Database connection failed")
            return
            
        try:
            # Parse query parameters
            parsed_url = urlparse.urlparse(self.path)
            params = urlparse.parse_qs(parsed_url.query)
            
            query = params.get('q', [''])[0]
            category = params.get('category', [''])[0]
            
            cursor = conn.cursor()
            
            # Build search query
            where_conditions = []
            sql_params = []
            
            if query:
                where_conditions.append("(product_name ILIKE %s OR brand_name ILIKE %s)")
                sql_params.extend([f'%{query}%', f'%{query}%'])
            
            if category:
                where_conditions.append("category_name = %s")
                sql_params.append(category)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            search_query = f"""
                SELECT DISTINCT ON (p.product_id)
                    p.product_id,
                    p.product_name,
                    p.brand_name,
                    p.category_name,
                    p.is_own_brand,
                    ph.price,
                    ph.recorded_at
                FROM products p
                LEFT JOIN price_history ph ON p.product_id = ph.product_id
                {where_clause}
                ORDER BY p.product_id, ph.recorded_at DESC NULLS LAST
                LIMIT 100
            """
            
            cursor.execute(search_query, sql_params)
            products = cursor.fetchall()
            
            search_term = self.escape_text(query or category or "All Products")
            
            html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Search Results - HEB Dashboard</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            margin: 10px;
            background: #f5f5f5;
            line-height: 1.6;
        }}
        .container {{
            max-width: 1000px;
            margin: 0 auto;
        }}
        .header {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .back-btn {{
            background: #3498db;
            color: white;
            padding: 10px 20px;
            text-decoration: none;
            border-radius: 6px;
            display: inline-block;
            margin-bottom: 15px;
            font-weight: 500;
        }}
        .back-btn:hover {{
            background: #2980b9;
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            color: #333;
        }}
        .header p {{
            margin: 5px 0;
            color: #666;
        }}
        .product-item {{
            background: #fff;
            padding: 20px;
            margin-bottom: 12px;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .product-item:hover {{
            background-color: #f8f9fa;
            transform: translateY(-1px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        }}
        .product-info {{
            flex: 1;
        }}
        .product-info h4 {{
            margin: 0 0 8px 0;
            color: #333;
            font-size: 1.1em;
        }}
        .product-info p {{
            margin: 0;
            color: #666;
            font-size: 0.9em;
        }}
        .product-price {{
            font-weight: bold;
            color: #e74c3c;
            font-size: 1.3em;
            margin-left: 20px;
        }}
        .heb-brand {{
            background: #e74c3c;
            color: white;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 0.8em;
            font-weight: 500;
        }}
        .history-link {{
            color: #3498db;
            font-size: 0.85em;
            margin-top: 5px;
        }}
        .no-results {{
            background: #fff;
            padding: 40px;
            border-radius: 8px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        @media (max-width: 768px) {{
            .product-item {{
                flex-direction: column;
                align-items: flex-start;
            }}
            .product-price {{
                margin-left: 0;
                margin-top: 10px;
                align-self: flex-end;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <a href="/" class="back-btn">← Back to Dashboard</a>
            <h1>Search Results</h1>
            <p>Found {len(products)} products for "{search_term}"</p>
            <p class="history-link">Click on any product to view price history</p>
        </div>"""
            
            if products:
                for product in products:
                    product_name = self.escape_text(product['product_name'])
                    brand_name = self.escape_text(product['brand_name'])
                    category_name = self.escape_text(product['category_name'])
                    product_id = self.escape_text(product['product_id'])
                    
                    heb_badge = '<span class="heb-brand">HEB</span>' if product['is_own_brand'] else ''
                    price_display = f"${product['price']:.2f}" if product['price'] else "No price"
                    last_updated = product['recorded_at'].strftime('%m/%d/%Y') if product['recorded_at'] else 'No data'
                    
                    html_content += f"""
        <div class="product-item" onclick="window.location.href='/product/{product_id}'">
            <div class="product-info">
                <h4>{product_name}</h4>
                <p>{brand_name} • {category_name} {heb_badge}</p>
                <p class="history-link">Last updated: {last_updated}</p>
            </div>
            <div class="product-price">{price_display}</div>
        </div>"""
            else:
                html_content += """
        <div class="no-results">
            <h3>No products found</h3>
            <p>Try a different search term or browse categories from the main page.</p>
            <a href="/" class="back-btn">Return to Dashboard</a>
        </div>"""
            
            html_content += """
    </div>
</body>
</html>"""
            
            self.send_html_response(html_content)
            
        except Exception as e:
            print(f"Error in serve_search: {e}")
            self.send_error(500, f"Search error: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    def serve_product_detail(self):
        # Extract product ID from URL
        product_id = self.path.split('/')[-1]
        
        conn = self.get_db_connection()
        if not conn:
            self.send_error(500, "Database connection failed")
            return
            
        try:
            cursor = conn.cursor()
            
            # Get product info
            cursor.execute("SELECT * FROM products WHERE product_id = %s", (product_id,))
            product = cursor.fetchone()
            
            if not product:
                self.send_error(404, "Product not found")
                return
            
            # Get price history
            cursor.execute("""
                SELECT price, recorded_at
                FROM price_history
                WHERE product_id = %s
                ORDER BY recorded_at DESC
                LIMIT 50
            """, (product_id,))
            price_history = cursor.fetchall()
            
            # Calculate price statistics
            prices = [float(p['price']) for p in price_history if p['price']]
            if prices:
                current_price = prices[0]
                min_price = min(prices)
                max_price = max(prices)
                avg_price = sum(prices) / len(prices)
                
                # Calculate price change
                if len(prices) > 1:
                    price_change = current_price - prices[-1]
                    price_change_percent = (price_change / prices[-1]) * 100
                else:
                    price_change = 0
                    price_change_percent = 0
            else:
                current_price = min_price = max_price = avg_price = price_change = price_change_percent = 0
            
            # Safely escape product information
            product_name = self.escape_text(product['product_name'])
            brand_name = self.escape_text(product['brand_name'])
            category_name = self.escape_text(product['category_name'])
            
            heb_badge = '<span class="heb-brand">HEB Brand</span>' if product['is_own_brand'] else ''
            change_color = "#27ae60" if price_change < 0 else "#e74c3c" if price_change > 0 else "#7f8c8d"
            
            # Use proper HTML entities for arrows
            if price_change < 0:
                change_arrow = "↓"  # Down arrow
            elif price_change > 0:
                change_arrow = "↑"  # Up arrow  
            else:
                change_arrow = "→"  # Right arrow
            
            html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{product_name} - Price History</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            margin: 10px;
            background: #f5f5f5;
            line-height: 1.6;
        }}
        .container {{
            max-width: 1000px;
            margin: 0 auto;
        }}
        .header {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .back-btn {{
            background: #3498db;
            color: white;
            padding: 10px 20px;
            text-decoration: none;
            border-radius: 6px;
            display: inline-block;
            margin-bottom: 15px;
            font-weight: 500;
        }}
        .back-btn:hover {{
            background: #2980b9;
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            color: #333;
        }}
        .header p {{
            margin: 0;
            color: #666;
        }}
        .price-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 12px;
            margin-bottom: 20px;
        }}
        .stat-card {{
            background: #fff;
            padding: 18px;
            border-radius: 8px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stat-number {{
            font-size: 1.6em;
            font-weight: bold;
            margin-bottom: 5px;
        }}
        .stat-label {{
            font-size: 0.9em;
            color: #666;
        }}
        .chart-container {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .price-bar {{
            height: 16px;
            background: #e74c3c;
            margin: 4px 0;
            border-radius: 3px;
        }}
        .price-history {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .history-item {{
            padding: 12px;
            border-bottom: 1px solid #eee;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .history-item:last-child {{
            border-bottom: none;
        }}
        .heb-brand {{
            background: #e74c3c;
            color: white;
            padding: 5px 12px;
            border-radius: 15px;
            font-size: 0.9em;
            font-weight: 500;
        }}
        @media (max-width: 768px) {{
            .price-stats {{
                grid-template-columns: repeat(2, 1fr);
            }}
            .history-item {{
                flex-direction: column;
                align-items: flex-start;
            }}
            .history-item span {{
                margin: 2px 0;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <a href="javascript:history.back()" class="back-btn">← Back</a>
            <h1>{product_name}</h1>
            <p>{brand_name} • {category_name} {heb_badge}</p>
        </div>

        <div class="price-stats">
            <div class="stat-card">
                <div class="stat-number" style="color: #e74c3c;">${current_price:.2f}</div>
                <div class="stat-label">Current Price</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" style="color: #27ae60;">${min_price:.2f}</div>
                <div class="stat-label">Lowest Price</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" style="color: #e67e22;">${max_price:.2f}</div>
                <div class="stat-label">Highest Price</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" style="color: #3498db;">${avg_price:.2f}</div>
                <div class="stat-label">Average Price</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" style="color: {change_color};">
                    {change_arrow} ${abs(price_change):.2f}
                </div>
                <div class="stat-label">Price Change</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" style="color: {change_color};">
                    {price_change_percent:+.1f}%
                </div>
                <div class="stat-label">Percent Change</div>
            </div>
        </div>"""
            
            if price_history:
                # Simple price chart
                max_chart_price = max(float(p['price']) for p in price_history if p['price'])
                min_chart_price = min(float(p['price']) for p in price_history if p['price'])
                price_range = max_chart_price - min_chart_price if max_chart_price > min_chart_price else 1
                
                html_content += """
        <div class="chart-container">
            <h3>Price Trend (Last 20 Records)</h3>"""
                
                for i, record in enumerate(price_history[:20]):
                    if record['price']:
                        bar_width = ((float(record['price']) - min_chart_price) / price_range) * 100
                        date_str = record['recorded_at'].strftime('%m/%d')
                        html_content += f"""
            <div style="margin: 6px 0; display: flex; align-items: center;">
                <span style="width: 50px; font-size: 0.85em; color: #666;">{date_str}</span>
                <div class="price-bar" style="width: {bar_width}%; max-width: 280px;"></div>
                <span style="margin-left: 12px; font-weight: bold; color: #333;">${record['price']:.2f}</span>
            </div>"""
                
                html_content += "</div>"
                
                # Price history table
                html_content += f"""
        <div class="price-history">
            <h3>Complete Price History ({len(price_history)} records)</h3>"""
                
                for i, record in enumerate(price_history):
                    if record['price']:
                        date_str = record['recorded_at'].strftime('%m/%d/%Y')
                        
                        # Calculate change from previous price
                        change_text = ""
                        change_style = ""
                        if i < len(price_history) - 1 and price_history[i+1]['price']:
                            prev_price = float(price_history[i+1]['price'])
                            curr_price = float(record['price'])
                            change = curr_price - prev_price
                            if change != 0:
                                change_text = f" ({change:+.2f})"
                                change_style = f"color: {'#27ae60' if change < 0 else '#e74c3c'};"
                        
                        html_content += f"""
            <div class="history-item">
                <span>{date_str}</span>
                <span style="font-weight: bold;">
                    ${record['price']:.2f}
                    <span style="{change_style}">{change_text}</span>
                </span>
            </div>"""
            else:
                html_content += """
        <div class="price-history">
            <h3>No Price History Available</h3>
            <p>No price data found for this product.</p>
        </div>"""
            
            html_content += """
    </div>
</body>
</html>"""
            
            self.send_html_response(html_content)
            
        except Exception as e:
            print(f"Error in serve_product_detail: {e}")
            self.send_error(500, f"Product detail error: {str(e)}")
        finally:
            if conn:
                conn.close()

if __name__ == '__main__':
    PORT = 8000
    
    try:
        with ThreadedTCPServer(("0.0.0.0", PORT), HEBHandler) as httpd:
            print(f"Starting HEB Dashboard on all interfaces, port {PORT}")
            print(f"Local access: http://localhost:{PORT}")
            print(f"Network access: http://192.168.86.29:{PORT}")
            print("Press Ctrl+C to stop")
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
    except Exception as e:
        print(f"Server error: {e}")