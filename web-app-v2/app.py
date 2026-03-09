#!/usr/bin/env python3
"""
HEB Price Tracker v2
A modern web dashboard for tracking grocery price inflation/deflation over time.

To run:
    cp .env.example .env          # then fill in your values
    pip install flask psycopg2-binary python-dotenv
    python app.py

Then open: http://localhost:8080
"""

import os
import re
from flask import Flask, render_template, request, abort
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()   # reads .env file if present

app = Flask(__name__)

# ── Secret key (required for session security) ─────────────────────────────────
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', os.urandom(32).hex())

# ── Database config — values come from .env, never from code ───────────────────
DB_CONFIG = dict(
    host     = os.getenv('DB_HOST',     'localhost'),
    port     = int(os.getenv('DB_PORT', '5432')),
    database = os.getenv('DB_NAME',     'heb_products'),
    user     = os.getenv('DB_USER',     'memorodriguez'),
    password = os.getenv('DB_PASSWORD', '')
)

# ── Allowed sort values (strict whitelist — prevents SQL injection) ─────────────
ALLOWED_SORTS = {
    'name':           'p.product_name ASC',
    'price_asc':      'current_price ASC  NULLS LAST',
    'price_desc':     'current_price DESC NULLS LAST',
    'pct_change':     'pct_change     DESC NULLS LAST',
    'pct_change_asc': 'pct_change     ASC  NULLS LAST',
}

# ── Reusable CTE: first and last price per product ─────────────────────────────
PRICE_BOUNDS_CTE = """
WITH price_bounds AS (
    SELECT
        product_id,
        MIN(CASE WHEN rn_asc  = 1 THEN price       END) AS first_price,
        MIN(CASE WHEN rn_asc  = 1 THEN recorded_at END) AS first_date,
        MIN(CASE WHEN rn_desc = 1 THEN price       END) AS last_price,
        MIN(CASE WHEN rn_desc = 1 THEN recorded_at END) AS last_date,
        COUNT(*)                                         AS record_count
    FROM (
        SELECT product_id, price, recorded_at,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY recorded_at ASC)  AS rn_asc,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY recorded_at DESC) AS rn_desc
        FROM price_history
    ) ranked
    GROUP BY product_id
)
"""


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_db():
    """Open a DB connection; returns 503 if database is unreachable."""
    try:
        return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
    except psycopg2.Error:
        abort(503)


def validate_product_id(product_id: str) -> bool:
    """Product IDs from HEB are alphanumeric with hyphens only."""
    return bool(re.match(r'^[a-zA-Z0-9_\-]{1,64}$', product_id))


# ── Security headers on every response ────────────────────────────────────────
@app.after_request
def set_security_headers(response):
    response.headers['X-Content-Type-Options']  = 'nosniff'
    response.headers['X-Frame-Options']         = 'SAMEORIGIN'
    response.headers['X-XSS-Protection']        = '1; mode=block'
    response.headers['Referrer-Policy']         = 'strict-origin-when-cross-origin'
    response.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; "
        "style-src  'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; "
        "font-src   'self' https://cdnjs.cloudflare.com; "
        "img-src    'self' data:;"
    )
    return response


# ── Error handlers — never expose stack traces ─────────────────────────────────
@app.errorhandler(400)
def bad_request(e):
    return render_template('error.html', code=400, message='Bad request.'), 400

@app.errorhandler(404)
def not_found(e):
    return render_template('error.html', code=404, message='Page not found.'), 404

@app.errorhandler(500)
def server_error(e):
    return render_template('error.html', code=500, message='Something went wrong on our end.'), 500

@app.errorhandler(503)
def db_unavailable(e):
    return render_template('error.html', code=503, message='Database is unavailable. Is PostgreSQL running?'), 503


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route('/')
def dashboard():
    conn = get_db()
    cur  = conn.cursor()

    # Overall stats
    cur.execute("""
        SELECT
            COUNT(DISTINCT p.product_id)    AS total_products,
            COUNT(DISTINCT p.category_name) AS total_categories,
            COUNT(ph.id)                    AS total_prices,
            MIN(ph.recorded_at)             AS first_record,
            MAX(ph.recorded_at)             AS last_record
        FROM products p
        LEFT JOIN price_history ph ON p.product_id = ph.product_id
    """)
    stats = dict(cur.fetchone())

    if stats['first_record'] and stats['last_record']:
        stats['days_tracked'] = (stats['last_record'] - stats['first_record']).days
    else:
        stats['days_tracked'] = 0

    # Overall average inflation across all tracked products
    cur.execute(PRICE_BOUNDS_CTE + """
        SELECT ROUND(
            AVG(
                (last_price - first_price)
                / NULLIF(first_price, 0) * 100
            )::numeric, 2
        ) AS overall_inflation
        FROM price_bounds
        WHERE first_price > 0 AND record_count > 1
    """)
    row = cur.fetchone()
    overall_inflation = float(row['overall_inflation']) if row and row['overall_inflation'] is not None else None

    # Products with biggest price INCREASES
    cur.execute(PRICE_BOUNDS_CTE + """
        SELECT
            p.product_id,
            p.product_name,
            p.brand_name,
            p.category_name,
            pb.first_price,
            pb.last_price,
            (pb.last_price - pb.first_price) AS abs_change,
            ROUND(((pb.last_price - pb.first_price) / pb.first_price * 100)::numeric, 2) AS pct_change
        FROM products p
        JOIN price_bounds pb ON p.product_id = pb.product_id
        WHERE pb.first_price > 0
          AND pb.record_count > 1
          AND pb.last_price > pb.first_price
        ORDER BY pct_change DESC
        LIMIT 8
    """)
    top_increases = cur.fetchall()

    # Products with biggest price DROPS
    cur.execute(PRICE_BOUNDS_CTE + """
        SELECT
            p.product_id,
            p.product_name,
            p.brand_name,
            p.category_name,
            pb.first_price,
            pb.last_price,
            (pb.last_price - pb.first_price)  AS abs_change,
            ROUND(((pb.last_price - pb.first_price) / pb.first_price * 100)::numeric, 2) AS pct_change
        FROM products p
        JOIN price_bounds pb ON p.product_id = pb.product_id
        WHERE pb.first_price > 0
          AND pb.record_count > 1
          AND pb.last_price < pb.first_price
        ORDER BY pct_change ASC
        LIMIT 8
    """)
    top_decreases = cur.fetchall()

    # Category inflation summary
    cur.execute(PRICE_BOUNDS_CTE + """
        SELECT
            p.category_name,
            COUNT(DISTINCT p.product_id) AS product_count,
            ROUND(AVG(pb.last_price)::numeric, 2) AS avg_current_price,
            ROUND(AVG(
                CASE WHEN pb.record_count > 1 AND pb.first_price > 0
                    THEN (pb.last_price - pb.first_price) / pb.first_price * 100
                END
            )::numeric, 2) AS avg_pct_change,
            COUNT(CASE WHEN pb.last_price > pb.first_price AND pb.record_count > 1 THEN 1 END) AS increased,
            COUNT(CASE WHEN pb.last_price < pb.first_price AND pb.record_count > 1 THEN 1 END) AS decreased
        FROM products p
        LEFT JOIN price_bounds pb ON p.product_id = pb.product_id
        GROUP BY p.category_name
        ORDER BY avg_pct_change DESC NULLS LAST
    """)
    categories = cur.fetchall()

    conn.close()
    return render_template('dashboard.html',
        stats=stats,
        overall_inflation=overall_inflation,
        top_increases=top_increases,
        top_decreases=top_decreases,
        categories=categories,
    )


@app.route('/search')
def search():
    # Input validation: cap length, strip whitespace
    q            = request.args.get('q', '').strip()[:100]
    category     = request.args.get('category', '').strip()[:100]
    brand_filter = request.args.get('brand', '').strip()

    # Strict whitelist for sort — any unknown value falls back to 'name'
    sort      = request.args.get('sort', 'name')
    sort      = sort if sort in ALLOWED_SORTS else 'name'
    order_sql = ALLOWED_SORTS[sort]

    conn = get_db()
    cur  = conn.cursor()

    cur.execute("SELECT DISTINCT category_name FROM products ORDER BY category_name")
    all_categories = [r['category_name'] for r in cur.fetchall()]

    where_parts = []
    params      = {}

    if q:
        where_parts.append("(p.product_name ILIKE %(q)s OR p.brand_name ILIKE %(q)s)")
        params['q'] = f'%{q}%'
    if category:
        where_parts.append("p.category_name = %(category)s")
        params['category'] = category
    if brand_filter == 'heb':
        where_parts.append("p.is_own_brand = TRUE")

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    # NOTE: order_sql comes exclusively from ALLOWED_SORTS (whitelist), never from user input directly
    cur.execute(PRICE_BOUNDS_CTE + f"""
        , current_prices AS (
            SELECT DISTINCT ON (product_id)
                product_id,
                price       AS current_price,
                recorded_at AS last_seen
            FROM price_history
            ORDER BY product_id, recorded_at DESC
        )
        SELECT
            p.product_id,
            p.product_name,
            p.brand_name,
            p.category_name,
            p.is_own_brand,
            cp.current_price,
            pb.first_price,
            pb.record_count,
            cp.last_seen,
            CASE
                WHEN pb.record_count > 1 AND pb.first_price > 0
                THEN ROUND(((cp.current_price - pb.first_price) / pb.first_price * 100)::numeric, 2)
                ELSE NULL
            END AS pct_change
        FROM products p
        LEFT JOIN current_prices cp ON p.product_id = cp.product_id
        LEFT JOIN price_bounds   pb ON p.product_id = pb.product_id
        {where_sql}
        ORDER BY {order_sql}
        LIMIT 200
    """, params)
    products = cur.fetchall()

    conn.close()
    return render_template('search.html',
        products=products,
        q=q,
        category=category,
        sort=sort,
        brand_filter=brand_filter,
        all_categories=all_categories,
        result_count=len(products),
    )


@app.route('/product/<product_id>')
def product_detail(product_id):
    # Validate product_id format before touching the database
    if not validate_product_id(product_id):
        abort(404)

    conn = get_db()
    cur  = conn.cursor()

    cur.execute("SELECT * FROM products WHERE product_id = %s", (product_id,))
    product = cur.fetchone()
    if not product:
        conn.close()
        abort(404)

    cur.execute("""
        SELECT price, recorded_at
        FROM price_history
        WHERE product_id = %s
        ORDER BY recorded_at ASC
    """, (product_id,))
    raw_history = cur.fetchall()

    history = []
    for i, row in enumerate(raw_history):
        price = float(row['price'])
        if i == 0:
            change     = None
            change_pct = None
        else:
            prev       = float(raw_history[i - 1]['price'])
            change     = round(price - prev, 2)
            change_pct = round((price - prev) / prev * 100, 2) if prev > 0 else None
        history.append({
            'date':       row['recorded_at'].strftime('%b %d, %Y  %H:%M'),
            'price':      price,
            'change':     change,
            'change_pct': change_pct,
        })

    chart_dates  = [h['date'][:12] for h in history]
    chart_prices = [h['price']     for h in history]

    stats = {}
    if chart_prices:
        stats['current']    = chart_prices[-1]
        stats['first']      = chart_prices[0]
        stats['min']        = min(chart_prices)
        stats['max']        = max(chart_prices)
        stats['avg']        = round(sum(chart_prices) / len(chart_prices), 2)
        stats['records']    = len(chart_prices)
        stats['abs_change'] = round(chart_prices[-1] - chart_prices[0], 2)
        stats['pct_change'] = (
            round((chart_prices[-1] - chart_prices[0]) / chart_prices[0] * 100, 2)
            if chart_prices[0] > 0 else 0
        )

    cur.execute(PRICE_BOUNDS_CTE + """
        , current_prices AS (
            SELECT DISTINCT ON (product_id) product_id, price AS current_price
            FROM price_history ORDER BY product_id, recorded_at DESC
        )
        SELECT
            p.product_id, p.product_name, p.brand_name,
            cp.current_price,
            CASE WHEN pb.record_count > 1 AND pb.first_price > 0
                THEN ROUND(((cp.current_price - pb.first_price) / pb.first_price * 100)::numeric, 2)
                ELSE NULL
            END AS pct_change
        FROM products p
        LEFT JOIN current_prices cp ON p.product_id = cp.product_id
        LEFT JOIN price_bounds   pb ON p.product_id = pb.product_id
        WHERE p.category_name = %(cat)s
          AND p.product_id   != %(pid)s
        ORDER BY cp.current_price ASC NULLS LAST
        LIMIT 6
    """, {'cat': product['category_name'], 'pid': product_id})
    similar = cur.fetchall()

    conn.close()
    history_display = list(reversed(history))

    return render_template('product.html',
        product=product,
        history_display=history_display,
        chart_dates=chart_dates,
        chart_prices=chart_prices,
        stats=stats,
        similar=similar,
    )


@app.route('/categories')
def categories():
    conn = get_db()
    cur  = conn.cursor()

    cur.execute(PRICE_BOUNDS_CTE + """
        SELECT
            p.category_name,
            COUNT(DISTINCT p.product_id) AS product_count,
            ROUND(AVG(pb.last_price)::numeric,  2) AS avg_price,
            ROUND(AVG(pb.first_price)::numeric, 2) AS avg_first_price,
            ROUND(AVG(
                CASE WHEN pb.record_count > 1 AND pb.first_price > 0
                    THEN (pb.last_price - pb.first_price) / pb.first_price * 100
                END
            )::numeric, 2) AS avg_pct_change,
            COUNT(CASE WHEN pb.last_price > pb.first_price AND pb.record_count > 1 THEN 1 END) AS increased,
            COUNT(CASE WHEN pb.last_price < pb.first_price AND pb.record_count > 1 THEN 1 END) AS decreased,
            COUNT(CASE WHEN pb.last_price = pb.first_price AND pb.record_count > 1 THEN 1 END) AS unchanged,
            COUNT(CASE WHEN pb.record_count = 1                                    THEN 1 END) AS one_record
        FROM products p
        LEFT JOIN price_bounds pb ON p.product_id = pb.product_id
        GROUP BY p.category_name
        ORDER BY avg_pct_change DESC NULLS LAST
    """)
    cats = cur.fetchall()

    conn.close()
    return render_template('categories.html', categories=cats)


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    debug_mode = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    port       = int(os.getenv('PORT', '8080'))

    print("=" * 50)
    print("  HEB Price Tracker v2")
    print(f"  Open: http://localhost:{port}")
    print(f"  Debug: {debug_mode}")
    print("=" * 50)
    # Use 0.0.0.0 on cloud platforms (Railway, Render) — 127.0.0.1 for local
    host = os.getenv('HOST', '0.0.0.0' if os.getenv('RAILWAY_ENVIRONMENT') or os.getenv('RENDER') else '127.0.0.1')
    app.run(debug=debug_mode, host=host, port=port)
