"""
Simple Production API for Bunnings Price Tracker
No authentication - just serves the dashboard

Run with: python simple_api.py
"""

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from db_manager import DatabaseManager
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Database connection
db = DatabaseManager()


# ============================================
# ROUTES
# ============================================

@app.route('/')
def index():
    """Serve main dashboard"""
    return send_from_directory('.', 'dashboard_simple.html')


@app.route('/api/stats')
def get_stats():
    """Get overall statistics"""
    stats = db.get_statistics()
    changes_7d = db.get_price_changes(days=7)

    return jsonify({
        'total_products': stats['total_products'],
        'total_records': stats['total_price_records'],
        'products_in_stock': stats['products_in_stock'],
        'changes_7d': len(changes_7d),
        'price_drops': len([c for c in changes_7d if c['change_amount'] < 0]),
        'price_increases': len([c for c in changes_7d if c['change_amount'] > 0])
    })


@app.route('/api/products')
def get_products():
    """Get all products with latest prices"""
    products = db.get_all_products()

    result = []
    for product in products:
        latest = db.get_latest_price(product.id)
        if latest:
            result.append({
                'id': product.id,
                'name': product.name,
                'sku': product.sku,
                'category': product.category or 'Uncategorized',
                'price': latest.price,
                'in_stock': latest.in_stock,
                'url': product.product_url,
                'last_updated': latest.scraped_at.isoformat()
            })

    return jsonify(result)


@app.route('/api/product/<int:product_id>/history')
def get_product_history(product_id):
    """Get price history for a product"""
    days = 90  # Default 90 days
    history = db.get_price_history(product_id, days=days)

    result = [{
        'date': h.scraped_at.isoformat(),
        'price': h.price,
        'in_stock': h.in_stock
    } for h in history]

    return jsonify(result)


@app.route('/api/changes/<int:days>')
def get_changes(days):
    """Get price changes for the last N days"""
    changes = db.get_price_changes(days=days)

    result = [{
        'product_id': c['product_id'],
        'name': c['name'],
        'sku': c['sku'],
        'previous_price': c['previous_price'],
        'current_price': c['current_price'],
        'change_amount': c['change_amount'],
        'change_percent': c['change_percent'],
        'previous_date': c['previous_date'].isoformat(),
        'current_date': c['current_date'].isoformat()
    } for c in changes]

    return jsonify(result)


@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        stats = db.get_statistics()
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'total_products': stats['total_products'],
            'total_records': stats['total_price_records']
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


# ============================================
# MAIN
# ============================================

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Starting Bunnings Price Tracker API")
    print("=" * 60)
    print(f"📊 Dashboard: http://localhost:5000")
    print(f"🔧 API: http://localhost:5000/api/...")
    print("=" * 60)

    app.run(
        debug=True,
        host='0.0.0.0',
        port=5000
    )