"""
Product URL Monitor
Handles URL changes, redirects, and product discontinuation
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_manager import DatabaseManager, ProductInfo
from datetime import datetime, timedelta
import logging
from typing import Optional, List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProductURLMonitor:
    """Monitor and handle product URL changes"""

    def __init__(self):
        self.db = DatabaseManager()

    def check_url_status(self, product_url: str) -> Dict:
        """
        Check if a URL is accessible and handle redirects

        Returns:
            {
                'status': 'active' | 'redirect' | 'not_found' | 'error',
                'new_url': 'new url if redirected',
                'status_code': 200 | 404 | etc
            }
        """
        try:
            import requests

            # Allow redirects and track them
            response = requests.get(
                product_url,
                allow_redirects=True,
                timeout=10,
                headers={'User-Agent': 'Mozilla/5.0'}
            )

            result = {
                'original_url': product_url,
                'status_code': response.status_code,
                'checked_at': datetime.now()
            }

            # Check if redirected
            if response.url != product_url:
                result['status'] = 'redirect'
                result['new_url'] = response.url
                logger.warning(f"URL redirected: {product_url} -> {response.url}")

            elif response.status_code == 200:
                result['status'] = 'active'
                result['new_url'] = None

            elif response.status_code == 404:
                result['status'] = 'not_found'
                result['new_url'] = None
                logger.error(f"Product not found (404): {product_url}")

            else:
                result['status'] = 'error'
                result['new_url'] = None
                logger.error(f"Unexpected status {response.status_code}: {product_url}")

            return result

        except Exception as e:
            logger.error(f"Error checking URL {product_url}: {e}")
            return {
                'original_url': product_url,
                'status': 'error',
                'status_code': None,
                'new_url': None,
                'error': str(e),
                'checked_at': datetime.now()
            }

    def find_stale_products(self, days: int = 7) -> List[ProductInfo]:
        """
        Find products that haven't been updated in N days
        These might have broken URLs
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        products = self.db.session.query(ProductInfo).filter(
            ProductInfo.last_updated < cutoff_date
        ).all()

        logger.info(f"Found {len(products)} products not updated in {days} days")
        return products

    def update_product_url(self, product_id: int, new_url: str) -> bool:
        """Update product URL in database"""
        try:
            product = self.db.session.query(ProductInfo).get(product_id)
            if product:
                old_url = product.product_url
                product.product_url = new_url
                product.last_updated = datetime.now()
                self.db.session.commit()

                logger.info(f"Updated URL for product {product_id}")
                logger.info(f"  Old: {old_url}")
                logger.info(f"  New: {new_url}")
                return True
            return False

        except Exception as e:
            logger.error(f"Error updating product URL: {e}")
            self.db.session.rollback()
            return False

    def mark_product_discontinued(self, product_id: int) -> bool:
        """Mark a product as discontinued (but keep history)"""
        try:
            product = self.db.session.query(ProductInfo).get(product_id)
            if product:
                # Add discontinued marker to product name
                if not product.name.startswith("[DISCONTINUED]"):
                    product.name = f"[DISCONTINUED] {product.name}"
                product.last_updated = datetime.now()
                self.db.session.commit()

                logger.info(f"Marked product {product_id} as discontinued")
                return True
            return False

        except Exception as e:
            logger.error(f"Error marking product discontinued: {e}")
            self.db.session.rollback()
            return False

    def run_url_health_check(self) -> Dict:
        """
        Check all tracked products for URL issues
        Returns summary of issues found
        """
        logger.info("=" * 60)
        logger.info("Starting URL Health Check")
        logger.info("=" * 60)

        products = self.db.get_all_products()

        results = {
            'total': len(products),
            'active': 0,
            'redirected': 0,
            'not_found': 0,
            'errors': 0,
            'issues': []
        }

        for i, product in enumerate(products, 1):
            logger.info(f"Checking {i}/{len(products)}: {product.name}")

            status = self.check_url_status(product.product_url)

            if status['status'] == 'active':
                results['active'] += 1

            elif status['status'] == 'redirect':
                results['redirected'] += 1
                results['issues'].append({
                    'product_id': product.id,
                    'product_name': product.name,
                    'sku': product.sku,
                    'issue': 'redirect',
                    'old_url': status['original_url'],
                    'new_url': status['new_url']
                })

                # Auto-update URL if redirected
                self.update_product_url(product.id, status['new_url'])

            elif status['status'] == 'not_found':
                results['not_found'] += 1
                results['issues'].append({
                    'product_id': product.id,
                    'product_name': product.name,
                    'sku': product.sku,
                    'issue': 'not_found',
                    'url': status['original_url']
                })

                # Mark as discontinued
                self.mark_product_discontinued(product.id)

            else:
                results['errors'] += 1
                results['issues'].append({
                    'product_id': product.id,
                    'product_name': product.name,
                    'sku': product.sku,
                    'issue': 'error',
                    'url': status['original_url']
                })

            # Small delay between checks
            import time
            time.sleep(1)

        # Print summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("URL Health Check Complete")
        logger.info("=" * 60)
        logger.info(f"Total Products: {results['total']}")
        logger.info(f"✅ Active: {results['active']}")
        logger.info(f"🔄 Redirected (auto-fixed): {results['redirected']}")
        logger.info(f"❌ Not Found (marked discontinued): {results['not_found']}")
        logger.info(f"⚠️  Errors: {results['errors']}")

        if results['issues']:
            logger.info("")
            logger.info("Issues Found:")
            for issue in results['issues']:
                logger.info(f"  • {issue['product_name']} (SKU: {issue['sku']})")
                logger.info(f"    Issue: {issue['issue']}")
                if issue['issue'] == 'redirect':
                    logger.info(f"    Fixed: URL updated automatically")
                elif issue['issue'] == 'not_found':
                    logger.info(f"    Action: Marked as discontinued")

        return results

    def close(self):
        self.db.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Monitor product URLs')
    parser.add_argument('--check-all', action='store_true', help='Check all product URLs')
    parser.add_argument('--find-stale', type=int, metavar='DAYS', help='Find products not updated in N days')

    args = parser.parse_args()

    monitor = ProductURLMonitor()

    try:
        if args.check_all:
            results = monitor.run_url_health_check()

        elif args.find_stale:
            stale = monitor.find_stale_products(days=args.find_stale)
            print(f"\nProducts not updated in {args.find_stale} days:")
            for product in stale:
                print(f"  • {product.name}")
                print(f"    Last updated: {product.last_updated}")
                print(f"    URL: {product.product_url}")
                print()

        else:
            print("Usage:")
            print("  python url_monitor.py --check-all")
            print("  python url_monitor.py --find-stale 7")

    finally:
        monitor.close()