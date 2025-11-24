"""
Automated data collection pipeline
Coordinates scraping and database storage
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bunnings_scraper import BunningsScraper
from db_manager import DatabaseManager
import logging
from typing import List
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class DataCollector:
    """Collect product data and save to database"""

    def __init__(self, headless: bool = True):
        self.scraper = BunningsScraper(scrape_delay=3, headless=headless)
        self.db = DatabaseManager()

    def collect_products(self, product_urls: List[str]) -> dict:
        """
        Collect data for a list of product URLs

        Args:
            product_urls: List of Bunnings product URLs

        Returns:
            Dictionary with collection statistics
        """
        logger.info(f"Starting collection for {len(product_urls)} products")

        stats = {
            'total': len(product_urls),
            'successful': 0,
            'failed': 0,
            'errors': []
        }

        for i, url in enumerate(product_urls, 1):
            logger.info(f"Processing product {i}/{len(product_urls)}")

            try:
                # Scrape product
                product = self.scraper.scrape_product(url)

                if product:
                    # Save to database
                    success = self.db.save_product_from_scraper(product)

                    if success:
                        stats['successful'] += 1
                        logger.info(f"✅ Saved: {product.name}")
                    else:
                        stats['failed'] += 1
                        stats['errors'].append({
                            'url': url,
                            'error': 'Database save failed'
                        })
                else:
                    stats['failed'] += 1
                    stats['errors'].append({
                        'url': url,
                        'error': 'Scraping failed'
                    })
                    logger.error(f"❌ Failed to scrape: {url}")

            except Exception as e:
                stats['failed'] += 1
                stats['errors'].append({
                    'url': url,
                    'error': str(e)
                })
                logger.error(f"❌ Error processing {url}: {e}")

        logger.info(f"Collection complete: {stats['successful']}/{stats['total']} successful")
        return stats

    def update_tracked_products(self, product_list_file: str = 'products.json') -> dict:
        """
        Update prices for tracked products from a JSON file

        JSON format:
        {
            "products": [
                {"url": "https://bunnings.com.au/...", "name": "Plywood 9mm"},
                ...
            ]
        }
        """
        logger.info(f"Loading products from {product_list_file}")

        try:
            with open(product_list_file, 'r') as f:
                data = json.load(f)
                product_urls = [p['url'] for p in data.get('products', [])]

            return self.collect_products(product_urls)

        except FileNotFoundError:
            logger.error(f"Product list file not found: {product_list_file}")
            return {'error': 'File not found'}
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in {product_list_file}")
            return {'error': 'Invalid JSON'}

    def close(self):
        """Clean up resources"""
        self.db.close()


# Example products.json file content
EXAMPLE_PRODUCTS_JSON = {
    "products": [
        {
            "url": "https://www.bunnings.com.au/ecoply-2400-x-1200mm-9mm-plywood-pine-structural-cd-grade_p0340162",
            "name": "Ecoply 9mm Plywood"
        },
        {
            "url": "https://www.bunnings.com.au/2440-x-1220mm-3mm-plywood-pine-premium-bc-grade_p0340267",
            "name": "3mm Pine Plywood"
        }
    ]
}

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Collect Bunnings product data')
    parser.add_argument(
        '--products-file',
        default='products.json',
        help='JSON file with product URLs to track'
    )
    parser.add_argument(
        '--headless',
        action='store_true',
        default=True,
        help='Run browser in headless mode'
    )
    parser.add_argument(
        '--create-example',
        action='store_true',
        help='Create example products.json file'
    )

    args = parser.parse_args()

    # Create example file if requested
    if args.create_example:
        with open('products.json', 'w') as f:
            json.dump(EXAMPLE_PRODUCTS_JSON, f, indent=2)
        print("✅ Created example products.json file")
        print("   Edit this file to add your product URLs")
        sys.exit(0)

    # Run collection
    collector = DataCollector(headless=args.headless)

    try:
        stats = collector.update_tracked_products(args.products_file)

        print("\n" + "=" * 60)
        print("COLLECTION SUMMARY")
        print("=" * 60)
        print(f"Total products: {stats.get('total', 0)}")
        print(f"Successful: {stats.get('successful', 0)}")
        print(f"Failed: {stats.get('failed', 0)}")

        if stats.get('errors'):
            print(f"\nErrors ({len(stats['errors'])}):")
            for error in stats['errors'][:5]:  # Show first 5 errors
                print(f"  • {error['url']}")
                print(f"    {error['error']}")

        print("=" * 60)

    finally:
        collector.close()