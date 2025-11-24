"""
Scheduler for automated price collection
Runs the collector at specified intervals
"""

import schedule
import time
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.collector import DataCollector
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class PriceScheduler:
    """Schedule automated price collection"""

    def __init__(self, products_file: str = 'products.json', headless: bool = True):
        self.products_file = products_file
        self.headless = headless
        self.run_count = 0

    def run_collection(self):
        """Run a single collection cycle"""
        self.run_count += 1

        logger.info("=" * 70)
        logger.info(f"Starting scheduled collection #{self.run_count} at {datetime.now()}")
        logger.info("=" * 70)

        collector = DataCollector(headless=self.headless)

        try:
            stats = collector.update_tracked_products(self.products_file)

            logger.info(f"Collection #{self.run_count} complete")
            logger.info(f"  Successful: {stats.get('successful', 0)}/{stats.get('total', 0)}")
            logger.info(f"  Failed: {stats.get('failed', 0)}")

            # Log errors
            if stats.get('errors'):
                logger.warning(f"  Errors encountered: {len(stats['errors'])}")
                for error in stats['errors'][:3]:  # Log first 3 errors
                    logger.error(f"    • {error.get('error')}")

        except Exception as e:
            logger.error(f"Collection failed: {e}", exc_info=True)
        finally:
            collector.close()

        logger.info(f"Next collection scheduled in 6 hours")

    def start(self):
        """Start the scheduler"""
        logger.info("=" * 70)
        logger.info("BUNNINGS PRICE TRACKER - SCHEDULER STARTED")
        logger.info("=" * 70)
        logger.info(f"Products file: {self.products_file}")
        logger.info(f"Headless mode: {self.headless}")
        logger.info("")

        # Run immediately on start
        logger.info("Running initial collection...")
        self.run_collection()

        # Schedule for every 6 hours
        schedule.every(6).hours.do(self.run_collection)

        # Also schedule at specific times (optional)
        schedule.every().day.at("08:00").do(self.run_collection)
        schedule.every().day.at("14:00").do(self.run_collection)
        schedule.every().day.at("20:00").do(self.run_collection)

        logger.info("")
        logger.info("Schedule configured:")
        logger.info("  • Every 6 hours")
        logger.info("  • Daily at 08:00, 14:00, 20:00")
        logger.info("")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 70)
        logger.info("")

        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute

        except KeyboardInterrupt:
            logger.info("")
            logger.info("=" * 70)
            logger.info("Scheduler stopped by user")
            logger.info(f"Total collections run: {self.run_count}")
            logger.info("=" * 70)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Schedule automated price collection')
    parser.add_argument(
        '--products-file',
        default='products.json',
        help='JSON file with product URLs to track'
    )
    parser.add_argument(
        '--no-headless',
        action='store_true',
        help='Show browser window (for debugging)'
    )

    args = parser.parse_args()

    scheduler = PriceScheduler(
        products_file=args.products_file,
        headless=not args.no_headless
    )

    scheduler.start()