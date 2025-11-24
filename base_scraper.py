"""
Base scraper class for Australian retailers
Handles common functionality like rate limiting, retries, and error handling
"""

import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
import random

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


@dataclass
class Product:
    """Standard product data structure"""
    name: str
    sku: str
    price: float
    url: str
    supplier: str
    category: str
    in_stock: bool
    unit: str = "each"
    location: Optional[str] = None
    scraped_at: datetime = None
    additional_info: dict = None

    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.now()
        if self.additional_info is None:
            self.additional_info = {}


class BaseScraper(ABC):
    """Base class for all retailer scrapers"""

    def __init__(self, supplier_name: str, scrape_delay: int = 3):
        self.supplier_name = supplier_name
        self.scrape_delay = scrape_delay
        self.logger = logging.getLogger(f"{__name__}.{supplier_name}")
        self.last_request_time = 0

    def _respect_rate_limit(self):
        """Ensure we don't make requests too quickly"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.scrape_delay:
            sleep_time = self.scrape_delay - time_since_last_request
            # Add small random delay to appear more human
            sleep_time += random.uniform(0.5, 1.5)
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    @abstractmethod
    def scrape_product(self, product_url: str) -> Optional[Product]:
        """Scrape a single product page"""
        pass

    @abstractmethod
    def search_products(self, search_term: str, max_results: int = 10) -> List[Product]:
        """Search for products"""
        pass

    def clean_price(self, price_text: str) -> float:
        """Extract numeric price from text like '$45.99' or '45.99'"""
        import re
        # Remove everything except digits and decimal point
        cleaned = re.sub(r'[^\d.]', '', str(price_text))
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            self.logger.error(f"Could not parse price: {price_text}")
            return 0.0

    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        # Remove extra whitespace
        return ' '.join(str(text).split())

    def extract_sku_from_url(self, url: str) -> Optional[str]:
        """Extract SKU/product ID from URL"""
        import re
        # Look for pattern like _p1234567
        match = re.search(r'_p(\d+)', url)
        if match:
            return match.group(1)
        return None