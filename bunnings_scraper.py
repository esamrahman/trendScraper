"""
Bunnings Warehouse scraper
Scrapes product information from bunnings.com.au
"""

import os
import time
from typing import Optional, List
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from base_scraper import BaseScraper, Product
import logging

logger = logging.getLogger(__name__)


class BunningsScraper(BaseScraper):
    """Scraper for Bunnings Warehouse Australia"""

    def __init__(self, scrape_delay: int = 3, headless: bool = True):
        super().__init__("Bunnings", scrape_delay)
        self.base_url = "https://www.bunnings.com.au"
        self.headless = headless
        self.user_agent = os.getenv(
            'USER_AGENT',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        )

    def _get_page_content(self, url: str, wait_for_selector: str = None, max_retries: int = 3) -> Optional[str]:
        """
        Get page content using Playwright
        Handles JavaScript-rendered content
        """
        self._respect_rate_limit()

        for attempt in range(max_retries):
            try:
                with sync_playwright() as p:
                    # Launch browser
                    browser = p.chromium.launch(headless=self.headless)

                    # Create context with realistic settings
                    context = browser.new_context(
                        user_agent=self.user_agent,
                        viewport={'width': 1920, 'height': 1080},
                        locale='en-AU',
                    )

                    page = context.new_page()

                    # Navigate to page
                    self.logger.info(f"Loading page: {url}")
                    page.goto(url, wait_until='domcontentloaded', timeout=30000)

                    # Wait for specific selector if provided
                    if wait_for_selector:
                        try:
                            page.wait_for_selector(wait_for_selector, timeout=10000)
                        except PlaywrightTimeout:
                            self.logger.warning(f"Timeout waiting for selector: {wait_for_selector}")
                    else:
                        # Default wait for network to be mostly idle
                        time.sleep(2)

                    # Get page content
                    html_content = page.content()

                    # Clean up
                    browser.close()

                    return html_content

            except PlaywrightTimeout:
                self.logger.error(f"Timeout loading page (attempt {attempt + 1}/{max_retries}): {url}")
                if attempt < max_retries - 1:
                    time.sleep(5)  # Wait before retry
                    continue
                return None

            except Exception as e:
                self.logger.error(f"Error loading page (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                return None

        return None

    def scrape_product(self, product_url: str) -> Optional[Product]:
        """
        Scrape a single Bunnings product page

        Args:
            product_url: Full URL to product page

        Returns:
            Product object or None if scraping failed
        """
        self.logger.info(f"Scraping product: {product_url}")

        # Get page content
        html_content = self._get_page_content(product_url)

        if not html_content:
            self.logger.error(f"Failed to load page: {product_url}")
            return None

        # Parse HTML
        soup = BeautifulSoup(html_content, 'lxml')

        try:
            # Extract product name
            product_name = self._extract_product_name(soup)
            if not product_name:
                self.logger.error("Could not find product name")
                return None

            # Extract price
            price = self._extract_price(soup)
            if price == 0.0:
                self.logger.warning("Could not find valid price")

            # Extract SKU
            sku = self.extract_sku_from_url(product_url)
            if not sku:
                sku = self._extract_sku_from_page(soup)

            # Extract stock status
            in_stock = self._extract_stock_status(soup)

            # Extract category
            category = self._extract_category(soup)

            # Extract unit (per sheet, per piece, etc.)
            unit = self._extract_unit(soup)

            # Extract additional info
            additional_info = {
                'brand': self._extract_brand(soup),
                'description': self._extract_description(soup),
                'specifications': self._extract_specifications(soup)
            }

            product = Product(
                name=product_name,
                sku=sku or "unknown",
                price=price,
                url=product_url,
                supplier=self.supplier_name,
                category=category or "Uncategorized",
                in_stock=in_stock,
                unit=unit or "each",
                additional_info=additional_info
            )

            self.logger.info(f"Successfully scraped: {product_name} - ${price}")
            return product

        except Exception as e:
            self.logger.error(f"Error parsing product page: {e}", exc_info=True)
            return None

    def _extract_product_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract product name from page"""
        # Try multiple possible selectors
        selectors = [
            'h1[class*="product"]',
            'h1.product-title',
            'h1',
            '[data-locator="product-title"]',
            'h1[itemprop="name"]'
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element and element.text.strip():
                return self.clean_text(element.text)

        return None

    def _extract_price(self, soup: BeautifulSoup) -> float:
        """Extract price from page"""
        # Try multiple selectors for price
        selectors = [
            '[data-locator="product-price"]',
            '.price-format__main-price',
            '[class*="price"]',
            '[itemprop="price"]',
            'span.price'
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                # Try to get content or text
                price_text = element.get('content') or element.text
                if price_text:
                    price = self.clean_price(price_text)
                    if price > 0:
                        return price

        # Look for price pattern in text
        import re
        text = soup.get_text()
        price_pattern = r'\$\s*(\d+(?:,\d{3})*(?:\.\d{2})?)'
        matches = re.findall(price_pattern, text)
        if matches:
            # Get first reasonable price (between $0.01 and $100,000)
            for match in matches:
                price = self.clean_price(match)
                if 0.01 <= price <= 100000:
                    return price

        return 0.0

    def _extract_sku_from_page(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract SKU from page content"""
        # Try multiple selectors
        selectors = [
            '[data-locator="product-sku"]',
            '.product-sku',
            '[itemprop="sku"]'
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                sku_text = element.text.strip()
                # Extract just the number
                import re
                match = re.search(r'\d+', sku_text)
                if match:
                    return match.group(0)

        return None

    def _extract_stock_status(self, soup: BeautifulSoup) -> bool:
        """Check if product is in stock"""
        # Look for out of stock indicators
        out_of_stock_texts = [
            'out of stock',
            'not available',
            'currently unavailable',
            'sold out'
        ]

        page_text = soup.get_text().lower()

        for text in out_of_stock_texts:
            if text in page_text:
                return False

        # Look for in stock indicators
        in_stock_selectors = [
            '[data-locator="in-stock"]',
            '.in-stock',
            '[class*="available"]'
        ]

        for selector in in_stock_selectors:
            element = soup.select_one(selector)
            if element:
                return True

        # Default to True if no clear indicator
        return True

    def _extract_category(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract product category from breadcrumbs or page structure"""
        # Try breadcrumbs
        breadcrumbs = soup.select('[class*="breadcrumb"] a')
        if breadcrumbs and len(breadcrumbs) > 1:
            # Return second-to-last breadcrumb (skip "Home" and current product)
            return self.clean_text(breadcrumbs[-2].text)

        # Try category from URL
        # Example: /products/building-hardware/timber/...
        # Extract "timber"

        return None

    def _extract_unit(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract unit of measurement (per sheet, per metre, each, etc.)"""
        # Look for unit indicators near price
        selectors = [
            '.price-format__unit',
            '[data-locator="price-unit"]',
            'span.unit'
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return self.clean_text(element.text)

        return "each"

    def _extract_brand(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract brand name"""
        selectors = [
            '[itemprop="brand"]',
            '[data-locator="brand"]',
            '.brand-name'
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return self.clean_text(element.text)

        return None

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract product description"""
        selectors = [
            '[itemprop="description"]',
            '[data-locator="product-description"]',
            '.product-description'
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                desc = self.clean_text(element.text)
                # Limit description length
                return desc[:500] if len(desc) > 500 else desc

        return None

    def _extract_specifications(self, soup: BeautifulSoup) -> dict:
        """Extract product specifications as key-value pairs"""
        specs = {}

        # Try to find specification table
        spec_tables = soup.select('[class*="specification"]')

        for table in spec_tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    key = self.clean_text(cells[0].text)
                    value = self.clean_text(cells[1].text)
                    if key and value:
                        specs[key] = value

        return specs

    def search_products(self, search_term: str, max_results: int = 10) -> List[Product]:
        """
        Search Bunnings for products

        Args:
            search_term: Search query (e.g., "plywood 4x8")
            max_results: Maximum number of products to return

        Returns:
            List of Product objects
        """
        self.logger.info(f"Searching for: {search_term}")

        # Construct search URL
        # Note: Bunnings robots.txt disallows /search, so we need to use category pages
        # For now, we'll use direct product URLs instead
        # In production, you'd maintain a list of known product URLs

        self.logger.warning("Search functionality limited - provide direct product URLs instead")
        return []

    def scrape_category_page(self, category_url: str, max_products: int = 20) -> List[Product]:
        """
        Scrape products from a category page

        Args:
            category_url: URL to category page (e.g., /products/building-hardware/timber/)
            max_products: Maximum products to scrape from this category

        Returns:
            List of Product objects
        """
        self.logger.info(f"Scraping category: {category_url}")

        # Get category page
        html_content = self._get_page_content(category_url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, 'lxml')
        products = []

        # Find product links on category page
        # This selector needs to be verified by inspecting actual Bunnings pages
        product_links = soup.select('a[href*="_p"]')[:max_products]

        self.logger.info(f"Found {len(product_links)} product links")

        for link in product_links:
            href = link.get('href')
            if not href:
                continue

            # Make absolute URL
            if href.startswith('/'):
                product_url = self.base_url + href
            else:
                product_url = href

            # Scrape individual product
            product = self.scrape_product(product_url)
            if product:
                products.append(product)

        self.logger.info(f"Successfully scraped {len(products)} products from category")
        return products


# Test the scraper
if __name__ == "__main__":
    scraper = BunningsScraper(scrape_delay=3, headless=True)

    # Test URLs - replace with actual Bunnings product URLs
    test_urls = [
        "https://www.bunnings.com.au/ecoply-2400-x-1200mm-9mm-plywood-pine-structural-cd-grade_p0340162",
        "https://www.bunnings.com.au/2440-x-1220mm-3mm-plywood-pine-premium-bc-grade_p0340267"
    ]

    print("=" * 60)
    print("TESTING BUNNINGS SCRAPER")
    print("=" * 60)
    print()

    for url in test_urls:
        print(f"Scraping: {url}")
        product = scraper.scrape_product(url)

        if product:
            print(f"✅ Success!")
            print(f"   Name: {product.name}")
            print(f"   SKU: {product.sku}")
            print(f"   Price: ${product.price}")
            print(f"   In Stock: {product.in_stock}")
            print(f"   Unit: {product.unit}")
            print()
        else:
            print(f"❌ Failed to scrape")
            print()