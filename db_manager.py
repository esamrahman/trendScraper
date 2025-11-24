"""
Database models and manager for storing price data
Uses SQLAlchemy ORM for database operations
Includes SKU-based product matching to handle URL and name changes
"""

import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, JSON, ForeignKey, Index, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from typing import List, Optional, Dict

Base = declarative_base()


class Supplier(Base):
    """Store supplier/retailer information"""
    __tablename__ = 'suppliers'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    website = Column(String(500))
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)

    # Relationships
    prices = relationship('PriceHistory', back_populates='supplier')

    def __repr__(self):
        return f"<Supplier(name='{self.name}')>"


class ProductInfo(Base):
    """Store product information"""
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    sku = Column(String(50), nullable=False, index=True)
    name = Column(String(500), nullable=False)
    category = Column(String(200), index=True)
    unit = Column(String(50))
    supplier_id = Column(Integer, ForeignKey('suppliers.id'), nullable=False)
    product_url = Column(String(1000))
    created_at = Column(DateTime, default=datetime.now)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Create unique constraint on supplier + SKU
    __table_args__ = (
        Index('ix_supplier_sku', 'supplier_id', 'sku', unique=True),
    )

    # Relationships
    supplier = relationship('Supplier')
    prices = relationship('PriceHistory', back_populates='product', cascade='all, delete-orphan')

    def __repr__(self):
        return f"<Product(sku='{self.sku}', name='{self.name}')>"


class PriceHistory(Base):
    """Store historical price data"""
    __tablename__ = 'price_history'

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'), nullable=False, index=True)
    supplier_id = Column(Integer, ForeignKey('suppliers.id'), nullable=False, index=True)

    price = Column(Float, nullable=False)
    in_stock = Column(Boolean, default=True)

    # Additional metadata
    additional_info = Column(JSON)

    scraped_at = Column(DateTime, default=datetime.now, index=True)

    # Relationships
    product = relationship('ProductInfo', back_populates='prices')
    supplier = relationship('Supplier', back_populates='prices')

    # Index for fast queries
    __table_args__ = (
        Index('ix_product_scraped', 'product_id', 'scraped_at'),
    )

    def __repr__(self):
        return f"<PriceHistory(product_id={self.product_id}, price=${self.price}, date={self.scraped_at})>"


class DatabaseManager:
    """Manage database operations with SKU-based product matching"""

    def __init__(self, db_url: str = None):
        if db_url is None:
            db_url = os.getenv('DATABASE_URL', 'sqlite:///bunnings_prices.db')

        self.engine = create_engine(db_url, echo=False)
        Base.metadata.create_all(self.engine)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def close(self):
        """Close database session"""
        self.session.close()

    # === SUPPLIER OPERATIONS ===

    def get_or_create_supplier(self, name: str, website: str = None) -> Supplier:
        """Get existing supplier or create new one"""
        supplier = self.session.query(Supplier).filter_by(name=name).first()

        if not supplier:
            supplier = Supplier(name=name, website=website)
            self.session.add(supplier)
            self.session.commit()
            print(f"Created supplier: {name}")

        return supplier

    # === PRODUCT OPERATIONS (IMPROVED WITH SKU-BASED MATCHING) ===

    def get_or_create_product(
            self,
            sku: str,
            name: str,
            supplier_id: int,
            category: str = None,
            unit: str = "each",
            product_url: str = None
    ) -> ProductInfo:
        """
        LEGACY METHOD - kept for backwards compatibility
        Use get_or_create_product_by_sku instead
        """
        return self.get_or_create_product_by_sku(
            sku=sku,
            supplier_id=supplier_id,
            name=name,
            category=category,
            unit=unit,
            product_url=product_url
        )

    def get_or_create_product_by_sku(
            self,
            sku: str,
            supplier_id: int,
            name: str = None,
            category: str = None,
            unit: str = "each",
            product_url: str = None
    ) -> ProductInfo:
        """
        Get or create product using SKU as primary identifier
        Updates product info if it exists but details changed

        This prevents duplicate products when:
        - Product name changes
        - URL changes
        - Description changes

        As long as SKU stays the same, it's the same product!
        """
        # Try to find by SKU + supplier
        product = self.session.query(ProductInfo).filter_by(
            sku=sku,
            supplier_id=supplier_id
        ).first()

        if not product:
            # Create new product
            product = ProductInfo(
                sku=sku,
                name=name,
                supplier_id=supplier_id,
                category=category,
                unit=unit,
                product_url=product_url
            )
            self.session.add(product)
            self.session.commit()
            print(f"✅ Created NEW product: {name} (SKU: {sku})")
        else:
            # Product exists - check if we need to update info
            updated = False
            changes = []

            # Check name change
            if name and product.name != name:
                old_name = product.name
                product.name = name
                updated = True
                changes.append(f"Name: '{old_name}' → '{name}'")

            # Check URL change
            if product_url and product.product_url != product_url:
                product.product_url = product_url
                updated = True
                changes.append(f"URL updated")

            # Check category change
            if category and product.category != category:
                product.category = category
                updated = True
                changes.append(f"Category updated")

            if updated:
                product.last_updated = datetime.now()
                self.session.commit()
                print(f"📝 Updated product {sku}:")
                for change in changes:
                    print(f"   • {change}")

        return product

    def get_product_by_sku(self, sku: str, supplier_id: int) -> Optional[ProductInfo]:
        """Get product by SKU and supplier"""
        return self.session.query(ProductInfo).filter_by(
            sku=sku,
            supplier_id=supplier_id
        ).first()

    def get_all_products(self, supplier_name: str = None) -> List[ProductInfo]:
        """Get all products, optionally filtered by supplier"""
        query = self.session.query(ProductInfo)

        if supplier_name:
            query = query.join(Supplier).filter(Supplier.name == supplier_name)

        return query.all()

    def find_duplicate_products(self) -> List[Dict]:
        """
        Find potential duplicate products
        (same SKU with different IDs - shouldn't happen but check anyway)
        """
        # Find SKUs that appear more than once
        duplicates = self.session.query(
            ProductInfo.sku,
            func.count(ProductInfo.id).label('count')
        ).group_by(ProductInfo.sku).having(func.count(ProductInfo.id) > 1).all()

        results = []
        for sku, count in duplicates:
            products = self.session.query(ProductInfo).filter_by(sku=sku).all()
            results.append({
                'sku': sku,
                'count': count,
                'products': [
                    {
                        'id': p.id,
                        'name': p.name,
                        'created': p.created_at
                    }
                    for p in products
                ]
            })

        return results

    def merge_duplicate_products(self, keep_id: int, merge_id: int) -> bool:
        """
        Merge two duplicate products
        Move all price history from merge_id to keep_id
        """
        try:
            # Update all price records to point to keep_id
            self.session.query(PriceHistory).filter_by(
                product_id=merge_id
            ).update({'product_id': keep_id})

            # Delete the duplicate product
            product_to_delete = self.session.query(ProductInfo).get(merge_id)
            if product_to_delete:
                self.session.delete(product_to_delete)

            self.session.commit()
            print(f"✅ Merged product {merge_id} into {keep_id}")
            return True

        except Exception as e:
            print(f"❌ Error merging products: {e}")
            self.session.rollback()
            return False

    # === PRICE OPERATIONS ===

    def save_price(
            self,
            product_id: int,
            supplier_id: int,
            price: float,
            in_stock: bool = True,
            additional_info: dict = None
    ) -> PriceHistory:
        """Save a new price record"""
        price_record = PriceHistory(
            product_id=product_id,
            supplier_id=supplier_id,
            price=price,
            in_stock=in_stock,
            additional_info=additional_info or {}
        )

        self.session.add(price_record)
        self.session.commit()

        print(f"Saved price: Product #{product_id} = ${price}")
        return price_record

    def save_product_from_scraper(self, product_data) -> bool:
        """
        Save product data from scraper
        Uses SKU as primary identifier to prevent duplicates
        Expects product_data to be a Product object from scraper
        """
        try:
            # Get or create supplier
            supplier = self.get_or_create_supplier(
                name=product_data.supplier,
                website="https://www.bunnings.com.au"
            )

            # Use SKU-based matching (prevents duplicates!)
            product = self.get_or_create_product_by_sku(
                sku=product_data.sku,
                supplier_id=supplier.id,
                name=product_data.name,
                category=product_data.category,
                unit=product_data.unit,
                product_url=product_data.url
            )

            # Save price
            self.save_price(
                product_id=product.id,
                supplier_id=supplier.id,
                price=product_data.price,
                in_stock=product_data.in_stock,
                additional_info=product_data.additional_info
            )

            return True

        except Exception as e:
            print(f"Error saving product: {e}")
            self.session.rollback()
            return False

    def get_latest_price(self, product_id: int) -> Optional[PriceHistory]:
        """Get most recent price for a product"""
        return self.session.query(PriceHistory) \
            .filter_by(product_id=product_id) \
            .order_by(PriceHistory.scraped_at.desc()) \
            .first()

    def get_price_history(
            self,
            product_id: int,
            days: int = 30
    ) -> List[PriceHistory]:
        """Get price history for a product"""
        cutoff_date = datetime.now() - timedelta(days=days)

        return self.session.query(PriceHistory) \
            .filter(
            PriceHistory.product_id == product_id,
            PriceHistory.scraped_at >= cutoff_date
        ) \
            .order_by(PriceHistory.scraped_at.asc()) \
            .all()

    def get_price_changes(self, days: int = 7) -> List[dict]:
        """
        Get products with price changes in the last N days
        Returns list of dicts with product info and price change
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        results = []
        products = self.get_all_products()

        for product in products:
            prices = self.session.query(PriceHistory) \
                .filter(
                PriceHistory.product_id == product.id,
                PriceHistory.scraped_at >= cutoff_date
            ) \
                .order_by(PriceHistory.scraped_at.desc()) \
                .limit(2) \
                .all()

            if len(prices) >= 2:
                latest = prices[0]
                previous = prices[1]

                if latest.price != previous.price:
                    change_amount = latest.price - previous.price
                    change_percent = (change_amount / previous.price) * 100

                    results.append({
                        'product_id': product.id,
                        'sku': product.sku,
                        'name': product.name,
                        'previous_price': previous.price,
                        'current_price': latest.price,
                        'change_amount': change_amount,
                        'change_percent': change_percent,
                        'previous_date': previous.scraped_at,
                        'current_date': latest.scraped_at
                    })

        # Sort by absolute change amount (biggest changes first)
        results.sort(key=lambda x: abs(x['change_amount']), reverse=True)
        return results

    def get_price_comparison(self, product_name_pattern: str) -> List[dict]:
        """
        Get current prices for products matching name pattern
        Returns comparison across all suppliers
        """
        # Find products matching pattern
        products = self.session.query(ProductInfo) \
            .filter(ProductInfo.name.like(f'%{product_name_pattern}%')) \
            .all()

        results = []
        for product in products:
            latest_price = self.get_latest_price(product.id)

            if latest_price:
                results.append({
                    'product_name': product.name,
                    'sku': product.sku,
                    'supplier': product.supplier.name,
                    'price': latest_price.price,
                    'in_stock': latest_price.in_stock,
                    'url': product.product_url,
                    'scraped_at': latest_price.scraped_at
                })

        # Sort by price (cheapest first)
        results.sort(key=lambda x: x['price'])
        return results

    def get_statistics(self) -> dict:
        """Get database statistics"""
        return {
            'total_suppliers': self.session.query(Supplier).count(),
            'total_products': self.session.query(ProductInfo).count(),
            'total_price_records': self.session.query(PriceHistory).count(),
            'products_in_stock': self.session.query(ProductInfo) \
                .join(PriceHistory) \
                .filter(PriceHistory.in_stock == True) \
                .distinct() \
                .count()
        }


# Test the database
if __name__ == "__main__":
    print("Testing database manager...")

    db = DatabaseManager()

    # Create test data
    supplier = db.get_or_create_supplier("Bunnings", "https://bunnings.com.au")

    # Test 1: Create product
    product = db.get_or_create_product_by_sku(
        sku="0340162",
        supplier_id=supplier.id,
        name="Ecoply 2400x1200mm 9mm Plywood",
        category="Building Materials",
        unit="per sheet"
    )

    # Test 2: Save price
    db.save_price(
        product_id=product.id,
        supplier_id=supplier.id,
        price=45.98,
        in_stock=True
    )

    # Test 3: Update same product with new name (simulating product name change)
    print("\n--- Testing SKU-based matching (product name change) ---")
    product_updated = db.get_or_create_product_by_sku(
        sku="0340162",  # Same SKU
        supplier_id=supplier.id,
        name="Ecoply Plus 2400x1200mm 9mm Structural Plywood",  # New name
        category="Building Materials",
        unit="per sheet"
    )

    # Test 4: Save another price (should be same product ID)
    db.save_price(
        product_id=product_updated.id,
        supplier_id=supplier.id,
        price=47.50,
        in_stock=True
    )

    # Test 5: Check price history
    history = db.get_price_history(product.id, days=30)
    print(f"\n--- Price History for Product {product.id} ---")
    for record in history:
        print(f"  {record.scraped_at}: ${record.price}")

    # Test 6: Get statistics
    stats = db.get_statistics()
    print("\n--- Database Statistics ---")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    # Test 7: Check for duplicates
    duplicates = db.find_duplicate_products()
    print(f"\n--- Duplicate Check ---")
    print(f"  Duplicates found: {len(duplicates)}")

    db.close()
    print("\n✅ Database test complete!")