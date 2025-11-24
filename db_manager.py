"""
Database models for storing price data
Uses SQLAlchemy ORM for database operations
"""

import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, JSON, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from typing import List, Optional

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
    additional_info = Column(JSON)  # Store specs, brand, description

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
    """Manage database operations"""

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

    # === PRODUCT OPERATIONS ===

    def get_or_create_product(
            self,
            sku: str,
            name: str,
            supplier_id: int,
            category: str = None,
            unit: str = "each",
            product_url: str = None
    ) -> ProductInfo:
        """Get existing product or create new one"""
        product = self.session.query(ProductInfo).filter_by(
            sku=sku,
            supplier_id=supplier_id
        ).first()

        if not product:
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
            print(f"Created product: {name} (SKU: {sku})")
        else:
            # Update product info if it changed
            updated = False
            if product.name != name:
                product.name = name
                updated = True
            if category and product.category != category:
                product.category = category
                updated = True
            if product_url and product.product_url != product_url:
                product.product_url = product_url
                updated = True

            if updated:
                product.last_updated = datetime.now()
                self.session.commit()

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
        Expects product_data to be a Product object from scraper
        """
        try:
            # Get or create supplier
            supplier = self.get_or_create_supplier(
                name=product_data.supplier,
                website="https://www.bunnings.com.au"
            )

            # Get or create product
            product = self.get_or_create_product(
                sku=product_data.sku,
                name=product_data.name,
                supplier_id=supplier.id,
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
        from datetime import timedelta
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
        from datetime import timedelta
        from sqlalchemy import func

        cutoff_date = datetime.now() - timedelta(days=days)

        # This is a complex query - get latest 2 prices for each product
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
        from sqlalchemy import func, and_

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
            'products_in_stock': self.session.query(ProductInfo).join(PriceHistory) \
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

    product = db.get_or_create_product(
        sku="0340162",
        name="Ecoply 2400x1200mm 9mm Plywood",
        supplier_id=supplier.id,
        category="Building Materials",
        unit="per sheet"
    )

    db.save_price(
        product_id=product.id,
        supplier_id=supplier.id,
        price=45.98,
        in_stock=True
    )

    # Get statistics
    stats = db.get_statistics()
    print("\nDatabase Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    db.close()
    print("\n✅ Database test complete!")