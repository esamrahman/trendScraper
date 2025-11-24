from db_manager import DatabaseManager

db = DatabaseManager()

# Get all products
print("\n" + "=" * 60)
print("YOUR TRACKED PRODUCTS")
print("=" * 60)

products = db.get_all_products()

for product in products:
    latest_price = db.get_latest_price(product.id)

    print(f"\n{product.name}")
    print(f"  SKU: {product.sku}")
    print(f"  Category: {product.category}")
    if latest_price:
        print(f"  Price: ${latest_price.price:.2f}")
        print(f"  In Stock: {'Yes' if latest_price.in_stock else 'No'}")
        print(f"  Last Checked: {latest_price.scraped_at.strftime('%Y-%m-%d %H:%M')}")

print("\n" + "=" * 60)

# Get statistics
stats = db.get_statistics()
print("\nDatabase Stats:")
print(f"  Total Products: {stats['total_products']}")
print(f"  Total Price Records: {stats['total_price_records']}")

db.close()