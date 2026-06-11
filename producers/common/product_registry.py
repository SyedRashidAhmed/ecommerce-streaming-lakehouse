# producers/common/product_registry.py

PRODUCTS = []

# ==========================================================
# CLOTHING
# ==========================================================

for i in range(1, 12):

    PRODUCTS.append(
        {
            "product_id": f"prod_{i}",
            "product_name": f"Zara Shirt {i}",
            "category": "Clothing",
            "subcategory": "Shirts",
            "brand": "Zara",
            "unit_price": 49.99
        }
    )

for i in range(12, 23):

    PRODUCTS.append(
        {
            "product_id": f"prod_{i}",
            "product_name": f"Levis Jeans {i}",
            "category": "Clothing",
            "subcategory": "Jeans",
            "brand": "Levis",
            "unit_price": 79.99
        }
    )

for i in range(23, 34):

    PRODUCTS.append(
        {
            "product_id": f"prod_{i}",
            "product_name": f"North Face Jacket {i}",
            "category": "Clothing",
            "subcategory": "Jackets",
            "brand": "North Face",
            "unit_price": 149.99
        }
    )

# ==========================================================
# ELECTRONICS
# ==========================================================

for i in range(34, 46):

    PRODUCTS.append(
        {
            "product_id": f"prod_{i}",
            "product_name": f"Sony Headphones {i}",
            "category": "Electronics",
            "subcategory": "Headphones",
            "brand": "Sony",
            "unit_price": 199.99
        }
    )

for i in range(46, 57):

    PRODUCTS.append(
        {
            "product_id": f"prod_{i}",
            "product_name": f"Apple iPhone {i}",
            "category": "Electronics",
            "subcategory": "Smartphones",
            "brand": "Apple",
            "unit_price": 999.99
        }
    )

for i in range(57, 68):

    PRODUCTS.append(
        {
            "product_id": f"prod_{i}",
            "product_name": f"Dell Laptop {i}",
            "category": "Electronics",
            "subcategory": "Laptops",
            "brand": "Dell",
            "unit_price": 1299.99
        }
    )

# ==========================================================
# SPORTS
# ==========================================================

for i in range(68, 79):

    PRODUCTS.append(
        {
            "product_id": f"prod_{i}",
            "product_name": f"Nike Football {i}",
            "category": "Sports",
            "subcategory": "Football",
            "brand": "Nike",
            "unit_price": 39.99
        }
    )

for i in range(79, 90):

    PRODUCTS.append(
        {
            "product_id": f"prod_{i}",
            "product_name": f"Yonex Badminton Racket {i}",
            "category": "Sports",
            "subcategory": "Badminton",
            "brand": "Yonex",
            "unit_price": 89.99
        }
    )

for i in range(90, 101):

    PRODUCTS.append(
        {
            "product_id": f"prod_{i}",
            "product_name": f"Adidas Running Shoes {i}",
            "category": "Sports",
            "subcategory": "Running",
            "brand": "Adidas",
            "unit_price": 119.99
        }
    )