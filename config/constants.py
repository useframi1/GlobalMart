"""
Business constants for GlobalMart.
Defines fixed business data like countries, categories, payment methods, etc.
"""

# Countries where GlobalMart operates
COUNTRIES = ["USA", "UK", "Germany", "France", "Japan"]

# Product categories (100 categories as per requirements)
CATEGORIES = [
    # Electronics (20)
    "Smartphones", "Laptops", "Tablets", "Desktop Computers", "Monitors",
    "Keyboards", "Mice", "Headphones", "Speakers", "Cameras",
    "Smart Watches", "Fitness Trackers", "Gaming Consoles", "VR Headsets", "Drones",
    "Printers", "Scanners", "External Hard Drives", "USB Drives", "Webcams",

    # Clothing & Fashion (20)
    "Men's T-Shirts", "Men's Jeans", "Men's Jackets", "Men's Shoes", "Men's Accessories",
    "Women's Dresses", "Women's Tops", "Women's Jeans", "Women's Shoes", "Women's Accessories",
    "Kids' Clothing", "Baby Clothing", "Sportswear", "Swimwear", "Underwear",
    "Socks", "Belts", "Hats", "Scarves", "Gloves",

    # Home & Kitchen (20)
    "Furniture", "Bedding", "Curtains", "Rugs", "Lighting",
    "Kitchen Appliances", "Cookware", "Dinnerware", "Cutlery", "Glassware",
    "Storage & Organization", "Cleaning Supplies", "Home Decor", "Wall Art", "Mirrors",
    "Bathroom Accessories", "Towels", "Pillows", "Blankets", "Mattresses",

    # Books & Media (10)
    "Fiction Books", "Non-Fiction Books", "E-Books", "Audiobooks", "Magazines",
    "Movies", "Music CDs", "Vinyl Records", "Video Games", "Board Games",

    # Sports & Outdoors (10)
    "Exercise Equipment", "Yoga Mats", "Dumbbells", "Treadmills", "Bicycles",
    "Camping Gear", "Hiking Equipment", "Fishing Gear", "Sports Balls", "Athletic Shoes",

    # Beauty & Personal Care (10)
    "Skincare", "Makeup", "Hair Care", "Fragrances", "Nail Care",
    "Bath & Body", "Shaving Products", "Oral Care", "Personal Hygiene", "Beauty Tools",

    # Toys & Games (10)
    "Action Figures", "Dolls", "Building Blocks", "Puzzles", "Educational Toys",
    "Remote Control Toys", "Stuffed Animals", "Arts & Crafts", "Musical Toys", "Outdoor Toys"
]

# Ensure we have exactly 100 categories
assert len(CATEGORIES) == 100, f"Expected 100 categories, got {len(CATEGORIES)}"

# Payment methods
PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "PayPal",
    "Apple Pay",
    "Google Pay"
]

# Event types for cart and product view events
EVENT_TYPES = {
    "CART": ["add_to_cart", "remove_from_cart", "update_quantity"],
    "PRODUCT": ["view", "search", "filter", "compare"]
}

# User age ranges for demographic distribution
AGE_RANGES = {
    "18-24": 0.15,
    "25-34": 0.30,
    "35-44": 0.25,
    "45-54": 0.20,
    "55+": 0.10
}

# Transaction amount ranges (in USD)
TRANSACTION_AMOUNT_RANGES = {
    "small": (10, 50),      # Small purchases
    "medium": (50, 200),    # Medium purchases
    "large": (200, 500),    # Large purchases
    "very_large": (500, 2000)  # Very large purchases
}

# Product price ranges by category type (simplified)
PRICE_RANGES = {
    "electronics": (50, 2000),
    "clothing": (10, 200),
    "home": (20, 1000),
    "books": (5, 50),
    "sports": (15, 500),
    "beauty": (5, 150),
    "toys": (10, 200)
}

# Inventory levels ranges
INVENTORY_RANGES = {
    "low": (0, 20),
    "medium": (20, 100),
    "high": (100, 1000)
}

# Shopping behavior patterns
SHOPPING_HOURS = {
    "peak": [10, 11, 12, 13, 14, 19, 20, 21],  # Peak shopping hours
    "normal": [8, 9, 15, 16, 17, 18, 22],      # Normal hours
    "low": [0, 1, 2, 3, 4, 5, 6, 7, 23]        # Low traffic hours
}

# User preference distribution (% of users interested in each category type)
USER_PREFERENCES = [
    "Electronics", "Clothing & Fashion", "Home & Kitchen",
    "Books & Media", "Sports & Outdoors", "Beauty & Personal Care",
    "Toys & Games"
]

# Product ratings
RATING_RANGE = (1.0, 5.0)

# Session behavior constants
MAX_PRODUCTS_PER_SESSION = 10
MAX_ITEMS_PER_CART = 5
CART_ABANDONMENT_RATE = 0.7  # 70% of carts are abandoned

# Browsing behavior probabilities
CATEGORY_AFFINITY_PROBABILITY = 0.7  # Stay in same category
USER_PREFERENCE_PROBABILITY = 0.6  # Browse preferred categories
CATEGORY_COMPARE_PROBABILITY = 0.8  # Compare within same category

# View duration range (seconds)
VIEW_DURATION_RANGE = (5, 120)

# Cart and compare ranges
COMPARE_PRODUCTS_RANGE = (2, 4)

# Cart quantity distribution (weighted towards single items)
CART_QUANTITY_CHOICES = [1, 1, 1, 2, 2, 3]
CART_QUANTITY_UPDATE_CHOICES = [1, 2, 3, 4, 5]

# Cart event type weights based on cart state
CART_EVENT_WEIGHTS_FULL = {
    "checkout": 0.4,
    "remove_from_cart": 0.4,
    "update_quantity": 0.2
}
CART_EVENT_WEIGHTS_NORMAL = {
    "add_to_cart": 0.5,
    "remove_from_cart": 0.2,
    "update_quantity": 0.2,
    "checkout": 0.1
}

# Product view event type weights
PRODUCT_VIEW_EVENT_WEIGHTS = {
    "view": 0.6,
    "search": 0.2,
    "filter": 0.15,
    "compare": 0.05
}

# Generic search terms for product searches
GENERIC_SEARCH_TERMS = [
    "laptop", "phone", "shoes", "dress", "table", "book",
    "headphones", "watch", "camera", "toy", "game", "shirt",
    "kitchen", "furniture", "electronics", "clothing"
]

# Search types for product search events
SEARCH_TYPES = ["category", "product_name", "generic"]

# Filter types for product filter events
FILTER_TYPES = ["category", "price_range", "both"]

# Filter price ranges for product filtering
FILTER_PRICE_RANGES = [
    (0, 50), (50, 100), (100, 200), (200, 500), (500, 1000), (1000, 2000)
]

# Transaction generation patterns
TRANSACTION_ITEMS_CHOICES = [1, 2, 3, 4, 5]
TRANSACTION_ITEMS_WEIGHTS = [0.5, 0.3, 0.15, 0.04, 0.01]

TRANSACTION_QUANTITY_CHOICES = [1, 2, 3]
TRANSACTION_QUANTITY_WEIGHTS = [0.8, 0.15, 0.05]

# Anomaly patterns for testing
ANOMALY_PATTERNS = {
    "high_value": 3000,  # Transactions above this are anomalies
    "rapid_purchase": 10,  # More than 10 purchases in a minute
    "unusual_quantity": 50  # More than 50 items in one transaction
}

ANOMALY_TYPES = ["high_value", "high_quantity"]
ANOMALY_VALUE_RANGE = (3000, 10000)
ANOMALY_QUANTITY_RANGE = (50, 100)
ANOMALY_PROBABILITY = 0.02  # 2% chance of anomaly

# Event type distribution for streaming
EVENT_DISTRIBUTION_TYPES = ["product_view", "cart_event", "transaction"]
EVENT_DISTRIBUTION_WEIGHTS = [0.6, 0.3, 0.1]  # 60% views, 30% cart, 10% transactions
