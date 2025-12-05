"""
Product Catalog Generator for GlobalMart
Generates realistic product catalog with categories, prices, and inventory
"""
import random
import json
from faker import Faker
from typing import List, Dict
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from config.constants import CATEGORIES, PRICE_RANGES, INVENTORY_RANGES, RATING_RANGE


class ProductGenerator:
    """Generates realistic product catalog"""

    def __init__(self, num_products: int = None):
        self.faker = Faker()
        self.num_products = num_products or settings.data_generation.num_products
        self.generated_products = []

    def get_category_type(self, category: str) -> str:
        """Map category to price range type"""
        category_lower = category.lower()

        if any(word in category_lower for word in ['laptop', 'computer', 'phone', 'tablet', 'camera', 'console', 'drone', 'watch']):
            return 'electronics'
        elif any(word in category_lower for word in ['shirt', 'jean', 'jacket', 'shoe', 'dress', 'clothing']):
            return 'clothing'
        elif any(word in category_lower for word in ['furniture', 'bedding', 'kitchen', 'appliance']):
            return 'home'
        elif any(word in category_lower for word in ['book', 'movie', 'music', 'game']):
            return 'books'
        elif any(word in category_lower for word in ['exercise', 'sport', 'yoga', 'bicycle', 'camping']):
            return 'sports'
        elif any(word in category_lower for word in ['skincare', 'makeup', 'beauty', 'fragrance']):
            return 'beauty'
        else:
            return 'toys'

    def generate_price(self, category: str) -> float:
        """Generate price based on category"""
        category_type = self.get_category_type(category)
        min_price, max_price = PRICE_RANGES.get(category_type, (10, 100))

        # Use weighted random to favor more common price ranges
        if random.random() < 0.7:  # 70% in lower-mid range
            price = random.uniform(min_price, (min_price + max_price) / 2)
        else:  # 30% in upper range
            price = random.uniform((min_price + max_price) / 2, max_price)

        return round(price, 2)

    def generate_inventory(self) -> int:
        """Generate inventory level"""
        # Distribution: 10% low, 50% medium, 40% high
        inventory_type = random.choices(
            ['low', 'medium', 'high'],
            weights=[0.1, 0.5, 0.4],
            k=1
        )[0]

        min_inv, max_inv = INVENTORY_RANGES[inventory_type]
        return random.randint(min_inv, max_inv)

    def generate_rating(self) -> float:
        """Generate product rating (1.0 to 5.0)"""
        # Weighted toward higher ratings (most products are decent)
        if random.random() < 0.6:  # 60% are 4.0-5.0
            rating = random.uniform(4.0, 5.0)
        elif random.random() < 0.8:  # 32% are 3.0-4.0
            rating = random.uniform(3.0, 4.0)
        else:  # 8% are below 3.0
            rating = random.uniform(1.0, 3.0)

        return round(rating, 1)

    def generate_product_name(self, category: str) -> str:
        """Generate realistic product name based on category"""
        category_lower = category.lower()

        # Electronics names
        if 'laptop' in category_lower or 'computer' in category_lower:
            brands = ['TechPro', 'MaxBook', 'UltraComp', 'ProBook']
            models = ['X1', 'Pro 15', 'Ultra', 'Elite', 'Slim']
            return f"{random.choice(brands)} {random.choice(models)}"

        elif 'phone' in category_lower:
            brands = ['PhoneX', 'SmartMax', 'ProPhone']
            models = ['12', '13', '14', 'Plus', 'Pro', 'Max']
            return f"{random.choice(brands)} {random.choice(models)}"

        # Clothing names
        elif any(word in category_lower for word in ['shirt', 'jean', 'jacket', 'dress']):
            styles = ['Classic', 'Modern', 'Vintage', 'Casual', 'Formal', 'Sport']
            item = category.split()[-1]  # Get last word (T-Shirts -> Shirts)
            return f"{random.choice(styles)} {item}"

        # Home & Kitchen
        elif 'kitchen' in category_lower or 'cookware' in category_lower:
            brands = ['ChefMaster', 'HomeChef', 'ProCook']
            return f"{random.choice(brands)} {category}"

        # Generic name
        else:
            adjectives = ['Premium', 'Deluxe', 'Professional', 'Standard', 'Quality']
            return f"{random.choice(adjectives)} {category}"

    def generate_product(self, product_id: int) -> Dict:
        """Generate a single product"""
        category = random.choice(CATEGORIES)

        product = {
            "product_id": f"PROD_{product_id:08d}",
            "name": self.generate_product_name(category),
            "category": category,
            "price": self.generate_price(category),
            "inventory": self.generate_inventory(),
            "ratings": self.generate_rating()
        }
        return product

    def generate_batch(self, batch_size: int = 1000) -> List[Dict]:
        """Generate a batch of products"""
        products = []
        for i in range(batch_size):
            products.append(self.generate_product(len(self.generated_products) + i + 1))
        return products

    def generate_all(self) -> List[Dict]:
        """Generate all products"""
        print(f"Generating {self.num_products} products...")

        batch_size = 10000
        all_products = []

        for batch_num in range(0, self.num_products, batch_size):
            current_batch_size = min(batch_size, self.num_products - batch_num)
            batch = self.generate_batch(current_batch_size)
            all_products.extend(batch)

            if (batch_num + current_batch_size) % 10000 == 0:
                print(f"Generated {batch_num + current_batch_size}/{self.num_products} products...")

        self.generated_products = all_products
        print(f"✓ Generated {len(all_products)} products")
        return all_products

    def save_to_file(self, filepath: str = "data/raw/products.json"):
        """Save generated products to JSON file"""
        if not self.generated_products:
            self.generate_all()

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, 'w') as f:
            json.dump(self.generated_products, f, indent=2)

        print(f"✓ Saved {len(self.generated_products)} products to {filepath}")

    def load_from_file(self, filepath: str = "data/raw/products.json") -> List[Dict]:
        """Load products from JSON file"""
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                self.generated_products = json.load(f)
            print(f"✓ Loaded {len(self.generated_products)} products from {filepath}")
            return self.generated_products
        else:
            print(f"✗ File not found: {filepath}")
            return []

    def get_random_product(self) -> Dict:
        """Get a random product from generated products"""
        if not self.generated_products:
            self.load_from_file()

        if not self.generated_products:
            raise ValueError("No products available. Generate products first.")

        return random.choice(self.generated_products)

    def get_products_by_category(self, category: str) -> List[Dict]:
        """Get all products in a specific category"""
        return [p for p in self.generated_products if p['category'] == category]


if __name__ == "__main__":
    # Test the generator
    generator = ProductGenerator(num_products=10)
    products = generator.generate_all()

    print("\nSample products:")
    for product in products[:5]:
        print(json.dumps(product, indent=2))

    generator.save_to_file()
