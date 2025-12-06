"""
Transaction Generator for GlobalMart
Generates realistic e-commerce transactions
"""
import random
import json
from datetime import datetime, timedelta
from typing import List, Dict
import sys
import os
import uuid

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from config.constants import (
    PAYMENT_METHODS,
    SHOPPING_HOURS,
    TRANSACTION_ITEMS_CHOICES,
    TRANSACTION_ITEMS_WEIGHTS,
    TRANSACTION_QUANTITY_CHOICES,
    TRANSACTION_QUANTITY_WEIGHTS,
    ANOMALY_TYPES,
    ANOMALY_VALUE_RANGE,
    ANOMALY_QUANTITY_RANGE,
    ANOMALY_PROBABILITY
)
from data_generation.generators.user_generator import UserGenerator
from data_generation.generators.product_generator import ProductGenerator


class TransactionGenerator:
    """Generates realistic e-commerce transactions"""

    def __init__(self, user_generator: UserGenerator = None, product_generator: ProductGenerator = None):
        self.user_generator = user_generator or UserGenerator()
        self.product_generator = product_generator or ProductGenerator()

        # Load or generate users and products
        if not self.user_generator.generated_users:
            self.user_generator.load_from_file()
            if not self.user_generator.generated_users:
                print("Generating users...")
                self.user_generator.generate_all()
                self.user_generator.save_to_file()

        if not self.product_generator.generated_products:
            self.product_generator.load_from_file()
            if not self.product_generator.generated_products:
                print("Generating products...")
                self.product_generator.generate_all()
                self.product_generator.save_to_file()

    def get_current_hour_type(self, hour: int = None) -> str:
        """Determine if current hour is peak, normal, or low traffic"""
        if hour is None:
            hour = datetime.utcnow().hour

        if hour in SHOPPING_HOURS['peak']:
            return 'peak'
        elif hour in SHOPPING_HOURS['normal']:
            return 'normal'
        else:
            return 'low'

    def generate_products_for_transaction(self) -> List[Dict]:
        """Generate products for a transaction (1-5 items)"""
        # Most transactions have 1-2 items, fewer have more
        num_items = random.choices(TRANSACTION_ITEMS_CHOICES, weights=TRANSACTION_ITEMS_WEIGHTS, k=1)[0]

        transaction_products = []
        selected_products = random.sample(self.product_generator.generated_products, num_items)

        for product in selected_products:
            # Quantity usually 1, sometimes 2-3
            quantity = random.choices(TRANSACTION_QUANTITY_CHOICES, weights=TRANSACTION_QUANTITY_WEIGHTS, k=1)[0]

            transaction_products.append({
                "product_id": product['product_id'],
                "quantity": quantity,
                "price": product['price']
            })

        return transaction_products

    def calculate_total_amount(self, products: List[Dict]) -> float:
        """Calculate total transaction amount"""
        total = sum(p['quantity'] * p['price'] for p in products)
        return round(total, 2)

    def generate_transaction(self, transaction_id: str = None, timestamp: datetime = None) -> Dict:
        """Generate a single transaction"""
        if transaction_id is None:
            transaction_id = str(uuid.uuid4())

        if timestamp is None:
            timestamp = datetime.utcnow()

        # Select random user
        user = self.user_generator.get_random_user()

        # Generate products for transaction
        products = self.generate_products_for_transaction()

        # Calculate total
        total_amount = self.calculate_total_amount(products)

        transaction = {
            "transaction_id": transaction_id,
            "user_id": user['user_id'],
            "timestamp": timestamp.isoformat(),
            "products": products,
            "total_amount": total_amount,
            "payment_method": random.choice(PAYMENT_METHODS),
            "country": user['country']
        }

        return transaction

    def generate_anomalous_transaction(self) -> Dict:
        """Generate an anomalous transaction for testing anomaly detection"""
        transaction = self.generate_transaction()

        # Make it anomalous in some way
        anomaly_type = random.choice(ANOMALY_TYPES)

        if anomaly_type == 'high_value':
            # Very high value transaction
            transaction['total_amount'] = random.uniform(*ANOMALY_VALUE_RANGE)

        elif anomaly_type == 'high_quantity':
            # Unusual quantity
            for product in transaction['products']:
                product['quantity'] = random.randint(*ANOMALY_QUANTITY_RANGE)
            transaction['total_amount'] = self.calculate_total_amount(transaction['products'])

        return transaction

    def generate_batch(self, count: int = 10, include_anomalies: bool = False) -> List[Dict]:
        """Generate a batch of transactions"""
        transactions = []

        for i in range(count):
            # Use anomaly probability from constants if enabled
            if include_anomalies and random.random() < ANOMALY_PROBABILITY:
                transaction = self.generate_anomalous_transaction()
            else:
                transaction = self.generate_transaction()

            transactions.append(transaction)

        return transactions


if __name__ == "__main__":
    # Test the generator
    print("Testing Transaction Generator...")

    generator = TransactionGenerator()

    # Generate sample transactions
    transactions = generator.generate_batch(count=5, include_anomalies=True)

    print("\nSample transactions:")
    for transaction in transactions:
        print(json.dumps(transaction, indent=2))
        print()
