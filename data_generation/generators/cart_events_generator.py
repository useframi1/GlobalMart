"""
Cart Events Generator
Generates realistic shopping cart events (add, remove, update, checkout)
"""
import random
from datetime import datetime
from typing import Dict, List, Any
import uuid
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.constants import (
    MAX_ITEMS_PER_CART,
    CART_ABANDONMENT_RATE,
    EVENT_TYPES,
    CART_QUANTITY_CHOICES,
    CART_QUANTITY_UPDATE_CHOICES,
    CART_EVENT_WEIGHTS_FULL,
    CART_EVENT_WEIGHTS_NORMAL
)


class CartEventsGenerator:
    """Generates realistic cart events for e-commerce platform"""

    def __init__(self, users: List[Dict], products: List[Dict]):
        """
        Initialize cart events generator

        Args:
            users: List of user dictionaries
            products: List of product dictionaries
        """
        self.users = users
        self.products = products
        self.active_carts = {}  # Track active shopping carts per user

    def generate_cart_event(self) -> Dict[str, Any]:
        """
        Generate a realistic cart event

        Returns:
            Dictionary containing cart event data
        """
        user = random.choice(self.users)
        user_id = user["user_id"]

        # Get or create cart for user
        if user_id not in self.active_carts:
            self.active_carts[user_id] = {
                "items": [],
                "session_id": str(uuid.uuid4())
            }

        cart = self.active_carts[user_id]

        # Get cart event types from constants
        add_to_cart, remove_from_cart, update_quantity = EVENT_TYPES["CART"]
        checkout = "checkout"  # Checkout is a special event type

        # Determine event type based on cart state
        if len(cart["items"]) == 0:
            event_type = add_to_cart
        elif len(cart["items"]) >= MAX_ITEMS_PER_CART:  # Max items reached
            # Higher chance of checkout or remove when cart is full
            event_type = random.choices(
                [checkout, remove_from_cart, update_quantity],
                weights=[
                    CART_EVENT_WEIGHTS_FULL["checkout"],
                    CART_EVENT_WEIGHTS_FULL["remove_from_cart"],
                    CART_EVENT_WEIGHTS_FULL["update_quantity"]
                ]
            )[0]
        else:
            # Normal cart operations
            event_type = random.choices(
                [add_to_cart, remove_from_cart, update_quantity, checkout],
                weights=[
                    CART_EVENT_WEIGHTS_NORMAL["add_to_cart"],
                    CART_EVENT_WEIGHTS_NORMAL["remove_from_cart"],
                    CART_EVENT_WEIGHTS_NORMAL["update_quantity"],
                    CART_EVENT_WEIGHTS_NORMAL["checkout"]
                ]
            )[0]

        timestamp = datetime.utcnow().isoformat()
        country = user["country"]

        if event_type == "add_to_cart":
            return self._add_to_cart(user_id, cart, timestamp, country)
        elif event_type == "remove_from_cart":
            return self._remove_from_cart(user_id, cart, timestamp, country)
        elif event_type == "update_quantity":
            return self._update_quantity(user_id, cart, timestamp, country)
        else:  # checkout
            return self._checkout(user_id, cart, timestamp, country)

    def _add_to_cart(self, user_id: str, cart: Dict, timestamp: str, country: str) -> Dict[str, Any]:
        """Add item to cart"""
        product = random.choice(self.products)
        quantity = random.choice(CART_QUANTITY_CHOICES)  # Most people add 1 item

        # Check if product already in cart
        existing_item = next((item for item in cart["items"] if item["product_id"] == product["product_id"]), None)

        if existing_item:
            # Update quantity instead
            existing_item["quantity"] += quantity
        else:
            # Add new item
            cart["items"].append({
                "product_id": product["product_id"],
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
                "quantity": quantity
            })

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "add_to_cart",
            "timestamp": timestamp,
            "user_id": user_id,
            "session_id": cart["session_id"],
            "product_id": product["product_id"],
            "product_name": product["name"],
            "category": product["category"],
            "price": product["price"],
            "quantity": quantity,
            "cart_size": len(cart["items"]),
            "cart_value": sum(item["price"] * item["quantity"] for item in cart["items"]),
            "country": country
        }

    def _remove_from_cart(self, user_id: str, cart: Dict, timestamp: str, country: str) -> Dict[str, Any]:
        """Remove item from cart"""
        if not cart["items"]:
            # If cart is empty, generate add event instead
            return self._add_to_cart(user_id, cart, timestamp, country)

        item = random.choice(cart["items"])
        cart["items"].remove(item)

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "remove_from_cart",
            "timestamp": timestamp,
            "user_id": user_id,
            "session_id": cart["session_id"],
            "product_id": item["product_id"],
            "product_name": item["product_name"],
            "category": item["category"],
            "price": item["price"],
            "quantity": item["quantity"],
            "cart_size": len(cart["items"]),
            "cart_value": sum(i["price"] * i["quantity"] for i in cart["items"]),
            "country": country
        }

    def _update_quantity(self, user_id: str, cart: Dict, timestamp: str, country: str) -> Dict[str, Any]:
        """Update item quantity in cart"""
        if not cart["items"]:
            # If cart is empty, generate add event instead
            return self._add_to_cart(user_id, cart, timestamp, country)

        item = random.choice(cart["items"])
        old_quantity = item["quantity"]

        # Update quantity (can be increase or decrease)
        item["quantity"] = random.choice(CART_QUANTITY_UPDATE_CHOICES)

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "update_quantity",
            "timestamp": timestamp,
            "user_id": user_id,
            "session_id": cart["session_id"],
            "product_id": item["product_id"],
            "product_name": item["product_name"],
            "category": item["category"],
            "price": item["price"],
            "old_quantity": old_quantity,
            "new_quantity": item["quantity"],
            "cart_size": len(cart["items"]),
            "cart_value": sum(i["price"] * i["quantity"] for i in cart["items"]),
            "country": country
        }

    def _checkout(self, user_id: str, cart: Dict, timestamp: str, country: str) -> Dict[str, Any]:
        """Checkout cart (complete purchase)"""
        if not cart["items"]:
            # If cart is empty, generate add event instead
            return self._add_to_cart(user_id, cart, timestamp, country)

        total_value = sum(item["price"] * item["quantity"] for item in cart["items"])
        total_items = sum(item["quantity"] for item in cart["items"])

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "checkout",
            "timestamp": timestamp,
            "user_id": user_id,
            "session_id": cart["session_id"],
            "items": cart["items"].copy(),
            "total_items": total_items,
            "total_value": total_value,
            "cart_size": len(cart["items"]),
            "country": country
        }

        # Clear cart after checkout
        cart["items"] = []
        cart["session_id"] = str(uuid.uuid4())  # New session

        return event

    def generate_cart_abandonment(self) -> Dict[str, Any]:
        """
        Generate a cart abandonment event (user leaves without checkout)
        """
        # Pick a user with active cart
        if not self.active_carts:
            return self.generate_cart_event()

        user_id = random.choice(list(self.active_carts.keys()))
        cart = self.active_carts[user_id]

        if not cart["items"]:
            return self.generate_cart_event()

        user = next((u for u in self.users if u["user_id"] == user_id), None)
        if not user:
            return self.generate_cart_event()

        timestamp = datetime.utcnow().isoformat()
        cart_value = sum(item["price"] * item["quantity"] for item in cart["items"])

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "cart_abandonment",
            "timestamp": timestamp,
            "user_id": user_id,
            "session_id": cart["session_id"],
            "items": cart["items"].copy(),
            "cart_size": len(cart["items"]),
            "cart_value": cart_value,
            "country": user["country"]
        }

        # Clear abandoned cart
        cart["items"] = []
        cart["session_id"] = str(uuid.uuid4())

        return event
