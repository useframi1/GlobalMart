"""
Product View Events Generator
Generates realistic product browsing events (view, search, filter, compare)
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
    MAX_PRODUCTS_PER_SESSION,
    EVENT_TYPES,
    CATEGORY_AFFINITY_PROBABILITY,
    USER_PREFERENCE_PROBABILITY,
    CATEGORY_COMPARE_PROBABILITY,
    VIEW_DURATION_RANGE,
    COMPARE_PRODUCTS_RANGE,
    PRODUCT_VIEW_EVENT_WEIGHTS,
    GENERIC_SEARCH_TERMS,
    SEARCH_TYPES,
    FILTER_TYPES,
    FILTER_PRICE_RANGES
)


class ProductViewGenerator:
    """Generates realistic product view and browsing events"""

    def __init__(self, users: List[Dict], products: List[Dict]):
        """
        Initialize product view generator

        Args:
            users: List of user dictionaries
            products: List of product dictionaries
        """
        self.users = users
        self.products = products
        self.user_sessions = {}  # Track user browsing sessions
        self.product_index = self._build_product_index()

    def _build_product_index(self) -> Dict[str, List[Dict]]:
        """Build index of products by category for efficient filtering"""
        index = {}
        for product in self.products:
            category = product["category"]
            if category not in index:
                index[category] = []
            index[category].append(product)
        return index

    def generate_product_view_event(self) -> Dict[str, Any]:
        """
        Generate a realistic product view event

        Returns:
            Dictionary containing product view event data
        """
        user = random.choice(self.users)
        user_id = user["user_id"]

        # Get or create session for user
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = {
                "session_id": str(uuid.uuid4()),
                "viewed_products": [],
                "current_category": None,
                "search_query": None
            }

        session = self.user_sessions[user_id]

        # Determine event type using constants
        product_event_types = EVENT_TYPES["PRODUCT"]
        event_type = random.choices(
            product_event_types,
            weights=[
                PRODUCT_VIEW_EVENT_WEIGHTS["view"],
                PRODUCT_VIEW_EVENT_WEIGHTS["search"],
                PRODUCT_VIEW_EVENT_WEIGHTS["filter"],
                PRODUCT_VIEW_EVENT_WEIGHTS["compare"]
            ]
        )[0]

        timestamp = datetime.utcnow().isoformat()
        country = user["country"]

        if event_type == "view":
            return self._product_view(user_id, user, session, timestamp, country)
        elif event_type == "search":
            return self._product_search(user_id, user, session, timestamp, country)
        elif event_type == "filter":
            return self._product_filter(user_id, user, session, timestamp, country)
        else:  # compare
            return self._product_compare(user_id, user, session, timestamp, country)

    def _product_view(self, user_id: str, user: Dict, session: Dict, timestamp: str, country: str) -> Dict[str, Any]:
        """Generate a product view event"""
        # If user has a current category, prefer products from that category (browsing behavior)
        if session["current_category"] and random.random() < CATEGORY_AFFINITY_PROBABILITY:
            products_in_category = self.product_index.get(session["current_category"], [])
            if products_in_category:
                product = random.choice(products_in_category)
            else:
                product = random.choice(self.products)
        else:
            # Browse based on user preferences
            preferred_categories = user.get("preferences", [])
            if preferred_categories and random.random() < USER_PREFERENCE_PROBABILITY:
                category = random.choice(preferred_categories)
                products_in_category = self.product_index.get(category, [])
                if products_in_category:
                    product = random.choice(products_in_category)
                else:
                    product = random.choice(self.products)
            else:
                product = random.choice(self.products)

        # Update session
        session["current_category"] = product["category"]
        session["viewed_products"].append(product["product_id"])

        # Calculate view duration (seconds)
        view_duration = random.randint(*VIEW_DURATION_RANGE)

        # Determine if user came from search
        from_search = session["search_query"] is not None

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "view",
            "timestamp": timestamp,
            "user_id": user_id,
            "session_id": session["session_id"],
            "product_id": product["product_id"],
            "product_name": product["name"],
            "category": product["category"],
            "price": product["price"],
            "view_duration": view_duration,
            "from_search": from_search,
            "search_query": session["search_query"] if from_search else None,
            "previously_viewed_count": len(session["viewed_products"]) - 1,
            "country": country
        }

    def _product_search(self, user_id: str, user: Dict, session: Dict, timestamp: str, country: str) -> Dict[str, Any]:
        """Generate a product search event"""
        # Generate realistic search query
        search_type = random.choice(SEARCH_TYPES)

        if search_type == "category":
            search_query = random.choice(list(self.product_index.keys()))
        elif search_type == "product_name":
            product = random.choice(self.products)
            # Take part of product name
            words = product["name"].split()
            search_query = " ".join(words[:random.randint(1, len(words))])
        else:  # generic
            search_query = random.choice(GENERIC_SEARCH_TERMS)

        # Update session
        session["search_query"] = search_query

        # Get matching products
        matching_products = [
            p for p in self.products
            if search_query.lower() in p["name"].lower() or
            search_query.lower() in p["category"].lower()
        ]

        results_count = len(matching_products)

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "search",
            "timestamp": timestamp,
            "user_id": user_id,
            "session_id": session["session_id"],
            "search_query": search_query,
            "results_count": results_count,
            "country": country
        }

    def _product_filter(self, user_id: str, user: Dict, session: Dict, timestamp: str, country: str) -> Dict[str, Any]:
        """Generate a product filter event"""
        # Filter by category or price range
        filter_type = random.choice(FILTER_TYPES)

        filter_category = None
        min_price = None
        max_price = None

        if filter_type in ["category", "both"]:
            # Use current category or user preference
            if session["current_category"] and random.random() < CATEGORY_AFFINITY_PROBABILITY:
                filter_category = session["current_category"]
            else:
                filter_category = random.choice(list(self.product_index.keys()))

        if filter_type in ["price_range", "both"]:
            # Generate price range
            min_price, max_price = random.choice(FILTER_PRICE_RANGES)

        # Count matching products
        matching_products = self.products
        if filter_category:
            matching_products = [p for p in matching_products if p["category"] == filter_category]
        if min_price is not None:
            matching_products = [p for p in matching_products if min_price <= p["price"] <= max_price]

        results_count = len(matching_products)

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "filter",
            "timestamp": timestamp,
            "user_id": user_id,
            "session_id": session["session_id"],
            "filter_category": filter_category,
            "min_price": min_price,
            "max_price": max_price,
            "results_count": results_count,
            "country": country
        }

    def _product_compare(self, user_id: str, user: Dict, session: Dict, timestamp: str, country: str) -> Dict[str, Any]:
        """Generate a product comparison event"""
        # Pick 2-4 products to compare (usually from same category)
        num_products = random.randint(*COMPARE_PRODUCTS_RANGE)

        if session["current_category"] and random.random() < CATEGORY_COMPARE_PROBABILITY:
            # Compare within same category
            products_in_category = self.product_index.get(session["current_category"], [])
            if len(products_in_category) >= num_products:
                compared_products = random.sample(products_in_category, num_products)
            else:
                compared_products = random.sample(self.products, min(num_products, len(self.products)))
        else:
            compared_products = random.sample(self.products, min(num_products, len(self.products)))

        compared_product_ids = [p["product_id"] for p in compared_products]
        compared_product_names = [p["name"] for p in compared_products]
        compared_prices = [p["price"] for p in compared_products]

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "compare",
            "timestamp": timestamp,
            "user_id": user_id,
            "session_id": session["session_id"],
            "product_ids": compared_product_ids,
            "product_names": compared_product_names,
            "prices": compared_prices,
            "category": compared_products[0]["category"] if len(set(p["category"] for p in compared_products)) == 1 else "mixed",
            "country": country
        }

    def end_session(self, user_id: str) -> None:
        """End a user's browsing session"""
        if user_id in self.user_sessions:
            del self.user_sessions[user_id]
