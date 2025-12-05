"""
Event Schema Definitions
Defines validation schemas for all event types
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.constants import (
    COUNTRIES,
    PAYMENT_METHODS,
    CATEGORIES,
    EVENT_TYPES
)


# Transaction Event Schema
TRANSACTION_SCHEMA = {
    "type": "object",
    "required": ["transaction_id", "user_id", "timestamp", "products", "total_amount", "payment_method", "country"],
    "properties": {
        "transaction_id": {
            "type": "string",
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"  # UUID format
        },
        "user_id": {
            "type": "string",
            "pattern": "^USER_[0-9]+$"
        },
        "timestamp": {
            "type": "string",
            "format": "date-time"
        },
        "products": {
            "type": "array",
            "minItems": 1,
            "maxItems": 5,
            "items": {
                "type": "object",
                "required": ["product_id", "quantity", "price"],
                "properties": {
                    "product_id": {
                        "type": "string",
                        "pattern": "^PROD_[0-9]+$"
                    },
                    "quantity": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100
                    },
                    "price": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 10000
                    }
                }
            }
        },
        "total_amount": {
            "type": "number",
            "minimum": 0,
            "maximum": 50000
        },
        "payment_method": {
            "type": "string",
            "enum": PAYMENT_METHODS
        },
        "country": {
            "type": "string",
            "enum": COUNTRIES
        }
    }
}


# Cart Event Schema
CART_EVENT_SCHEMA = {
    "type": "object",
    "required": ["event_id", "event_type", "timestamp", "user_id", "session_id", "country"],
    "properties": {
        "event_id": {
            "type": "string",
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        },
        "event_type": {
            "type": "string",
            "enum": EVENT_TYPES["CART"] + ["checkout", "cart_abandonment"]
        },
        "timestamp": {
            "type": "string",
            "format": "date-time"
        },
        "user_id": {
            "type": "string",
            "pattern": "^USER_[0-9]+$"
        },
        "session_id": {
            "type": "string",
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        },
        "product_id": {
            "type": "string",
            "pattern": "^PROD_[0-9]+$"
        },
        "product_name": {
            "type": "string",
            "minLength": 1
        },
        "category": {
            "type": "string",
            "enum": CATEGORIES
        },
        "price": {
            "type": "number",
            "minimum": 0,
            "maximum": 10000
        },
        "quantity": {
            "type": "integer",
            "minimum": 0,
            "maximum": 100
        },
        "old_quantity": {
            "type": "integer",
            "minimum": 0
        },
        "new_quantity": {
            "type": "integer",
            "minimum": 0
        },
        "cart_size": {
            "type": "integer",
            "minimum": 0,
            "maximum": 100
        },
        "cart_value": {
            "type": "number",
            "minimum": 0
        },
        "items": {
            "type": "array"
        },
        "total_items": {
            "type": "integer",
            "minimum": 0
        },
        "total_value": {
            "type": "number",
            "minimum": 0
        },
        "country": {
            "type": "string",
            "enum": COUNTRIES
        }
    }
}


# Product View Event Schema
PRODUCT_VIEW_EVENT_SCHEMA = {
    "type": "object",
    "required": ["event_id", "event_type", "timestamp", "user_id", "session_id", "country"],
    "properties": {
        "event_id": {
            "type": "string",
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        },
        "event_type": {
            "type": "string",
            "enum": EVENT_TYPES["PRODUCT"] + ["compare"]
        },
        "timestamp": {
            "type": "string",
            "format": "date-time"
        },
        "user_id": {
            "type": "string",
            "pattern": "^USER_[0-9]+$"
        },
        "session_id": {
            "type": "string",
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        },
        "product_id": {
            "type": "string",
            "pattern": "^PROD_[0-9]+$"
        },
        "product_name": {
            "type": "string",
            "minLength": 1
        },
        "category": {
            "type": ["string", "null"]
        },
        "price": {
            "type": "number",
            "minimum": 0,
            "maximum": 10000
        },
        "view_duration": {
            "type": "integer",
            "minimum": 0,
            "maximum": 3600
        },
        "from_search": {
            "type": "boolean"
        },
        "search_query": {
            "type": ["string", "null"]
        },
        "previously_viewed_count": {
            "type": "integer",
            "minimum": 0
        },
        "results_count": {
            "type": "integer",
            "minimum": 0
        },
        "filter_category": {
            "type": ["string", "null"]
        },
        "min_price": {
            "type": ["number", "null"],
            "minimum": 0
        },
        "max_price": {
            "type": ["number", "null"],
            "minimum": 0
        },
        "product_ids": {
            "type": "array",
            "items": {
                "type": "string",
                "pattern": "^PROD_[0-9]+$"
            }
        },
        "product_names": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "prices": {
            "type": "array",
            "items": {
                "type": "number",
                "minimum": 0
            }
        },
        "country": {
            "type": "string",
            "enum": COUNTRIES
        }
    }
}


# Schema mapping for easy access
EVENT_SCHEMAS = {
    "transaction": TRANSACTION_SCHEMA,
    "cart_event": CART_EVENT_SCHEMA,
    "product_view": PRODUCT_VIEW_EVENT_SCHEMA
}
