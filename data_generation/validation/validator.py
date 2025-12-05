"""
Event Validator
Validates event data against defined schemas and business rules
"""
import sys
import os
from typing import Dict, Any, List, Tuple
from datetime import datetime
import re

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from data_generation.validation.schemas import EVENT_SCHEMAS
from config.constants import COUNTRIES, PAYMENT_METHODS, CATEGORIES


class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass


class EventValidator:
    """Validates events against schemas and business rules"""

    def __init__(self, strict_mode: bool = False):
        """
        Initialize validator

        Args:
            strict_mode: If True, raise exceptions on validation failure
                        If False, log errors and return validation results
        """
        self.strict_mode = strict_mode
        self.validation_stats = {
            "total_validated": 0,
            "total_passed": 0,
            "total_failed": 0,
            "errors_by_type": {}
        }

    def validate_event(self, event: Dict[str, Any], event_type: str) -> Tuple[bool, List[str]]:
        """
        Validate an event against its schema

        Args:
            event: Event dictionary to validate
            event_type: Type of event (transaction, cart_event, product_view)

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        self.validation_stats["total_validated"] += 1
        errors = []

        # Get schema for event type
        schema = EVENT_SCHEMAS.get(event_type)
        if not schema:
            error = f"Unknown event type: {event_type}"
            errors.append(error)
            self._record_failure(event_type, error)
            return False, errors

        # Validate required fields
        required_fields = schema.get("required", [])
        for field in required_fields:
            if field not in event:
                error = f"Missing required field: {field}"
                errors.append(error)

        # Validate field types and constraints
        properties = schema.get("properties", {})
        for field, constraints in properties.items():
            if field not in event:
                continue

            value = event[field]
            field_errors = self._validate_field(field, value, constraints)
            errors.extend(field_errors)

        # Business rule validations
        business_errors = self._validate_business_rules(event, event_type)
        errors.extend(business_errors)

        # Record results
        if errors:
            self._record_failure(event_type, errors[0])
            if self.strict_mode:
                raise ValidationError(f"Validation failed for {event_type}: {errors}")
            return False, errors
        else:
            self.validation_stats["total_passed"] += 1
            return True, []

    def _validate_field(self, field_name: str, value: Any, constraints: Dict) -> List[str]:
        """Validate a single field against its constraints"""
        errors = []

        # Type validation
        expected_type = constraints.get("type")
        if expected_type:
            if isinstance(expected_type, list):
                # Multiple types allowed (e.g., string or null)
                if not any(self._check_type(value, t) for t in expected_type):
                    if value is not None:  # Allow None for nullable fields
                        errors.append(f"Field '{field_name}' has invalid type. Expected {expected_type}, got {type(value).__name__}")
            else:
                if not self._check_type(value, expected_type):
                    errors.append(f"Field '{field_name}' has invalid type. Expected {expected_type}, got {type(value).__name__}")

        # Skip further validation if value is None and None is allowed
        if value is None and isinstance(expected_type, list) and "null" in expected_type:
            return errors

        # Pattern validation (regex)
        pattern = constraints.get("pattern")
        if pattern and isinstance(value, str):
            if not re.match(pattern, value):
                errors.append(f"Field '{field_name}' does not match required pattern: {pattern}")

        # Enum validation
        enum_values = constraints.get("enum")
        if enum_values and value not in enum_values:
            errors.append(f"Field '{field_name}' value '{value}' not in allowed values: {enum_values}")

        # Numeric constraints
        if isinstance(value, (int, float)):
            minimum = constraints.get("minimum")
            maximum = constraints.get("maximum")
            if minimum is not None and value < minimum:
                errors.append(f"Field '{field_name}' value {value} is below minimum {minimum}")
            if maximum is not None and value > maximum:
                errors.append(f"Field '{field_name}' value {value} exceeds maximum {maximum}")

        # String constraints
        if isinstance(value, str):
            min_length = constraints.get("minLength")
            if min_length is not None and len(value) < min_length:
                errors.append(f"Field '{field_name}' length {len(value)} is below minimum {min_length}")

        # Array constraints
        if isinstance(value, list):
            min_items = constraints.get("minItems")
            max_items = constraints.get("maxItems")
            if min_items is not None and len(value) < min_items:
                errors.append(f"Field '{field_name}' has {len(value)} items, minimum is {min_items}")
            if max_items is not None and len(value) > max_items:
                errors.append(f"Field '{field_name}' has {len(value)} items, maximum is {max_items}")

            # Validate array items
            items_schema = constraints.get("items")
            if items_schema:
                for i, item in enumerate(value):
                    if isinstance(items_schema, dict):
                        item_errors = self._validate_field(f"{field_name}[{i}]", item, items_schema)
                        errors.extend(item_errors)

        return errors

    def _check_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type"""
        type_mapping = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None)
        }

        expected_python_type = type_mapping.get(expected_type)
        if expected_python_type is None:
            return True  # Unknown type, skip validation

        return isinstance(value, expected_python_type)

    def _validate_business_rules(self, event: Dict[str, Any], event_type: str) -> List[str]:
        """Validate business-specific rules"""
        errors = []

        if event_type == "transaction":
            # Validate total_amount matches sum of products
            products = event.get("products", [])
            if products:
                calculated_total = sum(p.get("quantity", 0) * p.get("price", 0) for p in products)
                reported_total = event.get("total_amount", 0)

                # Allow small floating point differences
                if abs(calculated_total - reported_total) > 0.01:
                    errors.append(f"Total amount mismatch: calculated {calculated_total}, reported {reported_total}")

        elif event_type == "cart_event":
            event_subtype = event.get("event_type")

            # Validate checkout events have items
            if event_subtype == "checkout":
                items = event.get("items", [])
                total_items = event.get("total_items", 0)
                if not items or total_items == 0:
                    errors.append("Checkout event must have items")

                # Validate total_value matches sum of items
                if items:
                    calculated_total = sum(item.get("price", 0) * item.get("quantity", 0) for item in items)
                    reported_total = event.get("total_value", 0)
                    if abs(calculated_total - reported_total) > 0.01:
                        errors.append(f"Cart total value mismatch: calculated {calculated_total}, reported {reported_total}")

            # Validate quantity updates
            if event_subtype == "update_quantity":
                old_qty = event.get("old_quantity")
                new_qty = event.get("new_quantity")
                if old_qty is not None and new_qty is not None and old_qty == new_qty:
                    errors.append("Quantity update event has same old and new quantity")

        elif event_type == "product_view":
            event_subtype = event.get("event_type")

            # Validate search events have search_query
            if event_subtype == "search":
                if not event.get("search_query"):
                    errors.append("Search event must have search_query")

            # Validate compare events have multiple products
            if event_subtype == "compare":
                product_ids = event.get("product_ids", [])
                if len(product_ids) < 2:
                    errors.append("Compare event must have at least 2 products")

            # Validate price range filters
            min_price = event.get("min_price")
            max_price = event.get("max_price")
            if min_price is not None and max_price is not None and min_price > max_price:
                errors.append(f"Invalid price range: min_price {min_price} > max_price {max_price}")

        return errors

    def _record_failure(self, event_type: str, error: str):
        """Record validation failure statistics"""
        self.validation_stats["total_failed"] += 1

        if event_type not in self.validation_stats["errors_by_type"]:
            self.validation_stats["errors_by_type"][event_type] = []

        self.validation_stats["errors_by_type"][event_type].append(error)

    def get_stats(self) -> Dict[str, Any]:
        """Get validation statistics"""
        total = self.validation_stats["total_validated"]
        passed = self.validation_stats["total_passed"]
        failed = self.validation_stats["total_failed"]

        return {
            "total_validated": total,
            "total_passed": passed,
            "total_failed": failed,
            "pass_rate": (passed / total * 100) if total > 0 else 0,
            "errors_by_type": self.validation_stats["errors_by_type"]
        }

    def reset_stats(self):
        """Reset validation statistics"""
        self.validation_stats = {
            "total_validated": 0,
            "total_passed": 0,
            "total_failed": 0,
            "errors_by_type": {}
        }


# Convenience functions
def validate_transaction(transaction: Dict[str, Any], strict: bool = False) -> Tuple[bool, List[str]]:
    """Validate a transaction event"""
    validator = EventValidator(strict_mode=strict)
    return validator.validate_event(transaction, "transaction")


def validate_cart_event(event: Dict[str, Any], strict: bool = False) -> Tuple[bool, List[str]]:
    """Validate a cart event"""
    validator = EventValidator(strict_mode=strict)
    return validator.validate_event(event, "cart_event")


def validate_product_view(event: Dict[str, Any], strict: bool = False) -> Tuple[bool, List[str]]:
    """Validate a product view event"""
    validator = EventValidator(strict_mode=strict)
    return validator.validate_event(event, "product_view")
