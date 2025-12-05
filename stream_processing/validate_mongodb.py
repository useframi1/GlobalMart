"""
MongoDB Collections Validator
Checks all stream processing collections for data quality issues
"""
import sys
import os
from pymongo import MongoClient
from datetime import datetime
from typing import Dict, List, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import settings


class MongoDBValidator:
    """Validate MongoDB collections written by stream processing"""

    def __init__(self):
        """Initialize MongoDB connection"""
        self.client = MongoClient(settings.mongo.connection_string)
        self.db = self.client[settings.mongo.database]

        # Expected collections from stream processing
        self.expected_collections = [
            # Sales Aggregator
            "sales_per_minute",
            "sales_by_country",
            # Cart Analyzer
            "cart_sessions",
            "abandoned_carts",
            "cart_metrics",
            # Product View Analyzer
            "trending_products",
            "category_performance",
            "search_behavior",
            "browsing_sessions",
            # Anomaly Detector
            "high_value_anomalies",
            "high_quantity_anomalies",
            "suspicious_patterns",
            "anomaly_stats",
            # Inventory Tracker
            "product_sales_velocity",
            "inventory_by_country",
            "high_demand_alerts",
            "restock_alerts",
            "inventory_metrics"
        ]

    def get_all_collections(self) -> List[str]:
        """Get all collection names in the database"""
        return self.db.list_collection_names()

    def check_collection_exists(self) -> Dict[str, bool]:
        """Check which expected collections exist"""
        existing = self.get_all_collections()
        results = {}

        print("=" * 80)
        print("COLLECTION EXISTENCE CHECK")
        print("=" * 80)
        print()

        for collection in self.expected_collections:
            exists = collection in existing
            results[collection] = exists
            status = "✓ EXISTS" if exists else "✗ MISSING"
            print(f"{status:12} {collection}")

        print()
        existing_count = sum(results.values())
        print(f"Summary: {existing_count}/{len(self.expected_collections)} collections exist")
        print()

        return results

    def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """Get statistics for a collection"""
        collection = self.db[collection_name]
        count = collection.count_documents({})

        if count == 0:
            return {
                "count": 0,
                "sample": None,
                "fields": [],
                "null_analysis": {}
            }

        # Get a sample document
        sample = collection.find_one()

        # Get all unique fields from first 100 documents
        pipeline = [
            {"$limit": 100},
            {"$project": {"arrayofkeyvalue": {"$objectToArray": "$$ROOT"}}},
            {"$unwind": "$arrayofkeyvalue"},
            {"$group": {"_id": None, "allkeys": {"$addToSet": "$arrayofkeyvalue.k"}}}
        ]

        result = list(collection.aggregate(pipeline))
        all_fields = result[0]["allkeys"] if result else []

        # Analyze null values for each field
        null_analysis = {}
        for field in all_fields:
            if field == "_id":
                continue

            null_count = collection.count_documents({field: None})
            missing_count = collection.count_documents({field: {"$exists": False}})
            total_issues = null_count + missing_count

            if total_issues > 0:
                null_analysis[field] = {
                    "null_count": null_count,
                    "missing_count": missing_count,
                    "total_issues": total_issues,
                    "percentage": (total_issues / count) * 100
                }

        return {
            "count": count,
            "sample": sample,
            "fields": all_fields,
            "null_analysis": null_analysis
        }

    def validate_all_collections(self):
        """Validate all collections and report issues"""
        print("=" * 80)
        print("DETAILED COLLECTION VALIDATION")
        print("=" * 80)
        print()

        existence = self.check_collection_exists()

        for collection_name in self.expected_collections:
            if not existence[collection_name]:
                continue

            print("-" * 80)
            print(f"Collection: {collection_name}")
            print("-" * 80)

            stats = self.get_collection_stats(collection_name)

            print(f"Document count: {stats['count']}")

            if stats['count'] == 0:
                print("⚠️  WARNING: Collection is EMPTY!")
                print()
                continue

            print(f"Fields found: {len(stats['fields'])}")
            print(f"Fields: {', '.join(stats['fields'])}")
            print()

            # Check for null/missing value issues
            if stats['null_analysis']:
                print("⚠️  NULL/MISSING VALUE ISSUES DETECTED:")
                print()
                for field, analysis in stats['null_analysis'].items():
                    print(f"  Field: {field}")
                    print(f"    - Null values: {analysis['null_count']}")
                    print(f"    - Missing (field doesn't exist): {analysis['missing_count']}")
                    print(f"    - Total issues: {analysis['total_issues']} ({analysis['percentage']:.1f}%)")
                    print()

                    # Show severity
                    if analysis['percentage'] == 100:
                        print(f"    🔴 CRITICAL: Field is ALWAYS null/missing!")
                    elif analysis['percentage'] >= 50:
                        print(f"    🟡 WARNING: Field is null/missing in >50% of documents")
                    print()
            else:
                print("✓ No null/missing value issues detected")
                print()

            # Show sample document
            print("Sample document:")
            print("-" * 40)
            if stats['sample']:
                for key, value in stats['sample'].items():
                    if key == "_id":
                        continue
                    # Truncate long values
                    str_value = str(value)
                    if len(str_value) > 60:
                        str_value = str_value[:60] + "..."
                    print(f"  {key}: {str_value}")
            print()

    def generate_summary_report(self):
        """Generate summary report of all issues"""
        print("=" * 80)
        print("SUMMARY REPORT")
        print("=" * 80)
        print()

        existence = self.check_collection_exists()
        missing_collections = [name for name, exists in existence.items() if not exists]
        empty_collections = []
        collections_with_null_issues = []

        for collection_name in self.expected_collections:
            if not existence[collection_name]:
                continue

            stats = self.get_collection_stats(collection_name)

            if stats['count'] == 0:
                empty_collections.append(collection_name)

            if stats['null_analysis']:
                # Check for critical issues (100% null)
                critical_fields = [
                    f"{collection_name}.{field}"
                    for field, analysis in stats['null_analysis'].items()
                    if analysis['percentage'] == 100
                ]
                if critical_fields:
                    collections_with_null_issues.append({
                        "collection": collection_name,
                        "critical_fields": critical_fields
                    })

        # Print summary
        if missing_collections:
            print("🔴 MISSING COLLECTIONS:")
            for name in missing_collections:
                print(f"  - {name}")
            print()

        if empty_collections:
            print("⚠️  EMPTY COLLECTIONS:")
            for name in empty_collections:
                print(f"  - {name}")
            print()

        if collections_with_null_issues:
            print("🔴 COLLECTIONS WITH ALWAYS-NULL FIELDS:")
            for item in collections_with_null_issues:
                print(f"  Collection: {item['collection']}")
                for field in item['critical_fields']:
                    print(f"    - {field}")
            print()

        if not (missing_collections or empty_collections or collections_with_null_issues):
            print("✓ ALL COLLECTIONS ARE HEALTHY!")
            print("  - All expected collections exist")
            print("  - All collections have data")
            print("  - No critical null value issues detected")
            print()

        # Overall statistics
        existing_count = sum(existence.values())
        non_empty_count = existing_count - len(empty_collections)

        print("OVERALL STATISTICS:")
        print(f"  Expected collections: {len(self.expected_collections)}")
        print(f"  Existing collections: {existing_count}")
        print(f"  Non-empty collections: {non_empty_count}")
        print(f"  Collections with issues: {len(collections_with_null_issues)}")
        print()

    def close(self):
        """Close MongoDB connection"""
        self.client.close()


def main():
    """Main entry point"""
    print()
    print("=" * 80)
    print("GlobalMart MongoDB Collections Validator")
    print("=" * 80)
    print()
    print(f"Database: {settings.mongo.database}")
    print(f"Connection: {settings.mongo.host}:{settings.mongo.port}")
    print()

    validator = MongoDBValidator()

    try:
        # Run validation
        validator.validate_all_collections()

        # Generate summary
        validator.generate_summary_report()

    except Exception as e:
        print(f"❌ Error during validation: {e}")
        import traceback
        traceback.print_exc()

    finally:
        validator.close()

    print("=" * 80)
    print("Validation complete")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
