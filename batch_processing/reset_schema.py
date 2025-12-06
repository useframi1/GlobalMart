"""
Schema Reset Utility
Forces re-initialization of the warehouse schema by running all SQL files
Use this when schema changes are made to SQL files
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from batch_processing.warehouse.schema_init import ensure_schema_initialized

if __name__ == "__main__":
    print("="*80)
    print("FORCING WAREHOUSE SCHEMA RE-INITIALIZATION")
    print("="*80)
    print("This will re-run all schema SQL files to ensure the warehouse is up to date.")
    print("It's safe to run this multiple times - SQL files are idempotent.")
    print()

    ensure_schema_initialized(force=True)

    print()
    print("="*80)
    print("SCHEMA RESET COMPLETE")
    print("="*80)
    print("You can now run: python main.py --jobs all")
