"""
GlobalMart Data Generation Main Entry Point
Orchestrates data generation and streams to Kafka
"""
import time
import sys
import os
import random
from datetime import datetime
import signal

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import settings
from config.constants import (
    ANOMALY_PROBABILITY,
    EVENT_DISTRIBUTION_TYPES,
    EVENT_DISTRIBUTION_WEIGHTS
)
from data_generation.generators.user_generator import UserGenerator
from data_generation.generators.product_generator import ProductGenerator
from data_generation.generators.transaction_generator import TransactionGenerator
from data_generation.generators.cart_events_generator import CartEventsGenerator
from data_generation.generators.product_view_generator import ProductViewGenerator
from data_generation.kafka_producers.transaction_producer import TransactionProducer
from data_generation.kafka_producers.cart_events_producer import CartEventsProducer
from data_generation.kafka_producers.product_view_producer import ProductViewProducer
from data_generation.validation.validator import EventValidator


class DataGenerationOrchestrator:
    """Orchestrates data generation and streaming to Kafka"""

    def __init__(self):
        self.running = False
        self.user_generator = UserGenerator()
        self.product_generator = ProductGenerator()
        self.transaction_generator = None
        self.cart_events_generator = None
        self.product_view_generator = None
        self.transaction_producer = None
        self.cart_events_producer = None
        self.product_view_producer = None

        # Event counters
        self.transaction_count = 0
        self.cart_event_count = 0
        self.product_view_count = 0

        # Validation
        self.enable_validation = settings.data_generation.enable_validation
        self.validator = EventValidator(strict_mode=False) if self.enable_validation else None
        self.validation_failed_count = 0

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print("\n\nReceived shutdown signal...")
        self.running = False

    def setup(self):
        """Setup: Generate or load users and products"""
        print("=" * 60)
        print("GlobalMart Data Generation Setup")
        print("=" * 60)
        print()

        # Generate users if not exists
        if not os.path.exists("data/raw/users.json"):
            print(f"Generating {settings.data_generation.num_users} users...")
            self.user_generator.generate_all()
            self.user_generator.save_to_file()
        else:
            print("Loading existing users...")
            self.user_generator.load_from_file()

        print()

        # Generate products if not exists
        if not os.path.exists("data/raw/products.json"):
            print(f"Generating {settings.data_generation.num_products} products...")
            self.product_generator.generate_all()
            self.product_generator.save_to_file()
        else:
            print("Loading existing products...")
            self.product_generator.load_from_file()

        print()

        # Initialize generators
        print("Initializing generators...")
        self.transaction_generator = TransactionGenerator(
            user_generator=self.user_generator,
            product_generator=self.product_generator
        )
        self.cart_events_generator = CartEventsGenerator(
            users=self.user_generator.generated_users,
            products=self.product_generator.generated_products
        )
        self.product_view_generator = ProductViewGenerator(
            users=self.user_generator.generated_users,
            products=self.product_generator.generated_products
        )

        print("✓ Setup complete")
        print()

    def start_streaming(self):
        """Start streaming all event types to Kafka"""
        print("=" * 60)
        print("Starting Multi-Event Stream")
        print("=" * 60)
        print(f"Target rate: {settings.data_generation.events_per_second} events/second")
        print(f"Event distribution: 60% product views, 30% cart events, 10% transactions")
        print(f"Validation: {'Enabled' if self.enable_validation else 'Disabled'}")
        print("Press Ctrl+C to stop")
        print()

        self.running = True

        # Calculate delay between messages
        events_per_second = settings.data_generation.events_per_second
        delay = 1.0 / events_per_second if events_per_second > 0 else 0.1

        # Connect to Kafka producers
        self.transaction_producer = TransactionProducer()
        self.cart_events_producer = CartEventsProducer()
        self.product_view_producer = ProductViewProducer()

        start_time = time.time()
        total_events = 0

        try:
            while self.running:
                # Determine event type based on realistic distribution
                # Product views are most common (60%), then cart events (30%), then transactions (10%)
                event_type = random.choices(
                    EVENT_DISTRIBUTION_TYPES,
                    weights=EVENT_DISTRIBUTION_WEIGHTS
                )[0]

                if event_type == "product_view":
                    # Generate and send product view event
                    event = self.product_view_generator.generate_product_view_event()

                    # Validate if enabled
                    if self.enable_validation:
                        is_valid, errors = self.validator.validate_event(event, "product_view")
                        if not is_valid:
                            self.validation_failed_count += 1
                            print(f"⚠️  Validation failed for product_view: {errors[0]}")
                            continue

                    self.product_view_producer.send_product_view_event(event)
                    self.product_view_count += 1

                elif event_type == "cart_event":
                    # Generate and send cart event
                    event = self.cart_events_generator.generate_cart_event()

                    # Validate if enabled
                    if self.enable_validation:
                        is_valid, errors = self.validator.validate_event(event, "cart_event")
                        if not is_valid:
                            self.validation_failed_count += 1
                            print(f"⚠️  Validation failed for cart_event: {errors[0]}")
                            continue

                    self.cart_events_producer.send_cart_event(event)
                    self.cart_event_count += 1

                else:  # transaction
                    # Generate and send transaction (use anomaly probability from constants)
                    if random.random() < ANOMALY_PROBABILITY:
                        transaction = self.transaction_generator.generate_anomalous_transaction()
                    else:
                        transaction = self.transaction_generator.generate_transaction()

                    # Validate if enabled
                    if self.enable_validation:
                        is_valid, errors = self.validator.validate_event(transaction, "transaction")
                        if not is_valid:
                            self.validation_failed_count += 1
                            print(f"⚠️  Validation failed for transaction: {errors[0]}")
                            continue

                    self.transaction_producer.send_transaction(transaction)
                    self.transaction_count += 1

                total_events += 1

                # Print stats every 100 events
                if total_events % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = total_events / elapsed if elapsed > 0 else 0
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                          f"Total: {total_events} | "
                          f"Rate: {rate:.2f}/sec | "
                          f"Views: {self.product_view_count} | "
                          f"Cart: {self.cart_event_count} | "
                          f"Txns: {self.transaction_count}")

                # Rate limiting
                time.sleep(delay)

        except KeyboardInterrupt:
            print("\nStopping...")
        except Exception as e:
            print(f"\n✗ Error during streaming: {e}")
        finally:
            self.cleanup(total_events, time.time() - start_time)

    def cleanup(self, total_events: int, elapsed: float):
        """Cleanup resources and print final stats"""
        print()
        print("=" * 60)
        print("Shutting Down")
        print("=" * 60)

        # Close all producers
        if self.transaction_producer:
            self.transaction_producer.close()
        if self.cart_events_producer:
            self.cart_events_producer.close()
        if self.product_view_producer:
            self.product_view_producer.close()

        # Print final statistics
        rate = total_events / elapsed if elapsed > 0 else 0

        print(f"\nFinal Statistics:")
        print(f"  Total events: {total_events}")
        print(f"  Duration: {elapsed:.2f} seconds")
        print(f"  Average rate: {rate:.2f} events/second")
        print()
        print(f"Event Breakdown:")
        if total_events > 0:
            print(f"  Product views: {self.product_view_count} ({self.product_view_count/total_events*100:.1f}%)")
            print(f"  Cart events: {self.cart_event_count} ({self.cart_event_count/total_events*100:.1f}%)")
            print(f"  Transactions: {self.transaction_count} ({self.transaction_count/total_events*100:.1f}%)")
        else:
            print(f"  Product views: {self.product_view_count}")
            print(f"  Cart events: {self.cart_event_count}")
            print(f"  Transactions: {self.transaction_count}")
        print()

        # Print validation statistics if enabled
        if self.enable_validation and self.validator:
            validation_stats = self.validator.get_stats()
            print(f"Validation Statistics:")
            print(f"  Total validated: {validation_stats['total_validated']}")
            print(f"  Passed: {validation_stats['total_passed']} ({validation_stats['pass_rate']:.1f}%)")
            print(f"  Failed: {validation_stats['total_failed']}")
            if validation_stats['errors_by_type']:
                print(f"  Errors by type:")
                for event_type, errors in validation_stats['errors_by_type'].items():
                    print(f"    {event_type}: {len(errors)} errors")
            print()

        # Print producer statistics
        if self.transaction_producer:
            stats = self.transaction_producer.get_stats()
            print(f"Transaction Producer:")
            print(f"  Sent: {stats['messages_sent']} | Failed: {stats['messages_failed']} | Success: {stats['success_rate']:.1f}%")

        if self.cart_events_producer:
            stats = self.cart_events_producer.get_stats()
            print(f"Cart Events Producer:")
            print(f"  Sent: {stats['messages_sent']} | Failed: {stats['messages_failed']} | Success: {stats['success_rate']:.1f}%")

        if self.product_view_producer:
            stats = self.product_view_producer.get_stats()
            print(f"Product View Producer:")
            print(f"  Sent: {stats['messages_sent']} | Failed: {stats['messages_failed']} | Success: {stats['success_rate']:.1f}%")

        print("\n✓ Data generation stopped")

    def run(self):
        """Main run method"""
        self.setup()
        self.start_streaming()


if __name__ == "__main__":
    orchestrator = DataGenerationOrchestrator()
    orchestrator.run()
