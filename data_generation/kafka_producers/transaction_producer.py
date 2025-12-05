"""
Transaction Kafka Producer for GlobalMart
Produces transaction events to Kafka
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from data_generation.kafka_producers.base_producer import BaseKafkaProducer


class TransactionProducer(BaseKafkaProducer):
    """Producer for transaction events"""

    def __init__(self):
        super().__init__(topic=settings.kafka.topic_transactions)

    def send_transaction(self, transaction: dict) -> bool:
        """
        Send a transaction to Kafka

        Args:
            transaction: Transaction dictionary with all transaction data

        Returns:
            bool: True if sent successfully
        """
        # Use transaction_id as key for consistent partitioning
        return self.send_message(
            message=transaction,
            key=transaction.get('transaction_id')
        )


if __name__ == "__main__":
    # Test the transaction producer
    import json
    from data_generation.generators.transaction_generator import TransactionGenerator

    print("Testing Transaction Producer...")

    # Generate test transaction
    transaction_gen = TransactionGenerator()
    transaction = transaction_gen.generate_transaction()

    print("\nGenerating transaction:")
    print(json.dumps(transaction, indent=2))

    # Send to Kafka
    with TransactionProducer() as producer:
        success = producer.send_transaction(transaction)

        if success:
            print("\n✓ Transaction sent to Kafka successfully")
        else:
            print("\n✗ Failed to send transaction to Kafka")

        stats = producer.get_stats()
        print(f"\nProducer stats: {stats}")
