"""
Base Kafka Producer for GlobalMart
Provides reusable Kafka producer functionality with error handling and metrics
"""
import json
import time
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings


class BaseKafkaProducer:
    """Base class for Kafka producers with common functionality"""

    def __init__(self, topic: str):
        self.topic = topic
        self.producer = None
        self.messages_sent = 0
        self.messages_failed = 0
        self.connect()

    def connect(self):
        """Establish connection to Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.kafka.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks=1,  # Wait for leader acknowledgment
                compression_type='snappy',  # Compress messages
                batch_size=16384,  # Batch messages for efficiency
                linger_ms=10,  # Wait up to 10ms to batch messages
                max_in_flight_requests_per_connection=5,
                retries=3  # Retry failed sends
            )
            print(f"✓ Connected to Kafka at {settings.kafka.bootstrap_servers}")
        except Exception as e:
            print(f"✗ Failed to connect to Kafka: {e}")
            raise

    def send_message(self, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Send a message to Kafka topic

        Args:
            message: Message data (will be JSON serialized)
            key: Optional message key for partitioning

        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            future = self.producer.send(self.topic, value=message, key=key)

            # Block for synchronous send (can be changed to async for better performance)
            record_metadata = future.get(timeout=10)

            self.messages_sent += 1
            return True

        except KafkaError as e:
            self.messages_failed += 1
            print(f"✗ Failed to send message: {e}")
            return False

    def send_batch(self, messages: list, keys: Optional[list] = None) -> int:
        """
        Send a batch of messages to Kafka

        Args:
            messages: List of message dictionaries
            keys: Optional list of keys (same length as messages)

        Returns:
            int: Number of successfully sent messages
        """
        if keys is None:
            keys = [None] * len(messages)

        success_count = 0

        for message, key in zip(messages, keys):
            if self.send_message(message, key):
                success_count += 1

        return success_count

    def flush(self):
        """Flush any pending messages"""
        if self.producer:
            self.producer.flush()

    def close(self):
        """Close the producer connection"""
        if self.producer:
            self.flush()
            self.producer.close()
            print(f"✓ Kafka producer closed. Sent: {self.messages_sent}, Failed: {self.messages_failed}")

    def get_stats(self) -> Dict[str, int]:
        """Get producer statistics"""
        return {
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "success_rate": (self.messages_sent / (self.messages_sent + self.messages_failed)) * 100
            if (self.messages_sent + self.messages_failed) > 0 else 0
        }

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


if __name__ == "__main__":
    # Test the base producer
    print("Testing Base Kafka Producer...")

    with BaseKafkaProducer(topic=settings.kafka.topic_transactions) as producer:
        # Send test message
        test_message = {
            "test_id": "TEST_001",
            "message": "Hello from GlobalMart",
            "timestamp": time.time()
        }

        success = producer.send_message(test_message, key="test")

        if success:
            print("✓ Test message sent successfully")
        else:
            print("✗ Test message failed to send")

        stats = producer.get_stats()
        print(f"\nProducer stats: {stats}")
