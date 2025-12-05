"""
Cart Events Kafka Producer
Sends cart events to Kafka topic
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from data_generation.kafka_producers.base_producer import BaseKafkaProducer


class CartEventsProducer(BaseKafkaProducer):
    """Producer for cart events"""

    def __init__(self):
        """Initialize cart events producer"""
        super().__init__(topic=settings.kafka.topic_cart_events)

    def send_cart_event(self, event: dict) -> bool:
        """
        Send a cart event to Kafka

        Args:
            event: Cart event dictionary

        Returns:
            bool: True if sent successfully
        """
        # Use user_id as key for consistent partitioning
        return self.send_message(
            message=event,
            key=event.get('user_id')
        )
