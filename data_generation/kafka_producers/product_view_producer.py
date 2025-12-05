"""
Product View Events Kafka Producer
Sends product view events to Kafka topic
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import settings
from data_generation.kafka_producers.base_producer import BaseKafkaProducer


class ProductViewProducer(BaseKafkaProducer):
    """Producer for product view events"""

    def __init__(self):
        """Initialize product view producer"""
        super().__init__(topic=settings.kafka.topic_product_views)

    def send_product_view_event(self, event: dict) -> bool:
        """
        Send a product view event to Kafka

        Args:
            event: Product view event dictionary

        Returns:
            bool: True if sent successfully
        """
        # Use user_id as key for consistent partitioning
        return self.send_message(
            message=event,
            key=event.get('user_id')
        )
