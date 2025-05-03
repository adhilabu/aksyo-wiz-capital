import logging
import os
from pulsar import Client

from app.utils.logger import setup_logger
from app.utils.utils import get_logging_level
LOGGING_LEVEL = get_logging_level()

class PulsarProducer:
    def __init__(self, pulsar_url: str, topic: str):
        self.pulsar_url = pulsar_url
        self.topic = topic
        self.client = None
        self.producer = None
        self.logger = setup_logger("PulsarProducerLogger", "pulsar_producer", LOGGING_LEVEL)

    def start(self):
        """Initialize the Pulsar client and non-persistent producer."""
        self.client = Client(self.pulsar_url)
        self.producer = self.client.create_producer(
            f"non-persistent://public/default/{self.topic}", 
            send_timeout_millis=2000,  # Message timeout of 5 seconds
            batching_enabled=True,
            batching_max_publish_delay_ms=2
        )
        print(f"Pulsar Producer connected to non-persistent topic: {self.topic}")

    def send_message(self, message: str):
        """Send a message to the non-persistent Pulsar topic."""
        if self.producer:
            self.producer.send(message.encode("utf-8"))
            self.logger.info(f"Message sent to Pulsar topic {self.topic}")
        else:
            print("Producer is not initialized.")

    def start_v2(self):
        """Initialize the Pulsar client and non-persistent producer."""
        self.client = Client(self.pulsar_url)
        self.producer = self.client.create_producer(
            f"non-persistent://public/default/{self.topic}"
        )
        print(f"Pulsar Producer connected to non-persistent topic: {self.topic}")

    def send_message_v2(self, message: str):
        """Send a message to the non-persistent Pulsar topic."""
        if self.producer is None:
            self.start_v2()

        self.producer.send(message.encode("utf-8"))
        # self.logger.info(f"Message sent to Pulsar topic {self.topic}")

    def close(self):
        """Close the Pulsar client and producer."""
        if self.client:
            self.client.close()
            print("Pulsar client closed.")