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
        """Initialize the Pulsar client and producer."""
        self.client = Client(self.pulsar_url)
        self.producer = self.client.create_producer(self.topic)
        print(f"Pulsar Producer connected to topic: {self.topic}")

    def send_message(self, message: str):
        """Send a message to the Pulsar topic."""
        if self.producer:
            self.producer.send(message.encode("utf-8"))
            self.logger.info(f"Message sent to Pulsar topic {self.topic}")
        else:
            print("Producer is not initialized.")

    def close(self):
        """Close the Pulsar client and producer."""
        if self.client:
            self.client.close()
            print("Pulsar client closed.")
