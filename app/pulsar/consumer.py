import asyncio
from datetime import datetime
from pulsar import Client, PulsarException
from _pulsar import ConsumerType
import json
import traceback
from app.analyse.analyse import StockIndicatorCalculator
from app.analyse.transform import transform_capital_ohlc
from app.database.db import DBConnection
from app.redis.redis import RedisCache

async def initialize_shared_data():
    """
    Perform one-time initialization of shared historical data.
    """
    db_con = DBConnection()
    redis_cache = RedisCache()
    await db_con.init()

    analyse = StockIndicatorCalculator(db_con, redis_cache, end_date=datetime.now().date())
    analyse.set_and_get_dates()
    await analyse.initialize_historical_data()
    return analyse


class PulsarConsumer:
    def __init__(self, pulsar_url, topic, analyse: StockIndicatorCalculator):
        self.pulsar_url = pulsar_url
        self.topic = topic
        self.analyse: StockIndicatorCalculator = analyse
        self.client = None
        self.consumer = None
        self._running = False

    async def start(self):
        """Initialize Pulsar client and consumer with retries."""
        retry_delay = 5
        max_retry_delay = 60

        while True:
            try:
                if self.client:
                    await asyncio.to_thread(self.client.close)

                # Create client and consumer in a thread
                def init_pulsar():
                    client = Client(self.pulsar_url)
                    consumer = client.subscribe(
                        f"non-persistent://public/default/{self.topic}",
                        subscription_name="upstox-subscription",
                        consumer_type=ConsumerType.Shared
                    )
                    return client, consumer

                self.client, self.consumer = await asyncio.to_thread(init_pulsar)
                print("Connected to Pulsar.")
                return
            except Exception as e:
                print(f"Connection failed: {e}. Retry in {retry_delay}s.")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)

    async def consume(self):
        """Start consuming messages."""
        self._running = True
        while self._running:
            try:
                if not self.consumer:
                    await self.start()

                # Receive messages with timeout to avoid blocking indefinitely
                batch = await asyncio.to_thread(
                    self.consumer.batch_receive
                )

                for msg in batch:
                    await self.process_message(msg)
            except PulsarException as e:
                print(f"Pulsar error: {e}. Reconnecting...")
                await self._safe_reconnect()
            except Exception as e:
                print(f"Unexpected error: {traceback.format_exc()}")
                await self._safe_reconnect()

    async def _safe_reconnect(self):
        """Safely reconnect and reset the consumer."""
        try:
            if self.client:
                await asyncio.to_thread(self.client.close)
        except Exception as e:
            print(f"Error closing client: {e}")
        self.client = None
        self.consumer = None

    async def process_message(self, msg):
        """Process a single message."""
        try:
            data = json.loads(msg.data().decode("utf-8"))
            self.consumer.acknowledge(msg)

            # if not filter_data_by_interval(data):
            #     return

            transformed_data = transform_capital_ohlc(data)
            if transformed_data.empty:
                return

            await self.analyse.update_all_stocks_data(transformed_data)
        except Exception as e:
            print(f"Failed to process message: {traceback.format_exc()}")
            await asyncio.to_thread(self.consumer.negative_acknowledge, msg)

    async def close(self):
        """Gracefully shutdown the consumer."""
        self._running = False
        if self.client:
            await asyncio.to_thread(self.client.close)