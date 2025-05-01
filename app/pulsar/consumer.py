import asyncio
from datetime import datetime
from pulsar import Client
from _pulsar import ConsumerType
import json
from app.analyse.analyse import StockIndicatorCalculator
from app.analyse.transform import filter_data_by_interval, transform_data
from app.database.db import DBConnection
from app.redis.redis import RedisCache
import traceback

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
    def __init__(self, pulsar_url, topic, analyse):
        self.pulsar_url = pulsar_url
        self.topic = topic
        self.client = None
        self.consumer = None
        self.analyse: StockIndicatorCalculator = analyse

    async def start(self):
        self.client = Client(self.pulsar_url)
        self.consumer = self.client.subscribe(
            self.topic,
            subscription_name="upstox-subscription",
            consumer_type=ConsumerType.Shared
        )
        print(f"Pulsar consumer subscribed to {self.topic}...")

    async def consume(self, worker_count=4):
        """
        Run multiple async workers to process messages concurrently.
        """
        tasks = [self._consume_worker() for _ in range(worker_count)]
        await asyncio.gather(*tasks)

    async def _consume_worker(self):
        """
        Single worker function that continuously processes messages.
        """
        while True:
            try:
                batch = await asyncio.to_thread(self.consumer.batch_receive)
                for msg in batch:
                    await self.process_message(msg)
            except Exception:
                print(f"Error in consumer worker:\n{traceback.format_exc()}")

    async def process_message(self, msg):
        """
        Process a single message from Pulsar.
        """
        try:
            message_data = msg.data().decode("utf-8")
            loaded_data = json.loads(message_data)

            self.consumer.acknowledge(msg)

            filtered_msg = filter_data_by_interval(loaded_data)
            if filtered_msg is None:
                return

            transformed_data = transform_data(loaded_data)
            if transformed_data.empty:
                return

            await self.analyse.update_all_stocks_data(transformed_data)

        except Exception:
            print(f"Error processing message:\n{traceback.format_exc()}")
            await asyncio.to_thread(self.consumer.negative_acknowledge, msg)

    async def close(self):
        if self.client:
            await asyncio.to_thread(self.client.close())