import asyncio
import os
from fastapi import FastAPI
from app.pulsar.consumer import PulsarConsumer, initialize_shared_data
from app.pulsar.producer import PulsarProducer
# from app.ws.feedV3 import fetch_market_data_V3 # Replaced with Capital.com feed
from app.capital.feed import capital_websocket_listener # Import the new Capital.com feed listener
import traceback
from dotenv import load_dotenv

load_dotenv()

PULSAR_URL = os.getenv("PULSAR_URL", "")
# Adjust topic naming based on strategy or other factors if needed
base_topic = os.getenv("PULSAR_TOPIC", "capital-topic") # Changed base topic name
# split_type = int(os.getenv("SPLIT_TYPE", "1")) # Keep if splitting is still relevant
# PULSAR_TOPIC = f"{base_topic}-{split_type}"
STRATEGY_TYPE = os.getenv("STRATEGY_TYPE", "DEFAULT") # Use STRATEGY_TYPE for topic/logic differentiation
PULSAR_TOPIC = f"{base_topic}-{STRATEGY_TYPE}" # Example: Topic per strategy type

CONSUMER_COUNT = int(os.getenv("CONSUMER_THREAD_COUNT", 6))

pulsar_producer = PulsarProducer(PULSAR_URL, PULSAR_TOPIC)


async def lifespan(app: FastAPI):
    async def producer_handler():
        try:
            # Use the Capital.com WebSocket listener
            await capital_websocket_listener(pulsar_producer)
        except asyncio.CancelledError:
            print("Producer task (Capital.com WebSocket Listener) cancelled.")
        except Exception:
            print(f"Error in Producer Handler (Capital.com WebSocket Listener):\n{traceback.format_exc()}")

    async def startup_tasks():
        try:
            print("Initializing resources...")
            pulsar_producer.start()
            # Initialize shared data - check if this needs changes for Capital.com data
            analyse = await initialize_shared_data()

            # Start multiple async consumers
            app.state.consumers = []
            for i in range(CONSUMER_COUNT):
                try:
                    # Pass STRATEGY_TYPE or other relevant config to consumer if needed
                    consumer = PulsarConsumer(PULSAR_URL, PULSAR_TOPIC, analyse)
                    await consumer.start()
                    task = asyncio.create_task(consumer.consume())  # Async task per consumer
                    app.state.consumers.append(task)
                    print(f"Consumer {i} started on topic {PULSAR_TOPIC}.")
                except Exception:
                    print(f"Error starting consumer {i}:\n{traceback.format_exc()}")

            app.state.producer_task = asyncio.create_task(producer_handler())
        except Exception:
            print(f"Error during startup:\n{traceback.format_exc()}")
            raise

    async def shutdown_tasks():
        try:
            print("Shutting down resources...")
            if hasattr(app.state, 'producer_task') and app.state.producer_task:
                app.state.producer_task.cancel()
                await asyncio.gather(app.state.producer_task, return_exceptions=True)

            # Shutdown all consumers
            if hasattr(app.state, 'consumers') and app.state.consumers:
                for consumer_task in app.state.consumers:
                    consumer_task.cancel()
                await asyncio.gather(*app.state.consumers, return_exceptions=True)

            pulsar_producer.close()
            print("Resources shut down successfully.")
        except Exception:
            print(f"Error during shutdown:\n{traceback.format_exc()}")

    await startup_tasks()
    yield  # Let the application run
    await shutdown_tasks()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health_check():
    return {"status": "ok"}
