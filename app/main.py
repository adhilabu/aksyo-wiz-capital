import asyncio
import os
from fastapi import FastAPI
from app.capital.feed import capital_websocket_listener
from app.notification.telegram import TelegramAPI
from app.pulsar.consumer import PulsarConsumer, initialize_shared_data
from app.pulsar.producer import PulsarProducer
import traceback
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env", override=True) 

PULSAR_URL = os.getenv("PULSAR_URL", "")
base_topic = os.getenv("PULSAR_TOPIC", "capital-topic")
split_type = int(os.getenv("SPLIT_TYPE", "1"))
PULSAR_TOPIC = f"{base_topic}-{split_type}"
CONSUMER_COUNT = int(os.getenv("CONSUMER_THREAD_COUNT", 6))  # Renamed to better reflect usage

pulsar_producer = PulsarProducer(PULSAR_URL, PULSAR_TOPIC)

TELEGRAM_NOTIFICATION = os.getenv("TELEGRAM_NOTIFICATION", "false")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


message = (
    f"Aksyo - Capital Strategy strarted with the following configuration:\n"
    f"- Pulsar URL: {PULSAR_URL}\n"
    f"- Pulsar Topic: {PULSAR_TOPIC}\n"
    f"- Split Type: {split_type}\n"
)

if TELEGRAM_NOTIFICATION.lower() == "true":
    telegram_api = TelegramAPI(TELEGRAM_TOKEN)
    telegram_api.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)


async def lifespan(app: FastAPI):
    async def producer_handler():
        try:
            # Use the Capital.com WebSocket listener
            await capital_websocket_listener(pulsar_producer)
        except asyncio.CancelledError:
            print("Producer task cancelled.")
        except Exception:
            print(f"Error in Producer Handler:\n{traceback.format_exc()}")

    async def startup_tasks():
        try:
            print("Initializing resources...")
            pulsar_producer.start()
            analyse = await initialize_shared_data()

            # Start multiple async consumers
            app.state.consumers = []
            # for i in range(CONSUMER_COUNT):
            try:
                consumer = PulsarConsumer(PULSAR_URL, PULSAR_TOPIC, analyse)
                await consumer.start()
                task = asyncio.create_task(consumer.consume())  # Async task per consumer
                app.state.consumers.append(task)
                print(f"Consumer started.")
            except Exception:
                print(f"Error starting consumer:\n{traceback.format_exc()}")

            app.state.producer_task = asyncio.create_task(producer_handler())
        except Exception:
            print(f"Error during startup:\n{traceback.format_exc()}")
            raise

    async def shutdown_tasks():
        try:
            print("Shutting down resources...")
            app.state.producer_task.cancel()
            await asyncio.gather(app.state.producer_task, return_exceptions=True)

            # Shutdown all consumers
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
