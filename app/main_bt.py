import asyncio
import os
from fastapi import FastAPI
from dotenv import load_dotenv
# from app.ws.feed_backtest import fetch_market_data_test

load_dotenv(dotenv_path=".env", override=True) 

# Configurations
PULSAR_URL = os.getenv('PULSAR_URL', '')
PULSAR_TOPIC = os.getenv('PULSAR_TOPIC', 'capital-topic')

async def lifespan(app: FastAPI):
    # Startup tasks
    try:
        print("Starting up...")
        # await fetch_market_data_test()  # Run your asynchronous task
    except Exception as e:
        print(f"Error during startup: {e}")
        raise
    
    # Yield to keep the application running
    yield

    # Shutdown tasks
    try:
        print("Shutting down...")
        # Add any cleanup tasks here if needed
    except Exception as e:
        print(f"Error during shutdown: {e}")

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health_check():
    return {"status": "ok"}
