# Placeholder for Capital.com WebSocket feed implementation
import asyncio
import json
import os
import ssl
import uuid
import websockets
from app.redis.redis import RedisCache # Assuming RedisCache is still used
from app.pulsar.producer import PulsarProducer # Assuming PulsarProducer is still used
from app.capital.capital_com import CapitalComAPI
from app.capital.instruments import get_capital_epics, get_epics_for_strategy # Use the new instrument function
# from app.capital.schemas import CapitalWebSocketMessage # Import schema for parsing messages

# Determine which set of EPICs to subscribe to based on environment variable or config
# This replaces the old WS_FEED_TYPE logic
STRATEGY_TYPE = os.getenv("STRATEGY_TYPE", "DEFAULT") # Example: Use STRATEGY_TYPE instead of WS_FEED_TYPE

async def process_websocket_message(message: str, producer: PulsarProducer, websocket: websockets.WebSocketClientProtocol):
    """Processes a raw message from the WebSocket and sends it to Pulsar."""
    try:
        data_dict = json.loads(message)
        if data_dict.get("destination") == "ohlc.event":
            payload = data_dict.get("payload", {})            
            producer.send_message(json.dumps(payload))

        elif data_dict.get("destination") == "ping":
            payload = data_dict.get("payload", {})
            timestamp = payload.get("timestamp")
            if timestamp is not None:
                pong_response = {
                    "destination": "pong",
                    "payload": {
                        "timestamp": timestamp
                    }
                }
                await websocket.send(json.dumps(pong_response))
                print(f"Sent pong response for timestamp {timestamp}")
            else:
                print("Received ping message without timestamp")

        else:
            print(f"Received unexpected message: {data_dict}")
            pass
            
    except json.JSONDecodeError:
        print(f"Failed to decode JSON message: {message}")
    except Exception as e:
        print(f"Error processing WebSocket message: {e}")

async def capital_websocket_listener(producer: PulsarProducer):
    """Connects to Capital.com WebSocket and listens for market data."""
    api_client = CapitalComAPI() # Initialize client to handle session
    websocket = None

    while True:
        try:
            print("Attempting to connect to Capital.com WebSocket...")
            # Ensure session is valid before connecting
            if not await api_client.ensure_session_valid():
                print("Failed to establish API session. Retrying in 60 seconds...")
                await asyncio.sleep(60)
                continue
            websocket = await websockets.connect(api_client.streaming_url)
            epics_to_subscribe = get_capital_epics()
            
            if not epics_to_subscribe:
                print(f"No EPICs found for strategy type '{STRATEGY_TYPE} '. Waiting...")
                await asyncio.sleep(60)
                continue

            # Subscribe to market data for the selected EPICs
            # Capital.com might require subscribing in batches if the list is large
            # Adjust batch size as needed
            batch_size = 50 
            for i in range(0, len(epics_to_subscribe), batch_size):
                batch = epics_to_subscribe[i:i + batch_size]
                subscription_message = {
                    "destination": "OHLCMarketData.subscribe",
                    "correlationId": str(uuid.uuid4()),
                    "cst": api_client.cst, # Include tokens if required per message
                    "securityToken": api_client.security_token,
                    "payload": {
                        "epics": batch,
                        "resolutions": [
                            "MINUTE"
                        ],
                        "type": "classic"
                    }
                }
                await websocket.send(json.dumps(subscription_message))
                sub_response = await websocket.recv() # Wait for confirmation
                print(f"Subscription response for batch {i//batch_size + 1}: {sub_response}")
                # Check response status
                if json.loads(sub_response).get("status") != "OK":
                    print(f"Warning: Subscription failed for batch {i//batch_size + 1}")
            
            print(f"Subscribed to {len(epics_to_subscribe)} EPICs.")

            # Start listening for messages
            while True:
                message = await websocket.recv()
                await process_websocket_message(message, producer, websocket)  # Pass websocket to handler

        except websockets.exceptions.ConnectionClosedError as e:
            print(f"WebSocket connection closed unexpectedly: {e}. Reconnecting in 10 seconds...")
            websocket = None
            await asyncio.sleep(10)
        except Exception as e:
            print(f"An error occurred in WebSocket listener: {e}. Retrying in 30 seconds...")
            if websocket:
                try:
                    await websocket.close()
                except Exception:
                    pass # Ignore errors during close
            websocket = None
            await asyncio.sleep(30)
