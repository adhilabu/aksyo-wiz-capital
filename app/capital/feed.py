import asyncio
import json
import os
import ssl
import uuid
import websockets
from app.redis.redis import RedisCache
from app.pulsar.producer import PulsarProducer
from app.capital.capital_com import CapitalComAPI
from app.capital.instruments import get_capital_epics, get_epics_for_strategy

STRATEGY_TYPE = os.getenv("STRATEGY_TYPE", "DEFAULT")
PING_INTERVAL = 15  # WebSocket-level ping interval in seconds
RECONNECT_DELAY = 3  # Base delay for reconnection attempts in seconds
INACTIVITY_TIMEOUT = 120  # Reconnect after 2 minutes of no data

class AuthenticationError(Exception):
    """Custom exception for authentication failures"""
    pass

async def process_websocket_message(message: str, producer: PulsarProducer, websocket: websockets.WebSocketClientProtocol) -> tuple[bool, bool]:
    """Processes WebSocket messages and returns (reconnect_needed, is_data_message)"""
    try:
        data_dict = json.loads(message)
        is_data = False
        
        # Handle market data events
        if data_dict.get("destination") == "ohlc.event":
            producer.send_message(json.dumps(data_dict.get("payload", {})))
            is_data = True
        
        # Handle authentication errors
        elif data_dict.get("status") == "error":
            error_code = data_dict.get("errorCode", "").lower()
            if "auth" in error_code or "token" in error_code:
                raise AuthenticationError(f"Auth error: {data_dict.get('errorMessage')}")
            
        return False, is_data
    
    except json.JSONDecodeError:
        print(f"Failed to decode JSON message: {message[:200]}")
    except AuthenticationError as ae:
        print(f"Authentication failure: {ae}")
        await websocket.close()
        return True, False
    except Exception as e:
        print(f"Error processing message: {str(e)}")
    
    return False, False

async def subscribe_to_epics(websocket, api_client, epics_to_subscribe):
    """Handle batch subscription with error handling"""
    batch_size = 50
    for i in range(0, len(epics_to_subscribe), batch_size):
        batch = epics_to_subscribe[i:i+batch_size]
        sub_id = str(uuid.uuid4())
        
        subscription_message = {
            "destination": "OHLCMarketData.subscribe",
            "correlationId": sub_id,
            "cst": api_client.cst,
            "securityToken": api_client.security_token,
            "payload": {
                "epics": batch,
                "resolutions": ["MINUTE"],
                "type": "classic"
            }
        }
        
        try:
            await websocket.send(json.dumps(subscription_message))
            response = await asyncio.wait_for(websocket.recv(), timeout=10)
            response_data = json.loads(response)
            
            if response_data.get("status") != "OK":
                print(f"Subscription failed for batch {i//batch_size}: {response_data}")
                if response_data.get("errorCode") == "AUTH_EXPIRED":
                    raise AuthenticationError("Auth expired during subscription")
        
        except asyncio.TimeoutError:
            print(f"Timeout waiting for subscription response (batch {i//batch_size})")
        except AuthenticationError as ae:
            print(f"Critical auth error during subscription: {ae}")
            await websocket.close()
            return False
        except Exception as e:
            print(f"Unexpected error during subscription: {str(e)}")
    
    return True


async def capital_websocket_listener(producer: PulsarProducer):
    """WebSocket listener with inactivity-based reconnection"""
    api_client = CapitalComAPI()
    reconnect_attempts = 0
    ssl_context = ssl.create_default_context()

    while True:
        try:
            if not await api_client.ensure_session_valid():
                print("Session validation failed")
                await asyncio.sleep(RECONNECT_DELAY ** reconnect_attempts)
                reconnect_attempts = min(reconnect_attempts + 1, 5)
                continue
            
            epics_to_subscribe = get_capital_epics()
            if not epics_to_subscribe:
                print("No EPICs available for subscription")
                await asyncio.sleep(60)
                continue

            async with websockets.connect(
                api_client.streaming_url,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_INTERVAL,
                ssl=ssl_context
            ) as websocket:
                
                print(f"Connected to WebSocket, subscribing to {len(epics_to_subscribe)} instruments")
                reconnect_attempts = 0  # Reset reconnect attempts
                
                # Perform subscription and reset inactivity timer
                if not await subscribe_to_epics(websocket, api_client, epics_to_subscribe):
                    continue
                last_data_time = asyncio.get_event_loop().time()
                
                # Main message loop with inactivity check
                while True:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=PING_INTERVAL * 2)
                        received_time = asyncio.get_event_loop().time()
                        reconnect_needed, is_data = await process_websocket_message(message, producer, websocket)
                        if is_data:
                            last_data_time = received_time
                        if reconnect_needed:
                            break
                    except asyncio.TimeoutError:
                        await websocket.ping()
                        print("Sent WebSocket-level ping")
                    
                    # Check inactivity after each operation
                    current_time = asyncio.get_event_loop().time()
                    if current_time - last_data_time > INACTIVITY_TIMEOUT:
                        print(f"No data for {INACTIVITY_TIMEOUT} seconds. Reconnecting...")
                        await websocket.close()
                        break
        
        except AuthenticationError as ae:
            print(f"Authentication failed: {ae}")
            await api_client.reset_session()
            await asyncio.sleep(RECONNECT_DELAY)
        except (websockets.ConnectionClosed, ConnectionResetError) as e:
            print(f"Connection closed: {str(e)}")
            await asyncio.sleep(RECONNECT_DELAY ** reconnect_attempts)
            reconnect_attempts = min(reconnect_attempts + 1, 5)
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            await asyncio.sleep(RECONNECT_DELAY ** reconnect_attempts)
            reconnect_attempts = min(reconnect_attempts + 1, 5)