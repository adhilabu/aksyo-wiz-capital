# Import necessary modules
import asyncio
import json
import os
import ssl
import uuid
import upstox_client
import websockets
from google.protobuf.json_format import MessageToDict
from app.redis.redis import RedisCache
from app.upstox.MarketDataFeed_pb2 import FeedResponse
from app.pulsar.producer import PulsarProducer
from app.upstox.instruments import get_instruments
from app.upstox.auth import make_auth
from app.utils.utils import get_accesstoken_filename

WS_FEED_TYPE = os.getenv("WS_FEED_TYPE")

'''
V1 : It is used to fetch the nifty 100 stocks from upstox
V2 : It is used to fetch the nifty future stocks from upstox
V3 : It is used to fetch the nifty and banknifty option details from upstox
V4 : It is used to fetch single sample stock from upstox
V5 : It is used to fetch the nifty and banknifty OCs from upstox
''' 


def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(
        upstox_client.ApiClient(configuration))
    api_response = api_instance.get_market_data_feed_authorize(api_version)
    return api_response

def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


def filter_data_by_interval(data_dict):
    """
    Filter the data_dict to include only entries where the timestamp is a multiple of push_interval.
    """
    feeds = data_dict.get("feeds", {})
    value_exists = False
    filtered_feeds = {}
    for feed_key, feed_value in feeds.items():
        market_ohlc = feed_value.get("ff", {}).get("marketFF", {}).get("marketOHLC", {}).get("ohlc", [])
        filtered_ohlc = [entry for entry in market_ohlc if entry.get("interval") == "I1"]
        feed_value['ff']['marketFF']['marketOHLC']['ohlc'] = filtered_ohlc
        filtered_feeds[feed_key] = feed_value
        if filtered_ohlc:
            value_exists = True
    
    updated_data_dict = data_dict
    if value_exists:
        updated_data_dict['feeds'] = filtered_feeds
        return updated_data_dict
    return None

async def fetch_market_data(producer: PulsarProducer):
    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    make_auth()

    # Configure OAuth2 access token for authorization
    configuration = upstox_client.Configuration()

    access_token = None
    filename = get_accesstoken_filename()
    with open(filename, "r") as file:
        access_token = file.read()

    api_version = "2.0"
    configuration.access_token = access_token

    # Initialize RedisCache instance
    redis_cache = RedisCache()

    # Get market data feed authorization
    response = get_market_data_feed_authorize(api_version, configuration)

    # Connect to the WebSocket with SSL context
    async with websockets.connect(response.data.authorized_redirect_uri, ssl=ssl_context) as websocket:
        print("Connection established")

        # await asyncio.sleep(1)  # Wait for 1 second

        batches = get_instruments(WS_FEED_TYPE)
        accumulated_batches = []

        for batch in batches:
            guid = str(uuid.uuid4()).replace("-", "")[:12]
            data = {
                "guid": guid,
                "method": "sub",
                "data": {
                    "mode": "full",
                    "instrumentKeys": batch
                }
            }

            binary_data = json.dumps(data).encode("utf-8")
            accumulated_batches.append(binary_data)

        # await asyncio.sleep(2)

        # Send all accumulated batches at once
        for index, batch_data in enumerate(accumulated_batches):
            await websocket.send(batch_data)
            print(f"Subscribed to batch: {index + 1}")

        # push_interval = get_push_interval()

        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)

            data_dict = MessageToDict(decoded_data)
            data_dict.update({"ws_feed_type": WS_FEED_TYPE})

            filtered_data = filter_data_by_interval(data_dict)
            if filtered_data:
                producer.send_message(json.dumps(filtered_data))
