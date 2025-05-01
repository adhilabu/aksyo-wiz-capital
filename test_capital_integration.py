# test_capital_integration.py
import asyncio
import os
import sys
import json
import uuid
from datetime import datetime, timedelta
import logging

# Ensure the project root is in the Python path
sys.path.insert(0, 	'/home/ubuntu/aksyo-wiz	')

# Setup basic logging for testing
logging.basicConfig(level=logging.INFO, format=	'%(asctime)s - %(name)s - %(levelname)s - %(message)s	')
logger = logging.getLogger(__name__)

# Attempt to load environment variables from .env file if present
try:
    from dotenv import load_dotenv
    dotenv_path = os.path.join(	'/home/ubuntu/aksyo-wiz	', 	'.env	')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
        logger.info("Loaded environment variables from .env file.")
    else:
        logger.info(".env file not found, relying on exported environment variables.")
except ImportError:
    logger.info("dotenv library not found, relying on exported environment variables.")
    pass

# Import Capital.com client and schemas after setting path
from app.capital.capital_com import CapitalComAPI
from app.capital.actions import CapitalAPI
from app.capital.schemas import CapitalMarketResolution, BasicPlaceOrderCapital, CapitalOrderType, CapitalTransactionType
from app.database.db import DBConnection # Import DBConnection
from app.redis.redis import RedisCache # Import RedisCache

# Import websockets library
try:
    import websockets
except ImportError:
    logger.error("Error: websockets library not found. Please install it: pip install websockets")
    sys.exit(1)

# Mock DB and Redis for testing analyse.py instantiation (optional)
class MockDBConnection:
    async def execute(self, *args, **kwargs):
        logger.info(f"Mock DB Execute: {args}, {kwargs}")
        return None
    async def fetch_open_orders(self, *args, **kwargs):
        logger.info(f"Mock DB Fetch Open Orders: {args}, {kwargs}")
        return [] # Return empty list to avoid errors
    # Add other methods used by StockIndicatorCalculator if needed
    async def load_data_from_db(self, *args, **kwargs):
        import pandas as pd
        logger.info(f"Mock DB Load Data: {args}, {kwargs}")
        return pd.DataFrame() # Return empty DataFrame
    async def fetch_stock_ltp_from_db(self, *args, **kwargs):
        logger.info(f"Mock DB Fetch LTP: {args}, {kwargs}")
        return 100.0 # Return a dummy LTP
    async def fetch_pivot_data_from_db(self, *args, **kwargs):
        logger.info(f"Mock DB Fetch Pivot: {args}, {kwargs}")
        return None
    async def insert_pivot_data_to_db(self, *args, **kwargs):
        logger.info(f"Mock DB Insert Pivot: {args}, {kwargs}")
        return None
    async def get_trade_stats(self, *args, **kwargs):
        logger.info(f"Mock DB Get Trade Stats: {args}, {kwargs}")
        return (False, False) # Return dummy stats
    async def check_open_trades_count(self, *args, **kwargs):
        logger.info(f"Mock DB Check Open Trades: {args}, {kwargs}")
        return False
    async def check_loss_trades_count(self, *args, **kwargs):
        logger.info(f"Mock DB Check Loss Trades: {args}, {kwargs}")
        return False
    async def log_trade_to_db(self, *args, **kwargs):
        logger.info(f"Mock DB Log Trade: {args}, {kwargs}")
        return None
    async def update_trade_confirmation(self, *args, **kwargs):
        logger.info(f"Mock DB Update Confirmation: {args}, {kwargs}")
        return None
    async def update_trade_status(self, *args, **kwargs):
        logger.info(f"Mock DB Update Status: {args}, {kwargs}")
        return None
    async def update_sl_and_pl(self, *args, **kwargs):
        logger.info(f"Mock DB Update SL/PL: {args}, {kwargs}")
        return None
    async def square_off_at_eod(self, *args, **kwargs):
        logger.info(f"Mock DB Square Off EOD: {args}, {kwargs}")
        return None
    async def exited_all_orders_for_the_day(self, *args, **kwargs):
        logger.info(f"Mock DB Exited All Orders: {args}, {kwargs}")
        return False

class MockRedisCache:
    def get_key(self, key):
        logger.info(f"Mock Redis Get: {key}")
        return None
    def set_key(self, key, value, ttl=None):
        logger.info(f"Mock Redis Set: {key}, {value}, ttl={ttl}")
        return True
    def key_exists(self, key):
        logger.info(f"Mock Redis Exists: {key}")
        return False
    # Add other methods if needed

async def run_tests():
    logger.info("Starting Capital.com API integration tests...")

    # Verify environment variables are accessible
    api_key = os.getenv("CAPITAL_API_KEY")
    identifier = os.getenv("CAPITAL_API_IDENTIFIER")
    password = os.getenv("CAPITAL_API_PASSWORD")
    base_url = os.getenv("CAPITAL_API_BASE_URL")
    streaming_url = os.getenv("CAPITAL_API_STREAMING_URL")

    if not all([api_key, identifier, password, base_url, streaming_url]):
        logger.error("Error: Missing Capital.com API credentials in environment variables.")
        logger.error("Please ensure CAPITAL_API_KEY, CAPITAL_API_IDENTIFIER, CAPITAL_API_PASSWORD, CAPITAL_API_BASE_URL, and CAPITAL_API_STREAMING_URL are set.")
        return

    logger.info(f"Using Base URL: {base_url}")
    logger.info(f"Using Streaming URL: {streaming_url}")

    # Initialize the clients
    # Assuming settings.py (used by CapitalComAPI) correctly loads from environment variables
    # Use mock DB/Redis for CapitalAPI actions client
    mock_db = MockDBConnection()
    mock_redis = MockRedisCache()
    # actions_client = CapitalAPI(db_conn=mock_db) # Use mock DB for actions client
    # For testing actual API calls, we need the real client but actions.py expects db_conn
    # We will instantiate CapitalAPI but rely on its internal CapitalComAPI client for API calls
    # Logging within CapitalAPI actions will use the mock DB
    actions_api = CapitalAPI(db_conn=mock_db)
    base_client = actions_api.client # Get the underlying CapitalComAPI client

    # Test 1: Authentication (Session Creation via base_client)
    logger.info("\n--- Test 1: Authentication ---")
    session_created = False
    try:
        session_created = await base_client.ensure_session_valid()
        if session_created and base_client.cst and base_client.security_token:
            logger.info("Authentication successful. Session created.")
        else:
            logger.error("Authentication failed. Client did not report success or tokens are missing.")
            return # Stop tests if auth fails
    except Exception as e:
        logger.error(f"Authentication test failed with error: {e}", exc_info=True)
        return # Stop tests if auth fails

    # Test 2: Fetch Historical Data (via actions_api)
    logger.info("\n--- Test 2: Fetch Historical Data (via actions_api) ---")
    test_epic = "US500" # S&P 500
    interval = "5minute"
    today = datetime.utcnow().date()
    start_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    logger.info(f"Fetching {interval} data for {test_epic} from {start_date} to {end_date}...")
    try:
        history_df = await actions_api.get_historical_data(
            epic=test_epic,
            start_date=start_date,
            end_date=end_date,
            interval=interval
        )

        if not history_df.empty:
            logger.info(f"Successfully fetched historical data. Shape: {history_df.shape}")
            logger.info(f"Sample data:\n{history_df.head()}")
        else:
            logger.warning("Failed to fetch historical data or DataFrame is empty.")

    except Exception as e:
        logger.error(f"Historical data test failed with error: {e}", exc_info=True)

    # Test 3: Place and Cancel Working Order (LIMIT BUY)
    logger.info("\n--- Test 3: Place and Cancel Working Order (LIMIT BUY) ---")
    # Use a less volatile EPIC if possible, e.g., EURUSD
    order_epic = "EURUSD"
    # Fetch current price to set a realistic limit level (e.g., 1% below current)
    current_price_info = await base_client.get_market_details(order_epic)
    current_bid = current_price_info.get(	'snapshot	', {}).get(	'bid	')
    limit_price = None
    if current_bid:
        limit_price = round(current_bid * 0.995, 5) # Place limit 0.5% below current bid
        logger.info(f"Current bid for {order_epic}: {current_bid}. Setting limit buy at {limit_price}")
    else:
        logger.warning(f"Could not fetch current price for {order_epic}. Using dummy limit price.")
        limit_price = 1.05000 # Dummy price

    order_payload = BasicPlaceOrderCapital(
        epic=order_epic,
        order_type=CapitalOrderType.LIMIT,
        transaction_type=CapitalTransactionType.BUY,
        quantity=1, # Minimal quantity
        price=limit_price,
        stop_loss=None, # Optional
        profit_level=None # Optional
    )
    
    deal_ref = None
    working_order_id = None
    try:
        logger.info(f"Placing LIMIT BUY order for {order_epic} at {limit_price}...")
        deal_ref = await actions_api.place_order(order_payload)
        if deal_ref:
            logger.info(f"LIMIT order placed successfully. Deal Reference: {deal_ref}")
            # Wait a bit and try to confirm to get working order ID
            await asyncio.sleep(3)
            confirmation = await actions_api.get_confirmation(deal_ref)
            if confirmation and confirmation.get(	'dealStatus	') == 	'ACCEPTED	':
                working_order_id = confirmation.get(	'dealId	') # This is the working order ID
                logger.info(f"Order confirmed. Working Order ID: {working_order_id}")
            else:
                logger.warning(f"Order confirmation not found or not accepted yet. Status: {confirmation}")
        else:
            logger.error("Failed to place LIMIT order.")

    except Exception as e:
        logger.error(f"Placing LIMIT order test failed with error: {e}", exc_info=True)

    # Cancel the working order if ID was obtained
    if working_order_id:
        try:
            logger.info(f"Attempting to cancel working order {working_order_id}...")
            cancel_ref = await actions_api.cancel_working_order(working_order_id)
            if cancel_ref:
                logger.info(f"Cancellation request successful. Deal Reference: {cancel_ref}")
                # Verify cancellation
                await asyncio.sleep(3)
                orders_after_cancel = await actions_api.get_working_orders()
                if not any(o.get(	'workingOrderData	', {}).get(	'dealId	') == working_order_id for o in orders_after_cancel):
                    logger.info(f"Working order {working_order_id} successfully cancelled.")
                else:
                    logger.warning(f"Working order {working_order_id} still appears in list after cancellation attempt.")
            else:
                logger.error(f"Failed to cancel working order {working_order_id}.")
        except Exception as e:
            logger.error(f"Cancelling working order test failed with error: {e}", exc_info=True)
    elif deal_ref:
         logger.warning(f"Could not obtain working order ID from deal reference {deal_ref} to cancel.")

    # Test 4: Place Market Order, Modify SL/TP, Close Position
    logger.info("\n--- Test 4: Place Market Order, Modify, Close ---")
    market_order_epic = "EURUSD" # Use a liquid market
    market_order_payload = BasicPlaceOrderCapital(
        epic=market_order_epic,
        order_type=CapitalOrderType.MARKET,
        transaction_type=CapitalTransactionType.BUY,
        quantity=1, # Minimal quantity
        price=None, # Not needed for market
        stop_loss=None, # Will add later
        profit_level=None # Will add later
    )
    market_deal_ref = None
    position_deal_id = None
    try:
        logger.info(f"Placing MARKET BUY order for {market_order_epic}...")
        market_deal_ref = await actions_api.place_order(market_order_payload)
        if market_deal_ref:
            logger.info(f"MARKET order placed successfully. Deal Reference: {market_deal_ref}")
            # Wait and confirm to get position deal ID
            await asyncio.sleep(5) # Market orders might take slightly longer to confirm
            confirmation = await actions_api.get_confirmation(market_deal_ref)
            if confirmation and confirmation.get(	'dealStatus	') == 	'ACCEPTED	' and confirmation.get(	'status	') == 	'OPEN	':
                position_deal_id = confirmation.get(	'dealId	')
                logger.info(f"Position opened successfully. Position Deal ID: {position_deal_id}")
            else:
                logger.warning(f"Position confirmation not found or not accepted/open yet. Status: {confirmation}")
        else:
            logger.error("Failed to place MARKET order.")

    except Exception as e:
        logger.error(f"Placing MARKET order test failed with error: {e}", exc_info=True)

    # Modify the position if ID was obtained
    if position_deal_id:
        try:
            # Fetch current price again to set SL/TP
            pos_details = await actions_api.get_position_details(position_deal_id)
            entry_price = pos_details.get(	'position	', {}).get(	'level	')
            if entry_price:
                new_sl = round(entry_price * 0.99, 5) # SL 1% below entry
                new_tp = round(entry_price * 1.01, 5) # TP 1% above entry
                logger.info(f"Attempting to modify position {position_deal_id} with SL={new_sl}, TP={new_tp}...")
                modify_ref = await actions_api.modify_position(position_deal_id, stop_level=new_sl, profit_level=new_tp)
                if modify_ref:
                    logger.info(f"Position modification request successful. Deal Reference: {modify_ref}")
                    # Optionally verify modification after confirmation
                else:
                    logger.error(f"Failed to modify position {position_deal_id}.")
            else:
                logger.warning(f"Could not get entry price for position {position_deal_id} to set SL/TP.")
        except Exception as e:
            logger.error(f"Modifying position test failed with error: {e}", exc_info=True)
        
        # Close the position
        try:
            logger.info(f"Attempting to close position {position_deal_id}...")
            close_ref = await actions_api.exit_position(position_deal_id)
            if close_ref:
                logger.info(f"Position closure request successful. Deal Reference: {close_ref}")
                # Verify closure
                await asyncio.sleep(5)
                pos_after_close = await actions_api.get_position_details(position_deal_id)
                # Check if position is gone or status is CLOSED (API might return closed positions for a while)
                if not pos_after_close or pos_after_close.get(	'position	', {}).get(	'status	') == 	'CLOSED	': # Check actual status field
                     logger.info(f"Position {position_deal_id} successfully closed.")
                else:
                     logger.warning(f"Position {position_deal_id} still appears open after closure attempt. Details: {pos_after_close}")
            else:
                logger.error(f"Failed to close position {position_deal_id}.")
        except Exception as e:
            logger.error(f"Closing position test failed with error: {e}", exc_info=True)
            
    elif market_deal_ref:
        logger.warning(f"Could not obtain position deal ID from market order reference {market_deal_ref} to modify/close.")

    # Test 5: Instantiate StockIndicatorCalculator (Basic Check)
    logger.info("\n--- Test 5: Instantiate StockIndicatorCalculator ---")
    try:
        # Import late to ensure other tests run first
        from app.analyse.analyse import StockIndicatorCalculator
        # Need mock DB and Redis
        calculator = StockIndicatorCalculator(db_connection=mock_db, redis_cache=mock_redis, test_mode=True)
        logger.info("StockIndicatorCalculator instantiated successfully with mock DB/Redis.")
        # Optional: Call a simple method that uses the capital_client
        # e.g., await calculator.capital_client.get_account_details()
        # Note: This requires get_capital_epics in instruments.py to exist and work
        # logger.info(f"Calculator EPICs: {calculator.epics}")
    except ImportError as e:
         logger.error(f"Failed to import StockIndicatorCalculator or dependencies: {e}")
    except Exception as e:
        logger.error(f"Instantiating StockIndicatorCalculator failed with error: {e}", exc_info=True)

    # Test 6: WebSocket Connection (from base_client)
    # This test remains largely the same as before, using base_client
    logger.info("\n--- Test 6: WebSocket Connection (via base_client) ---")
    websocket = None
    try:
        logger.info(f"Attempting WebSocket connection to {base_client.streaming_url}...")
        websocket = await asyncio.wait_for(websockets.connect(base_client.streaming_url), timeout=15)
        logger.info("WebSocket connection initiated.")
        
        auth_message = {
            "destination": "session.logon",
            "cst": base_client.cst,
            "securityToken": base_client.security_token,
            "correlationId": str(uuid.uuid4())
        }
        logger.info("Sending authentication message...")
        await websocket.send(json.dumps(auth_message))
        
        logger.info("Waiting for authentication response...")
        auth_response_str = await asyncio.wait_for(websocket.recv(), timeout=15)
        auth_response = json.loads(auth_response_str)
        logger.info(f"WebSocket Auth Response: {auth_response}")
        
        if auth_response.get("status") == "OK":
            logger.info("WebSocket connection and authentication successful.")
            # Optional: Subscribe and receive data
        else:
            logger.error("WebSocket authentication failed based on response status.")
            
    except asyncio.TimeoutError:
        logger.error("WebSocket test timed out waiting for connection or response.")
    except websockets.exceptions.ConnectionClosedOK:
        logger.info("WebSocket connection closed normally.")
    except websockets.exceptions.ConnectionClosedError as e:
        logger.error(f"WebSocket connection closed with error: {e}")
    except Exception as e:
        logger.error(f"WebSocket test failed with error: {e}", exc_info=True)
    finally:
        if websocket and not websocket.closed:
            await websocket.close()
            logger.info("WebSocket connection closed.")

    logger.info("\nIntegration tests finished.")

# Run the async function
if __name__ == "__main__":
    # Ensure event loop is managed correctly
    try:
        asyncio.run(run_tests())
    except KeyboardInterrupt:
        logger.info("Test execution interrupted by user.")
