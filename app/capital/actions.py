# Enhanced Capital.com API actions
import asyncio
import json
import os
from typing import Any, List, Optional, Dict
from app.database.db import DBConnection
from app.capital.capital_com import CapitalComAPI
from app.capital.schemas import BasicPlaceOrderCapital, CapitalOrderType, CapitalTransactionType, CapitalMarketResolution
from datetime import datetime
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class CapitalAPI:
    def __init__(self, db_conn: DBConnection):
        self.db_conn: DBConnection = db_conn
        self.enable_capital_call = os.getenv("ENABLE_CAPITAL_CALL", "False").lower() == "true"
        self.client = CapitalComAPI()

    async def log_request_response(self, endpoint: str, request_payload: dict, response_payload: dict, status_code: int):
        """Logs API request and response to the database."""
        # Ensure the capital_log table exists via migrations.sql
        # Example: CREATE TABLE IF NOT EXISTS capital_log (...);
        try:
            await self.db_conn.execute(
                """
                INSERT INTO capital_log (endpoint, request_payload, response_payload, status_code)
                VALUES (%s, %s::json, %s::json, %s)
                """,
                (
                    endpoint,
                    json.dumps(request_payload),
                    json.dumps(response_payload),
                    status_code,
                ),
            )
            logger.debug(f"Logged API interaction for endpoint: {endpoint}")
        except Exception as e:
            logger.error(f"Error logging Capital API interaction: {e}")

    def _map_interval_to_resolution(self, interval: str) -> CapitalMarketResolution:
        """Maps aksyo-wiz interval strings to Capital.com resolution enums."""
        mapping = {
            "1minute": CapitalMarketResolution.MINUTE,
            "5minute": CapitalMarketResolution.MINUTE_5,
            "15minute": CapitalMarketResolution.MINUTE_15,
            "30minute": CapitalMarketResolution.MINUTE_30,
            "hour": CapitalMarketResolution.HOUR,
            "day": CapitalMarketResolution.DAY,
            "week": CapitalMarketResolution.WEEK,
            "month": CapitalMarketResolution.MONTH,
        }
        resolution = mapping.get(interval.lower())
        if not resolution:
            logger.warning(f"Unsupported interval 	'{interval}	'. Defaulting to MINUTE.")
            return CapitalMarketResolution.MINUTE
        return resolution

    def _transform_historical_data(self, prices: List[Dict], epic: str) -> pd.DataFrame:
        """Transforms Capital.com price history list into a pandas DataFrame [timestamp, open, high, low, close, volume]."""
        records = []
        for price_point in prices:
            # Use mid-price (average of bid and ask) or just bid/ask? Using bid for consistency example.
            # Timestamps need parsing. Assuming UTC format like 'YYYY-MM-DDTHH:MM:SS'
            try:
                ts_str = price_point.get(	'snapshotTimeUTC	')
                # Attempt to parse with or without milliseconds
                try:
                    ts = pd.to_datetime(ts_str, format=	'%Y-%m-%dT%H:%M:%S.%f	', errors=	'raise	')
                except ValueError:
                    ts = pd.to_datetime(ts_str, format=	'%Y-%m-%dT%H:%M:%S	', errors=	'raise	')
                
                # Ensure timezone is handled (Capital.com uses UTC)
                # Convert to naive datetime if analyse.py expects it, or keep tz-aware
                ts = ts.tz_localize(None) # Making naive for compatibility with original code

                o = price_point.get(	'openPrice	', {}).get(	'bid	')
                h = price_point.get(	'highPrice	', {}).get(	'bid	')
                l = price_point.get(	'lowPrice	', {}).get(	'bid	')
                c = price_point.get(	'closePrice	', {}).get(	'bid	')
                v = price_point.get(	'lastTradedVolume	')

                if all(val is not None for val in [ts, o, h, l, c, v]):
                    records.append([ts, o, h, l, c, v])
                else:
                    logger.warning(f"Skipping incomplete data point for {epic} at {ts_str}")
            except Exception as e:
                logger.error(f"Error parsing price point for {epic}: {price_point} - Error: {e}")
                continue

        if not records:
            return pd.DataFrame(columns=[	'timestamp	', 	'open	', 	'high	', 	'low	', 	'close	', 	'volume	'])

        df = pd.DataFrame(records, columns=[	'timestamp	', 	'open	', 	'high	', 	'low	', 	'close	', 	'volume	'])
        df = df.sort_values(by=	'timestamp	').reset_index(drop=True)
        # Add 'stock' and 'ltp' columns as expected by analyse.py
        df[	'stock	'] = epic
        df[	'ltp	'] = df[	'close	'] # Use close price as LTP for historical data
        # Add other columns if needed, e.g., open_interest (usually 0 for non-futures)
        df[	'open_interest	'] = 0 
        df = df[[	'stock	', 	'open	', 	'high	', 	'low	', 	'close	', 	'volume	', 	'open_interest	', 	'timestamp	', 	'ltp	']]
        return df

    async def get_historical_data(self, epic: str, start_date: str, end_date: str, interval: str = 	'1minute	') -> pd.DataFrame:
        """Fetch and transform historical price data from Capital.com API."""
        resolution = self._map_interval_to_resolution(interval)
        # Ensure start_date and end_date are in the correct format (e.g., YYYY-MM-DDTHH:MM:SS)
        # Assuming input dates are strings like 'YYYY-MM-DD'
        start_dt_str = f"{start_date}T00:00:00"
        end_dt_str = f"{end_date}T23:59:59"
        
        request_payload = {
            "epic": epic,
            "resolution": resolution.value,
            "from": start_dt_str,
            "to": end_dt_str,
            "max": 1000 # Adjust max_bars as needed, Capital.com might have limits
        }
        
        try:
            response = await self.client.get_price_history(**request_payload)
            status_code = 200 # Assuming success if no exception
            # await self.log_request_response(f"/api/v1/prices/{epic}", request_payload, response, status_code)
            
            prices = response.get(	'prices	', [])
            if not prices:
                logger.warning(f"No historical prices returned for {epic} in the specified range.")
                return pd.DataFrame()
                
            df = self._transform_historical_data(prices, epic)
            return df
            
        except Exception as e:
            logger.error(f"Error fetching historical data for {epic}: {e}")
            # await self.log_request_response(f"/api/v1/prices/{epic}", request_payload, {"error": str(e)}, 500)
            return pd.DataFrame()

    async def get_current_trading_day_data(self, epic: str, interval: str = 	'1minute	') -> pd.DataFrame:
        """Fetch intraday data for the current day."""
        # Use get_historical_data with today's date range
        today_str = datetime.utcnow().strftime(	'%Y-%m-%d	')
        logger.info(f"Fetching current trading day data for {epic} ({today_str}) with interval {interval}")
        return await self.get_historical_data(epic, today_str, today_str, interval)

    def get_market_holidays(self):
        """Fetch market holidays. Capital.com might not have a direct equivalent. Find alternative source or omit."""
        # Capital.com API docs don't explicitly list a holiday endpoint.
        # May need to fetch from another source (e.g., NSE website) or handle trading times directly.
        logger.warning("Market holiday endpoint not directly available in Capital.com API. Consider alternative sources.")
        return []

    def _map_order_to_position_payload(self, basic_order: BasicPlaceOrderCapital) -> dict:
        """Maps the internal order schema to the Capital.com API payload for creating a position (MARKET order)."""
        payload = {
            "epic": basic_order.epic,
            "direction": basic_order.transaction_type.value, # BUY/SELL
            "size": basic_order.quantity,
            "guaranteedStop": False, # Default, can be made configurable
            "forceOpen": True # Ensures it opens a new position
        }
        if basic_order.stop_loss:
            payload["stopLevel"] = basic_order.stop_loss
        if basic_order.profit_level:
            payload["profitLevel"] = basic_order.profit_level
        # Add trailingStop, stopDistance if needed
        return payload

    def _map_order_to_working_order_payload(self, basic_order: BasicPlaceOrderCapital) -> dict:
        """Maps the internal order schema to the Capital.com API payload for creating a working order (LIMIT/STOP)."""
        payload = {
            "epic": basic_order.epic,
            "direction": basic_order.transaction_type.value,
            "size": basic_order.quantity,
            "level": basic_order.price, # The price for LIMIT/STOP
            "type": basic_order.order_type.value, # LIMIT or STOP
            "goodTillDate": None, # Or set an expiry, e.g., (datetime.utcnow() + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
            "guaranteedStop": False,
        }
        if basic_order.stop_loss:
            payload["stopLevel"] = basic_order.stop_loss
        if basic_order.profit_level:
            payload["profitLevel"] = basic_order.profit_level
        # Add stopDistance if needed for STOP orders
        return payload

    async def place_order(self, basic_order: BasicPlaceOrderCapital) -> Optional[str]:
        """Place an order (MARKET, LIMIT, or STOP) via Capital.com API. Returns dealReference."""
        if not self.enable_capital_call:
            logger.info("Capital.com API calls disabled.")
            return None

        try:
            if basic_order.order_type == CapitalOrderType.MARKET:
                endpoint = "/api/v1/positions/otc" # OTC endpoint for market orders
                payload = self._map_order_to_position_payload(basic_order)
                response = await self.client.post(endpoint, data=payload)
            elif basic_order.order_type in [CapitalOrderType.LIMIT, CapitalOrderType.STOP]:
                endpoint = "/api/v1/workingorders/otc" # OTC endpoint for working orders
                payload = self._map_order_to_working_order_payload(basic_order)
                response = await self.client.post(endpoint, data=payload)
            else:
                logger.error(f"Unsupported order type: {basic_order.order_type}")
                return None

            status_code = 200 # Assuming success if no exception
            # await self.log_request_response(endpoint, payload, response, status_code)
            
            deal_reference = response.get("dealReference")
            if deal_reference:
                logger.info(f"Placed order request for {basic_order.epic} ({basic_order.order_type.value}), reference: {deal_reference}")
                return deal_reference
            else:
                logger.error(f"Order placement for {basic_order.epic} succeeded but no dealReference received. Response: {response}")
                return None

        except Exception as e:
            logger.error(f"Error placing order for {basic_order.epic}: {e}")
            # await self.log_request_response(endpoint, payload, {"error": str(e)}, 500)
            return None

    async def modify_working_order(self, deal_id: str, updates: Dict[str, Any]) -> Optional[str]:
        """Modify an existing working order (LIMIT/STOP). Returns dealReference."""
        if not self.enable_capital_call:
            logger.info("Capital.com API calls disabled.")
            return None
        
        endpoint = f"/api/v1/workingorders/otc/{deal_id}"
        payload = updates # e.g., {"level": new_price, "size": new_size, "stopLevel": new_sl}
        
        try:
            response = await self.client.put(endpoint, data=payload)
            status_code = 200
            # await self.log_request_response(endpoint, payload, response, status_code)
            deal_reference = response.get("dealReference")
            logger.info(f"Modified working order {deal_id}, reference: {deal_reference}")
            return deal_reference
        except Exception as e:
            logger.error(f"Error modifying working order {deal_id}: {e}")
            # await self.log_request_response(endpoint, payload, {"error": str(e)}, 500)
            return None

    async def cancel_working_order(self, deal_id: str) -> Optional[str]:
        """Cancel an existing working order (LIMIT/STOP). Returns dealReference."""
        if not self.enable_capital_call:
            logger.info("Capital.com API calls disabled.")
            return None
            
        endpoint = f"/api/v1/workingorders/otc/{deal_id}"
        payload = {} # No payload needed for delete
        
        try:
            response = await self.client.delete(endpoint)
            status_code = 200
            # await self.log_request_response(endpoint, payload, response, status_code)
            deal_reference = response.get("dealReference")
            logger.info(f"Cancelled working order {deal_id}, reference: {deal_reference}")
            return deal_reference
        except Exception as e:
            logger.error(f"Error cancelling working order {deal_id}: {e}")
            # await self.log_request_response(endpoint, payload, {"error": str(e)}, 500)
            return None

    async def get_working_orders(self) -> List[Dict]:
        """Get all active working orders."""
        endpoint = "/api/v1/workingorders"
        try:
            response = await self.client.get(endpoint)
            orders = response.get(	'workingOrders	', [])
            logger.debug(f"Fetched {len(orders)} working orders.")
            return orders
        except Exception as e:
            logger.error(f"Error fetching working orders: {e}")
            return []

    async def get_open_positions(self) -> List[Dict]:
        """Get all open positions."""
        endpoint = "/api/v1/positions"
        try:
            response = await self.client.get(endpoint)
            positions = response.get(	'positions	', [])
            logger.debug(f"Fetched {len(positions)} open positions.")
            return positions
        except Exception as e:
            logger.error(f"Error fetching open positions: {e}")
            return []

    async def modify_position(self, deal_id: str, stop_level: Optional[float] = None, profit_level: Optional[float] = None) -> Optional[str]:
        """Modify stop loss or take profit for an open position. Returns dealReference."""
        if not self.enable_capital_call:
            logger.info("Capital.com API calls disabled.")
            return None

        endpoint = f"/api/v1/positions/{deal_id}"
        payload = {}
        if stop_level is not None:
            payload["stopLevel"] = stop_level
        if profit_level is not None:
            payload["profitLevel"] = profit_level
        
        if not payload:
            logger.warning(f"No updates provided for modifying position {deal_id}")
            return None
            
        try:
            response = await self.client.put(endpoint, data=payload)
            status_code = 200
            # await self.log_request_response(endpoint, payload, response, status_code)
            deal_reference = response.get("dealReference")
            logger.info(f"Modified position {deal_id}, reference: {deal_reference}")
            return deal_reference
        except Exception as e:
            logger.error(f"Error modifying position {deal_id}: {e}")
            # await self.log_request_response(endpoint, payload, {"error": str(e)}, 500)
            return None

    async def exit_position(self, deal_id: str, size: Optional[float] = None, direction: Optional[CapitalTransactionType] = None) -> Optional[str]:
        """Close an open position using its deal ID. Returns dealReference."""
        # Note: Capital.com uses DELETE /api/v1/positions/{dealId} but requires payload
        if not self.enable_capital_call:
            logger.info("Capital.com API calls disabled.")
            return None
        
        endpoint = f"/api/v1/positions/otc" # Use OTC endpoint for closing
        payload = {
            "dealId": deal_id,
            # Direction and size might be needed depending on API version/requirements for partial close
            # "direction": direction.value if direction else None, # Opposite of position direction for closing
            # "size": size, # Size to close (omit for full close)
            "orderType": "MARKET" # Usually market order to close
        }
        # Filter out None values from payload if API doesn't accept them
        payload = {k: v for k, v in payload.items() if v is not None}
        
        try:
            # Capital.com uses DELETE with a payload for closing positions
            # Need to adjust the http_client in capital_com.py to support DELETE with body or use POST to a close endpoint if available
            # Assuming client.delete can handle payload or there's an alternative endpoint
            # Let's try using POST to /positions/otc with dealId as per some docs
            response = await self.client.post(endpoint, data=payload) # Using POST based on potential API structure
            
            status_code = 200
            # await self.log_request_response(endpoint, payload, response, status_code)
            deal_reference = response.get("dealReference")
            logger.info(f"Closed position request for deal ID: {deal_id}, reference: {deal_reference}")
            return deal_reference
        except Exception as e:
            logger.error(f"Error closing position {deal_id}: {e}")
            # await self.log_request_response(endpoint, payload, {"error": str(e)}, 500)
            return None

    async def get_confirmation(self, deal_reference: str) -> Optional[dict]:
        """Get the confirmation status of a deal using its reference."""
        endpoint = f"/api/v1/confirms/{deal_reference}"
        try:
            response = await self.client.get(endpoint)
            status_code = 200
            # await self.log_request_response(endpoint, {}, response, status_code)
            logger.debug(f"Fetched confirmation for reference: {deal_reference}")
            # Check response status, e.g., response.get('dealStatus') == 'ACCEPTED'
            return response
        except Exception as e:
            # Handle 404 if confirmation not found yet
            if hasattr(e, 	'response	') and e.response.status_code == 404:
                logger.debug(f"Confirmation for {deal_reference} not found yet.")
                return None
            logger.error(f"Error getting confirmation for {deal_reference}: {e}")
            # await self.log_request_response(endpoint, {}, {"error": str(e)}, 500)
            return None

    async def get_position_details(self, deal_id: str) -> Optional[dict]:
        """Get details of a specific open position by its deal ID."""
        try:
            positions = await self.get_open_positions()
            for p in positions:
                if p.get(	'position	', {}).get(	'dealId	') == deal_id:
                    logger.debug(f"Found details for position {deal_id}")
                    return p
            logger.warning(f"Position with deal ID {deal_id} not found among open positions.")
            return None
        except Exception as e:
            logger.error(f"Error getting details for position {deal_id}: {e}")
            return None

    async def exit_all_positions(self) -> List[str]:
        """Closes all open positions. Returns list of dealReferences for close requests."""
        closed_references = []
        try:
            positions = await self.get_open_positions()
            if not positions:
                logger.info("No open positions to close.")
                return []
                
            logger.info(f"Attempting to close {len(positions)} open positions.")
            for p_data in positions:
                deal_id = p_data.get(	'position	', {}).get(	'dealId	')
                if deal_id:
                    ref = await self.exit_position(deal_id)
                    if ref:
                        closed_references.append(ref)
                    await asyncio.sleep(0.1) # Small delay between close requests
            logger.info(f"Requested closure for {len(closed_references)} positions.")
            return closed_references
        except Exception as e:
            logger.error(f"Error closing all positions: {e}")
            return closed_references

    # exit_all_positions_for_a_tag: This requires a mapping strategy as Capital.com doesn't have tags.
    # Could be implemented by filtering positions based on EPIC or other metadata if stored during placement.
    async def exit_positions_by_epic(self, epic: str) -> List[str]:
        """Closes all open positions for a specific EPIC. Returns list of dealReferences."""
        closed_references = []
        try:
            positions = await self.get_open_positions()
            positions_to_close = [p for p in positions if p.get(	'market	', {}).get(	'epic	') == epic]
            
            if not positions_to_close:
                logger.info(f"No open positions found for EPIC {epic}.")
                return []
                
            logger.info(f"Attempting to close {len(positions_to_close)} positions for EPIC {epic}.")
            for p_data in positions_to_close:
                deal_id = p_data.get(	'position	', {}).get(	'dealId	')
                if deal_id:
                    ref = await self.exit_position(deal_id)
                    if ref:
                        closed_references.append(ref)
                    await asyncio.sleep(0.1)
            logger.info(f"Requested closure for {len(closed_references)} positions for EPIC {epic}.")
            return closed_references
        except Exception as e:
            logger.error(f"Error closing positions for EPIC {epic}: {e}")
            return closed_references

    async def get_account_details(self) -> List[Dict]:
        """Get details for all accounts associated with the API key."""
        endpoint = "/api/v1/accounts"
        try:
            response = await self.client.get(endpoint)
            accounts = response.get(	'accounts	', [])
            logger.debug(f"Fetched details for {len(accounts)} accounts.")
            return accounts
        except Exception as e:
            logger.error(f"Error fetching account details: {e}")
            return []


