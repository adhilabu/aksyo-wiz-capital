# Enhanced Capital.com API actions
import asyncio
import json
import os
from typing import Any, List, Optional, Dict

import pytz
from app.analyse.schemas import CapitalMarketDetails
from app.database.db import DBConnection
from app.capital.capital_com import CapitalComAPI
from app.capital.schemas import BasicPlaceOrderCapital, CapitalOrderType, CapitalTransactionType, CapitalMarketResolution
from datetime import datetime
import pandas as pd
import logging
import yfinance as yf

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
                VALUES ($1, $2::json, $3::json, $4)
                """,
                endpoint,
                json.dumps(request_payload),
                json.dumps(response_payload),
                status_code,
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
            try:
                ts_str = price_point.get('snapshotTimeUTC')
                try:
                    ts = pd.to_datetime(ts_str, format='%Y-%m-%dT%H:%M:%S.%f', errors='raise')
                except ValueError:
                    ts = pd.to_datetime(ts_str, format='%Y-%m-%dT%H:%M:%S', errors='raise')
                
                # Convert to IST timezone
                ts = ts.tz_localize('UTC').tz_convert('Asia/Kolkata')
                ts = ts.tz_localize(None) # Making naive for compatibility with original code

                o = price_point.get('openPrice', {}).get('bid')
                h = price_point.get('highPrice', {}).get('bid')
                l = price_point.get('lowPrice', {}).get('bid')
                c = price_point.get('closePrice', {}).get('bid')
                v = price_point.get('lastTradedVolume')

                if all(val is not None for val in [ts, o, h, l, c, v]):
                    records.append([ts, o, h, l, c, v])
                else:
                    logger.warning(f"Skipping incomplete data point for {epic} at {ts_str}")
            except Exception as e:
                logger.error(f"Error parsing price point for {epic}: {price_point} - Error: {e}")
                continue

        if not records:
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        df = pd.DataFrame(records, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df = df.sort_values(by='timestamp').reset_index(drop=True)
        # Add 'stock' and 'ltp' columns as expected by analyse.py
        df['stock'] = epic
        df['ltp'] = df['close'] # Use close price as LTP for historical data
        # Add other columns if needed, e.g., open_interest (usually 0 for non-futures)
        df['open_interest'] = 0 
        df = df[['stock', 'open', 'high', 'low', 'close', 'volume', 'open_interest', 'timestamp', 'ltp']]
        return df

    async def get_recent_data(self, epic: str, start_dt_str: str, end_dt_str: str, interval: str = 	'1minute') -> pd.DataFrame:
        """Fetch and transform recent price data from Capital.com API."""
        resolution = self._map_interval_to_resolution(interval)
        
        request_payload = {
            "epic": epic,
            "resolution": resolution.value,
            "from_date": start_dt_str,
            "to_date": end_dt_str,
            "max_bars": 1000 # Adjust max_bars as needed, Capital.com might have limits
        }
        
        try:
            response = await self.client.get_price_history(**request_payload)
            
            prices = response.get('prices', [])
            if not prices:
                logger.warning(f"No historical prices returned for {epic} in the specified range.")
                return pd.DataFrame()
                
            df = self._transform_historical_data(prices, epic)
            return df
            
        except Exception as e:
            logger.error(f"Error fetching historical data for {epic}: {e}")
            return pd.DataFrame()

    async def get_current_trading_day_data(self, epic: str, interval: str = '1minute') -> pd.DataFrame:
        """Fetch intraday data for the current day."""
        end_dt_str = datetime.now(pytz.UTC).replace(second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S')
        start_dt_str = (datetime.now(pytz.UTC) - pd.Timedelta(minutes=999)).replace(second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S')
        logger.info(f"Fetching current trading day data for {epic} ({end_dt_str}) - ({start_dt_str}) with interval {interval}")
        return await self.get_recent_data(epic, start_dt_str, end_dt_str, interval)

    def _map_order_to_position_payload(self, basic_order: BasicPlaceOrderCapital) -> dict:
        """Maps the internal order schema to the Capital.com API payload for creating a position (MARKET order)."""
        payload = {
            "epic": basic_order.epic,
            "direction": basic_order.transaction_type.value, # BUY/SELL
            "size": basic_order.quantity,
            "guaranteedStop": False, # Set to True if guaranteed stop is needed
        }
        if basic_order.stop_distance:
            payload["stopDistance"] = basic_order.stop_distance
        if basic_order.profit_distance:
            payload["profitDistance"] = basic_order.profit_distance
        # if basic_order.stop_loss:
        #     payload["stopLevel"] = basic_order.stop_loss
        # if basic_order.profit_level:
        #     payload["profitLevel"] = basic_order.profit_level
        # Add trailingStop, stopDistance if needed
        return payload

    def _map_order_to_working_order_payload(self, basic_order: BasicPlaceOrderCapital) -> dict:
        """Maps the internal order schema to the Capital.com API payload for creating a working order (LIMIT/STOP)."""
        payload = {
            "epic": basic_order.epic,
            "direction": basic_order.transaction_type.value,
            "size": basic_order.quantity,
            "level": basic_order.price, # The price for LIMIT/STOP
            "type": basic_order.order_type.value,
            "guaranteedStop": False,
        }
        if basic_order.stop_distance:
            payload["stopDistance"] = basic_order.stop_distance
        if basic_order.profit_distance:
            payload["profitDistance"] = basic_order.profit_distance
        # if basic_order.stop_loss:
        #     payload["stopLevel"] = basic_order.stop_loss
        # if basic_order.profit_level:
        #     payload["profitLevel"] = basic_order.profit_level
        return payload

    async def place_order(self, basic_order: BasicPlaceOrderCapital) -> Optional[str]:
        """Place an order (MARKET, LIMIT, or STOP) via Capital.com API. Returns dealReference."""
        if not self.enable_capital_call:
            logger.info("Capital.com API calls disabled.")
            return None

        try:
            if basic_order.order_type == CapitalOrderType.MARKET:
                endpoint = "/api/v1/positions" # OTC endpoint for market orders
                payload = self._map_order_to_position_payload(basic_order)
                response = await self.client.post(endpoint, data=payload)
            elif basic_order.order_type in [CapitalOrderType.LIMIT, CapitalOrderType.STOP]:
                endpoint = "/api/v1/workingorders" # OTC endpoint for working orders
                payload = self._map_order_to_working_order_payload(basic_order)
                response = await self.client.post(endpoint, data=payload)
            else:
                logger.error(f"Unsupported order type: {basic_order.order_type}")
                return None

            status_code = 200 # Assuming success if no exception
            await self.log_request_response(endpoint, payload, response, status_code)
            
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
            accounts = response.get('accounts', [])
            logger.debug(f"Fetched details for {len(accounts)} accounts.")
            return accounts
        except Exception as e:
            logger.error(f"Error fetching account details: {e}")
            return []

    def get_historical_data(self, instruments):
        """
        Fetches 1-minute historical candle data for the given list of instruments using yfinance.
        
        Args:
            instruments (str): Comma-separated string of instrument symbols.
            
        Returns:
            dict: A dictionary where keys are instrument symbols and values are pandas DataFrames with OHLC data.
        """
        symbol_map = {
            'ETHUSD': 'ETH-USD',
            'EURUSD': 'EURUSD=X',
            'USDJPY': 'USDJPY=X',
            'J225': '^N225',
            'US30': '^DJI',
            'BTCUSD': 'BTC-USD'
        }
        
        data = {}
        for symbol in instruments:
            # Get the corresponding Yahoo Finance ticker symbol
            ticker = symbol_map.get(symbol, symbol)  # Fallback to the original symbol if not found
            
            try:
                # Fetch 1-minute data for the last 7 days (max allowed by Yahoo Finance for 1m interval)
                df = yf.download(ticker, period='7d', interval='1m')
                if df.empty:
                    print(f"No data found for {symbol} (ticker: {ticker})")
                else:
                    data[symbol] = df
            except Exception as e:
                print(f"Error fetching data for {symbol} (ticker: {ticker}): {e}")
        
        return data

    async def get_instrument_details(self, epic: str) -> Optional[CapitalMarketDetails]:        
        # Fetch from Capital API
        instrument_data = await self.client.fetch_epic_market_details(epic)
        if not instrument_data:
            return None

        dealing_rules = instrument_data.get('dealingRules', {})
        snapshot = instrument_data.get('snapshot', {})
        capital_market_details = CapitalMarketDetails(
            epic=epic,
            min_step_distance=dealing_rules.get('minStepDistance', {}).get('value'),
            min_step_distance_unit=dealing_rules.get('minStepDistance', {}).get('unit'),
            min_deal_size=dealing_rules.get('minDealSize', {}).get('value'),
            min_deal_size_unit=dealing_rules.get('minDealSize', {}).get('unit'),
            max_deal_size=dealing_rules.get('maxDealSize', {}).get('value'),
            max_deal_size_unit=dealing_rules.get('maxDealSize', {}).get('unit'),
            min_size_increment=dealing_rules.get('minSizeIncrement', {}).get('value'),
            min_size_increment_unit=dealing_rules.get('minSizeIncrement', {}).get('unit'),
            min_guaranteed_stop_distance=dealing_rules.get('minGuaranteedStopDistance', {}).get('value'),
            min_guaranteed_stop_distance_unit=dealing_rules.get('minGuaranteedStopDistance' , {}).get('unit'),
            min_stop_or_profit_distance=dealing_rules.get('minStopOrProfitDistance', {}).get('value'),
            min_stop_or_profit_distance_unit=dealing_rules.get('minStopOrProfitDistance', {}).get('unit'),
            max_stop_or_profit_distance=dealing_rules.get('maxStopOrProfitDistance', {}).get('value'),
            max_stop_or_profit_distance_unit=dealing_rules.get('maxStopOrProfitDistance', {}).get('unit'),
            decimal_places=snapshot.get('decimalPlaces'),
            margin_factor=snapshot.get('scalingFactor')
        )

        # Save to database with ON CONFLICT DO UPDATE
        await self.db_conn.execute(
            """
            INSERT INTO capital_market_details 
            (epic, min_step_distance, min_step_distance_unit, min_deal_size, 
            min_deal_size_unit, max_deal_size, max_deal_size_unit, min_size_increment,
            min_size_increment_unit, min_guaranteed_stop_distance, 
            min_guaranteed_stop_distance_unit, min_stop_or_profit_distance,
            min_stop_or_profit_distance_unit, max_stop_or_profit_distance,
            max_stop_or_profit_distance_unit, decimal_places, margin_factor)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (epic) DO UPDATE SET
                min_step_distance = EXCLUDED.min_step_distance,
                min_step_distance_unit = EXCLUDED.min_step_distance_unit,
                min_deal_size = EXCLUDED.min_deal_size,
                min_deal_size_unit = EXCLUDED.min_deal_size_unit,
                max_deal_size = EXCLUDED.max_deal_size,
                max_deal_size_unit = EXCLUDED.max_deal_size_unit,
                min_size_increment = EXCLUDED.min_size_increment,
                min_size_increment_unit = EXCLUDED.min_size_increment_unit,
                min_guaranteed_stop_distance = EXCLUDED.min_guaranteed_stop_distance,
                min_guaranteed_stop_distance_unit = EXCLUDED.min_guaranteed_stop_distance_unit,
                min_stop_or_profit_distance = EXCLUDED.min_stop_or_profit_distance,
                min_stop_or_profit_distance_unit = EXCLUDED.min_stop_or_profit_distance_unit,
                max_stop_or_profit_distance = EXCLUDED.max_stop_or_profit_distance,
                max_stop_or_profit_distance_unit = EXCLUDED.max_stop_or_profit_distance_unit,
                decimal_places = EXCLUDED.decimal_places,
                margin_factor = EXCLUDED.margin_factor
            """,
            capital_market_details.epic,
            capital_market_details.min_step_distance,
            capital_market_details.min_step_distance_unit,
            capital_market_details.min_deal_size,
            capital_market_details.min_deal_size_unit,
            capital_market_details.max_deal_size,
            capital_market_details.max_deal_size_unit,
            capital_market_details.min_size_increment,
            capital_market_details.min_size_increment_unit,
            capital_market_details.min_guaranteed_stop_distance,
            capital_market_details.min_guaranteed_stop_distance_unit,
            capital_market_details.min_stop_or_profit_distance,
            capital_market_details.min_stop_or_profit_distance_unit,
            capital_market_details.max_stop_or_profit_distance,
            capital_market_details.max_stop_or_profit_distance_unit,
            capital_market_details.decimal_places,
            capital_market_details.margin_factor
        )

        return capital_market_details
    
    async def get_deal_reference_status(self, deal_reference):
        deal_ref_data = await self.client.get_confirmation(deal_reference)
        deal_status = deal_ref_data.get("status")
        working_order_id = deal_ref_data.get("dealId")
        # If order isn't filled, return status
        if working_order_id:
            return {"order_status": True, "status": deal_status, "working_order_id": working_order_id}
        return {"order_status": False, "status": deal_status}
    
    async def get_position_status_working_id(self, working_id):
        """Fetch position status and profit/loss details."""
        positions = await self.client.get_positions()
        if not positions:
            print("No positions found.")
            return None

        positions_list = positions.get("positions", [])
        if not positions_list:
            print(f"Position not found for ID: {working_id}")
            position_status = "CLOSED"
            profit_loss = 0
            pl_status = "UNKNOWN"
            deal_id = None
            return {
                "position_status": position_status,
                "profit_loss_status": pl_status,
                "profit_loss_amount": profit_loss,
                "deal_id": deal_id
            }
        
        position_data = next((p for p in positions_list if p.get("position", {}).get("workingOrderId") == working_id), None)
        if not position_data:
            print(f"Position not found for ID: {working_id}")
            position_status = "CLOSED"
            profit_loss = 0
            pl_status = "UNKNOWN"
            deal_id = None
            return {
                "position_status": position_status,
                "profit_loss_status": pl_status,
                "profit_loss_amount": profit_loss,
                "deal_id": deal_id
            }
        
        position_status = position_data.get("market", {}).get('marketStatus')
        deal_id = position_data.get("position", {}).get("dealId")
        # If position is closed, return status
        profit_loss = position_data.get('position', {}).get("upl", 0)        
        pl_status = "PROFIT" if profit_loss > 0 else "LOSS" if profit_loss < 0 else "BREAKEVEN"
        if position_status not in ["TRADEABLE", "CLOSED"]:
            print(f"Position doesn't exist or is closed for ID: {working_id}")
            return {
                "position_status": position_status,
                "profit_loss_status": pl_status,
                "profit_loss_amount": profit_loss,
                "deal_id": deal_id
            }
        
        # Step 4: Determine profit/loss (adjust field name as per API)

        if position_status == 'CLOSED':
            return {
                "position_status": position_status,
                "profit_loss_status": pl_status,
                "profit_loss_amount": profit_loss,
                "deal_id": deal_id
            }
        
        return {
            "position_status": "OPEN",
            "profit_loss_status": pl_status,
            "profit_loss_amount": profit_loss,
            "deal_id": deal_id
        }
        
    async def get_position_status_deal_id(self, deal_id: str) -> dict:
        """Get position status (including closed) by deal_id."""
        positions_resp = await self.client.get_positions(status="ALL")
        positions = positions_resp.get("positions", [])

        # Find position by deal_id in open/closed positions
        position = next(
            (p for p in positions if p.get("position", {}).get("dealId") == deal_id),
            None
        )

        if not position:
            return {"position_status": "CLOSED", "profit_loss_status": "UNKNOWN", "profit_loss_amount": 0}

        # Extract status and P/L
        market_status = position.get("market", {}).get("marketStatus", "UNKNOWN")
        position_status = "CLOSED" if market_status == "CLOSED" else "OPEN"
        profit_loss = position.get("position", {}).get("upl", 0)
        pl_status = "PROFIT" if profit_loss > 0 else "LOSS" if profit_loss < 0 else "BREAKEVEN"

        return {
            "position_status": position_status,
            "profit_loss_status": pl_status,
            "profit_loss_amount": profit_loss
        }
