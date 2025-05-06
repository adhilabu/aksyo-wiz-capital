import asyncio
from datetime import date, datetime, time, timedelta
from decimal import ROUND_DOWN, ROUND_HALF_UP, ROUND_UP, Decimal
import json
import os
from typing import Any, Optional
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import pytz
from finta import TA
from talib import stream
import talib
import random
from app.analyse.schemas import CapitalMarketDetails, IndicatorValues, StockType, Trade, TradeAnalysisType, TradeStatus
from app.analyse.pivots import find_support_resistance
from app.database.db import DBConnection
from app.notification.telegram import TelegramAPI
from app.redis.redis import RedisCache
from app.capital.actions import CapitalAPI
from app.capital.instruments import get_capital_epics # Assuming this function exists
from app.capital.schemas import BasicPlaceOrderCapital, CapitalOrderType, CapitalTransactionType, CapitalMarketResolution
from app.utils.logger import setup_logger
from app.utils.utils import get_logging_level
from fastapi import BackgroundTasks
load_dotenv(dotenv_path=".env", override=True) 

LOGGING_LEVEL = get_logging_level()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
HISTORY_DATA_PERIOD = int(os.getenv("HISTORY_DATA_PERIOD", 100))
TELEGRAM_NOTIFICATION = os.getenv("TELEGRAM_NOTIFICATION", "False")
STOCK_PER_PRICE_LIMIT = os.getenv("STOCK_PER_PRICE_LIMIT", 2000)
TRADE_ANALYSIS_TYPE = os.getenv("TRADE_ANALYSIS_TYPE", TradeAnalysisType.NORMAL)
NIFTY_50_SYMBOL = 'NSE_INDEX|Nifty 50'
SPLIT_TYPE = int(os.getenv("SPLIT_TYPE", "1"))

class StockIndicatorCalculator:
    TICK_SIZE = 0.01
    TRADE_PERC = 0.005
    MAX_ADJUSTMENTS = 3
    TOTAL_TRADES_LIMIT = 10
    OPEN_TRADES_LIMIT = 5
    LOSS_TRADE_LIMIT = 5

    def __init__(self, db_connection: DBConnection, redis_cache: RedisCache, start_date=None, end_date=None, test_mode=False):
        self.db_con: DBConnection = db_connection
        self.redis_cache: RedisCache = redis_cache
        self.logger = setup_logger("StockIndicatorLogger", "stock_indicators", LOGGING_LEVEL)
        self.start_date = start_date
        self.end_date = end_date
        self.test_mode = test_mode
        self.capital_client = CapitalAPI(db_conn=db_connection)
        self.epics = get_capital_epics() # Assuming get_capital_epics exists and returns list of EPICs
        self.filename = self.get_notes_filename()
        self.stock_reversal_data: dict[str, Any] = {}  # {stock_symbol: (high, low, timestamp)}
        self.index_reversal_data: dict[str, Any] = {}  # {index_symbol: (high, low, timestamp)}
        self.executed_breakouts = {}
        self.stock_index_map = {}
        self.stock_name_map = {}
        self.stock_per_price_limit = float(STOCK_PER_PRICE_LIMIT)
        self.market_details = None

    def set_and_get_dates(self, for_historical_data=False):
        self.db_con.end_date = self.end_date
        self.start_date = self.db_con.start_date = self.end_date - timedelta(days=HISTORY_DATA_PERIOD)

        if for_historical_data:
            adjusted_end_date = self.end_date - timedelta(days=1)
            return self.start_date, adjusted_end_date

    def get_notes_filename(self):
        note_dir = "notes" if not self.test_mode else "test_notes"
        current_date = self.end_date.strftime("%Y-%m-%d") if self.end_date else datetime.now().strftime("%Y-%m-%d")
        if not os.path.exists(note_dir):
            os.makedirs(note_dir)
        filename = f"{current_date}.txt"
        return f"{note_dir}/{filename}"
    
    async def set_market_details_from_file(self, file_path: str = 'holiday.json') -> dict[str, dict[str, Any]]:
        """Load market details from a JSON file"""
        if not os.path.exists(file_path):
            self.logger.error(f"Market details file {file_path} does not exist.")
            return {}
        
        with open(file_path, 'r') as file:
            market_details = json.load(file)
        
        self.market_details = market_details
    
    async def initialize_historical_data(self):
        start_time = datetime.now()
        await self.set_market_details_from_file()
        await self.process_historical_combined_stock_data()
        await self.process_current_day_combined_stock_data()
        end_time = datetime.now()
        self.logger.info(f"Data initialized in {end_time - start_time} seconds.")

    async def update_all_stocks_data(self, transformed_data: pd.DataFrame):
        semaphore = asyncio.Semaphore(100)
        async def update_with_semaphore(row):
            try:
                async with semaphore:
                    await self.update_stock_data(row)
            except Exception as e:
                self.logger.error(f"Error processing row: {e}")
        tasks = [self.update_stock_data(row) for _, row in transformed_data.iterrows()]
        await asyncio.gather(*tasks)
        timestamp = transformed_data.iloc[0]['timestamp'] if len(transformed_data) > 0 else 'Empty'
        self.logger.info(f"Inserted data for timestamp: {timestamp}.")

    async def update_stock_data(self, data: pd.Series):
        stock = data['stock']
        timestamp = pd.to_datetime(data['timestamp'])
        existing_timestamps = await self.db_con.fetch_existing_timestamps(stock)
        if timestamp in existing_timestamps:
            await self.db_con.update_stock_data_for_historical_data_v2(data)
            return

        self.logger.info(f"Inserting data for stock: {stock} and timestamp: {timestamp}.")
        await self.db_con.save_data_to_db(data.to_frame().T)

        await self.analyze_sma_strategy(stock)
        return

    async def get_stock_ltp(self, stock, current_price):
        stock_ltp = await self.db_con.fetch_stock_ltp_from_db(stock) or current_price
        return stock_ltp

    async def transform_1_min_data_to_5_min_data(self, stock: str, in_data: pd.DataFrame):
        data = in_data.copy()
        if data.empty:
            return data
        
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data['timestamp'] = data['timestamp'].dt.tz_localize(None)
        data['timestamp'] = data['timestamp'].dt.floor('5min')
        data = data.groupby(['stock', pd.Grouper(key='timestamp', freq='5min')]).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'open_interest': 'last',
            'ltp': 'last'
        }).reset_index()
        data['stock'] = stock
        return data

    def is_trading_day(self, date: datetime, market_config: dict) -> bool:
        """Check if a date is a valid trading day"""
        tz = pytz.timezone(market_config["timezone"])
        local_date = date.astimezone(tz)
        
        # Check weekend trading
        if not market_config["weekend_trading"] and local_date.weekday() >= 5:
            return False
            
        # Check configured holidays
        if local_date.strftime("%Y-%m-%d") in market_config.get("holidays", []):
            return False
            
        # Add market-specific holiday checks here (e.g., using pandas_market_calendars)
        # ...
        
        return True
    
    async def process_current_day_combined_stock_data(self):
        for stock in self.epics:
            market_config = self.market_details.get(stock, {})
            current_date = datetime.now(pytz.UTC)
            if self.is_trading_day(current_date, market_config):
                self.logger.info(f"Processing current day data for {stock}.")
                batch_data = await self.capital_client.get_current_trading_day_data(stock)
                if not batch_data.empty:
                    # await self.calculate_pivot_data(batch_data)
                    await self.db_con.save_data_to_db(batch_data)


    async def process_historical_combined_stock_data(self):
        for stock in self.epics:
            market_config = self.market_details.get(stock, {})
            
            # Process last 10 trading days
            days_checked = 0
            current_date = datetime.now(pytz.UTC)               
            
            while days_checked < HISTORY_DATA_PERIOD:
                current_date -= timedelta(days=1)
                if not self.is_trading_day(current_date, market_config):
                    continue
                    
                days_checked += 1
                day_start, day_end = self.get_market_hours_utc(current_date, market_config)
                
                if not day_start or not day_end:
                    continue
                    
                daily_data = pd.DataFrame()
                current_batch_start = day_start
                
                while current_batch_start < day_end:
                    current_batch_end = min(
                        current_batch_start + timedelta(minutes=999),
                        day_end
                    )
                    
                    # Format timestamps
                    start_str = current_batch_start.strftime('%Y-%m-%dT%H:%M:%S')
                    end_str = current_batch_end.strftime('%Y-%m-%dT%H:%M:%S')
                    
                    self.logger.info(f"Fetching {stock} data from {start_str} to {end_str}")
                    batch_data = await self.capital_client.get_recent_data(
                        epic=stock,
                        start_dt_str=start_str,
                        end_dt_str=end_str,
                        interval='1minute'
                    )
                    
                    if not batch_data.empty:
                        daily_data = pd.concat([daily_data, batch_data])
                    
                    # Move to next batch (add 1 minute to avoid overlap)
                    current_batch_start = current_batch_end + timedelta(minutes=1)
            
                if not daily_data.empty:
                    await self.calculate_pivot_data(daily_data)
                    await self.db_con.save_data_to_db(daily_data)

    def get_market_hours_utc(self, date: datetime, market_config: dict):
        """Get trading hours for a specific date"""
        tz = pytz.timezone(market_config["timezone"])
        open_time = datetime.strptime(market_config["open"], "%H:%M").time()
        close_time = datetime.strptime(market_config["close"], "%H:%M").time()
        
        # Create market open/close datetime in local timezone
        market_open = tz.localize(datetime.combine(date.date(), open_time))
        market_close = tz.localize(datetime.combine(date.date(), close_time))
        
        # Handle overnight sessions
        if close_time < open_time:
            market_close += timedelta(days=1)
            
        # Convert to UTC
        market_open_utc = market_open.astimezone(pytz.UTC).replace(tzinfo=None)
        market_close_utc = market_close.astimezone(pytz.UTC).replace(tzinfo=None)
        
        return market_open_utc, market_close_utc

    async def calculate_mfi_talib(self, stock_data: pd.DataFrame, period=14) -> np.float64:
        if len(stock_data) <= 14:
            return np.float64(0.0)
        mfi = stream.MFI(
            stock_data['high'].astype(float).values,
            stock_data['low'].astype(float).values,
            stock_data['close'].astype(float).values,
            stock_data['volume'].astype(float).values,
            timeperiod=period
        )
        return mfi if mfi else 0

    async def calculate_adx_talib(self, stock_data: pd.DataFrame, period=14) -> np.float64:
        if len(stock_data) <= 14:
            return np.float64(0.0)
        adx = stream.ADX(
            stock_data['high'].astype(float).values,
            stock_data['low'].astype(float).values,
            stock_data['close'].astype(float).values,
            timeperiod=period
        )
        return adx if adx else 0

    async def calculate_rsi_talib(self, stock_data: pd.DataFrame, period=14) -> np.float64:
        if len(stock_data) <= 14:
            return np.float64(0.0)
        rsi = stream.RSI(stock_data['close'].astype(float).values, timeperiod=period)
        return rsi if rsi else 0
    
    async def calculate_vwap_finta(self, stock_data: pd.DataFrame) -> np.float64:
        vwap_data = stock_data[['close', 'high', 'low', 'volume', 'open']]
        vwap = TA.VWAP(vwap_data)
        return vwap.iloc[-1] if not vwap.empty else 0

    async def calculate_bbands_talib(self, stock_data: pd.DataFrame, period=20, nbdevup=2, nbdevdn=2):
        upperband, middleband, lowerband = stream.BBANDS(
            stock_data['close'].astype(float).values,
            timeperiod=period,
            nbdevup=nbdevup,
            nbdevdn=nbdevdn
        )
        return upperband, middleband, lowerband

    async def calculate_pivot_data(self, stock_data: pd.DataFrame):
        if stock_data.empty:
            return [], []
        close_prices = stock_data['close'].astype(float).values
        pivot_data = await self.db_con.fetch_pivot_data_from_db(stock_data['stock'].iloc[0])
        if pivot_data:
            return pivot_data.get('support_levels', []), pivot_data.get('resistance_levels', [])
        support_levels, resistance_levels = find_support_resistance(close_prices, 21)
        await self.db_con.insert_pivot_data_to_db(stock_data['stock'].iloc[0], {"support_levels": support_levels, "resistance_levels": resistance_levels})
        return support_levels, resistance_levels

    async def last_close_price_broke_resistance(self, stock_data: pd.DataFrame, transaction_type: CapitalTransactionType):
        support_levels, resistance_levels = await self.calculate_pivot_data(stock_data)
        last_close = stock_data['close'].iloc[-1]
        previous_close = stock_data['close'].iloc[-2]
        two_days_ago_close = stock_data['close'].iloc[-3]
        pivot_broken = False
        broken_level = None

        if transaction_type == CapitalTransactionType.BUY:
            for res in resistance_levels:
                if last_close > res and (previous_close < res or two_days_ago_close < res):
                    pivot_broken = True
                    broken_level = res
                    break
        else:
            for res in support_levels:
                if last_close < res and (previous_close > res or two_days_ago_close > res):
                    pivot_broken = True
                    broken_level = res
                    break

        return pivot_broken, broken_level

    async def check_and_execute_exit_trade_type_2(self, stock_data: pd.DataFrame):
        """Check and execute exit conditions for open trades based on strategy type 2."""
        current_timestamp = stock_data['timestamp']
        stock = stock_data['stock'] # EPIC
        current_ltp = stock_data['ltp']
        current_close = stock_data['close']
        current_low = stock_data['low']
        current_high = stock_data['high']

        # Fetch open trades (assuming DB stores dealId or dealReference)
        open_trades: list[Trade] = await self._fetch_open_trades(stock, current_timestamp)
        if not open_trades:
            return

        # Handle EOD square off first
        if await self._handle_eod_square_off(current_timestamp, stock, current_close):
            return

        executed_trades_updates = []
        for trade in open_trades:
            # Capital.com: Check position/order status using dealId/dealReference
            # This logic needs refinement based on how dealReference/dealId are stored and confirmed
            if not trade.order_status: # If DB status is not confirmed
                confirmed = False
                if trade.deal_reference: # Check confirmation using reference
                    confirmation = await self.capital_client.get_confirmation(trade.deal_reference)
                    if confirmation and confirmation.get('dealStatus') == 'ACCEPTED':
                        trade.deal_id = confirmation.get('dealId')
                        trade.order_status = True
                        confirmed = True
                        # Update DB with dealId and status
                        await self.db_con.update_trade_confirmation(trade.id, trade.deal_id, True)
                    elif confirmation and confirmation.get('dealStatus') == 'REJECTED':
                        # Handle rejection - update DB to closed/failed status
                        await self.db_con.update_trade_status(trade.id, TradeStatus.FAILED, current_ltp, current_ltp)
                        continue # Move to next trade
                    # else: Still pending confirmation
                
                # If still not confirmed, potentially check working orders if it was LIMIT/STOP
                # Or check open positions if it was MARKET
                # This part depends heavily on the exact workflow and API details
                if not confirmed:
                    # Placeholder: Assume test mode logic or skip if not confirmed
                    if self.test_mode:
                        # Simplified test mode check
                        if trade.trade_type == CapitalTransactionType.BUY and current_low <= trade.entry_cp:
                            trade.order_status = True
                        elif trade.trade_type == CapitalTransactionType.SELL and current_high >= trade.entry_cp:
                            trade.order_status = True
                        if trade.order_status:
                             await self.db_con.update_trade_confirmation(trade.id, f"test_{trade.id}", True) # Use dummy dealId for test
                             trade.deal_id = f"test_{trade.id}"
                    else:
                        # In live mode, if confirmation isn't found after some time, might need manual check or different logic
                        self.logger.debug(f"Trade {trade.id} for {stock} not confirmed yet.")
                        continue # Skip processing this trade for now
            
            # If order/position is confirmed (trade.order_status is True and trade.deal_id exists)
            if trade.order_status and trade.deal_id:
                # Pass both current_close (for logic) and current_ltp (for DB update)
                adjusted = await self._adjust_sl_pl(
                    trade,
                    current_ltp  # Added current LTP parameter
                )
                
                if not adjusted:
                    status, exit_price = await self._check_exit_conditions(trade, current_ltp)
                    if status:
                        # Execute exit using dealId
                        exit_ref = await self._execute_exit(trade, status, current_ltp)
                        if exit_ref: # Check if exit request was successful
                            # Update DB status
                            executed_trades_updates.append((
                                status.value, trade.id, current_ltp,
                                exit_price, stock
                            ))
                        else:
                            self.logger.error(f"Failed to execute exit for trade {trade.id} (Deal ID: {trade.deal_id})")

        # Batch update DB for executed exits
        if executed_trades_updates:
            await self.db_con.update_trade_statuses(executed_trades_updates)

    async def update_open_trades_status(self, stock: str, current_timestamp):
        """Check and update exit conditions for open/closed trades."""
        open_trades: list[Trade] = await self._fetch_open_trades(stock, current_timestamp)
        if not open_trades:
            return

        executed_trades_updates = []
        for trade in open_trades:
            metadata_json = trade.metadata_json.copy()
            deal_id = metadata_json.get("deal_id")
            working_order_id = metadata_json.get("working_order_id")

            # Step 1: Resolve deal_id if missing using working_order_id
            if not deal_id and working_order_id:
                working_pos_data = await self.capital_client.get_position_status_working_id(working_order_id)
                if working_pos_data and working_pos_data.get("deal_id"):
                    deal_id = working_pos_data["deal_id"]
                    metadata_json["deal_id"] = deal_id
                    await self.db_con.update_metadata_json(trade.id, metadata_json)

            # Step 2: Check position status using deal_id (includes closed positions)
            if deal_id:
                position_data = await self.capital_client.get_position_status_deal_id(deal_id)
                if not position_data:
                    continue

                # Update database if position is closed
                if position_data["position_status"] == "CLOSED":
                    metadata_json.update({
                        "profit_loss_status": position_data["profit_loss_status"],
                        "profit_loss_amount": position_data["profit_loss_amount"]
                    })
                    executed_trades_updates.append((position_data["position_status"], trade.id))
                    await self.db_con.update_metadata_json(trade.id, metadata_json)

        if executed_trades_updates:
            await self.db_con.update_trade_status_by_id(executed_trades_updates)

    async def _handle_eod_square_off(self, timestamp: datetime, stock: str, close_price: float) -> bool:
        """Handle end-of-day position squaring."""
        if timestamp >= timestamp.replace(hour=15, minute=0):
            exited_all = await self.db_con.exited_all_orders_for_the_day()
            if exited_all:
                return False

            # Use CapitalAPI to close all positions
            close_references = await self.capital_client.exit_all_positions()
            if close_references:
                self.logger.info(f"EOD square off initiated for {len(close_references)} positions.")
                # Update DB for all open trades associated with the closed positions
                await self.db_con.square_off_at_eod(stock, close_price) # Might need adjustment based on how trades are tracked
                return True
            else:
                self.logger.warning("EOD square off: No positions were closed or failed to initiate closure.")
                return False

        return False

    async def _fetch_open_trades(self, stock: str, timestamp: datetime) -> list[Trade]:
        """
        Fetch open trades and convert to Trade objects
        """

        rows = await self.db_con.fetch_open_orders(stock, timestamp) # Assuming this query returns deal_id and deal_reference
        return [Trade(
            id=row['id'],
            stock=stock, # EPIC
            entry_ltp=row['ltp'],
            entry_cp=row['cp'],
            entry_price=row['entry_price'],
            sl=row['sl'],
            pl=row['pl'],
            entry_time=row['timestamp'],
            trade_type=row['trade_type'],
            tag=f"{stock}_tag",
            metadata_json=json.loads(row['metadata_json']) if row['metadata_json'] else {},
            order_ids=json.loads(row.get('order_ids')),
            order_status=row['order_status'],
            deal_id=json.loads(row.get('order_ids'))[0] if row.get('order_ids') else ''
        ) for row in rows] if rows else []

    async def _adjust_sl_pl(
        self, 
        trade: Trade, 
        current_ltp: float
    ) -> bool:
        """
        Adjust SL and PL using 0.8% increments with trailing stop loss.
        """
        # Get adjustment count from metadata
        adjustment_count = trade.metadata_json.get('adjustment_count', 0)
        # max_adjustments = 2
        
        if adjustment_count >= self.MAX_ADJUSTMENTS:
            return False

        entry_price = trade.entry_ltp
        price_movement = abs(current_ltp - entry_price)
        required_movement = (adjustment_count + 1) * (self.TRADE_PERC - 0.002) * entry_price

        in_profit = (
            (trade.trade_type == CapitalTransactionType.BUY and current_ltp > entry_price) or
            (trade.trade_type == CapitalTransactionType.SELL and current_ltp < entry_price)
        )

        if in_profit and price_movement >= required_movement:
            # Calculate new values based on adjustment count
            # adjustment_factor = (adjustment_count + 1) * self.TRADE_PERC
            # base_sl = entry_price * (1 + (self.TRADE_PERC if trade.trade_type == CapitalTransactionType.SELL else -self.TRADE_PERC))
            
            if trade.trade_type == CapitalTransactionType.BUY:
                new_sl = entry_price + (adjustment_count * self.TRADE_PERC * entry_price)
                new_pl = entry_price + (adjustment_count + 1) * self.TRADE_PERC * entry_price
            else:
                new_sl = entry_price - (adjustment_count * self.TRADE_PERC * entry_price)
                new_pl = entry_price - (adjustment_count + 1) * self.TRADE_PERC * entry_price

            # Update metadata
            trade.metadata_json['adjustment_count'] = adjustment_count + 1

            # Update database with new values
            await self.db_con.update_sl_and_pl(
                sl=new_sl,
                pl=new_pl,
                ltp=current_ltp,
                id=trade.id,
                metadata_json=trade.metadata_json
            )
            
            # Also update the position on Capital.com
            if trade.deal_id:
                await self.capital_client.modify_position(trade.deal_id, stop_level=new_sl, profit_level=new_pl)
            else:
                self.logger.warning(f"Cannot modify position SL/PL for trade {trade.id} as deal_id is missing.")

            # Update trade object

            trade.sl = new_sl
            trade.pl = new_pl
            
            self.logger.info(
                f"Trailing {trade.stock} (Adjustment {adjustment_count + 1}/{self.MAX_ADJUSTMENTS}): "
                f"New SL: {new_sl:.2f}, New PL: {new_pl:.2f}"
            )
            return True
        return False


    async def _check_exit_conditions(self, trade: Trade, current_ltp: float) -> tuple[Optional[TradeStatus], Optional[float]]:
        """Check if current price hits SL or PL."""
        if trade.trade_type == CapitalTransactionType.BUY:
            trade_status = TradeStatus.PROFIT if current_ltp >= trade.entry_price else TradeStatus.LOSS
            if current_ltp <= trade.sl:
                return trade_status, current_ltp
            if current_ltp >= trade.pl:
                return trade_status, current_ltp
        else:  # sell
            trade_status = TradeStatus.PROFIT if current_ltp <= trade.entry_price else TradeStatus.LOSS
            if current_ltp >= trade.sl:
                return trade_status, current_ltp
            if current_ltp <= trade.pl:
                return trade_status, current_ltp
        return None, None

    async def _execute_exit(self, trade: Trade, status: TradeStatus, exit_price: float) -> Optional[str]:
        """Execute trade exit via Capital.com API and log results. Returns dealReference or None."""
        action = "Stop loss" if status == TradeStatus.LOSS else "Profit target"
        self.logger.info(f"{action} reached for {trade.stock} (Deal ID: {trade.deal_id}). Initiating exit.")
        
        if not trade.deal_id:
            self.logger.error(f"Cannot exit trade {trade.id} as deal_id is missing.")
            return None
            
        # Use CapitalAPI to close the position
        exit_reference = await self.capital_client.exit_position(deal_id=trade.deal_id)
        
        if exit_reference:
            self.logger.info(f"Exit request successful for trade {trade.id} (Deal ID: {trade.deal_id}), reference: {exit_reference}")
            # DB update happens in the calling function after checking exit_reference
            return exit_reference
        else:
            self.logger.error(f"Failed to initiate exit request for trade {trade.id} (Deal ID: {trade.deal_id})")
            return None  

    async def get_index_5min_df(self, stock: str, start_date=None, end_date=None):
        index = self.stock_index_map.get(stock)
        if index is None:
            return None
        
        if start_date is None or end_date is None:
            start_date = self.start_date
            end_date = self.end_date

        index_data: pd.DataFrame = await self.db_con.load_data_from_db(index, start_date, end_date)
        return await self.transform_1_min_data_to_5_min_data(index, index_data)

    async def identify_candle_color_reversal(self, stock_data_5_min):
        """Identifies the first candle color reversal and tracks breakout status."""
        # Existing implementation remains unchanged
        if len(stock_data_5_min) < 2:
            return None
        
        today = stock_data_5_min['timestamp'].iloc[-1].date()
        stock = stock_data_5_min['stock'].iloc[-1]
        today_data = stock_data_5_min[stock_data_5_min['timestamp'].dt.date == today]
        
        if len(today_data) < 2:
            return None
        
        first_candle = today_data.iloc[0]
        first_candle_bullish = first_candle['close'] > first_candle['open']
        
        reversal_idx = None
        for i in range(1, len(today_data)):
            current_candle = today_data.iloc[i]
            current_bullish = current_candle['close'] > current_candle['open']
            
            if current_bullish != first_candle_bullish:
                reversal_idx = i
                break
        
        if reversal_idx is None:
            return None
        
        pre_reversal_candles = today_data.iloc[:reversal_idx+1]
        reversal_high = pre_reversal_candles['high'].max()
        reversal_low = pre_reversal_candles['low'].min()
        reversal_time = today_data.iloc[reversal_idx]['timestamp']
        
        self.logger.info(f"Candle color reversal {stock} detected at {reversal_time}. High: {reversal_high}, Low: {reversal_low}")
        return (reversal_high, reversal_low, reversal_time)

    async def get_or_calculate_index_reversal_data(self, index_symbol, index_data_5_min):
        """
        Returns stored index reversal data if available, otherwise calculates it.
        """
        # Check if we already have reversal data for this index
        if index_symbol in self.index_reversal_data:
            return self.index_reversal_data[index_symbol]
        
        # Calculate new reversal data since we don't have it stored
        reversal_data = await self.identify_candle_color_reversal(index_data_5_min)
        
        # Store the reversal data if found
        if reversal_data:
            self.index_reversal_data[index_symbol] = reversal_data
            
        return reversal_data

    async def check_index_confirmation(self, stock, breakout_direction, stock_reversal_time=None):
        """
        Checks if the corresponding index confirms the breakout direction.
        Returns True if confirmed, False otherwise.
        """
        # Determine the index for this stock
        current_index_price = None
        index_symbol = self.stock_index_map.get(stock)
        if not index_symbol:
            return False
        
        # Check if we already have reversal data for this index
        index_reversal_data = self.index_reversal_data.get(index_symbol)
        if index_reversal_data is None:
            # Load index data
            index_data = await self.db_con.load_data_from_db(index_symbol, self.end_date, self.end_date)
            if index_data.empty:
                return False
            
            # Transform to 5-min data if needed
            index_data_5_min = await self.transform_1_min_data_to_5_min_data(index_symbol, index_data)
            if index_data_5_min.empty:
                return False

            index_reversal_data = await self.identify_candle_color_reversal(index_data_5_min)
            
            # Store the reversal data if found
            if index_reversal_data is None:
                return False

            self.index_reversal_data[index_symbol] = index_reversal_data

        index_high, index_low, index_reversal_time = index_reversal_data
        current_index_price = await self.db_con.fetch_stock_ltp_from_db(index_symbol)

        if stock_reversal_time and index_reversal_time != stock_reversal_time:
            return False

        # Check if index confirms the stock's breakout direction
        if breakout_direction == CapitalTransactionType.BUY and current_index_price > index_high:
            return True
        elif breakout_direction == CapitalTransactionType.SELL and current_index_price < index_low:
            return True
        
        return False

    async def valid_market_sentiment(self, breakout_direction):
        """
        Checks market sentiment with the breakout direction.
        Returns True if confirmed, False otherwise.
        """
        index_data = await self.db_con.load_data_from_db(NIFTY_50_SYMBOL, self.end_date - timedelta(days=10), self.end_date)
        if index_data.empty:
            return None

        # Transform to 5-min data
        index_data_5_min = await self.transform_1_min_data_to_5_min_data(NIFTY_50_SYMBOL, index_data)
        if index_data_5_min.empty:
            return False

        # Ensure timestamp is datetime type
        index_data_5_min['timestamp'] = pd.to_datetime(index_data_5_min['timestamp'])
        
        # Get unique trading dates
        trading_dates = sorted(index_data_5_min['timestamp'].dt.date.unique())
        current_date = self.end_date.date()
        
        # Find the most recent trading date before the current day
        previous_trading_dates = [d for d in trading_dates if d < current_date]
        if not previous_trading_dates:
            return False
        previous_day_date = previous_trading_dates[-1]
        
        # Extract previous trading day's data
        previous_day_data = index_data_5_min[index_data_5_min['timestamp'].dt.date == previous_day_date]
        if previous_day_data.empty:
            return False
        previous_day_close = previous_day_data['close'].iloc[-1]

        # Extract current day's 5-min data
        current_day_data = index_data_5_min[index_data_5_min['timestamp'].dt.date == current_date]
        if current_day_data.empty:
            return False

        current_close = current_day_data['close'].iloc[-1]
        current_open = current_day_data['open'].iloc[0]

        # Count bullish/bearish candles
        # bullish_candles = (current_day_data['close'] > current_day_data['open']).sum()
        # bearish_candles = (current_day_data['close'] < current_day_data['open']).sum()

        # Validate breakout direction
        if breakout_direction == CapitalTransactionType.BUY:
            return (current_close >= previous_day_close)
        elif breakout_direction == CapitalTransactionType.SELL:
            return (current_close <= previous_day_close)
        return False


    async def get_or_calculate_reversal_data(self, stock_symbol, stock_data_5_min):
        """
        Returns stored reversal data if available, otherwise calculates it.
        """
        # Check if we already have reversal data for this stock
        just_created = False
        if stock_symbol in self.stock_reversal_data:
            return just_created, self.stock_reversal_data[stock_symbol]
        
        # Calculate new reversal data since we don't have it stored
        reversal_data = await self.identify_candle_color_reversal(stock_data_5_min)
        
        # Store the reversal data if found
        if reversal_data:
            self.stock_reversal_data[stock_symbol] = reversal_data
            just_created = True
        return just_created, reversal_data

    async def ensure_market_details(self, epic: str, timestamp) -> Optional[CapitalMarketDetails]:
        """Ensure market details are available, fetching from API if necessary"""
        market_data = await self.db_con.get_capital_market_details(epic, timestamp)
        if market_data:
            return market_data

        # Fetch from API if not found in DB
        return await self.capital_client.get_instrument_details(epic)

    def round_to_increment(self, value: float, increment: float, round_up: bool = None) -> float:
        """Rounds a value up or down to the nearest multiple of the increment."""
        if increment <= 0:
            return value
        decimal_value = Decimal(str(value))
        decimal_increment = Decimal(str(increment))
        rounding_mode = ROUND_UP if round_up else ROUND_DOWN
        if round_up is None:
            rounding_mode = ROUND_HALF_UP
        rounded = (decimal_value / decimal_increment).quantize(Decimal('1.'), rounding=rounding_mode) * decimal_increment
        return float(rounded)
    
    async def calculate_rounded_value_with_step(self, value: float, details: CapitalMarketDetails) -> float:
        """Rounds a value (like distance or potential order price) to the nearest valid step."""
        # Calculate step size based on min_step_distance and unit
        step = details.min_step_distance
        if details.min_step_distance_unit == 'PERCENTAGE':
            # Step as a percentage needs a base value. Using the input value itself.
            # This is suitable for rounding distances.
            step = value * (details.min_step_distance / 100)
            # Ensure step is positive
            if step <= 0:
                 # Fallback to the raw point value if percentage calculation is zero or negative
                 step = details.min_step_distance

        return self.round_to_increment(value, step)

    async def calculate_sl_for_breakout(
        self,
        direction: CapitalTransactionType,
        entry_price: float,
        details: CapitalMarketDetails, # Added details argument
        stock_data: pd.DataFrame
    ) -> float:
        """Calculates Stop Loss price based on strategy and market rules."""
        # Calculate step size for rounding final SL price
        # Using entry_price to calculate percentage step if applicable
        step = details.min_step_distance
        if details.min_step_distance_unit == 'PERCENTAGE':
            # Note: The step size as a percentage depends on the value it's applied to.
            # Here we calculate it based on the entry price for consistency before rounding the final price.
            step = entry_price * (details.min_step_distance / 100)
            # Ensure step is positive
            if step <= 0:
                 # Fallback to the raw point value if percentage calculation is zero or negative
                 step = details.min_step_distance


        # Calculate min/max allowed distance in absolute terms based on entry price
        # min_distance = entry_price * (details.min_stop_or_profit_distance / 100)
        min_distance = max(entry_price * (details.min_stop_or_profit_distance / 100), entry_price * (details.min_guaranteed_stop_distance / 100) if details.min_guaranteed_stop_distance_unit == 'PERCENTAGE' else details.min_guaranteed_stop_distance)
        max_distance = entry_price * (details.max_stop_or_profit_distance / 100)

        # --- Strategy Specific Logic ---
        # Calculate desired SL based on initial percentage (e.g., 2%)
        sl_percent = self.TRADE_PERC # Assume self.TRADE_PERC is defined (e.g., 0.02 for 2%)
        if direction == CapitalTransactionType.BUY:
            desired_sl = entry_price * (1 - sl_percent)
        else: # CapitalTransactionType.SELL
            desired_sl = entry_price * (1 + sl_percent)

        # Search for reversal candle and potentially use its low/high for SL
        reversal_price = None
        # Iterate backwards from the second to last candle (assuming last is current/entry)
        # for i in reversed(range(1, len(stock_data))):
        #      if i < 1: # Need at least one previous candle to check for reversal pattern
        #          break
        #      candle = stock_data.iloc[i]
        #      prev_candle = stock_data.iloc[i - 1]

        #      # Basic reversal logic: check if the previous candle was opposite direction
        #      is_bearish_prev = prev_candle['close'] < prev_candle['open']
        #      is_bullish_prev = prev_candle['close'] > prev_candle['open']
        #      is_current_bearish = candle['close'] < candle['open']
        #      is_current_bullish = candle['close'] > candle['open']


        #      if direction == CapitalTransactionType.BUY and is_bullish_prev and is_current_bearish:
        #          # Potential bearish reversal (peak), low of the previous candle might be support
        #          reversal_price = prev_candle['low']
        #          break
        #      elif direction == CapitalTransactionType.SELL and is_bearish_prev and is_current_bullish:
        #          # Potential bullish reversal (trough), high of the previous candle might be resistance
        #          reversal_price = prev_candle['high']
        #          break

        # If a reversal price was found, check if its distance is within allowed API range
        if reversal_price is not None:
             reversal_distance = abs(entry_price - reversal_price)
             if min_distance <= reversal_distance <= max_distance:
                 self.logger.info(f"Using reversal candle price {reversal_price:.4f} for SL, distance {reversal_distance:.4f} is within API limits.")
                 desired_sl = reversal_price
             else:
                 self.logger.info(f"Reversal candle price {reversal_price:.4f} found, but distance {reversal_distance:.4f} is outside API limits ({min_distance:.4f}-{max_distance:.4f}). Sticking to percentage-based SL.")

        # --- End Strategy Specific Logic ---


        # --- Apply API Constraints ---
        # Re-calculate current distance based on the potentially adjusted desired_sl
        current_distance = abs(entry_price - desired_sl)

        # Ensure the final SL distance is within the min/max allowed by the API
        if current_distance < min_distance:
            self.logger.warning(f"Calculated SL distance {current_distance:.4f} is below API minimum {min_distance:.4f}. Adjusting SL.")
            app_min_distance = min_distance * 1.01
            if direction == CapitalTransactionType.BUY:
                desired_sl = entry_price - app_min_distance
            else: # SELL
                desired_sl = entry_price + app_min_distance
        elif current_distance > max_distance:
            self.logger.warning(f"Calculated SL distance {current_distance:.4f} exceeds API maximum {max_distance:.4f}. Adjusting SL.")
            app_max_distance = max_distance * 0.99
            if direction == CapitalTransactionType.BUY:
                desired_sl = entry_price - app_max_distance
            else: # SELL
                desired_sl = entry_price + app_max_distance
        
        # --- End Apply API Constraints ---
        # check if the desired SL is greater than the min_guaranteed_stop_distance PERCENTAGE if not set it as the value
        if details.min_guaranteed_stop_distance > 0:
            min_guaranteed_stop_distance = entry_price * (details.min_guaranteed_stop_distance / 100) if details.min_guaranteed_stop_distance_unit == 'PERCENTAGE' else details.min_guaranteed_stop_distance
            if current_distance < min_guaranteed_stop_distance:
                desired_sl = entry_price - min_guaranteed_stop_distance if direction == CapitalTransactionType.BUY else entry_price + min_guaranteed_stop_distance


        # Finally, round the desired SL price to the nearest valid step
        round_dir = False if direction == CapitalTransactionType.BUY else True
        final_sl = self.round_to_increment(desired_sl, step, round_up=round_dir)

        self.logger.info(f"Calculated final SL price: {final_sl:.4f}")
        return final_sl

    async def calculate_pl_for_breakout(
        self,
        direction: CapitalTransactionType,
        entry_price: float,
        stop_loss: float, # The calculated and rounded stop loss price
        details: CapitalMarketDetails # Added details argument
    ) -> float:
        """Calculates Profit Level price based on SL and market rules (1:2 RR)."""
        # Calculate step size for rounding final PL price
        # Using entry_price to calculate percentage step if applicable
        step = details.min_step_distance
        if details.min_step_distance_unit == 'PERCENTAGE':
            # Note: The step size as a percentage depends on the value it's applied to.
            # Here we calculate it based on the entry price for consistency before rounding the final price.
            step = entry_price * (details.min_step_distance / 100)
            # Ensure step is positive
            if step <= 0:
                 # Fallback to the raw point value if percentage calculation is zero or negative
                 step = details.min_step_distance

        # Calculate min/max allowed distance in absolute terms based on entry price
        min_distance = entry_price * (details.min_stop_or_profit_distance / 100)
        max_distance = entry_price * (details.max_stop_or_profit_distance / 100)

        # --- Strategy Specific Logic ---
        # Calculate risk (absolute distance between entry and calculated SL)
        risk = abs(entry_price - stop_loss)

        # Calculate desired reward based on Risk/Reward ratio (e.g., 1:2)
        reward = 1 * risk # Assuming a 1:2 Risk-Reward ratio strategy

        # Calculate desired PL price based on entry price and reward
        if direction == CapitalTransactionType.BUY:
            desired_pl = entry_price + reward
        else: # CapitalTransactionType.SELL
            desired_pl = entry_price - reward
        # --- End Strategy Specific Logic ---

        # --- Apply API Constraints ---
        # Re-calculate current distance based on the potentially adjusted desired_pl
        current_distance = abs(entry_price - desired_pl)

        # Ensure the final PL distance is within the min/max allowed by the API
        if current_distance < min_distance:
            self.logger.warning(f"Calculated PL distance {current_distance:.4f} is below API minimum {min_distance:.4f}. Adjusting PL.")
            if direction == CapitalTransactionType.BUY:
                desired_pl = entry_price + min_distance
            else: # SELL
                desired_pl = entry_price - min_distance
        elif current_distance > max_distance:
            self.logger.warning(f"Calculated PL distance {current_distance:.4f} exceeds API maximum {max_distance:.4f}. Adjusting PL.")
            if direction == CapitalTransactionType.BUY:
                desired_pl = entry_price + max_distance
            else: # SELL
                desired_pl = entry_price - max_distance
        # --- End Apply API Constraints ---


        # Finally, round the desired PL price to the nearest valid step
        round_dir = True if direction == CapitalTransactionType.BUY else False
        final_pl = self.round_to_increment(desired_pl, step, round_up=round_dir)

        self.logger.info(f"Calculated final PL price: {final_pl:.4f}")
        return final_pl


    async def get_base_payload_for_capital_order(self, epic: str, stock_ltp: float,
                                                 trans_type: CapitalTransactionType = CapitalTransactionType.BUY,
                                                 order_type: CapitalOrderType = CapitalOrderType.MARKET, timestamp = None, stock_data: pd.DataFrame = None) -> Optional[BasicPlaceOrderCapital]:
        if stock_data is None or stock_data.empty:
            self.logger.warning(f"Stock data is empty for {epic}")
            return None

        if stock_ltp <= 0:
            self.logger.warning(f"Invalid LTP {stock_ltp} for {epic}")
            return None

        # Fetch instrument details to ensure rules are loaded
        market_details = await self.ensure_market_details(epic, timestamp)
        if not market_details:
             self.logger.error(f"Failed to retrieve market details for {epic}")
             return None


        # Calculate dynamic SL/PL
        # Pass market_details to SL/PL calculations
        stop_loss = await self.calculate_sl_for_breakout(trans_type, stock_ltp, market_details, stock_data)
        profit_level = await self.calculate_pl_for_breakout(trans_type, stock_ltp, stop_loss, market_details)

        # Quantity calculation
        # Calculate initial desired quantity based on capital allocation
        initial_quantity = float(self.stock_per_price_limit / stock_ltp)

        # Apply min/max constraints and round to the nearest valid increment
        quantity = initial_quantity

        # 1. Enforce min and max deal size
        if quantity < market_details.min_deal_size:
             self.logger.info(f"Calculated quantity {quantity:.4f} is below minimum deal size {market_details.min_deal_size} for {epic}. Setting to minimum.")
             quantity = float(market_details.min_deal_size)
        elif quantity > market_details.max_deal_size:
             self.logger.warning(f"Calculated quantity {quantity:.4f} exceeds maximum deal size {market_details.max_deal_size} for {epic}. Setting to maximum.")
             quantity = float(market_details.max_deal_size)

        # 2. Round the adjusted quantity to the nearest multiple of the minimum size increment
        # Use the helper function with min_size_increment
        quantity = self.round_to_increment(quantity, market_details.min_size_increment)


        if quantity <= 0:
            self.logger.warning(f"Calculated quantity is zero or negative for {epic} at LTP {stock_ltp}. Skipping order.")
            return None

        # Calculate distances for the payload - these also need to be rounded to the step distance
        stop_distance = await self.calculate_rounded_value_with_step(abs(stock_ltp - stop_loss), market_details)
        profit_distance = await self.calculate_rounded_value_with_step(abs(stock_ltp - profit_level), market_details)

        # Determine order price - only needed for LIMIT or STOP orders
        order_price = None
        if order_type in [CapitalOrderType.LIMIT, CapitalOrderType.STOP]:
            # For limit/stop orders, the specific price needs to be set and rounded to the step distance
            # If order type is LIMIT/STOP, stock_ltp might not be the intended order price.
            # Assuming for this code, order_price is also based on LTP for simplicity or calculated elsewhere
            # but needs rounding. If it's a different price (e.g., breakout level), use that price here.
            # For robustness, you might pass the *intended* order price to this function.
            # Using stock_ltp and rounding it as a placeholder:
             order_price = await self.calculate_rounded_value_with_step(stock_ltp, market_details)
             # Note: In a real scenario, the 'price' for a limit/stop order should be the *target* price
             # which would be determined by your strategy, not necessarily the LTP.
             # This code uses LTP as the 'order_price' placeholder if order_type is not MARKET.
             # You should replace stock_ltp here with the actual limit/stop price from your strategy.


        return BasicPlaceOrderCapital(
            quantity=quantity,
            price=order_price, # This will be None for MARKET orders
            epic=epic,
            order_type=order_type,
            transaction_type=trans_type,
            stop_loss=stop_loss, # The calculated and rounded stop price
            profit_level=profit_level, # The calculated and rounded profit price
            stop_distance=stop_distance, # The rounded distance from entry price to SL price
            profit_distance=profit_distance, # The rounded distance from entry price to PL price
        )

    async def calculate_atr(self, stock_data_5_min, period=14):
        """Calculate Average True Range for volatility measurement."""
        if len(stock_data_5_min) <= 14:
            return np.float64(0.0)

        high = stock_data_5_min['high'].values
        low = stock_data_5_min['low'].values
        close = stock_data_5_min['close'].values
        
        tr = np.maximum(high[1:] - low[1:], 
                    np.maximum(np.abs(high[1:] - close[:-1]), 
                    np.abs(low[1:] - close[:-1]))
        )
        
        atr = np.zeros(len(close))
        atr[period] = tr[:period].mean()
        
        for i in range(period+1, len(close)):
            atr[i] = (atr[i-1] * (period-1) + tr[i-1]) / period
        
        return atr[-1]

    def is_invalid_momentum_trade(self, indicator_values: IndicatorValues):
        if indicator_values.rsi >= 60 and indicator_values.adx >= 25:
            return False
        return True
        
    async def analyze_reversal_breakout_strategy(self, stock) -> None:
        stock_data: pd.DataFrame = await self.db_con.load_data_from_db(stock, self.end_date - timedelta(days=5), self.end_date)
        if stock_data.empty:
            return

        final_stock_data = stock_data.iloc[-1]
        final_timestamp = final_stock_data['timestamp']
        
        # Existing checks remain unchanged
        await self.check_and_execute_exit_trade_type_2(final_stock_data)
        if stock in self.executed_breakouts and self.executed_breakouts[stock]:
            self.logger.info(f"{stock}: Breakout already executed today. Skipping.")
            return
    
        results = await self.db_con.get_trade_stats(stock, open_count=self.OPEN_TRADES_LIMIT, total_count=self.TOTAL_TRADES_LIMIT)
        if any(results):
            self.logger.info(f"RH: {stock}: Condition met, skipping trade")
            return
        
        current_price = final_stock_data['ltp']
        breakout_direction = None

        
        pivot_broken, broken_level = await self.last_close_price_broke_resistance(stock_data, breakout_direction)
        if not pivot_broken:
            self.logger.info(f"RH: stock: Didn't broke any pivot levels")
            return
        

        # Calculate dynamic position size based on volatility
        atr = await self.calculate_atr(stock_data)
        
        # In analyze_reversal_breakout_strategy:
        stock_ltp = await self.get_stock_ltp(stock, current_price)
        sl = await self.calculate_sl_for_breakout(breakout_direction, stock_ltp)
        pl = await self.calculate_pl_for_breakout(breakout_direction, stock_ltp)

        # Create payload for Capital.com order
        base_payload = await self.get_base_payload_for_capital_order(
            epic=stock,
            stock_ltp=stock_ltp, # This might be used as the level for LIMIT/STOP
            stop_loss=sl,
            profit_level=pl,
            trans_type=breakout_direction,
            order_type=CapitalOrderType.MARKET # Defaulting to MARKET, adjust if needed
        )
        
        if not base_payload:
            return
        
        await asyncio.sleep(random.uniform(0, 1))

        # # Redis keys
        # stock_ts_key = f"order:{stock}:{final_timestamp}"
        # ts_count_key = f"trade_count:{final_timestamp}"
    
        
        # # Check and set stock-specific timestamp key using SETNX
        # if not self.redis_cache.client.setnx(stock_ts_key, 1):
        #     # self.logger.info(f"Order for {stock} in {final_timestamp} exists. Skipping.")
        #     return

        # # Set TTL for the stock key (e.g., 5 minutes)
        # self.redis_cache.client.expire(stock_ts_key, 100)
        
        # # Increment and check timestamp trade count
        # current_count = self.redis_cache.client.incr(ts_count_key)
        # # Set TTL on timestamp count key if first increment
        # if current_count == 1:
        #     self.redis_cache.client.expire(ts_count_key, 100)
        
        # if current_count > 2:
        #     # Revert: delete stock key and decrement count
        #     self.redis_cache.client.delete(stock_ts_key)
        #     self.redis_cache.client.decr(ts_count_key)
        #     self.logger.info(f"RH: ax trades (2) reached for {final_timestamp}. Skipping.")
        #     return

        # Check existing open trades
        if await self.db_con.check_open_trades_count(self.OPEN_TRADES_LIMIT):
            self.logger.info("RH: Max open trades reached. Skipping.")
            # self.redis_cache.client.delete(stock_ts_key)
            # self.redis_cache.client.decr(ts_count_key)
            return

        # Check existing open trades
        if await self.db_con.check_loss_trades_count(self.LOSS_TRADE_LIMIT):
            self.logger.info("RH: Max loss trades reached. Skipping.")
            # self.redis_cache.client.delete(stock_ts_key)
            # self.redis_cache.client.decr(ts_count_key)
            return
        
        # Place order and log trade
        rsi, adx, mfi = await asyncio.gather(
            self.calculate_rsi_talib(stock_data),
            self.calculate_adx_talib(stock_data),
            self.calculate_mfi_talib(stock_data)
        )
        
        indicator_values = IndicatorValues(rsi=rsi, adx=adx, mfi=mfi)
        await self.send_telegram_notification(final_stock_data, indicator_values, broken_level, sl, pl, breakout_direction)
        if self.is_invalid_momentum_trade(indicator_values):
            self.logger.info("RH: Invalid trade based on momentum. Skipping.")
            return

        metadata_json = {
            "atr": atr
        }

        await self.capital_client.place_order(base_payload)
        await self.db_con.log_trade_to_db(
            final_stock_data,
            indicator_values,
            broken_level,
            sl,
            pl,
            breakout_direction,
            stock,
            metadata_json,
            base_payload.quantity,
            order_status=True if self.test_mode else False,
            order_ids=[],
            stock_ltp=stock_ltp
        )

        self.logger.info(f"RH:  High-probability trade executed for {stock}: ")

    async def analyze_sma_strategy(self, stock) -> None:
        stock_data: pd.DataFrame = await self.db_con.load_data_from_db(stock, self.end_date - timedelta(days=HISTORY_DATA_PERIOD), self.end_date)
        if stock_data.empty:
            return
        
        #  order by timestamp
        stock_data = stock_data.sort_values(by='timestamp', ascending=True)
        final_stock_data = stock_data.iloc[-1]
        current_timestamp = stock_data.iloc[-1]['timestamp']
        # Existing checks remain unchanged
        await self.update_open_trades_status(stock, current_timestamp)        
        results = await self.db_con.get_trade_stats(stock, open_count=self.OPEN_TRADES_LIMIT, total_count=self.TOTAL_TRADES_LIMIT)
        if any(results):
            self.logger.info(f"SMA: {stock}: Condition met, skipping trade")
            return
        
        current_price = final_stock_data['ltp']
        breakout_direction = None

        if stock_data.empty or len(stock_data) < 200:
            self.logger.info(f"SMA: {stock}: Insufficient data to calculate SMAs. Skipping.")
            return

        # Calculate 13-period and 200-period SMAs using closing prices
        # Calculate SMA with partial window initialization
        stock_data['sma13'] = stock_data['close'].rolling(window=13, min_periods=1).mean()
        stock_data['sma200'] = stock_data['close'].rolling(window=200, min_periods=1).mean()

        # Extract latest SMA values and closing price
        sma13 = stock_data['sma13'].iloc[-1]
        sma200 = stock_data['sma200'].iloc[-1]
        latest_close = final_stock_data['close']
        prev_sma13 = stock_data['sma13'].iloc[-2]
        prev_high = stock_data['high'].iloc[-2]
        prev_low = stock_data['low'].iloc[-2]
        
        # Check if SMAs are valid (not NaN)
        if pd.isna(sma13) or pd.isna(sma200):
            self.logger.info(f"SMA: {stock}: Insufficient data to calculate SMAs. Skipping.")
            return

        breakout_direction = CapitalTransactionType.BUY
        broken_level = sma13  # Using 200 SMA as confirmation level

        if latest_close >= sma13 and latest_close > sma200 and prev_high < prev_sma13:
            breakout_direction = CapitalTransactionType.BUY
        elif latest_close <= sma13 and latest_close < sma200 and prev_low > prev_sma13:
            breakout_direction = CapitalTransactionType.SELL
        else:
            self.logger.info(f"SMA: {stock}: Close price not above/below both SMAs. Skipping.")
            return

        # pivot_broken, broken_level = await self.last_close_price_broke_resistance(stock_data, breakout_direction)
        # if not pivot_broken:
        #     self.logger.info(f"SMA: stock: Didn't broke any pivot levels")
        #     return

        # Determine SL and PL based on breakout direction
        stock_ltp = await self.get_stock_ltp(stock, current_price)
        # Prepare order payload
        order_type = CapitalOrderType.MARKET
        base_payload = await self.get_base_payload_for_capital_order(
            epic=stock,
            stock_ltp=stock_ltp,
            trans_type=breakout_direction,
            order_type=order_type,
            timestamp=current_timestamp,
            stock_data=stock_data
        )
        
        if not base_payload:
            return
        
        await asyncio.sleep(random.uniform(0, 1))

        # Redis key for stock timestamp validation
        stock_ts_key = f"order:{stock}:{current_timestamp}"

        # Check if this stock timestamp combination already exists
        if not self.redis_cache.client.setnx(stock_ts_key, 1):
            return  # Exit if key already exists

        # Set TTL for the stock timestamp key (100 seconds)
        self.redis_cache.client.expire(stock_ts_key, 30)

        # Check open trade limits
        if await self.db_con.check_open_trades_count(self.OPEN_TRADES_LIMIT):
            self.logger.info("SMA: Max open trades reached. Skipping.")
            return

        if await self.db_con.check_loss_trades_count(self.LOSS_TRADE_LIMIT):
            self.logger.info("SMA: Max loss trades reached. Skipping.")
            return
        
        # Calculate momentum indicators
        rsi, adx, mfi = await asyncio.gather(
            self.calculate_rsi_talib(stock_data),
            self.calculate_adx_talib(stock_data),
            self.calculate_mfi_talib(stock_data)
        )
        
        indicator_values = IndicatorValues(rsi=rsi, adx=adx, mfi=mfi)
        await self.send_telegram_notification(
            final_stock_data,
            indicator_values,
            broken_level,
            base_payload.stop_loss,
            base_payload.profit_level,
            breakout_direction
        )

        # Prepare metadata including ATR and SMAs
        metadata_json = {
            "sma13": sma13,
            "sma200": sma200
        }

        # Place order and log trade
        deal_reference = await self.capital_client.place_order(base_payload)
        await self.db_con.log_trade_to_db(
            final_stock_data,
            indicator_values,
            broken_level,
            base_payload.stop_loss if order_type != CapitalOrderType.MARKET else base_payload.stop_distance,
            base_payload.profit_level if order_type != CapitalOrderType.MARKET else base_payload.profit_distance,
            breakout_direction,
            stock,
            metadata_json,
            base_payload.quantity,
            order_status=True if self.test_mode else False,
            order_ids=[deal_reference] if deal_reference else [],
            stock_ltp=stock_ltp
        )

        self.logger.info(f"SMA: High-probability trade executed for {stock}: Direction {breakout_direction}")


    async def send_telegram_notification(self, stock_data: pd.Series, indicator_values: IndicatorValues, broken_level, sl, pl, trade_type: str = 'BUY'):
        stock = stock_data['stock']
        redis_key = f"message:{stock}"
        stock_name = self.stock_name_map.get(stock, stock)
        formatted_date_time = stock_data['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
        ist_timezone = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist_timezone).strftime("%Y-%m-%d %H:%M:%S")
        if self.test_mode:
            message = (
                "Test Mode\n"
                f"Stock: {stock_name}\n"
                f"LTP: {stock_data['ltp']}\n"
                f"Action: {trade_type.value}\n"
                f"SL: {sl}\n"
                f"PL: {pl}\n"
                f"Broke Resistance: {broken_level}\n"
                f"RSI: {round(indicator_values.rsi, 2)}\n"
                f"ADX: {round(indicator_values.adx, 2)}\n"
                f"MFI: {round(indicator_values.mfi, 2)}\n"
                f"Stock Date & Time: {formatted_date_time}\n"
                f"MSG Date & Time: {current_time}"

            )

            if TELEGRAM_NOTIFICATION.lower() == "true":
                telegram_api = TelegramAPI(TELEGRAM_TOKEN)
                telegram_api.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
                self.redis_cache.set_key(redis_key, 1, ttl=1800)
                self.logger.info("Telegram notification sent successfully.")

        
            with open(self.filename, "a") as f:
                f.write(f"{message}\n\n")
                return

        message = (
            f"Stock: {stock_name}\n"
            f"LTP: {stock_data['ltp']}\n"
            f"Action: {trade_type.value}\n"
            f"SL: {sl}\n"
            f"PL: {pl}\n"
            f"Broke Resistance: {broken_level}\n"
            f"RSI: {round(indicator_values.rsi, 2)}\n"
            f"ADX: {round(indicator_values.adx, 2)}\n"
            f"MFI: {round(indicator_values.mfi, 2)}\n"
            f"Stock Date & Time: {formatted_date_time}\n"
            f"MSG Date & Time: {current_time}"
        )

        with open(self.filename, "a") as f:
            f.write(f"{message}\n\n")

        if self.redis_cache.key_exists(redis_key):
            self.logger.info(f"Telegram notification already sent for stock: {stock}.")
            return

        if TELEGRAM_NOTIFICATION.lower() == "true":
            telegram_api = TelegramAPI(TELEGRAM_TOKEN)
            telegram_api.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
            self.redis_cache.set_key(redis_key, 1, ttl=1800)
            self.logger.info("Telegram notification sent successfully.")
