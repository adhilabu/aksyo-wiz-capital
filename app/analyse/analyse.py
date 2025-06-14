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
STOCK_PER_PRICE_LIMIT = os.getenv("STOCK_PER_PRICE_LIMIT", 1000)
TRADE_ANALYSIS_TYPE = os.getenv("TRADE_ANALYSIS_TYPE", TradeAnalysisType.NORMAL)
NIFTY_50_SYMBOL = 'NSE_INDEX|Nifty 50'
SPLIT_TYPE = int(os.getenv("SPLIT_TYPE", "1"))
TRADE_PERC = float(os.getenv("TRADE_PERC", 0.006))
SL_PERC = float(os.getenv("SL_PERC", 0.0025))
LOG_TRADE_TO_DB = os.getenv("LOG_TRADE_TO_DB", "False").lower() == "true"

print(f"Trade Analysis Type: {TRADE_ANALYSIS_TYPE}")


class StockIndicatorCalculator:
    TICK_SIZE = 0.01
    TRADE_PERC = TRADE_PERC
    SL_PERC = SL_PERC
    MAX_ADJUSTMENTS = 3
    TOTAL_TRADES_LIMIT = 30
    OPEN_TRADES_LIMIT = 15
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
        # for _, row in transformed_data.iterrows():
        #     await self.update_stock_data(row)
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

        await asyncio.gather(
            self.analyze_mean_reversion_strategy(stock),
            self.analyze_ma_crossover_strategy_type_2(stock),
            self.analyze_sma_strategy_type_2(stock),
            self.analyze_sma_macd_crossover_strategy(stock),
            self.analyze_reversal_breakout_strategy(stock, timestamp),
            self.analyze_sma_strategy_type_1(stock),
        )
    

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
            if market_config and self.is_trading_day(current_date, market_config):
                self.logger.info(f"Processing current day data for {stock}.")
                batch_data = await self.capital_client.get_current_trading_day_data(stock)
                if not batch_data.empty:
                    # await self.calculate_pivot_data(batch_data)
                    await self.db_con.save_data_to_db(batch_data)



    async def process_historical_combined_stock_data(self):
        for stock in self.epics:
            market_config = self.market_details.get(stock, {})
            if not market_config:
                self.logger.warning(f"No market configuration found for {stock}, skipping...")
                continue

            days_checked = 0
            current_date = datetime.now(pytz.UTC)
            max_retries = 3
            retry_delay = 5  # seconds
            batch_size_minutes = 480  # Reduced batch size to 8 hours for better reliability

            while days_checked < HISTORY_DATA_PERIOD:
                current_date -= timedelta(days=1)
                if not self.is_trading_day(current_date, market_config):
                    continue

                days_checked += 1
                self.logger.info(f"Processing historical data for {stock} on {current_date.date()}")

                # Handle multiple sessions per day (e.g., J225)
                sessions = []
                if "session1_start" in market_config and "session1_end" in market_config:
                    sessions.append((
                        datetime.strptime(market_config["session1_start"], "%H:%M").time(),
                        datetime.strptime(market_config["session1_end"], "%H:%M").time()
                    ))
                if "session2_start" in market_config and "session2_end" in market_config:
                    sessions.append((
                        datetime.strptime(market_config["session2_start"], "%H:%M").time(),
                        datetime.strptime(market_config["session2_end"], "%H:%M").time()
                    ))
                
                # If no specific sessions defined, use regular open/close
                if not sessions:
                    sessions = [(
                        datetime.strptime(market_config["open"], "%H:%M").time(),
                        datetime.strptime(market_config["close"], "%H:%M").time()
                    )]

                for session_start, session_end in sessions:
                    # For overnight sessions, we need to handle the date transition
                    is_overnight = session_end < session_start
                    
                    if is_overnight:
                        # For overnight sessions, we need to fetch data from current day's start to next day's end
                        day_start, day_end = self.get_market_hours_utc(
                            current_date, 
                            market_config, 
                            session_start, 
                            session_end
                        )
                    else:
                        # For regular sessions, use the same day
                        day_start, day_end = self.get_market_hours_utc(
                            current_date, 
                            market_config, 
                            session_start, 
                            session_end
                        )

                    if not day_start or not day_end:
                        continue

                    daily_data = pd.DataFrame()
                    current_batch_start = day_start
                    retry_count = 0

                    while current_batch_start < day_end:
                        current_batch_end = min(
                            current_batch_start + timedelta(minutes=batch_size_minutes),
                            day_end
                        )

                        # Skip if data already exists
                        if not await self.db_con.should_fetch_range(
                            stock, 
                            current_batch_start, 
                            current_batch_end, 
                            interval_minutes=1
                        ):
                            self.logger.info(
                                f"Skipping {stock} data fetch from "
                                f"{current_batch_start} to {current_batch_end} (already present)"
                            )
                            current_batch_start = current_batch_end + timedelta(minutes=1)
                            continue

                        start_str = current_batch_start.strftime('%Y-%m-%dT%H:%M:%S')
                        end_str = current_batch_end.strftime('%Y-%m-%dT%H:%M:%S')
                        self.logger.info(f"Fetching {stock} data from {start_str} to {end_str}")

                        while retry_count < max_retries:
                            try:
                                batch_data = await self.capital_client.get_recent_data(
                                    epic=stock,
                                    start_dt_str=start_str,
                                    end_dt_str=end_str,
                                    interval='1minute'
                                )
                                
                                if not batch_data.empty:
                                    daily_data = pd.concat([daily_data, batch_data])
                                break  # Success, exit retry loop
                            except Exception as e:
                                retry_count += 1
                                if retry_count == max_retries:
                                    self.logger.error(f"Failed to fetch data for {stock} after {max_retries} attempts: {str(e)}")
                                    break
                                self.logger.warning(f"Retry {retry_count}/{max_retries} for {stock} due to: {str(e)}")
                                await asyncio.sleep(retry_delay)

                        # Reset retry count for next batch
                        retry_count = 0
                        current_batch_start = current_batch_end + timedelta(minutes=1)

                    if not daily_data.empty:
                        try:
                            await self.calculate_pivot_data(daily_data)
                            await self.db_con.save_data_to_db(daily_data)
                            self.logger.info(f"Successfully saved {len(daily_data)} records for {stock} on {current_date.date()}")
                        except Exception as e:
                            self.logger.error(f"Error processing data for {stock} on {current_date.date()}: {str(e)}")

    def get_market_hours_utc(self, date: datetime, market_config: dict, session_start: time = None, session_end: time = None):
        """Get trading hours for a specific date and session"""
        tz = pytz.timezone(market_config["timezone"])
        
        # Use provided session times or fall back to market open/close
        open_time = session_start or datetime.strptime(market_config["open"], "%H:%M").time()
        close_time = session_end or datetime.strptime(market_config["close"], "%H:%M").time()
        
        # Create market open/close datetime in local timezone
        market_open = tz.localize(datetime.combine(date.date(), open_time))
        
        # For overnight sessions, close time is on the next day
        if close_time < open_time:
            market_close = tz.localize(datetime.combine(date.date() + timedelta(days=1), close_time))
        else:
            market_close = tz.localize(datetime.combine(date.date(), close_time))
            
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
        stock = stock_data['stock']  # EPIC
        current_ltp = stock_data['ltp']
        current_close = stock_data['close']
        current_low = stock_data['low']
        current_high = stock_data['high']

        # Fetch open trades
        open_trades: list[Trade] = await self._fetch_open_trades(stock, current_timestamp)
        if not open_trades:
            return

        executed_trades_updates = []
        for trade in open_trades:
            # Check if the order is already confirmed
            if not trade.order_status:
                if trade.deal_id:
                    response = await self.capital_client.get_deal_reference_status(trade.deal_id)
                    trade.order_status = response.get('order_status')
                    trade.metadata_json['working_order_id'] = response.get('working_order_id')
                    await self.db_con.update_metadata_json(trade.id, trade.metadata_json)
                    await self.db_con.update_final_order_status(trade.id, trade.order_status)
                    continue        

            # If order is confirmed, check exit conditions
            if trade.order_status:
                working_order_id = trade.metadata_json.get("working_order_id")
                if working_order_id:
                    position_data = await self.capital_client.get_position_status_working_id(working_order_id)
                    if position_data and position_data.get("deal_id"):
                        trade.metadata_json.update(position_data)
                        if position_data.get("position_status") == "CLOSED":
                            profit_or_loss_status = position_data.get("profit_loss_status")
                            profit_or_loss_amount = position_data.get("profit_loss_status")
                            # self.logger.info(f"Trade {trade.id} exitted with status {trade.order_status} (Deal ID: {trade.deal_id})")
                            self.logger.info(f"Trade {trade.id} exitted with status {profit_or_loss_status} and amount {profit_or_loss_amount} (Deal ID: {trade.deal_id})")
                            executed_trades_updates.append((
                                TradeStatus.PROFIT if position_data["profit_loss_status"] == "PROFIT" else TradeStatus.LOSS,
                                current_ltp,
                                current_ltp,
                                stock,
                                trade.id,
                            ))

                        await self.db_con.update_metadata_json(trade.id, trade.metadata_json)
                    
                    else:
                        self.logger.warning(f"No positions found for trade id {trade.id}. So assuming it as exitted.")
                        executed_trades_updates.append((
                            TradeStatus.PROFIT if trade.metadata_json.get("profit_loss_status") == "PROFIT" else TradeStatus.LOSS,
                            current_ltp,
                            current_ltp,
                            stock,
                            trade.id,
                        ))
                        continue
                        

        # Batch update DB for executed exits
        if executed_trades_updates:
            await self.db_con.update_trade_statuses(executed_trades_updates)
            
    async def update_open_trades_status(self, stock_data: pd.DataFrame):
        """Check and update exit conditions for open/closed trades."""
        current_timestamp = stock_data['timestamp'].iloc[-1]
        stock = stock_data['stock'].iloc[-1]  # EPIC
        current_ltp = stock_data['ltp'].iloc[-1]
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
                if working_pos_data.get("position_status") == "CLOSED":
                    profit_or_loss_status = working_pos_data.get("profit_loss_status")
                    profit_or_loss_amount = working_pos_data.get("profit_loss_amount")
                    # self.logger.info(f"Trade {trade.id} exitted with status {trade.order_status} (Deal ID: {trade.deal_id})")
                    self.logger.info(f"Trade {trade.id} exitted with status {profit_or_loss_status} and amount {profit_or_loss_amount} (Deal ID: {trade.deal_id})")
                    executed_trades_updates.append((working_pos_data["position_status"], trade.id))

                await self.db_con.update_metadata_json(trade.id, trade.metadata_json)

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

        if reversal_time.tzinfo is not None:
            local_tz = pytz.timezone('Asia/Kolkata')
            reversal_time = reversal_time.astimezone(local_tz).replace(tzinfo=None)

        self.logger.info(f"Candle color reversal {stock} detected at {reversal_time}. High: {reversal_high}, Low: {reversal_low}")
        return (reversal_high, reversal_low, reversal_time)

    async def get_or_calculate_reversal_data(self, stock_symbol, stock_data_5_min):
        # if cached already, skip recalculation
        if stock_symbol in self.stock_reversal_data:
            return False, self.stock_reversal_data[stock_symbol]

        # load market open info (all times in UTC)
        open_str = self.market_details[stock_symbol]['open']    # e.g. "13:30" UTC
        tz_name  = self.market_details[stock_symbol]['timezone']  # e.g. "America/New_York"
        market_tz = pytz.timezone(tz_name)
        local_tz = pytz.timezone('Asia/Kolkata')
        if not open_str:
            self.logger.error(f"Missing market open time for {stock_symbol}.")
            return False, None

        # parse open time in UTC
        market_open_time = datetime.strptime(open_str, "%H:%M").time()

        # determine relevant date: today or yesterday based on open time
        now_utc = datetime.now(market_tz)
        today_utc = now_utc.date()
        # combine today's date with market open time
        open_datetime_today = datetime.combine(today_utc, market_open_time, tzinfo=market_tz)
        if open_datetime_today > now_utc:
            # before today's open, use yesterday's open
            relevant_date = today_utc - timedelta(days=1)
            open_datetime_today = open_datetime_today - timedelta(days=1)
        else:
            relevant_date = today_utc

        # standardize timestamps: ensure UTC-aware
        df = stock_data_5_min.copy()
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(local_tz)
        df['timestamp_utc'] = df['timestamp'].dt.tz_convert(market_tz)
         
        open_bars = df[df['timestamp_utc'] >= open_datetime_today]

        # attempt to load stored reversal data
        db_data = await self.db_con.get_reversal_data(stock_symbol, relevant_date, 'stock')
        if db_data:
            self.stock_reversal_data[stock_symbol] = db_data
            return False, db_data

        # compute reversal on filtered bars
        reversal_data = await self.identify_candle_color_reversal(open_bars)
        if reversal_data:
            rh, rl, rt = reversal_data
            await self.db_con.save_reversal_data(stock_symbol, relevant_date, rh, rl, rt, 'stock')
            self.stock_reversal_data[stock_symbol] = reversal_data
            return True, reversal_data

        # no reversal found
        return False, None


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
        details: CapitalMarketDetails,
        stock_data: pd.DataFrame
    ) -> float:
        """Calculates Stop Loss price based on strategy and Capital.com API rules."""
        # 1. Determine rounding step
        stock_symbol = details.epic
        if details.min_step_distance_unit == 'PERCENTAGE':
            step = max(entry_price * (details.min_step_distance / 100), details.min_step_distance)
        else:
            step = details.min_step_distance

        # 2. Compute allowed distances
        # Guaranteed and normal stop distances
        grn_unit = details.min_guaranteed_stop_distance_unit
        nrm_unit = details.min_stop_or_profit_distance_unit
        if grn_unit == 'PERCENTAGE':
            min_grn = entry_price * (details.min_guaranteed_stop_distance / 100)
        else:
            min_grn = details.min_guaranteed_stop_distance
        if nrm_unit == 'PERCENTAGE':
            min_nrm = entry_price * (details.min_stop_or_profit_distance / 100)
        else:
            min_nrm = details.min_stop_or_profit_distance

        # For BUY ensure SL not too close, for SELL similarly
        min_distance = max(min_grn, min_nrm)

        # Max distance
        if details.max_stop_or_profit_distance_unit == 'PERCENTAGE':
            max_distance = entry_price * (details.max_stop_or_profit_distance / 100)
        else:
            max_distance = details.max_stop_or_profit_distance

        # 3. Base SL based on strategy percentage
        # sl_pct = self.TRADE_PERC
        sl_pct = self.market_details.get(stock_symbol, {}).get('sl_perc', self.SL_PERC) or self.SL_PERC
        if direction == CapitalTransactionType.BUY:
            desired_sl = entry_price * (1 - sl_pct)
        else:
            desired_sl = entry_price * (1 + sl_pct)

        # 4. Optional: check for reversal candle SL override
        reversal_price = None
        # for i in range(len(stock_data)-2, 0, -1):
        #     prev = stock_data.iloc[i-1]
        #     curr = stock_data.iloc[i]
        #     bear_prev = prev['close'] < prev['open']
        #     bull_prev = prev['close'] > prev['open']
        #     bear_curr = curr['close'] < curr['open']
        #     bull_curr = curr['close'] > curr['open']
        #     if direction == CapitalTransactionType.BUY and bull_prev and bear_curr:
        #         reversal_price = prev['low']; break
        #     if direction == CapitalTransactionType.SELL and bear_prev and bull_curr:
        #         reversal_price = prev['high']; break

        if reversal_price is not None:
            rev_dist = abs(entry_price - reversal_price)
            if min_distance <= rev_dist <= max_distance:
                desired_sl = reversal_price

        # 5. Apply API constraints
        current_distance = abs(entry_price - desired_sl)
        if current_distance < min_distance:
            self.logger.warning(f"SL distance {current_distance:.4f} < min {min_distance:.4f}, adjusting.")
            adj = min_distance * 1.001
            desired_sl = entry_price - adj if direction == CapitalTransactionType.BUY else entry_price + adj
            current_distance = abs(entry_price - desired_sl)
        if current_distance > max_distance:
            self.logger.warning(f"SL distance {current_distance:.4f} > max {max_distance:.4f}, adjusting.")
            adj = max_distance * 0.999
            desired_sl = entry_price - adj if direction == CapitalTransactionType.BUY else entry_price + adj
            current_distance = abs(entry_price - desired_sl)

        # 6. Enforce guaranteed stop at least
        if details.min_guaranteed_stop_distance > 0:
            if grn_unit == 'PERCENTAGE':
                grn_min = entry_price * (details.min_guaranteed_stop_distance / 100)
            else:
                grn_min = details.min_guaranteed_stop_distance
            if current_distance < grn_min:
                adjustment = grn_min * 1.001
                self.logger.warning(f"SL distance {current_distance:.4f} < GRN min {grn_min:.4f}, adjusting to GRN.")
                # Adjust SL based on direction
                if direction == CapitalTransactionType.BUY:
                    desired_sl = entry_price - adjustment if desired_sl > entry_price else entry_price + adjustment
                else:
                    desired_sl = entry_price + adjustment if desired_sl < entry_price else entry_price - adjustment
                current_distance = abs(entry_price - desired_sl)

        # 7. Round to valid increment
        round_up = (direction == CapitalTransactionType.SELL)
        final_sl = self.round_to_increment(desired_sl, step, round_up=round_up)
        self.logger.info(f"Final SL set to {final_sl:.4f}")
        return final_sl


    async def calculate_pl_for_breakout(
        self,
        direction: CapitalTransactionType,
        entry_price: float,
        stop_loss: float,
        details: CapitalMarketDetails
    ) -> float:
        """Calculates Profit Level price based on SL and Capital.com API rules (1:2 RR by default)."""
        # 1. Determine rounding step
        if details.min_step_distance_unit == 'PERCENTAGE':
            step = max(entry_price * (details.min_step_distance / 100), details.min_step_distance)
        else:
            step = details.min_step_distance

        # 2. Compute API min/max distances
        def to_abs(value, unit):
            return entry_price * (value / 100) if unit == 'PERCENTAGE' else value

        min_distance = to_abs(details.min_stop_or_profit_distance, details.min_stop_or_profit_distance_unit)
        max_distance = to_abs(details.max_stop_or_profit_distance, details.max_stop_or_profit_distance_unit)

        risk = abs(entry_price - stop_loss)
        reward = risk * 1.5  # Default 1:1.5 risk-reward ratio
        if direction == CapitalTransactionType.BUY:
            desired_pl = entry_price + reward
        else:
            desired_pl = entry_price - reward

        # 4. Apply API constraints
        current_distance = abs(entry_price - desired_pl)
        if current_distance < min_distance:
            self.logger.warning(
                f"PL distance {current_distance:.4f} < API min {min_distance:.4f}, adjusting to min."
            )
            adjustment = min_distance * 1.01
            # Adjust PL based on direction
            if direction == CapitalTransactionType.BUY:
                desired_pl = entry_price + adjustment
            else:
                desired_pl = entry_price - adjustment
            # Recalculate current distance
            current_distance = abs(entry_price - desired_pl)

        elif current_distance > max_distance:
            self.logger.warning(
                f"PL distance {current_distance:.4f} > API max {max_distance:.4f}, adjusting to max."
            )
            adjustment = max_distance * 0.99
            # Adjust PL based on direction
            if direction == CapitalTransactionType.BUY:
                desired_pl = entry_price + adjustment
            else:
                desired_pl = entry_price - adjustment
            # Recalculate current distance
            current_distance = abs(entry_price - desired_pl)


        # 5. Round to valid increment
        round_up = direction == CapitalTransactionType.BUY
        final_pl = self.round_to_increment(desired_pl, step, round_up=round_up)

        self.logger.info(f"Final PL set to {final_pl:.4f}")
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
        raw_leverage = self.market_details.get(epic, {}).get('leverage', '1:1')
        try:
            leverage_factor = float(raw_leverage.split(":")[1])
        except Exception:
            self.logger.warning(f"Malformed leverage '{raw_leverage}' for {epic}, defaulting to 1:1")
            leverage_factor = 1.0
            
        stock_per_price = self.stock_per_price_limit
        if leverage_factor < 100:
            stock_per_price = stock_per_price / 2
        initial_quantity = float(stock_per_price / stock_ltp)
        if initial_quantity <= 0:
            self.logger.warning(f"Calculated quantity is zero or negative for {epic} at LTP {stock_ltp}. Skipping order.")
            return None
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
        
    async def analyze_reversal_breakout_strategy(self, stock, timestamp) -> None:
        stock_data: pd.DataFrame = await self.db_con.load_data_from_db_with_timestamp(stock, self.end_date - timedelta(days=5), timestamp)
        if stock_data.empty:
            return

        final_stock_data = stock_data.iloc[-1]
        final_timestamp = final_stock_data['timestamp']
        
        # Existing checks remain unchanged
        # await self.check_and_execute_exit_trade_type_2(final_stock_data)
        # await self.update_open_trades_status(stock_data)
        if stock in self.executed_breakouts and self.executed_breakouts[stock]:
            self.logger.info(f"{stock}: Breakout already executed today. Skipping.")
            return
    
        if (final_timestamp.minute + 1) % 5 != 0:
            return

        # results = await self.db_con.get_trade_stats(stock, open_count=self.OPEN_TRADES_LIMIT, total_count=self.TOTAL_TRADES_LIMIT)
        # if any(results):
        #     self.logger.info(f"HN :{stock}: Condition met, skipping trade")
        #     return

        # open_result = await self.db_con.get_open_trade_stats(stock, open_count=20)
        # if open_result:
        #     self.logger.info(f"HN :{stock}: Condition met, skipping trade")
        #     return

        # Existing data processing remains unchanged
        stock_data_5_min = await self.transform_1_min_data_to_5_min_data(stock, stock_data)
        if stock_data_5_min.empty:
            return

        just_created, reversal_data = await self.get_or_calculate_reversal_data(stock, stock_data_5_min)
        if not reversal_data:
            return
        
        reversal_high, reversal_low, reversal_time = reversal_data
        current_timestamp = stock_data_5_min['timestamp'].iloc[-1]
        if reversal_time == current_timestamp:
            return
        
        post_reversal_data = stock_data_5_min[
            (stock_data_5_min['timestamp'] > reversal_time) & 
            (stock_data_5_min['timestamp'] < current_timestamp)
        ]
        
        current_price = final_stock_data['ltp']
        breakout_direction = CapitalTransactionType.BUY
        broken_level = reversal_high
        
        if current_price > reversal_high:
            breakout_direction = CapitalTransactionType.BUY
            broken_level = reversal_high
        elif current_price < reversal_low:
            breakout_direction = CapitalTransactionType.SELL
            broken_level = reversal_low
        else:
            return
        
        already_broken = False
        if not post_reversal_data.empty:
            if breakout_direction == CapitalTransactionType.BUY:
                already_broken = (post_reversal_data['close'] > reversal_high).any()
            else:
                already_broken = (post_reversal_data['close'] < reversal_low).any()


        # Mark breakout as executed for today
        # self.executed_breakouts[stock] = True

        if already_broken:
            self.logger.info(f"{stock}: Breakout already occurred in historical data. Skipping.")
            return
        
        # if not await self.check_index_confirmation(stock, breakout_direction, reversal_time):
        #     self.logger.info(f"HN {stock}: Index does not confirm the breakout direction")
        #     return

        index_confirmation = True
        # nifty_in_confirmation = await self.check_index_confirmation(breakout_direction, stock_reversal_time=reversal_time)
        
        # pivot_broken, broken_level = await self.last_close_price_broke_resistance(stock_data_5_min, breakout_direction)
        # if not pivot_broken:
        #     self.logger.info(f"HN {stock}: Didn't broke any pivot levels")
        #     return
        

        # Calculate dynamic position size based on volatility
        atr = 0
        
        # In analyze_reversal_breakout_strategy:
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
        
        # stock_name = self.stock_name_map.get(stock, stock)

        # Place order and log trade
        # rsi, adx, mfi = await asyncio.gather(
        #     self.calculate_rsi_talib(stock_data_5_min),
        #     self.calculate_adx_talib(stock_data_5_min),
        #     self.calculate_mfi_talib(stock_data_5_min)
        # )

        rsi = 0
        adx = 0
        mfi = 0
        
        indicator_values = IndicatorValues(rsi=rsi, adx=adx, mfi=mfi)

        await asyncio.sleep(random.uniform(0, 1))

        # Redis key for stock timestamp validation
        stock_ts_key = f"order:{stock}:{current_timestamp}"

        # Check if this stock timestamp combination already exists
        if not self.redis_cache.client.setnx(stock_ts_key, 1):
            return  # Exit if key already exists

        # Set TTL for the stock timestamp key (100 seconds)
        self.redis_cache.client.expire(stock_ts_key, 30)

        await self.send_telegram_notification(final_stock_data, indicator_values, broken_level, base_payload.stop_loss, base_payload.profit_level, breakout_direction, "REVERSAL_BREAKOUT")
        # Momentum check
        # if self.is_invalid_momentum_trade(indicator_values):
        #     self.logger.info("HN: Invalid momentum indicators. Skipping.")
        #     return

        # Check existing open trades
        # if await self.db_con.check_open_trades_count(self.OPEN_TRADES_LIMIT):
        #     self.logger.info("HN: Max open trades reached. Skipping.")
        #     # self.redis_cache.client.delete(stock_ts_key)
        #     # self.redis_cache.client.decr(ts_count_key)
        #     return

        # # Check existing open trades
        # if await self.db_con.check_loss_trades_count(self.LOSS_TRADE_LIMIT):
        #     self.logger.info("HN: Max loss trades reached. Skipping.")
        #     # self.redis_cache.client.delete(stock_ts_key)
        #     # self.redis_cache.client.decr(ts_count_key)
        #     return
        

        metadata_json = {
            "atr": atr
        }

        deal_reference = await self.capital_client.place_order(base_payload)
        if LOG_TRADE_TO_DB:
            await self.db_con.log_trade_to_db(
                final_stock_data,
                indicator_values,
                float(broken_level),
                float(base_payload.stop_loss if order_type != CapitalOrderType.MARKET else base_payload.stop_distance),
                float(base_payload.profit_level if order_type != CapitalOrderType.MARKET else base_payload.profit_distance),
                breakout_direction,
                stock,
                metadata_json,
                qty=int(base_payload.quantity),
                order_status=True if self.test_mode else False,
                order_ids=[deal_reference] if deal_reference else [],
                stock_ltp=str(stock_ltp)
            )

        self.logger.info(f"HN : High-probability trade executed for {stock}: ")


    async def analyze_sma_strategy_type_1(self, stock) -> None:
        stock_data: pd.DataFrame = await self.db_con.load_data_from_db(stock, self.end_date - timedelta(days=HISTORY_DATA_PERIOD), self.end_date)
        if stock_data.empty:
            return
        
        #  order by timestamp
        stock_data = stock_data.sort_values(by='timestamp', ascending=True)
        final_stock_data = stock_data.iloc[-1]
        current_timestamp = stock_data.iloc[-1]['timestamp']
        # Existing checks remain unchanged
        # await self.update_open_trades_status(stock_data)        
        # open_results = await self.db_con.get_open_trade_stats(stock, open_count=self.OPEN_TRADES_LIMIT)
        # if open_results:
        #     self.logger.info(f"SMA: {stock}: Condition met, skipping trade")
        #     return
        
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
        prev_high_2 = stock_data['high'].iloc[-3]
        prev_low_2 = stock_data['low'].iloc[-3]
        prev_sma13_2 = stock_data['sma13'].iloc[-3]
        prev_high_3 = stock_data['high'].iloc[-4]
        prev_low_3 = stock_data['low'].iloc[-4]
        prev_sma13_3 = stock_data['sma13'].iloc[-4]
        
        # Check if SMAs are valid (not NaN)
        if pd.isna(sma13) or pd.isna(sma200):
            self.logger.info(f"SMA: {stock}: Insufficient data to calculate SMAs. Skipping.")
            return

        breakout_direction = None
        broken_level = sma13

        if latest_close >= sma13 and latest_close > sma200 and prev_high < prev_sma13 and prev_high_2 < prev_sma13_2 and prev_high_3 < prev_sma13_3:
        # if latest_close >= sma13 and latest_close > sma200 and prev_high < prev_sma13:
            breakout_direction = CapitalTransactionType.BUY
        elif latest_close <= sma13 and latest_close < sma200 and prev_low > prev_sma13 and prev_low_2 > prev_sma13_2 and prev_low_3 > prev_sma13_3:
        # elif latest_close <= sma13 and latest_close < sma200 and prev_low > prev_sma13:
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
        # if await self.db_con.check_open_trades_count(self.OPEN_TRADES_LIMIT):
        #     self.logger.info("SMA: Max open trades reached. Skipping.")
        #     return

        # if await self.db_con.check_loss_trades_count(self.LOSS_TRADE_LIMIT):
        #     self.logger.info("SMA: Max loss trades reached. Skipping.")
        #     return
        
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
            float(broken_level),
            float(base_payload.stop_loss if order_type != CapitalOrderType.MARKET else base_payload.stop_distance),
            float(base_payload.profit_level if order_type != CapitalOrderType.MARKET else base_payload.profit_distance),
            breakout_direction,
            "SMA_STRATEGY"
        )

        # Prepare metadata including ATR and SMAs
        metadata_json = {
            "sma13": sma13,
            "sma200": sma200
        }

        # Place order and log trade
        deal_reference = await self.capital_client.place_order(base_payload)
        if LOG_TRADE_TO_DB:
            await self.db_con.log_trade_to_db(
                final_stock_data,
                indicator_values,
                float(broken_level),
                float(base_payload.stop_loss if order_type != CapitalOrderType.MARKET else base_payload.stop_distance),
                float(base_payload.profit_level if order_type != CapitalOrderType.MARKET else base_payload.profit_distance),
                breakout_direction,
                stock,
                metadata_json,
                qty=int(base_payload.quantity),
                order_status=True if self.test_mode else False,
                order_ids=[deal_reference] if deal_reference else [],
                stock_ltp=str(stock_ltp)
            )

        self.logger.info(f"SMA: High-probability trade executed for {stock}: Direction {breakout_direction.value}")

    async def analyze_sma_macd_crossover_strategy(self, stock) -> None:
        """
        Implements RSI + MACD crossover strategy on 15-minute timeframes.
        Entry conditions:
        Long: RSI < 30 (oversold) + MACD crosses above Signal
        Short: RSI > 70 (overbought) + MACD crosses below Signal
        """
        # Load historical data for the past 5 days to ensure enough data for calculations
        stock_data: pd.DataFrame = await self.db_con.load_data_from_db(
            stock, 
            self.end_date - timedelta(days=5), 
            self.end_date
        )

        # sort by timestamp
        if stock_data.empty:
            return

        # Sort data by timestamp
        stock_data = stock_data.sort_values(by='timestamp', ascending=True)
        final_stock_data = stock_data.iloc[-1]
        current_timestamp = final_stock_data['timestamp']

        # continue if current_timestamp + 1 is not in the multiple of 15 minutes
        if (current_timestamp + timedelta(minutes=1)).minute % 15 != 0:
            return

        # Update existing trades and check limits
        # await self.update_open_trades_status(stock_data)
        # open_results = await self.db_con.get_open_trade_stats(stock, open_count=self.OPEN_TRADES_LIMIT)
        # if open_results:
        #     self.logger.info(f"MACD: {stock}: Max open trades reached, skipping")
        #     return

        # Transform 1-minute data to 15-minute timeframe
        stock_data_15min = await self.transform_data_to_15min(stock, stock_data)
        if len(stock_data_15min) < 35:  # Need enough data for indicators
            return

        # Calculate RSI
        rsi = talib.RSI(stock_data_15min['close'].values, timeperiod=14)
        
        # Calculate MACD
        macd, signal, hist = talib.MACD(
            stock_data_15min['close'].values,
            fastperiod=12,
            slowperiod=26,
            signalperiod=9
        )

        # Get current and previous values
        current_rsi = rsi[-1]
        prev_rsi = rsi[-2]
        current_macd = macd[-1]
        current_signal = signal[-1]
        prev_macd = macd[-2]
        prev_signal = signal[-2]

        # Determine trade direction based on conditions
        breakout_direction = None
        if (current_rsi < 30 and prev_rsi < 30 and 
            current_macd > current_signal and prev_macd <= prev_signal):
            breakout_direction = CapitalTransactionType.BUY
        elif (current_rsi > 70 and prev_rsi > 70 and 
              current_macd < current_signal and prev_macd >= prev_signal):
            breakout_direction = CapitalTransactionType.SELL
        else:
            self.logger.info(f"MACD: {stock}: No valid crossover conditions met. Skipping.")
            return

        # Get current price and prepare order
        current_price = final_stock_data['ltp']
        stock_ltp = await self.get_stock_ltp(stock, current_price)

        # Custom take profit and stop loss percentages
        # tp_percentage = 0.0007  # 0.07%
        # sl_percentage = 0.00051  # 0.051%

        # # Calculate take profit and stop loss levels
        # if breakout_direction == CapitalTransactionType.BUY:
        #     take_profit = stock_ltp * (1 + tp_percentage)
        #     stop_loss = stock_ltp * (1 - sl_percentage)
        # else:
        #     take_profit = stock_ltp * (1 - tp_percentage)
        #     stop_loss = stock_ltp * (1 + sl_percentage)

        # Prepare order payload with custom TP/SL
        base_payload = await self.get_base_payload_for_capital_order(
            epic=stock,
            stock_ltp=stock_ltp,
            trans_type=breakout_direction,
            order_type=CapitalOrderType.MARKET,
            timestamp=current_timestamp,
            stock_data=stock_data
        )
        
        if not base_payload:
            return


        # Add random delay to prevent simultaneous orders
        await asyncio.sleep(random.uniform(0, 1))

        # Redis key for stock timestamp validation
        stock_ts_key = f"order:{stock}:{current_timestamp}"
        if not self.redis_cache.client.setnx(stock_ts_key, 1):
            return
        self.redis_cache.client.expire(stock_ts_key, 30)

        # Final checks before placing order
        # if await self.db_con.check_open_trades_count(self.OPEN_TRADES_LIMIT):
        #     self.logger.info("MACD: Max open trades reached. Skipping.")
        #     return

        # if await self.db_con.check_loss_trades_count(self.LOSS_TRADE_LIMIT):
        #     self.logger.info("MACD: Max loss trades reached. Skipping.")
        #     return

        # Prepare indicator values for logging
        indicator_values = IndicatorValues(
            rsi=current_rsi,
            adx=0,  # Not used in this strategy
            mfi=0   # Not used in this strategy
        )

        # Send notification
        await self.send_telegram_notification(
            final_stock_data,
            indicator_values,
            current_price,  # Using current price as broken level
            base_payload.stop_loss,
            base_payload.profit_level,
            breakout_direction,
            "RSI_MACD_CROSS"
        )

        # Prepare metadata
        metadata_json = {
            "rsi": current_rsi,
            "macd": current_macd,
            "macd_signal": current_signal,
            "strategy": "RSI_MACD_CROSS"
        }

        # Place order and log trade
        broken_level = 0

        deal_reference = await self.capital_client.place_order(base_payload)
        if LOG_TRADE_TO_DB:
            await self.db_con.log_trade_to_db(
                final_stock_data,
                indicator_values,
                float(broken_level),
                float(base_payload.stop_distance),
                float(base_payload.profit_distance),
                breakout_direction,
                stock,
                metadata_json,
                qty=int(base_payload.quantity),
                order_status=True if self.test_mode else False,
                order_ids=[deal_reference] if deal_reference else [],
                stock_ltp=str(stock_ltp)
            )

        self.logger.info(
            f"MACD: Trade executed for {stock}: Direction {breakout_direction.value}, "
            f"RSI: {current_rsi:.2f}, MACD Cross: {current_macd:.6f}/{current_signal:.6f}"
        )

    async def transform_data_to_15min(self, stock: str, in_data: pd.DataFrame) -> pd.DataFrame:
        """Transform 1-minute data to 15-minute timeframe."""
        data = in_data.copy()
        if data.empty:
            return data
        
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data['timestamp'] = data['timestamp'].dt.tz_localize(None)
        data['timestamp'] = data['timestamp'].dt.floor('15min')
        
        data = data.groupby(['stock', pd.Grouper(key='timestamp', freq='15min')]).agg({
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


    async def analyze_sma_strategy_type_2(self, stock) -> None:
        """
        Implements a dual-SMA crossover strategy (13-period and 200-period) with additional filter checks.
        Entry conditions:
          - BUY when close >= sma13 > sma200 and previously sma13 <= sma200
          - SELL when close <= sma13 < sma200 and previously sma13 >= sma200
        Exits and notifications are handled separately.
        """
        # 1. Load historical data
        stock_data = await self.db_con.load_data_from_db(
            stock,
            self.end_date - timedelta(days=10),
            self.end_date
        )
        # Require at least 200 bars for SMA200
        if stock_data.empty or len(stock_data) < 200:
            return

        stock_data = stock_data.sort_values('timestamp').reset_index(drop=True)
        final = stock_data.iloc[-1]

        # 2. Handle exits and update open trades status
        # await self.check_and_execute_exit_trade_type_2(final)
        # await self.update_open_trades_status(stock_data)
        # await asyncio.gather(
        #     self.check_and_execute_exit_trade_type_2(final),
        #     self.update_open_trades_status(stock_data)
        # )

        # 3. Enforce open/loss limits
        # if await self.db_con.check_open_trades_count(self.OPEN_TRADES_LIMIT):
        #     return
        # if await self.db_con.check_loss_trades_count(self.TOTAL_TRADES_LIMIT):
        #     return

        # 4. Compute SMAs
        stock_data['sma13'] = stock_data['close'].rolling(window=13).mean()
        stock_data['sma200'] = stock_data['close'].rolling(window=200).mean()

        # Latest values
        sma13 = stock_data['sma13'].iloc[-1]
        sma200 = stock_data['sma200'].iloc[-1]
        close = final['close']

        # Previous bar values
        prev_sma13 = stock_data['sma13'].iloc[-2]
        prev_sma200 = stock_data['sma200'].iloc[-2]

        # 5. Determine entry signal
        # BUY: sma13 has just crossed above sma200 and price is above sma13
        buy_cond = (prev_sma13 <= prev_sma200) and (sma13 > sma200) and (close >= sma13)
        # SELL: sma13 has just crossed below sma200 and price is below sma13
        sell_cond = (prev_sma13 >= prev_sma200) and (sma13 < sma200) and (close <= sma13)

        if not (buy_cond or sell_cond):
            return

        # direction = "BUY" if buy_cond else "SELL"
        direction = CapitalTransactionType.BUY if buy_cond else CapitalTransactionType.SELL
        level = sma13

        # 6. Fetch current LTP and build order payload
        stock_ltp = await self.get_stock_ltp(stock, final['ltp'])
        payload = await self.get_base_payload_for_capital_order(
            epic=stock,
            stock_ltp=stock_ltp,
            trans_type=direction,
            order_type=CapitalOrderType.MARKET,
            timestamp=final['timestamp'],
            stock_data=stock_data
        )
        if not payload:
            return

        # 7. Deduplicate via Redis
        await asyncio.sleep(random.random())
        key = f"order:{stock}:{final['timestamp']}"
        if not self.redis_cache.client.setnx(key, 1):
            return
        self.redis_cache.client.expire(key, 30)

        # 8. Calculate additional indicators
        rsi, adx, mfi = await asyncio.gather(
            self.calculate_rsi_talib(stock_data),
            self.calculate_adx_talib(stock_data),
            self.calculate_mfi_talib(stock_data)
        )

        # 9. Notify and place order
        await self.send_telegram_notification(
            final,
            IndicatorValues(rsi=rsi, adx=adx, mfi=mfi),
            level,
            payload.stop_loss,
            payload.profit_level,
            direction,
            "SMA_STRATEGY_TYPE_2"
        )

        deal_ref = await self.capital_client.place_order(payload)

        # 10. Log trade
        if LOG_TRADE_TO_DB:
            await self.db_con.log_trade_to_db(
                final,
                IndicatorValues(rsi=rsi, adx=adx, mfi=mfi),
                level,
                payload.stop_loss,
                payload.profit_level,
                direction,
                stock,
                {"sma13": sma13, "sma200": sma200},
                qty=int(payload.quantity),
                order_status=bool(deal_ref),
                order_ids=[deal_ref] if deal_ref else [],
                stock_ltp=stock_ltp
            )

        self.logger.info(f"{stock}: SMA strategy trade executed.")

    # 2. MA Crossover Strategy (15-min bars)
    async def analyze_ma_crossover_strategy_type_2(self, symbol) -> None:
        """
        40/100 MA crossover on 15-min timeframe.
        Entry Long: 40MA crosses above 100MA, gap ≥5 pips & close > crossover bar
        Entry Short: opposite
        SL: 10 pips, TP: 20 pips, optional trailing SL
        """
        df = await self.db_con.load_data_from_db(
            symbol,
            self.end_date - timedelta(days=5),
            self.end_date
        )
        if df.empty:
            return
        df = df.sort_values('timestamp')
        # align to 15-min
        df = await self.transform_data_to_15min(symbol, df)
        if len(df) < 100:
            return
        now = df['timestamp'].iloc[-1]

        df['ma_short'] = df['close'].rolling(40).mean()
        df['ma_long'] = df['close'].rolling(100).mean()
        prev_s, prev_l = df['ma_short'].iloc[-2], df['ma_long'].iloc[-2]
        cur_s, cur_l = df['ma_short'].iloc[-1], df['ma_long'].iloc[-1]

        # Check entry
        direction = None
        if prev_s <= prev_l and cur_s > cur_l and df['close'].iloc[-1] > df['close'].iloc[-2]:
            direction = CapitalTransactionType.BUY
        elif prev_s >= prev_l and cur_s < cur_l and df['close'].iloc[-1] < df['close'].iloc[-2]:
            direction = CapitalTransactionType.SELL
        else:
            return

        entry = df['ltp'].iloc[-1]

        payload = await self.get_base_payload_for_capital_order(
            epic=symbol,
            stock_ltp=entry,
            trans_type=direction,
            order_type=CapitalOrderType.MARKET,
            timestamp=now,
            stock_data=df
        )
        if not payload:
            return

        await asyncio.sleep(random.uniform(0,1))
        key = f"ma:{symbol}:{now}"
        if not self.redis_cache.client.setnx(key,1):
            return
        self.redis_cache.client.expire(key,30)

        indicator_values = IndicatorValues(rsi=0, adx=0, mfi=0)

        await self.send_telegram_notification(df.iloc[-1], indicator_values, entry,
                                             payload.stop_loss,
                                             payload.profit_level,
                                             payload.transaction_type,
                                             "MA_CROSSOVER")
        deal_ref = await self.capital_client.place_order(payload)
        if LOG_TRADE_TO_DB:
            await self.db_con.log_trade_to_db(
                df.iloc[-1],
                indicator_values,
                entry,
                payload.stop_loss,
                payload.profit_level,
                direction,
                symbol,
                metadata_json=None,
                qty=payload.quantity,
                order_status=not self.test_mode,
                order_ids=[deal_ref] if deal_ref else [],
                stock_ltp=str(entry)
            )
        self.logger.info(f"MA Cross: {symbol} {direction.value} @ {entry:.5f}")

    # 3. Mean-Reversion Strategy (15-min bars)
    async def analyze_mean_reversion_strategy(self, symbol) -> None:
        """
        Mean-reversion on 15-min bars using RSI, Bollinger Bands, Z-score.
        Entry Long: RSI<30, price ≤ lower BB, z-score ≤ -1.5
        Entry Short: RSI>70, price ≥ upper BB, z-score ≥ +1.5
        SL: 15 pips or opposite BB touch, TP: revert to mean, Time-exit 30min
        """
        df = await self.db_con.load_data_from_db(
            symbol,
            self.end_date - timedelta(days=5),
            self.end_date
        )
        if df.empty:
            return
        df = df.sort_values('timestamp')
        df = await self.transform_data_to_15min(symbol, df)
        if len(df) < 30:
            return
        now = df['timestamp'].iloc[-1]

        closes = df['close'].values
        rsi = talib.RSI(closes, timeperiod=14)
        upper, middle, lower = talib.BBANDS(closes, timeperiod=20, nbdevup=2, nbdevdn=2)
        mean = pd.Series(closes).rolling(20).mean().iloc[-1]
        std = pd.Series(closes).rolling(20).std().iloc[-1]
        z = (closes[-1] - mean) / std if std>0 else 0
        cur_price = df['ltp'].iloc[-1]

        direction = None
        if rsi[-1] < 30 and cur_price <= lower[-1] and z <= -1.5:
            direction = CapitalTransactionType.BUY
        elif rsi[-1] > 70 and cur_price >= upper[-1] and z >= 1.5:
            direction = CapitalTransactionType.SELL
        else:
            return

        entry = cur_price

        payload = await self.get_base_payload_for_capital_order(
            epic=symbol,
            stock_ltp=entry,
            trans_type=direction,
            order_type=CapitalOrderType.MARKET,
            timestamp=now,
            stock_data=df
        )
        if not payload:
            return

        await asyncio.sleep(random.uniform(0,1))
        key = f"mr:{symbol}:{now}"
        if not self.redis_cache.client.setnx(key,1):
            return
        
        self.redis_cache.client.expire(key,30)

        indicator_values = IndicatorValues(rsi=0, adx=0, mfi=0)

        await self.send_telegram_notification(df.iloc[-1], indicator_values, entry,
                                             payload.stop_loss,
                                             payload.profit_level,
                                             payload.transaction_type,
                                             "MEAN_REVERSION")
        deal_ref = await self.capital_client.place_order(payload)
        if LOG_TRADE_TO_DB:
            await self.db_con.log_trade_to_db(
                df.iloc[-1],
                indicator_values,
                float(entry),
                float(payload.stop_loss),
                float(payload.profit_level),
                direction,
                symbol,
                metadata_json=None,
                qty=int(payload.quantity),
                order_status=not self.test_mode,
                order_ids=[deal_ref] if deal_ref else [],
                stock_ltp=str(entry)
            )
        self.logger.info(f"MeanRev: {symbol} {direction.value} @ {entry:.5f}")

    async def send_telegram_notification(self, stock_data: pd.Series, indicator_values: IndicatorValues, broken_level, sl, pl, trade_type: str = 'BUY', strategy_type: str = None):
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
                f"Strategy: {strategy_type}\n"
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
            f"Strategy: {strategy_type}\n"
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


    # 1. Scalping Strategy (1-minute bars)
    # async def analyze_scalping_strategy(self, symbol) -> None:
    #     """
    #     Scalping on 1-minute timeframe.
    #     Entry Conditions:
    #     - Mid-price shift > 2 pips in 5 seconds
    #     - Order-book imbalance: bidVol/askVol > 1.5 or vice versa
    #     - Spread > 1 pip above 1-min rolling average
    #     - Momentum burst: 10 consecutive upticks or downticks
    #     SL: 1 pip, TP: 2 pips, Time-exit: 10s
    #     """
    #     # Load 1-min data for last 5 minutes + live ticks
    #     df = await self.db_con.load_data_from_db(
    #         symbol,
    #         self.end_date - timedelta(minutes=10),
    #         self.end_date
    #     )
    #     if df.empty:
    #         return

    #     df = df.sort_values('timestamp')
    #     latest = df.iloc[-1]
    #     # Tick-level checks (assuming 'tick_time' and 'tick_price' columns exist)
    #     now = latest['timestamp']
    #     window_start = now - timedelta(seconds=5)
    #     ticks = df[df['timestamp'] >= window_start]
    #     if len(ticks) < 2:
    #         return

    #     mid_prices = (ticks['bid'] + ticks['ask']) / 2
    #     delta = mid_prices.iloc[-1] - mid_prices.iloc[0]
    #     pips = abs(delta) / symbol.pip_value
    #     direction = 'LONG' if delta > 0 else 'SHORT'
    #     if pips < 2:
    #         return

    #     # Order-book imbalance & spread
    #     bid_vol = latest['bid_volume']
    #     ask_vol = latest['ask_volume']
    #     if not (bid_vol/ask_vol > 1.5 or ask_vol/bid_vol > 1.5):
    #         return
    #     spread = latest['ask'] - latest['bid']
    #     avg_spread = df['ask'].sub(df['bid']).rolling('1min').mean().iloc[-1]
    #     if spread <= avg_spread + symbol.pip_to_price(1):
    #         return

    #     # Momentum burst
    #     last_ticks = ticks['tick_price'].diff().dropna()
    #     if not (all(last_ticks > 0) or all(last_ticks < 0)):
    #         return

    #     # Compute entry, SL, TP
    #     entry_price = latest['ltp']
    #     pip = symbol.pip_to_price(1)
    #     sl = entry_price - pip if direction=='LONG' else entry_price + pip
    #     tp = entry_price + 2*pip if direction=='LONG' else entry_price - 2*pip

    #     # Place order payload
    #     payload = await self.get_base_payload_for_capital_order(
    #         epic=symbol,
    #         stock_ltp=entry_price,
    #         trans_type=CapitalTransactionType.BUY if direction=='LONG' else CapitalTransactionType.SELL,
    #         order_type=CapitalOrderType.MARKET,
    #         timestamp=now,
    #         stock_data=df
    #     )
    #     if not payload:
    #         return
    #     payload.stop_loss = sl
    #     payload.profit_level = tp
    #     payload.stop_distance = abs(entry_price - sl)
    #     payload.profit_distance = abs(tp - entry_price)

    #     # Random delay
    #     await asyncio.sleep(random.uniform(0,1))
    #     # Redis lock
    #     key = f"scalp:{symbol}:{now}"
    #     if not self.redis_cache.client.setnx(key,1):
    #         return
    #     self.redis_cache.client.expire(key,30)

    #     # Send notification & place
    #     await self.send_telegram_notification(latest, None, entry_price, sl, tp,
    #                                          payload.transaction_type)
    #     deal_ref = await self.capital_client.place_order(payload)
    #     await self.db_con.log_trade_to_db(latest, None, entry_price, sl, tp,
    #                                       payload.transaction_type, symbol,
    #                                       {"strategy":"SCALPING"},
    #                                       payload.quantity,
    #                                       order_status=not self.test_mode,
    #                                       order_ids=[deal_ref] if deal_ref else [])
    #     self.logger.info(f"Scalping: {symbol} {direction} @ {entry_price:.5f}")
