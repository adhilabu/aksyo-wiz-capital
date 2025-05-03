import asyncio
from datetime import date, datetime, time, timedelta
from decimal import Decimal
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
STOCK_PER_PRICE_LIMIT = os.getenv("STOCK_PER_PRICE_LIMIT", 200)
TRADE_ANALYSIS_TYPE = os.getenv("TRADE_ANALYSIS_TYPE", TradeAnalysisType.NORMAL)
NIFTY_50_SYMBOL = 'NSE_INDEX|Nifty 50'
SPLIT_TYPE = int(os.getenv("SPLIT_TYPE", "1"))

class StockIndicatorCalculator:
    TICK_SIZE = 0.01
    TRADE_PERC = 0.007
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
    
    async def initialize_historical_data(self):
        start_time = datetime.now()
        await self.process_combined_stock_data()
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

    def apply_tick_size(self, price: float) -> float:
        """Round the price down to the nearest tick size."""
        return int(price / self.TICK_SIZE) * self.TICK_SIZE

    async def get_stock_ltp(self, stock, current_price):
        stock_ltp = await self.db_con.fetch_stock_ltp_from_db(stock) or current_price
        return self.apply_tick_size(stock_ltp)

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
    
    async def process_combined_stock_data(self):
        for stock in self.epics:
            current_day_data = await self.capital_client.get_current_trading_day_data(stock)
            await self.calculate_pivot_data(current_day_data)
            await self.db_con.save_data_to_db(current_day_data)

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
            id=row[	'id'],
            stock=stock, # EPIC
            entry_ltp=row[	'ltp'],
            entry_cp=row[	'cp'],
            entry_price=row[	'entry_price'],
            sl=row[	'sl'],
            pl=row[	'pl'],
            entry_time=row[	'timestamp'],
            trade_type=row[	'trade_type'], # Ensure this maps to CapitalTransactionType
            tag=f"{stock}_tag", # Tag concept needs review for Capital.com
            metadata_json=json.loads(row[	'metadata_json']) if row[	'metadata_json'] else {},
            order_ids=[], # Deprecated, use deal_id/deal_reference
            deal_id=row.get(	'deal_id	'), # Fetch from DB
            deal_reference=row.get(	'deal_reference	'), # Fetch from DB
            order_status=row[	'order_status'],
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

    async def ensure_market_details(self, epic: str) -> Optional[CapitalMarketDetails]:
        """Ensure market details are available, fetching from API if necessary"""
        market_data = await self.db_con.get_capital_market_details(epic)
        if market_data:
            return market_data

        # Fetch from API if not found in DB
        return await self.capital_client.get_instrument_details(epic)

    async def calculate_sl_for_breakout(self, direction: CapitalTransactionType, entry_price: float, epic: str, details: CapitalMarketDetails) -> float:        
        # Calculate step size in points
        min_step = details.min_step_distance
        if details.min_step_distance_unit == 'PERCENTAGE':
            step = entry_price * (min_step / 100)
        else:
            step = min_step
        
        sl_percent = self.TRADE_PERC  # e.g., 0.8%
        if direction == CapitalTransactionType.BUY:
            desired_sl = entry_price * (1 - sl_percent)
            min_distance = entry_price * (details.min_stop_or_profit_distance / 100)
            max_distance = entry_price * (details.max_stop_or_profit_distance / 100)
            current_distance = entry_price - desired_sl
        else:
            desired_sl = entry_price * (1 + sl_percent)
            min_distance = entry_price * (details.min_stop_or_profit_distance / 100)
            max_distance = entry_price * (details.max_stop_or_profit_distance / 100)
            current_distance = desired_sl - entry_price
        
        # Apply min/max constraints
        if current_distance < min_distance:
            desired_sl = entry_price - min_distance if direction == CapitalTransactionType.BUY else entry_price + min_distance
        elif current_distance > max_distance:
            desired_sl = entry_price - max_distance if direction == CapitalTransactionType.BUY else entry_price + max_distance
        
        # Round to nearest step
        return round(desired_sl / step) * step if step != 0 else desired_sl

    async def calculate_pl_for_breakout(self, direction: CapitalTransactionType, entry_price: float, epic: str, details: CapitalMarketDetails) -> float:        
        # Calculate step size
        min_step = details.min_step_distance
        if details.min_step_distance_unit == 'PERCENTAGE':
            step = entry_price * (min_step / 100)
        else:
            step = min_step
        
        # Strategy PL percentage (double SL)
        pl_percent = self.TRADE_PERC * 2  # e.g., 1.6%
        if direction == CapitalTransactionType.BUY:
            desired_pl = entry_price * (1 + pl_percent)
            min_distance = entry_price * (details.min_stop_or_profit_distance / 100)
            max_distance = entry_price * (details.max_stop_or_profit_distance / 100)
            current_distance = desired_pl - entry_price
        else:
            desired_pl = entry_price * (1 - pl_percent)
            min_distance = entry_price * (details.min_stop_or_profit_distance / 100)
            max_distance = entry_price * (details.max_stop_or_profit_distance / 100)
            current_distance = entry_price - desired_pl
        
        # Apply constraints
        if current_distance < min_distance:
            desired_pl = entry_price + min_distance if direction == CapitalTransactionType.BUY else entry_price - min_distance
        elif current_distance > max_distance:
            desired_pl = entry_price + max_distance if direction == CapitalTransactionType.BUY else entry_price - max_distance
        
        return round(desired_pl / step) * step if step != 0 else desired_pl

    async def get_base_payload_for_capital_order(self, epic: str, stock_ltp: float, 
                                            trans_type: CapitalTransactionType = CapitalTransactionType.BUY, 
                                            order_type: CapitalOrderType = CapitalOrderType.MARKET) -> Optional[BasicPlaceOrderCapital]:
        if stock_ltp <= 0:
            self.logger.warning(f"Invalid LTP {stock_ltp} for {epic}")
            return None
        
        # Fetch instrument details to ensure rules are loaded
        market_details = await self.ensure_market_details(epic)
        
        # Calculate dynamic SL/PL
        stop_loss = await self.calculate_sl_for_breakout(trans_type, stock_ltp, epic, market_details)
        profit_level = await self.calculate_pl_for_breakout(trans_type, stock_ltp, epic, market_details)
        
        # Quantity calculation
        quantity = int(self.stock_per_price_limit // stock_ltp)
        
        # Validate quantity against market rules
        if market_details:
            if quantity < market_details.min_deal_size:
                self.logger.warning(f"Calculated quantity {quantity} is below minimum deal size {market_details.min_deal_size} for {epic}.")
                quantity = int(market_details.min_deal_size)
            elif quantity > market_details.max_deal_size:
                self.logger.warning(f"Calculated quantity {quantity} exceeds maximum deal size {market_details.max_deal_size} for {epic}.")
                quantity = int(market_details.max_deal_size)
            
            # Round to the nearest valid increment
            quantity = int(quantity // market_details.min_size_increment * market_details.min_size_increment)
        
        if quantity <= 0:
            self.logger.warning(f"Calculated quantity is zero or negative for {epic} at LTP {stock_ltp}. Skipping order.")
            return None

        return BasicPlaceOrderCapital(
            quantity=quantity,
            price=stock_ltp if order_type in [CapitalOrderType.LIMIT, CapitalOrderType.STOP] else None,
            epic=epic,
            order_type=order_type,
            transaction_type=trans_type,
            stop_loss=stop_loss,
            profit_level=profit_level
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
        stock_data: pd.DataFrame = await self.db_con.load_data_from_db(stock, self.end_date - timedelta(days=5), self.end_date)
        if stock_data.empty:
            return

        final_stock_data = stock_data.iloc[-1]
        
        # Existing checks remain unchanged
        await self.check_and_execute_exit_trade_type_2(final_stock_data)
        if stock in self.executed_breakouts and self.executed_breakouts[stock]:
            self.logger.info(f"{stock}: Breakout already executed today. Skipping.")
            return
        
        results = await self.db_con.get_trade_stats(stock, open_count=self.OPEN_TRADES_LIMIT, total_count=self.TOTAL_TRADES_LIMIT)
        if any(results):
            self.logger.info(f"SMA: {stock}: Condition met, skipping trade")
            return
        
        current_price = final_stock_data['ltp']
        breakout_direction = None

        # Calculate 13-period and 200-period SMAs using closing prices
        stock_data['sma13'] = stock_data['close'].rolling(window=13).mean()
        stock_data['sma200'] = stock_data['close'].rolling(window=200).mean()
        
        # Extract latest SMA values and closing price
        sma13 = stock_data['sma13'].iloc[-1]
        sma200 = stock_data['sma200'].iloc[-1]
        latest_close = final_stock_data['close']
        prev_close = stock_data['close'].iloc[-2]
        
        # Check if SMAs are valid (not NaN)
        if pd.isna(sma13) or pd.isna(sma200):
            self.logger.info(f"SMA: {stock}: Insufficient data to calculate SMAs. Skipping.")
            return

        breakout_direction = CapitalTransactionType.BUY
        broken_level = sma13  # Using 200 SMA as confirmation level

        if latest_close > sma13 and latest_close > sma200 and prev_close < sma13:
            breakout_direction = CapitalTransactionType.BUY
        elif latest_close < sma13 and latest_close < sma200 and prev_close > sma13:
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
        base_payload = await self.get_base_payload_for_capital_order(
            epic=stock,
            stock_ltp=stock_ltp,
            trans_type=breakout_direction,
            order_type=CapitalOrderType.LIMIT
        )
        
        if not base_payload:
            return
        
        await asyncio.sleep(random.uniform(0, 1))

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
        await self.send_telegram_notification(final_stock_data, indicator_values, broken_level, sl, pl, breakout_direction)

        # Prepare metadata including ATR and SMAs
        metadata_json = {
            "sma13": sma13,
            "sma200": sma200
        }

        # Place order and log trade
        await self.capital_client.place_order(base_payload)
        await self.db_con.log_trade_to_db(
            final_stock_data,
            indicator_values,
            broken_level,
            base_payload.stop_loss,
            base_payload.profit_level,
            breakout_direction,
            stock,
            metadata_json,
            base_payload.quantity,
            order_status=True if self.test_mode else False,
            order_ids=[],
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
