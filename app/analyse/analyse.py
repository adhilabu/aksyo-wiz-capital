import asyncio
from datetime import date, datetime, time, timedelta
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
from app.analyse.schemas import IndicatorValues, StockType, Trade, TradeAnalysisType, TradeStatus
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
WS_FEED_TYPE = os.getenv("WS_FEED_TYPE")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
HISTORY_DATA_PERIOD = int(os.getenv("HISTORY_DATA_PERIOD", 100))
TELEGRAM_NOTIFICATION = os.getenv("TELEGRAM_NOTIFICATION", "False")
STOCK_PER_PRICE_LIMIT = os.getenv("STOCK_PER_PRICE_LIMIT", 200000)
TRADE_ANALYSIS_TYPE = os.getenv("TRADE_ANALYSIS_TYPE", TradeAnalysisType.NORMAL)
NIFTY_50_SYMBOL = 'NSE_INDEX|Nifty 50'
SPLIT_TYPE = int(os.getenv("SPLIT_TYPE", "1"))

def get_stocks_without_futures(stocks: list , instrument_future_map: dict):
    futures = instrument_future_map.values()
    return [s for s in stocks if s not in futures]

class StockIndicatorCalculator:
    CACHE_FILE = "market_holidays_cache.json"
    TICK_SIZE = 0.05
    TRADE_PERC = 0.007
    MAX_ADJUSTMENTS = 3
    TOTAL_TRADES_LIMIT = 10
    OPEN_TRADES_LIMIT = 4
    LOSS_TRADE_LIMIT = 5

    def __init__(self, db_connection: DBConnection, redis_cache: RedisCache, start_date=None, end_date=None, test_mode=False):
        self.db_con: DBConnection = db_connection
        self.redis_cache: RedisCache = redis_cache
        self.logger = setup_logger("StockIndicatorLogger", "stock_indicators", LOGGING_LEVEL)
        self.start_date = start_date
        self.end_date = end_date
        self.test_mode = test_mode
        self.capital_client = CapitalAPI(db_conn=db_connection)
        # Instrument handling needs update for Capital.com EPICs
        self.epics = get_capital_epics() # Assuming get_capital_epics exists and returns list of EPICs
        # self.stock_name_map = get_stock_instrument_name_map(self.stocks) # Needs Capital.com equivalent if used
        # self.stock_name_map = {epic: epic for epic in self.epics} # Simple map for now
        self.filename = self.get_notes_filename()
        # self.indices_stocks_map, self.stock_index_map = get_indices_stocks_map() # Needs Capital.com equivalent
        # Placeholder for index mapping if needed
        # self.indices_epics_map = {} # e.g., {"IX.D.NIFTY.DAILY.IP": ["EPIC1", "EPIC2"]}
        # self.epic_index_map = {} # e.g., {"EPIC1": "IX.D.NIFTY.DAILY.IP"}
        # Update NIFTY_50_SYMBOL if needed for Capital.com
        # NIFTY_50_SYMBOL = "IX.D.NIFTY.DAILY.IP" # Example EPIC for Nifty 50

        self.trade_analysis_type = TRADE_ANALYSIS_TYPE
        self.stock_train_data_map: dict[str, Any] = {}
        self.stock_reversal_data: dict[str, Any] = {}  # {stock_symbol: (high, low, timestamp)}
        self.index_reversal_data: dict[str, Any] = {}  # {index_symbol: (high, low, timestamp)}
        self.executed_breakouts = {}

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
        # await self.fetch_historical_data()
        # await self.fetch_historical_data_day(self.end_date)
        await self.db_con.delete_existing_index_data()
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
        #     await self.update_with_semaphore(row)
        tasks = [self.update_stock_data(row) for _, row in transformed_data.iterrows()]
        await asyncio.gather(*tasks)
        timestamp = transformed_data.iloc[0]['timestamp'] if len(transformed_data) > 0 else 'Empty'
        self.logger.info(f"Inserted data for timestamp: {timestamp}.")

    async def update_stock_data(self, data: pd.Series):
        stock = data['stock']
        stock_type = self.get_stock_type(stock)
        timestamp = pd.to_datetime(data['timestamp'])
        existing_timestamps = await self.db_con.fetch_existing_timestamps(stock)
        if timestamp in existing_timestamps:
            await self.db_con.update_stock_data_for_historical_data_v2(data)
            return

        self.logger.info(f"Inserting data for stock: {stock} and timestamp: {timestamp}.")
        await self.db_con.save_data_to_db(data.to_frame().T)
        if stock_type == StockType.INDEX:
            return

        await self.analyze_reversal_breakout_strategy(stock)
        return
    
    def check_the_date_is_weekend(self, date_str: str) -> bool:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        return date.weekday() in [5, 6]
    
    def load_cache(self):
        """Loads the cached holidays data from the file."""
        if os.path.exists(self.CACHE_FILE):
            with open(self.CACHE_FILE, 'r') as f:
                return json.load(f)
        return []

    def save_cache(self, holidays):
        """Saves the holidays data to the cache file."""
        with open(self.CACHE_FILE, 'w') as f:
            json.dump(holidays, f)
    
    def check_the_date_is_holiday(self, date_str: str) -> bool:
        market_holidays = self.load_cache()
        if not market_holidays:
            market_holidays = [
                "2024-01-26",
                "2024-03-08",
                "2024-03-29",
                "2024-04-19",
                "2024-05-01",
                "2024-08-15",
                "2024-08-22",
                "2024-09-05",
                "2024-10-02",
                "2024-10-08",
                "2024-10-27",
                "2024-10-28",
                "2024-11-12",
                "2024-12-25",
                "2024-11-15",
                "2024-11-20",
            ]
        
            cy_holidays_data = self.capital_client.get_market_holidays() # Capital.com might not support this
            # Assuming cy_holidays_data is a list of date strings if supported, else empty
            cy_holidays = cy_holidays_data if isinstance(cy_holidays_data, list) else [] 
            # cy_holidays = [data.get("date") for data in cy_holidays_data.get("data", [])] # Original Upstox structure

            market_holidays.extend(cy_holidays)
            self.save_cache(market_holidays)
        
        return date_str in market_holidays

    def get_missing_dates(self, start_date: str, end_date: str, cached_data: pd.DataFrame) -> list:
        date_range = pd.date_range(start=start_date, end=end_date)
        cached_dates = pd.to_datetime(cached_data['timestamp']).dt.date.unique() if not cached_data.empty else []
        missing_dates = [d.strftime("%Y-%m-%d") for d in date_range if d.date() not in cached_dates]
        if not missing_dates:
            return missing_dates
        
        valid_missing_date = []
        for date in missing_dates:
            if self.check_the_date_is_weekend(date) or self.check_the_date_is_holiday(date):
                continue
            valid_missing_date.append(date)
        return valid_missing_date
    
    def is_trading_day(self, date_str: str = None) -> bool:
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")
        return not self.check_the_date_is_weekend(date_str) and not self.check_the_date_is_holiday(date_str)
    
    def is_current_day_and_trading_day(self, date_str: str = None) -> bool:
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")
        return date_str == datetime.now().strftime("%Y-%m-%d") and self.is_trading_day(date_str)

    def apply_tick_size(self, price: float) -> float:
        """Round the price down to the nearest tick size."""
        return int(price / self.TICK_SIZE) * self.TICK_SIZE

    async def get_stock_ltp(self, stock, current_price):
        stock_ltp = await self.db_con.fetch_stock_ltp_from_db(stock) or current_price
        return self.apply_tick_size(stock_ltp)

    async def transform_candle_data_to_df(self, stock: str, candles: dict):
        transformed_data = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest'])
        transformed_data['timestamp'] = pd.to_datetime(transformed_data['timestamp'])
        transformed_data['timestamp'] = transformed_data['timestamp'].dt.tz_localize(None)
        transformed_data['ltp'] = transformed_data['close']
        transformed_data['stock'] = stock
        transformed_data = transformed_data[['stock', 'close', 'volume', 'high', 'low', 'timestamp', 'ltp', 'open', 'open_interest']]
        return transformed_data

    def clean_empty_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(axis=1, how='all')

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
    
    async def process_stock_data(self, stock, start_date, end_date, is_trading_day, current_time):
        """
        Asynchronous helper function to fetch and process data for a single stock.
        """
        self.logger.info(f"Fetching historical data for stock: {stock}.")
        cached_data = await self.db_con.load_data_from_db(stock, start_date, end_date)
        missing_dates = self.get_missing_dates(start_date, end_date, cached_data)
        
        # Handle current day trading data if applicable
        if is_trading_day and current_time.hour >= 9 and not self.test_mode:
            await self.db_con.delete_data_from_db(stock)
            current_day_data_df = await self.capital_client.get_current_trading_day_data(stock)
            # current_day_td_data = await self.transform_candle_data_to_df(stock, current_day_data) # Transformation now happens in actions.py
            await self.db_con.save_data_to_db(current_day_data_df)

        # If no missing dates, calculate pivot data and exit
        if not missing_dates:
            await self.calculate_pivot_data(cached_data)
            self.logger.info(f"Data already exists for stock: {stock}.")
            return

        # Fetch historical data for missing dates
        missing_start_date = min(missing_dates)
        missing_end_date = max(missing_dates)
        stock_data_df = await self.capital_client.get_historical_data(stock, missing_start_date, missing_end_date)
        # transformed_data = await self.transform_candle_data_to_df(stock, stock_data) # Transformation now in actions.py
        await self.calculate_pivot_data(stock_data_df)
        await self.db_con.save_data_to_db(stock_data_df)
        self.logger.info(f"Data fetched for stock: {stock}.")

    async def fetch_historical_data(self, batch_size=50):
        """
        Main function to fetch and process historical data for multiple stocks asynchronously, in batches.
        """
        
        start_date, end_date = self.set_and_get_dates(for_historical_data=True)
        is_trading_day = self.is_trading_day()
        current_time = datetime.now(pytz.timezone('Asia/Kolkata')).time()
        batches = [self.stocks[i:i + batch_size] for i in range(0, len(self.stocks), batch_size)]
        update_start_time = datetime.now()
        for batch in batches:
            tasks = [
                self.process_stock_data(
                    stock, start_date, end_date, is_trading_day, current_time,
                )
                for stock in batch
            ]
            await asyncio.gather(*tasks)
            await asyncio.sleep(1)
        update_end_time = datetime.now()
        self.logger.info(f"Fetched historical data and inserted in {update_end_time - update_start_time} seconds.")


    async def process_stock_for_day(self, stock, date, is_trading_day):
        """
        Processes historical data for a single stock on a specific date.
        """
        try:
            await self.db_con.delete_data_from_db(stock, date)
            
            # Fetch historical or current day data using CapitalAPI
            stock_data_df = await self.capital_client.get_historical_data(stock, date.strftime("%Y-%m-%d"), date.strftime("%Y-%m-%d"))
            if stock_data_df.empty and is_trading_day:
                stock_data_df = await self.capital_client.get_current_trading_day_data(stock)
            elif stock_data_df.empty:
                self.logger.info(f"Data not available for stock: {stock} on date: {date}.")
                return pd.DataFrame()

            # Transform data to a DataFrame (already done in actions.py)
            # transformed_data = await self.transform_candle_data_to_df(stock, stock_data)
            transformed_data = self.clean_empty_columns(stock_data_df) # Clean if needed
            self.logger.info(f"Data processed for stock: {stock}.")
            return transformed_data
        except Exception as e:
            self.logger.error(f"Error processing data for stock: {stock}. Error: {e}")
            return pd.DataFrame()

    async def fetch_historical_data_day(self, date: date, batch_size=100) -> pd.DataFrame:
        """
        Fetches and processes historical data for all stocks on a specific date.

        Parameters:
        - date (date): Date for which data needs to be fetched.
        - batch_size (int): Number of stocks to process concurrently in each batch.

        Returns:
        - pd.DataFrame: Combined DataFrame of all transformed data.
        """
        is_trading_day = self.is_current_day_and_trading_day(date.strftime("%Y-%m-%d"))
        combined_transformed_data = pd.DataFrame()
        batches = [self.stocks[i:i + batch_size] for i in range(0, len(self.stocks), batch_size)]

        for batch in batches:
            tasks = [
                self.process_stock_for_day(
                    stock, date, is_trading_day
                )
                for stock in batch
            ]

            results = await asyncio.gather(*tasks)
            for result in results:
                if not result.empty:
                    combined_transformed_data = (
                        result
                        if combined_transformed_data.empty
                        else pd.concat([combined_transformed_data, result], ignore_index=True)
                    )

            await asyncio.sleep(1)

        return combined_transformed_data

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

    async def get_base_payload_for_capital_order(self, epic: str, stock_ltp: float, stop_loss: float, profit_level: Optional[float] = None, trans_type: CapitalTransactionType = CapitalTransactionType.BUY, order_type: CapitalOrderType = CapitalOrderType.MARKET, quantity: int = None) -> Optional[BasicPlaceOrderCapital]:
        """Creates the base payload object for placing a Capital.com order."""
        if stock_ltp <= 0:
            self.logger.warning(f"Invalid LTP {stock_ltp} for {epic}. Cannot calculate quantity.")
            return None
            
        if stock_ltp > self.stock_per_price_limit:
            self.logger.info(f"Price: {stock_ltp} is greater than the stock {epic} per price limit: {self.stock_per_price_limit}.")
            return None
        
        # Calculate quantity based on price limit if not provided
        quantity = int(self.stock_per_price_limit // stock_ltp) if quantity is None else quantity
        if quantity <= 0:
             self.logger.warning(f"Calculated quantity is zero or negative for {epic} at LTP {stock_ltp}. Skipping order.")
             return None

        # Use the specific price for LIMIT/STOP orders, LTP might be used differently
        price_for_order = stock_ltp if order_type in [CapitalOrderType.LIMIT, CapitalOrderType.STOP] else None 

        return BasicPlaceOrderCapital(
            quantity=quantity,
            price=price_for_order, # Price level for LIMIT/STOP orders
            epic=epic,
            order_type=order_type,
            transaction_type=trans_type,
            stop_loss=stop_loss,
            profit_level=profit_level # Add profit level if available
        )
        
    def follows_the_trend(self, indicator_values: IndicatorValues):
        """ Check if the stock follows the trend based on the indicator values. """
        return indicator_values.rsi > 70 and indicator_values.adx > 25 and (indicator_values.mfi > 80 or indicator_values.stock_type == StockType.INDEX)

    def get_stock_type(self, stock: str):
        indices: list = list(self.indices_stocks_map.keys()) + [NIFTY_50_SYMBOL]
        if stock in indices:
            return StockType.INDEX
        return StockType.EQUITY

    async def check_and_execute_exit_trade_type_2(self, stock_data: pd.DataFrame):
        """Check and execute exit conditions for open trades based on strategy type 2."""
        current_timestamp = stock_data[	'timestamp	']
        stock = stock_data[	'stock	'] # EPIC
        current_ltp = stock_data[	'ltp	']
        current_close = stock_data[	'close	']
        current_low = stock_data[	'low	']
        current_high = stock_data[	'high	']
        stock_name = self.stock_name_map.get(stock, stock)

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
                    if confirmation and confirmation.get(	'dealStatus	') == 	'ACCEPTED	':
                        trade.deal_id = confirmation.get(	'dealId	') # Get the actual dealId
                        trade.order_status = True
                        confirmed = True
                        # Update DB with dealId and status
                        await self.db_con.update_trade_confirmation(trade.id, trade.deal_id, True)
                    elif confirmation and confirmation.get(	'dealStatus	') == 	'REJECTED	':
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
                                exit_price, stock_name # stock_name might not be needed here
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
            id=row[	'id	'],
            stock=stock, # EPIC
            entry_ltp=row[	'ltp	'],
            entry_cp=row[	'cp	'],
            entry_price=row[	'entry_price	'],
            sl=row[	'sl	'],
            pl=row[	'pl	'],
            entry_time=row[	'timestamp	'],
            trade_type=row[	'trade_type	'], # Ensure this maps to CapitalTransactionType
            tag=f"{stock}_tag", # Tag concept needs review for Capital.com
            metadata_json=json.loads(row[	'metadata_json	']) if row[	'metadata_json	'] else {},
            order_ids=[], # Deprecated, use deal_id/deal_reference
            deal_id=row.get(	'deal_id	'), # Fetch from DB
            deal_reference=row.get(	'deal_reference	'), # Fetch from DB
            order_status=row[	'order_status	'],
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

    async def calculate_sl_for_breakout(self, direction, entry_price):
        """Calculate SL as 0.8% of entry price"""
        sl_percent = self.TRADE_PERC
        if direction == CapitalTransactionType.BUY:
            return entry_price * (1 - sl_percent)
        return self.apply_tick_size(entry_price * (1 + sl_percent))

    async def calculate_pl_for_breakout(self, direction, entry_price):
        """Calculate PL as 0.8% of entry price"""
        pl_percent = self.TRADE_PERC
        if direction == CapitalTransactionType.BUY:
            return entry_price * (1 + pl_percent)
        return self.apply_tick_size(entry_price * (1 - pl_percent))

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
    
        if (final_timestamp.minute + 1) % 5 != 0:
            return
        
        if final_timestamp <= final_timestamp.replace(hour=9, minute=23):
            return

        if final_timestamp >= final_timestamp.replace(hour=11, minute=00):
            return

        results = await self.db_con.get_trade_stats(stock, open_count=self.OPEN_TRADES_LIMIT, total_count=self.TOTAL_TRADES_LIMIT)
        if any(results):
            self.logger.info(f"HN :{stock}: Condition met, skipping trade")
            return

        # Existing data processing remains unchanged
        stock_data_5_min = await self.transform_1_min_data_to_5_min_data(stock, stock_data)
        if stock_data_5_min.empty:
            return
        
        just_created, reversal_data = await self.get_or_calculate_reversal_data(stock, stock_data_5_min)
        if not reversal_data:
            return
        
        if just_created:
            return
        
        reversal_high, reversal_low, reversal_time = reversal_data
        if reversal_time == final_timestamp:
            return
        
        current_price = final_stock_data['ltp']
        breakout_direction = None
        
        if current_price > reversal_high:
            breakout_direction = CapitalTransactionType.BUY # Use Capital enum
            breakout_level = reversal_high
        elif current_price < reversal_low:
            breakout_direction = CapitalTransactionType.SELL # Use Capital enum
            breakout_level = reversal_low
        else:
            return
        
        # if not await self.valid_market_sentiment(breakout_direction):
        #     self.logger.info(f"HN {stock}: Market sentiment does not confirm the breakout direction")
        #     return

        # Mark breakout as executed for today
        self.executed_breakouts[stock] = True

        if not await self.check_index_confirmation(stock, breakout_direction, reversal_time):
            self.logger.info(f"HN {stock}: Index does not confirm the breakout direction")
            return
        
        pivot_broken, broken_level = await self.last_close_price_broke_resistance(stock_data_5_min, breakout_direction)
        if not pivot_broken:
            self.logger.info(f"HN {stock}: Didn't broke any pivot levels")
            return
        

        # Calculate dynamic position size based on volatility
        atr = await self.calculate_atr(stock_data_5_min)
        
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
        
        stock_name = self.stock_name_map.get(stock, stock)

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
        #     self.logger.info(f"HN Max trades (2) reached for {final_timestamp}. Skipping.")
        #     return

        # Check existing open trades
        if await self.db_con.check_open_trades_count(self.OPEN_TRADES_LIMIT):
            self.logger.info("HN: Max open trades reached. Skipping.")
            # self.redis_cache.client.delete(stock_ts_key)
            # self.redis_cache.client.decr(ts_count_key)
            return

        # Check existing open trades
        if await self.db_con.check_loss_trades_count(self.LOSS_TRADE_LIMIT):
            self.logger.info("HN: Max loss trades reached. Skipping.")
            # self.redis_cache.client.delete(stock_ts_key)
            # self.redis_cache.client.decr(ts_count_key)
            return
        
        # Place order and log trade
        rsi, adx, mfi = await asyncio.gather(
            self.calculate_rsi_talib(stock_data_5_min),
            self.calculate_adx_talib(stock_data_5_min),
            self.calculate_mfi_talib(stock_data_5_min)
        )
        
        indicator_values = IndicatorValues(rsi=rsi, adx=adx, mfi=mfi)
        await self.send_telegram_notification(final_stock_data, indicator_values, broken_level, sl, pl, breakout_direction)
        if self.is_invalid_momentum_trade(indicator_values):
            self.logger.info("HN: Invalid trade based on momentum. Skipping.")
            return

        metadata_json = {
            "atr": atr
        }

        order_ids = await self.upstox_client.place_order_v3(base_payload)
        await self.db_con.log_trade_to_db(
            final_stock_data,
            indicator_values,
            broken_level,
            sl,
            pl,
            breakout_direction,
            stock_name,
            metadata_json,
            base_payload.quantity,
            order_status=True if self.test_mode else False,
            order_ids=order_ids,
            stock_ltp=stock_ltp
        )

        self.logger.info(f"HN : High-probability trade executed for {stock}: ")
