from datetime import date, datetime, timedelta
from enum import Enum
import json
from typing import Optional
import asyncpg
import os

import pandas as pd

from app.analyse.schemas import CapitalMarketDetails, IndicatorValues
from app.capital.schemas import CapitalTransactionType

DATABASE_URL = os.getenv('DATABASE_URL', '')

class DBConnection:
    def __init__(self, start_date: Optional[date] = None, end_date: Optional[date] = None):
        self.pool = None
        self.start_date = start_date
        self.end_date = end_date

    async def init(self):
        self.pool = await asyncpg.create_pool(dsn=DATABASE_URL)

    async def close(self):
        await self.pool.close()

    async def fetch(self, query, *args):
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args)

    async def fetchrow(self, query, *args):
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, *args)

    async def execute(self, query, *args):
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args)

    async def load_data_from_db(self, stock: str, start_date: date, end_date: date) -> pd.DataFrame:
        query = """
            SELECT stock, timestamp, open, high, low, close, volume, open_interest, ltp
            FROM historical_data
            WHERE stock = $1 AND DATE(timestamp) >= $2 AND DATE(timestamp) <= $3
            ORDER BY timestamp ASC
        """
        data = await self.fetch(query, stock, start_date, end_date)
        if data:
            return pd.DataFrame(data, columns=['stock', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest', 'ltp'])
        return pd.DataFrame(columns=['stock', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest', 'ltp'])

    async def load_data_for_date_from_db(self, stock: str, date_str: str) -> pd.DataFrame:
        query = """
            SELECT stock, timestamp, open, high, low, close, volume, open_interest, ltp
            FROM historical_data
            WHERE stock = $1 AND DATE(timestamp) = $2
            ORDER BY timestamp ASC
        """
        data = await self.fetch(query, stock, date_str)
        if data:
            return pd.DataFrame(data, columns=['stock', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest', 'ltp'])
        return pd.DataFrame(columns=['stock', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest', 'ltp'])
    
    async def delete_data_from_db(self, stock: str, date_str: Optional[date] = None):
        if not date_str:
            date_str = datetime.now().date()
        query = """
            DELETE FROM historical_data
            WHERE stock = $1 AND DATE(timestamp) = $2
        """
        await self.execute(query, stock, date_str)

    async def insert_pivot_data_to_db(self, stock: str, pivot_data: dict):
        start_date, end_date = self.start_date, self.end_date - timedelta(days=1)
        query = """
            INSERT INTO support_resistance_levels (stock, start_date, end_date, pivot_data)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (stock, start_date, end_date) DO UPDATE SET pivot_data = $4
        """
        await self.execute(query, stock, start_date, end_date, json.dumps(pivot_data))

    async def fetch_pivot_data_from_db(self, stock: str):
        start_date, end_date = self.start_date, self.end_date - timedelta(days=1)
        query = """
            SELECT pivot_data
            FROM support_resistance_levels
            WHERE stock = $1 AND start_date = $2 AND end_date = $3
        """
        data = await self.fetch(query, stock, start_date, end_date)
        return json.loads(data[0]['pivot_data']) if data else {}
    
    async def delete_historical_data(self, stock: str):
        query = """
            DELETE FROM historical_data
            WHERE stock = $1
        """
        await self.execute(query, stock)

    async def save_data_to_db(self, data: pd.DataFrame):
        if data.empty:
            return
        
        # data = data.infer_objects(copy=False).fillna(0)
        with pd.option_context("future.no_silent_downcasting", True):
            data = data.fillna(0)

        query = """
            INSERT INTO historical_data (stock, timestamp, open, high, low, close, volume, open_interest, ltp)
            SELECT x.stock, x.timestamp, x.open, x.high, x.low, x.close, x.volume, x.open_interest, x.ltp
            FROM UNNEST($1::text[], $2::timestamp[], $3::float[], $4::float[], $5::float[], $6::float[], $7::int[], $8::float[], $9::float[])
            AS x(stock, timestamp, open, high, low, close, volume, open_interest, ltp)
            ON CONFLICT (stock, timestamp) DO NOTHING
        """
        values = [
            data['stock'].tolist(),
            data['timestamp'].tolist(),
            data['open'].tolist(),
            data['high'].tolist(),
            data['low'].tolist(),
            data['close'].tolist(),
            data['volume'].tolist(),
            data['open_interest'].tolist(),
            data['ltp'].tolist(),
        ]

        async with self.pool.acquire() as connection:
            await connection.execute(query, *values)

    async def fetch_existing_timestamps(self, stock: str):
        """Fetch existing timestamps from the database to avoid re-inserting."""
        query = """
            SELECT timestamp
            FROM historical_data
            WHERE stock = $1 and DATE(timestamp) = $2
        """
        data = await self.fetch(query, stock, self.end_date)
        return [row['timestamp'] for row in data]

    async def log_trade_to_db(self, stock_data, indicator_values: IndicatorValues, broken_level, sl, pl, trade_type: CapitalTransactionType = CapitalTransactionType.BUY, stock_name = None, metadata_json: Optional[dict] = None, qty: int = 0, order_status: bool = False, order_ids: list = [], stock_ltp: Optional[str] = None): # Added deal_id and deal_reference
        if metadata_json is None:
            metadata_json = {}

        stock = stock_data['stock']
        stock_name = stock_name or stock
        ltp = stock_ltp or stock_data['ltp']
        timestamp = stock_data['timestamp']
        query = """
        INSERT INTO order_details (stock, ltp, sl, pl, cp, broke_resistance_level, rsi, adx, mfi, timestamp, entry_price, trade_type, stock_name, metadata_json, qty, order_status, order_ids)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $2, $11, $12, $13, $14, $15, $16)
        """
        # Ensure trade_type is the string value ('BUY' or 'SELL') for DB insertion
        trade_type_value = trade_type.value if isinstance(trade_type, Enum) else trade_type
        values = (stock, ltp, sl, pl, ltp, broken_level, indicator_values.rsi, indicator_values.adx, indicator_values.mfi, timestamp, trade_type_value, stock_name, json.dumps(metadata_json), qty, order_status, json.dumps(order_ids))
        await self.execute(query, *values)

    async def get_historical_data_for_stock(self, stock, timestamp, rows=3):
        query = """
        SELECT stock, timestamp, open, high, low, close, volume, ltp FROM historical_data WHERE stock = $1 AND timestamp <= $2 ORDER BY timestamp DESC LIMIT $3
        """
        rows = await self.fetch(query, stock, timestamp, rows)
        return pd.DataFrame(rows, columns=['stock', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'ltp'])
        
    async def square_off_at_eod(self, stock, cp):
        query = """
        UPDATE order_details
        SET 
            status = CASE
                {status_cases}
            END,
            exit_price = CASE
                {exit_price_cases}
            END,
            cp = $1
        WHERE 
            DATE(timestamp) = $2 
            AND stock = $3
            AND status = 'OPEN';
        """
        
        # Fetch trade_type along with id and ltp
        data = await self.fetch("""
            SELECT id, ltp, trade_type, cp FROM order_details 
            WHERE status = 'OPEN' 
            AND DATE(timestamp) = $1 
            AND stock = $2
        """, self.end_date, stock)

        if data:
            status_cases = []
            for row in data:
                # Handle BUY trades
                if row['trade_type'] == 'BUY':
                    status_cases.extend([
                        f"WHEN id = {row['id']} AND {cp} > {row['cp']} THEN 'PROFIT'",
                        f"WHEN id = {row['id']} AND {cp} < {row['cp']} THEN 'LOSS'",
                        f"WHEN id = {row['id']} THEN 'SQUARED_OFF'"
                    ])
                # Handle SELL trades
                else:
                    status_cases.extend([
                        f"WHEN id = {row['id']} AND {cp} < {row['cp']} THEN 'PROFIT'",
                        f"WHEN id = {row['id']} AND {cp} > {row['cp']} THEN 'LOSS'",
                        f"WHEN id = {row['id']} THEN 'SQUARED_OFF'"
                    ])
            
            exit_price_cases = " ".join(
                f"WHEN id = {row['id']} THEN {cp}" for row in data
            )

            query = query.replace(
                "{status_cases}", " ".join(status_cases)
            ).replace(
                "{exit_price_cases}", exit_price_cases
            )
            
            await self.execute(query, cp, self.end_date, stock)

            
    async def fetch_open_orders(self, stock, trade_date: datetime):
        """
        Fetch open orders for a stock on a specific date.
        """
        query = """
        SELECT * FROM order_details WHERE stock = $1 AND status = 'OPEN' AND DATE(timestamp) = $2 AND order_ids IS NOT NULL AND order_ids != '[]'
        """
        return await self.fetch(query, stock, trade_date)

    async def update_trade_status_by_id(self, executed_trades: list):
        """
        Update the trade statuses in the database for given trade IDs.
        executed_trades: list of tuples [(status, trade_id), ...]
        """
        if not executed_trades:
            return

        # Start the update query
        query = """
        UPDATE order_details
        SET 
            status = CASE 
        """
        for status, trade_id in executed_trades:
            query += f"WHEN id = {trade_id} THEN '{status}' "
        query += "END\n"

        # Add WHERE clause to limit updates to relevant IDs
        ids = ', '.join(str(trade_id) for _, trade_id in executed_trades)
        query += f"WHERE id IN ({ids});"

        try:
            await self.execute(query)
        except asyncpg.exceptions.UndefinedColumnError as e:
            print(f"Failed to update trade statuses due to undefined column: {e}. Query: {query}")
            raise
        except asyncpg.exceptions.PostgresSyntaxError as e:
            print(f"Postgres syntax error: {e}. Query: {query}")
            raise


    async def update_trade_statuses(self, executed_trades: list):
        """
        Update the trade statuses, cp (close price), exit prices, and stock_name in the database.
        """
        if not executed_trades:
            return

        query = """
        UPDATE order_details
        SET 
            status = CASE 
        """
        # Build the CASE for the status column.
        for status, trade_id, cp, exit_price, stock_name in executed_trades:
            query += f"WHEN id = {trade_id} THEN '{status}' "
        query += "END, cp = CASE "

        # Build the CASE for the cp column.
        for status, trade_id, cp, exit_price, stock_name in executed_trades:
            query += f"WHEN id = {trade_id} THEN {cp} "
        query += "END, exit_price = CASE "

        # Build the CASE for the exit_price column.
        for status, trade_id, cp, exit_price, stock_name in executed_trades:
            query += f"WHEN id = {trade_id} THEN {exit_price} "
        query += "END, stock_name = CASE "

        # Build the CASE for the stock_name column.
        for status, trade_id, cp, exit_price, stock_name in executed_trades:
            query += f"WHEN id = {trade_id} THEN '{stock_name}' "
        query += "END WHERE id IN ({ids});"

        # Replace placeholder with comma-separated list of trade ids.
        ids = ', '.join(str(trade_id) for _, trade_id, _, _, _ in executed_trades)
        query = query.replace("{ids}", ids)
        
        try:
            await self.execute(query)
        except asyncpg.exceptions.UndefinedColumnError as e:
            print(f"Failed to update trade statuses due to undefined column: {e}. Query: {query}")
            raise  # Re-raise or handle as needed

    async def check_open_trades_exist(self, stock):
        query = """
        SELECT * FROM order_details WHERE stock = $1 AND status = 'OPEN' AND DATE(timestamp) = $2
        """
        data = await self.fetch(query, stock, self.end_date)
        return data
    async def exited_all_orders_for_the_day(self):
        query = """
        SELECT * FROM order_details WHERE status = 'OPEN' AND DATE(timestamp) = $1
        """
        data = await self.fetch(query, self.end_date)
        return len(data) == 0
    
    async def check_open_trades_count(self, count=10):
        query = """
        SELECT * FROM order_details WHERE status = 'OPEN' AND DATE(timestamp) = $1
        """
        data = await self.fetch(query, self.end_date)
        return len(data) >= count
    
    async def check_loss_trades_count(self, count=5):
        query = """
        SELECT * FROM order_details WHERE status = 'LOSS' AND DATE(timestamp) = $1
        """
        data = await self.fetch(query, self.end_date)
        return len(data) >= count
    
    async def check_trades_count(self, count=10):
        query = """
        SELECT * FROM order_details WHERE DATE(timestamp) = $1
        """
        data = await self.fetch(query, self.end_date)
        return len(data) >= count
    
    async def open_trades_exist(self, count=1):
        query = """
        SELECT COUNT(*) FROM order_details WHERE status = 'OPEN'
        """
        data = await self.fetch(query)
        return data[0]['count'] >= count

    async def delete_data_from_db_for_date(self, date_str: Optional[date] = None):
        if not date_str:
            date_str = datetime.now().date()
        query = """
            DELETE FROM historical_data
            WHERE DATE(timestamp) >= $1
        """
        await self.execute(query, date_str)

    async def get_trade_stats(self, stock, open_count=2, total_count=4):
        query = """
        SELECT 
            COUNT(*) FILTER (WHERE stock = $1 AND status = 'OPEN' AND order_ids IS NOT NULL AND order_ids != '[]') > 0 as has_open_stock,
            COUNT(*) FILTER (WHERE status = 'OPEN' AND order_ids IS NOT NULL AND order_ids != '[]') as open_count,
            COUNT(*) FILTER (WHERE order_ids IS NOT NULL AND order_ids != '[]') as total_count
        FROM order_details
        WHERE DATE(timestamp) = $2
        """
        data = await self.fetchrow(query, stock, self.end_date)

        return (
            data['has_open_stock'],
            data['open_count'] >= open_count,
            data['total_count'] >= total_count
        )

    async def get_daily_trade_analysis(self):
        query = """
            SELECT
                COUNT(*) AS total_trades,
                SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END) AS total_profit_trades,
                SUM(CASE WHEN status = 'LOSS' THEN 1 ELSE 0 END) AS total_loss_trades
            FROM order_details
            WHERE DATE(timestamp) = $1;
        """
        data = await self.fetch(query, self.end_date)
        formatted_data = f"Total Trades: {data[0]['total_trades']}, Profit Trades: {data[0]['total_profit_trades']}, Loss Trades: {data[0]['total_loss_trades']}"
        return formatted_data

    async def update_stock_data_for_historical_data(self, data: pd.Series):
        data: pd.DataFrame = data.to_frame().T
        with pd.option_context("future.no_silent_downcasting", True):
            data = data.fillna(0)

        query = """
            UPDATE historical_data SET open = $1, high = $2, low = $3, close = $4, volume = $5, open_interest = $6, ltp = $7
            WHERE stock = $8 AND timestamp = $9
        """
        values = (
            data['open'].values[0],
            data['high'].values[0],
            data['low'].values[0],
            data['close'].values[0],
            data['volume'].values[0],
            data['open_interest'].values[0],
            data['ltp'].values[0],
            data['stock'].values[0],
            data['timestamp'].values[0]
        )
        await self.execute(query, *values)

    async def update_stock_data_for_historical_data_v2(self, data: pd.Series):
        data: pd.DataFrame = data.to_frame().T
        with pd.option_context("future.no_silent_downcasting", True):
            data = data.fillna(0)

        query = """
            UPDATE historical_data SET ltp = $1
            WHERE stock = $2 AND timestamp = $3
        """
        values = (
            data['ltp'].values[0],
            data['stock'].values[0],
            data['timestamp'].values[0]
        )
        await self.execute(query, *values)

    async def save_active_indicator_values_to_db(self, index_symbol, index_name, timestamp, indicator_values: IndicatorValues):
        """
        Saves or updates active indicator values for a given index in the database.
        """
        query = """
        INSERT INTO index_indicators_data (index_symbol, index_name, timestamp, rsi, adx, mfi, active)
        VALUES ($1, $2, $3, $4, $5, $6, TRUE)
        ON CONFLICT (index_symbol, index_name) DO UPDATE SET 
            timestamp = $3, rsi = $4, adx = $5, mfi = $6, active = TRUE
        """
        values = (index_symbol, index_name, timestamp, indicator_values.rsi, indicator_values.adx, indicator_values.mfi)
        await self.execute(query, *values)

    async def is_index_active(self, index, timestamp):
        """
        Checks if an index is marked as active in the database.
        """
        query = """
        SELECT active FROM index_indicators_data WHERE index_symbol = $1 AND timestamp = $2
        """
        data = await self.fetch(query, index, timestamp)
        return data[0]['active'] if data else False
    
    async def mark_index_as_inactive(self, index):
        """
        Marks an index as inactive in the database.
        """
        query = """
        UPDATE index_indicators_data SET active = FALSE WHERE index_symbol = $1
        """
        await self.execute(query, index)
    
    async def delete_existing_index_data(self):
        """
        Deletes existing index data from the database.
        """
        query = """
        DELETE FROM index_indicators_data
        """
        await self.execute(query)

    async def save_ict_trade_to_db(self, stock, trade_data):
        """
        Saves the ICT trade data to the database.
        """

        is_trade_open = await self.check_open_trades_exist(stock)
        if is_trade_open:
            return

        query = """
        INSERT INTO order_details (stock, ltp, sl, pl, cp, broke_resistance_level, rsi, adx, mfi, timestamp, trade_type)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
        """
        values = (stock, trade_data['cp'], trade_data['sl'], trade_data['pl'], trade_data['cp'], 0,0,0,0, trade_data['timestamp'], trade_data['trade_type'])
        # values = (stock, trade_data['timestamp'], trade_data['cp'], trade_data['sl'], trade_data['pl'], trade_data['trade_type'])
        await self.execute(query, *values)
    
    async def update_sl_and_pl(self, sl: float, pl: float, ltp: float, id: int, metadata_json: dict):
        """CORRECTED METHOD WITH PROPER PARAMETER ORDER"""
        query = """
        UPDATE order_details
        SET sl = $1, pl = $2, ltp = $3, metadata_json = $4
        WHERE id = $5
        """
        await self.execute(query, sl, pl, ltp, json.dumps(metadata_json), id)

    async def fetch_stock_ltp_from_db(self, stock: str):
        query = """
            SELECT ltp
            FROM historical_data
            WHERE stock = $1
            ORDER BY timestamp DESC
            LIMIT 1
        """
        data = await self.fetch(query, stock)
        return float(data[0]['ltp']) if data else None

    async def update_final_order_status(self, id: int, order_status: bool):
        """UPDATED FINAL ORDER STATUS FOR THE ORDER ID"""
        query = """
        UPDATE order_details
        SET order_status = $1
        WHERE id = $2
        """
        await self.execute(query, order_status, id)
    
    async def update_metadata_json(self, id: int, metadata_json: dict):
        """UPDATE METADATA JSON FOR THE ORDER DATA"""
        query = """
        UPDATE order_details
        SET metadata_json = $1
        WHERE id = $2
        """
        await self.execute(query, json.dumps(metadata_json), id)

    async def check_loss_trades_count(self, count=5):
        query = """
        SELECT * FROM order_details WHERE status = 'LOSS' AND DATE(timestamp) = $1
        """
        data = await self.fetch(query, self.end_date)
        return len(data) >= count

    async def get_capital_market_details(self, epic: str, timestamp) -> Optional[CapitalMarketDetails]:
        """Retrieve market details from the database"""

        query = """
            SELECT epic, min_step_distance, min_step_distance_unit, min_deal_size, min_deal_size_unit, max_deal_size, max_deal_size_unit,
                     min_size_increment, min_size_increment_unit, min_guaranteed_stop_distance, min_guaranteed_stop_distance_unit,
                     min_stop_or_profit_distance, min_stop_or_profit_distance_unit, max_stop_or_profit_distance, max_stop_or_profit_distance_unit,
                     decimal_places, margin_factor
            FROM capital_market_details
            WHERE epic = $1 and DATE(created_at) = $2
        """ 

        data = await self.fetch(query, epic, timestamp.date())
        if data:
            return CapitalMarketDetails(
                epic=data[0]['epic'],
                min_step_distance=data[0]['min_step_distance'],
                min_step_distance_unit=data[0]['min_step_distance_unit'],
                min_deal_size=data[0]['min_deal_size'],
                min_deal_size_unit=data[0]['min_deal_size_unit'],
                max_deal_size=data[0]['max_deal_size'],
                max_deal_size_unit=data[0]['max_deal_size_unit'],
                min_size_increment=data[0]['min_size_increment'],
                min_size_increment_unit=data[0]['min_size_increment_unit'],
                min_guaranteed_stop_distance=data[0]['min_guaranteed_stop_distance'],
                min_guaranteed_stop_distance_unit=data[0]['min_guaranteed_stop_distance_unit'],
                min_stop_or_profit_distance=data[0]['min_stop_or_profit_distance'],
                min_stop_or_profit_distance_unit=data[0]['min_stop_or_profit_distance_unit'],
                max_stop_or_profit_distance=data[0]['max_stop_or_profit_distance'],
                max_stop_or_profit_distance_unit=data[0]['max_stop_or_profit_distance_unit'],
                decimal_places=data[0]['decimal_places'],
                margin_factor=data[0]['margin_factor']
            )
        return None