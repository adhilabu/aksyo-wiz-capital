from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime

class ICTStockAnalyzer:
    def __init__(self, historical_data: Optional[pd.DataFrame] = None, pl_ratio: float = 2.0, sl_ratio: float = 1.0):
        """
        Initializes the stock analyzer.

        Args:
            historical_data (pd.DataFrame): Historical 1-minute OHLC data with 'timestamp', 'open', 'high', 'low', 'close'.
            pl_ratio (float): Profit target ratio (default is 2x the stop-loss).
            sl_ratio (float): Stop-loss ratio (default is 1x the risk).
        """
        self.historical_data = historical_data
        self.pl_ratio = pl_ratio
        self.sl_ratio = sl_ratio

    def identify_ict_zones(self, data: pd.DataFrame) -> dict:
        """
        Identifies ICT zones such as order blocks, fair value gaps (FVGs), and liquidity zones.

        Args:
            data (pd.DataFrame): 1-minute OHLC data.

        Returns:
            dict: Key ICT levels and zones.
        """
        high = data['high'].max()
        low = data['low'].min()
        equilibrium = (high + low) / 2  # ICT's Equilibrium Level

        # Identifying Fair Value Gap (FVG)
        fvg_up = data[(data['low'].shift(-1) > data['high'])]
        fvg_down = data[(data['high'].shift(-1) < data['low'])]

        return {
            'high': high,
            'low': low,
            'equilibrium': equilibrium,
            'fvg_up': fvg_up,
            'fvg_down': fvg_down,
        }

    def analyze_and_trade(self, current_day_data: pd.DataFrame):
        """
        Analyzes real-time data, identifies trade opportunities, and executes trades.

        Args:
            current_day_data (pd.DataFrame): Real-time 1-minute OHLC data.

        Returns:
            dict: Trade details if a trade is executed, else an empty dictionary.
        """
        current_trade = {}

        # Split the data into the last row and the rest
        last_row = current_day_data.iloc[-1]
        excluding_last_row_data = current_day_data.iloc[:-1]

        # Identify ICT zones
        ict_zones = self.identify_ict_zones(excluding_last_row_data)

        # Check if the current price is within FVG zones
        if not ict_zones['fvg_down'].empty:
            for _, fvg in ict_zones['fvg_down'].iterrows():
                if fvg['low'] <= last_row['low'] <= fvg['high']:
                    # Long setup
                    entry_price = last_row['close']
                    stop_loss = ict_zones['low']
                    take_profit = entry_price + (entry_price - stop_loss) * self.pl_ratio

                    current_trade = {
                        'cp': entry_price,
                        'sl': stop_loss,
                        'pl': take_profit,
                        'trade_type': 'buy',
                        'timestamp': last_row['timestamp'],
                    }
                    return current_trade

        if not ict_zones['fvg_up'].empty:
            for _, fvg in ict_zones['fvg_up'].iterrows():
                if fvg['low'] <= last_row['low'] <= fvg['high']:
                    # Short setup
                    entry_price = last_row['close']
                    stop_loss = ict_zones['high']
                    take_profit = entry_price - (stop_loss - entry_price) * self.pl_ratio

                    current_trade = {
                        'cp': entry_price,
                        'sl': stop_loss,
                        'pl': take_profit,
                        'trade_type': 'sell',
                        'timestamp': last_row['timestamp'],
                    }
                    return current_trade

        return current_trade

    def find_similar_trading_days(self, current_day_data: pd.DataFrame, similarity_threshold: float = 0.9) -> list:
        """
        Finds similar trading days to the current day's pattern.

        Args:
            current_day_data (pd.DataFrame): Current day's 1-minute OHLC data.
            similarity_threshold (float): Threshold for cosine similarity.

        Returns:
            list: Dates of similar trading patterns.
        """
        current_pattern = current_day_data[['open', 'high', 'low', 'close']].values.flatten()
        current_time_limit = current_day_data['timestamp'].max()
        historical_data = self.historical_data[self.historical_data['timestamp'] <= current_time_limit]

        historical_data['date'] = historical_data['timestamp'].dt.date
        unique_dates = historical_data['date'].unique()
        similar_days = []

        for date in unique_dates:
            day_data = historical_data[historical_data['date'] == date]
            day_pattern = day_data[day_data['timestamp'].dt.time <= current_time_limit.time()][['open', 'high', 'low', 'close']].values.flatten()

            if len(day_pattern) != len(current_pattern):
                continue

            similarity = np.dot(
                current_pattern, day_pattern
            ) / (np.linalg.norm(current_pattern) * np.linalg.norm(day_pattern))

            if similarity >= similarity_threshold:
                similar_days.append(date)

        return similar_days