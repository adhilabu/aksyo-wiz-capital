import datetime
import pandas as pd
import numpy as np
from math import floor

class MovingAverageCrossoverStrategy:
    def __init__(self, risk_per_trade=100, slowMA_period=50, fastMA_period=21):
        """
        Initialize the MovingAverageCrossoverStrategy.

        Args:
            risk_per_trade: Risk per trade in INR (default: 100).
            slowMA_period: Period for slow moving average (default: 50).
            fastMA_period: Period for fast moving average (default: 21).
        """
        self.risk_per_trade = risk_per_trade
        self.slowMA_period = slowMA_period
        self.fastMA_period = fastMA_period
        self.orderslist = []

    def calculate_moving_averages(self, df):
        """
        Calculate fast and slow moving averages.

        Args:
            df: DataFrame containing historical data.

        Returns:
            df: DataFrame with moving averages added.
        """
        df["FMA"] = df['close'].rolling(self.fastMA_period).mean()
        df["SMA"] = df['close'].rolling(self.slowMA_period).mean()
        return df

    def analyze_and_trade(self, current_data: pd.DataFrame):
        """
        Analyzes the current market data and returns a trade decision based on moving average crossover.

        Args:
            current_data: Current market data as a pandas DataFrame.

        Returns:
            current_trade: A dictionary containing trade details if a trade is taken, otherwise None.
        """
        if current_data is None or len(current_data) < max(self.slowMA_period, self.fastMA_period):
            raise ValueError("Insufficient data to calculate moving averages.")

        # Calculate moving averages
        current_data = self.calculate_moving_averages(current_data)

        # Get moving averages and their difference
        FMA = current_data.FMA.values[-10:]
        SMA = current_data.SMA.values[-10:]
        MA_diff = SMA - FMA

        # Get latest close price and stop loss levels
        lastclose = current_data.close.values[-1]
        stoploss_buy = current_data.low.values[-3]  # Third last candle as stop loss
        stoploss_sell = current_data.high.values[-3]  # Third last candle as stop loss

        # Adjust stop loss levels
        if stoploss_buy > lastclose * 0.996:
            stoploss_buy = lastclose * 0.996  # Minimum stop loss as 0.4%
        if stoploss_sell < lastclose * 1.004:
            stoploss_sell = lastclose * 1.004  # Minimum stop loss as 0.4%

        print(f"Last Close: {lastclose}, FMA: {FMA[-1]}, SMA: {SMA[-1]}, MA_diff: {MA_diff[-1]}")

        # Buy signal: Fast MA crosses above Slow MA
        if MA_diff[-1] > 0:  # and MA_diff[-3] < 0:
            stoploss_buy = lastclose - stoploss_buy
            quantity = floor(max(1, (self.risk_per_trade / stoploss_buy)))
            target = stoploss_buy * 3  # Risk reward as 3
            price = int(100 * (floor(lastclose / 0.05) * 0.05)) / 100
            stoploss_buy = int(100 * (floor(stoploss_buy / 0.05) * 0.05)) / 100
            quantity = int(quantity)
            target = int(100 * (floor(target / 0.05) * 0.05)) / 100

            current_trade = {
                'cp': lastclose,  # Entry price
                'sl': stoploss_buy,  # Stop loss
                'pl': target,  # Take profit
                'trade_type': 'buy',
                'timestamp': current_data['date'].iloc[-1],
                'quantity': quantity,
            }
            return current_trade

        # Sell signal: Fast MA crosses below Slow MA
        elif MA_diff[-1] < 0 and MA_diff[-3] > 0:
            stoploss_sell = stoploss_sell - lastclose
            quantity = floor(max(1, (self.risk_per_trade / stoploss_sell)))
            target = stoploss_sell * 3  # Risk reward as 3
            price = int(100 * (floor(lastclose / 0.05) * 0.05)) / 100
            stoploss_sell = int(100 * (floor(stoploss_sell / 0.05) * 0.05)) / 100
            quantity = int(quantity)
            target = int(100 * (floor(target / 0.05) * 0.05)) / 100

            current_trade = {
                'cp': lastclose,  # Entry price
                'sl': stoploss_sell,  # Stop loss
                'pl': target,  # Take profit
                'trade_type': 'sell',
                'timestamp': current_data['date'].iloc[-1],
                'quantity': quantity,
            }
            return current_trade

        else:
            print("No trade taken. Moving average crossover signal not detected.")
            return None