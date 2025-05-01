import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

class TradingDayAnalyzerSuperTrend:
    def __init__(self, probability_threshold=0.7, future_period=5, supertrend_period=10, supertrend_multiplier=3):
        """
        Initialize the TradingDayAnalyzer.

        Args:
            probability_threshold: Minimum probability to take a trade (default: 0.7).
            future_period: Number of candles to look ahead for profit calculation (default: 5).
            supertrend_period: Period for SuperTrend calculation (default: 10).
            supertrend_multiplier: Multiplier for SuperTrend calculation (default: 3).
        """
        self.probability_threshold = probability_threshold
        self.future_period = future_period
        self.supertrend_period = supertrend_period
        self.supertrend_multiplier = supertrend_multiplier

    def calculate_supertrend(self, df):
        """
        Calculate the SuperTrend indicator for the given DataFrame.

        Args:
            df: DataFrame containing OHLC data.

        Returns:
            df: DataFrame with SuperTrend columns added.
        """
        # Calculate ATR
        df['hl'] = df['high'] - df['low']
        df['hpc'] = abs(df['high'] - df['close'].shift())
        df['lpc'] = abs(df['low'] - df['close'].shift())
        df['tr'] = df[['hl', 'hpc', 'lpc']].max(axis=1)
        df['atr'] = df['tr'].rolling(window=self.supertrend_period).mean()

        # Calculate SuperTrend
        df['basic_ub'] = (df['high'] + df['low']) / 2 + self.supertrend_multiplier * df['atr']
        df['basic_lb'] = (df['high'] + df['low']) / 2 - self.supertrend_multiplier * df['atr']

        df['final_ub'] = 0.0
        df['final_lb'] = 0.0
        for i in range(self.supertrend_period, len(df)):
            df.loc[df.index[i], 'final_ub'] = (
                df.loc[df.index[i], 'basic_ub']
                if df.loc[df.index[i], 'basic_ub'] < df.loc[df.index[i - 1], 'final_ub']
                or df.loc[df.index[i - 1], 'close'] > df.loc[df.index[i - 1], 'final_ub']
                else df.loc[df.index[i - 1], 'final_ub']
            )
            df.loc[df.index[i], 'final_lb'] = (
                df.loc[df.index[i], 'basic_lb']
                if df.loc[df.index[i], 'basic_lb'] > df.loc[df.index[i - 1], 'final_lb']
                or df.loc[df.index[i - 1], 'close'] < df.loc[df.index[i - 1], 'final_lb']
                else df.loc[df.index[i - 1], 'final_lb']
            )

        df['supertrend'] = 0.0
        for i in range(self.supertrend_period, len(df)):
            df.loc[df.index[i], 'supertrend'] = (
                df.loc[df.index[i], 'final_ub']
                if df.loc[df.index[i - 1], 'supertrend'] == df.loc[df.index[i - 1], 'final_ub']
                and df.loc[df.index[i], 'close'] <= df.loc[df.index[i], 'final_ub']
                else df.loc[df.index[i], 'final_lb']
                if df.loc[df.index[i - 1], 'supertrend'] == df.loc[df.index[i - 1], 'final_ub']
                and df.loc[df.index[i], 'close'] > df.loc[df.index[i], 'final_ub']
                else df.loc[df.index[i], 'final_lb']
                if df.loc[df.index[i - 1], 'supertrend'] == df.loc[df.index[i - 1], 'final_lb']
                and df.loc[df.index[i], 'close'] >= df.loc[df.index[i], 'final_lb']
                else df.loc[df.index[i], 'final_ub']
                if df.loc[df.index[i - 1], 'supertrend'] == df.loc[df.index[i - 1], 'final_lb']
                and df.loc[df.index[i], 'close'] < df.loc[df.index[i], 'final_lb']
                else 0.0
            )

        df['supertrend_signal'] = np.where(df['close'] > df['supertrend'], 'up', 'down')
        return df

    def analyze_and_trade(self, current_data: pd.DataFrame):
        """
        Analyzes the current market data using SuperTrend and returns a trade decision.
        """

        # Calculate SuperTrend for historical and current data
        current_data = self.calculate_supertrend(current_data)

        # Get the latest SuperTrend signals
        super_trend = current_data['supertrend_signal'].values

        # Check for buy signal
        if (super_trend[-1] == 'up' and super_trend[-3] == 'down' and
            super_trend[-4] == 'down' and super_trend[-5] == 'down' and super_trend[-6] == 'down'):
            current_trade = {
                'cp': current_data['close'].iloc[-1],  # Entry price
                'sl': current_data['low'].iloc[-3],  # Stop loss (third last candle's low)
                'pl': current_data['close'].iloc[-1] * 1.01,  # Take profit (1% above entry)
                'trade_type': 'buy',
                'timestamp': current_data['timestamp'].iloc[-1],
                'probability': 1.0,  # SuperTrend signals are deterministic
            }
            return current_trade

        # Check for sell signal
        elif (super_trend[-1] == 'down' and super_trend[-3] == 'up' and
            super_trend[-4] == 'up' and super_trend[-5] == 'up' and super_trend[-6] == 'up'):
            current_trade = {
                'cp': current_data['close'].iloc[-1],  # Entry price
                'sl': current_data['high'].iloc[-3],  # Stop loss (third last candle's high)
                'pl': current_data['close'].iloc[-1] * 0.99,  # Take profit (1% below entry)
                'trade_type': 'sell',
                'timestamp': current_data['timestamp'].iloc[-1],
                'probability': 1.0,  # SuperTrend signals are deterministic
            }
            return current_trade

        else:
            print("No trade taken. SuperTrend signal did not meet the confirmation criteria.")
            return None

    # def analyze_and_trade(self, current_data: pd.DataFrame):
    #     """
    #     Analyzes the current market data using SuperTrend and returns a trade decision.

    #     Args:
    #         current_data: Current market data as a pandas DataFrame.

    #     Returns:
    #         current_trade: A dictionary containing trade details if a trade is taken, otherwise None.
    #     """
    #     if self.historical_data is None:
    #         raise ValueError("Historical data not provided. Please provide historical_data during initialization.")

    #     # Calculate SuperTrend for historical and current data
    #     self.historical_data = self.calculate_supertrend(self.historical_data)
    #     current_data = self.calculate_supertrend(current_data)

    #     # Append current data to historical data
    #     self.historical_data = pd.concat([self.historical_data, current_data], ignore_index=True)

    #     # Get the latest SuperTrend signal
    #     latest_signal = current_data['supertrend_signal'].iloc[-1]
    #     previous_signal = current_data['supertrend_signal'].iloc[-2]

    #     # Determine trade action based on SuperTrend signal
    #     if latest_signal == 'up' and previous_signal == 'down':  # Buy signal
    #         current_trade = {
    #             'cp': current_data['close'].iloc[-1],  # Entry price
    #             'sl': current_data['low'].iloc[-1],  # Stop loss (low of the current candle)
    #             'pl': current_data['close'].iloc[-1] * 1.01,  # Take profit (1% above entry)
    #             'trade_type': 'buy',
    #             'timestamp': current_data['timestamp'].iloc[-1],
    #             'probability': 1.0,  # SuperTrend signals are deterministic
    #         }
    #         return current_trade
    #     elif latest_signal == 'down' and previous_signal == 'up':  # Sell signal
    #         current_trade = {
    #             'cp': current_data['close'].iloc[-1],  # Entry price
    #             'sl': current_data['high'].iloc[-1],  # Stop loss (high of the current candle)
    #             'pl': current_data['close'].iloc[-1] * 0.99,  # Take profit (1% below entry)
    #             'trade_type': 'sell',
    #             'timestamp': current_data['timestamp'].iloc[-1],
    #             'probability': 1.0,  # SuperTrend signals are deterministic
    #         }
    #         return current_trade
    #     else:
    #         print("No trade taken. SuperTrend signal did not change.")
    #         return None

