import datetime
import pandas as pd
import numpy as np
from math import floor

class CandlestickBreakoutStrategy:
    def __init__(self, risk_per_trade=10000):
        """
        Initialize the CandlestickBreakoutStrategy.

        Args:
            risk_per_trade: Risk per trade in INR (default: 10000).
        """
        self.risk_per_trade = risk_per_trade
        self.orderslist = []
        self.pattern_data = {}  # Store pattern information for each stock
        
    def is_red_candle(self, row):
        """Check if the candle is red (close < open)."""
        return row['close'] < row['open']
    
    def is_green_candle(self, row):
        """Check if the candle is green (close > open)."""
        return row['close'] > row['open']
    
    def detect_color_change(self, current_data):
        """
        Detect if there's a color change in adjacent candles before 10 AM.
        
        Args:
            current_data: DataFrame with OHLC data
            
        Returns:
            bool: True if color change detected, False otherwise
        """
        # Filter data before 10 AM
        morning_data = current_data[current_data['date'].dt.time <= datetime.time(10, 0)]
        
        if len(morning_data) < 2:
            return False
            
        # Get last two candles
        last_candle = morning_data.iloc[-1]
        prev_candle = morning_data.iloc[-2]
        
        # Check for color change
        last_is_red = self.is_red_candle(last_candle)
        prev_is_red = self.is_red_candle(prev_candle)
        
        return last_is_red != prev_is_red
    
    def update_pattern_data(self, stock_symbol, current_data):
        """
        Update pattern data for a stock including highs and lows.
        
        Args:
            stock_symbol: Symbol of the stock
            current_data: Current market data DataFrame
        """
        if stock_symbol not in self.pattern_data:
            self.pattern_data[stock_symbol] = {
                'active': False,
                'pattern_high': float('-inf'),
                'pattern_low': float('inf'),
                'entry_time': None
            }
            
        pattern = self.pattern_data[stock_symbol]
        
        # If pattern not active and color change detected, start tracking
        if not pattern['active'] and self.detect_color_change(current_data):
            pattern['active'] = True
            pattern['pattern_high'] = current_data['high'].max()
            pattern['pattern_low'] = current_data['low'].min()
            pattern['entry_time'] = current_data['date'].iloc[-1]
        
        # If pattern is active, update highs and lows
        elif pattern['active']:
            pattern['pattern_high'] = max(pattern['pattern_high'], current_data['high'].max())
            pattern['pattern_low'] = min(pattern['pattern_low'], current_data['low'].min())
    
    def analyze_and_trade(self, stock_symbol: str, current_data: pd.DataFrame):
        """
        Analyzes the current market data and returns a trade decision based on breakout.

        Args:
            stock_symbol: Symbol of the stock
            current_data: Current market data as a pandas DataFrame.

        Returns:
            current_trade: A dictionary containing trade details if a trade is taken, otherwise None.
        """
        if current_data is None or len(current_data) < 2:
            raise ValueError("Insufficient data for analysis.")
            
        # Update pattern data
        self.update_pattern_data(stock_symbol, current_data)
        
        if stock_symbol not in self.pattern_data or not self.pattern_data[stock_symbol]['active']:
            return None
            
        pattern = self.pattern_data[stock_symbol]
        lastclose = current_data['close'].iloc[-1]
        
        # Calculate breakout levels
        pattern_high = pattern['pattern_high']
        pattern_low = pattern['pattern_low']
        
        # Buy signal: Break above pattern high
        if lastclose > pattern_high:
            stop_loss = lastclose * 0.99  # 1% stop loss
            target = lastclose * 1.01     # 1% target
            quantity = floor(max(1, (self.risk_per_trade / (lastclose - stop_loss))))
            
            current_trade = {
                'symbol': stock_symbol,
                'cp': lastclose,           # Entry price
                'sl': stop_loss,           # Stop loss
                'pl': target,              # Take profit
                'trade_type': 'buy',
                'timestamp': current_data['date'].iloc[-1],
                'quantity': quantity,
                'pattern_start': pattern['entry_time']
            }
            
            # Reset pattern data after trade
            pattern['active'] = False
            return current_trade
            
        # Sell signal: Break below pattern low
        elif lastclose < pattern_low:
            stop_loss = lastclose * 1.01  # 1% stop loss
            target = lastclose * 0.99     # 1% target
            quantity = floor(max(1, (self.risk_per_trade / (stop_loss - lastclose))))
            
            current_trade = {
                'symbol': stock_symbol,
                'cp': lastclose,           # Entry price
                'sl': stop_loss,           # Stop loss
                'pl': target,              # Take profit
                'trade_type': 'sell',
                'timestamp': current_data['date'].iloc[-1],
                'quantity': quantity,
                'pattern_start': pattern['entry_time']
            }
            
            # Reset pattern data after trade
            pattern['active'] = False
            return current_trade
            
        return None