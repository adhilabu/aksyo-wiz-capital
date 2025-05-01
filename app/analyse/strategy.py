from typing import Optional
import pandas as pd
import numpy as np

class Strategy:
    def __init__(
        self, 
        profit_percent: float = 1.5, 
        loss_percent: float = 1.0,
        rsi_oversold: int = 30,
        rsi_overbought: int = 70,
        displacement_factor: float = 1.5,
        eq_tolerance: float = 0.01
    ):
        """
        Enhanced stock trading strategy with multiple technical indicators.
        
        Args:
            profit_percent: Target profit percentage (1.5%)
            loss_percent: Stop-loss percentage (1.0%)
            rsi_oversold: RSI threshold for oversold conditions (30)
            rsi_overbought: RSI threshold for overbought conditions (70)
            displacement_factor: Multiplier for volatility to detect displacement
            eq_tolerance: Tolerance for equal highs/lows detection
        """
        self.profit_percent = profit_percent
        self.loss_percent = loss_percent
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought
        self.displacement_factor = displacement_factor
        self.eq_tolerance = eq_tolerance
        self.current_trade = None
        self.market_structure = {'STH': [], 'STL': []}  # Initialize market structure

    def calculate_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate multiple technical indicators:
        - EMA (20, 50)
        - RSI (14)
        - MACD (12, 26, 9)
        - Bollinger Bands (20, 2)
        - Stochastic Oscillator (14, 3)
        """
        # Moving Averages
        data['EMA20'] = data['close'].ewm(span=20, adjust=False).mean()
        data['EMA50'] = data['close'].ewm(span=50, adjust=False).mean()

        # RSI
        delta = data['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        data['RSI'] = 100 - (100 / (1 + rs))

        # MACD
        ema12 = data['close'].ewm(span=12, adjust=False).mean()
        ema26 = data['close'].ewm(span=26, adjust=False).mean()
        data['MACD'] = ema12 - ema26
        data['Signal'] = data['MACD'].ewm(span=9, adjust=False).mean()
        data['Histogram'] = data['MACD'] - data['Signal']

        # Bollinger Bands
        data['SMA20'] = data['close'].rolling(window=20).mean()
        data['STD20'] = data['close'].rolling(window=20).std()
        data['UpperBand'] = data['SMA20'] + (data['STD20'] * 2)
        data['LowerBand'] = data['SMA20'] - (data['STD20'] * 2)

        # Stochastic Oscillator
        low14 = data['low'].rolling(window=14).min()
        high14 = data['high'].rolling(window=14).max()
        data['%K'] = 100 * ((data['close'] - low14) / (high14 - low14))
        data['%D'] = data['%K'].rolling(window=3).mean()

        return data.dropna()

    def update_market_structure(self, data: pd.DataFrame):
        """
        Update market structure (STH and STL) based on recent price action.
        """
        if len(data) < 3:
            return
        
        current = data.iloc[-1]
        prev1 = data.iloc[-2]
        prev2 = data.iloc[-3]
        
        # Update STH (Swing High)
        if current['high'] < prev1['high'] and prev1['high'] > prev2['high']:
            self.market_structure['STH'].append({'price': prev1['high'], 'timestamp': prev1['timestamp']})
        
        # Update STL (Swing Low)
        if current['low'] > prev1['low'] and prev1['low'] < prev2['low']:
            self.market_structure['STL'].append({'price': prev1['low'], 'timestamp': prev1['timestamp']})

    def generate_signals(self, data: pd.DataFrame) -> list:
        """Generate trading signals using ICT concepts with optimized iteration"""
        signals = []
        
        # Update market structure
        self.update_market_structure(data)
        
        # Only analyze last 3 candles for pattern detection
        start_idx = max(2, len(data)-3)  # Ensure minimum 3 bars for pattern detection
        end_idx = len(data)
        
        for i in range(start_idx, end_idx):
            current = data.iloc[i]
            prev1 = data.iloc[i-1]
            prev2 = data.iloc[i-2]
            
            # Market Structure Analysis
            sth = self.market_structure['STH'][-1]['price'] if self.market_structure['STH'] else None
            stl = self.market_structure['STL'][-1]['price'] if self.market_structure['STL'] else None
            
            # Displacement Check (Using rolling volatility)
            body_size = abs(prev1['open'] - prev1['close'])
            displacement = body_size > data['STD20'].iloc[i] * self.displacement_factor
            
            # FVG Detection
            bull_fvg = current['low'] > prev2['high'] and displacement
            bear_fvg = current['high'] < prev2['low'] and displacement
            
            # Liquidity Check
            liquidity_break = (
                (current['high'] > sth if sth else False) or 
                (current['low'] < stl if stl else False)
            )
            
            # Equal Highs/Lows Check
            eq_high = abs(current['high'] - prev1['high']) <= self.eq_tolerance
            eq_low = abs(current['low'] - prev1['low']) <= self.eq_tolerance
            
            # Signal Generation Logic
            if bull_fvg and eq_low and liquidity_break:
                signals.append({
                    'timestamp': current['timestamp'],
                    'signal': 'BUY',
                    'price': current['close'],
                    'sl': min(prev2['low'], prev1['low']),
                    'tp': current['close'] + (current['close'] - prev2['low'])
                })
                
            if bear_fvg and eq_high and liquidity_break:
                signals.append({
                    'timestamp': current['timestamp'],
                    'signal': 'SELL',
                    'price': current['close'],
                    'sl': max(prev2['high'], prev1['high']),
                    'tp': current['close'] - (prev2['high'] - current['close'])
                })
        
        # Only return signals from the latest 2 candles
        return signals[-2:] if len(signals) > 0 else []  

    def analyze_and_trade(self, real_time_data: pd.DataFrame) -> dict:
        """
        Execute trades based on real-time data analysis.
        Implements dynamic stop-loss and profit targets.
        """
        if len(real_time_data) < 50:
            return {}  # Insufficient data for reliable signals

        data = self.calculate_indicators(real_time_data)
        signals = self.generate_signals(data)

        if signals:
            last_signal = signals[-1]
            action, timestamp, price = last_signal['signal'], last_signal['timestamp'], last_signal['price']
            
            # Dynamic position sizing based on volatility
            atr = data['STD20'].iloc[-1] * 2
            position_size = min(0.1, 1000 / (price * atr))  # Risk 1% of capital

            trade_details = {
                'action': action,
                'price': price,
                'timestamp': timestamp,
                'sl': price * (1 - self.loss_percent/100),
                'tp': price * (1 + self.profit_percent/100),
                'size': position_size,
                'indicators': {
                    'rsi': data['RSI'].iloc[-1],
                    'macd': data['MACD'].iloc[-1],
                    'stoch': data['%K'].iloc[-1]
                }
            }

            # Update current trade state
            if 'exit' in action:
                self.current_trade = None
            else:
                self.current_trade = trade_details

            return trade_details
        
        return {}