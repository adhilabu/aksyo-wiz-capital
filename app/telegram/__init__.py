"""
Telegram signal reader module for automated trading.

This module monitors Telegram channels for trading signals,
parses them, analyzes with OpenAI, and executes trades via Capital.com API.
"""

__all__ = [
    'TelegramSignalListener',
    'TradingSignal',
    'SignalParser',
    'SignalProcessor',
    'InstrumentMapper',
]
