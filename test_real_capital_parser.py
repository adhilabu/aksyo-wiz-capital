"""
Test the updated signal parser with real Capital.com messages.
"""
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.telegram.signal_parser import SignalParser

# Real messages from Capital.com Telegram channel
messages = [
    """📈 [Buy Limit] XAU/USD - Bullish - We look to Buy at 4946 

▪ Posted mild net daily losses but all trading confined to the previous days range, an indecisive Inside Day.
▪ Trading within a Corrective Channel formation.
▪ The AB=CD formation target is located at 5376.
▪ We have a 78.6% Fibonacci pullback level of 5348 from  5602 to 4403.
▪ There is scope for mild selling at the open but losses should be limited.
▪ Bespoke support is located at 4946.

Confidence: ⭐⭐⭐⭐
⛔ Stop: 4796
🎯 Target 1: 5348
🎯 Target 2: 5376
⌛ Expires: 2026-02-12 06:00 (GMT+00:00)""",
    
    """📈 [Buy Limit] EUR/USD - Bullish - We look to Buy at 1.1870 

▪ The primary trend remains bullish.
▪ Price action looks to be forming a bottom.
▪ Preferred trade is to buy on dips.
▪ Bespoke support is located at 1.1870.
▪ Risk/Reward would be poor to call a buy from current levels.

Confidence: ⭐⭐⭐
⛔ Stop: 1.184
🎯 Target 1: 1.1975
🎯 Target 2: 1.2
⌛ Expires: 2026-02-12 06:00 (GMT+00:00)"""
]

def test_parser():
    print("=" * 80)
    print("TESTING UPDATED SIGNAL PARSER WITH REAL CAPITAL.COM MESSAGES")
    print("=" * 80)
    print()
    
    for i, message in enumerate(messages, 1):
        print(f"Test {i}:")
        print("-" * 80)
        
        # Show first 100 chars of message
        preview = message[:100].replace('\n', ' ')
        print(f"Message preview: {preview}...")
        print()
        
        # Parse the message
        signal = SignalParser.parse(message, datetime.now())
        
        if signal:
            print("✅ PARSING SUCCESSFUL!")
            print(f"   Instrument: {signal.instrument}")
            print(f"   Direction: {signal.direction}")
            print(f"   Entry Price: {signal.entry_price:,.2f}")
            print(f"   Stop Loss: {signal.stop_loss:,.2f}")
            print(f"   Target 1: {signal.target_1:,.2f}")
            print(f"   Target 2: {signal.target_2:,.2f}")
            print(f"   Confidence: {'⭐' * signal.confidence} ({signal.confidence} stars)")
            print(f"   Expires: {signal.expires_at}")
            print(f"   R/R Ratio: {signal.risk_reward_ratio():.2f}")
            print(f"   Valid: {signal.is_valid()}")
        else:
            print("❌ PARSING FAILED - Signal returned None")
        
        print()
    
    print("=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    test_parser()
