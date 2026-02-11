"""
Test script to demonstrate risk-based position sizing.
Shows how position size is calculated to maintain consistent dollar risk.
"""
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock environment before importing
os.environ['RISK_PER_TRADE_DOLLARS'] = '50.0'
os.environ['DATABASE_URL'] = 'mock'  # Prevent DB connection

from app.telegram.signal_parser import TradingSignal
from datetime import datetime, timedelta


def demonstrate_position_sizing():
    """Demonstrate position sizing calculations."""
    
    print("=" * 80)
    print("RISK-BASED POSITION SIZING DEMONSTRATION")
    print("=" * 80)
    print(f"\nFixed Risk Per Trade: $50.00")
    print(f"Formula: Position Size = Risk / |Entry - Stop|\n")
    
    # Example signals with different stop distances
    test_signals = [
        {
            "name": "CHINA50 - Wide Stop",
            "instrument": "CHINA50",
            "direction": "BUY",
            "entry_price": 14851.0,
            "stop_loss": 14759.0,
            "target_1": 15125.0,
            "target_2": 15195.0,
            "confidence": 3,
        },
        {
            "name": "US100 - Tight Stop",
            "instrument": "US100",
            "direction": "SELL",
            "entry_price": 25346.0,
            "stop_loss": 25646.0,
            "target_1": 24338.0,
            "target_2": 24205.0,
            "confidence": 3,
        },
        {
            "name": "GOLD - Medium Stop",
            "instrument": "GOLD",
            "direction": "BUY",
            "entry_price": 2050.0,
            "stop_loss": 2030.0,
            "target_1": 2080.0,
            "target_2": 2100.0,
            "confidence": 4,
        },
        {
            "name": "EURUSD - Very Tight Stop",
            "instrument": "EURUSD",
            "direction": "BUY",
            "entry_price": 1.0850,
            "stop_loss": 1.0820,
            "target_1": 1.0920,
            "target_2": 1.0970,
            "confidence": 5,
        }
    ]
    
    print("-" * 80)
    
    risk_amount = 50.0
    
    for test in test_signals:
        signal = TradingSignal(
            instrument=test["instrument"],
            direction=test["direction"],
            entry_price=test["entry_price"],
            stop_loss=test["stop_loss"],
            target_1=test["target_1"],
            target_2=test["target_2"],
            confidence=test["confidence"],
            expires_at=datetime.now() + timedelta(days=1),
            raw_message="",
            timestamp=datetime.now()
        )
        
        # Calculate stop distance
        stop_distance = abs(signal.entry_price - signal.stop_loss)
        
        # Calculate position size
        position_size = risk_amount / stop_distance
        
        # Calculate actual risk
        actual_risk = position_size * stop_distance
        
        # Calculate potential reward (to Target 1)
        if signal.direction == "BUY":
            reward_distance = signal.target_1 - signal.entry_price
        else:
            reward_distance = signal.entry_price - signal.target_1
        
        potential_reward = position_size * reward_distance
        
        print(f"\n{test['name']}")
        print(f"  Entry: {signal.entry_price:,.2f}")
        print(f"  Stop:  {signal.stop_loss:,.2f}")
        print(f"  Stop Distance: {stop_distance:.2f} points")
        print(f"  ")
        print(f"  → Position Size: {position_size:.2f} lots/contracts")
        print(f"  → Actual Risk: ${actual_risk:.2f}")
        print(f"  → Potential Reward (to T1): ${potential_reward:.2f}")
        print(f"  → Risk/Reward Ratio: {signal.risk_reward_ratio():.2f}")
        print(f"  → Confidence: {'⭐' * signal.confidence}")
        print("-" * 80)
    
    print("\n✅ Key Benefit: Every trade risks the same $50, regardless of:")
    print("   - Instrument (stocks, forex, commodities)")
    print("   - Stop loss distance (tight or wide)")
    print("   - Entry price level")
    print("\n" + "=" * 80)


if __name__ == "__main__":
    demonstrate_position_sizing()
