"""
Test script for signal parser using sample messages.
"""
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.telegram.signal_parser import SignalParser


def test_sample_messages():
    """Test parsing of sample messages from sample_msg.txt"""
    
    # Read sample messages
    with open('sample_msg.txt', 'r') as f:
        content = f.read()
    
    # Split into individual messages (separated by double newlines and "Market Chart")
    messages = content.split('Market Chart')
    
    print("=" * 80)
    print("TESTING SIGNAL PARSER")
    print("=" * 80)
    print()
    
    for i, message in enumerate(messages, 1):
        if not message.strip():
            continue
        
        print(f"\n--- Message {i} ---")
        print(message.strip()[:200] + "..." if len(message.strip()) > 200 else message.strip())
        print()
        
        # Parse the message
        signal = SignalParser.parse(message, datetime.now())
        
        if signal:
            print("✅ PARSED SUCCESSFULLY")
            print(f"   Instrument: {signal.instrument}")
            print(f"   Direction: {signal.direction}")
            print(f"   Entry: {signal.entry_price}")
            print(f"   Stop: {signal.stop_loss}")
            print(f"   Target 1: {signal.target_1}")
            print(f"   Target 2: {signal.target_2}")
            print(f"   Confidence: {'⭐' * signal.confidence} ({signal.confidence})")
            print(f"   R/R Ratio: {signal.risk_reward_ratio():.2f}")
            print(f"   Valid: {signal.is_valid()}")
        else:
            print("❌ FAILED TO PARSE (or confidence < 3)")
        
        print("-" * 80)
    
    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    test_sample_messages()
