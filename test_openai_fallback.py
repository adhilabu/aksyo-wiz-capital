#!/usr/bin/env python3
"""Test OpenAI fallback parsing"""
import sys
sys.path.insert(0, '/home/pretradify/aksyo-wiz-capital')

from app.telegram.signal_parser import SignalParser
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

# Test 1: Standard format (should use regex)
print("=" * 60)
print("TEST 1: Standard format (regex should work)")
print("=" * 60)
message1 = '''📈 [Live Trade] US100 - Bullish - We look to Buy at 25160 

Confidence: ⭐⭐⭐⭐
⛔ Stop: 24860
🎯 Target 1: 25790
🎯 Target 2: 26133
⌛ Expires: 2026-02-12 12:00 (GMT+00:00)'''

signal1 = SignalParser.parse(message1, datetime.now())
if signal1:
    print(f'✅ SUCCESS: {signal1.instrument} {signal1.direction} @ {signal1.entry_price}')
else:
    print('❌ FAILED')

# Test 2: Non-standard format (should trigger OpenAI fallback)
print("\n" + "=" * 60)
print("TEST 2: Non-standard format (OpenAI fallback)")
print("=" * 60)
message2 = '''GOLD Trading Signal

Direction: Long (Buy)
Entry around: 2650
Stop Loss: 2634
Take Profit 1: 2669
Take Profit 2: 2680
Confidence: 4 stars ⭐⭐⭐⭐
Valid until: 2026-02-12 18:00'''

signal2 = SignalParser.parse(message2, datetime.now())
if signal2:
    print(f'✅ SUCCESS: {signal2.instrument} {signal2.direction} @ {signal2.entry_price}')
else:
    print('❌ FAILED')

print("\n" + "=" * 60)
