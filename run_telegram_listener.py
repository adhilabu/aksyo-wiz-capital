#!/usr/bin/env python3
"""
Telegram Trading Signal Listener

Monitors Telegram channel for trading signals and executes trades via Capital.com API.

Usage:
    python run_telegram_listener.py
"""
import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.telegram.telegram_listener import main

if __name__ == "__main__":
    asyncio.run(main())
