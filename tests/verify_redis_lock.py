import asyncio
import logging
from unittest.mock import MagicMock, AsyncMock, patch
import os
import sys

# Add project root to path
sys.path.append(os.getcwd())

from app.telegram.signal_processor import SignalProcessor
from app.telegram.signal_parser import TradingSignal
from app.database.db import DBConnection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_redis_lock():
    print("\n--- Testing Redis Lock Logic ---")
    
    # Mock dependencies
    mock_db = MagicMock(spec=DBConnection)
    mock_db.execute = AsyncMock()
    
    # Patch RedisCache to use a simple dict instead of real Redis
    with patch('app.telegram.signal_processor.RedisCache') as MockRedisCache, \
         patch('app.telegram.signal_processor.CapitalAPI') as MockCapitalAPI, \
         patch('app.telegram.signal_processor.OpenAI') as MockOpenAI, \
         patch.dict(os.environ, {
             "OPENAI_API_KEY": "fake_key",
             "ENABLE_AUTO_TRADING": "true",
             "TELEGRAM_TOKEN": "fake_token",
             "TELEGRAM_CHAT_ID": "fake_chat_id"
         }):
        
        # Setup mock Redis behavior
        mock_redis_instance = MockRedisCache.return_value
        redis_store = {}
        
        def set_key(key, value, ttl=None):
            print(f"Redis SET: {key} = {value} (ttl={ttl})")
            redis_store[key] = value
            
        def key_exists(key):
            exists = key in redis_store
            print(f"Redis EXISTS: {key} -> {exists}")
            return exists
            
        mock_redis_instance.set_key.side_effect = set_key
        mock_redis_instance.key_exists.side_effect = key_exists
        
        # Setup SignalProcessor
        processor = SignalProcessor(mock_db)
        # Manually set market details to avoid file load issues and ensure instrument exists
        processor.market_details = {"GOLD": {"qty": 1, "min_deal_size": 0.1, "max_deal_size": 100, "min_size_increment": 0.1}}
        
        # Setup basic signal
        signal = TradingSignal(
            raw_message="BUY GOLD",
            instrument="GOLD",
            direction="BUY",
            entry_price=2000.0,
            stop_loss=1990.0,
            target_1=2020.0,
            target_2=2040.0,
            confidence=5,
            timestamp=None,
            order_type="MARKET",
            expires_at=None
        )
        
        # Mock internal methods to isolate redis logic
        processor._analyze_with_openai = AsyncMock(return_value={
            "should_trade": True,
            "suggested_size": 1.0,
            "reasoning": "Test"
        })
        processor._create_order = AsyncMock(return_value={})
        processor.capital_api.place_order = AsyncMock(return_value="DEAL_123")
        processor._log_signal = AsyncMock()
        processor._send_notification = AsyncMock()
        
        # 1. First run - should execute
        print("\n1. Processing first signal (should execute)...")
        result1 = await processor.process_signal(signal)
        print(f"Result 1 Status: {result1['status']}")
        
        assert result1['status'] == 'executed', f"Expected 'executed', got {result1['status']}"
        assert 'trade_lock:GOLD:BUY' in redis_store, "Redis key should be set"
        
        # 2. Second run - should be skipped
        print("\n2. Processing duplicate signal (should be skipped)...")
        result2 = await processor.process_signal(signal)
        print(f"Result 2 Status: {result2['status']}")
        
        assert result2['status'] == 'skipped_duplicate', f"Expected 'skipped_duplicate', got {result2['status']}"
        
        # 3. Different signal (SELL) - should execute
        print("\n3. Processing SELL signal (should execute)...")
        signal_sell = TradingSignal(
            raw_message="SELL GOLD",
            instrument="GOLD",
            direction="SELL",
            entry_price=2000.0,
            stop_loss=2010.0,
            target_1=1980.0,
            target_2=1960.0,
            confidence=5,
            timestamp=None,
            order_type="MARKET",
            expires_at=None
        )
        result3 = await processor.process_signal(signal_sell)
        print(f"Result 3 Status: {result3['status']}")
        
        assert result3['status'] == 'executed', f"Expected 'executed', got {result3['status']}"
        assert 'trade_lock:GOLD:SELL' in redis_store, "Redis key for SELL should be set"
        
        print("\n--- Test Passed Successfully ---")

if __name__ == "__main__":
    asyncio.run(test_redis_lock())
