#!/usr/bin/env python3
"""
Test script for the new LLM-based sentiment analysis
"""
import os
import sys
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.analyse.sentiment_trader import SentimentTrader

def main():
    load_dotenv()
    
    print("="*80)
    print("Testing LLM-based Sentiment Analysis")
    print("="*80)
    
    # Initialize the sentiment trader
    trader = SentimentTrader()
    
    # Check which APIs are available
    print(f"\nAPI Availability:")
    print(f"  OpenAI: {'✓' if trader.openai_available else '✗'}")
    print(f"  Gemini: {'✓' if trader.gemini_available else '✗'}")
    print(f"  Redis Cache: {'✓' if trader.redis_client else '✗'}")
    
    if not trader.openai_available and not trader.gemini_available:
        print("\n⚠️  WARNING: Neither OpenAI nor Gemini APIs are available!")
        print("   The system will fall back to keyword-based analysis.")
        print("\n   To enable LLM analysis, add to your .env file:")
        print("   OPENAI_API_KEY=sk-...")
        print("   GEMINI_API_KEY=...")
    
    # Test with a simple symbol
    print(f"\n{'='*80}")
    print("Testing Sentiment Analysis for BTCUSD")
    print(f"{'='*80}\n")
    
    try:
        # Use cache to avoid API calls if already cached
        result = trader.get_sentiment_signal('BTCUSD', days_back=1, max_queries=1, use_cache=True)
        
        print(f"Signal: {result['signal']}")
        print(f"Polarity: {result['weighted_polarity']:.3f}")
        print(f"Total Articles: {result['total_articles']}")
        print(f"Reasoning: {result['reasoning']}")
        print(f"Instrument Type: {result['instrument_type']}")
        print(f"Cache Hit: {result.get('cache_hit', False)}")
        
        if result['sentiment_df']:
            print(f"\nFirst article analysis:")
            first = result['sentiment_df'][0]
            print(f"  Title: {first.get('title', 'N/A')}")
            print(f"  Sentiment: {first.get('sentiment', 'N/A')}")
            print(f"  Polarity: {first.get('polarity', 0):.3f}")
            print(f"  Confidence: {first.get('confidence', 0):.3f}")
            print(f"  Analyzer Used: {first.get('analyzer_used', 'N/A')}")
            print(f"  Reasoning: {first.get('reasoning', 'N/A')[:80]}...")
        
        print(f"\n✅ Test completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
