import requests
from textblob import TextBlob
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from typing import Dict, List, Optional, Tuple
import logging
import redis
import json
import hashlib
import time
load_dotenv(dotenv_path=".env")

try:
    from bs4 import BeautifulSoup
    BEAUTIFUL_SOUP_AVAILABLE = True
except ImportError:
    BEAUTIFUL_SOUP_AVAILABLE = False
    logging.warning("BeautifulSoup not available. HTML extraction will be limited.")
    
class SentimentTrader:
    """
    Enhanced Sentiment analysis class for multiple trading instruments with Redis caching
    """
    
    def __init__(self):
        self.api_key = os.getenv("THE_NEWS_API_KEY")
        self.logger = logging.getLogger(__name__)
        
        # Initialize Redis client
        self.redis_client = self._init_redis()
        
        # Enhanced instrument search queries with better targeting
        self.instrument_search_queries = {
            # Cryptocurrencies
            "BTCUSD": [
                "Bitcoin BTC price analysis", "BTC cryptocurrency market", "Bitcoin trading",
                "BTC technical analysis", "Bitcoin investment news", "BTC market sentiment",
                "Bitcoin ETF news", "crypto market Bitcoin", "BTC price prediction"
            ],
            # Indices
            "US100": [
                "NASDAQ 100 analysis", "tech stocks market", "NASDAQ composite",
                "US100 technical analysis", "technology stocks news", "growth stocks market",
                "NASDAQ futures trading", "big tech companies", "tech index performance"
            ],
            "US30": [
                "Dow Jones Industrial Average", "Dow 30 stocks", "blue chip stocks market",
                "US30 technical analysis", "industrial stocks news", "DOW Jones trading",
                "blue chip companies", "industrial average performance"
            ],
            "J225": [
                "Nikkei 225 index", "Japan stock market news", "Tokyo stock exchange",
                "J225 technical analysis", "Japanese equities market", "Nikkei performance",
                "Japan economy stocks", "Japanese exports market"
            ],
            # Commodities
            "OIL_CRUDE": [
                "crude oil prices", "oil market news", "WTI crude oil",
                "oil supply demand", "OPEC news today", "oil inventory report",
                "energy market trading", "crude oil futures", "oil price market"
            ],
            "USCOCOA": [
                "cocoa prices market", "cocoa futures trading", "chocolate market news",
                "cocoa supply demand", "cocoa trading news", "soft commodities market",
                "cocoa production news", "cocoa weather market"
            ],
            # Forex
            "USDJPY": [
                "USD JPY currency", "dollar yen exchange rate", "USDJPY forex trading",
                "Bank of Japan policy", "Federal Reserve dollar", "yen currency market",
                "USDJPY technical analysis", "dollar yen forecast"
            ],
            "EURJPY": [
                "EUR JPY currency", "euro yen exchange rate", "EURJPY forex trading",
                "European Central Bank euro", "Bank of Japan policy", "euro yen market",
                "EURJPY technical analysis", "euro yen forecast"
            ]
        }
        
        # Enhanced sentiment indicators by instrument type
        self.sentiment_indicators = {
            # General market indicators
            'general': {
                'bullish_terms': [
                    'bullish', 'rally', 'surge', 'soar', 'jump', 'climb', 'breakout',
                    'rebound', 'recovery', 'outperform', 'bull run', 'uptrend',
                    'buying pressure', 'accumulation', 'bull market', 'positive',
                    'strength', 'momentum', 'break higher', 'new highs', 'uptrend',
                    'optimistic', 'confidence', 'growth', 'expansion', 'profits',
                    'earnings beat', 'strong results', 'beats estimates'
                ],
                'bearish_terms': [
                    'bearish', 'crash', 'plunge', 'drop', 'fall', 'dump', 'breakdown',
                    'decline', 'correction', 'underperform', 'bear market', 'selloff',
                    'selling pressure', 'distribution', 'negative', 'warning',
                    'weakness', 'losses', 'break lower', 'new lows', 'downtrend',
                    'pessimistic', 'fear', 'contraction', 'recession', 'losses',
                    'earnings miss', 'weak results', 'misses estimates'
                ]
            },
            # Crypto-specific indicators
            'crypto': {
                'bullish_terms': [
                    'adoption', 'institutional', 'halving', 'burn', 'deflationary',
                    'staking rewards', 'yield', 'DeFi growth', 'NFT boom', 'web3',
                    'blockchain adoption', 'crypto ETF', 'regulation clarity',
                    'mainstream adoption', 'hash rate', 'mining difficulty'
                ],
                'bearish_terms': [
                    'regulation crackdown', 'ban', 'restriction', 'hack', 'exploit',
                    'rug pull', 'scam', 'fraud', 'wash trading', 'manipulation',
                    'energy consumption', 'environmental concern', 'FUD', 'FOMO',
                    'whale selling', 'supply dump', 'network congestion'
                ]
            },
            # Equity indices indicators
            'indices': {
                'bullish_terms': [
                    'record highs', 'all-time high', 'market rally', 'economic growth',
                    'low unemployment', 'strong GDP', 'consumer confidence', 'retail boom',
                    'manufacturing expansion', 'services growth', 'corporate profits',
                    'dividend growth', 'share buybacks', 'mergers acquisitions'
                ],
                'bearish_terms': [
                    'market correction', 'valuation concerns', 'overbought', 'bubble',
                    'economic slowdown', 'recession fears', 'inflation concerns',
                    'rate hikes', 'hawkish Fed', 'geopolitical risk', 'trade war',
                    'supply chain issues', 'labor shortage', 'cost pressures'
                ]
            },
            # Commodities indicators
            'commodities': {
                'bullish_terms': [
                    'supply disruption', 'inventory draw', 'production cut', 'OPEC+',
                    'geopolitical tension', 'export restrictions', 'strong demand',
                    'seasonal demand', 'weather impact', 'harvest issues', 'low stocks',
                    'backwardation', 'physical tightness', 'shipping delays'
                ],
                'bearish_terms': [
                    'oversupply', 'inventory build', 'production increase', 'weak demand',
                    'recession demand', 'alternative sources', 'technological substitution',
                    'high stocks', 'contango', 'storage full', 'export increase',
                    'favorable weather', 'bumper harvest'
                ]
            },
            # Forex indicators
            'forex': {
                'bullish_terms': [
                    'hawkish', 'rate hike', 'tightening', 'strong economy', 'growth outlook',
                    'inflation target', 'positive data', 'yield advantage', 'carry trade',
                    'safe haven', 'risk on', 'dollar strength', 'yen weakness'
                ],
                'bearish_terms': [
                    'dovish', 'rate cut', 'easing', 'weak economy', 'recession risk',
                    'inflation concern', 'negative data', 'yield disadvantage',
                    'risk off', 'safe haven flows', 'dollar weakness', 'yen strength'
                ]
            }
        }
        
        # Instrument type mapping
        self.instrument_types = {
            'BTCUSD': 'crypto',
            'US100': 'indices', 
            'US30': 'indices',
            'J225': 'indices',
            'OIL_CRUDE': 'commodities',
            'USCOCOA': 'commodities',
            'USDJPY': 'forex',
            'EURJPY': 'forex'
        }

    def _init_redis(self) -> redis.Redis:
        """Initialize Redis connection with error handling"""
        try:
            redis_host = os.getenv("REDIS_HOST", "localhost")
            redis_port = int(os.getenv("REDIS_PORT", 6379))
            redis_db = int(os.getenv("REDIS_DB", 0))
            redis_password = os.getenv("REDIS_PASSWORD")
            
            client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            # Test connection
            client.ping()
            self.logger.info("Redis connection established successfully")
            return client
            
        except Exception as e:
            self.logger.warning(f"Redis connection failed: {e}. Continuing without caching.")
            return None

    def _generate_cache_key(self, symbol: str, days_back: int, max_queries: int) -> str:
        """Generate a unique cache key for the query parameters"""
        key_string = f"sentiment:{symbol}:{days_back}:{max_queries}"
        return hashlib.md5(key_string.encode()).hexdigest()

    def _get_cached_result(self, cache_key: str) -> Optional[Dict]:
        """Retrieve result from Redis cache if available"""
        if not self.redis_client:
            return None
            
        try:
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                self.logger.info(f"Cache hit for key: {cache_key}")
                return json.loads(cached_data)
            else:
                self.logger.info(f"Cache miss for key: {cache_key}")
                return None
        except Exception as e:
            self.logger.warning(f"Error reading from cache: {e}")
            return None

    def _set_cached_result(self, cache_key: str, result: Dict, expiration_seconds: int = 3600) -> bool:
        """Store result in Redis cache with expiration"""
        if not self.redis_client:
            return False
            
        try:
            # Remove the pandas DataFrame from cache as it's not JSON serializable
            cacheable_result = result.copy()
            if 'sentiment_df' in cacheable_result:
                cacheable_result['sentiment_df'] = []
            
            self.redis_client.setex(
                cache_key,
                expiration_seconds,
                json.dumps(cacheable_result, default=str)
            )
            self.logger.info(f"Result cached with key: {cache_key} for {expiration_seconds} seconds")
            return True
        except Exception as e:
            self.logger.warning(f"Error writing to cache: {e}")
            return False

    def clear_cache(self, symbol: str = None) -> bool:
        """Clear cache for a specific symbol or all cache"""
        if not self.redis_client:
            return False
            
        try:
            if symbol:
                # Clear all cache entries for this symbol
                pattern = f"sentiment:{symbol}:*"
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
                    self.logger.info(f"Cleared cache for symbol: {symbol}")
            else:
                # Clear all sentiment cache
                pattern = "sentiment:*"
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
                    self.logger.info("Cleared all sentiment cache")
            return True
        except Exception as e:
            self.logger.error(f"Error clearing cache: {e}")
            return False

    def get_cache_info(self) -> Dict:
        """Get cache statistics and information"""
        if not self.redis_client:
            return {"status": "Redis not available"}
            
        try:
            pattern = "sentiment:*"
            keys = self.redis_client.keys(pattern)
            cache_info = {
                "status": "Connected",
                "total_cached_items": len(keys),
                "cached_symbols": {}
            }
            
            # Group by symbol
            for key in keys:
                parts = key.split(":")
                if len(parts) >= 2:
                    symbol = parts[1]
                    if symbol not in cache_info["cached_symbols"]:
                        cache_info["cached_symbols"][symbol] = 0
                    cache_info["cached_symbols"][symbol] += 1
            
            return cache_info
        except Exception as e:
            return {"status": f"Error: {str(e)}"}

    def get_sentiment_signal(self, symbol: str, days_back: int = 4, max_queries: int = 3, 
                           use_cache: bool = True) -> Dict[str, any]:
        """
        Main method to get sentiment signal for trading integration with Redis caching
        
        Args:
            symbol: Trading symbol (BTCUSD, US100, OIL_CRUDE, etc.)
            days_back: Number of days to look back for news
            max_queries: Maximum number of search queries to use (default: 3)
            use_cache: Whether to use Redis caching (default: True)
            
        Returns:
            Dictionary with sentiment signal and metadata
        """
        # Generate cache key
        cache_key = self._generate_cache_key(symbol, days_back, max_queries)
        
        # Try to get from cache first
        if use_cache:
            cached_result = self._get_cached_result(cache_key)
            if cached_result:
                # Add cache hit indicator
                cached_result['cache_hit'] = True
                cached_result['cache_key'] = cache_key
                return cached_result
        
        try:
            # Fetch market-relevant articles from multiple queries
            articles = self._fetch_news(symbol, days_back, max_queries)
            
            if not articles:
                self.logger.warning(f"No quality market analysis found for {symbol}")
                result = self._get_default_sentiment()
            else:
                # Analyze market sentiment
                sentiment_df = self._analyze_market_sentiment(articles, symbol)
                
                # Generate trading recommendation
                signal, weighted_polarity, total_articles, reasoning = self._generate_trading_recommendation(sentiment_df, symbol)
                
                # Convert to the expected format for trading integration
                result = {
                    'signal': signal,
                    'weighted_polarity': weighted_polarity,
                    'total_articles': total_articles,
                    'reasoning': reasoning,
                    'sentiment_df': sentiment_df.to_dict('records') if not sentiment_df.empty else [],
                    'timestamp': datetime.now().isoformat(),
                    'instrument_type': self.instrument_types.get(symbol, 'unknown')
                }
            
            # Add cache info
            result['cache_hit'] = False
            result['cache_key'] = cache_key
            
            # Store in cache (without the DataFrame for efficiency)
            if use_cache:
                self._set_cached_result(cache_key, result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in sentiment analysis for {symbol}: {e}")
            result = self._get_default_sentiment()
            result['cache_hit'] = False
            result['cache_key'] = cache_key
            return result

    def _get_default_sentiment(self) -> Dict[str, any]:
        """Return default sentiment when analysis fails"""
        return {
            'signal': 'NEUTRAL',
            'weighted_polarity': 0.0,
            'total_articles': 0,
            'reasoning': 'Sentiment analysis unavailable',
            'sentiment_df': [],
            'timestamp': datetime.now().isoformat(),
            'instrument_type': 'unknown'
        }

    def _fetch_news(self, symbol: str, days_back: int = 1, max_queries: int = 3) -> List[Dict]:
        """
        Fetches news for the given instrument with targeted search queries.
        Handles both JSON and HTML responses gracefully.
        """
        url = "https://api.thenewsapi.com/v1/news/all"
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Get specific queries for the instrument
        specific_queries = self.instrument_search_queries.get(symbol, [
            f"{symbol} market analysis",
            f"{symbol} price outlook",
            f"{symbol} trading forecast"
        ])
        
        # Use multiple queries (up to max_queries)
        queries_to_use = specific_queries[:max_queries]
        all_articles = []
        
        print(f"Using {len(queries_to_use)} search queries for {symbol}: {queries_to_use}\n")
        
        for i, query in enumerate(queries_to_use):
            #sleep for 1.5s
            time.sleep(2)
            print(f"Query {i+1}/{len(queries_to_use)} for {symbol}: {query}")
            
            params = {
                "api_token": self.api_key,
                "search": query,
                "language": "en",
                "limit": 10,
                "published_after": start_date.strftime("%Y-%m-%d"),
                "categories": "business,tech,politics,finance",
                "exclude_categories": "sports,entertainment,lifestyle"
            }
            
            try:
                self.logger.info(f"Fetching market analysis for {symbol} with query: {query}")
                
                # Add headers to avoid blocking
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'application/json, text/html, */*',
                    'Accept-Encoding': 'gzip, deflate, br'
                }
                
                response = requests.get(url, params=params, headers=headers, timeout=15)
                
                # Check if response is successful
                if response.status_code != 200:
                    self.logger.warning(f"API returned status code {response.status_code} for query '{query}'")
                    continue
                
                # Enhanced response handling for both JSON and HTML
                content_type = response.headers.get('content-type', '').lower()
                
                # Check if response is HTML (error page)
                if 'text/html' in content_type:
                    self.logger.warning(f"Received HTML response instead of JSON for query '{query}'. This may indicate API issues.")

                    # Check if response is readable (not binary garbage)
                    try:
                        # Try to detect if it's mostly garbage characters
                        readable_chars = sum(c.isprintable() or c.isspace() for c in html_text[:200])
                        if readable_chars < 100:  # Less than 50% readable
                            self.logger.error(f"Received corrupted/binary HTML response for query '{query}'")
                            continue
                    except Exception:
                        self.logger.error(f"Could not parse HTML response for query '{query}'")
                        continue
                    
                    # Check for common error patterns in HTML
                    html_articles = response.text.lower()
                    print('html_articles :', html_articles[:100])
                    if any(error_indicator in html_articles for error_indicator in [
                        'error', 'not found', 'unauthorized', 'rate limit', 'api limit',
                        'invalid api', 'access denied', 'blocked', 'captcha'
                    ]):
                        self.logger.error(f"API error detected in HTML response for query '{query}'")
                        
                        # Try to extract error information from HTML
                        error_info = self._extract_error_from_html(response.text)
                        if error_info:
                            self.logger.error(f"HTML error details: {error_info}")
                        
                        continue
                    else:
                        print('else')
                        # It might be a valid HTML page with content, try to extract articles
                        self.logger.info(f"Attempting to extract articles from HTML response for query '{query}'")
                        html_articles = self._extract_articles_from_html(response.text, query)
                        
                        # Filter for market-relevant content
                        market_articles = []
                        for article in html_articles:
                            if self._is_market_relevant_article(article, symbol):
                                article['search_query'] = query
                                article['source'] = 'html_extraction'
                                market_articles.append(article)
                        
                        # Remove duplicates by URL
                        new_articles = []
                        for article in market_articles:
                            url = article.get('url', '')
                            if not any(a.get('url') == url for a in all_articles):
                                new_articles.append(article)
                        
                        all_articles.extend(new_articles)
                        self.logger.info(f"HTML extraction returned {len(market_articles)} articles, {len(new_articles)} new unique")
                        continue
                
                # Handle JSON response
                elif 'application/json' in content_type or response.text.strip().startswith(('{', '[')):
                    try:
                        data = json.loads(response.text)
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"JSON decode error for query '{query}': {e}")
                        
                        # Save problematic response for debugging
                        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                        filename = f"error_response_{symbol}_{timestamp}.json"
                        try:
                            with open(filename, "w", encoding='utf-8') as f:
                                f.write(response.text[:5000])  # Limit size
                            self.logger.info(f"Saved problematic response to {filename}")
                        except Exception as file_error:
                            self.logger.warning(f"Could not save error response: {file_error}")
                        
                        continue
                    
                    # Check if data contains expected structure
                    if not isinstance(data, dict) or 'data' not in data:
                        self.logger.warning(f"Unexpected API response structure for query '{query}': {data}")
                        continue
                    
                    # Advanced filtering for market-relevant content
                    market_articles = []
                    for article in data.get('data', []):
                        if self._is_market_relevant_article(article, symbol):
                            # Add query info to article for tracking
                            article['search_query'] = query
                            article['source'] = 'api_json'
                            market_articles.append(article)
                    
                    # Remove duplicates by URL
                    new_articles = []
                    for article in market_articles:
                        url = article.get('url', '')
                        if not any(a.get('url') == url for a in all_articles):
                            new_articles.append(article)
                    
                    all_articles.extend(new_articles)
                    self.logger.info(f"JSON API returned {len(market_articles)} articles, {len(new_articles)} new unique")
                    
                else:
                    # Unknown content type
                    self.logger.warning(f"Unknown content type '{content_type}' for query '{query}'. Response: {response.text[:200]}")
                    continue
                    
            except requests.exceptions.Timeout:
                self.logger.error(f"Timeout fetching news for {symbol} with query '{query}'")
                continue
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request error fetching news for {symbol} with query '{query}': {e}")
                continue
            except Exception as e:
                self.logger.error(f"Unexpected error in news fetching for {symbol} with query '{query}': {e}")
                continue
        
        self.logger.info(f"Total unique market-relevant articles for {symbol}: {len(all_articles)}")
        return all_articles

    def _extract_error_from_html(self, html_content: str) -> Optional[str]:
        """
        Extract error information from HTML response
        """
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for common error elements
            error_selectors = [
                '.error', '.error-message', '.alert-error',
                '[class*="error"]', '[class*="Error"]',
                'title', 'h1', 'h2', 'h3'
            ]
            
            for selector in error_selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(strip=True)
                    if text and any(error_word in text.lower() for error_word in ['error', 'invalid', 'unauthorized', 'limit']):
                        return text[:200]  # Limit length
            
            # If no specific error found, return page title
            title = soup.find('title')
            if title:
                return title.get_text(strip=True)[:200]
                
        except Exception as e:
            self.logger.warning(f"Error parsing HTML for error extraction: {e}")
        
        return None

    def _extract_articles_from_html(self, html_content: str, query: str) -> List[Dict]:
        """
        Basic article extraction from HTML content as fallback
        This is a simplified version that extracts basic article information
        """
        articles = []
        
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for common article patterns
            article_selectors = [
                'article', '.article', '[class*="article"]',
                '.news-item', '.story', '.post',
                '.headline', '.title', 'h1', 'h2', 'h3'
            ]
            
            for selector in article_selectors:
                elements = soup.select(selector)
                for element in elements:
                    # Try to extract title and link
                    title_elem = element.find(['h1', 'h2', 'h3', 'h4']) or element
                    title = title_elem.get_text(strip=True)
                    
                    if not title or len(title) < 10:
                        continue
                    
                    # Try to find a link
                    link_elem = element.find('a') or title_elem.find('a')
                    url = link_elem.get('href') if link_elem else None
                    
                    # Try to find description/snippet
                    desc_elem = element.find('p') or element.find('div', class_=lambda x: x and 'desc' in x.lower())
                    description = desc_elem.get_text(strip=True) if desc_elem else ""
                    
                    # Create basic article structure
                    article = {
                        'title': title,
                        'description': description,
                        'snippet': description,
                        'url': url or f"#extracted_{hash(title)}",
                        'published_at': datetime.now().isoformat()
                    }
                    
                    articles.append(article)
                    
        except Exception as e:
            self.logger.warning(f"Error extracting articles from HTML: {e}")
        
        return articles
        
    def _is_market_relevant_article(self, article: Dict, symbol: str) -> bool:
        """
        Check if article is market-relevant for the given instrument
        """
        title = article.get('title', '').lower()
        description = article.get('description', '').lower()
        snippet = article.get('snippet', '').lower()
        content = f"{title} {description} {snippet}"
        
        # Skip articles with very short content
        if len(content.strip()) < 50:
            return False
        
        # Positive indicators - articles about market movement, analysis, trading
        positive_indicators = [
            'price', 'analysis', 'market', 'trading', 'outlook', 
            'prediction', 'forecast', 'technical', 'fundamental',
            'volatility', 'momentum', 'resistance', 'support',
            'breakout', 'breakdown', 'rally', 'crash', 'dump',
            'bullish', 'bearish', 'investment', 'institutional',
            'etf', 'adoption', 'regulation', 'regulatory',
            'earnings', 'revenue', 'profit', 'economic', 'GDP',
            'inflation', 'rates', 'central bank', 'fed', 'ecb', 'boj',
            'stock', 'currency', 'commodity', 'futures', 'options',
            'trading', 'investing', 'portfolio', 'asset', 'security'
        ]
        
        # Negative indicators - generic, non-market content
        negative_indicators = [
            'giveaway', 'airdrop', 'meme', 'celebrity',
            'nft art', 'gaming', 'metaverse', 'entertainment',
            'lifestyle', 'sports', 'celebrity', 'rumor', 'gossip',
            'movie', 'music', 'tv show', 'fashion', 'recipe'
        ]
        
        # Check if article contains market-relevant terms
        has_market_relevance = any(indicator in content for indicator in positive_indicators)
        
        # Check if article is NOT generic/non-market
        is_not_generic = not any(indicator in content for indicator in negative_indicators)
        
        # Must be specifically about the instrument
        is_about_instrument = self._is_about_instrument(content, symbol)
        
        return has_market_relevance and is_not_generic and is_about_instrument

    def _is_about_instrument(self, text: str, symbol: str) -> bool:
        """
        Check if text is about the specific instrument with improved matching
        """
        symbol_terms = {
            'BTCUSD': ['bitcoin', 'btc', 'crypto', 'digital currency', 'cryptocurrency'],
            'US100': ['nasdaq', 'tech stocks', 'technology index', 'us100', 'nasdaq 100'],
            'US30': ['dow jones', 'dow 30', 'industrial average', 'us30', 'dow industrial'],
            'J225': ['nikkei', 'japan stocks', 'japanese index', 'j225', 'nikkei 225'],
            'OIL_CRUDE': ['crude oil', 'wti', 'brent', 'oil price', 'energy', 'petroleum'],
            'USCOCOA': ['cocoa', 'chocolate', 'soft commodity', 'cocoa futures', 'cocoa price'],
            'USDJPY': ['usd/jpy', 'dollar yen', 'usdjpy', 'currency pair', 'usd jpy'],
            'EURJPY': ['eur/jpy', 'euro yen', 'eurjpy', 'currency pair', 'eur jpy']
        }
        
        terms = symbol_terms.get(symbol, [symbol.lower()])
        
        # Check for exact matches in the text
        text_lower = text.lower()
        for term in terms:
            if term in text_lower:
                return True
        
        # For forex pairs, also check without slashes
        if symbol in ['USDJPY', 'EURJPY']:
            base_symbol = symbol[:3] + ' ' + symbol[3:]
            if base_symbol.lower() in text_lower:
                return True
                
        return False

    def _analyze_market_sentiment(self, articles: List[Dict], symbol: str) -> pd.DataFrame:
        """
        Analyzes sentiment with instrument-specific context.
        """
        sentiments = []
        instrument_type = self.instrument_types.get(symbol, 'general')
        
        for article in articles:
            title = article.get('title', '')
            description = article.get('description', '')
            snippet = article.get('snippet', '')
            text = f"{title}. {description}. {snippet}".lower()
            
            # Skip if text is too short for meaningful analysis
            if len(text.strip()) < 20:
                continue
                
            # TextBlob sentiment
            try:
                analysis = TextBlob(text)
                polarity = analysis.sentiment.polarity
                subjectivity = analysis.sentiment.subjectivity
            except Exception as e:
                self.logger.warning(f"TextBlob analysis failed: {e}")
                polarity = 0.0
                subjectivity = 0.5
            
            # Market context adjustment with instrument-specific terms
            market_context_score = self._calculate_market_context(text, instrument_type)
            
            # Adjusted polarity with market context
            adjusted_polarity = polarity + market_context_score
            adjusted_polarity = max(min(adjusted_polarity, 1.0), -1.0)
            
            # Enhanced sentiment classification for trading
            sentiment_label, trade_sentiment = self._classify_sentiment(adjusted_polarity)
            
            # Confidence based on subjectivity and market terms
            confidence = self._calculate_confidence(adjusted_polarity, subjectivity, text, instrument_type)
            
            sentiments.append({
                'title': title[:80] + '...' if len(title) > 80 else title,
                'sentiment': sentiment_label,
                'trade_sentiment': trade_sentiment,
                'polarity': round(adjusted_polarity, 3),
                'raw_polarity': round(polarity, 3),
                'market_context': market_context_score,
                'subjectivity': round(subjectivity, 3),
                'confidence': round(confidence, 3),
                'source': article.get('source', 'Unknown'),
                'search_query': article.get('search_query', 'Unknown'),
                'published_at': article.get('published_at', '')[:16],
                'instrument_type': instrument_type
            })
        
        return pd.DataFrame(sentiments)

    def _calculate_market_context(self, text: str, instrument_type: str) -> float:
        """Calculate market context adjustment score with instrument-specific terms"""
        # Get general and instrument-specific terms
        general_terms = self.sentiment_indicators['general']
        specific_terms = self.sentiment_indicators.get(instrument_type, {})
        
        # Combine terms
        all_bullish = general_terms['bullish_terms'] + specific_terms.get('bullish_terms', [])
        all_bearish = general_terms['bearish_terms'] + specific_terms.get('bearish_terms', [])
        
        # Count occurrences
        bullish_count = sum(1 for term in all_bullish if term in text)
        bearish_count = sum(1 for term in all_bearish if term in text)
        
        # Calculate score (more sophisticated weighting)
        if bullish_count > 0 or bearish_count > 0:
            total_terms = bullish_count + bearish_count
            if total_terms > 0:
                # Weighted score based on term frequency
                base_score = (bullish_count - bearish_count) / total_terms
                return base_score * 0.3  # Scale to reasonable adjustment
        return 0.0

    def _calculate_confidence(self, adjusted_polarity: float, subjectivity: float, 
                            text: str, instrument_type: str) -> float:
        """Calculate confidence score based on multiple factors"""
        # Base confidence from polarity strength
        base_confidence = min(abs(adjusted_polarity) * 2, 0.7)
        
        # Subjectivity adjustment (less subjective = more confident)
        subjectivity_boost = (1 - subjectivity) * 0.2
        
        # Term confidence from instrument-specific terms
        general_terms = self.sentiment_indicators['general']
        specific_terms = self.sentiment_indicators.get(instrument_type, {})
        all_terms = general_terms['bullish_terms'] + general_terms['bearish_terms'] + \
                   specific_terms.get('bullish_terms', []) + specific_terms.get('bearish_terms', [])
        
        term_count = sum(1 for term in all_terms if term in text)
        term_confidence = min(term_count * 0.08, 0.3)  # Cap term contribution
        
        total_confidence = base_confidence + subjectivity_boost + term_confidence
        return min(total_confidence, 1.0)

    def _classify_sentiment(self, adjusted_polarity: float) -> Tuple[str, str]:
        """Classify sentiment based on polarity with refined thresholds"""
        if adjusted_polarity > 0.3:
            return "STRONGLY BULLISH", "BULLISH"
        elif adjusted_polarity > 0.15:
            return "BULLISH", "BULLISH"
        elif adjusted_polarity < -0.3:
            return "STRONGLY BEARISH", "BEARISH"
        elif adjusted_polarity < -0.15:
            return "BEARISH", "BEARISH"
        elif adjusted_polarity > 0.05:
            return "MILDLY BULLISH", "BULLISH"
        elif adjusted_polarity < -0.05:
            return "MILDLY BEARISH", "BEARISH"
        else:
            return "NEUTRAL", "NEUTRAL"

    def _generate_trading_recommendation(self, sentiment_df: pd.DataFrame, symbol: str) -> Tuple[str, float, int, str]:
        """
        Generates trading recommendations based on weighted sentiment analysis.
        """
        if sentiment_df.empty:
            return "NEUTRAL", 0.0, 0, "No market analysis available"
        
        # Calculate weighted scores
        if sentiment_df['confidence'].sum() > 0:
            weighted_polarity = (sentiment_df['polarity'] * sentiment_df['confidence']).sum() / sentiment_df['confidence'].sum()
            weighted_confidence = sentiment_df['confidence'].mean()
        else:
            weighted_polarity = sentiment_df['polarity'].mean()
            weighted_confidence = 0.5
        
        # Sentiment distribution
        bullish_count = len(sentiment_df[sentiment_df['trade_sentiment'] == 'BULLISH'])
        bearish_count = len(sentiment_df[sentiment_df['trade_sentiment'] == 'BEARISH'])
        neutral_count = len(sentiment_df[sentiment_df['trade_sentiment'] == 'NEUTRAL'])
        
        total_articles = len(sentiment_df)
        
        # Enhanced trading logic with instrument-specific confidence thresholds
        confidence_threshold = 0.4  # Lowered threshold for broader coverage
        
        if weighted_confidence > confidence_threshold:
            if weighted_polarity > 0.25 and bullish_count >= max(bearish_count, 1):
                signal = "STRONG BULLISH"
                reasoning = f"Strong bullish sentiment ({bullish_count}/{total_articles} articles) with high confidence"
            elif weighted_polarity > 0.1 and bullish_count > bearish_count:
                signal = "BULLISH"
                reasoning = f"Bullish sentiment ({bullish_count}/{total_articles} articles) with moderate confidence"
            elif weighted_polarity < -0.25 and bearish_count >= max(bullish_count, 1):
                signal = "STRONG BEARISH"
                reasoning = f"Strong bearish sentiment ({bearish_count}/{total_articles} articles) with high confidence"
            elif weighted_polarity < -0.1 and bearish_count > bullish_count:
                signal = "BEARISH"
                reasoning = f"Bearish sentiment ({bearish_count}/{total_articles} articles) with moderate confidence"
            else:
                signal = "NEUTRAL"
                reasoning = f"Mixed signals ({bullish_count} bullish, {bearish_count} bearish, {neutral_count} neutral)"
        else:
            signal = "NEUTRAL"
            reasoning = f"Low confidence in market sentiment analysis ({weighted_confidence:.1%})"
        
        return signal, weighted_polarity, total_articles, reasoning

    def get_available_instruments(self) -> List[str]:
        """Return list of supported instruments"""
        return list(self.instrument_search_queries.keys())

    def print_detailed_analysis(self, symbol: str, sentiment_result: Dict):
        """
        Optional method to print detailed analysis (for debugging/monitoring)
        """
        signal = sentiment_result['signal']
        weighted_polarity = sentiment_result['weighted_polarity']
        total_articles = sentiment_result['total_articles']
        reasoning = sentiment_result['reasoning']
        cache_info = " (CACHE HIT)" if sentiment_result.get('cache_hit') else ""
        sentiment_df = pd.DataFrame(sentiment_result['sentiment_df'])
        
        print("\n" + "="*90)
        print(f"🎯 {symbol} MARKET SENTIMENT ANALYSIS & TRADING EVALUATION{cache_info}")
        print("="*90)
        
        if not sentiment_df.empty:
            print(f"\n📊 MARKET ANALYSIS ARTICLES ({total_articles} relevant):")
            display_columns = ['title', 'sentiment', 'polarity', 'confidence', 'source', 'search_query']
            print(sentiment_df[display_columns].to_string(index=False, max_colwidth=45))
        
        print(f"\n🎯 TRADING RECOMMENDATION: {signal}")
        print(f"   Reasoning: {reasoning}")
        print(f"   Weighted Sentiment: {weighted_polarity:+.3f}")
        print(f"   Instrument Type: {sentiment_result.get('instrument_type', 'unknown')}")
        print(f"   Cache Key: {sentiment_result.get('cache_key', 'N/A')}")


# For backward compatibility and standalone testing
def main():
    """Standalone testing function"""
    import sys
    import time as time_module
    start_time = time_module.time()
    UI_INSTRUMENTS = os.getenv("UI_INSTRUMENTS", "")
    if UI_INSTRUMENTS:
        symbol = UI_INSTRUMENTS.split(",")
    else:
        symbol = ['BTCUSD']
    
    # symbol = ['BTCUSD']
    
    sentiment_trader = SentimentTrader()
    
    # Print cache info
    cache_info = sentiment_trader.get_cache_info()
    print(f"📊 Cache Status: {cache_info}")
    
    for s in symbol:
        if s not in sentiment_trader.get_available_instruments():
            print(f"❌ Instrument {s} not supported. Available instruments:")
            for inst in sentiment_trader.get_available_instruments():
                print(f"   - {inst}")
            return
    
    # Test with caching
    # print("\n🧪 Testing with caching enabled:")
    # for s in symbol:
    #     result = sentiment_trader.get_sentiment_signal(s, max_queries=1, use_cache=False)
    #     sentiment_trader.print_detailed_analysis(s, result)
    
    # Test cache hit
    print("\n🧪 Testing cache hit (same request):")
    for s in symbol:
        result = sentiment_trader.get_sentiment_signal(s, max_queries=1, use_cache=True)
        sentiment_trader.print_detailed_analysis(s, result)
    
    # Test cache clearing
    # print("\n🧪 Testing cache clearing:")
    # sentiment_trader.clear_cache(symbol[0])
    cache_info = sentiment_trader.get_cache_info()
    print(f"📊 Cache Status after clearing: {cache_info}")
    end_time = time_module.time()
    print(f"Sentiment analysis took {end_time - start_time:.2f} seconds")
if __name__ == "__main__":
    main()