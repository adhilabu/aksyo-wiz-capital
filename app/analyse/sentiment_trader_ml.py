import requests
import pandas as pd
import numpy as np
from textblob import TextBlob
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import warnings
warnings.filterwarnings('ignore')

load_dotenv(dotenv_path=".env")

# Configuration
THE_NEWS_API_KEY = os.getenv("THE_NEWS_API_KEY")

# Supported instruments with specific search queries
INSTRUMENTS = {
    "BTCUSD": {
        "type": "crypto",
        "search_queries": [
            "Bitcoin technical analysis", "BTC price prediction", 
            "Bitcoin market outlook", "BTC trading signals",
            "Bitcoin volatility", "BTC institutional flows"
        ],
        "categories": "business,tech"
    },
    "OIL_CRUDE": {
        "type": "commodity", 
        "search_queries": [
            "crude oil price", "oil market analysis", "OPEC news",
            "oil supply demand", "crude oil trading", "energy markets"
        ],
        "categories": "business,politics"
    },
    "US100": {
        "type": "index",
        "search_queries": [
            "Nasdaq 100 analysis", "tech stocks outlook", 
            "US100 technical analysis", "Nasdaq market prediction"
        ],
        "categories": "business,tech"
    },
    "J225": {
        "type": "index",
        "search_queries": [
            "Nikkei 225 analysis", "Japan stock market",
            "J225 technical analysis", "Japanese equities"
        ],
        "categories": "business"
    },
    "US30": {
        "type": "index", 
        "search_queries": [
            "Dow Jones analysis", "US30 technical analysis",
            "Dow Jones industrial average", "blue chip stocks"
        ],
        "categories": "business"
    },
    "USCOCOA": {
        "type": "commodity",
        "search_queries": [
            "cocoa prices", "cocoa market", "chocolate supply",
            "cocoa futures", "agricultural commodities"
        ],
        "categories": "business"
    },
    "USDJPY": {
        "type": "forex",
        "search_queries": [
            "USD JPY analysis", "dollar yen exchange rate",
            "USDJPY technical", "Bank of Japan", "Federal Reserve"
        ],
        "categories": "business,politics"
    },
    "EURJPY": {
        "type": "forex", 
        "search_queries": [
            "EUR JPY analysis", "euro yen exchange rate",
            "EURJPY technical", "European Central Bank"
        ],
        "categories": "business,politics"
    }
}

class MLTrendAnalyzer:
    """Machine Learning trend analysis for market sentiment"""
    
    def __init__(self):
        self.model = RandomForestClassifier(n_estimators=100, random_state=42)
        self.scaler = StandardScaler()
        self.is_trained = False
        
    def prepare_features(self, sentiment_data):
        """Prepare features for ML model"""
        features = []
        
        for data in sentiment_data:
            feature_vector = [
                data['weighted_polarity'],           # Overall sentiment
                data['bullish_ratio'],               # Bullish article ratio
                data['avg_confidence'],              # Analysis confidence
                data['sentiment_strength'],          # Absolute sentiment strength
                data['consistency_score'],           # How consistent are signals
                len(data['articles']),               # Number of articles
                data['recency_score']                # How recent are articles
            ]
            features.append(feature_vector)
            
        return np.array(features)
    
    def train_model(self, historical_data, historical_labels):
        """Train the ML model on historical data"""
        if len(historical_data) < 10:
            print("⚠️  Insufficient historical data for ML training")
            return
            
        X = self.prepare_features(historical_data)
        y = np.array(historical_labels)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train model
        self.model.fit(X_train_scaled, y_train)
        
        # Evaluate
        y_pred = self.model.predict(X_test_scaled)
        accuracy = accuracy_score(y_test, y_pred)
        
        print(f"🤖 ML Model trained with {accuracy:.1%} accuracy")
        self.is_trained = True
    
    def predict_trend(self, current_sentiment):
        """Predict market trend using ML model"""
        if not self.is_trained:
            return "MODEL_NOT_TRAINED", 0.5
            
        features = self.prepare_features([current_sentiment])
        features_scaled = self.scaler.transform(features)
        
        prediction = self.model.predict(features_scaled)[0]
        probability = np.max(self.model.predict_proba(features_scaled))
        
        trend_map = {0: "BEARISH", 1: "NEUTRAL", 2: "BULLISH"}
        return trend_map[prediction], probability

def fetch_instrument_news(api_key, instrument, days_back=2):
    """Fetch news for specific financial instrument"""
    url = "https://api.thenewsapi.com/v1/news/all"
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    instrument_config = INSTRUMENTS.get(instrument, INSTRUMENTS["BTCUSD"])
    primary_query = instrument_config["search_queries"][0]
    
    params = {
        "api_token": api_key,
        "search": primary_query,
        "language": "en",
        "limit": 8,  # Increased for better analysis
        "published_after": start_date.strftime("%Y-%m-%d"),
        "categories": instrument_config.get("categories", "business"),
        "exclude_categories": "sports,entertainment,lifestyle"
    }
    
    try:
        print(f"🔍 Fetching news for {instrument}...")
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        articles = data.get('data', [])
        print(f"   Found {len(articles)} articles")
        
        # Filter relevant articles
        relevant_articles = filter_relevant_articles(articles, instrument, instrument_config)
        print(f"   Relevant articles: {len(relevant_articles)}")
        
        return relevant_articles
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching {instrument} news: {e}")
        return []
    except Exception as e:
        print(f"❌ Unexpected error for {instrument}: {e}")
        return []

def filter_relevant_articles(articles, instrument, config):
    """Filter articles for relevance to the specific instrument"""
    relevant_articles = []
    
    # Instrument-specific keywords
    if instrument == "BTCUSD":
        keywords = ['bitcoin', 'btc', 'crypto', 'cryptocurrency', 'digital currency']
    elif instrument == "OIL_CRUDE":
        keywords = ['oil', 'crude', 'opec', 'energy', 'petroleum', 'brent', 'wti']
    elif instrument == "US100":
        keywords = ['nasdaq', 'tech stocks', 'technology', 'us100', 'growth stocks']
    elif instrument == "J225":
        keywords = ['nikkei', 'japan', 'japanese', 'j225', 'tokyo stock']
    elif instrument == "US30":
        keywords = ['dow jones', 'dow', 'us30', 'industrial', 'blue chip']
    elif instrument == "USCOCOA":
        keywords = ['cocoa', 'chocolate', 'commodity', 'agriculture', 'cocoa beans']
    elif instrument == "USDJPY":
        keywords = ['usd/jpy', 'dollar yen', 'usdjpy', 'japanese yen', 'forex']
    elif instrument == "EURJPY":
        keywords = ['eur/jpy', 'euro yen', 'eurjpy', 'euro japanese']
    else:
        keywords = [instrument.lower()]
    
    for article in articles:
        title = article.get('title', '').lower()
        description = article.get('description', '').lower()
        content = f"{title} {description}"
        
        # Check if article mentions instrument-specific keywords
        if any(keyword in content for keyword in keywords):
            relevant_articles.append(article)
    
    return relevant_articles

def analyze_market_sentiment(articles, instrument):
    """Advanced sentiment analysis with market context"""
    if not articles:
        return None
    
    sentiments = []
    
    # Market-specific sentiment modifiers
    sentiment_modifiers = get_sentiment_modifiers(instrument)
    
    for article in articles:
        title = article.get('title', '')
        description = article.get('description', '')
        text = f"{title}. {description}"
        
        # TextBlob sentiment
        analysis = TextBlob(text)
        polarity = analysis.sentiment.polarity
        subjectivity = analysis.sentiment.subjectivity
        
        # Apply market-specific sentiment adjustment
        adjusted_polarity = apply_market_context(polarity, text, sentiment_modifiers)
        
        # Enhanced classification
        sentiment_label, trade_sentiment = classify_sentiment(adjusted_polarity)
        
        # Calculate confidence
        confidence = calculate_confidence(adjusted_polarity, subjectivity, text, sentiment_modifiers)
        
        # Recency score (more recent = higher weight)
        published_at = article.get('published_at', '')
        recency_score = calculate_recency_score(published_at)
        
        sentiments.append({
            'title': title[:60] + '...' if len(title) > 60 else title,
            'sentiment': sentiment_label,
            'trade_sentiment': trade_sentiment,
            'polarity': round(adjusted_polarity, 3),
            'confidence': round(confidence * recency_score, 3),  # Weight by recency
            'source': article.get('source', 'Unknown'),
            'published_at': published_at[:16]
        })
    
    return aggregate_sentiment_analysis(sentiments, instrument)

def get_sentiment_modifiers(instrument):
    """Get sentiment modifiers specific to instrument type"""
    base_modifiers = {
        'bullish_terms': ['bullish', 'rally', 'surge', 'soar', 'breakout', 'recovery', 'outperform'],
        'bearish_terms': ['bearish', 'crash', 'plunge', 'drop', 'breakdown', 'decline', 'underperform'],
        'strong_modifiers': ['surge', 'crash', 'plunge', 'soar', 'collapse', 'explode'],
        'weak_modifiers': ['slightly', 'modestly', 'gradually', 'marginally']
    }
    
    # Add instrument-specific terms
    if instrument == "BTCUSD":
        base_modifiers['bullish_terms'].extend(['adoption', 'institutional', 'halving', 'bull run'])
        base_modifiers['bearish_terms'].extend(['regulation', 'ban', 'crackdown', 'bear market'])
    elif instrument == "OIL_CRUDE":
        base_modifiers['bullish_terms'].extend(['supply cut', 'inventory draw', 'geopolitical tension'])
        base_modifiers['bearish_terms'].extend(['demand worry', 'inventory build', 'recession fear'])
    
    return base_modifiers

def apply_market_context(polarity, text, modifiers):
    """Apply market context to sentiment score"""
    text_lower = text.lower()
    
    # Count bullish and bearish terms
    bullish_count = sum(1 for term in modifiers['bullish_terms'] if term in text_lower)
    bearish_count = sum(1 for term in modifiers['bearish_terms'] if term in text_lower)
    
    # Apply context adjustment
    context_score = (bullish_count - bearish_count) * 0.05
    adjusted_polarity = polarity + context_score
    
    # Apply strong/weak modifiers
    strong_count = sum(1 for term in modifiers['strong_modifiers'] if term in text_lower)
    weak_count = sum(1 for term in modifiers['weak_modifiers'] if term in text_lower)
    
    if strong_count > weak_count:
        adjusted_polarity *= 1.2  # Amplify strong moves
    
    return max(min(adjusted_polarity, 1.0), -1.0)

def classify_sentiment(polarity):
    """Classify sentiment with finer granularity"""
    if polarity > 0.3:
        return "STRONGLY BULLISH", "BULLISH"
    elif polarity > 0.15:
        return "BULLISH", "BULLISH"
    elif polarity > 0.05:
        return "MILDLY BULLISH", "BULLISH"
    elif polarity < -0.3:
        return "STRONGLY BEARISH", "BEARISH"
    elif polarity < -0.15:
        return "BEARISH", "BEARISH"
    elif polarity < -0.05:
        return "MILDLY BEARISH", "BEARISH"
    else:
        return "NEUTRAL", "NEUTRAL"

def calculate_confidence(polarity, subjectivity, text, modifiers):
    """Calculate confidence score for sentiment analysis"""
    # Base confidence from polarity strength
    base_confidence = min(abs(polarity) * 2, 0.6)
    
    # Adjust for subjectivity (less subjective = more confident)
    objectivity_bonus = (1 - subjectivity) * 0.2
    
    # Term-based confidence
    text_lower = text.lower()
    term_count = sum(1 for term in modifiers['bullish_terms'] + modifiers['bearish_terms'] if term in text_lower)
    term_confidence = min(term_count * 0.05, 0.2)
    
    return base_confidence + objectivity_bonus + term_confidence

def calculate_recency_score(published_at):
    """Calculate recency score for article weighting"""
    try:
        if not published_at:
            return 0.7
        
        # Parse timestamp (simplified)
        article_time = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
        current_time = datetime.now().replace(tzinfo=article_time.tzinfo)
        hours_diff = (current_time - article_time).total_seconds() / 3600
        
        # More recent articles get higher scores
        if hours_diff < 6:
            return 1.0
        elif hours_diff < 24:
            return 0.9
        elif hours_diff < 48:
            return 0.7
        else:
            return 0.5
    except:
        return 0.7

def aggregate_sentiment_analysis(sentiments, instrument):
    """Aggregate sentiment analysis across all articles"""
    if not sentiments:
        return None
    
    df = pd.DataFrame(sentiments)
    
    # Weighted average polarity
    if df['confidence'].sum() > 0:
        weighted_polarity = (df['polarity'] * df['confidence']).sum() / df['confidence'].sum()
    else:
        weighted_polarity = df['polarity'].mean()
    
    # Sentiment distribution
    bullish_count = len(df[df['trade_sentiment'] == 'BULLISH'])
    bearish_count = len(df[df['trade_sentiment'] == 'BEARISH'])
    neutral_count = len(df[df['trade_sentiment'] == 'NEUTRAL'])
    total_articles = len(df)
    
    # Calculate metrics for ML
    bullish_ratio = bullish_count / total_articles if total_articles > 0 else 0
    avg_confidence = df['confidence'].mean()
    sentiment_strength = abs(weighted_polarity)
    consistency_score = max(bullish_ratio, bearish_count/total_articles) if total_articles > 0 else 0
    recency_score = df['confidence'].mean()  # Simplified
    
    return {
        'instrument': instrument,
        'articles': sentiments,
        'weighted_polarity': weighted_polarity,
        'bullish_ratio': bullish_ratio,
        'avg_confidence': avg_confidence,
        'sentiment_strength': sentiment_strength,
        'consistency_score': consistency_score,
        'recency_score': recency_score,
        'bullish_count': bullish_count,
        'bearish_count': bearish_count,
        'neutral_count': neutral_count,
        'total_articles': total_articles
    }

def generate_trading_signals(sentiment_analysis, ml_analyzer):
    """Generate trading signals using ML and sentiment analysis"""
    if not sentiment_analysis:
        return "NO_DATA", 0.0, "Insufficient data for analysis"
    
    # Get ML prediction if model is trained
    ml_trend, ml_confidence = "NEUTRAL", 0.5
    if ml_analyzer.is_trained:
        ml_trend, ml_confidence = ml_analyzer.predict_trend(sentiment_analysis)
    
    weighted_polarity = sentiment_analysis['weighted_polarity']
    bullish_ratio = sentiment_analysis['bullish_ratio']
    avg_confidence = sentiment_analysis['avg_confidence']
    
    # Combine ML prediction with sentiment analysis
    if ml_confidence > 0.7:  # High ML confidence
        if ml_trend == "BULLISH" and weighted_polarity > 0.1:
            signal = "STRONG BUY"
            reasoning = f"ML predicts BULLISH trend ({ml_confidence:.1%} confidence) + positive sentiment"
        elif ml_trend == "BEARISH" and weighted_polarity < -0.1:
            signal = "STRONG SELL"
            reasoning = f"ML predicts BEARISH trend ({ml_confidence:.1%} confidence) + negative sentiment"
        else:
            signal = "HOLD - ML CONFLICT"
            reasoning = f"ML trend ({ml_trend}) conflicts with sentiment analysis"
    else:
        # Fall back to sentiment-based signals
        if weighted_polarity > 0.2 and bullish_ratio > 0.6 and avg_confidence > 0.6:
            signal = "BUY"
            reasoning = f"Strong bullish sentiment ({weighted_polarity:+.3f}) with high confidence"
        elif weighted_polarity < -0.2 and bullish_ratio < 0.3 and avg_confidence > 0.6:
            signal = "SELL"
            reasoning = f"Strong bearish sentiment ({weighted_polarity:+.3f}) with high confidence"
        elif weighted_polarity > 0.1 and bullish_ratio > 0.5:
            signal = "WEAK BUY"
            reasoning = f"Moderate bullish sentiment ({weighted_polarity:+.3f})"
        elif weighted_polarity < -0.1 and bullish_ratio < 0.4:
            signal = "WEAK SELL"
            reasoning = f"Moderate bearish sentiment ({weighted_polarity:+.3f})"
        else:
            signal = "HOLD"
            reasoning = "Mixed or neutral market sentiment"
    
    return signal, weighted_polarity, reasoning, ml_trend, ml_confidence

def print_comprehensive_analysis(instrument, sentiment_analysis, signal, reasoning, ml_trend, ml_confidence):
    """Print comprehensive market analysis"""
    print(f"\n{'='*80}")
    print(f"🎯 {instrument} - COMPREHENSIVE MARKET ANALYSIS")
    print(f"{'='*80}")
    
    if not sentiment_analysis:
        print("❌ No analysis available - insufficient data")
        return
    
    # Display articles
    print(f"\n📊 ANALYZED ARTICLES ({sentiment_analysis['total_articles']}):")
    df = pd.DataFrame(sentiment_analysis['articles'])
    display_cols = ['title', 'sentiment', 'polarity', 'confidence', 'source']
    print(df[display_cols].to_string(index=False, max_colwidth=40))
    
    # Market metrics
    print(f"\n📈 MARKET METRICS:")
    print(f"   Weighted Sentiment: {sentiment_analysis['weighted_polarity']:+.3f}")
    print(f"   Bullish Ratio: {sentiment_analysis['bullish_ratio']:.1%}")
    print(f"   Analysis Confidence: {sentiment_analysis['avg_confidence']:.1%}")
    print(f"   Sentiment Strength: {sentiment_analysis['sentiment_strength']:.3f}")
    
    # ML Insights
    print(f"\n🤖 ML TREND ANALYSIS:")
    print(f"   Predicted Trend: {ml_trend}")
    print(f"   ML Confidence: {ml_confidence:.1%}")
    
    # Trading signal
    print(f"\n🎯 TRADING SIGNAL: {signal}")
    print(f"   Reasoning: {reasoning}")
    
    # Risk assessment
    print(f"\n⚠️  RISK ASSESSMENT:")
    confidence = sentiment_analysis['avg_confidence']
    if confidence > 0.7:
        print("   ✅ HIGH CONFIDENCE - Strong signal reliability")
    elif confidence > 0.5:
        print("   ⚠️  MODERATE CONFIDENCE - Consider additional factors")
    else:
        print("   🔴 LOW CONFIDENCE - Use caution, verify with technical analysis")

def main():
    """Main execution function"""
    print("🚀 ADVANCED MULTI-INSTRUMENT SENTIMENT ANALYZER WITH ML")
    print(f"📅 Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    # Initialize ML analyzer
    ml_analyzer = MLTrendAnalyzer()
    
    # Note: For production, you would load historical data here
    # ml_analyzer.train_model(historical_data, historical_labels)
    
    results = []
    
    # Analyze each instrument
    for instrument in INSTRUMENTS.keys():
        print(f"\n{'='*50}")
        print(f"Analyzing {instrument}...")
        print(f"{'='*50}")
        
        # Fetch and analyze news
        articles = fetch_instrument_news(THE_NEWS_API_KEY, instrument)
        sentiment_analysis = analyze_market_sentiment(articles, instrument)
        
        if sentiment_analysis:
            # Generate trading signals
            signal, polarity, reasoning, ml_trend, ml_confidence = generate_trading_signals(
                sentiment_analysis, ml_analyzer
            )
            
            # Print analysis
            print_comprehensive_analysis(instrument, sentiment_analysis, signal, reasoning, ml_trend, ml_confidence)
            
            # Store results
            results.append({
                'instrument': instrument,
                'signal': signal,
                'sentiment': polarity,
                'confidence': sentiment_analysis['avg_confidence'],
                'articles_count': sentiment_analysis['total_articles'],
                'ml_trend': ml_trend,
                'ml_confidence': ml_confidence
            })
        else:
            print(f"❌ No sufficient data for {instrument}")
            results.append({
                'instrument': instrument,
                'signal': 'NO_DATA',
                'sentiment': 0.0,
                'confidence': 0.0,
                'articles_count': 0,
                'ml_trend': 'UNKNOWN',
                'ml_confidence': 0.0
            })
    
    # Print summary report
    print_summary_report(results)

def print_summary_report(results):
    """Print executive summary report"""
    print(f"\n{'='*100}")
    print("📋 EXECUTIVE SUMMARY - MULTI-INSTRUMENT ANALYSIS")
    print(f"{'='*100}")
    
    df = pd.DataFrame(results)
    
    # Categorize by signal strength
    strong_buy = df[df['signal'].str.contains('STRONG BUY', case=False)]
    buy = df[df['signal'].str.contains('BUY', case=False) & ~df['signal'].str.contains('STRONG', case=False)]
    strong_sell = df[df['signal'].str.contains('STRONG SELL', case=False)]
    sell = df[df['signal'].str.contains('SELL', case=False) & ~df['signal'].str.contains('STRONG', case=False)]
    
    print(f"\n🎯 TRADING OPPORTUNITIES:")
    print(f"   🟢 STRONG BUY: {len(strong_buy)} instruments")
    for inst in strong_buy['instrument']:
        print(f"      • {inst}")
    
    print(f"   ✅ BUY: {len(buy)} instruments")
    for inst in buy['instrument']:
        print(f"      • {inst}")
    
    print(f"   🔴 STRONG SELL: {len(strong_sell)} instruments")
    for inst in strong_sell['instrument']:
        print(f"      • {inst}")
    
    print(f"   🔻 SELL: {len(sell)} instruments")
    for inst in sell['instrument']:
        print(f"      • {inst}")
    
    # Highest conviction trades
    high_confidence = df[df['confidence'] > 0.7]
    if not high_confidence.empty:
        print(f"\n💎 HIGHEST CONVICTION TRADES (Confidence > 70%):")
        for _, row in high_confidence.iterrows():
            print(f"   • {row['instrument']}: {row['signal']} (Confidence: {row['confidence']:.1%})")
    
    # Market overview
    avg_sentiment = df[df['sentiment'] != 0]['sentiment'].mean()
    print(f"\n📊 MARKET OVERVIEW:")
    print(f"   Average Market Sentiment: {avg_sentiment:+.3f}")
    print(f"   Total Instruments Analyzed: {len(df)}")
    print(f"   Instruments with Strong Signals: {len(strong_buy) + len(strong_sell)}")
    
    print(f"\n⏰ Analysis Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()