from datetime import datetime, timedelta, timezone
import json
import pandas as pd
import pytz
import traceback


def filter_data_by_interval(data_dict):
    """
    Filter the data_dict to include only entries where the timestamp is a multiple of push_interval.
    Handles both 'marketFF' and 'indexFF'.
    """
    feeds = data_dict.get("feeds", {})
    value_exists = False
    filtered_feeds = {}
    for feed_key, feed_value in feeds.items():
        full_feed = feed_value.get("fullFeed", {})
        
        # Check if marketFF exists
        market_ohlc = full_feed.get("marketFF", {}).get("marketOHLC", {}).get("ohlc", [])
        filtered_market_ohlc = [entry for entry in market_ohlc if entry.get("interval") == "I1"]
        
        # Check if indexFF exists
        index_ohlc = full_feed.get("indexFF", {}).get("marketOHLC", {}).get("ohlc", [])
        filtered_index_ohlc = [entry for entry in index_ohlc if entry.get("interval") == "I1"]
        
        # Merge filtered OHLC data
        filtered_ohlc = filtered_market_ohlc + filtered_index_ohlc
        
        # Update the feed only if there is filtered data
        if filtered_ohlc:
            value_exists = True
            if "marketFF" in full_feed:
                full_feed["marketFF"]["marketOHLC"]["ohlc"] = filtered_market_ohlc
            if "indexFF" in full_feed:
                full_feed["indexFF"]["marketOHLC"]["ohlc"] = filtered_index_ohlc
            
            filtered_feeds[feed_key] = feed_value

    if value_exists:
        data_dict['feeds'] = filtered_feeds
        return data_dict
    return None

def transform_data(json_data) -> pd.DataFrame:
    """
    Transforms the filtered JSON data into a structured Pandas DataFrame.
    Handles both 'marketFF' and 'indexFF' data sources.
    """
    data_list = []
    feeds = json_data.get("feeds", {})

    for stock_key, stock_data in feeds.items():
        full_feed = stock_data.get("fullFeed", {})

        # Extract OHLC data from both 'marketFF' and 'indexFF'
        market_ohlc = full_feed.get("marketFF", {}).get("marketOHLC", {}).get("ohlc", [])
        index_ohlc = full_feed.get("indexFF", {}).get("marketOHLC", {}).get("ohlc", [])
        all_ohlc = market_ohlc + index_ohlc  # Merge both lists

        # Extract LTP and Open Interest data from either 'marketFF' or 'indexFF'
        ltpc_data = full_feed.get("marketFF", {}).get("ltpc", {})
        if not ltpc_data:  # Fallback to indexFF if marketFF does not have ltpc data
            ltpc_data = full_feed.get("indexFF", {}).get("ltpc", {})

        for market_data in all_ohlc:
            try:
                timestamp = datetime.fromtimestamp(int(market_data.get("ts")) / 1000, tz=timezone.utc).astimezone(pytz.timezone("Asia/Kolkata"))
                if timestamp < timestamp.replace(hour=9, minute=15):
                    continue
                
                data = {
                    "stock": stock_key,
                    "interval": market_data.get("interval"),
                    "open": market_data.get("open"),
                    "high": market_data.get("high"),
                    "low": market_data.get("low"),
                    "close": market_data.get("close"),
                    "volume": int(market_data.get("vol", 0)),
                    "timestamp": timestamp.replace(tzinfo=None),
                    "ltp": ltpc_data.get("ltp"),
                    "open_interest": ltpc_data.get("oi", 0)
                }
                data_list.append(data)
            except Exception as e:
                print(f"Error processing OHLC data for {stock_key}: {e}")
                print(traceback.format_exc())

    transformed_data = pd.DataFrame(data_list)
    return transformed_data


# def transform_data_temp(json_data) -> pd.DataFrame:
#     updated_data_list = []
#     data_list = json_data.get("feeds")
#     for data in data_list:
#         timestamp = datetime.fromtimestamp(data["minute_timestamp"], tz=timezone.utc).astimezone(pytz.timezone("Asia/Kolkata"))
#         ohlc = data["ohlc"]
#         updated_data = {
#             "stock": data["instrument"],
#             "interval": "1m",
#             "open": ohlc["open"],
#             "high": ohlc["high"],
#             "low": ohlc["low"],
#             "close": ohlc["close"],
#             "volume": data["volume"],
#             "timestamp": timestamp.replace(tzinfo=None),
#             "ltp": ohlc["close"],
#             "open_interest": 0
#         }
#         updated_data_list.append(updated_data)
#     transformed_data = pd.DataFrame(updated_data_list)
#     return transformed_data