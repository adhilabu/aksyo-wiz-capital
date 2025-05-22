# def opening_range_breakout(symbol_data, range_minutes=30, risk_reward_ratio=2):
#     print(f'Implementing ORB strategy for {symbol_data["stock"].iloc[0]}...')
    
#     # Create a copy of the data to avoid modifying the original
#     data = symbol_data.copy()
    
#     # Convert timestamp to datetime if not already
#     if not pd.api.types.is_datetime64_any_dtype(data['timestamp']):
#         data['timestamp'] = pd.to_datetime(data['timestamp'])
    
#     # Group by date to identify opening ranges
#     data['date'] = data['timestamp'].dt.date
    
#     # Process each day separately
#     trades = []
#     for date, day_data in data.groupby('date'):
#         # Reset index for easier access
#         day_data = day_data.reset_index(drop=True)
        
#         # Skip days with insufficient data
#         if len(day_data) < range_minutes + 10:  # Need enough data for range + some trading time
#             continue
        
#         # Define the opening range
#         opening_range = day_data.iloc[:range_minutes]
#         range_high = opening_range['high'].max()
#         range_low = opening_range['low'].min()
#         range_size = range_high - range_low
        
#         # Skip days with very small ranges (avoid noise)
#         if range_size < day_data['close'].iloc[0] * 0.001:  # Range less than 0.1% of price
#             continue
        
#         # Look for breakouts after the opening range
#         in_long = False
#         in_short = False
#         entry_price = 0
#         entry_time = None
#         stop_loss = 0
#         take_profit = 0
        
#         for i in range(range_minutes, len(day_data)):
#             current_row = day_data.iloc[i]
            
#             # Check for breakouts if not in a position
#             if not in_long and not in_short:
#                 # Long breakout
#                 if current_row['high'] > range_high:
#                     in_long = True
#                     entry_price = range_high  # Assume entry at breakout level
#                     entry_time = current_row['timestamp']
#                     stop_loss = range_low
#                     take_profit = entry_price + (entry_price - stop_loss) * risk_reward_ratio
                    
#                 # Short breakout
#                 elif current_row['low'] < range_low:
#                     in_short = True
#                     entry_price = range_low  # Assume entry at breakout level
#                     entry_time = current_row['timestamp']
#                     stop_loss = range_high
#                     take_profit = entry_price - (stop_loss - entry_price) * risk_reward_ratio
            
#             # Check for exit conditions if in a position
#             elif in_long:
#                 # Stop loss hit
#                 if current_row['low'] <= stop_loss:
#                     trades.append({
#                         'symbol': day_data['stock'].iloc[0],
#                         'entry_time': entry_time,
#                         'exit_time': current_row['timestamp'],
#                         'entry_price': entry_price,
#                         'exit_price': stop_loss,
#                         'direction': 'long',
#                         'pnl': stop_loss - entry_price,
#                         'pnl_percent': (stop_loss - entry_price) / entry_price * 100,
#                         'exit_type': 'stop_loss',
#                         'strategy': 'ORB',
#                         'duration': (current_row['timestamp'] - entry_time).total_seconds() / 60  # in minutes
#                     })
#                     in_long = False
                
#                 # Take profit hit
#                 elif current_row['high'] >= take_profit:
#                     trades.append({
#                         'symbol': day_data['stock'].iloc[0],
#                         'entry_time': entry_time,
#                         'exit_time': current_row['timestamp'],
#                         'entry_price': entry_price,
#                         'exit_price': take_profit,
#                         'direction': 'long',
#                         'pnl': take_profit - entry_price,
#                         'pnl_percent': (take_profit - entry_price) / entry_price * 100,
#                         'exit_type': 'take_profit',
#                         'strategy': 'ORB',
#                         'duration': (current_row['timestamp'] - entry_time).total_seconds() / 60  # in minutes
#                     })
#                     in_long = False
            
#             elif in_short:
#                 # Stop loss hit
#                 if current_row['high'] >= stop_loss:
#                     trades.append({
#                         'symbol': day_data['stock'].iloc[0],
#                         'entry_time': entry_time,
#                         'exit_time': current_row['timestamp'],
#                         'entry_price': entry_price,
#                         'exit_price': stop_loss,
#                         'direction': 'short',
#                         'pnl': entry_price - stop_loss,
#                         'pnl_percent': (entry_price - stop_loss) / entry_price * 100,
#                         'exit_type': 'stop_loss',
#                         'strategy': 'ORB',
#                         'duration': (current_row['timestamp'] - entry_time).total_seconds() / 60  # in minutes
#                     })
#                     in_short = False
                
#                 # Take profit hit
#                 elif current_row['low'] <= take_profit:
#                     trades.append({
#                         'symbol': day_data['stock'].iloc[0],
#                         'entry_time': entry_time,
#                         'exit_time': current_row['timestamp'],
#                         'entry_price': entry_price,
#                         'exit_price': take_profit,
#                         'direction': 'short',
#                         'pnl': entry_price - take_profit,
#                         'pnl_percent': (entry_price - take_profit) / entry_price * 100,
#                         'exit_type': 'take_profit',
#                         'strategy': 'ORB',
#                         'duration': (current_row['timestamp'] - entry_time).total_seconds() / 60  # in minutes
#                     })
#                     in_short = False
            
#             # Close any open positions at the end of the day
#             if i == len(day_data) - 1:
#                 if in_long:
#                     trades.append({
#                         'symbol': day_data['stock'].iloc[0],
#                         'entry_time': entry_time,
#                         'exit_time': current_row['timestamp'],
#                         'entry_price': entry_price,
#                         'exit_price': current_row['close'],
#                         'direction': 'long',
#                         'pnl': current_row['close'] - entry_price,
#                         'pnl_percent': (current_row['close'] - entry_price) / entry_price * 100,
#                         'exit_type': 'day_end',
#                         'strategy': 'ORB',
#                         'duration': (current_row['timestamp'] - entry_time).total_seconds() / 60  # in minutes
#                     })
#                 elif in_short:
#                     trades.append({
#                         'symbol': day_data['stock'].iloc[0],
#                         'entry_time': entry_time,
#                         'exit_time': current_row['timestamp'],
#                         'entry_price': entry_price,
#                         'exit_price': current_row['close'],
#                         'direction': 'short',
#                         'pnl': entry_price - current_row['close'],
#                         'pnl_percent': (entry_price - current_row['close']) / entry_price * 100,
#                         'exit_type': 'day_end',
#                         'strategy': 'ORB',
#                         'duration': (current_row['timestamp'] - entry_time).total_seconds() / 60  # in minutes
#                     })
    
#     return pd.DataFrame(trades) if trades else pd.DataFrame()
