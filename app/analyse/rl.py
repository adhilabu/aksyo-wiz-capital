import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt

# Download historical data for a stock (e.g., Reliance Industries)
ticker = "RELIANCE.NS"  # NSE ticker for Reliance Industries
data = yf.download(ticker, start="2024-10-01", end="2025-01-20")

# Calculate Moving Averages
data['SMA_50'] = data['Close'].rolling(window=50).mean()  # Short-term SMA
data['SMA_200'] = data['Close'].rolling(window=200).mean()  # Long-term SMA

# Volume Filter Threshold
volume_threshold = data['Volume'].quantile(0.75)  # 75th percentile of volume

# Generate Signals
data['Signal'] = 0
data.loc[50:, 'Signal'] = np.where(
    (data['SMA_50'][50:] > data['SMA_200'][50:]) & (data['Volume'][50:] > volume_threshold), 1, 0
)
data['Position'] = data['Signal'].diff()

# Backtesting
initial_capital = 100000  # Initial capital in INR
position = 0  # 0 = No position, 1 = Long position
portfolio_value = []

for index, row in data.iterrows():
    if row['Position'] == 1 and position == 0:  # Buy Signal
        position = 1
        buy_price = row['Close']
        print(f"Buy at: {buy_price} on {index}")
    elif row['Position'] == -1 and position == 1:  # Sell Signal
        position = 0
        sell_price = row['Close']
        print(f"Sell at: {sell_price} on {index}")
        # Calculate Profit/Loss
        pnl = (sell_price - buy_price) * (initial_capital // buy_price)
        initial_capital += pnl
        print(f"PnL: {pnl}, Portfolio Value: {initial_capital}")
    portfolio_value.append(initial_capital)

# Plot Results
data['Portfolio_Value'] = portfolio_value
plt.figure(figsize=(14, 7))
plt.plot(data['Portfolio_Value'], label='Portfolio Value')
plt.title(f"{ticker} Trading Strategy Performance")
plt.xlabel('Date')
plt.ylabel('Portfolio Value (INR)')
plt.legend()
plt.show()

# Print Final Portfolio Value
print(f"Final Portfolio Value: {initial_capital}")