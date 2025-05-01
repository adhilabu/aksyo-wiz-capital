from scipy.signal import savgol_filter
import numpy as np

def find_support_resistance(prices, window_size = 21):
    """
    Identifies support and resistance levels in a price time series.
    
    Parameters:
    - prices (numpy.ndarray): Array of last traded prices.
    - window_size (int): Number of points to scan for detecting support and resistance.
    
    Returns:
    - (list, list): A tuple containing two lists:
        - support levels (local minima).
        - resistance levels (local maxima).
    """
    # Ensure the window size is odd for symmetric processing with Savitzky-Golay
    if window_size % 2 == 0:
        window_size += 1

    num_prices = len(prices)
    if num_prices < window_size:
        print("Error: window_size is larger than the number of prices.")
        return [], []

    # Smooth the price data using Savitzky-Golay filter
    smoothed_prices = savgol_filter(prices, window_size, polyorder=3)

    # Calculate the first derivative of the smoothed prices
    price_derivative = np.diff(smoothed_prices, prepend=smoothed_prices[0])

    support_levels = []
    resistance_levels = []

    half_window = window_size // 2

    # Scan for support and resistance levels
    for i in range(num_prices - window_size):
        segment = price_derivative[i:i + window_size]
        first_half = segment[:half_window]
        second_half = segment[half_window:]

        # Check for resistance: rising in the first half, falling in the second half
        if np.all(first_half > 0) and np.all(second_half < 0):
            resistance_levels.append(prices[i + half_window - 1])

        # Check for support: falling in the first half, rising in the second half
        if np.all(first_half < 0) and np.all(second_half > 0):
            support_levels.append(prices[i + half_window - 1])

    return support_levels, resistance_levels
