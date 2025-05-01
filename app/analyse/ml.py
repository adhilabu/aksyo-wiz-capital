import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

class TradingDayAnalyzer:
    def __init__(self, model=None, historical_data=None, probability_threshold=0.7, future_period=5):
        self.model = model if model else RandomForestClassifier()
        self.historical_data = historical_data
        self.probability_threshold = probability_threshold  # Minimum probability to take a trade
        self.future_period = future_period  # Number of candles to look ahead for profit calculation

    def find_similar_trading_days(
        self,
        historical_data: pd.DataFrame,
        current_day_data: pd.DataFrame,
        similarity_threshold: float = 0.9
    ) -> list:
        historical_data = historical_data.sort_values(by="timestamp")
        current_day_data = current_day_data.sort_values(by="timestamp")
        current_time_limit = current_day_data['timestamp'].max()
        current_day_pattern = current_day_data[current_day_data['timestamp'] <= current_time_limit][['open', 'high', 'low', 'close']].values.flatten()
        historical_data['date'] = historical_data['timestamp'].dt.date
        unique_dates = historical_data['date'].unique()
        similar_days = []
        for date in unique_dates:
            historical_day_data = historical_data[historical_data['date'] == date]
            historical_day_pattern = historical_day_data[historical_day_data['timestamp'].dt.time <= current_time_limit.time()][['open', 'high', 'low', 'close']].values.flatten()
            if len(historical_day_pattern) != len(current_day_pattern):
                continue

            similarity = np.dot(
                current_day_pattern, historical_day_pattern
            ) / (
                np.linalg.norm(current_day_pattern) * np.linalg.norm(historical_day_pattern)
            )

            if similarity >= similarity_threshold:
                similar_days.append(date)

        return similar_days

    def train_entry_exit_model(self, historical_data: pd.DataFrame):
        """
        Trains a model to predict entry and exit points based on historical data.
        The target is whether the price will increase over the next `future_period` candles.
        """
        # Calculate future price change
        historical_data['future_close'] = historical_data['close'].shift(-self.future_period)
        historical_data['price_change'] = historical_data['future_close'] - historical_data['close']
        historical_data['target'] = (historical_data['price_change'] > 0).astype(int)

        # Drop rows with NaN values (last `future_period` rows will have NaN for `future_close`)
        historical_data = historical_data.dropna()

        features = historical_data[['open', 'high', 'low', 'close', 'volume']]
        targets = historical_data['target']

        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)

        X_train, X_test, y_train, y_test = train_test_split(features_scaled, targets, test_size=0.2, random_state=42)

        self.model.fit(X_train, y_train)
        predictions = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)
        print(f"Model trained with accuracy: {accuracy * 100:.2f}%")

    def append_new_data(self, new_data: pd.DataFrame):
        """
        Appends new data to the historical data and retrains the model.
        """
        self.historical_data = pd.concat([self.historical_data, new_data], ignore_index=True)
        self.train_entry_exit_model(self.historical_data)

    def predict_entry_exit(self, current_data: pd.DataFrame):
        """
        Predicts whether to enter or exit a trade based on the current market data.
        """
        if self.model:
            features = current_data[['open', 'high', 'low', 'close', 'volume']]
            scaler = StandardScaler()
            features_scaled = scaler.fit_transform(features)
            return self.model.predict(features_scaled), self.model.predict_proba(features_scaled)
        else:
            raise ValueError("Model not trained. Call train_entry_exit_model first.")

    def analyze_and_trade(self, current_data: pd.DataFrame):
        """
        Analyzes the current market data and returns a trade decision only if the probability is above the threshold.
        """
        if not self.model:
            raise ValueError("Model not trained. Call train_entry_exit_model first.")

        # Predict entry/exit and probabilities
        predictions, probabilities = self.predict_entry_exit(current_data)

        # Get the probability of the predicted class
        predicted_class = predictions[-1]
        predicted_probability = probabilities[-1][predicted_class]

        self.append_new_data(current_data)
        # Only take the trade if the probability exceeds the threshold
        if predicted_probability >= self.probability_threshold:
            if predicted_class == 1:  # Buy signal
                current_trade = {
                    'cp': current_data['close'].iloc[-1],  # Entry price
                    'sl': current_data['close'].iloc[-1] * 0.99,  # Stop loss (1% below entry)
                    'pl': current_data['close'].iloc[-1] * 1.01,  # Take profit (1% above entry)
                    'trade_type': 'buy',
                    'timestamp': current_data['timestamp'].iloc[-1],
                    'probability': predicted_probability,  # Include probability in the trade details
                }
            else:  # Sell signal
                current_trade = {
                    'cp': current_data['close'].iloc[-1],  # Entry price
                    'sl': current_data['close'].iloc[-1] * 1.01,  # Stop loss (1% above entry)
                    'pl': current_data['close'].iloc[-1] * 0.99,  # Take profit (1% below entry)
                    'trade_type': 'sell',
                    'timestamp': current_data['timestamp'].iloc[-1],
                    'probability': predicted_probability,  # Include probability in the trade details
                }
            return current_trade
        else:
            print(f"No trade taken. Probability ({predicted_probability:.2f}) is below the threshold ({self.probability_threshold}).")
            return None