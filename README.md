# Aksyo-Wiz (Capital.com Integration)

This project implements trading strategies for the Indian stock market, utilizing the Capital.com API for market data and order execution.

This version has been refactored from an earlier implementation that used the Upstox API and further enhanced to include more comprehensive order management and analysis logic tailored for Capital.com.

## Features

*   Connects to Capital.com API for real-time market data via WebSocket.
*   Fetches historical market data from Capital.com, transforming it into pandas DataFrames.
*   Places and manages trades (positions and working orders) through the Capital.com API:
    *   Places MARKET, LIMIT, and STOP orders.
    *   Retrieves open positions and working orders.
    *   Modifies Stop Loss (SL) and Take Profit (TP) levels for open positions.
    *   Cancels specific working orders.
    *   Closes specific open positions.
    *   Closes all open positions (e.g., for End-of-Day square-off).
    *   Confirms order/position status using deal references.
*   Includes enhanced trading strategy analysis components in `analyse.py` adapted for Capital.com data:
    *   Processes live market feed data (via Pulsar).
    *   Calculates indicators (e.g., RSI, ADX, MFI - *Note: TA-Lib dependency issue persists*).
    *   Implements strategy logic (e.g., reversal breakout) using Capital.com data structures and order types.
    *   Manages trade lifecycle including entry, SL/TP adjustments, and exit based on strategy rules and market data.
*   Uses Pulsar for message queuing between data feed and analysis components.
*   Uses Redis for caching.
*   Includes a basic FastAPI application structure (`app/main.py`).

## Setup

1.  **Clone the repository:**
    ```bash
    # If you haven't already cloned:
    # git clone <repository_url>
    cd /home/ubuntu/aksyo-wiz # Navigate to the existing directory
    ```

2.  **Install/Update Dependencies:**
    Ensure you have Python 3 installed. It is recommended to use a virtual environment.
    ```bash
    # If using a venv:
    # python3 -m venv venv
    # source venv/bin/activate # On Windows use `venv\Scripts\activate`
    pip install -r requirements.txt
    # Ensure asyncpg and redis are installed if not already:
    pip install asyncpg redis
    ```
    *Note: The `requirements.txt` file includes dependencies for both the original project and the Capital.com integration. The TA-Lib dependency might cause issues on non-Windows platforms or if the C library is not installed. If you encounter issues with `ta-lib`, you might need to install the underlying TA-Lib C library first or remove/comment out the `ta-lib` line in `requirements.txt` if you don't need the specific TA functions it provides.*

3.  **Configure Environment Variables:**
    Create a `.env` file in the project root directory (`/home/ubuntu/aksyo-wiz/.env`) or export the following environment variables:

    ```dotenv
    # Capital.com API Credentials (Use Demo or Live credentials)
    CAPITAL_API_KEY="your_capital_api_key"
    CAPITAL_API_IDENTIFIER="your_capital_login_identifier"
    CAPITAL_API_PASSWORD="your_capital_password"
    CAPITAL_API_BASE_URL="https://demo-api-capital.backend-capital.com" # Use https://api-capital.backend-capital.com for Live
    CAPITAL_API_STREAMING_URL="wss://demo-streaming-capital.backend-capital.com/connect" # Use wss://api-streaming-capital.backend-capital.com/connect for Live

    # Pulsar Configuration
    PULSAR_URL="pulsar://localhost:6650" # Adjust if your Pulsar instance is elsewhere
    PULSAR_TOPIC="capital-topic" # Base topic name

    # Application Settings
    STRATEGY_TYPE="DEFAULT" # Identifier for which set of instruments/logic to run (used in instruments.py and feed.py)
    CONSUMER_THREAD_COUNT="6" # Number of Pulsar consumer instances
    ENABLE_CAPITAL_CALL="True" # Set to "False" to disable actual API calls for testing
    LOGGING_LEVEL="INFO" # e.g., DEBUG, INFO, WARNING, ERROR

    # Database Configuration (Required for trade logging in analyse.py)
    DATABASE_URL="postgresql://user:password@host:port/dbname" # Replace with your actual PostgreSQL connection string
    # Example: DATABASE_URL="postgresql://aksyo_user:aksyo_pass@localhost:5432/aksyo_db"

    # Redis Configuration
    REDIS_HOST="localhost"
    REDIS_PORT="6379"
    REDIS_DB="0"
    ```

4.  **Database Setup (Required for `analyse.py`):**
    The enhanced `analyse.py` relies on a PostgreSQL database for logging trades and managing state. Ensure you have PostgreSQL running and update `DATABASE_URL` in your environment.
    *   **Schema:** You need the `order_details` table. The `log_trade_to_db` method in `app/database/db.py` now includes `deal_id` and `deal_reference` columns. You may need to adapt the table schema accordingly. Example `ALTER TABLE` commands (adjust as needed):
        ```sql
        ALTER TABLE order_details ADD COLUMN deal_id VARCHAR(255);
        ALTER TABLE order_details ADD COLUMN deal_reference VARCHAR(255);
        -- Ensure other columns match the log_trade_to_db function parameters
        ```
    *   The `historical_data` and `support_resistance_levels` tables are also used if fetching/storing historical data and pivots via `analyse.py`.

## Running the Application

The main application entry point is `app/main.py`, which runs a FastAPI server and starts the WebSocket listener (producer) and Pulsar consumers.

```bash
# Ensure environment variables are set or .env file is present
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

This will:
*   Start the FastAPI server.
*   Initiate the Capital.com WebSocket connection (`capital_websocket_listener`).
*   Subscribe to market data based on the `STRATEGY_TYPE`.
*   Publish received market data to the Pulsar topic.
*   Start Pulsar consumers (`Analyse` instances) to process the data using the logic in `app/analyse/analyse.py`.

## Project Structure Highlights

*   `app/capital/`:
    *   `capital_com.py`: Core API client (auth, HTTP requests).
    *   `actions.py`: High-level functions wrapping `capital_com.py` for data fetching, order placement, position management, and data transformation (e.g., history to DataFrame).
    *   `schemas.py`: Pydantic models and enums for Capital.com API.
    *   `instruments.py`: Logic for fetching/filtering Capital.com EPICs.
    *   `feed.py`: WebSocket listener implementation.
*   `app/pulsar/`: Pulsar producer and consumer logic.
*   `app/analyse/`:
    *   `analyse.py`: Contains `StockIndicatorCalculator` and `Analyse` classes implementing trading strategy logic, using `CapitalAPI` for interactions and processing live data from Pulsar.
    *   `schemas.py`: Data structures used within the analysis module (e.g., `Trade`).
*   `app/database/`: Database interaction logic (`db.py`).
*   `app/redis/`: Redis caching logic (`redis.py`).
*   `app/shared/config/`: Settings management.
*   `app/main.py`: FastAPI application entry point.
*   `requirements.txt`: Project dependencies.
*   `README.md`: This file.
*   `test_capital_integration.py`: Integration test script for `actions.py` and `capital_com.py`.

## Important Notes

*   **Credentials & Environment:** Ensure all required environment variables (API keys, DB URL, Pulsar URL, Redis) are correctly set before running.
*   **Database Schema:** The `order_details` table schema must include `deal_id` and `deal_reference` columns to work with the updated `log_trade_to_db` function.
*   **Error Handling:** The implementation includes error handling, but review and enhance it based on your specific needs and risk tolerance.
*   **Testing:** The `test_capital_integration.py` script provides tests for `actions.py` functionality. Ensure environment variables are available to the test execution environment.
*   **TA-Lib:** The TA-Lib dependency issue noted previously still exists and may affect indicator calculations if not resolved in your environment.
*   **Instrument Mapping:** Ensure `instruments.py` correctly maps your strategy needs to Capital.com EPICs.
