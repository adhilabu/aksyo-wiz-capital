# Telegram Trading Signal Listener

Automated trading system that monitors Telegram channels for trading signals and executes trades via Capital.com API.

## Features

- **Real-time Signal Monitoring**: Uses Telethon userbot to listen to Telegram channels
- **Intelligent Parsing**: Extracts trading signals (instrument, direction, entry, stops, targets)
- **AI Analysis**: Integrates with OpenAI to validate and analyze signals
- **Automated Trading**: Executes trades via existing Capital.com API integration
- **Comprehensive Logging**: Tracks all signals and decisions in database

## Setup

### 1. Get Telegram API Credentials

1. Go to [my.telegram.org](https://my.telegram.org)
2. Log in with your phone number
3. Click on **API development tools**
4. Create an application (any name)
5. Copy your `api_id` and `api_hash`

### 2. Configure Environment Variables

Edit `.env` file and uncomment/set these values:

```bash
# Telegram Userbot Settings
TELEGRAM_API_ID=your_api_id_here
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_PHONE_NUMBER=+1234567890
TELEGRAM_SOURCE_CHANNEL=channel_username

# Trading Signal Settings
MIN_CONFIDENCE_STARS=3
ENABLE_AUTO_TRADING=False  # Set to True to enable actual trading
MAX_POSITION_SIZE=1000
RISK_PER_TRADE_DOLLARS=50.0  # Dollar amount to risk per trade (recommended: 40-60)
```

### Position Sizing Explained

The system calculates position size to risk a **fixed dollar amount** per trade:

- **Formula**: `Position Size = Risk Amount / |Entry Price - Stop Loss|`
- **Example**: If risking $50 and stop is 100 points away, position size = 50/100 = 0.5 lots
- **Benefit**: Consistent risk across all trades, regardless of instrument or stop distance

**Configuration**:
- Set `RISK_PER_TRADE_DOLLARS` to your desired risk (recommended: $40-60)
- The system automatically calculates the appropriate position size for each signal
- `MAX_POSITION_SIZE` acts as a safety limit

**Note**: `OPENAI_API_KEY` is already configured in your `.env`

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run Database Migration

```bash
python migrations/add_telegram_signals_table.py
```

### 5. Test Signal Parser

```bash
python test_signal_parser.py
```

This will test the parser with messages from `sample_msg.txt`.

### 6. Start the Listener

```bash
./run_telegram_listener.sh
```

Or directly with Python:

```bash
python run_telegram_listener.py
```

**First Run**: You'll be prompted to enter an OTP code sent to your Telegram app. This authenticates your session and creates a session file. You won't need to do this again.

## How It Works

1. **Listen**: Telethon monitors the configured Telegram channel
2. **Parse**: Each message is analyzed for trading signal patterns
3. **Filter**: Only signals with confidence ≥ 3 stars are processed
4. **Analyze**: OpenAI evaluates the signal quality and risk/reward
5. **Execute**: If approved and `ENABLE_AUTO_TRADING=True`, execute the trade
6. **Log**: All signals and decisions are logged to `telegram_signals` table
7. **Notify**: Send notification via Telegram bot about the action taken

## Signal Format

The parser expects messages in this format:

```
📈 [Buy Limit] INSTRUMENT - Bullish - We look to Buy at PRICE

Confidence: ⭐⭐⭐
⛔ Stop: STOP_PRICE
🎯 Target 1: TARGET1
🎯 Target 2: TARGET2
⌛ Expires: YYYY-MM-DD HH:MM (GMT+00:00)
```

## Supported Instruments

See `app/telegram/instrument_mapper.py` for the full list of supported instruments:

- **Indices**: US100, US30, J225, CHINA50, etc.
- **Forex**: EURUSD, USDJPY, GBPUSD, etc.
- **Commodities**: GOLD, SILVER, OIL_CRUDE, etc.
- **Crypto**: BTCUSD, ETHUSD

## Database Schema

Signals are stored in the `telegram_signals` table:

| Column | Type | Description |
|--------|------|-------------|
| instrument | VARCHAR | Market identifier |
| direction | VARCHAR | BUY or SELL |
| entry_price | DECIMAL | Entry price from signal |
| stop_loss | DECIMAL | Stop loss level |
| target_1 | DECIMAL | First target |
| target_2 | DECIMAL | Second target |
| confidence | INT | Number of stars (1-5) |
| expires_at | TIMESTAMP | Signal expiration |
| openai_analysis | JSONB | OpenAI analysis results |
| executed | BOOLEAN | Whether trade was executed |
| deal_reference | VARCHAR | Capital.com deal reference |
| status | VARCHAR | Processing status |
| raw_message | TEXT | Original Telegram message |

## Safety Features

- **Manual Approval Mode**: Set `ENABLE_AUTO_TRADING=False` to log signals without executing
- **Confidence Filtering**: Only processes signals with ≥3 stars
- **OpenAI Validation**: AI reviews each signal before execution
- **Position Limits**: Configurable max position size
- **Comprehensive Logging**: All signals tracked in database

## Monitoring

View logs:

```bash
tail -f logs/telegram_listener.log
```

Check database:

```bash
psql $DATABASE_URL -c "SELECT * FROM telegram_signals ORDER BY received_at DESC LIMIT 10;"
```

## Troubleshooting

### Session Issues

If you get authentication errors, delete the session file and restart:

```bash
rm telegram_session.session
./run_telegram_listener.sh
```

### OpenAI Errors

Verify your API key:

```bash
echo $OPENAI_API_KEY
```

### Channel Not Found

Make sure you've joined the channel and use the exact username (without @):

```bash
TELEGRAM_SOURCE_CHANNEL=channelname  # Not @channelname
```

## Architecture

```
Telegram Channel
    ↓
telegram_listener.py (Telethon)
    ↓
signal_parser.py (Regex extraction)
    ↓
signal_processor.py (OpenAI + Capital API)
    ↓
Database + Notifications
```

## Files

- `app/telegram/telegram_listener.py` - Main Telethon listener
- `app/telegram/signal_parser.py` - Message parsing logic
- `app/telegram/signal_processor.py` - OpenAI integration & trade execution
- `app/telegram/instrument_mapper.py` - Instrument name mapping
- `run_telegram_listener.py` - Entry point
- `test_signal_parser.py` - Test suite
- `migrations/add_telegram_signals_table.py` - Database schema

## Next Steps

1. Get Telegram API credentials from my.telegram.org
2. Update `.env` with your credentials and channel name
3. Run migration to create database table
4. Test with `ENABLE_AUTO_TRADING=False` first
5. Monitor logs and database to verify signals are being parsed
6. Once confident, enable auto-trading if desired
