"""
Telegram userbot listener for trading signals.
Uses Telethon to monitor channels and extract trading signals.
"""
import os
import asyncio
import logging
from datetime import datetime
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

from app.telegram.signal_parser import SignalParser
from app.telegram.signal_processor import SignalProcessor
from app.database.db import DBConnection
from dotenv import load_dotenv
from app.notification.telegram import TelegramAPI

logger = logging.getLogger(__name__)
load_dotenv(dotenv_path=".env", override=True)
PULSAR_URL = os.getenv("PULSAR_URL")
PULSAR_TOPIC = os.getenv("PULSAR_TOPIC")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_NOTIFICATION = os.getenv("TELEGRAM_NOTIFICATION")

SPLIT_TYPE = os.getenv("SPLIT_TYPE")
CONSUMER_COUNT = os.getenv("CONSUMER_COUNT")
TRADE_ANALYSIS_TYPE = os.getenv("TRADE_ANALYSIS_TYPE")
CAPITAL_API_BASE_URL = os.getenv("CAPITAL_API_BASE_URL")
UI_INSTRUMENTS = os.getenv("UI_INSTRUMENTS")

message = (
    f"Aksyo - Capital Telegram Listener started with the following configuration:\n"
    f"- Pulsar URL: {PULSAR_URL}\n"
    f"- Pulsar Topic: {PULSAR_TOPIC}\n"
    f"- Split Type: {SPLIT_TYPE}\n"
    f"- Consumer Count: {CONSUMER_COUNT}\n"
    f"- Trade Analysis Type: {TRADE_ANALYSIS_TYPE}\n"
    f"- Capital API Base URL: {CAPITAL_API_BASE_URL}\n"
    f"- Current UI Instruments: {UI_INSTRUMENTS}\n"
)

if TELEGRAM_NOTIFICATION.lower() == "true":
    telegram_api = TelegramAPI(TELEGRAM_TOKEN)
    telegram_api.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)


class TelegramSignalListener:
    """
    Telegram userbot that listens to trading signal channels.
    """
    
    def __init__(self):
        """Initialize the Telegram listener."""
        # Load configuration from environment
        self.api_id = os.getenv("TELEGRAM_API_ID")
        self.api_hash = os.getenv("TELEGRAM_API_HASH")
        self.phone_number = os.getenv("TELEGRAM_PHONE_NUMBER")
        self.target_channel = os.getenv("TELEGRAM_SOURCE_CHANNEL")
        
        # Validate configuration
        if not all([self.api_id, self.api_hash, self.phone_number, self.target_channel]):
            raise ValueError(
                "Missing Telegram configuration. Please set: "
                "TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE_NUMBER, TELEGRAM_SOURCE_CHANNEL"
            )
        
        # Session file path
        self.session_file = os.path.join(
            os.path.dirname(__file__),
            "../../telegram_session.session"
        )
        
        # Create Telethon client
        self.client = TelegramClient(
            self.session_file,
            int(self.api_id),
            self.api_hash
        )
        
        # Initialize signal processor
        self.db_conn = DBConnection()
        self.signal_processor = SignalProcessor(self.db_conn)
        
        # Stats
        self.messages_received = 0
        self.signals_parsed = 0
        self.signals_processed = 0
        
        logger.info(f"TelegramSignalListener initialized for channel: {self.target_channel}")
    
    async def start(self):
        """Start the Telegram listener."""
        logger.info("Starting Telegram listener...")
        
        # Initialize database connection pool
        logger.info("Initializing database connection...")
        await self.db_conn.init()
        logger.info("Database connection initialized")
        
        # Start the client
        await self.client.start(phone=self.phone_number)
        
        # Verify we're connected
        me = await self.client.get_me()
        logger.info(f"Connected as: {me.username or me.first_name} ({me.phone})")
        
        # Register event handler
        @self.client.on(events.NewMessage(chats=self.target_channel))
        async def message_handler(event):
            await self._handle_message(event)
        
        logger.info(f"Listening for messages in channel: {self.target_channel}")
        logger.info("Press Ctrl+C to stop...")
        
        # Run until disconnected
        await self.client.run_until_disconnected()
    
    async def _handle_message(self, event):
        """
        Handle incoming message from monitored channel.
        
        Args:
            event: Telethon NewMessage event
        """
        self.messages_received += 1
        
        try:
            message_text = event.message.message
            message_time = event.message.date
            
            logger.info(f"Received message #{self.messages_received} from {self.target_channel}")
            logger.info(f"Message preview: {message_text[:100]}...")
            
            # Parse the message
            signal = SignalParser.parse(message_text, message_time)
            print(signal)
            if signal:
                self.signals_parsed += 1
                logger.info(
                    f"Parsed signal #{self.signals_parsed}: "
                    f"{signal.instrument} {signal.direction} @ {signal.entry_price}, "
                    f"confidence={signal.confidence}"
                )
                
                # Process the signal
                result = await self.signal_processor.process_signal(signal)
                self.signals_processed += 1
                 
                logger.info(
                    f"Signal processed #{self.signals_processed}: "
                    f"status={result['status']}, reason={result.get('reason', 'N/A')}"
                )
            else:
                logger.info("Message did not contain a valid trading signal or confidence < 3")
        
        except FloodWaitError as e:
            # Telegram is rate limiting - must wait as instructed to avoid ban
            wait_seconds = e.seconds
            logger.warning(
                f"⚠️  FloodWaitError: Telegram requires a wait of {wait_seconds} seconds. "
                f"This is normal if processing many messages. Pausing to comply with rate limits..."
            )
            await asyncio.sleep(wait_seconds)
            logger.info(f"✅ Rate limit wait complete ({wait_seconds}s). Resuming normal operation.")
            
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)
    
    def get_stats(self) -> dict:
        """Get listener statistics."""
        return {
            "messages_received": self.messages_received,
            "signals_parsed": self.signals_parsed,
            "signals_processed": self.signals_processed,
            "parse_rate": (
                self.signals_parsed / self.messages_received * 100
                if self.messages_received > 0 else 0
            )
        }
    
    async def stop(self):
        """Stop the listener gracefully."""
        logger.info("Stopping Telegram listener...")
        stats = self.get_stats()
        logger.info(f"Final stats: {stats}")
        
        # Close database connection
        if self.db_conn and self.db_conn.pool:
            await self.db_conn.close()
            logger.info("Database connection closed")
        
        await self.client.disconnect()
        logger.info("Telegram listener stopped")


async def main():
    """Main entry point for the Telegram listener."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/telegram_listener.log'),
            logging.StreamHandler()
        ]
    )
    
    # Create and start listener
    listener = TelegramSignalListener()
    
    try:
        await listener.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        await listener.stop()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        await listener.stop()
        raise


if __name__ == "__main__":
    asyncio.run(main())
