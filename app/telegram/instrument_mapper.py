"""
Instrument name mapping between Telegram signals and Capital.com epics.
"""
import logging
from typing import Optional
from dotenv import load_dotenv
import os
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=".env", override=True)
class InstrumentMapper:
    """Maps Telegram instrument names to Capital.com epics."""
    
    # Mapping from Telegram signal names to Capital.com epics
    INSTRUMENT_MAP = {
        # Indices
        "US100": "US100",           # NASDAQ 100
        "ETHUSD": "ETHUSD",         # Ethereum
        "ETH": "ETHUSD",         # Ethereum
        "US30": "US30",             # Dow Jones 30
        "J225": "J225",             # Nikkei 225
        "CHINA50": "CHINA50",       # China A50
        "US500": "US500",           # S&P 500
        "GER40": "GER40",           # DAX 40
        "UK100": "UK100",           # FTSE 100
        "FRA40": "FRA40",           # CAC 40
        "XRP": "XRPUSD",         # Ripple
        "XRPUSD": "XRPUSD",         # Ripple
        
        # Forex
        "EURUSD": "EURUSD",
        "GBPUSD": "GBPUSD",
        "USDJPY": "USDJPY",
        "USDCHF": "USDCHF",
        "AUDUSD": "AUDUSD",
        "USDCAD": "USDCAD",
        "NZDUSD": "NZDUSD",
        "EURJPY": "EURJPY",
        "GBPJPY": "GBPJPY",
        "EURGBP": "EURGBP",
        "AUDJPY": "AUDJPY",
        
        # Commodities
        "GOLD": "GOLD",
        "SILVER": "SILVER",
        "OIL_CRUDE": "OIL_CRUDE",   # WTI Crude Oil
        "OIL": "OIL_CRUDE",
        "BRENT": "BRENT_CRUDE",
        "COPPER": "COPPER",
        "NATURAL_GAS": "NATURAL_GAS",
        "USCOCOA": "USCOCOA",
        
        # Cryptocurrencies
        "BTCUSD": "BTCUSD",
        "ETHUSD": "ETHUSD",
        "BITCOIN": "BTCUSD",
        "ETHEREUM": "ETHUSD",
    }
    
    # Reverse mapping (Capital.com epic -> display name)
    REVERSE_MAP = {v: k for k, v in INSTRUMENT_MAP.items()}
    
    @classmethod
    def to_capital_epic(cls, telegram_name: str) -> Optional[str]:
        """
        Convert Telegram instrument name to Capital.com epic.
        Uses OpenAI fallback to match against holiday.json if static mapping fails.
        
        Args:
            telegram_name: Instrument name from Telegram signal
            
        Returns:
            Capital.com epic code, or None if not found
        """
        # Normalize input
        normalized = telegram_name.upper().strip()
        # Get UI_INSTRUMENTS from env (comma-separated list)
        ui_instruments_str = os.getenv("UI_INSTRUMENTS", "")
        if not ui_instruments_str:
            logger.warning("UI_INSTRUMENTS not set in .env, cannot use OpenAI mapping")
            return None
        
        # Parse UI_INSTRUMENTS into a list
        ui_instruments = [inst.strip() for inst in ui_instruments_str.split(",") if inst.strip()]
        
        if not ui_instruments:
            logger.warning("UI_INSTRUMENTS is empty, cannot use OpenAI mapping")
            return None

        # Try static mapping first
        epic = cls.INSTRUMENT_MAP.get(normalized)
        if epic and epic in ui_instruments:
            logger.debug(f"Mapped {telegram_name} -> {epic}")
            return epic
        
        # Try OpenAI fallback to match against holiday.json
        logger.info(f"Static mapping failed for {telegram_name}, trying OpenAI fallback...")
        epic = cls._map_with_openai(telegram_name)
        if epic:
            logger.info(f"OpenAI mapped {telegram_name} -> {epic}")
            return epic
        
        logger.warning(f"Unknown instrument: {telegram_name}")
        return None
    
    @classmethod
    def _map_with_openai(cls, telegram_name: str) -> Optional[str]:
        """
        Use OpenAI to match instrument name to holiday.json epics.
        Only considers instruments listed in UI_INSTRUMENTS env variable.
        
        Args:
            telegram_name: Instrument name from Telegram
            
        Returns:
            Matched epic from holiday.json, or None
        """
        try:
            import json
            from openai import OpenAI
            
            # Get UI_INSTRUMENTS from env (comma-separated list)
            ui_instruments_str = os.getenv("UI_INSTRUMENTS", "")
            if not ui_instruments_str:
                logger.warning("UI_INSTRUMENTS not set in .env, cannot use OpenAI mapping")
                return None
            
            # Parse UI_INSTRUMENTS into a list
            ui_instruments = [inst.strip() for inst in ui_instruments_str.split(",") if inst.strip()]
            
            if not ui_instruments:
                logger.warning("UI_INSTRUMENTS is empty, cannot use OpenAI mapping")
                return None
            
            # Load holiday.json to verify epics exist
            holiday_path = os.path.join(os.path.dirname(__file__), '../../holiday.json')
            if not os.path.exists(holiday_path):
                logger.warning(f"holiday.json not found at {holiday_path}")
                return None
            
            with open(holiday_path, 'r') as f:
                holiday_data = json.load(f)
            
            # Filter to only UI_INSTRUMENTS that exist in holiday.json
            available_epics = [epic for epic in ui_instruments if epic in holiday_data]
            
            if not available_epics:
                logger.warning(f"None of the UI_INSTRUMENTS exist in holiday.json: {ui_instruments}")
                return None
            
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            prompt = f"""Given the instrument name "{telegram_name}", which of these epics does it match?

Available epics: {', '.join(available_epics)}

Rules:
- Return ONLY the exact epic name from the list above
- Match common aliases (e.g., "NASDAQ" -> "US100", "DOW" -> "US30", "NIKKEI" -> "J225")
- Match crypto symbols (e.g., "BTC" -> "BTCUSD", "ETH" -> "ETHUSD")
- Match forex pairs (e.g., "USD/JPY" -> "USDJPY")
- Match commodities (e.g., "WTI" or "CRUDE" -> "OIL_CRUDE")
- If no match, return "NONE"
- Return ONLY the epic name, no explanation"""

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are an instrument name matcher. Return only the epic name or NONE."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=50
            )
            
            result = response.choices[0].message.content.strip().upper()
            
            # Validate result is in available epics
            if result in available_epics:
                logger.info(f"OpenAI matched {telegram_name} to {result} from UI_INSTRUMENTS")
                return result
            elif result == "NONE":
                return None
            else:
                logger.warning(f"OpenAI returned invalid epic: {result}")
                return None
                
        except Exception as e:
            logger.error(f"OpenAI mapping error: {e}")
            return None
    
    @classmethod
    def from_capital_epic(cls, epic: str) -> str:
        """
        Convert Capital.com epic to display name.
        
        Args:
            epic: Capital.com epic code
            
        Returns:
            Display name (defaults to epic if not found in reverse map)
        """
        return cls.REVERSE_MAP.get(epic, epic)
    
    @classmethod
    def is_valid_instrument(cls, telegram_name: str) -> bool:
        """Check if instrument name is valid/supported."""
        return telegram_name.upper().strip() in cls.INSTRUMENT_MAP
    
    @classmethod
    def get_all_supported_instruments(cls) -> list[str]:
        """Get list of all supported Telegram instrument names."""
        return list(cls.INSTRUMENT_MAP.keys())
