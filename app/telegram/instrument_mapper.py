"""
Instrument name mapping between Telegram signals and Capital.com epics.
"""
import logging
from typing import Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=".env", override=True)
class InstrumentMapper:
    """Maps Telegram instrument names to Capital.com epics."""
    
    # Mapping from Telegram signal names to Capital.com epics
    INSTRUMENT_MAP = {
        # Indices
        "US100": "US100",           # NASDAQ 100
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
        
        Args:
            telegram_name: Instrument name from Telegram signal
            
        Returns:
            Capital.com epic code, or None if not found
        """
        # Normalize input
        normalized = telegram_name.upper().strip()
        
        epic = cls.INSTRUMENT_MAP.get(normalized)
        if epic:
            logger.debug(f"Mapped {telegram_name} -> {epic}")
            return epic
        else:
            logger.warning(f"Unknown instrument: {telegram_name}")
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
