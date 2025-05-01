# Schemas for Capital.com API integration
from enum import Enum
from typing import Optional
from pydantic import BaseModel

# --- Enums based on Capital.com API --- 

class CapitalMarketResolution(str, Enum):
    """Resolution for price history requests."""
    SECOND = "SECOND"
    MINUTE = "MINUTE"
    MINUTE_2 = "MINUTE_2"
    MINUTE_3 = "MINUTE_3"
    MINUTE_5 = "MINUTE_5"
    MINUTE_10 = "MINUTE_10"
    MINUTE_15 = "MINUTE_15"
    MINUTE_30 = "MINUTE_30"
    HOUR = "HOUR"
    HOUR_2 = "HOUR_2"
    HOUR_3 = "HOUR_3"
    HOUR_4 = "HOUR_4"
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"

class CapitalTransactionType(str, Enum):
    """Direction for placing orders/positions."""
    BUY = "BUY"
    SELL = "SELL"

class CapitalOrderType(str, Enum):
    """Order types - Map from aksyo-wiz types to potential Capital.com types or simulation logic."""
    MARKET = "MARKET" # May map directly or be the default for create_position
    LIMIT = "LIMIT"   # May require /workingorders endpoint
    STOP = "STOP"     # May require /workingorders endpoint (mapping SL/SLM)
    # Note: Capital.com might have different concepts (e.g., create_position vs workingorders)

# --- Pydantic Models --- 

class CapitalCredentialsSchema(BaseModel):
    """Schema for Capital.com credentials (if loaded from a file, though client uses env vars)."""
    api_key: Optional[str]
    api_identifier: Optional[str]
    api_password: Optional[str]
    # Add other relevant fields if needed

class BasicPlaceOrderCapital(BaseModel):
    """Internal schema for placing orders, adapted for Capital.com."""
    epic: str # Capital.com uses EPIC for instrument identifier
    quantity: float # Capital.com uses size (float)
    price: Optional[float] = None # Required for LIMIT/STOP orders
    order_type: CapitalOrderType
    transaction_type: CapitalTransactionType
    stop_loss: Optional[float] = None # Maps to stopLevel
    profit_level: Optional[float] = None # Maps to profitLevel
    # Add other fields if needed, e.g., guaranteedStop, trailingStop
    # parent_instrument_key is removed as Capital.com uses epic directly

# --- WebSocket Schemas (Placeholder) ---

class CapitalWebSocketMessage(BaseModel):
    """Represents a parsed message from Capital.com WebSocket."""
    # Define fields based on actual WebSocket message structure
    # e.g., epic, timestamp, bid, ask, etc.
    epic: str
    updateTime: Optional[str] = None # Example field
    bid: Optional[float] = None      # Example field
    ask: Optional[float] = None      # Example field
    # Add other relevant fields based on Capital.com stream format

# Add other necessary schemas as the integration progresses

