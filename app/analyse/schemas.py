from enum import Enum
from typing import Optional
from pydantic import BaseModel
from dataclasses import dataclass
from app.capital.schemas import CapitalTransactionType
from datetime import datetime
class StockType(str, Enum):
    EQUITY = "equity"
    FUTURE = "future"
    OPTION = "option"
    INDEX = "index"

class IndicatorValues(BaseModel):
    rsi: Optional[float] = 0
    adx: Optional[float] = 0
    mfi: Optional[float] = 0
    vwap: Optional[float] = 0
    upperband: Optional[float] = 0
    middleband: Optional[float] = 0
    lowerband: Optional[float] = 0
    stock_type: StockType = StockType.EQUITY 

class TradeAnalysisType(str, Enum):
    NORMAL = "normal"

@dataclass
class Trade:
    id: int
    stock: str
    entry_ltp: float
    entry_cp: float
    sl: float
    pl: float
    entry_price: float
    entry_time: datetime
    trade_type: CapitalTransactionType
    tag: str
    metadata_json: dict
    order_ids: list
    order_status: bool

class TradeStatus(str, Enum):
    PROFIT = "PROFIT"
    LOSS = "LOSS"
    OPEN = "OPEN"
