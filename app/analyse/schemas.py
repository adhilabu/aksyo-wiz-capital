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
    deal_id: str
    metadata_json: dict
    order_ids: list
    order_status: bool

class TradeStatus(str, Enum):
    PROFIT = "PROFIT"
    LOSS = "LOSS"
    OPEN = "OPEN"
    REJECTED = "REJECTED"

class CapitalMarketDetails:
    def __init__(self, epic: str, min_step_distance: float, 
                 min_step_distance_unit: str, min_deal_size: float,
                 min_deal_size_unit: str, max_deal_size: float,
                 max_deal_size_unit: str, min_size_increment: float,
                 min_size_increment_unit: str, min_guaranteed_stop_distance: float,
                 min_guaranteed_stop_distance_unit: str, min_stop_or_profit_distance: float,
                 min_stop_or_profit_distance_unit: str, max_stop_or_profit_distance: float,
                 max_stop_or_profit_distance_unit: str, decimal_places: int,
                 margin_factor: float):
        self.epic = epic
        self.min_step_distance = min_step_distance
        self.min_step_distance_unit = min_step_distance_unit
        self.min_deal_size = min_deal_size
        self.min_deal_size_unit = min_deal_size_unit
        self.max_deal_size = max_deal_size
        self.max_deal_size_unit = max_deal_size_unit
        self.min_size_increment = min_size_increment
        self.min_size_increment_unit = min_size_increment_unit
        self.min_guaranteed_stop_distance = min_guaranteed_stop_distance
        self.min_guaranteed_stop_distance_unit = min_guaranteed_stop_distance_unit
        self.min_stop_or_profit_distance = min_stop_or_profit_distance
        self.min_stop_or_profit_distance_unit = min_stop_or_profit_distance_unit
        self.max_stop_or_profit_distance = max_stop_or_profit_distance
        self.max_stop_or_profit_distance_unit = max_stop_or_profit_distance_unit
        self.decimal_places = decimal_places
        self.margin_factor = margin_factor