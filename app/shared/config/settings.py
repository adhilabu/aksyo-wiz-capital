from pydantic_settings import BaseSettings
from pydantic import Field, field_validator, model_validator
from typing import Optional, List
from dotenv import load_dotenv

from app.analyse.schemas import TradeAnalysisType

# Load environment variables from .env file
load_dotenv(dotenv_path=".env", override=True)

class Settings(BaseSettings):
    # Environment settings
    ENVIRONMENT: str = "development"  # development, sandbox, production
    TELEGRAM_CHAT_ID: Optional[str] = None
    TELEGRAM_TOKEN: Optional[str] = None
    TELEGRAM_NOTIFICATION: bool = False
    # Capital.com API settings
    CAPITAL_API_KEY: Optional[str] = None
    CAPITAL_API_IDENTIFIER: Optional[str] = None
    CAPITAL_API_PASSWORD: Optional[str] = None
    CAPITAL_API_BASE_URL: str = "https://demo-api-capital.backend-capital.com"
    CAPITAL_API_STREAMING_URL: str = "wss://api-streaming-capital.backend-capital.com/connect"
    ENABLE_CAPITAL_CALL: bool = False
    
    # Database settings
    DATABASE_URL: str = "postgresql://postgres:postgres@db:5432/algo_trading"
    
    # Redis settings
    REDIS_URL: str = "redis://redis:6379/0"
    
    # Pulsar settings
    PULSAR_URL: str = "pulsar://pulsar:6650"
    
    # API settingsa
    API_PREFIX: str = "/api/v1"
    
    # Security settings
    SECRET_KEY: str = "your-secret-key-for-jwt-tokens"
    
    # Trading instruments
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    HISTORY_DATA_PERIOD: int = 10  # 10 DAYS
    
    # UI_INSTRUMENTS_LIST: List[str] = Field(default_factory=lambda: ["ETHUSD,EURUSD","USDJPY","J225","US30","BTCUSD"])
    UI_INSTRUMENTS: str = "OIL_CRUDE,US100,GOLD,J225,US30,BTCUSD"
    TRADE_ANALYSIS_TYPE: TradeAnalysisType = TradeAnalysisType.NORMAL
    PULSAR_TOPIC: str = 'capital-topic'
    STOCK_PER_PRICE_LIMIT: int = 1000
    MIGRATIONS_FILE: str = "D:/projects/aksyo-wiz/app/sql/migrations.sql"    
    SL_PERC: float = 0.004  # 1% stop loss
    LOG_TRADE_TO_DB: bool = False
    RISK_REWARD_RATIO: float = 1.2
    THE_NEWS_API_KEY: Optional[str] = None
    THE_NEWS_API_KEY_2: Optional[str] = None
    QTY_MULTIPLIER: float = 1.5


    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # env_nested_delimiter = "__"


CAPITAL_SETTINGS = Settings()