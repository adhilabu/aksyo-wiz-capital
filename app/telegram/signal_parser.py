"""
Trading signal data models and parsing logic.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import re
import logging
import json
import os
from app.capital.schemas import CapitalOrderType

logger = logging.getLogger(__name__)


@dataclass
class TradingSignal:
    """Structured representation of a trading signal from Telegram."""
    instrument: str  # e.g., "US100", "CHINA50"
    direction: str   # "BUY" or "SELL"
    order_type: CapitalOrderType
    entry_price: float
    stop_loss: float
    target_1: float
    target_2: float
    confidence: int  # number of stars (1-5)
    expires_at: datetime
    raw_message: str
    timestamp: datetime
    
    def is_valid(self) -> bool:
        """Check if signal has all required fields and meets minimum standards."""
        return (
            self.confidence >= 3 and
            self.entry_price > 0 and
            self.stop_loss > 0 and
            self.target_1 > 0 and
            self.target_2 > 0 and
            self.expires_at > datetime.now()
        )
    
    def risk_reward_ratio(self) -> float:
        """Calculate risk/reward ratio to target 1."""
        if self.direction == "BUY":
            risk = abs(self.entry_price - self.stop_loss)
            reward = abs(self.target_1 - self.entry_price)
        else:  # SELL
            risk = abs(self.stop_loss - self.entry_price)
            reward = abs(self.entry_price - self.target_1)
        
        return reward / risk if risk > 0 else 0.0


class SignalParser:
    """Parser for trading signals from Telegram messages."""
    
    # Regex patterns for Capital.com Telegram format
    # Format 1: 📈 [Buy Limit] XAU/USD - Bullish - We look to Buy at 4946
    # Format 2: 📈 [Live Trade] US100 - Bullish - We look to Buy at 25160
    INSTRUMENT_PATTERN = r'\[(?:Buy|Sell|Live Trade)[^\]]*\]\s+([A-Z0-9/]+)'  # Matches XAU/USD, EUR/USD, US100, etc.
    
    # Direction patterns - look for "Buy" or "Sell" in the header
    BUY_PATTERN = r'(?:📈|📊).*?\[(?:Buy|BUY|Live Trade)[^\]]*\]|We look to (?:Buy|BUY)'
    SELL_PATTERN = r'(?:📉|📊).*?\[(?:Sell|SELL|Live Trade)[^\]]*\]|We look to (?:Sell|SELL)'
    
    # Entry price: "We look to Buy at 4946" or "We look to Sell at 1.1870"
    ENTRY_PRICE_PATTERN = r'(?:Buy|Sell|BUY|SELL)\s+at\s+([\d,]+\.?\d*)'
    
    # Confidence: "Confidence: ⭐⭐⭐⭐" or "Confidence: ⭐⭐⭐"
    CONFIDENCE_PATTERN = r'Confidence:\s*(⭐+)'
    
    # Stop loss: "⛔ Stop: 4796" or "Stop: 1.184"
    STOP_PATTERN = r'⛔?\s*Stop:\s*([\d,]+\.?\d*)'
    
    # Targets: "🎯 Target 1: 5348" and "🎯 Target 2: 5376"
    TARGET_1_PATTERN = r'🎯?\s*Target\s+1:\s*([\d,]+\.?\d*)'
    TARGET_2_PATTERN = r'🎯?\s*Target\s+2:\s*([\d,]+\.?\d*)'
    
    # Expiration: "⌛ Expires: 2026-02-12 06:00 (GMT+00:00)"
    EXPIRES_PATTERN = r'⌛?\s*Expires?:\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})'
    
    @staticmethod
    def parse(message_text: str, message_timestamp: datetime = None) -> Optional[TradingSignal]:
        """
        Parse a Telegram message into a TradingSignal.
        
        Args:
            message_text: The raw message text from Telegram
            message_timestamp: When the message was received
            
        Returns:
            TradingSignal if parsing successful and confidence >= 3, None otherwise
        """
        if message_timestamp is None:
            message_timestamp = datetime.now()
        
        try:
            # Extract instrument
            instrument_match = re.search(SignalParser.INSTRUMENT_PATTERN, message_text)
            if not instrument_match:
                logger.debug("No instrument found in message")
                raise ValueError("Missing instrument")
            instrument = instrument_match.group(1)
            
            # Determine order type
            order_type = CapitalOrderType.MARKET
            if "Limit" in message_text and ("Buy Limit" in message_text or "Sell Limit" in message_text):
                order_type = CapitalOrderType.LIMIT
            elif "Limit" in message_text:
                 # Fallback/Safety: If "Limit" is mentioned but not in standard format, 
                 # force OpenAI fallback to understand context
                 raise ValueError("Ambiguous Limit order detected - triggering OpenAI fallback")
            
            # Extract direction
            direction = SignalParser._parse_direction(message_text)
            if not direction:
                logger.debug("No direction found in message")
                raise ValueError("Missing direction")
            
            # Extract entry price
            entry_match = re.search(SignalParser.ENTRY_PRICE_PATTERN, message_text)
            if not entry_match:
                logger.debug("No entry price found in message")
                raise ValueError("Missing entry price")
            entry_price = float(entry_match.group(1).replace(',', ''))  # Remove commas from numbers
            
            # Extract confidence (count stars)
            confidence_match = re.search(SignalParser.CONFIDENCE_PATTERN, message_text)
            if not confidence_match:
                logger.debug("No confidence found in message")
                raise ValueError("Missing confidence")
            confidence = len(confidence_match.group(1))
            
            # Filter out low confidence signals early
            if confidence < 3:
                logger.info(f"Signal confidence {confidence} < 3, skipping")
                return None
            
            # Extract stop loss
            stop_match = re.search(SignalParser.STOP_PATTERN, message_text)
            if not stop_match:
                logger.debug("No stop loss found in message")
                raise ValueError("Missing stop loss")
            stop_loss = float(stop_match.group(1).replace(',', ''))
            
            # Extract targets
            target_1_match = re.search(SignalParser.TARGET_1_PATTERN, message_text)
            target_2_match = re.search(SignalParser.TARGET_2_PATTERN, message_text)
            if not target_1_match or not target_2_match:
                logger.debug("Missing target(s) in message")
                raise ValueError("Missing targets")
            target_1 = float(target_1_match.group(1).replace(',', ''))
            target_2 = float(target_2_match.group(1).replace(',', ''))
            
            # Extract expiration
            expires_match = re.search(SignalParser.EXPIRES_PATTERN, message_text)
            if not expires_match:
                logger.debug("No expiration found in message")
                raise ValueError("Missing expiration")
            expires_at = SignalParser._parse_expiration(expires_match.group(1))
            
            signal = TradingSignal(
                instrument=instrument,
                direction=direction,
                order_type=order_type,
                entry_price=entry_price,
                stop_loss=stop_loss,
                target_1=target_1,
                target_2=target_2,
                confidence=confidence,
                expires_at=expires_at,
                raw_message=message_text,
                timestamp=message_timestamp
            )
            
            if signal.is_valid():
                logger.info(f"Successfully parsed signal: {instrument} {direction} @ {entry_price}, confidence={confidence}")
                return signal
            else:
                logger.warning(f"Parsed signal failed validation: {signal}")
                return None
                
        except Exception as e:
            logger.warning(f"Regex parsing failed: {e}. Attempting OpenAI fallback...")
            
            # Try OpenAI-based extraction as fallback
            try:
                signal = SignalParser._parse_with_openai(message_text, message_timestamp)
                if signal and signal.is_valid():
                    logger.info(f"Successfully parsed signal with OpenAI: {signal.instrument} {signal.direction} @ {signal.entry_price}")
                    return signal
                else:
                    logger.debug("OpenAI parsing failed or returned invalid signal")
                    return None
            except Exception as openai_error:
                logger.error(f"OpenAI fallback parsing failed: {openai_error}", exc_info=True)
                return None
    
    @staticmethod
    def _parse_with_openai(message_text: str, message_timestamp: datetime) -> Optional[TradingSignal]:
        """
        Use OpenAI to extract trading signal from message when regex fails.
        
        Args:
            message_text: The raw message text
            message_timestamp: When the message was received
            
        Returns:
            TradingSignal if extraction successful, None otherwise
        """
        try:
            from openai import OpenAI
            
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            prompt = f"""Extract the trading signal information from this message. Return ONLY a valid JSON object with these exact fields:

{{
  "instrument": "string (e.g., US100, GOLD, EURUSD)",
  "direction": "BUY or SELL",
  "entry_price": number,
  "stop_loss": number,
  "target_1": number,
  "target_2": number,
  "confidence": number (1-5, count the stars),
  "order_type": "MARKET or LIMIT",
  "expires_at": "YYYY-MM-DD HH:MM format"
}}

Message:
{message_text}

Rules:
- Extract the instrument name (e.g., US100, XAU/USD, GOLD)
- Direction is BUY or SELL based on the message
- Determine Order Type (MARKET or LIMIT) based on the context (e.g. "Buy Limit", "Sell Limit", or "Limit at"). Default to MARKET.
- All prices must be numbers
- Confidence is the number of stars (⭐)
- If any field is missing, return null for that field
- Return ONLY the JSON, no explanation"""

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a trading signal parser. Extract structured data from trading messages. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=500
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Remove markdown code blocks if present
            if result_text.startswith("```"):
                result_text = result_text.split("```")[1]
                if result_text.startswith("json"):
                    result_text = result_text[4:]
                result_text = result_text.strip()
            
            data = json.loads(result_text)
            
            # Validate all required fields are present
            required_fields = ["instrument", "direction", "entry_price", "stop_loss", "target_1", "target_2", "confidence", "expires_at"]
            # order_type is optional in JSON extract, default to MARKET if missing

            if not all(data.get(field) is not None for field in required_fields):
                logger.warning(f"OpenAI extraction missing required fields: {data}")
                return None
            
            # Parse expiration
            expires_at = SignalParser._parse_expiration(data["expires_at"])
            
            signal = TradingSignal(
                instrument=str(data["instrument"]).upper().replace("/", ""),
                direction=str(data["direction"]).upper(),
                order_type=CapitalOrderType[data.get("order_type", "MARKET").upper()],
                entry_price=float(data["entry_price"]),
                stop_loss=float(data["stop_loss"]),
                target_1=float(data["target_1"]),
                target_2=float(data["target_2"]),
                confidence=int(data["confidence"]),
                expires_at=expires_at,
                raw_message=message_text,
                timestamp=message_timestamp
            )
            
            logger.info(f"OpenAI extracted: {signal.instrument} {signal.direction} @ {signal.entry_price}, confidence={signal.confidence}")
            return signal
            
        except Exception as e:
            logger.error(f"OpenAI parsing error: {e}", exc_info=True)
            return None
    
    @staticmethod
    def _parse_direction(message_text: str) -> Optional[str]:
        """Extract trade direction from message."""
        if re.search(SignalParser.BUY_PATTERN, message_text, re.IGNORECASE):
            return "BUY"
        elif re.search(SignalParser.SELL_PATTERN, message_text, re.IGNORECASE):
            return "SELL"
        return None
    
    @staticmethod
    def _parse_expiration(expires_str: str) -> datetime:
        """
        Parse expiration datetime string.
        
        Examples:
            "2026-02-11 12:00" -> datetime object
        """
        try:
            # Try parsing with standard format
            return datetime.strptime(expires_str.strip(), "%Y-%m-%d %H:%M")
        except ValueError:
            # Fallback: assume expires in 24 hours
            from datetime import timedelta
            logger.warning(f"Could not parse expiration '{expires_str}', defaulting to 24h from now")
            return datetime.now() + timedelta(hours=24)
