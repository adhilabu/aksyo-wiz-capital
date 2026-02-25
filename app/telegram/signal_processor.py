"""
Signal processing and trade execution logic.
Integrates OpenAI analysis with Capital.com trading.
"""
import os
import json
import logging
from typing import Dict, Optional
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv
from app.telegram.signal_parser import TradingSignal
from app.telegram.instrument_mapper import InstrumentMapper
from app.capital.actions import CapitalAPI
from app.capital.schemas import BasicPlaceOrderCapital, CapitalOrderType, CapitalTransactionType
from app.database.db import DBConnection
from app.redis.redis import RedisCache
from app.shared.config.settings import CAPITAL_SETTINGS
from urllib.parse import urlparse

logger = logging.getLogger(__name__)
load_dotenv(dotenv_path=".env", override=True)

class SignalProcessor:
    """
    Processes trading signals from Telegram.
    Analyzes with OpenAI and executes trades via Capital.com API.
    """
    
    def __init__(self, db_conn: DBConnection):
        """
        Initialize signal processor.
        
        Args:
            db_conn: Database connection for logging and trading
        """
        self.db_conn = db_conn
        self.capital_api = CapitalAPI(db_conn)
        
        # OpenAI client
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found in environment")
        self.openai_client = OpenAI(api_key=api_key)
        
        # Configuration
        self.enable_auto_trading = os.getenv("ENABLE_AUTO_TRADING", "False").lower() == "true"
        self.max_position_size = float(os.getenv("MAX_POSITION_SIZE", "1000"))
        
        # Risk per trade in dollars (default $50, range $40-60 recommended)
        self.risk_per_trade_dollars = float(os.getenv("RISK_PER_TRADE_DOLLARS", "50.0"))
        
        # Quantity multiplier (same as analyse.py)
        self.qty_multiplier = float(os.getenv("QTY_MULTIPLIER", "1.0"))
        
        # Load market details from holiday.json
        self.market_details = {}
        self._load_market_details()

        # Initialize Redis Cache
        try:
            redis_url = urlparse(CAPITAL_SETTINGS.REDIS_URL)
            self.redis_cache = RedisCache(
                host=redis_url.hostname,
                port=redis_url.port or 6379,
                db=int(redis_url.path.lstrip('/')) if redis_url.path else 0
            )
            logger.info(f"Redis cache initialized connected to {redis_url.hostname}:{redis_url.port}")
        except Exception as e:
            logger.error(f"Failed to initialize Redis cache: {e}")
            self.redis_cache = None
        
        logger.info(f"SignalProcessor initialized - Auto-trading: {self.enable_auto_trading}, Risk per trade: ${self.risk_per_trade_dollars}, QTY multiplier: {self.qty_multiplier}")
    
    def _load_market_details(self, file_path: str = 'holiday.json') -> None:
        """
        Load market configuration from holiday.json.
        
        Args:
            file_path: Path to the market details JSON file
        """
        if not os.path.exists(file_path):
            logger.warning(f"Market details file {file_path} not found, using defaults")
            return
        
        try:
            with open(file_path, 'r') as f:
                self.market_details = json.load(f)
            logger.info(f"Loaded market details for {len(self.market_details)} instruments: {list(self.market_details.keys())}")
        except Exception as e:
            logger.error(f"Failed to load market details from {file_path}: {e}", exc_info=True)
    
    async def process_signal(self, signal: TradingSignal) -> Dict:
        """
        Process a trading signal end-to-end.
        
        Args:
            signal: Parsed trading signal
            
        Returns:
            Dict with processing results
        """
        result = {
            "signal": signal,
            "timestamp": datetime.now(),
            "status": "pending",
            "reason": None,
            "openai_analysis": None,
            "deal_reference": None,
            "error": None
        }
        
        try:
            # Step 1: Map instrument
            epic = InstrumentMapper.to_capital_epic(signal.instrument)
            if not epic:
                result["status"] = "rejected"
                result["reason"] = f"Unknown instrument: {signal.instrument}"
                logger.warning(result["reason"])
                await self._log_signal(signal, result)
                return result
            
            # Step 1.5: Check if instrument is configured in holiday.json
            if epic not in self.market_details:
                result["status"] = "rejected"
                result["reason"] = f"Instrument {epic} not configured in holiday.json"
                logger.warning(result["reason"])
                await self._log_signal(signal, result)
                return result
            
            # Step 2: Analyze with OpenAI
            logger.info(f"Analyzing signal with OpenAI: {signal.instrument} {signal.direction}")
            openai_analysis = await self._analyze_with_openai(signal)
            result["openai_analysis"] = openai_analysis
            
            if not openai_analysis.get("should_trade", False):
                result["status"] = "rejected"
                result["reason"] = f"OpenAI rejected: {openai_analysis.get('reasoning', 'Unknown')}"
                logger.info(result["reason"])
                await self._log_signal(signal, result)
                return result
            
            # Step 3: Calculate position size
            position_size = self._calculate_position_size(
                signal,
                epic,
                suggested_size=openai_analysis.get("suggested_size")
            )
            
            # Step 4: Execute trade if enabled
            if self.enable_auto_trading:
                # Redis duplicate check
                redis_key = f"trade_lock:{epic}:{signal.direction}"
                if self.redis_cache and self.redis_cache.key_exists(redis_key):
                    logger.warning(f"Skipping order for {epic} {signal.direction} - order executed within last hour")
                    result["status"] = "skipped_duplicate"
                    result["reason"] = f"Duplicate order attempt within 1 hour for {epic} {signal.direction}"
                    await self._log_signal(signal, result)
                    return result

                logger.info(f"Executing trade: {epic} {signal.direction} size={position_size}")
                order = await self._create_order(signal, epic, position_size)
                
                try:
                    deal_reference = await self.capital_api.place_order(order)
                    result["deal_reference"] = deal_reference
                    result["status"] = "executed"
                    result["reason"] = f"Trade executed: {deal_reference}"
                    logger.info(f"Trade executed successfully: {deal_reference}")

                    # Set Redis lock for 1 hour (3600 seconds)
                    if self.redis_cache:
                        self.redis_cache.set_key(redis_key, "1", ttl=3600)
                        logger.info(f"Set Redis lock for {redis_key} (1 hour)")
                except Exception as e:
                    result["status"] = "failed"
                    result["error"] = str(e)
                    result["reason"] = f"Trade execution failed: {e}"
                    logger.error(f"Trade execution failed: {e}", exc_info=True)
            else:
                result["status"] = "approved_not_executed"
                result["reason"] = "Auto-trading disabled - signal approved but not executed"
                logger.info(result["reason"])
            
            # Step 5: Log to database
            await self._log_signal(signal, result)
            
            # Step 6: Send notification
            await self._send_notification(signal, result)
            
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            result["reason"] = f"Processing error: {e}"
            logger.error(f"Error processing signal: {e}", exc_info=True)
            await self._log_signal(signal, result)
        
        return result
    
    async def _analyze_with_openai(self, signal: TradingSignal) -> Dict:
        """
        Analyze trading signal with OpenAI.
        
        Args:
            signal: Trading signal to analyze
            
        Returns:
            Dict with analysis results
        """
        try:
            prompt = f"""Analyze this trading signal and determine if it should be executed:

Instrument: {signal.instrument}
Direction: {signal.direction}
Entry Price: {signal.entry_price}
Stop Loss: {signal.stop_loss}
Target 1: {signal.target_1}
Target 2: {signal.target_2}
Confidence: {signal.confidence} stars (out of 5)
Expires: {signal.expires_at}
Risk/Reward Ratio: {signal.risk_reward_ratio():.2f}

Evaluation Criteria:
1. Risk/Reward Ratio: Should be > 1.5 (current: {signal.risk_reward_ratio():.2f})
2. Confidence Level: Signals with 3+ stars are acceptable if R/R is good
3. Stop Loss & Targets: Should be reasonable and achievable

Respond in JSON format:
{{
    "should_trade": true/false,
    "reasoning": "brief explanation of your decision",
    "suggested_size": 1.0 (multiplier, 1.0 = normal, 0.5 = half size, etc.),
    "risk_assessment": "low/medium/high"
}}"""

            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a professional trading analyst. Approve signals with 3+ stars confidence AND risk/reward ratio > 1.5. For 4-5 star signals with good R/R, approve with normal size. For 3-star signals with good R/R, approve with reduced size (0.5-0.75x). Only reject if R/R is poor (<1.5) or confidence is below 3 stars."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            
            analysis = json.loads(response.choices[0].message.content)
            logger.info(f"OpenAI analysis: {analysis}")
            return analysis
            
        except Exception as e:
            logger.error(f"OpenAI analysis failed: {e}", exc_info=True)
            # Default to conservative rejection on error
            return {
                "should_trade": False,
                "reasoning": f"Analysis failed: {str(e)}",
                "suggested_size": 0.0,
                "risk_assessment": "unknown"
            }
    
    def _calculate_position_size(self, signal: TradingSignal, epic: str, suggested_size: Optional[float] = None) -> float:
        """
        Calculate position size using configured quantity from holiday.json.
        Falls back to risk-based calculation if not configured.
        
        This follows the pattern from analyse.py:1159-1185 where quantity is
        read from market_details and multiplied by QTY_MULTIPLIER.
        
        Args:
            signal: Trading signal with entry and stop loss prices
            epic: Capital.com epic code
            suggested_size: Optional multiplier from OpenAI (default 1.0)
            
        Returns:
            Position size (number of contracts/lots)
        """
        # Get configured quantity from holiday.json (same as analyse.py:1163)
        epic_config = self.market_details.get(epic, {})
        configured_qty = epic_config.get('qty')
        
        if configured_qty:
            # Use configured quantity with multiplier (same as analyse.py:1169)
            quantity = float(configured_qty) * self.qty_multiplier
            
            # Apply OpenAI suggestion multiplier if provided
            if suggested_size and suggested_size > 0:
                quantity *= suggested_size
                logger.debug(f"Applied OpenAI size multiplier: {suggested_size}x")
            
            # Apply max position size limit for safety
            final_size = min(quantity, self.max_position_size)
            
            # Round to reasonable precision (2 decimal places)
            final_size = round(final_size, 2)
            
            logger.info(
                f"Using configured quantity from holiday.json: {configured_qty} × {self.qty_multiplier} "
                f"(OpenAI: {suggested_size or 1.0}x) = {final_size}"
            )
            
            return final_size
        else:
            # Fallback to risk-based calculation
            logger.warning(f"No configured quantity for {epic}, using risk-based calculation")
            
            stop_distance = abs(signal.entry_price - signal.stop_loss)
            
            if stop_distance == 0:
                logger.warning("Stop distance is zero, using default size")
                return 1.0
            
            # Calculate position size to risk the target dollar amount
            calculated_size = self.risk_per_trade_dollars / stop_distance
            
            # Apply OpenAI suggestion multiplier if provided
            if suggested_size and suggested_size > 0:
                calculated_size *= suggested_size
                logger.debug(f"Applied OpenAI size multiplier: {suggested_size}x")
            
            # Apply max position size limit for safety
            final_size = min(calculated_size, self.max_position_size)
            
            # Round to reasonable precision (2 decimal places)
            final_size = round(final_size, 2)
            
            logger.info(
                f"Position size calculated (risk-based): {final_size} "
                f"(Risk: ${self.risk_per_trade_dollars}, Stop distance: {stop_distance:.2f}, "
                f"Potential loss: ${final_size * stop_distance:.2f})"
            )
            
            return final_size
    
    
    def _round_to_increment(self, value: float, increment: float, round_up: bool = None) -> float:
        """
        Rounds a value up or down to the nearest multiple of the increment.
        
        Args:
            value: Value to round
            increment: Increment to round to
            round_up: If True, round up; if False, round down; if None, round to nearest
        """
        if increment <= 0:
            return value
        from decimal import Decimal, ROUND_HALF_UP, ROUND_UP, ROUND_DOWN
        decimal_value = Decimal(str(value))
        decimal_increment = Decimal(str(increment))
        
        if round_up is None:
            rounding_mode = ROUND_HALF_UP
        else:
            rounding_mode = ROUND_UP if round_up else ROUND_DOWN
            
        rounded = (decimal_value / decimal_increment).quantize(Decimal('1.'), rounding=rounding_mode) * decimal_increment
        return float(rounded)
    
    
    def _calculate_rounded_distance(self, value: float, market_details) -> float:
        """Round a distance/value to the nearest valid step."""
        if market_details.min_step_distance_unit == 'PERCENTAGE':
            step = value * (market_details.min_step_distance / 100)
            step = max(step, market_details.min_step_distance * 0.1)
        else:
            step = market_details.min_step_distance
        return self._round_to_increment(value, step)
    
    async def _calculate_sl_for_signal(
        self,
        direction: str,
        entry_price: float,
        epic: str,
        market_details
    ) -> float:
        """
        Calculate Stop Loss price using sl_perc from holiday.json.
        Follows the same logic as analyse.py:calculate_sl_for_breakout.
        
        Args:
            direction: "BUY" or "SELL"
            entry_price: Entry price for the trade
            epic: Capital.com epic code
            market_details: Market details from Capital.com API
            
        Returns:
            Calculated and validated stop loss price
        """
        # Calculate step size
        if market_details.min_step_distance_unit == 'PERCENTAGE':
            step = max(entry_price * (market_details.min_step_distance / 100), market_details.min_step_distance * 0.1)
        else:
            step = market_details.min_step_distance

        # Calculate min/max distances
        def calculate_distance(value, unit):
            if unit == 'PERCENTAGE':
                return entry_price * (value / 100)
            return value

        min_grn = calculate_distance(market_details.min_guaranteed_stop_distance, market_details.min_guaranteed_stop_distance_unit)
        min_nrm = calculate_distance(market_details.min_stop_or_profit_distance, market_details.min_stop_or_profit_distance_unit)
        max_distance = calculate_distance(market_details.max_stop_or_profit_distance, market_details.max_stop_or_profit_distance_unit)
        
        min_distance = max(min_grn, min_nrm)

        # Get SL percentage from holiday.json (default to 0.01 = 1%)
        sl_perc = self.market_details.get(epic, {}).get('sl_perc', 0.01)
        logger.info(f"SL percentage: {sl_perc} for {epic}")
        
        trans_type = CapitalTransactionType.BUY if direction == "BUY" else CapitalTransactionType.SELL
        
        if trans_type == CapitalTransactionType.BUY:
            desired_sl = entry_price * (1 - sl_perc)
        else:
            desired_sl = entry_price * (1 + sl_perc)

        # Apply distance constraints FIRST
        current_distance = abs(entry_price - desired_sl)
        
        if current_distance < min_distance:
            logger.warning(f"SL distance {current_distance:.4f} < min {min_distance:.4f}, adjusting.")
            if trans_type == CapitalTransactionType.BUY:
                desired_sl = entry_price - min_distance
            else:
                desired_sl = entry_price + min_distance
        
        if current_distance > max_distance:
            logger.warning(f"SL distance {current_distance:.4f} > max {max_distance:.4f}, adjusting.")
            if trans_type == CapitalTransactionType.BUY:
                desired_sl = entry_price - max_distance
            else:
                desired_sl = entry_price + max_distance

        # ROUND with correct direction
        round_up = (trans_type == CapitalTransactionType.SELL)
        rounded_sl = self._round_to_increment(desired_sl, step, round_up=round_up)

        # VALIDATE after rounding
        final_distance = abs(entry_price - rounded_sl)
        if final_distance < min_distance:
            logger.warning(f"Rounded SL distance {final_distance:.4f} < min {min_distance:.4f}, re-adjusting.")
            if trans_type == CapitalTransactionType.BUY:
                rounded_sl = self._round_to_increment(entry_price - min_distance, step, round_up=False)
            else:
                rounded_sl = self._round_to_increment(entry_price + min_distance, step, round_up=True)
        
        if final_distance > max_distance:
            logger.warning(f"Rounded SL distance {final_distance:.4f} > max {max_distance:.4f}, re-adjusting.")
            if trans_type == CapitalTransactionType.BUY:
                rounded_sl = self._round_to_increment(entry_price - max_distance, step, round_up=True)
            else:
                rounded_sl = self._round_to_increment(entry_price + max_distance, step, round_up=False)

        # Final validation
        final_distance = abs(entry_price - rounded_sl)
        if final_distance < min_distance or final_distance > max_distance:
            logger.error(f"SL validation failed: distance {final_distance:.4f} not in range [{min_distance:.4f}, {max_distance:.4f}]")
            # Fallback: use the minimum distance
            if trans_type == CapitalTransactionType.BUY:
                rounded_sl = entry_price - min_distance
            else:
                rounded_sl = entry_price + min_distance
            rounded_sl = self._round_to_increment(rounded_sl, step, round_up=(trans_type == CapitalTransactionType.SELL))

        logger.info(f"Final SL: {rounded_sl:.4f} (distance: {abs(entry_price - rounded_sl):.4f})")
        return rounded_sl
    
    async def _calculate_pl_for_signal(
        self,
        direction: str,
        entry_price: float,
        stop_loss: float,
        market_details
    ) -> float:
        """
        Calculate Profit Level with risk/reward ratio.
        Follows the same logic as analyse.py:calculate_pl_for_breakout.
        
        Args:
            direction: "BUY" or "SELL"
            entry_price: Entry price for the trade
            stop_loss: Calculated stop loss price
            market_details: Market details from Capital.com API
            
        Returns:
            Calculated and validated profit level price
        """
        # Get risk/reward ratio from environment (default 1.2)
        risk_reward_ratio = float(os.getenv("RISK_REWARD_RATIO", "1.2"))
        
        # Calculate step size
        if market_details.min_step_distance_unit == 'PERCENTAGE':
            step = max(entry_price * (market_details.min_step_distance / 100), market_details.min_step_distance * 0.1)
        else:
            step = market_details.min_step_distance

        # Calculate min/max distances
        def calculate_distance(value, unit):
            if unit == 'PERCENTAGE':
                return entry_price * (value / 100)
            return value

        min_distance = calculate_distance(market_details.min_stop_or_profit_distance, market_details.min_stop_or_profit_distance_unit)
        max_distance = calculate_distance(market_details.max_stop_or_profit_distance, market_details.max_stop_or_profit_distance_unit)

        # Calculate desired PL with risk-reward ratio
        risk = abs(entry_price - stop_loss)
        reward = risk * risk_reward_ratio
        
        trans_type = CapitalTransactionType.BUY if direction == "BUY" else CapitalTransactionType.SELL
        
        if trans_type == CapitalTransactionType.BUY:
            desired_pl = entry_price + reward
        else:
            desired_pl = entry_price - reward

        # Apply distance constraints FIRST
        current_distance = abs(entry_price - desired_pl)
        
        if current_distance < min_distance:
            logger.warning(f"PL distance {current_distance:.4f} < min {min_distance:.4f}, adjusting.")
            if trans_type == CapitalTransactionType.BUY:
                desired_pl = entry_price + min_distance
            else:
                desired_pl = entry_price - min_distance
        
        if current_distance > max_distance:
            logger.warning(f"PL distance {current_distance:.4f} > max {max_distance:.4f}, adjusting.")
            if trans_type == CapitalTransactionType.BUY:
                desired_pl = entry_price + max_distance
            else:
                desired_pl = entry_price - max_distance

        # ROUND with correct direction
        round_up = (trans_type == CapitalTransactionType.BUY)
        rounded_pl = self._round_to_increment(desired_pl, step, round_up=round_up)

        # VALIDATE after rounding
        final_distance = abs(entry_price - rounded_pl)
        if final_distance < min_distance:
            logger.warning(f"Rounded PL distance {final_distance:.4f} < min {min_distance:.4f}, re-adjusting.")
            if trans_type == CapitalTransactionType.BUY:
                rounded_pl = self._round_to_increment(entry_price + min_distance, step, round_up=True)
            else:
                rounded_pl = self._round_to_increment(entry_price - min_distance, step, round_up=False)
        
        if final_distance > max_distance:
            logger.warning(f"Rounded PL distance {final_distance:.4f} > max {max_distance:.4f}, re-adjusting.")
            if trans_type == CapitalTransactionType.BUY:
                rounded_pl = self._round_to_increment(entry_price + max_distance, step, round_up=False)
            else:
                rounded_pl = self._round_to_increment(entry_price - max_distance, step, round_up=True)

        # Final validation
        final_distance = abs(entry_price - rounded_pl)
        if final_distance < min_distance or final_distance > max_distance:
            logger.error(f"PL validation failed: distance {final_distance:.4f} not in range [{min_distance:.4f}, {max_distance:.4f}]")
            # Fallback: use 1:1 risk-reward
            if trans_type == CapitalTransactionType.BUY:
                rounded_pl = entry_price + risk
            else:
                rounded_pl = entry_price - risk
            rounded_pl = self._round_to_increment(rounded_pl, step, round_up=(trans_type == CapitalTransactionType.BUY))

        logger.info(f"Final PL: {rounded_pl:.4f} (distance: {abs(entry_price - rounded_pl):.4f}, R:R {risk_reward_ratio})")
        return rounded_pl
    
    async def _create_order(self, signal: TradingSignal, epic: str, size: float) -> BasicPlaceOrderCapital:
        """
        Create Capital.com order from signal with proper market constraints.
        
        This follows the pattern from analyse.py to ensure orders don't fail
        due to invalid quantities, stop distances, or other market rules.
        
        Args:
            signal: Trading signal
            epic: Capital.com epic code
            size: Initial position size (will be adjusted for constraints)
            
        Returns:
            BasicPlaceOrderCapital order object with all constraints applied
        """
        try:
            # Fetch market details to get constraints
            market_details = await self.capital_api.get_instrument_details(epic)
            if not market_details:
                logger.error(f"Failed to retrieve market details for {epic}")
                # Fallback to simple order without constraints (risky)
                direction = CapitalTransactionType.BUY if signal.direction == "BUY" else CapitalTransactionType.SELL
                return BasicPlaceOrderCapital(
                    epic=epic,
                    direction=direction,
                    size=size,
                    order_type=CapitalOrderType.MARKET,
                    stop_level=signal.stop_loss,
                    profit_level=signal.target_1,
                    guaranteed_stop=False,
                    currency_code="USD"
                )
            
            # Apply min/max deal size constraints
            quantity = size
            if quantity < market_details.min_deal_size:
                logger.info(
                    f"Calculated quantity {quantity:.4f} below minimum {market_details.min_deal_size} for {epic}. "
                    f"Setting to minimum."
                )
                quantity = float(market_details.min_deal_size)
            elif quantity > market_details.max_deal_size:
                logger.warning(
                    f"Calculated quantity {quantity:.4f} exceeds maximum {market_details.max_deal_size} for {epic}. "
                    f"Setting to maximum."
                )
                quantity = float(market_details.max_deal_size)
            
            # Round quantity to the nearest valid increment
            quantity = self._round_to_increment(quantity, market_details.min_size_increment)
            
            if quantity <= 0:
                logger.error(f"Final quantity is zero or negative for {epic}. Cannot create order.")
                raise ValueError(f"Invalid quantity after constraints: {quantity}")
            
            
            # Calculate SL and PL using holiday.json configuration (same as analyse.py)
            # This replaces the signal's SL/TP values with calculated values
            stop_level = await self._calculate_sl_for_signal(
                signal.direction,
                signal.entry_price,
                epic,
                market_details
            )
            profit_level = await self._calculate_pl_for_signal(
                signal.direction,
                signal.entry_price,
                stop_level,
                market_details
            )
            
            # Calculate distances (required by Capital.com API)
            stop_distance = self._calculate_rounded_distance(
                abs(signal.entry_price - stop_level),
                market_details
            )
            profit_distance = self._calculate_rounded_distance(
                abs(signal.entry_price - profit_level),
                market_details
            )
            
            direction = CapitalTransactionType.BUY if signal.direction == "BUY" else CapitalTransactionType.SELL
            
            order = BasicPlaceOrderCapital(
                quantity=quantity,
                price=signal.entry_price if signal.order_type == CapitalOrderType.LIMIT else None,
                epic=epic,
                order_type=signal.order_type,
                transaction_type=direction,
                stop_loss=stop_level,
                profit_level=profit_level,
                stop_distance=stop_distance,
                profit_distance=profit_distance,
            )
            
            logger.info(
                f"Created order: {epic} {signal.direction} qty={quantity:.2f} "
                f"(min={market_details.min_deal_size}, max={market_details.max_deal_size}, "
                f"increment={market_details.min_size_increment})"
            )
            logger.debug(
                f"Order distances: stop={stop_distance:.2f}, profit={profit_distance:.2f}, "
                f"step={market_details.min_step_distance} {market_details.min_step_distance_unit}"
            )
            
            return order
            
        except Exception as e:
            logger.error(f"Error creating order for {epic}: {e}", exc_info=True)
            raise
    
    async def _log_signal(self, signal: TradingSignal, result: Dict) -> None:
        """
        Log signal processing to database.
        
        Args:
            signal: Trading signal
            result: Processing result
        """
        try:
            query = """
            INSERT INTO telegram_signals 
            (instrument, direction, entry_price, stop_loss, target_1, target_2, 
             confidence, expires_at, received_at, openai_analysis, executed, 
             deal_reference, rejection_reason, raw_message, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            """
            
            # Convert timezone-aware datetimes to naive for PostgreSQL compatibility
            expires_at = signal.expires_at.replace(tzinfo=None) if signal.expires_at and hasattr(signal.expires_at, 'tzinfo') else signal.expires_at
            received_at = signal.timestamp.replace(tzinfo=None) if signal.timestamp and hasattr(signal.timestamp, 'tzinfo') else signal.timestamp
            
            await self.db_conn.execute(
                query,
                signal.instrument,
                signal.direction,
                signal.entry_price,
                signal.stop_loss,
                signal.target_1,
                signal.target_2,
                signal.confidence,
                expires_at,
                received_at,
                json.dumps(result.get("openai_analysis")),
                result["status"] == "executed",
                result.get("deal_reference"),
                result.get("reason"),
                signal.raw_message,
                result["status"]
            )
            logger.debug("Signal logged to database")
        except Exception as e:
            logger.error(f"Failed to log signal to database: {e}", exc_info=True)
    
    async def _send_notification(self, signal: TradingSignal, result: Dict) -> None:
        """
        Send notification about signal processing.
        
        Args:
            signal: Trading signal
            result: Processing result
        """
        try:
            # Get Telegram bot credentials from environment
            bot_token = os.getenv("TELEGRAM_TOKEN")
            chat_id = os.getenv("TELEGRAM_CHAT_ID")
            
            if not bot_token or not chat_id:
                logger.warning("Telegram notification credentials not configured (TELEGRAM_TOKEN or TELEGRAM_CHAT_ID missing)")
                return
            
            from app.notification.telegram import TelegramAPI
            
            status_emoji = {
                "executed": "✅",
                "approved_not_executed": "⚠️",
                "rejected": "❌",
                "failed": "🚫",
                "error": "⚠️"
            }
            
            emoji = status_emoji.get(result["status"], "ℹ️")
            
            message = f"""{emoji} Telegram Signal Processed

Instrument: {signal.instrument}
Direction: {signal.direction}
Entry: {signal.entry_price}
Stop: {signal.stop_loss}
Target 1: {signal.target_1}
Confidence: {'⭐' * signal.confidence}
R/R Ratio: {signal.risk_reward_ratio():.2f}

Status: {result['status'].replace('_', ' ').title()}
Reason: {result.get('reason', 'N/A')}

OpenAI Assessment: {result.get('openai_analysis', {}).get('reasoning', 'N/A')}
"""
            
            if result.get("deal_reference"):
                message += f"\nDeal Reference: {result['deal_reference']}"
            
            telegram_api = TelegramAPI(bot_token)
            telegram_api.send_message(chat_id, message)
            logger.debug("Notification sent")
        except Exception as e:
            logger.error(f"Failed to send notification: {e}", exc_info=True)
