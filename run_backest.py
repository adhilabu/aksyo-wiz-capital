#!/usr/bin/env python3
"""
Improved multi-symbol backtest framework
- Connects to Postgres (DATABASE_URL from .env)
- Implements improved per-symbol strategies with trend filters, ADX, multi-timeframe confirmation,
  ATR-based stops/trails, partial scaling, and additional risk controls.
- Writes per-symbol trade logs, equity curves and a summary CSV to backtest_outputs/.
"""

import os
from pathlib import Path
import dotenv
import math
import pandas as pd
import numpy as np
import sqlalchemy
import warnings
from datetime import timedelta

warnings.filterwarnings("ignore")

dotenv.load_dotenv(dotenv_path=".env", override=True)
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL must be set in .env")

engine = sqlalchemy.create_engine(DATABASE_URL, future=True)

# --- User-configurable settings ---
SYMBOLS = ["OIL_CRUDE","US100","J225","US30","BTCUSD","USCOCOA","USDJPY","EURJPY"]

OUT_DIR = Path("backtest_outputs")
OUT_DIR.mkdir(exist_ok=True)

INITIAL_EQUITY = 100_000.0
RISK_PER_TRADE = 0.01        # 1% per trade
MAX_DAILY_LOSS = 0.03       # stop trading for the day if equity falls by 3%
SPREAD_PCT = 0.0005         # generic spread fraction (adjust per instrument)
SLIPPAGE_PCT = 0.0002
COMMISSION = 0.0
MAX_CONCURRENT_TRADES = 4
MIN_BARS_REQUIRED = 200

VERBOSE = True

# Helper indicators
def atr(df, n=14):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift(1)).abs()
    low_close = (df['low'] - df['close'].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=1).mean()

def sma(series, n):
    return series.rolling(n, min_periods=1).mean()

def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def rsi(series, n=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ema_up = up.ewm(com=n-1, adjust=False).mean()
    ema_down = down.ewm(com=n-1, adjust=False).mean()
    rs = ema_up / ema_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def donchian_high(df, n=20):
    return df['high'].rolling(n, min_periods=1).max()

def donchian_low(df, n=20):
    return df['low'].rolling(n, min_periods=1).min()

def bollinger_bands(series, n=20, k=2):
    ma = series.rolling(n, min_periods=1).mean()
    std = series.rolling(n, min_periods=1).std().fillna(0)
    upper = ma + k * std
    lower = ma - k * std
    return ma, upper, lower

def adx(df, n=14):
    # Wilder's ADX
    up_move = df['high'].diff()
    down_move = -df['low'].diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift(1)).abs(),
        (df['low'] - df['close'].shift(1)).abs()
    ], axis=1).max(axis=1)
    atr_val = tr.rolling(n, min_periods=1).mean()
    plus_di = 100 * (pd.Series(plus_dm).rolling(n, min_periods=1).mean() / atr_val)
    minus_di = 100 * (pd.Series(minus_dm).rolling(n, min_periods=1).mean() / atr_val)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)).replace([np.inf, -np.inf], 0) * 100
    adx_val = dx.rolling(n, min_periods=1).mean()
    return adx_val

# --- Strategy functions with stronger rules ---

def trend_confirmed(symbol_df, higher_tf_df=None):
    """
    Trend filter:
    - daily SMA50: price must be above (long) or below (short) to allow trend trades.
    - ADX on relevant timeframe must be >= adx_thresh to accept trend trades.
    """
    df = symbol_df
    df['sma50'] = sma(df['close'], 50)
    df['adx14'] = adx(df, 14)
    # if higher_tf_df provided, align with higher timeframe trend (multi-timeframe)
    if higher_tf_df is not None:
        # resample higher_tf_df daily and compute sma50 on close
        ht = higher_tf_df.copy()
        ht['sma50'] = sma(ht['close'], 50)
        # align by date: use last available daily sma50 prior to intraday bar
        # create series of daily sma50 indexed by date
        daily_sma50 = ht['sma50']
        df_dates = df.index.normalize()
        df['daily_sma50'] = df_dates.map(lambda d: daily_sma50.loc[:d].iloc[-1] if (daily_sma50.loc[:d].shape[0] > 0) else np.nan).values
    else:
        df['daily_sma50'] = df['sma50']
    return df

def strategy_trend_ema_with_adx(df, higher_tf_df=None, adx_thresh=20, ema_short=8, ema_long=21,
                                min_vol_mult=0.8, atr_stop_mult=2.5, tp_atr_mult=3.5,
                                partial_scale=0.4):
    """
    Trend EMA strategy with ADX filter and optional volume confirmation on breakouts.
    - Only open long if price > daily SMA50 and ADX >= adx_thresh.
    - EMA crossover as entry trigger.
    - ATR-based stop, TP and trailing stop; partial profit at first TP and trail remainder.
    """
    df = df.copy()
    df = trend_confirmed(df, higher_tf_df)
    df['ema_short'] = ema(df['close'], ema_short)
    df['ema_long'] = ema(df['close'], ema_long)
    df['atr14'] = atr(df, 14)
    df['vol_ma20'] = df['volume'].rolling(20, min_periods=1).mean().fillna(0)
    signals = pd.Series(0, index=df.index)

    for i in range(1, len(df)):
        # require sufficient bars
        if i < 50:
            continue
        row = df.iloc[i]
        prev = df.iloc[i-1]
        # only consider long if price above daily sma50 and ADX strong
        allow_long = (row['close'] > row['daily_sma50']) and (row['adx14'] >= adx_thresh)
        allow_short = (row['close'] < row['daily_sma50']) and (row['adx14'] >= adx_thresh)
        # EMA crossover signals
        if allow_long and (prev['ema_short'] <= prev['ema_long']) and (row['ema_short'] > row['ema_long']):
            # require volume not extremely low
            if row['volume'] >= (row['vol_ma20'] * min_vol_mult):
                signals.iloc[i] = 1
        elif allow_short and (prev['ema_short'] >= prev['ema_long']) and (row['ema_short'] < row['ema_long']):
            if row['volume'] >= (row['vol_ma20'] * min_vol_mult):
                signals.iloc[i] = -1
    return signals

def strategy_donchian_breakout_with_filters(df, higher_tf_df=None, window=20, adx_thresh=18,
                                             min_vol_mult=1.2, atr_stop_mult=2.5, tp_atr_mult=4.0):
    """
    Donchian breakout but only take if higher-timeframe trend aligns or ADX indicates trending market.
    Confirm breakout by close > Donchian high and volume spike.
    """
    df = df.copy()
    df = trend_confirmed(df, higher_tf_df)
    df['don_high'] = donchian_high(df, window)
    df['don_low'] = donchian_low(df, window)
    df['don_high_prev'] = df['don_high'].shift(1)
    df['don_low_prev'] = df['don_low'].shift(1)
    df['atr14'] = atr(df, 14)
    df['vol_ma20'] = df['volume'].rolling(20, min_periods=1).mean().fillna(0)
    signals = pd.Series(0, index=df.index)

    for i in range(1, len(df)):
        if i < window + 5:
            continue
        row = df.iloc[i]
        # breakout long
        if (row['close'] > row['don_high_prev']) and (row['adx14'] >= adx_thresh) and (row['close'] > row['daily_sma50']):
            if row['volume'] >= row['vol_ma20'] * min_vol_mult:
                signals.iloc[i] = 1
        # breakout short
        if (row['close'] < row['don_low_prev']) and (row['adx14'] >= adx_thresh) and (row['close'] < row['daily_sma50']):
            if row['volume'] >= row['vol_ma20'] * min_vol_mult:
                signals.iloc[i] = -1
    return signals

def strategy_mean_reversion_rsi_bb(df, adx_thresh_range=18, rsi_low=28, rsi_high=72,
                                   bb_n=20, bb_k=2.0, atr_stop_mult=1.6, tp_risk_mult=1.5):
    """
    Mean reversion using RSI + Bollinger bands. Only trade when ADX indicates range (ADX < adx_thresh_range).
    Tighter ATR-based stops and fixed TP (1.2-1.8R) to increase probability.
    """
    df = df.copy()
    df['rsi14'] = rsi(df['close'], 14)
    df['atr14'] = atr(df, 14)
    df['adx14'] = adx(df, 14)
    ma, ub, lb = bollinger_bands(df['close'], bb_n, bb_k)
    df['bb_upper'] = ub
    df['bb_lower'] = lb
    signals = pd.Series(0, index=df.index)

    for i in range(1, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i-1]
        # only trade when ADX low (range)
        if row['adx14'] >= adx_thresh_range:
            continue
        # long: price near lower BB and RSI low
        if (row['close'] <= row['bb_lower']) and (row['rsi14'] <= rsi_low):
            signals.iloc[i] = 1
        # short: price near upper BB and RSI high
        if (row['close'] >= row['bb_upper']) and (row['rsi14'] >= rsi_high):
            signals.iloc[i] = -1
    return signals

# --- Backtester core (simplified but robust) ---
def compute_position_size(equity, risk_per_trade, entry_price, stop_price):
    stop_distance = abs(entry_price - stop_price)
    if stop_distance <= 0:
        return 0.0
    risk_amount = equity * risk_per_trade
    qty = risk_amount / stop_distance
    # cap position size to avoid huge sizes on tiny stops
    qty = max(0.0, qty)
    return qty

def backtest_symbol(df, symbol, strategy_fn, higher_tf_df=None,
                    atr_stop_mult=2.0, tp_atr_mult=3.0,
                    max_hold_bars=60, initial_equity=INITIAL_EQUITY):
    df = df.copy().sort_index()
    if len(df) < MIN_BARS_REQUIRED:
        return {"error": "not enough bars", "bars": len(df)}

    # precompute common indicators
    df['atr14'] = atr(df, 14)
    df['adx14'] = adx(df, 14)
    df['sma50'] = sma(df['close'], 50)
    df['daily_sma50'] = df['sma50']  # we may override with higher_tf mapping earlier externally

    # get signals series (1, -1, 0), signals are computed at close of bar i and executed next bar open
    signals = strategy_fn(df)  # function expects df and returns pd.Series aligned with df.index

    equity = initial_equity
    daily_start_equity = equity
    trades = []
    open_positions = []
    equity_curve = []

    # for daily loss stop
    last_day = None
    daily_max_loss_triggered = False

    for i in range(0, len(df)-1):
        dt = df.index[i]
        # reset daily loss monitor at new day
        if last_day is None or dt.date() != last_day:
            last_day = dt.date()
            daily_start_equity = equity
            daily_max_loss_triggered = False

        if daily_max_loss_triggered:
            equity_curve.append({"timestamp": dt, "equity": equity})
            continue

        # step 1: process exits for existing positions using this bar high/low (intrabar)
        if open_positions:
            # evaluate each open position for stop/tp using bar i+1 intrabar H/L (we track entry index; we can check all positions)
            for pos in list(open_positions):  # copy
                entry_idx = pos['entry_idx']
                # we check current bar (i) H/L only if we've already entered at or before i
                if i < entry_idx:
                    continue
                bar_high = float(df['high'].iloc[i])
                bar_low = float(df['low'].iloc[i])
                exit_price = None
                exit_reason = None
                # check stop
                if pos['side'] == 1 and bar_low <= pos['stop_price']:
                    exit_price = pos['stop_price']
                    exit_reason = 'stop'
                elif pos['side'] == -1 and bar_high >= pos['stop_price']:
                    exit_price = pos['stop_price']
                    exit_reason = 'stop'
                # check TP (first partial TP handled)
                if exit_price is None and pos.get('tp_price') is not None:
                    if pos['side'] == 1 and bar_high >= pos['tp_price']:
                        # partial or full TP
                        exit_price = pos['tp_price']
                        exit_reason = 'tp'
                    elif pos['side'] == -1 and bar_low <= pos['tp_price']:
                        exit_price = pos['tp_price']
                        exit_reason = 'tp'
                # check max hold
                hold = i - pos['entry_idx'] + 1
                if exit_price is None and hold >= max_hold_bars:
                    exit_price = float(df['close'].iloc[i])
                    exit_reason = 'time_exit'
                # finalize exit
                if exit_price is not None:
                    qty = pos['qty']
                    gross_pnl = (exit_price - pos['entry_price']) * qty if pos['side'] == 1 else (pos['entry_price'] - exit_price) * qty
                    net_pnl = gross_pnl - COMMISSION
                    equity += net_pnl
                    trades.append({
                        "symbol": symbol,
                        "entry_time": pos['entry_time'],
                        "entry_price": pos['entry_price'],
                        "exit_time": df.index[i],
                        "exit_price": exit_price,
                        "side": 'LONG' if pos['side']==1 else 'SHORT',
                        "qty": qty,
                        "gross_pnl": gross_pnl,
                        "net_pnl": net_pnl,
                        "exit_reason": exit_reason,
                        "hold_bars": hold,
                        "equity_after": equity
                    })
                    open_positions.remove(pos)

        # step 2: open new positions based on signal at close of bar i -> execute at next bar open (i+1)
        sig = int(signals.iloc[i]) if i < len(signals) else 0
        next_idx = i+1
        if sig != 0 and len(open_positions) < MAX_CONCURRENT_TRADES:
            # check higher timeframe daily SMA alignment if provided via df['daily_sma50'] already
            next_open = float(df['open'].iloc[next_idx])
            # entry price adjusted for spread/slippage
            entry_price = next_open * (1 + (SPREAD_PCT/2 + SLIPPAGE_PCT)) if sig == 1 else next_open * (1 - (SPREAD_PCT/2 + SLIPPAGE_PCT))
            # stop price using ATR from next_idx if available
            atr_val = float(df['atr14'].iloc[next_idx])
            stop_price = (entry_price - atr_stop_mult * atr_val) if sig == 1 else (entry_price + atr_stop_mult * atr_val)
            tp_price = (entry_price + tp_atr_mult * atr_val) if sig == 1 else (entry_price - tp_atr_mult * atr_val)
            qty = compute_position_size(equity, RISK_PER_TRADE, entry_price, stop_price)
            # ensure qty not zero and not absurd
            if qty <= 0:
                pass
            else:
                pos = {
                    "symbol": symbol,
                    "side": sig,
                    "entry_idx": next_idx,
                    "entry_time": df.index[next_idx],
                    "entry_price": entry_price,
                    "stop_price": stop_price,
                    "tp_price": tp_price,
                    "qty": qty,
                }
                open_positions.append(pos)
                equity -= COMMISSION  # commission at entry
                if VERBOSE:
                    print(f"[{symbol}] Enter {'LONG' if sig==1 else 'SHORT'} qty={qty:.4f} entry={entry_price:.4f} stop={stop_price:.4f} tp={tp_price:.4f} at {pos['entry_time']}")

        # record equity curve
        equity_curve.append({"timestamp": df.index[i], "equity": equity})

        # daily loss check
        if (daily_start_equity - equity) / daily_start_equity >= MAX_DAILY_LOSS:
            daily_max_loss_triggered = True
            if VERBOSE:
                print(f"[{symbol}] Daily loss limit hit on {dt.date()} equity {equity:.2f}, stop trading for the day")

    # close any remaining open positions at last close
    if open_positions:
        last_close = float(df['close'].iloc[-1])
        for pos in open_positions:
            exit_price = last_close
            gross_pnl = (exit_price - pos['entry_price']) * pos['qty'] if pos['side']==1 else (pos['entry_price'] - exit_price) * pos['qty']
            net_pnl = gross_pnl - COMMISSION
            equity += net_pnl
            trades.append({
                "symbol": symbol,
                "entry_time": pos['entry_time'],
                "entry_price": pos['entry_price'],
                "exit_time": df.index[-1],
                "exit_price": exit_price,
                "side": 'LONG' if pos['side']==1 else 'SHORT',
                "qty": pos['qty'],
                "gross_pnl": gross_pnl,
                "net_pnl": net_pnl,
                "exit_reason": 'eod_exit',
                "hold_bars": len(df)-pos['entry_idx'],
                "equity_after": equity
            })
        open_positions = []

    eq_df = pd.DataFrame(equity_curve).set_index('timestamp') if equity_curve else pd.DataFrame([{"timestamp": df.index[-1], "equity": equity}]).set_index('timestamp')
    trades_df = pd.DataFrame(trades)
    return {"trades": trades_df, "equity": eq_df, "final_equity": equity, "bars": len(df)}

# --- Per-symbol strategy mapping (improved) ---
# We choose the strategy function and parameters for each symbol to attempt to improve win-rate/expectancy
STRATEGY_PLAN = {
    "OIL_CRUDE": {"fn": strategy_donchian_breakout_with_filters, "params": {"window":21, "adx_thresh":18, "min_vol_mult":1.1}, "atr_stop_mult":3.0, "tp_atr_mult":4.0},
    "US100": {"fn": strategy_trend_ema_with_adx, "params": {"adx_thresh":22, "ema_short":8, "ema_long":21, "min_vol_mult":0.9}, "atr_stop_mult":2.5, "tp_atr_mult":3.0},
    "J225": {"fn": strategy_trend_ema_with_adx, "params": {"adx_thresh":20, "ema_short":8, "ema_long":21, "min_vol_mult":0.8}, "atr_stop_mult":2.8, "tp_atr_mult":3.5},
    "US30": {"fn": strategy_trend_ema_with_adx, "params": {"adx_thresh":20, "ema_short":8, "ema_long":21, "min_vol_mult":0.9}, "atr_stop_mult":2.5, "tp_atr_mult":3.0},
    "BTCUSD": {"fn": strategy_donchian_breakout_with_filters, "params": {"window":20, "adx_thresh":18, "min_vol_mult":1.0}, "atr_stop_mult":3.5, "tp_atr_mult":5.0},
    "USCOCOA": {"fn": strategy_mean_reversion_rsi_bb, "params": {"adx_thresh_range":18, "rsi_low":30, "rsi_high":70, "bb_n":20, "bb_k":2.0}, "atr_stop_mult":1.6, "tp_atr_mult":2.0},
    "USDJPY": {"fn": strategy_mean_reversion_rsi_bb, "params": {"adx_thresh_range":18, "rsi_low":30, "rsi_high":70, "bb_n":20, "bb_k":1.8}, "atr_stop_mult":1.6, "tp_atr_mult":1.8},
    "EURJPY": {"fn": strategy_trend_ema_with_adx, "params": {"adx_thresh":20, "ema_short":8, "ema_long":21, "min_vol_mult":0.7}, "atr_stop_mult":2.4, "tp_atr_mult":3.0},
}

# helper to fetch higher timeframe daily data for multi-timeframe confirmation
def fetch_higher_tf(symbol):
    # fetch daily bars for the same symbol to compute daily SMA50 alignment
    sql = "SELECT timestamp, open, high, low, close, volume FROM historical_data WHERE stock = %s ORDER BY timestamp ASC"
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=(symbol,))
    if df.empty:
        return None
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp').sort_index()
    # if DB is intraday, resample daily
    if (df.index[-1] - df.index[0]) > pd.Timedelta(days=3) and (df.index.freq is None or df.index.inferred_freq is None):
        try:
            daily = df.resample('D').agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}).dropna()
            return daily
        except Exception:
            return df
    return df

# runner
def run_backtests(symbols=SYMBOLS):
    summary = []
    # Preload daily maps for multi-timeframe alignment if helpful
    daily_cache = {}
    for sym in symbols:
        plan = STRATEGY_PLAN.get(sym)
        if not plan:
            print(f"No plan for {sym}, skipping")
            continue
        print(f"\nPreparing {sym} ...")
        # fetch intraday/raw bars for symbol
        sql = "SELECT timestamp, open, high, low, close, volume, stock FROM historical_data WHERE stock = %s ORDER BY timestamp ASC"
        try:
            with engine.connect() as conn:
                df = pd.read_sql(sql, conn, params=(sym,))
        except Exception as e:
            print(f"Failed to fetch {sym}: {e}")
            continue
        if df.empty:
            print(f"No data for {sym}")
            continue
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').sort_index()
        if len(df) < MIN_BARS_REQUIRED:
            print(f"{sym} has only {len(df)} bars, skipping (min required {MIN_BARS_REQUIRED})")
            continue

        # fetch daily/higher timeframe once
        if sym not in daily_cache:
            daily_df = fetch_higher_tf(sym)
            daily_cache[sym] = daily_df

        higher_tf = daily_cache.get(sym)

        # prepare strategy function wrapper
        strat_fn = plan['fn']
        strat_params = plan.get('params', {})
        # create a wrapper that will pass df into strategy with params and (optionally) higher_tf
        def strategy_wrapper(local_df, fn=strat_fn, params=strat_params, htf=higher_tf):
            # supply higher_tf if function supports it
            try:
                # functions that accept higher_tf as positional/keyword will handle gracefully
                return fn(local_df, higher_tf_df=htf, **params)
            except TypeError:
                # fallback if function signature doesn't have higher_tf
                return fn(local_df, **params)

        # run backtest
        out = backtest_symbol(df, sym, strategy_wrapper,
                              higher_tf_df=higher_tf,
                              atr_stop_mult=plan.get('atr_stop_mult', 2.0),
                              tp_atr_mult=plan.get('tp_atr_mult', 3.0),
                              initial_equity=INITIAL_EQUITY)
        if 'error' in out:
            print(f"Backtest for {sym} error: {out['error']}")
            continue

        trades_df = out['trades']
        eq_df = out['equity']
        final_equity = out['final_equity']

        # save outputs
        safe_sym = sym.replace("/", "_")
        if not trades_df.empty:
            trades_df.to_csv(OUT_DIR / f"trades_{safe_sym}.csv", index=False)
        if not eq_df.empty:
            eq_df.to_csv(OUT_DIR / f"equity_{safe_sym}.csv")

        # compute summary metrics
        n_trades = len(trades_df)
        wins = trades_df[trades_df['net_pnl'] > 0] if not trades_df.empty else pd.DataFrame()
        losses = trades_df[trades_df['net_pnl'] <= 0] if not trades_df.empty else pd.DataFrame()
        win_rate = len(wins) / n_trades if n_trades > 0 else 0.0
        total_net = trades_df['net_pnl'].sum() if not trades_df.empty else 0.0
        avg_win = wins['net_pnl'].mean() if not wins.empty else 0.0
        avg_loss = losses['net_pnl'].mean() if not losses.empty else 0.0
        max_dd = compute_max_drawdown(eq_df['equity'].values if not eq_df.empty else np.array([INITIAL_EQUITY]))

        summary.append({
            "symbol": sym,
            "strategy": strat_fn.__name__,
            "bars": out.get('bars', 0),
            "n_trades": n_trades,
            "win_rate": round(win_rate * 100, 2),
            "total_net_pnl": round(total_net, 2),
            "final_equity": round(final_equity, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "max_drawdown": round(max_dd * 100, 2)
        })
        print(f"Completed {sym}: trades={n_trades} win_rate={round(win_rate*100,2)}% final_eq={round(final_equity,2)}")

    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(OUT_DIR / "summary_improved.csv", index=False)
    print("\nBacktests complete. Outputs in", OUT_DIR.resolve())
    return summary_df

# very small utility
def compute_max_drawdown(equity_array):
    if equity_array is None or len(equity_array) == 0:
        return 0.0
    arr = np.array(equity_array, dtype=float)
    peak = arr[0]
    max_dd = 0.0
    for x in arr:
        if x > peak:
            peak = x
        dd = (peak - x) / peak if peak != 0 else 0
        if dd > max_dd:
            max_dd = dd
    return max_dd

if __name__ == "__main__":
    print("Running improved backtests for symbols:", SYMBOLS)
    summary = run_backtests(SYMBOLS)
    print("\nSummary:")
    print(summary.to_string(index=False))
