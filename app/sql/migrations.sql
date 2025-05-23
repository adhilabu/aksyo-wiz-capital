-- Create table for storing the feed data
CREATE TABLE IF NOT EXISTS feed_data_ltpc (
    id SERIAL PRIMARY KEY,         -- Auto increment ID for reference
    instrument_code VARCHAR(255) UNIQUE NOT NULL, -- Instrument code like "NSE_EQ|INE002A01018"
    ltp DECIMAL(10, 2),            -- Last Traded Price
    ltt TIMESTAMP,                 -- Last Traded Time (converted from milliseconds)
    ltq INT,                       -- Last Traded Quantity
    cp DECIMAL(10, 2),             -- Closing Price
    current_ts TIMESTAMP           -- Current timestamp (converted from milliseconds)
);

-- Single table to store all the option chain feed data
CREATE TABLE IF NOT EXISTS feed_data_option_chain (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    market_ltp DECIMAL,
    market_ltt BIGINT,
    market_ltq INT,
    market_cp DECIMAL,
    bidAskQuote JSONB,
    op DECIMAL,
    up DECIMAL,
    iv DECIMAL,
    delta DECIMAL,
    theta DECIMAL,
    gamma DECIMAL,
    vega DECIMAL,
    rho DECIMAL,
    marketOHLC JSONB,
    atp DECIMAL,
    cp_eFeed DECIMAL,
    vtt BIGINT,
    oi INT,
    tbq INT,
    tsq INT,
    lc DECIMAL,
    uc DECIMAL,
    fp DECIMAL,
    fv INT,
    dh_oi INT,
    dl_oi INT,
    poi INT,
    current_ts BIGINT
);

CREATE TABLE IF NOT EXISTS stock_data (
    stock_symbol TEXT NOT NULL,
    close FLOAT NOT NULL,
    volume FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    PRIMARY KEY (stock_symbol, timestamp)
);


CREATE TABLE IF NOT EXISTS support_resistance_levels (
    id SERIAL PRIMARY KEY,
    stock TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    pivot_data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- For support_resistance_levels table
ALTER TABLE support_resistance_levels 
    DROP CONSTRAINT IF EXISTS unique_stock_date;
ALTER TABLE support_resistance_levels 
    ADD CONSTRAINT unique_stock_date UNIQUE (stock, start_date, end_date);

CREATE TABLE IF NOT EXISTS order_details (
    id SERIAL PRIMARY KEY,
    stock TEXT NOT NULL,
    ltp REAL NOT NULL,
    sl REAL NOT NULL,
    pl REAL NOT NULL,
    cp REAL NOT NULL,
    broke_resistance_level REAL NOT NULL,
    rsi REAL NOT NULL,
    adx REAL NOT NULL,
    mfi REAL NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    status TEXT DEFAULT 'OPEN',
    trade_type TEXT DEFAULT 'BUY'
);

-- Get profit and loss
-- SELECT SUM(CASE WHEN status = 'PROFIT' THEN ((pl - ltp) * (10000 / ltp)) ELSE 0 END) AS total_profit, SUM(CASE WHEN status = '
--  LOSS' THEN ((ltp - sl) * (10000 / ltp)) ELSE 0 END) AS total_loss FROM order_details WHERE status IN ('PROFIT', 'LOSS');

CREATE TABLE IF NOT EXISTS capital_log (
    id SERIAL PRIMARY KEY,                         
    endpoint VARCHAR(255) NOT NULL,               
    request_payload JSON NOT NULL,                
    response_payload JSON NOT NULL,               
    status_code INT NOT NULL,                     
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

CREATE TABLE IF NOT EXISTS index_indicators_data (
  id SERIAL PRIMARY KEY, 
  index_symbol VARCHAR(255) NULL, 
  index_name VARCHAR(255) NULL, 
  timestamp TIMESTAMP NULL, 
  rsi FLOAT NULL, 
  adx FLOAT NULL, 
  mfi FLOAT NULL, 
  active BOOLEAN DEFAULT FALSE, 
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- For index_indicators_data table
ALTER TABLE index_indicators_data 
    DROP CONSTRAINT IF EXISTS unique_index_symbol_name;
ALTER TABLE index_indicators_data 
    ADD CONSTRAINT unique_index_symbol_name UNIQUE (index_symbol, index_name);

ALTER TABLE order_details ADD COLUMN IF NOT EXISTS entry_price REAL NOT NULL DEFAULT 0;
ALTER TABLE order_details ADD COLUMN IF NOT EXISTS exit_price REAL NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS historical_data ( id SERIAL, ltp DOUBLE PRECISION, "timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL, open DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION, close DOUBLE PRECISION, volume BIGINT, open_interest BIGINT, stock VARCHAR(255) NOT NULL );
ALTER TABLE order_details ADD COLUMN IF NOT EXISTS stock_name TEXT NULL DEFAULT NULL;

-- SELECT date(timestamp) AS order_date, COUNT(*) AS total_trades, ROUND(SUM( CASE WHEN status = 'PROFIT' THEN (exit_price - entry_price) * (100000 / entry_price) ELSE 0 END )::numeric, 2) AS total_profit, ROUND(SUM( CASE WHEN status = 'LOSS' THEN (entry_price - exit_price) * (100000 / entry_price) ELSE 0 END )::numeric, 2) AS total_loss, ROUND( (SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100), 2 ) AS trade_accuracy_percentage, ROUND( ( (SUM(CASE WHEN status = 'PROFIT' THEN (exit_price - entry_price) * (100000 / entry_price) ELSE 0 END) - SUM(CASE WHEN status = 'LOSS' THEN (entry_price - exit_price) * (100000 / entry_price) ELSE 0 END) )::numeric / 100000.0 * 100 ), 2 ) AS net_profit_percentage, ROUND((COUNT(*) * 60)::numeric, 2) AS total_charges, ROUND( ( ( SUM(CASE WHEN status = 'PROFIT' THEN (exit_price - entry_price) * (100000 / entry_price) ELSE 0 END) - SUM(CASE WHEN status = 'LOSS' THEN (entry_price - exit_price) * (100000 / entry_price) ELSE 0 END) ) - (COUNT(*) * 60) )::numeric / 100000.0 * 100, 2 ) AS final_profit_percentage FROM order_details WHERE status IN ('PROFIT', 'LOSS') GROUP BY date(timestamp) ORDER BY order_date;
-- SELECT date(timestamp) AS order_date, COUNT(*) AS total_trades, ROUND(SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2) AS total_profit, ROUND(SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2) AS total_loss, ROUND((SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100), 2) AS trade_accuracy_percentage, ROUND(((SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ))::numeric / 100000.0 * 100), 2) AS net_profit_percentage, ROUND((COUNT(*) * 60)::numeric, 2) AS total_charges, ROUND(((SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - (COUNT(*) * 60))::numeric / 100000.0 * 100), 2) AS final_profit_percentage FROM order_details WHERE status IN ('PROFIT', 'LOSS') GROUP BY date(timestamp) ORDER BY order_date;
-- SELECT date(timestamp) AS order_date, COUNT(*) AS total_trades, ROUND(SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_p rice - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END )::numeri c, 2) AS total_profit, ROUND(SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND tra de_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2) AS total_loss, ROUND((SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100), 2) AS trade_accuracy_percentage, ROUND(( ( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * ( 100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOS S' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (10000 0 / exit_price) ELSE 0 END ) )::numeric / 100000.0 * 100 ), 2) AS net_profit_percentage, ROUND((COUNT(*) * 60)::numeric, 2) AS total_charges, ROUND(( ( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exi t_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN stat us = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END ) - (COUNT(*) * 60) )::numeric / 100000.0 * 100 ), 2) AS final_p rofit_percentage FROM order_details WHERE status IN ('PROFIT', 'LOSS') GROUP BY date(timestamp) ORDER BY order_date;
-- SELECT date(timestamp) AS order_date, COUNT(*) AS total_trades, ROUND(SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2) AS total_profit, ROUND(SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2) AS total_loss, ROUND((SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0END)::numeric / COUNT(*) * 100), 2) AS trade_accuracy_percentage, ROUND(( ( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * ( 100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOS S' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END ) )::numeric / 100000.0 * 100 ), 2) AS net_profit_percentage, ROUND((COUNT(*) * 60)::numeric, 2) AS total_charges, ROUND(( ( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END ) - (COUNT(*) * 60) )::numeric / 100000.0 * 100 ), 2) AS final_profit_percentage FROM order_details WHERE status IN ('PROFIT', 'LOSS') GROUP BY date(timestamp) ORDER BY o
-- SELECT DATE(timestamp) AS order_date, COUNT(*) AS total_trades, ROUND( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2 ) AS total_profit, ROUND( SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2 ) AS total_loss, ROUND( (SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100), 2 ) AS trade_accuracy_percentage, ROUND( (( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END ) )::numeric / 100000.0 * 100), 2 ) AS net_profit_percentage, ROUND((COUNT(*) * 60)::numeric, 2) AS total_charges, ROUND( (( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END ) - (COUNT(*) * 60) )::numeric / 100000.0 * 100), 2 ) AS final_profit_percentage FROM order_details WHERE status IN ('PROFIT', 'LOSS') GROUP BY DATE(timestamp) ORDER BY order_date;
ALTER TABLE order_details ADD COLUMN IF NOT EXISTS metadata_json JSONB NOT NULL DEFAULT '{}';
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS mfi FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS adx FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS rsi FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS vwap FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS upperband FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS middleband FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS lowerband FLOAT DEFAULT 0;
ALTER TABLE order_details ADD COLUMN IF NOT EXISTS qty int default 0;
ALTER TABLE order_details ADD COLUMN IF NOT EXISTS order_ids JSONB NOT NULL DEFAULT '[]';
ALTER TABLE order_details ADD COLUMN IF NOT EXISTS order_status BOOLEAN DEFAULT FALSE;
-- DROP INDEX IF EXISTS unique_stock_timestamp;
-- ALTER TABLE historical_data ADD CONSTRAINT unique_stock_timestamp UNIQUE (stock, timestamp);

CREATE TABLE IF NOT EXISTS capital_market_details (
  epic VARCHAR(255) PRIMARY KEY,
  min_step_distance FLOAT,
  min_step_distance_unit VARCHAR(50),
  min_deal_size FLOAT,
  min_deal_size_unit VARCHAR(50),
  max_deal_size FLOAT,
  max_deal_size_unit VARCHAR(50),
  min_size_increment FLOAT,
  min_size_increment_unit VARCHAR(50),
  min_guaranteed_stop_distance FLOAT,
  min_guaranteed_stop_distance_unit VARCHAR(50),
  min_stop_or_profit_distance FLOAT,
  min_stop_or_profit_distance_unit VARCHAR(50),
  max_stop_or_profit_distance FLOAT,
  max_stop_or_profit_distance_unit VARCHAR(50),
  decimal_places INT,
  margin_factor FLOAT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_capital_market_details_updated_at
BEFORE UPDATE ON capital_market_details
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- For support_resistance_levels index
DROP INDEX IF EXISTS idx_stock_start_date_end_date;
CREATE INDEX IF NOT EXISTS idx_stock_start_date_end_date 
    ON support_resistance_levels (stock, start_date, end_date);

CREATE TABLE upstox_log (
    id SERIAL PRIMARY KEY,                         
    endpoint VARCHAR(255) NOT NULL,               
    request_payload JSON NOT NULL,                
    response_payload JSON NOT NULL,               
    status_code INT NOT NULL,                     
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

CREATE TABLE index_indicators_data (
  id SERIAL PRIMARY KEY, 
  index_symbol VARCHAR(255) NULL, 
  index_name VARCHAR(255) NULL, 
  timestamp TIMESTAMP NULL, 
  rsi FLOAT NULL, 
  adx FLOAT NULL, 
  mfi FLOAT NULL, 
  active BOOLEAN DEFAULT FALSE, 
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE index_indicators_data ADD CONSTRAINT unique_index_symbol_name UNIQUE (index_symbol, index_name);
ALTER TABLE order_details ADD COLUMN entry_price REAL NOT NULL DEFAULT 0;
ALTER TABLE order_details ADD COLUMN exit_price REAL NOT NULL DEFAULT 0;
CREATE TABLE historical_data ( id SERIAL, ltp DOUBLE PRECISION, "timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL, open DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION, close DOUBLE PRECISION, volume BIGINT, open_interest BIGINT, stock VARCHAR(255) NOT NULL );
ALTER TABLE order_details ADD COLUMN stock_name TEXT NULL DEFAULT NULL;

-- SELECT date(timestamp) AS order_date, COUNT() AS total_trades, ROUND(SUM( CASE WHEN status = 'PROFIT' THEN (exit_price - entry_price) * (100000 / entry_price) ELSE 0 END )::numeric, 2) AS total_profit, ROUND(SUM( CASE WHEN status = 'LOSS' THEN (entry_price - exit_price) * (100000 / entry_price) ELSE 0 END )::numeric, 2) AS total_loss, ROUND( (SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END)::numeric / COUNT() * 100), 2 ) AS trade_accuracy_percentage, ROUND( ( (SUM(CASE WHEN status = 'PROFIT' THEN (exit_price - entry_price) * (100000 / entry_price) ELSE 0 END) - SUM(CASE WHEN status = 'LOSS' THEN (entry_price - exit_price) * (100000 / entry_price) ELSE 0 END) )::numeric / 100000.0 * 100 ), 2 ) AS net_profit_percentage, ROUND((COUNT() * 60)::numeric, 2) AS total_charges, ROUND( ( ( SUM(CASE WHEN status = 'PROFIT' THEN (exit_price - entry_price) * (100000 / entry_price) ELSE 0 END) - SUM(CASE WHEN status = 'LOSS' THEN (entry_price - exit_price) * (100000 / entry_price) ELSE 0 END) ) - (COUNT() * 60) )::numeric / 100000.0 * 100, 2 ) AS final_profit_percentage FROM order_details WHERE status IN ('PROFIT', 'LOSS') GROUP BY date(timestamp) ORDER BY order_date;
-- SELECT date(timestamp) AS order_date, COUNT() AS total_trades, ROUND(SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2) AS total_profit, ROUND(SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2) AS total_loss, ROUND((SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END)::numeric / COUNT() * 100), 2) AS trade_accuracy_percentage, ROUND(((SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ))::numeric / 100000.0 * 100), 2) AS net_profit_percentage, ROUND((COUNT() * 60)::numeric, 2) AS total_charges, ROUND(((SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - (COUNT() * 60))::numeric / 100000.0 * 100), 2) AS final_profit_percentage FROM order_details WHERE status IN ('PROFIT', 'LOSS') GROUP BY date(timestamp) ORDER BY order_date;
-- SELECT date(timestamp) AS order_date, COUNT() AS total_trades, ROUND(SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_p rice - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END )::numeri c, 2) AS total_profit, ROUND(SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND tra de_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2) AS total_loss, ROUND((SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END)::numeric / COUNT() * 100), 2) AS trade_accuracy_percentage, ROUND(( ( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * ( 100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOS S' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (10000 0 / exit_price) ELSE 0 END ) )::numeric / 100000.0 * 100 ), 2) AS net_profit_percentage, ROUND((COUNT() * 60)::numeric, 2) AS total_charges, ROUND(( ( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exi t_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN stat us = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END ) - (COUNT() * 60) )::numeric / 100000.0 * 100 ), 2) AS final_p rofit_percentage FROM order_details WHERE status IN ('PROFIT', 'LOSS') GROUP BY date(timestamp) ORDER BY order_date;
-- SELECT date(timestamp) AS order_date, COUNT() AS total_trades, ROUND(SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2) AS total_profit, ROUND(SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2) AS total_loss, ROUND((SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0END)::numeric / COUNT() * 100), 2) AS trade_accuracy_percentage, ROUND(( ( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * ( 100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOS S' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END ) )::numeric / 100000.0 * 100 ), 2) AS net_profit_percentage, ROUND((COUNT() * 60)::numeric, 2) AS total_charges, ROUND(( ( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END ) - (COUNT() * 60) )::numeric / 100000.0 * 100 ), 2) AS final_profit_percentage FROM order_details WHERE status IN ('PROFIT', 'LOSS') GROUP BY date(timestamp) ORDER BY o
-- SELECT DATE(timestamp) AS order_date, COUNT() AS total_trades, ROUND( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2 ) AS total_profit, ROUND( SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END )::numeric, 2 ) AS total_loss, ROUND( (SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END)::numeric / COUNT() * 100), 2 ) AS trade_accuracy_percentage, ROUND( (( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END ) )::numeric / 100000.0 * 100), 2 ) AS net_profit_percentage, ROUND((COUNT() * 60)::numeric, 2) AS total_charges, ROUND( (( SUM( CASE WHEN status = 'PROFIT' AND trade_type = 'BUY' THEN (exit_price - entry_price) * (100000 / exit_price) WHEN status = 'PROFIT' AND trade_type = 'SELL' THEN (entry_price - exit_price) * (100000 / exit_price) ELSE 0 END ) - SUM( CASE WHEN status = 'LOSS' AND trade_type = 'BUY' THEN (entry_price - exit_price) * (100000 / exit_price) WHEN status = 'LOSS' AND trade_type = 'SELL' THEN (exit_price - entry_price) * (100000 / exit_price) ELSE 0 END ) - (COUNT() * 60) )::numeric / 100000.0 * 100), 2 ) AS final_profit_percentage FROM order_details WHERE status IN ('PROFIT', 'LOSS') GROUP BY DATE(timestamp) ORDER BY order_date;
ALTER TABLE order_details ADD COLUMN metadata_json JSONB NOT NULL DEFAULT '{}';
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS mfi FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS adx FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS rsi FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS vwap FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS upperband FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS middleband FLOAT DEFAULT 0;
ALTER TABLE historical_data ADD COLUMN IF NOT EXISTS lowerband FLOAT DEFAULT 0;
CREATE INDEX idx_stock_start_date_end_date ON support_resistance_levels (stock, start_date, end_date);
ALTER TABLE order_details ADD COLUMN qty int default 0;
ALTER TABLE order_details ADD COLUMN order_ids JSONB NOT NULL DEFAULT '[]';
ALTER TABLE order_details ADD COLUMN order_status BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS feed_messages (
    id SERIAL PRIMARY KEY,
    message_data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE historical_data ADD CONSTRAINT historical_data_stock_timestamp_key UNIQUE (stock, timestamp);

CREATE TABLE IF NOT EXISTS reversal_data (
    symbol VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    reversal_high FLOAT,
    reversal_low FLOAT,
    reversal_time TIMESTAMP,
    type VARCHAR(10) CHECK (type IN ('stock', 'index')),
    PRIMARY KEY (symbol, date)
);