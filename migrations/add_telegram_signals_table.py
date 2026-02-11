"""
Database migration: Add telegram_signals table

This table stores all trading signals received from Telegram,
along with their processing status and results.
"""
from datetime import datetime


def upgrade(cursor):
    """Create telegram_signals table."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telegram_signals (
            id SERIAL PRIMARY KEY,
            instrument VARCHAR(50) NOT NULL,
            direction VARCHAR(10) NOT NULL,
            entry_price DECIMAL(10, 2) NOT NULL,
            stop_loss DECIMAL(10, 2) NOT NULL,
            target_1 DECIMAL(10, 2) NOT NULL,
            target_2 DECIMAL(10, 2) NOT NULL,
            confidence INT NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            received_at TIMESTAMP NOT NULL DEFAULT NOW(),
            openai_analysis JSONB,
            executed BOOLEAN DEFAULT FALSE,
            deal_reference VARCHAR(100),
            rejection_reason TEXT,
            raw_message TEXT,
            status VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        
        -- Create indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_telegram_signals_instrument 
            ON telegram_signals(instrument);
        
        CREATE INDEX IF NOT EXISTS idx_telegram_signals_status 
            ON telegram_signals(status);
        
        CREATE INDEX IF NOT EXISTS idx_telegram_signals_executed 
            ON telegram_signals(executed);
        
        CREATE INDEX IF NOT EXISTS idx_telegram_signals_received_at 
            ON telegram_signals(received_at DESC);
        
        -- Add comment
        COMMENT ON TABLE telegram_signals IS 'Trading signals received from Telegram channels';
    """)
    print("Created telegram_signals table")


def downgrade(cursor):
    """Drop telegram_signals table."""
    cursor.execute("DROP TABLE IF EXISTS telegram_signals CASCADE;")
    print("Dropped telegram_signals table")


if __name__ == "__main__":
    """Run migration manually."""
    import os
    import psycopg
    from dotenv import load_dotenv
    
    load_dotenv()
    
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in environment")
    
    print("Connecting to database...")
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cursor:
            print("Running upgrade...")
            upgrade(cursor)
            conn.commit()
            print("Migration completed successfully!")
