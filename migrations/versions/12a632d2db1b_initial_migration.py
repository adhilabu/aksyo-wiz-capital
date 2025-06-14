"""initial_migration

Revision ID: 12a632d2db1b
Revises: 
Create Date: 2024-03-19 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '12a632d2db1b'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create feed_data_ltpc table
    op.create_table('feed_data_ltpc',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('instrument_code', sa.String(length=255), nullable=False),
        sa.Column('ltp', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column('ltt', sa.DateTime(), nullable=True),
        sa.Column('ltq', sa.Integer(), nullable=True),
        sa.Column('cp', sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column('current_ts', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('instrument_code')
    )

    # Create feed_data_option_chain table
    op.create_table('feed_data_option_chain',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('symbol', sa.String(length=50), nullable=False),
        sa.Column('market_ltp', sa.Numeric(), nullable=True),
        sa.Column('market_ltt', sa.BigInteger(), nullable=True),
        sa.Column('market_ltq', sa.Integer(), nullable=True),
        sa.Column('market_cp', sa.Numeric(), nullable=True),
        sa.Column('bidAskQuote', postgresql.JSONB(), nullable=True),
        sa.Column('op', sa.Numeric(), nullable=True),
        sa.Column('up', sa.Numeric(), nullable=True),
        sa.Column('iv', sa.Numeric(), nullable=True),
        sa.Column('delta', sa.Numeric(), nullable=True),
        sa.Column('theta', sa.Numeric(), nullable=True),
        sa.Column('gamma', sa.Numeric(), nullable=True),
        sa.Column('vega', sa.Numeric(), nullable=True),
        sa.Column('rho', sa.Numeric(), nullable=True),
        sa.Column('marketOHLC', postgresql.JSONB(), nullable=True),
        sa.Column('atp', sa.Numeric(), nullable=True),
        sa.Column('cp_eFeed', sa.Numeric(), nullable=True),
        sa.Column('vtt', sa.BigInteger(), nullable=True),
        sa.Column('oi', sa.Integer(), nullable=True),
        sa.Column('tbq', sa.Integer(), nullable=True),
        sa.Column('tsq', sa.Integer(), nullable=True),
        sa.Column('lc', sa.Numeric(), nullable=True),
        sa.Column('uc', sa.Numeric(), nullable=True),
        sa.Column('fp', sa.Numeric(), nullable=True),
        sa.Column('fv', sa.Integer(), nullable=True),
        sa.Column('dh_oi', sa.Integer(), nullable=True),
        sa.Column('dl_oi', sa.Integer(), nullable=True),
        sa.Column('poi', sa.Integer(), nullable=True),
        sa.Column('current_ts', sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Create stock_data table
    op.create_table('stock_data',
        sa.Column('stock_symbol', sa.Text(), nullable=False),
        sa.Column('close', sa.Float(), nullable=False),
        sa.Column('volume', sa.Float(), nullable=False),
        sa.Column('high', sa.Float(), nullable=False),
        sa.Column('low', sa.Float(), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.PrimaryKeyConstraint('stock_symbol', 'timestamp')
    )

    # Create support_resistance_levels table
    op.create_table('support_resistance_levels',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('stock', sa.Text(), nullable=False),
        sa.Column('start_date', sa.Date(), nullable=False),
        sa.Column('end_date', sa.Date(), nullable=False),
        sa.Column('pivot_data', postgresql.JSONB(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('stock', 'start_date', 'end_date', name='unique_stock_date')
    )

    # Create order_details table
    op.create_table('order_details',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('stock', sa.Text(), nullable=False),
        sa.Column('ltp', sa.Float(), nullable=False),
        sa.Column('sl', sa.Float(), nullable=False),
        sa.Column('pl', sa.Float(), nullable=False),
        sa.Column('cp', sa.Float(), nullable=False),
        sa.Column('broke_resistance_level', sa.Float(), nullable=False),
        sa.Column('rsi', sa.Float(), nullable=False),
        sa.Column('adx', sa.Float(), nullable=False),
        sa.Column('mfi', sa.Float(), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('status', sa.Text(), server_default='OPEN', nullable=True),
        sa.Column('trade_type', sa.Text(), server_default='BUY', nullable=True),
        sa.Column('entry_price', sa.Float(), server_default='0', nullable=False),
        sa.Column('exit_price', sa.Float(), server_default='0', nullable=False),
        sa.Column('stock_name', sa.Text(), nullable=True),
        sa.Column('metadata_json', postgresql.JSONB(), server_default='{}', nullable=False),
        sa.Column('qty', sa.Integer(), server_default='0', nullable=True),
        sa.Column('order_ids', postgresql.JSONB(), server_default='[]', nullable=False),
        sa.Column('order_status', sa.Boolean(), server_default='false', nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Create capital_log table
    op.create_table('capital_log',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('endpoint', sa.String(length=255), nullable=False),
        sa.Column('request_payload', postgresql.JSON(), nullable=False),
        sa.Column('response_payload', postgresql.JSON(), nullable=False),
        sa.Column('status_code', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Create index_indicators_data table
    op.create_table('index_indicators_data',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('index_symbol', sa.String(length=255), nullable=True),
        sa.Column('index_name', sa.String(length=255), nullable=True),
        sa.Column('timestamp', sa.DateTime(), nullable=True),
        sa.Column('rsi', sa.Float(), nullable=True),
        sa.Column('adx', sa.Float(), nullable=True),
        sa.Column('mfi', sa.Float(), nullable=True),
        sa.Column('active', sa.Boolean(), server_default='false', nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('index_symbol', 'index_name', name='unique_index_symbol_name')
    )

    # Create historical_data table
    op.create_table('historical_data',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('ltp', sa.Float(), nullable=True),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('open', sa.Float(), nullable=True),
        sa.Column('high', sa.Float(), nullable=True),
        sa.Column('low', sa.Float(), nullable=True),
        sa.Column('close', sa.Float(), nullable=True),
        sa.Column('volume', sa.BigInteger(), nullable=True),
        sa.Column('open_interest', sa.BigInteger(), nullable=True),
        sa.Column('stock', sa.String(length=255), nullable=False),
        sa.Column('mfi', sa.Float(), server_default='0', nullable=True),
        sa.Column('adx', sa.Float(), server_default='0', nullable=True),
        sa.Column('rsi', sa.Float(), server_default='0', nullable=True),
        sa.Column('vwap', sa.Float(), server_default='0', nullable=True),
        sa.Column('upperband', sa.Float(), server_default='0', nullable=True),
        sa.Column('middleband', sa.Float(), server_default='0', nullable=True),
        sa.Column('lowerband', sa.Float(), server_default='0', nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('stock', 'timestamp', name='historical_data_stock_timestamp_key')
    )

    # Create capital_market_details table
    op.create_table('capital_market_details',
        sa.Column('epic', sa.String(length=255), nullable=False),
        sa.Column('min_step_distance', sa.Float(), nullable=True),
        sa.Column('min_step_distance_unit', sa.String(length=50), nullable=True),
        sa.Column('min_deal_size', sa.Float(), nullable=True),
        sa.Column('min_deal_size_unit', sa.String(length=50), nullable=True),
        sa.Column('max_deal_size', sa.Float(), nullable=True),
        sa.Column('max_deal_size_unit', sa.String(length=50), nullable=True),
        sa.Column('min_size_increment', sa.Float(), nullable=True),
        sa.Column('min_size_increment_unit', sa.String(length=50), nullable=True),
        sa.Column('min_guaranteed_stop_distance', sa.Float(), nullable=True),
        sa.Column('min_guaranteed_stop_distance_unit', sa.String(length=50), nullable=True),
        sa.Column('min_stop_or_profit_distance', sa.Float(), nullable=True),
        sa.Column('min_stop_or_profit_distance_unit', sa.String(length=50), nullable=True),
        sa.Column('max_stop_or_profit_distance', sa.Float(), nullable=True),
        sa.Column('max_stop_or_profit_distance_unit', sa.String(length=50), nullable=True),
        sa.Column('decimal_places', sa.Integer(), nullable=True),
        sa.Column('margin_factor', sa.Float(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('epic')
    )

    # Create upstox_log table
    op.create_table('upstox_log',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('endpoint', sa.String(length=255), nullable=False),
        sa.Column('request_payload', postgresql.JSON(), nullable=False),
        sa.Column('response_payload', postgresql.JSON(), nullable=False),
        sa.Column('status_code', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Create feed_messages table
    op.create_table('feed_messages',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('message_data', postgresql.JSONB(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Create reversal_data table
    op.create_table('reversal_data',
        sa.Column('symbol', sa.String(length=100), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('reversal_high', sa.Float(), nullable=True),
        sa.Column('reversal_low', sa.Float(), nullable=True),
        sa.Column('reversal_time', sa.DateTime(), nullable=True),
        sa.Column('type', sa.String(length=10), nullable=True),
        sa.PrimaryKeyConstraint('symbol', 'date'),
        sa.CheckConstraint("type IN ('stock', 'index')")
    )

    # Create trigger function for updating updated_at
    op.execute("""
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ language 'plpgsql';
    """)

    # Create trigger for capital_market_details
    op.execute("""
    CREATE TRIGGER update_capital_market_details_updated_at
    BEFORE UPDATE ON capital_market_details
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
    """)

    # Create index for support_resistance_levels
    op.create_index('idx_stock_start_date_end_date', 'support_resistance_levels', ['stock', 'start_date', 'end_date'])


def downgrade() -> None:
    # Drop all tables in reverse order
    op.drop_table('reversal_data')
    op.drop_table('feed_messages')
    op.drop_table('upstox_log')
    op.drop_table('capital_market_details')
    op.drop_table('historical_data')
    op.drop_table('index_indicators_data')
    op.drop_table('capital_log')
    op.drop_table('order_details')
    op.drop_table('support_resistance_levels')
    op.drop_table('stock_data')
    op.drop_table('feed_data_option_chain')
    op.drop_table('feed_data_ltpc')
    
    # Drop trigger function
    op.execute('DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;')
