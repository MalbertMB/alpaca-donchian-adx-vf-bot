# databases/market/market_data_manager.py

from .market_database import MarketDatabase
from database.interfaces import TradingDatabaseInterface


class MarketDataManager:
    """
    Shared manager used by both live trader and backtester.
    Reads market data and writes signals/trades to the injected database.
    """

    def __init__(self, market_db: MarketDatabase, trade_db: TradingDatabaseInterface):
        self.market_db = market_db
        self.trade_db = trade_db

    def get_candles(self, symbol: str, start: str, end: str):
        return self.market_db.get_candles(symbol, start, end)

    def save_trade(self, trade):
        self.trade_db.save_trade(trade)

    def save_signal(self, signal):
        self.trade_db.save_signal(signal)

    def get_positions(self):
        return self.trade_db.get_open_positions()

    def close_position(self, position_id: int):
        self.trade_db.close_position(position_id)
