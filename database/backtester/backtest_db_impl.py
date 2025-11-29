"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: .py
Description: 
    
Author: Albert Marín
Date Created: 2025-11-25
Last Modified: 2025-11-29
"""

import sqlite3
from ..interfaces import TradingDatabaseInterface
from domain import Trade, Position


class BacktestDataManager(TradingDatabaseInterface):

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path)
        self._init_tables()

    def _init_tables(self):
        """Initialize database tables if they do not exist."""
        cursor = self.connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id INTEGER,
                exit_price REAL,
                exit_date TEXT,
                profit_loss REAL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock TEXT,
                position_type TEXT,
                entry_price REAL,
                quantity_type TEXT,
                quantity INTEGER
            )
        """)
        self.connection.commit()

    def save_trade(self, trade: Trade) -> None:
        """Store trade information."""
        pass

    def save_position(self, position: Position) -> None:
        """Store position information."""
        pass

    def get_positions(self) -> list[Position]:
        """Return a list of open positions."""
        return []