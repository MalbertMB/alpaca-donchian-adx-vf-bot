"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: database_interface.py
Description: 
    This module defines an abstract base class for database operations.
    It includes methods for connecting to the database, creating tables,
    inserting OHLCV data, retrieving OHLCV data, inserting trades, and retrieving open trades.

Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-29 
"""


from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Optional

class DatabaseInterface(ABC):
    @abstractmethod
    def connect(self):
        """Establish a database connection."""
        pass

    @abstractmethod
    def create_tables(self):
        """Create necessary tables if they don't exist."""
        pass

    @abstractmethod
    def insert_ohlcv_data(self, symbol: str, data: List[Dict]):
        """Insert OHLCV data for a given symbol."""
        pass

    @abstractmethod
    def get_ohlcv_data(self, symbol: str, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Retrieve OHLCV data for a symbol within a date range."""
        pass

    @abstractmethod
    def has_ohlcv_data(self, symbol: str, start_date: datetime, end_date: datetime) -> bool:
        """Check if OHLCV data exists for a symbol within a date range."""
        pass

    @abstractmethod
    def insert_open_trade_backtest(self, position: Dict):
        """Insert an open trade record into the database for backtesting purposes."""
        pass

    @abstractmethod
    def insert_close_trade_backtest(self, trade: Dict):
        """Insert a closed trade record into the database for backtesting purposes."""
        pass

    @abstractmethod
    def get_open_trades_backtest(self) -> List[Dict]:
        """Retrieve all open trades from the database for backtesting purposes."""
        pass

    @abstractmethod
    def get_closed_trades_backtest(self) -> List[Dict]:
        """Retrieve all closed trades from the database for backtesting purposes."""
        pass

    @abstractmethod
    def delete_open_trade_backtest(self, position: Dict):
        """Delete an open trade record from the database for backtesting purposes."""
        pass

    @abstractmethod
    def insert_dow_jones_tickers(self, tickers: List[str]):
        """Insert a list of tickers into the database."""
        pass

    @abstractmethod
    def get_dow_jones_tickers(self) -> List[str]:
        """Retrieve all Dow Jones tickers from the database."""
        pass

    @abstractmethod
    def insert_sp500_tickers(self, tickers: List[str]):
        """Insert a list of S&P 500 tickers into the database."""
        pass

    @abstractmethod
    def get_sp500_tickers(self) -> List[str]:
        """Retrieve all S&P 500 tickers from the database."""
        pass

    @abstractmethod
    def populate_stock_calendar(self, calendar: List[datetime]):
        """Populates the calendar table with a list of Calendar objects from Alpaca."""
        pass

    @abstractmethod
    def clear_database(self):
        """Clears the database by dropping all tables."""
        pass

    @abstractmethod
    def clear_backtest_tables(self):
        """Clears the backtest tables by dropping them."""
        pass