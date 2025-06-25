"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: database_interface.py
Description: 
    This module defines an abstract base class for database operations.
    It includes methods for connecting to the database, creating tables,
    inserting OHLCV data, retrieving OHLCV data, inserting trades, and retrieving open trades.

Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-25
Version: 1.0.0
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
    def insert_trade(self, trade: Dict):
        """Store a trade from Alpaca."""
        pass

    @abstractmethod
    def get_open_trades(self) -> List[Dict]:
        """Retrieve all currently open trades."""
        pass
