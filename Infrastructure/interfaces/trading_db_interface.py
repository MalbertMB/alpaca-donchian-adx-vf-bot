"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: trading_db_interface.py
Description: 
    This module defines an abstract base class for the trading database interface.

Author: Albert Marín Blasco
Date Created: 2025-06-25
Last Modified: 2026-02-20
"""

from abc import ABC, abstractmethod
import pandas as pd
from Domain import Signal, OpenPosition, Trade

class TradingDataBaseInterface(ABC):
    """
    Abstract interface for the trading database.
    Implemented differently for live trading and backtesting.
    """

    def __enter__(self):
        """
        Allows the database manager to be used in a 'with' statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensures proper cleanup of resources when exiting a 'with' block.
        Handles exception arguments passed by Python's context manager.
        """
        self.close()

    @abstractmethod
    def commit(self):
        """
        Commits the current transaction to the database.
        """
        pass
    
    @abstractmethod
    def close(self):
        """
        Closes the database connection. Should be called when the database is no longer needed.
        """
        pass

    @abstractmethod
    def insert_signal(self, signal: Signal) -> int:
        """
        Inserts a signal into the database and returns the generated signal_id.
        Args:
            signal (Signal): The Signal object to be inserted.
        Returns:
            int: The ID of the inserted signal.
        """
        pass

    @abstractmethod
    def insert_open_position(self, open_position: OpenPosition) -> int:
        """
        Inserts an open position into the database and returns the generated open_position_id.
        Args:
            open_position (OpenPosition): The OpenPosition object to be inserted.
        Returns:
            int: The ID of the inserted open position.
        """
        pass

    @abstractmethod
    def close_open_position(self, open_position_id: int, trade: Trade) -> int:
        """
        Closes an open position by inserting a corresponding trade and deleting the open position.
        Args:
            open_position_id (int): The ID of the open position to close.
            trade (Trade): The Trade object representing the closed trade.
        Returns:
            int: The ID of the newly created trade.
        """
        pass

    @abstractmethod
    def get_signals(self, start_date: pd.Timestamp | None = None, end_date: pd.Timestamp | None = None) -> list[Signal]:
        """
        Retrieves signals from the database within the specified date range.
        Args:
            start_date (pd.Timestamp | None): Lower bound for filtering.
            end_date (pd.Timestamp | None): Upper bound for filtering.
        Returns:
            list[Signal]: A list of Signal objects.
        """
        pass

    @abstractmethod
    def get_open_positions(self) -> list[OpenPosition]:
        """
        Retrieves all current open positions.
        Returns:
            list[OpenPosition]: A list of OpenPosition objects.
        """
        pass

    @abstractmethod
    def get_trades(self, start_date: pd.Timestamp | None = None, end_date: pd.Timestamp | None = None) -> list[Trade]:
        """
        Retrieves trades from the database within the specified date range.
        Args:
            start_date (pd.Timestamp | None): Lower bound for filtering.
            end_date (pd.Timestamp | None): Upper bound for filtering.
        Returns:
            list[Trade]: A list of Trade objects.
        """
        pass