"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: database_interface.py
Description: 
    This module defines an abstract base class for the trading database interface.


Author: Albert Marín Blasco
Date Created: 2025-06-25
Last Modified: 2026-02-19
"""


from abc import ABC, abstractmethod
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

    def __exit__(self):
        """
        Ensures proper cleanup of resources when exiting a 'with' block.
        """
        self.close()

    @abstractmethod
    def commit(self):
        """
        Commits the current transaction to the database. This method is particularly useful for backtesting scenarios where multiple operations can be batched together before committing.
        """
        pass
    
    @abstractmethod
    def close(self):
        """
        Closes the database connection. Should be called when the database is no longer needed to free up resources.
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
            run_id (int): The ID of the backtest or live run this position belongs to.
            position (OpenPosition): The OpenPosition object to be inserted.
        Returns:
            int: The ID of the inserted open position.
        """
        pass

    @abstractmethod
    def close_open_position(self, open_position_id: int, trade: Trade) -> int:
        """
        Closes an open position by inserting a corresponding trade and deleting the open position.
        This method handles the entire transaction, including committing changes. If any part of the process fails, it will roll back to maintain data integrity.
        Args:
            run_id (int): The ID of the backtest or live run this position belongs to.
            open_position_id (int): The ID of the open position to close.
            trade (Trade): The Trade object representing the closed trade.
        Returns:
            int: The ID of the newly created trade.
        """
        pass
    