"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: database_interface.py
Description: 
    This module defines an abstract base class for the trading database interface.


Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-29 
"""


from abc import ABC, abstractmethod
from Domain.objects import Position, Trade

class TradingDatabaseInterface(ABC):
    """
    Abstract interface for the trading database.
    Implemented differently for live trading and backtesting.
    """

    @abstractmethod
    def save_trade(self, trade: Trade) -> None:
        """Store trade information."""
        pass

    @abstractmethod
    def save_position(self, position: Position) -> None:
        """Store position information."""
        pass

    @abstractmethod
    def get_positions(self) -> list[Position]:
        """Return a list of open positions."""
        pass