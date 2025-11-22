"""
Abstract trader interface:

class Trader(ABC):
    @abstractmethod
    async def run(self):
        pass
"""

from abc import ABC, abstractmethod
from datetime import datetime

class Trader(ABC):

    @abstractmethod
    def run(self, group: str, start_date: datetime, end_date: datetime) -> None:
        """Run the trader interface."""
        pass

    @abstractmethod
    def get_balance(self, group: str) -> float:
        """Get the current account balance."""
        pass

    """ More methods may be needed """