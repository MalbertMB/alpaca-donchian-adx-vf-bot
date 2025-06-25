"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: strategy_interface.py
Description: 
    This module defines an abstract base class for trading strategies.
    It includes methods for generating entry and exit signals based on market data.

Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-25
Version: 1.0.0
"""


from abc import ABC, abstractmethod
import pandas as pd

class Strategy(ABC):
    """
    Abstract base class for trading strategies.
    """
    @abstractmethod
    def generate_entry_signals(self, data: pd.DataFrame) -> pd.Series:
        """
        Generate trading signals based on the provided data.

        Parameters:
        - data (pd.DataFrame): Input data containing market information.

        Returns:
        - pd.Series: A series of trading signals.
        """
        pass

    @abstractmethod
    def generate_exit_signals(self, data: pd.DataFrame) -> pd.Series:
        """
        Generate exit signals based on the provided data.

        Parameters:
        - data (pd.DataFrame): Input data containing market information.

        Returns:
        - pd.Series: A series of exit signals.
        """
        pass