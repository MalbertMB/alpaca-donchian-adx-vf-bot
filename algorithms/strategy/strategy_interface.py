"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: strategy_interface.py
Description: 
    This module defines an abstract base class for trading strategies.
    It includes methods for generating entry and exit signals based on market data.

Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-25
"""


from abc import ABC, abstractmethod
import pandas as pd

class Strategy(ABC):
    """
    Abstract base class for trading strategies.
    """
    @abstractmethod
    def generate_entry_signal(self, data: pd.DataFrame) -> bool:
        """
        Generates the entry signal for the current date based on the provided data.

        Parameters:
        - data (pd.DataFrame): Input data containing market information.

        Returns:
        - bool: True if entry signal is generated, False otherwise.
        """
        pass

    @abstractmethod
    def generate_exit_signal(self, data: pd.DataFrame) -> bool:
        """
        Generates the exit signal for the current date based on the provided data.

        Parameters:
        - data (pd.DataFrame): Input data containing market information.

        Returns:
        - bool: True if exit signal is generated, False otherwise.
        """
        pass


    # Additional backtesting-specific methods

    @abstractmethod
    def backtest_signals(self, data: pd.DataFrame) -> pd.Series:
        """
        Generates the backtesting signals based on the provided data.

        Parameters:
        - data (pd.DataFrame): Input data containing market information.

        Returns:
        - pd.Series: A series of backtesting signals.
        """
        pass
