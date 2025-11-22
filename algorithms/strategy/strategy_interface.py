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
from domain import TradingSignal

class Strategy(ABC):
    """
    Abstract base class for trading strategies.
    """
    @abstractmethod
    def generate_signal(self, data: pd.DataFrame) -> TradingSignal:
        """
        Generates a trading signal for the current date.
        """
        pass


    # Additional backtesting-specific methods

    @abstractmethod
    def generate_backtest_signals(self, data: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        """
        Generates entry and exit signals for backtesting based on the provided data.
        Parameters:
        - data (pd.DataFrame): Input data containing market information.
        Returns:
        - tuple[pd.Series, pd.Series]: A tuple containing two pandas Series:
            - entry_signals: pd.Series(bool)
            - exit_signals:  pd.Series(bool)
        """
        pass
