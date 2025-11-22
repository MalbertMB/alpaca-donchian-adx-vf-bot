"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: signals.py
Description: 
    This module defines trading signal types and a class to represent trading signals
    with associated metadata such as confidence and reason.
Author: Albert Marín
Date Created: 2025-11-22
"""


from dataclasses import dataclass
from enum import Enum
import pandas as pd

class SignalType(Enum):
    NONE = "none"
    ENTRY = "entry"
    EXIT = "exit"
    REVERSE = "reverse"      # To flip positions
    ERROR = "error"          # Invalid data or computation failure

@dataclass
class TradingSignal:
    """
    Class representing a trading signal with associated metadata.
    Attributes:
    - stock (str): The stock ticker for which the signal is generated.
    - signal (SignalType): The type of trading signal.
    - confidence (float): Confidence level of the signal (default is -1, unknown).
    - reason (str): Description of why the signal was generated.
    - date (pd.Timestamp): The date and time where the signal was generated.
    """
    stock: str
    signal: SignalType
    date: pd.Timestamp
    price: float
    confidence: float = -1
    reason: str = ""
