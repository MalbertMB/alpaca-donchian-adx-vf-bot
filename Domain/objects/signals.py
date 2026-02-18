"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: signals.py
Description: 
    Defines the Signal data class representing trading opportunities.
Author: Albert Marín
Date Created: 2026-02-18
"""

from dataclasses import dataclass
import pandas as pd
from .common import SignalType, Direction

@dataclass
class Signal:
    """
    Class representing a trading signal with associated metadata.
    Attributes:
    - stock (str): The stock ticker for which the signal is generated.
    - signal (SignalType): The type of trading signal (ENTRY, EXIT, etc.).
    - direction (Direction): The intended direction (LONG/SHORT).
    - date (pd.Timestamp): The date and time where the signal was generated.
    - price (float): The price at which the signal was generated.
    - confidence (float): Confidence level of the signal (default -1).
    - reason (str): Description of why the signal was generated.
    - id (int | None): Unique identifier assigned by database.
    """
    stock: str
    signal: SignalType
    direction: Direction  # Added to specify Long/Short intent
    date: pd.Timestamp
    price: float
    confidence: float = -1
    reason: str = ""
    id: int | None = None