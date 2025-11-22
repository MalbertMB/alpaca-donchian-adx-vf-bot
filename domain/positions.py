"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: positions.py
Description: 
    This module defines data structures and classes related to trading positions.
Author: Albert Marín
Date Created: 2025-11-22
"""

from dataclasses import dataclass
from enum import Enum
import pandas as pd

class PositionType(Enum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"  # No position

class PositionQuantity(Enum):
    SHARES = "shares"
    CAPITAL = "capital"


@dataclass
class TradingPosition:
    """
    Class representing a trading position.
    Attributes:
    - stock (str): The stock ticker for which the position is held.
    - position_type (PositionType): The type of trading position.
    - entry_price (float): The price at which the position was entered.
    - quantity (int): The number of shares/contracts held in the position.
    """
    stock: str
    position_type: PositionType
    date: pd.Timestamp
    entry_price: float
    quantity_type: PositionQuantity
    quantity: int