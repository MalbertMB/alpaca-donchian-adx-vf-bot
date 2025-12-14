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
from .signals import Signal

class Direction(Enum):
    LONG = "long"
    SHORT = "short"

class QuantityType(Enum):
    SHARES = "shares"
    CAPITAL = "capital"


@dataclass
class OpenPosition:
    """
    Class representing a trading position.
    Attributes:
    - stock (str): The stock ticker for which the position is held.
    - position_type (PositionType): The type of trading position.
    - date (pd.Timestamp): Date and time where the position was oppened.
    - entry_price (float): The price at which the position was entered.
    - quantity_type (PositionQuantity): Type of quantity used (capital or shares). 
    - quantity (int): The number of shares/contracts held in the position.
    - entry_signal_id (int): The identifier of the signal that generated the position.
    - id (int): Unique identifier for the position, assigned by the database.
    """
    stock: str
    direction: Direction
    date: pd.Timestamp
    entry_price: float
    quantity_type: QuantityType
    quantity: int
    entry_signal_id: int
    id: int | None = None



@dataclass
class Trade:
    """
    Class representing a completed trade.
    Attributes:
    - stock (str): The stock ticker for which the trade was made.
    - direction (Direction): The direction of the trade (long or short).
    - quantity_type (QuantityType): Type of quantity used (capital or shares).
    - quantity (int): The number of shares/contracts traded.
    - entry_price (float): The price at which the position was entered.
    - exit_price (float): The price at which the position was exited.
    - entry_date (pd.Timestamp): Date and time where the position was entered.
    - exit_date (pd.Timestamp): Date and time where the position was exited.
    - result (float): The profit or loss from the trade.
    - entry_signal_id (int): The identifier of the signal that generated the entry.
    - exit_signal_id (int): The identifier of the signal that generated the exit.
    - id (int): Unique identifier for the trade, assigned by the database.
    """
    stock: str
    direction: Direction
    quantity_type: QuantityType
    quantity: int
    entry_price: float
    exit_price: float
    entry_date: pd.Timestamp
    exit_date: pd.Timestamp
    result: float
    entry_signal_id: int
    exit_signal_id: int
    id: int | None = None