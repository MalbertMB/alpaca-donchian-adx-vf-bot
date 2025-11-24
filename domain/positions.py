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

class PositionType(Enum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"  # No position

class PositionQuantity(Enum):
    SHARES = "shares"
    CAPITAL = "capital"


@dataclass
class Position:
    """
    Class representing a trading position.
    Attributes:
    - id (int): Unique identifier for the position.
    - stock (str): The stock ticker for which the position is held.
    - position_type (PositionType): The type of trading position.
    - entry_price (float): The price at which the position was entered.
    - quantity (int): The number of shares/contracts held in the position.
    """
    id: int | None = None # ID assigned by the database
    stock: str
    position_type: PositionType
    date: pd.Timestamp
    entry_price: float
    quantity_type: PositionQuantity
    quantity: int
    signal: Signal



@dataclass
class Trade:
    """
    Class representing a completed trade.
    Attributes:
    - position (TradingPosition): The trading position associated with the trade.
    - exit_price (float): The price at which the position was exited.
    - exit_date (pd.Timestamp): The date when the position was closed.
    - profit_loss (float): The profit or loss from the trade.
    """
    position: Position
    exit_price: float
    exit_date: pd.Timestamp
    profit_loss: float