"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: positions.py
Description: 
    Defines data structures for active positions and completed trades.
Author: Albert Marín
Date Created: 2025-11-22
"""

from dataclasses import dataclass
import pandas as pd
from .common import Direction, QuantityType

@dataclass
class OpenPosition:
    """
    Class representing an active trading position.
    Attributes:
    - stock (str): The stock ticker.
    - direction (Direction): Long or Short.
    - date (pd.Timestamp): Date/time position was opened.
    - entry_price (float): Price at which position was entered.
    - quantity_type (QuantityType): Method used to size the position.
    - quantity (float): Number of shares/contracts (Float for fractional support).
    - entry_signal_id (int): ID of the signal that triggered this position.
    - open_position_id (int | None): Unique identifier assigned by database.
    - run_id (int | None): Identifier for the backtest or live run this position belongs to.
    """
    stock: str
    direction: Direction
    date: pd.Timestamp
    entry_price: float
    quantity_type: QuantityType
    quantity: float  # Changed to float for fractional shares
    entry_signal_id: int
    open_position_id: int | None = None
    run_id: int | None = None  # Added to link to backtest/live run


@dataclass
class Trade:
    """
    Class representing a completed trade (Round trip).
    Attributes:
    - stock (str): The stock ticker.
    - direction (Direction): Long or Short.
    - quantity_type (QuantityType): Method used to size the position.
    - quantity (float): Number of shares/contracts traded.
    - entry_price (float): Average entry price.
    - exit_price (float): Average exit price.
    - entry_date (pd.Timestamp): Date/time entered.
    - exit_date (pd.Timestamp): Date/time exited.
    - gross_result (float): Raw PnL (Price diff * quantity).
    - commission (float): Total fees/commissions for the trade.
    - net_result (float): gross_result - commission.
    - entry_signal_id (int): ID of entry signal.
    - exit_signal_id (int): ID of exit signal.
    - trade_id (int | None): Unique identifier assigned by database.
    - run_id (int | None): Identifier for the backtest or live run this trade belongs to.
    """
    stock: str
    direction: Direction
    quantity_type: QuantityType
    quantity: float
    entry_price: float
    exit_price: float
    entry_date: pd.Timestamp
    exit_date: pd.Timestamp
    gross_result: float
    commission: float 
    net_result: float
    entry_signal_id: int
    exit_signal_id: int
    trade_id: int | None = None
    run_id: int | None = None  # Added to link to backtest/live run

    def __post_init__(self):
        """
        Auto-calculate results if they are not explicitly provided.
        Useful for backtesting where commission might be estimated.
        """
        # Ensure quantity is positive
        self.quantity = abs(self.quantity)
        # Calculate Gross Result if missing
        if self.gross_result is None or self.gross_result == 0.0:
            multiplier = 1 if self.direction == Direction.LONG else -1
            self.gross_result = (self.exit_price - self.entry_price) * self.quantity * multiplier
        
        # Calculate Net Result if missing
        if self.net_result is None or self.net_result == 0.0:
            self.net_result = self.gross_result - (self.commission if self.commission else 0.0)