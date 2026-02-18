"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: common.py
Description: 
    Shared enumerations and constants used across the domain layer to avoid 
    circular dependencies.
Author: Albert Marín
Date Created: 2026-02-18
"""

from enum import Enum

class Direction(Enum):
    LONG = "long"
    SHORT = "short"

class SignalType(Enum):
    NONE = "none"
    ENTRY = "entry"
    EXIT = "exit"
    REVERSE = "reverse"      # Implies closing current position and opening opposite
    ERROR = "error"          # Invalid data or computation failure

class QuantityType(Enum):
    SHARES = "shares"
    CAPITAL = "capital"