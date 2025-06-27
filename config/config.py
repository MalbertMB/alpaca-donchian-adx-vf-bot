"""
Loads .env variables using python-dotenv

Central place for global config like:

API keys

Symbols

Date ranges

Trading mode ("backtest" or "live")
"""

from dotenv import load_dotenv
import os

# Load variables from .env file into environment
load_dotenv()

# Access variables
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_PAPER_URL = os.getenv("ALPACA_PAPER_URL")