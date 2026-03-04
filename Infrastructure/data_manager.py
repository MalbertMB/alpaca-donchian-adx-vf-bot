"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: data_manager.py
Description: 
    This module implements the DataManager class, which provides the logic that unifies the interactions between
    the local database, market database, and the Alpaca API.
    
Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-29
"""


from .interfaces import TradingDataBaseInterface
from .api import AlpacaAPI
from .market import MarketDatabase

class DataManager:

    def __init__(self, local_db: TradingDataBaseInterface, market_db: MarketDatabase, alpaca_api: AlpacaAPI):
        self.local_db = local_db
        self.market_db = market_db
        self.alpaca_api = alpaca_api


    