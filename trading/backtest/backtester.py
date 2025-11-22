"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: backtester.py
Description: 
    This module simulates trades using historical data from the database.
    It evaluates performance metrics based on the simulated trades.

Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-29
"""

import os
import pandas as pd
from data import DataManager
from algorithms import Strategy
from datetime import datetime
from trading.live_trading import Trader
from config import OUTPUT_DIR


class Backtester(Trader):
    def __init__(self, manager: DataManager, strategy: Strategy):
        self.manager = manager
        self.strategy = strategy
        
    def run(self, group: str, start_date: datetime, end_date: datetime) -> None:
        """
        Runs the backtesting simulation for a given group of stocks.
        Args:
            group (str): The group of stocks to backtest.
            start_date (datetime): The start date for the backtest.
            end_date (datetime): The end date for the backtest.
        """
        # Load historical data
        symbols = self.manager.get_symbols_by_group(group)
        print(f"Running backtest for group: {group} from {start_date} to {end_date}")
        print(f"Symbols in group: {symbols}")
        for symbol in symbols:
            ohlcv_data = self.manager.get_ohlcv_data(symbol, start_date, end_date)
            if not ohlcv_data:
                print(f"No data for {symbol} in the specified date range.")
                continue
            
            # Simulate trades based on some strategy
            self.run_strategy(symbol, ohlcv_data, start_date, end_date)

            # Save data and trades to results directory
            self.save_results(symbol, ohlcv_data)

    def run_strategy(self, symbol: str, ohlcv_data: list, start_date: datetime, end_date: datetime) -> None:
        """
        Simulates trades based on the provided strategy and OHLCV data.
        Args:
            symbol (str): The stock symbol.
            ohlcv_data (list): List of OHLCV data points.
        """

        return None # Placeholder for strategy execution logic

        

    def save_results(self, symbol: str, ohlcv_data: list) -> None:
        """
        Saves the OHLCV data and simulated trades to the results directory.
        Args:
            symbol (str): The stock symbol.
            ohlcv_data (list): List of OHLCV data points.
        """

        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
        
        # Select relevant columns and convert to DataFrame, then save to CSV

    def get_balance(self, group: str) -> float:
        """
        Returns the simulated account balance for the backtest.
        Args:
            group (str): The group of stocks to check balance for.
        Returns:
            float: The simulated account balance.
        """
        # Placeholder for balance calculation logic
        # Implement your logic to calculate the balance based on simulated trades
        return 10000.0  # Return a dummy balance for now
