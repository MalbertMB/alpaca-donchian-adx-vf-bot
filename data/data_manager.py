"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: data_manager.py
Description: 
    This module implements the DataManager class, which provides methods to manage market data and trades.
    It includes methods for updating OHLCV data from Alpaca, retrieving OHLCV data, fetching and storing open trades,
    and retrieving open trades from the database.

Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-25
Version: 1.0.0
"""

import os
from datetime import datetime
from typing import List
from config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_PAPER_URL
from alpaca.data.requests import StockBarsRequest
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest,MarketOrderRequest
from alpaca.trading.enums import OrderSide, QueryOrderStatus, OrderSide, TimeInForce
from .sqlite_database import SQLiteDatabase


class DataManager:
    def __init__(self, db: SQLiteDatabase):
        self.db = db
        self.db.connect()

        self.api_key = ALPACA_API_KEY
        self.secret_key = ALPACA_SECRET_KEY

        self.data_client = StockHistoricalDataClient(self.api_key, self.secret_key)
        self.trading_client = TradingClient(self.api_key, self.secret_key, paper=True)


    #
    # OHLCV Data Management
    #

    def update_ohlcv_data(self, symbol: str, start: datetime, end: datetime):
        """
        Fetches OHLCV data for a given symbol from Alpaca and stores it in the database.
        Parameters:
            symbol (str): The stock symbol to fetch data for.
            start (datetime): The start date for the data.
            end (datetime): The end date for the data.
        """

        print(f"Fetching data from Alpaca for {symbol} from {start.date()} to {end.date()}")

        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Day,
            start=start,
            end=end
        )

        bars = self.data_client.get_stock_bars(request_params).df

        if bars.empty:
            print(f"No data returned for {symbol}.")
            return

        bars.reset_index(inplace=True)

        data = []
        for _, row in bars.iterrows():
            data.append({
                "date": row["timestamp"].strftime('%Y-%m-%d'),
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"]
            })

        self.db.insert_ohlcv_data(symbol, data)


    def get_ohlcv_data(self, symbol: str, start: datetime, end: datetime) -> List[dict]:
        """
        Retrieves OHLCV data for a given symbol from the database.
        Parameters:
            symbol (str): The stock symbol to retrieve data for.
            start (datetime): The start date for the data.
            end (datetime): The end date for the data.
        Returns:
            List[dict]: A list of dictionaries containing OHLCV data.
        """

        # Check if the database has the required data
        if not self.db.has_ohlcv_data(symbol, start, end):
            print(f"Data for {symbol} not found in database. Fetching from Alpaca...")
            self.update_ohlcv_data(symbol, start, end)

        print(f"Retrieving OHLCV data for {symbol} from {start.date()} to {end.date()}")
        return self.db.get_ohlcv_data(symbol, start, end)
    


    #
    # Trade Management
    #

    def open_trade_qty(self, _symbol: str, _qty: float, _side: OrderSide):
        """
        Opens a trade for a given symbol with a specified quantity and side (buy/sell).
        Parameters:
            _symbol (str): The stock symbol to trade.
            _qty (float): The quantity of shares to trade.
            _side (OrderSide): The side of the trade (buy or sell).
        """
        print(f"Opening trade for {_symbol} with quantity {_qty} on side {_side}")

        market_order_data = MarketOrderRequest(
            symbol=_symbol,
            qty=_qty,
            side=_side,
            time_in_force=TimeInForce.DAY
        )

        market_order = self.trading_client.submit_order(market_order_data)
        print(f"Market order submitted: {market_order.id} for {market_order.symbol} with quantity {market_order.qty} on side {market_order.side}")


    def open_trade_notional(self, _symbol: str, _notional: float):
        """
        Opens a trade for a given symbol with a specified notional value.
        Parameters:
            _symbol (str): The stock symbol to trade.
            _notional (float): The notional value of the trade.
        """

        print(f"Opening trade for {_symbol} with notional {_notional} on side buy")

        market_order_data = MarketOrderRequest(
            symbol=_symbol,
            notional=_notional,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY
        )

        market_order = self.trading_client.submit_order(market_order_data)
        print(f"Market order submitted: {market_order.id} for {market_order.symbol} with notional {market_order.notional} on side {market_order.side}")


    def get_open_positions(self) -> List[dict]:
        """
        Retrieves all open positions from Alpaca.
        Returns:
            List[dict]: A list of dictionaries containing open positions.
        """
        positions = self.trading_client.get_all_positions()
        active_trades = []
        
        for position in positions:
            print(f"Symbol: {position.symbol}, Quantity: {position.qty}, Side: {position.side}")
            active_trades.append({
                "symbol": position.symbol,
                "qty": float(position.qty),
                "side": position.side
            })
            
        return active_trades
    
    def get_orders(self) -> List[dict]:
        """
        Retrieves all open orders from Alpaca.
        Returns:
            List[dict]: A list of dictionaries containing open orders.
        """

        request_params = GetOrdersRequest(
            status=QueryOrderStatus.OPEN,  # Get only open orders
        )
        
        orders = self.trading_client.get_orders(request_params)
        active_orders = []
        
        for order in orders:
            print(f"Order ID: {order.id}, Symbol: {order.symbol}, Status: {order.status}, Quantity: {order.qty}, Side: {order.side}")
            active_orders.append({
                "id": order.id,
                "symbol": order.symbol,
                "qty": float(order.qty),
                "side": order.side,
                "status": order.status,
            })
            
        return active_orders


    #
    # Calendar Management
    #
    def populate_stock_calendar(self):
        """
        DON'T USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.
        Populates the calendar table with dates available on the Alpaca API.
        -- Calendar data is already populated form 1970 to 2029 --
        """
        print(f"Populating calendar")
        
        calendar = self.trading_client.get_calendar()
        self.db.populate_stock_calendar(calendar)