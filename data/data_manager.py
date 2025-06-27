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
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import OrderSide, QueryOrderStatus
from .sqlite_database import SQLiteDatabase


class DataManager:
    def __init__(self, db: SQLiteDatabase):
        self.db = db
        self.db.connect()

        self.api_key = ALPACA_API_KEY
        self.secret_key = ALPACA_SECRET_KEY

        self.data_client = StockHistoricalDataClient(self.api_key, self.secret_key)
        self.trading_client = TradingClient(self.api_key, self.secret_key, paper=True)


    def update_ohlcv_data(self, symbol: str, start: datetime, end: datetime):
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

        # If requesting a single symbol, no need to filter again
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
        return self.db.get_ohlcv_data(symbol, start, end)


    def get_open_positions(self) -> List[dict]:
        positions = self.trading_client.get_all_positions()
        active_trades = []
        
        for position in positions:
            print(f"Symbol: {position.symbol}, Quantity: {position.qty}, Side: {position.side}, Average Entry Price: {position.lastday_avg_entry_price}")
            print(position)
            active_trades.append({
                "symbol": position.symbol,
                "qty": float(position.qty),
                "side": position.side,  # 'long' or 'short'
                "entry_date": position.lastday_avg_entry_price,
            })
            
        return active_trades
    
    def get_orders(self) -> List[dict]:

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


