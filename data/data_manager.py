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
from config.config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_PAPER_URL
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from .sqlite_database import SQLiteDatabase

class DataManager:
    def __init__(self, db: SQLiteDatabase):
        self.db = db
        self.db.connect()

        self.api_key = os.getenv(ALPACA_API_KEY)
        self.secret_key = os.getenv(ALPACA_SECRET_KEY)

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

        bars = bars[bars['symbol'] == symbol]  # in case of multi-symbol returns
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

    def fetch_and_store_open_trades(self):
        open_orders = self.trading_client.get_orders(status='open')

        for order in open_orders:
            trade = {
                "id": str(order.id),
                "symbol": order.symbol,
                "qty": int(order.qty),
                "side": order.side.value,
                "type": order.order_class.value if order.order_class else "market",
                "time": order.submitted_at.isoformat(),
                "status": order.status.value
            }
            self.db.insert_trade(trade)

    def get_open_trades(self) -> List[dict]:
        return self.db.get_open_trades()
