# databases/market/market_data_manager.py

from .market.market_database import MarketDatabase
from Infrastucture.interfaces import TradingDatabaseInterface


class MarketDataManager:
    """
    Shared manager used by both live trader and backtester.
    Reads market data and writes signals/trades to the injected database.
    """

    def __init__(self, market_db: MarketDatabase, trade_db: TradingDatabaseInterface):
        self.market_db = market_db
        self.trade_db = trade_db

    def get_candles(self, symbol: str, start: str, end: str):
        return self.market_db.get_candles(symbol, start, end)

    def save_trade(self, trade):
        self.trade_db.save_trade(trade)

    def save_signal(self, signal):
        self.trade_db.save_signal(signal)

    def get_positions(self):
        return self.trade_db.get_open_positions()

    def close_position(self, position_id: int):
        self.trade_db.close_position(position_id)


"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: data_manager.py
Description: 
    This module implements the DataManager class, which provides methods to manage market data and trades.
    It includes methods for updating OHLCV data from Alpaca, retrieving OHLCV data, fetching and storing open trades,
    and retrieving open trades from the database.

Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-29
"""

import os
from datetime import datetime
from typing import List
from alpaca.data.requests import StockBarsRequest
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest,MarketOrderRequest
from alpaca.trading.enums import OrderSide, QueryOrderStatus, OrderSide, TimeInForce
from .interfaces import TradingDatabaseInterface


class DataManager:
    def __init__(self, db: TradingDatabaseInterface, ALPACA_API_KEY: str, ALPACA_SECRET_KEY: str):
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

    def get_ohlcv_data(self, symbol: str, start: datetime, end: datetime, update: bool = True) -> List[dict]:
        """
        Retrieves OHLCV data for a given symbol from the database.
        If the data is not available in the database, it fetches it from Alpaca and stores it.
        Parameters:
            symbol (str): The stock symbol to retrieve data for.
            start (datetime): The start date for the data.
            end (datetime): The end date for the data.
            update (bool): Whether to update the data from Alpaca if not found in the database.
        Returns:
            List[dict]: A list of dictionaries containing OHLCV data.
        """

        # Check if the database has the required data
        if not self.db.has_ohlcv_data(symbol, start, end):
            if not update:
                raise ValueError(f"Data for {symbol} not found in database. Set update=True to fetch from Alpaca.")
            print(f"Data for {symbol} not found in database. Fetching from Alpaca...")
            self.update_ohlcv_data(symbol, start, end)

        print(f"Retrieving OHLCV data for {symbol} from {start.date()} to {end.date()}")
        return self.db.get_ohlcv_data(symbol, start, end)    


    #
    # API Live Positions Management
    #

    def open_position_qty(self, _symbol: str, _qty: float, _side: OrderSide):
        """
        Opens a position for a given symbol with a specified quantity and side (buy/sell).
        Parameters:
            _symbol (str): The stock symbol to trade.
            _qty (float): The quantity of shares to trade.
            _side (OrderSide): The side of the trade (buy or sell).
        """
        print(f"Opening position for {_symbol} with quantity {_qty} on side {_side}")

        market_order_data = MarketOrderRequest(
            symbol=_symbol,
            qty=_qty,
            side=_side,
            time_in_force=TimeInForce.DAY
        )

        market_order = self.trading_client.submit_order(market_order_data)
        print(f"Market order submitted: {market_order.id} for {market_order.symbol} with quantity {market_order.qty} on side {market_order.side}")


    def open_position_notional(self, _symbol: str, _notional: float):
        """
        Opens a buy position for a given symbol with a specified notional value.
        Parameters:
            _symbol (str): The stock symbol to trade.
            _notional (float): The notional value of the trade.
        """
        print(f"Opening position for {_symbol} with notional {_notional} on side buy")

        market_order_data = MarketOrderRequest(
            symbol=_symbol,
            notional=_notional,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY
        )

        market_order = self.trading_client.submit_order(market_order_data)
        print(f"Market order submitted: {market_order.id} for {market_order.symbol} with notional {market_order.notional} on side {market_order.side}")


    def get_positions(self) -> List[dict]:
        """
        Retrieves all open positions from Alpaca.
        Returns:
            List[dict]: A list of dictionaries containing open positions.
        """
        print("Retrieving open positions from Alpaca")
        positions = self.trading_client.get_all_positions()
        active_positions = []
        
        if not positions:
            print("No active positions found.")
            return active_positions

        print(f"Found {len(positions)} active positions:")
        for position in positions:
            print(f" - Symbol: {position.symbol}, Quantity: {position.qty}, Side: {position.side}")
            active_positions.append({
                "symbol": position.symbol,
                "qty": float(position.qty),
                "side": position.side
            })
            
        return active_positions
    
    def get_orders(self) -> List[dict]:
        """
        Retrieves all open orders from Alpaca.
        Returns:
            List[dict]: A list of dictionaries containing open orders.
        """
        print("Retrieving open orders from Alpaca")
        
        request_params = GetOrdersRequest(
            status=QueryOrderStatus.OPEN,
        )
        
        orders = self.trading_client.get_orders(request_params)
        active_orders = []

        if not orders:
            print("No active orders found.")
            return active_orders

        print(f"Retrieving open orders from Alpaca:")
        for order in orders:
            print(f" - Order ID: {order.id}, Symbol: {order.symbol}, Status: {order.status}, Quantity: {order.qty}, Side: {order.side}")
            active_orders.append({
                "id": order.id,
                "symbol": order.symbol,
                "qty": float(order.qty),
                "side": order.side,
                "status": order.status,
            })
            
        return active_orders

    def cancel_order_by_id(self, order_id: str):
        """
        Cancels an open order by its ID.
        Parameters:
            order_id (str): The ID of the order to cancel.
        """
        print(f"Cancelling order with ID {order_id}")
        try:
            self.trading_client.cancel_order_by_id(order_id)
            print(f"Order {order_id} cancelled successfully.")
        except Exception as e:
            print(f"Error cancelling order {order_id}: {e}")
            raise e

    def close_position_by_symbol(self, symbol: str):
        """
        Closes an open position by its symbol.
        Parameters:
            symbol (str): The stock symbol of the trade to close.
        """
        print(f"Closing position for {symbol}")
        try:
            self.trading_client.close_position(symbol)
            print(f"position for {symbol} closed successfully.")
        except Exception as e:
            print(f"Error closing position for {symbol}: {e}")
            raise e

 
    #
    # Backtesting Trade Management
    #

    def insert_open_trade_backtest(self, position: dict):
        """
        Inserts an open trade record into the database for backtesting purposes.
        Parameters:
            position (Dict): A dictionary containing the details of the open trade.
                Expected keys: 'id', 'symbol', 'entry_date', 'entry_price', 'quantity'.
        """
        print(f"Inserting open trade for backtest: {position}")
        self.db.insert_open_trade_backtest(position)

    def insert_close_trade_backtest(self, trade: dict):
        """
        Inserts a closed trade record into the database for backtesting purposes.
        Parameters:
            trade (Dict): A dictionary containing the details of the closed trade.
                Expected keys: 'id', 'symbol', 'entry_date', 'entry_price', 'exit_date', 'exit_price', 'quantity', 'profit_loss'.
        """
        print(f"Inserting closed trade for backtest: {trade}")
        self.db.insert_close_trade_backtest(trade)
    
    def get_open_trades_backtest(self) -> List[dict]:
        """
        Retrieves all open trades from the database for backtesting purposes.
        Returns:
            List[dict]: A list of dictionaries containing open trade records.
        """
        print("Retrieving open trades for backtest")
        return self.db.get_open_trades_backtest()
    
    def get_closed_trades_backtest(self) -> List[dict]:
        """
        Retrieves all closed trades from the database for backtesting purposes.
        Returns:
            List[dict]: A list of dictionaries containing closed trade records.
        """
        print("Retrieving closed trades for backtest")
        return self.db.get_closed_trades_backtest()
    

    #
    # Dow Jones and S&P 500 Tickers Management
    #

    def get_symbols_by_group(self, group: str) -> List[str]:
        """
        Retrieves Dow Jones or S&P 500 tickers from the database.
        Parameters:
            group (str): The group of tickers to retrieve ('dow_jones' or 'sp500').
        Returns:
            List[str]: A list of stock symbols.
        """
        if group == "dow_jones" or group == "Dow Jones":
            return self.db.get_dow_jones_tickers()
        elif group == "sp500" or group == "S&P 500" or group == "SP500" or group == "S&P500":
            return self.db.get_sp500_tickers()
        else:
            raise ValueError("Invalid group. Use 'dow_jones' or 'sp500'.")
        

    def get_dow_jones_tickers(self) -> List[str]:
        """
        Retrieves all Dow Jones tickers from the database.
        Returns:
            List[str]: A list of Dow Jones tickers.
        """
        print("Retrieving Dow Jones tickers")
        return self.db.get_dow_jones_tickers()
    

    def get_sp500_tickers(self) -> List[str]:
        """
        Retrieves all S&P 500 tickers from the database.
        Returns:
            List[str]: A list of S&P 500 tickers.
        """
        print("Retrieving S&P 500 tickers")
        return self.db.get_sp500_tickers()

    #
    # Other Methods, THIS SHOULD NOT BE USED UNLESS YOU KNOW WHAT YOU ARE DOING
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

    def clear_database(self):
        """
        DON'T USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.
        Clears the database by dropping all tables.
        WARNING: This will delete all data in the database.
        """
        print("Clearing database")
        self.db.clear_database()

    def clear_backtest_tables(self):
        """
        DON'T USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.
        Clears the backtest tables by dropping them.
        WARNING: This will delete all backtest data in the database.
        """
        print("Clearing backtest tables")
        self.db.clear_backtest_tables()

    def insert_sp500_tickers(self, tickers: List[str]):
        """
        TICKERS ARE ALREADY POPULATED, DO NOT USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.
        Inserts a list of S&P 500 tickers into the database.
        Parameters:
            tickers (List[str]): A list of S&P 500 tickers to insert.
        """
        print(f"Inserting S&P 500 tickers: {tickers}")
        self.db.insert_sp500_tickers(tickers)

    def insert_dow_jones_tickers(self, tickers: List[str]):
        """
        TICKERS ARE ALREADY POPULATED, DO NOT USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.
        Inserts a list of Dow Jones tickers into the database.
        Parameters:
            tickers (List[str]): A list of Dow Jones tickers to insert.
        """
        print(f"Inserting Dow Jones tickers: {tickers}")
        self.db.insert_dow_jones_tickers(tickers)
