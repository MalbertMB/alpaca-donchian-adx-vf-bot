"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: sqlite_database.py
Description: 
    This module implements the SQLiteDatabase class, which provides methods to interact with an SQLite database.
    It includes methods for connecting to the database, creating tables, inserting OHLCV data, retrieving OHLCV data,
    inserting trades, and retrieving open trades.
    
Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-28
"""


import sqlite3
from typing import List, Dict
from datetime import datetime
from .database_interface import DatabaseInterface


class SQLiteDatabase(DatabaseInterface):
    def __init__(self, db_path="data/data_base.db"):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()


    #
    # Table creation method
    #

    def create_tables(self):
        """
        Creates the necessary tables in the SQLite database if they do not already exist.
        This includes tables for OHLCV data, calendar, open trades, and closed trades.
        """

        cursor = self.conn.cursor()

        # OHLCV Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv (
                symbol TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (symbol, date)
            )
        """)

        # Calendar Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS calendar (
                date TEXT PRIMARY KEY
            )
        """)

        # Open Trades Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS open_trades (
                id TEXT PRIMARY KEY,
                symbol TEXT,
                entry_date TEXT,
                entry_price REAL,
                quantity INTEGER
            )
        """)

        # Closed Trades Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS closed_trades (
                id TEXT PRIMARY KEY,
                symbol TEXT,
                entry_date TEXT,
                entry_price REAL,
                exit_date TEXT,
                exit_price REAL,
                quantity INTEGER,
                profit REAL
            )
        """)
        
        self.conn.commit()


    #
    # OHLCV (Open, High, Low, Close, Volume) data methods
    #

    def insert_ohlcv_data(self, symbol: str, data: List[Dict]):
        """
        Inserts OHLCV data for a given symbol into the database.
        If the data already exists, it will be replaced.
        Parameters:
            symbol (str): The stock symbol for which the data is being inserted.
            data (List[Dict]): A list of dictionaries containing OHLCV data.
        """
        cursor = self.conn.cursor()
        for row in data:
            cursor.execute("""
                INSERT OR REPLACE INTO ohlcv (symbol, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol,
                row["date"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"]
            ))
        self.conn.commit()

    def get_ohlcv_data(self, symbol: str, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Retrieves OHLCV data for a given symbol within a specified date range.
        Parameters:
            symbol (str): The stock symbol for which the data is being retrieved.
            start_date (datetime): The start date of the range.
            end_date (datetime): The end date of the range.
        Returns:
            List[Dict]: A list of dictionaries containing OHLCV data for the specified symbol and date range.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM ohlcv
            WHERE symbol = ? AND date BETWEEN ? AND ?
            ORDER BY date ASC
        """, (
            symbol,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def has_ohlcv_data(self, symbol: str, start_date: datetime, end_date: datetime) -> bool:
        """
        Checks if OHLCV data exists for a given symbol within a specified date range.
        Parameters:
            symbol (str): The stock symbol for which the data is being checked.
            start_date (datetime): The start date of the range.
            end_date (datetime): The end date of the range.
        Returns:
            bool: True if OHLCV data exists for the specified symbol and date range, False otherwise.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT date FROM calendar
            WHERE date BETWEEN ? AND ?
        """, (
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ))
        trading_days = [row[0] for row in cursor.fetchall()]

        if not trading_days:
            raise ValueError("No trading days found in start {start_date} and end {end_date} range.")

        cursor.execute("""
            SELECT COUNT(*) FROM ohlcv
            WHERE symbol = ? AND date BETWEEN ? AND ?
        """, (
            symbol,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ))
        data_count = cursor.fetchone()[0]

        return data_count == len(trading_days)


    #
    # Backtesting trade methods
    #

    def insert_open_trade_backtest(self, position: Dict):
        """
        Inserts an open trade record into the database for backtesting purposes.
        Parameters:
            position (Dict): A dictionary containing the details of the open trade.
                Expected keys: 'id', 'symbol', 'entry_date', 'entry_price', 'quantity'.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO open_trades (id, symbol, entry_date, entry_price, quantity)
            VALUES (?, ?, ?, ?, ?)
        """, (
            position["id"],
            position["symbol"],
            position["entry_date"].strftime('%Y-%m-%d'),
            position["entry_price"],
            position["quantity"]
        ))
        self.conn.commit()

    def insert_close_trade_backtest(self, trade: Dict):
        """
        Inserts a closed trade record into the database for backtesting purposes.
        Parameters:
            trade (Dict): A dictionary containing the details of the closed trade.
                Expected keys: 'id', 'symbol', 'entry_date', 'entry_price', 'exit_date', 'exit_price', 'quantity', 'profit'.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO closed_trades (id, symbol, entry_date, entry_price, exit_date, exit_price, quantity, profit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade["id"],
            trade["symbol"],
            trade["entry_date"].strftime('%Y-%m-%d'),
            trade["entry_price"],
            trade["exit_date"].strftime('%Y-%m-%d'),
            trade["exit_price"],
            trade["quantity"],
            trade["profit"]
        ))
        self.conn.commit()

    def get_open_trades_backtest(self) -> List[Dict]:
        """
        Retrieves all open trades from the database for backtesting purposes.
        Returns:
            List[Dict]: A list of dictionaries containing open trade records.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM open_trades")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def get_closed_trades_backtest(self) -> List[Dict]:
        """
        Retrieves all closed trades from the database for backtesting purposes.
        Returns:
            List[Dict]: A list of dictionaries containing closed trade records.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM closed_trades")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def delete_open_trade_backtest(self, trade_id: str):
        """
        Deletes an open trade from the backtesting table by its ID.
        Parameters:
            trade_id (str): The ID of the trade to delete.
        """
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM open_trades WHERE id = ?", (trade_id,))
        self.conn.commit()


    #
    # Other methods, THIS SHOULD NOT BE USED UNLESS YOU KNOW WHAT YOU ARE DOING
    #
    
    def populate_stock_calendar(self, calendar_data: List):
        """
        DON'T USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.
        Populates the calendar table with dates available on the Alpaca API.
        -- Calendar data is already populated form 1970 to 2029 --
        Parameters:
            calendar_data (List): List of Alpaca Calendar objects.
        """
        cursor = self.conn.cursor()
        cursor.executemany("""
            INSERT OR IGNORE INTO calendar (date)
            VALUES (?)
        """, [(item.date.strftime('%Y-%m-%d'),) for item in calendar_data])
        self.conn.commit()

    def clear_database(self):
        """
        DON'T USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.
        Clears the database by dropping all tables.
        WARNING: This will delete all data in the database.
        """
        cursor = self.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS ohlcv")
        cursor.execute("DROP TABLE IF EXISTS calendar")
        cursor.execute("DROP TABLE IF EXISTS open_trades")
        cursor.execute("DROP TABLE IF EXISTS closed_trades")
        self.conn.commit()
    
    def clear_backtest_tables(self):
        """
        DON'T USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.
        Clears all data from the backtesting tables.
        """
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM open_trades")
        cursor.execute("DELETE FROM closed_trades")
        self.conn.commit()