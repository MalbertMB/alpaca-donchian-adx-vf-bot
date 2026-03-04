"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: sqlite_database.py
Description: 
    This module implements the SQLiteDatabase class, which provides methods to interact with an SQLite database.
    It includes methods for connecting to the database, creating tables, inserting OHLCV data, retrieving OHLCV data,
    inserting trades, and retrieving open trades.
    
Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-29
"""


import sqlite3
import threading
import pandas as pd

from typing import List, Dict
from datetime import datetime


class MarketDatabase():

    def __init__(self, db_path: str):
        """
        Initializes the MarketDatabase with a SQLite database connection.
        If the database file does not exist, it will be created.
        Args:
            db_path (str): The file path for the SQLite database.
        """

        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.db_lock = threading.Lock()
        self._create_tables()


    def __enter__(self):
        """Allows the database to be used as a context manager with the 'with' statement."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures the database connection is closed when exiting a 'with' block."""
        self.close()

    def close(self) -> None:
        """Closes the database connection. Should be called when the database is no longer needed to free up resources."""
        if self.conn:
            self.conn.close()


    def _create_tables(self):
        """
        Creates the necessary tables in the SQLite database if they do not already exist.
        This includes tables for OHLCV data, calendar, and tickers.
        """

        schema = """
        CREATE TABLE IF NOT EXISTS ohlcv (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            PRIMARY KEY (symbol, date)
        );

        CREATE TABLE IF NOT EXISTS calendar (
            date TEXT PRIMARY KEY,
            open BOOLEAN NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dow_jones_tickers (
            symbol TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS SP500_tickers (
            symbol TEXT PRIMARY KEY
        );

        CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_date ON ohlcv(symbol, date);
        CREATE INDEX IF NOT EXISTS idx_calendar_date_open ON calendar(date, open);
        """

        with self.db_lock:
            cur = self.conn.cursor()
            cur.executescript(schema)
            self.conn.commit()


    def insert_ohlcv_data(self, symbol: str, data: pd.DataFrame):
        """
        Inserts OHLCV data for a given symbol into the database.
        If the data already exists, it will be replaced.
        Parameters:
            symbol (str): The stock symbol for which the data is being inserted.
            data (pd.DataFrame): A pandas DataFrame containing OHLCV data for the symbol.
        """

        if data.empty:
            return

        with self.db_lock:
            cur = self.conn.cursor()
            cur.executemany(
                """
                INSERT OR REPLACE INTO ohlcv (
                    symbol,
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        symbol,
                        index.to_pydatetime() if isinstance(index, pd.Timestamp) else index,
                        row['open'],
                        row['high'],
                        row['low'],
                        row['close'],
                        row['volume']
                    )
                    for index, row in data.iterrows()
                ]
            )
            self.conn.commit()


    def get_ohlcv_data(self, symbol: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        """
        Retrieves OHLCV data for a given symbol within a specified date range.
        The returned DataFrame will have a DatetimeIndex.
        Parameters:
            symbol (str): The stock symbol for which the data is being retrieved.
            start_date (pd.Timestamp): The start date of the range.
            end_date (pd.Timestamp): The end date of the range.
        Returns:
            pd.DataFrame: A DataFrame containing the OHLCV data for the specified symbol and date range, indexed by date.
        """
        
        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT date, open, high, low, close, volume
                FROM ohlcv
                WHERE symbol = ? AND date BETWEEN ? AND ?
                ORDER BY date ASC
                """,
                (
                    symbol,
                    start_date.to_pydatetime(),
                    end_date.to_pydatetime()
                )
            )
            rows = cur.fetchall()
            columns = [description[0] for description in cur.description]

            df = pd.DataFrame(rows, columns=columns)            
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            return df

    
    def has_ohlcv_data(self, symbol: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> bool:
        """
        Checks if OHLCV data exists for a given symbol within a specified date range.
        It compares the number of OHLCV records with the number of trading days in the calendar.
        Parameters:
            symbol (str): The stock symbol for which the data is being checked.
            start_date (pd.Timestamp): The start date of the range.
            end_date (pd.Timestamp): The end date of the range.
        Returns:
            bool: True if OHLCV data exists for the specified symbol and date range, False otherwise.
        """

        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT COUNT(*)
                FROM ohlcv
                WHERE symbol = ? AND date BETWEEN ? AND ?
                """,
                (
                    symbol,
                    start_date.to_pydatetime(),
                    end_date.to_pydatetime()
                )
            )
            count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM calendar
                WHERE date BETWEEN ? AND ? AND open = 1
                """,
                (
                    start_date.to_pydatetime(),
                    end_date.to_pydatetime()
                )
            )
            trading_days = cur.fetchone()[0]

            return count > 0 and count == trading_days


    def delete_ohlcv_data(self, symbol: str) -> None:
        """
        Deletes all OHLCV data for a given symbol from the database.
        Parameters:
            symbol (str): The stock symbol whose data should be deleted.
        """
        with self.db_lock:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM ohlcv WHERE symbol = ?", (symbol,))
            self.conn.commit()


    def get_dow_jones_tickers(self) -> pd.DataFrame:
        """
        Retrieves all Dow Jones tickers from the database.
        Returns:
            pd.DataFrame: A DataFrame containing the stock symbols in the Dow Jones index.
        """
        with self.db_lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT symbol FROM dow_jones_tickers")
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=['symbol'])
    
    
    def get_sp500_tickers(self) -> pd.DataFrame:
        """
        Retrieves all S&P 500 tickers from the database.
        Returns:
            pd.DataFrame: A DataFrame containing the stock symbols in the S&P 500 index.
        """
        with self.db_lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT symbol FROM SP500_tickers")
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=['symbol'])


    
    def _insert_stock_calendar(self, calendar_data: pd.DataFrame):
        """
        DON'T USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.
        Populates the calendar table with dates available on the Alpaca API.
        -- Calendar data is already populated form 1970 to 2029 --
        Parameters:
            calendar_data (pd.DataFrame): DataFrame of Alpaca Calendar objects.
        """
        with self.db_lock:
            cur = self.conn.cursor()
            cur.executemany("""
                INSERT OR IGNORE INTO calendar (
                    date,
                    open
                )
                VALUES (?, ?)
                """,
                [
                    (
                        index.to_pydatetime() if isinstance(index, pd.Timestamp) else index,
                        1 if row['open'] else 0
                    )
                    for index, row in calendar_data.iterrows()
                ]
            )
            self.conn.commit()


    def _insert_dow_jones_tickers(self, tickers: List[str]):
        """
        TICKERS ARE ALREADY POPULATED, DO NOT USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.
        Inserts a list of tickers into the database.
        If a ticker already exists, it will be ignored.
        Parameters:
            tickers (List[str]): A list of stock symbols to insert into the database.
        """
        with self.db_lock:
            cur = self.conn.cursor()
            cur.executemany("""
                INSERT OR IGNORE INTO dow_jones_tickers (symbol)
                VALUES (?)
            """, [(ticker,) for ticker in tickers])
            self.conn.commit()


    def _insert_sp500_tickers(self, tickers: List[str]):
        """
        TICKERS ARE ALREADY POPULATED, DO NOT USE THIS METHOD UNLESS YOU KNOW WHAT YOU ARE DOING.
        Inserts a list of S&P 500 tickers into the database.
        If a ticker already exists, it will be ignored.
        Parameters:
            tickers (List[str]): A list of stock symbols to insert into the database.
        """
        with self.db_lock:
            cur = self.conn.cursor()
            cur.executemany("""
                INSERT OR IGNORE INTO SP500_tickers (symbol)
                VALUES (?)
            """, [(ticker,) for ticker in tickers])
            self.conn.commit()