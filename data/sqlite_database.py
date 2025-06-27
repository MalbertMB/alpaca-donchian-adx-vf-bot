"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: sqlite_database.py
Description: 
    This module implements the SQLiteDatabase class, which provides methods to interact with an SQLite database.
    It includes methods for connecting to the database, creating tables, inserting OHLCV data, retrieving OHLCV data,
    inserting trades, and retrieving open trades.
    
Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2025-06-25
Version: 1.0.0
"""


import sqlite3
from typing import List, Dict
from datetime import datetime
from .database_interface import DatabaseInterface

class SQLiteDatabase(DatabaseInterface):
    def __init__(self, db_path="data/market_data.db"):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()

    def create_tables(self):
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
        

        self.conn.commit()

    def insert_ohlcv_data(self, symbol: str, data: List[Dict]):
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
        cursor = self.conn.cursor()

        # Step 1: Get all expected trading days from local market calendar
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

        # Step 2: Count how many of those days exist in ohlcv table
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

        
