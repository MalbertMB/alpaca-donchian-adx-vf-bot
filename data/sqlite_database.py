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

        # Trades Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                symbol TEXT,
                qty INTEGER,
                side TEXT,
                type TEXT,
                time TEXT,
                status TEXT
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

    def insert_trade(self, trade: Dict):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO trades (id, symbol, qty, side, type, time, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            trade["id"],
            trade["symbol"],
            trade["qty"],
            trade["side"],
            trade["type"],
            trade["time"],
            trade["status"]
        ))
        self.conn.commit()

    def get_open_trades(self) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM trades WHERE status = 'open'
        """)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
