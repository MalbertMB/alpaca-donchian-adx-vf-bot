"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: .py
Description: 
    
Author: Albert Marín
Date Created: 2025-11-25
Last Modified: 2025-11-29
"""

import sqlite3
import json

from datetime import datetime, timezone
from typing import Optional
from domain import Signal, OpenPosition, Trade, Direction, QuantityType


class BacktestDataManager:

    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        
        # Enable Foreign Keys enforcement
        self.conn.execute("PRAGMA foreign_keys = ON;") 
        self.cursor = self.conn.cursor()
        
        # Ensure tables exist on initialization
        self._create_tables()

    def _create_tables(self):
        """Creates the necessary schema if it doesn't exist."""
        schema = """
        CREATE TABLE IF NOT EXISTS backtest_run (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_name TEXT NOT NULL,
            strategy_version TEXT,
            parameters TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            data_start TIMESTAMP,
            data_end TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS signal (
            signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            stock TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            price REAL,
            confidence REAL,
            reason TEXT,
            timestamp TIMESTAMP,
            FOREIGN KEY(run_id) REFERENCES backtest_run(run_id)
        );

        CREATE TABLE IF NOT EXISTS position (
            position_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            stock TEXT NOT NULL,
            position_type TEXT NOT NULL,
            quantity_type TEXT,
            quantity REAL,
            entry_time TIMESTAMP,
            entry_price REAL,
            entry_signal_id INTEGER,
            exit_time TIMESTAMP,
            exit_price REAL,
            exit_signal_id INTEGER,
            status TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES backtest_run(run_id),
            FOREIGN KEY(entry_signal_id) REFERENCES signal(signal_id),
            FOREIGN KEY(exit_signal_id) REFERENCES signal(signal_id)
        );

        CREATE TABLE IF NOT EXISTS trade (
            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            position_id INTEGER NOT NULL,
            profit_loss REAL,
            FOREIGN KEY(run_id) REFERENCES backtest_run(run_id),
            FOREIGN KEY(position_id) REFERENCES position(position_id)
        );
        """
        self.cursor.executescript(schema)
        self.conn.commit()

    def commit(self):
        """Manual commit to allow batching during backtests."""
        self.conn.commit()

    def create_backtest_run(
        self,
        strategy_name: str,
        strategy_version: str,
        parameters: dict,
        data_start: datetime,
        data_end: datetime
    ) -> int:
        params_json = json.dumps(parameters) 

        cur = self.cursor.execute(
            """
            INSERT INTO backtest_run (
                strategy_name,
                strategy_version,
                parameters,
                start_time,
                data_start,
                data_end
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                strategy_name,
                strategy_version,
                params_json,  # Insert the JSON string here
                datetime.now(timezone.utc),
                data_start,
                data_end,
            ),
        )
        self.conn.commit()
        return cur.lastrowid
    
    def close_backtest_run(self, run_id: int):
        self.cursor.execute(
            """
            UPDATE backtest_run
            SET end_time = ?
            WHERE run_id = ?
            """,
            (datetime.now(timezone.utc), run_id),
        )
        self.conn.commit()

    def get_backtest_run(self, run_id: int):
        self.cursor.execute("SELECT * FROM backtest_run WHERE run_id = ?", (run_id,))
        row = self.cursor.fetchone()
        
        if row:
            data = dict(row)
            data['parameters'] = json.loads(data['parameters']) 
            return data
        return None

    def insert_signal(self, run_id: int, signal: Signal) -> int:
        cur = self.cursor.execute(
            """
            INSERT INTO signal (
                run_id,
                stock,
                signal_type,
                price,
                confidence,
                reason,
                timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                signal.stock,
                signal.signal.value,
                signal.price,
                signal.confidence,
                signal.reason,
                signal.date,
            ),
        )

        signal_id = cur.lastrowid
        signal.id = signal_id
        return signal_id

    def open_position(self, run_id: int, position: OpenPosition, entry_signal_id: int) -> int:
        cur = self.cursor.execute(
            """
            INSERT INTO position (
                run_id,
                stock,
                position_type,
                quantity_type,
                quantity,
                entry_time,
                entry_price,
                entry_signal_id,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
            """,
            (
                run_id,
                position.stock,
                position.position_type.value,
                position.quantity_type.value,
                position.quantity,
                position.date,
                position.entry_price,
                entry_signal_id,
            ),
        )
        
        position_id = cur.lastrowid
        position.id = position_id
        return position_id

    # When closing a position we insert a trade and delete the open position
    # def close_position(
    #     self,
    #     position_id: int,
    #     exit_signal_id: int,
    #     exit_price: float,
    #     exit_time: datetime
    # ):
    #     self.cursor.execute(
    #         """
    #         UPDATE position
    #         SET
    #             exit_time = ?,
    #             exit_price = ?,
    #             exit_signal_id = ?,
    #             status = 'CLOSED'
    #         WHERE position_id = ?
    #         """,
    #         (
    #             exit_time,
    #             exit_price,
    #             exit_signal_id,
    #             position_id,
    #         ),
    #     )

    def insert_trade(self, run_id: int, position_id: int, profit_loss: float) -> int:
        cur = self.cursor.execute(
            """
            INSERT INTO trade (
                run_id,
                position_id,
                profit_loss
            )
            VALUES (?, ?, ?)
            """,
            (
                run_id,
                position_id,
                profit_loss,
            ),
        )
        return cur.lastrowid

    def get_open_positions(self, run_id: int):
        self.cursor.execute(
            "SELECT * FROM position WHERE run_id = ? AND status = 'OPEN'",
            (run_id,),
        )
        return self.cursor.fetchall()
    
    def get_trades(self, run_id: int):
        self.cursor.execute(
            """
            SELECT t.*, p.stock, p.entry_price, p.exit_price
            FROM trade t
            JOIN position p ON t.position_id = p.position_id
            WHERE t.run_id = ?
            """,
            (run_id,),
        )
        return self.cursor.fetchall()