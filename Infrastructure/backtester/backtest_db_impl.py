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
from Domain import Signal, OpenPosition, Trade, Direction, QuantityType


class BacktestDataManager:

    def __init__(self, db_path: str):
        """
        Initializes the BacktestDataManager with a SQLite database connection.
        If the database or tables do not exist, they will be created.
        Args:
            db_path (str): Path to the SQLite database file.
        """
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;") 
        self.cursor = self.conn.cursor()
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

        CREATE TABLE IF NOT EXISTS open_position (
            position_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            stock TEXT NOT NULL,
            direction TEXT NOT NULL,
            date TIMESTAMP,
            entry_price REAL,
            quantity_type TEXT NOT NULL,
            quantity REAL,
            entry_signal_id INTEGER,
            FOREIGN KEY(run_id) REFERENCES backtest_run(run_id),
            FOREIGN KEY(entry_signal_id) REFERENCES signal(signal_id)
        );

        CREATE TABLE IF NOT EXISTS trade (
            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            stock TEXT NOT NULL,
            direction TEXT NOT NULL,
            quantity_type TEXT NOT NULL,
            quantity REAL,
            entry_price REAL,
            exit_price REAL,
            entry_date TIMESTAMP,
            exit_date TIMESTAMP,
            result REAL,
            entry_signal_id INTEGER,
            exit_signal_id INTEGER,
            FOREIGN KEY(run_id) REFERENCES backtest_run(run_id),
            FOREIGN KEY(entry_signal_id) REFERENCES signal(signal_id),
            FOREIGN KEY(exit_signal_id) REFERENCES signal(signal_id)
        );
        """
        self.cursor.executescript(schema)
        self.conn.commit()


    def commit(self):
        """Manual commit to allow batching during backtests."""
        self.conn.commit()


    def create_backtest_run(self, strategy_name: str, strategy_version: str, parameters: dict,
                            data_start: datetime, data_end: datetime ) -> int:
        """
        Inserts a new backtest run into the database.
        Args:
            strategy_name (str): Name of the strategy.
            strategy_version (str): Version of the strategy.
            parameters (dict): Parameters used for the backtest.
            data_start (datetime): Start date of the data used.
            data_end (datetime): End date of the data used.
        Returns:
            int: The ID of the newly created backtest run.
        """
        
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
                params_json,
                datetime.now(timezone.utc),
                data_start,
                data_end,
            ),
        )
        self.conn.commit()
        return cur.lastrowid
    

    def close_backtest_run(self, run_id: int):
        """
        Updates the end_time of a backtest run to mark its completion.
        Args:
            run_id (int): The ID of the backtest run to close.    
        """
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
        """
        Retrieves a backtest run by its ID.
        Args:
            run_id (int): The ID of the backtest run to retrieve.
        Returns:
            dict: A dictionary containing the backtest run details, or None if not found.
        """

        self.cursor.execute("SELECT * FROM backtest_run WHERE run_id = ?", (run_id,))
        row = self.cursor.fetchone()
        
        if row:
            data = dict(row)
            data['parameters'] = json.loads(data['parameters']) 
            return data
        return None


    def insert_signal(self, run_id: int, signal: Signal) -> int:
        """
        Inserts a new signal into the database and assigns its ID.
        This method does not commit changes, the caller must handle committing.
        Args:
            run_id (int): The ID of the backtest run.
            signal (Signal): The Signal object to insert.
        Returns:
            int: The ID of the newly created signal.
        """

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

        signal.id = cur.lastrowid
        return signal.id

    def insert_openposition(self, run_id: int, position: OpenPosition) -> int:
        """
        Inserts a new open position into the database and assigns its ID.
        This method does not commit changes, the caller must handle committing.
        Args:
            run_id (int): The ID of the backtest run.
            position (OpenPosition): The OpenPosition object to insert.
        Returns:
            int: The ID of the newly created open position.
        """

        cur = self.cursor.execute(
            """
            INSERT INTO open_position (
                run_id,
                stock,
                direction,
                date,
                entry_price,
                quantity_type,
                quantity,
                entry_signal_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                position.stock,
                position.direction.value,
                position.date,
                position.entry_price,
                position.quantity_type.value,
                position.quantity,
                position.entry_signal_id,
            ),
        )
        
        position.id = cur.lastrowid
        return position.id


    def _insert_trade(self, run_id: int, trade: Trade) -> int:
        """
        Inserts a new trade into the database and assigns its ID.
        This method does not commit changes, the caller must handle committing.
        Args:
            run_id (int): The ID of the backtest run.
            trade (Trade): The Trade object to insert.
        Returns:
            int: The ID of the newly created trade.
        """

        cur = self.cursor.execute(
            """
            INSERT INTO trade (
                run_id,
                stock,
                direction,
                quantity_type,
                quantity,
                entry_price,
                exit_price,
                entry_date,
                exit_date,
                result,
                entry_signal_id,
                exit_signal_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                trade.stock,
                trade.direction.value,
                trade.quantity_type.value,
                trade.quantity,
                trade.entry_price,
                trade.exit_price,
                trade.entry_date,
                trade.exit_date,
                trade.result,
                trade.entry_signal_id,
                trade.exit_signal_id

            ),
        )
        trade.id = cur.lastrowid
        return trade.id
    

    def close_openposition(self, run_id: int, position: OpenPosition, trade: Trade):
        """
        Closes an open position by inserting a corresponding trade and deleting the open position.
        Args:
            run_id (int): The ID of the backtest run.
            position (OpenPosition): The OpenPosition object to close.
            trade (Trade): The Trade object representing the closed trade.
        Returns:
            int: The ID of the newly created trade.
        """

        try:
            trade_id = self._insert_trade(run_id, trade)
            
            self.cursor.execute(
                "DELETE FROM open_position WHERE position_id = ?", 
                (position.id,)
            )
            
            self.conn.commit()
            return trade_id

        except Exception as e:
            self.conn.rollback()  # Undo the trade insert if delete fails
            print(f"Error closing position: {e}")
            raise e


    def get_signals_for_run(self, run_id: int):
        """
        Retrieves all signals for a given backtest run.
        Args:
            run_id (int): The ID of the backtest run.
        Returns:
            list: A list of Signal objects associated with the run.
        """

        self.cursor.execute("SELECT * FROM signal WHERE run_id = ?", (run_id,))
        rows = self.cursor.fetchall()
        signals = []
        for row in rows:
            data = dict(row)
            signal = Signal(
                stock=data['stock'],
                signal=Direction(data['signal_type']),
                date=data['timestamp'],
                price=data['price'],
                confidence=data['confidence'],
                reason=data['reason'],
                id=data['signal_id']
            )
            signals.append(signal)
        return signals
    

    def get_open_positions_for_run(self, run_id: int):
        """
        Retrieves all open positions for a given backtest run.
        Args:
            run_id (int): The ID of the backtest run.
        Returns:
            list: A list of OpenPosition objects associated with the run.
        """

        self.cursor.execute("SELECT * FROM open_position WHERE run_id = ?", (run_id,))
        rows = self.cursor.fetchall()
        positions = []
        for row in rows:
            data = dict(row)
            position = OpenPosition(
                stock=data['stock'],
                direction=Direction(data['direction']),
                date=data['date'],
                entry_price=data['entry_price'],
                quantity_type=QuantityType(data['quantity_type']),
                quantity=data['quantity'],
                entry_signal_id=data['entry_signal_id'],
                id=data['position_id']
            )
            positions.append(position)
        return positions
    
    def get_trades_for_run(self, run_id: int):
        """
        Retrieves all trades for a given backtest run.
        Args:
            run_id (int): The ID of the backtest run.
        Returns:
            list: A list of Trade objects associated with the run.
        """

        self.cursor.execute("SELECT * FROM trade WHERE run_id = ?", (run_id,))
        rows = self.cursor.fetchall()
        trades = []
        for row in rows:
            data = dict(row)
            trade = Trade(
                stock=data['stock'],
                direction=Direction(data['direction']),
                quantity_type=QuantityType(data['quantity_type']),
                quantity=data['quantity'],
                entry_price=data['entry_price'],
                exit_price=data['exit_price'],
                entry_date=data['entry_date'],
                exit_date=data['exit_date'],
                result=data['result'],
                entry_signal_id=data['entry_signal_id'],
                exit_signal_id=data['exit_signal_id'],
                id=data['trade_id']
            )
            trades.append(trade)
        return trades