"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: backtest_db_impl.py
Description: 
    This module implements the TradingDataBaseInterface for backtesting scenarios using SQLite.
    It provides methods to manage backtest runs, signals, open positions, and trades, allowing for efficient
    data storage and retrieval during backtests.
    
Author: Albert Marín Blasco
Date Created: 2025-11-25
Last Modified: 2026-02-19
"""

import sqlite3
import json
import pandas as pd

from datetime import datetime, timezone
from ..interfaces import TradingDataBaseInterface
from Domain import Signal, OpenPosition, Trade, Direction, QuantityType, SignalType


class BacktestDataBaseManager(TradingDataBaseInterface):


    def __init__(self, db_path: str):
        """
        Initializes the BacktestDataBaseManager with a SQLite database connection.
        If the database or tables do not exist, they will be created.
        Args:
            db_path (str): Path to the SQLite database file.
        """
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;") 
        self.cursor = self.conn.cursor()
        self._create_tables()
        self.current_run_id = None

    def close(self):
        """
        Closes the database connection. Should be called when the database is no longer needed to free up resources.
        """
        if self.conn:
            self.conn.close()


    def commit(self):
        """Manual commit to allow batching during backtests."""
        self.conn.commit()


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
            direction TEXT NOT NULL,
            date TIMESTAMP,
            signal_type TEXT NOT NULL,
            price REAL,
            confidence REAL,
            reason TEXT,
            FOREIGN KEY(run_id) REFERENCES backtest_run(run_id)
        );

        CREATE TABLE IF NOT EXISTS open_position (
            open_position_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            gross_result REAL,
            commission REAL,
            net_result REAL,
            entry_signal_id INTEGER,
            exit_signal_id INTEGER,
            FOREIGN KEY(run_id) REFERENCES backtest_run(run_id),
            FOREIGN KEY(entry_signal_id) REFERENCES signal(signal_id),
            FOREIGN KEY(exit_signal_id) REFERENCES signal(signal_id)
        );

        CREATE INDEX IF NOT EXISTS idx_signal_run ON signal(run_id);
        CREATE INDEX IF NOT EXISTS idx_position_run ON open_position(run_id);
        CREATE INDEX IF NOT EXISTS idx_trade_run ON trade(run_id);
        CREATE INDEX IF NOT EXISTS idx_trade_entry_sig ON trade(entry_signal_id);
        """
        self.cursor.executescript(schema)
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

        # Store the current run ID for use in subsequent operations
        self.current_run_id = cur.lastrowid
        return self.current_run_id
    

    def close_backtest_run(self):
        """
        Updates the end_time of a backtest run to mark its completion.
        This method should be called when a backtest run is finished to record the end time.
        """
        self.cursor.execute(
            """
            UPDATE backtest_run
            SET end_time = ?
            WHERE run_id = ?
            """,
            (datetime.now(timezone.utc), self.current_run_id),
        )
        self.conn.commit()


    def get_backtest_run(self):
        """
        Retrieves a backtest run by its ID.
        Returns:
            dict: A dictionary containing the backtest run details, or None if not found.
        """

        self.cursor.execute("SELECT * FROM backtest_run WHERE run_id = ?", (self.current_run_id,))
        row = self.cursor.fetchone()
        
        if row:
            data = dict(row)
            data['parameters'] = json.loads(data['parameters']) 
            return data
        return None


    def insert_signal(self, signal: Signal) -> int:
        """
        Inserts a new signal into the database and assigns its ID.
        This method does not commit changes, the caller must handle committing.
        Args:
            signal (Signal): The Signal object to insert.
        Returns:
            int: The ID of the newly created signal.
        """

        date_value = signal.date.to_pydatetime() if isinstance(signal.date, pd.Timestamp) else signal.date

        cur = self.cursor.execute(
            """
            INSERT INTO signal (
                run_id,
                stock,
                direction,
                date,
                signal_type,
                price,
                confidence,
                reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.current_run_id,
                signal.stock,
                signal.direction.value,
                date_value,
                signal.signal_type.value,
                signal.price,
                signal.confidence,
                signal.reason,
            ),
        )

        signal.signal_id = cur.lastrowid
        return signal.signal_id


    def insert_open_position(self, position: OpenPosition) -> int:
        """
        Inserts a new open position into the database and assigns its ID.
        This method does not commit changes, the caller must handle committing.
        Args:
            position (OpenPosition): The OpenPosition object to insert.
        Returns:
            int: The ID of the newly created open position.
        """

        date_value = position.date.to_pydatetime() if isinstance(position.date, pd.Timestamp) else position.date

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
                self.current_run_id,
                position.stock,
                position.direction.value,
                date_value,
                position.entry_price,
                position.quantity_type.value,
                position.quantity,
                position.entry_signal_id,
            ),
        )
        
        position.open_position_id = cur.lastrowid
        return position.open_position_id


    def _insert_trade(self, trade: Trade) -> int:
        """
        Inserts a new trade into the database and assigns its ID.
        This method is intended for internal use when closing positions, as it does not handle deleting the open position or committing the transaction.
        This method does not commit changes, the caller must handle committing.
        Args:
            trade (Trade): The Trade object to insert.
        Returns:
            int: The ID of the newly created trade.
        """

        entry_date_value = trade.entry_date.to_pydatetime() if isinstance(trade.entry_date, pd.Timestamp) else trade.entry_date
        exit_date_value = trade.exit_date.to_pydatetime() if isinstance(trade.exit_date, pd.Timestamp) else trade.exit_date

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
                gross_result,
                commission,
                net_result,
                entry_signal_id,
                exit_signal_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.current_run_id,
                trade.stock,
                trade.direction.value,
                trade.quantity_type.value,
                trade.quantity,
                trade.entry_price,
                trade.exit_price,
                entry_date_value,
                exit_date_value,
                trade.gross_result,
                trade.commission,
                trade.net_result,
                trade.entry_signal_id,
                trade.exit_signal_id

            ),
        )
        trade.trade_id = cur.lastrowid
        return trade.trade_id
    

    def close_open_position(self, open_position_id: int, trade: Trade):
        """
        Closes an open position by inserting a corresponding trade and deleting the open position.
        This method handles the entire transaction, including committing changes. If any part of the process fails, it will roll back to maintain data integrity.
        Args:
            open_position_id (int): The ID of the open position to close.
            trade (Trade): The Trade object representing the closed trade.
        Returns:
            int: The ID of the newly created trade.
        """

        try:
            trade_id = self._insert_trade(trade)
            
            self.cursor.execute(
                "DELETE FROM open_position WHERE open_position_id = ?", 
                (open_position_id,)
            )
            
            self.conn.commit()
            return trade_id

        except Exception as e:
            self.conn.rollback()  # Undo the trade insert if delete fails
            print(f"Error closing position: {e}")
            raise e


    def get_signals_for_run(self, run_id: int, start_date: datetime | None = None, end_date: datetime | None = None):
        """
        Retrieves all signals for a given backtest run within the specified date range.
        If no date range is provided, all signals for the run will be returned.
        Args:
            run_id (int): The ID of the backtest run.
        Returns:
            list: A list of Signal objects associated with the run.
        """

        query = "SELECT * FROM signal WHERE run_id = ?"
        params = [run_id]

        if start_date:
            query += " AND date >= ?"
            params.append(start_date)

        if end_date:
            query += " AND date <= ?"
            params.append(end_date)

        self.cursor.execute(query, tuple(params))
        rows = self.cursor.fetchall()
        signals = []
        for row in rows:
            data = dict(row)
            signal = Signal(
                stock=data['stock'],
                signal_type=SignalType(data['signal_type']),
                direction=Direction(data['direction']),
                date=pd.Timestamp(data['date']),
                price=data['price'],
                confidence=data['confidence'],
                reason=data['reason'],
                signal_id=data['signal_id'],
                run_id=data['run_id']
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
                date=pd.Timestamp(data['date']),
                entry_price=data['entry_price'],
                quantity_type=QuantityType(data['quantity_type']),
                quantity=data['quantity'],
                entry_signal_id=data['entry_signal_id'],
                open_position_id=data['open_position_id'],
                run_id=data['run_id']
            )
            positions.append(position)
        return positions
    

    def get_trades_for_run(self, run_id: int, start_date: datetime | None = None, end_date: datetime | None = None):
        """
        Retrieves all trades for a given backtest run within the specified date range.
        If no date range is provided, all trades for the run will be returned.
        Args:
            run_id (int): The ID of the backtest run.
        Returns:
            list: A list of Trade objects associated with the run.
        """

        query = "SELECT * FROM trade WHERE run_id = ?"
        params = [run_id]

        if start_date:
            query += " AND exit_date >= ?"
            params.append(start_date)

        if end_date:
            query += " AND exit_date <= ?"
            params.append(end_date)

        self.cursor.execute(query, tuple(params))
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
                entry_date=pd.Timestamp(data['entry_date']),
                exit_date=pd.Timestamp(data['exit_date']),
                gross_result=data['gross_result'],
                commission=data['commission'],
                net_result=data['net_result'],
                entry_signal_id=data['entry_signal_id'],
                exit_signal_id=data['exit_signal_id'],
                trade_id=data['trade_id'],
                run_id=data['run_id']
            )
            trades.append(trade)
        return trades

        