"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: live_trader_db_impl.py
Description: 
    This module implements the TradingDataBaseInterface for live trading scenarios using SQLite.
    It provides methods to manage signals, open positions, and trades in a live trading environment,
    ensuring that all operations are immediately reflected in the database to maintain an accurate record of live trading activity.

Author: Albert Marín Blasco
Date Created: 2025-06-25
Last Modified: 2026-02-19
"""

import sqlite3
import pandas as pd

from ..interfaces import TradingDataBaseInterface
from Domain import Signal, OpenPosition, Trade, Direction, QuantityType, SignalType


class LiveTraderDataBaseManager(TradingDataBaseInterface):


    def __init__(self, db_path: str):
        """
        Initializes the LiveTraderDataBaseManager with a SQLite database connection.
        If the database or tables do not exist, they will be created.
        Args:
            db_path (str): Path to the SQLite database file.
        """
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.cursor = self.conn.cursor()
        self._create_tables()


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
        """Initialize database tables if they do not exist."""
        schema = """
        CREATE TABLE IF NOT EXISTS signal (
            signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock TEXT NOT NULL,
            direction TEXT NOT NULL,
            date TIMESTAMP,
            signal_type TEXT NOT NULL,
            price REAL,
            confidence REAL,
            reason TEXT
        );

        CREATE TABLE IF NOT EXISTS open_position (
            open_position_id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock TEXT NOT NULL,
            direction TEXT NOT NULL,
            date TIMESTAMP,
            entry_price REAL,
            quantity_type TEXT NOT NULL,
            quantity REAL,
            entry_signal_id INTEGER,
            FOREIGN KEY(entry_signal_id) REFERENCES signal(signal_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS trade (
            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            FOREIGN KEY(entry_signal_id) REFERENCES signal(signal_id) ON DELETE CASCADE,
            FOREIGN KEY(exit_signal_id) REFERENCES signal(signal_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_signal_date ON signal(date);
        CREATE INDEX IF NOT EXISTS idx_open_position_date ON open_position(date);
        CREATE INDEX IF NOT EXISTS idx_trade_entry_date ON trade(entry_date);
        CREATE INDEX IF NOT EXISTS idx_trade_exit_date ON trade(exit_date);
        """
        self.cursor.executescript(schema)
        self.conn.commit()


    def insert_signal(self, signal: Signal) -> int:
        """
        Inserts a signal into the database and returns the generated signal_id.
        This method does not commit changes, the caller must handle committing.
        Args:
            signal (Signal): The Signal object to be inserted.
        Returns:
            int: The generated signal_id for the inserted signal.
        """
        date_value = signal.date.to_pydatetime() if isinstance(signal.date, pd.Timestamp) else signal.date

        cur = self.cursor.execute(
            """
            INSERT INTO signal (
                stock,
                direction,
                date,
                signal_type,
                price,
                confidence,
                reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
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
    

    def insert_open_position(self, open_position: OpenPosition) -> int:
        """
        Inserts an open position into the database and returns the generated open_position_id.
        This method does not commit changes, the caller must handle committing.
        Args:
            open_position (OpenPosition): The OpenPosition object to be inserted.
        Returns:
            int: The generated open_position_id for the inserted open position.
        """
        date_value = open_position.date.to_pydatetime() if isinstance(open_position.date, pd.Timestamp) else open_position.date

        cur = self.cursor.execute(
            """
            INSERT INTO open_position (
                stock,
                direction,
                date,
                entry_price,
                quantity_type,
                quantity,
                entry_signal_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                open_position.stock,
                open_position.direction.value,
                date_value,
                open_position.entry_price,
                open_position.quantity_type.value,
                open_position.quantity,
                open_position.entry_signal_id
            ),
        )

        open_position.open_position_id = cur.lastrowid
        return open_position.open_position_id
    

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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
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
        This method handles the entire transaction, including committing changes.
        If the open_position_id does not exist, it will roll back to maintain data integrity.
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

            if self.cursor.rowcount == 0:
                raise ValueError(f"Open position with ID {open_position_id} does not exist.")
            
            self.conn.commit()
            return trade_id

        except Exception as e:
            self.conn.rollback()  # Undo the trade insert if delete fails
            print(f"Error closing position: {e}")
            raise e


    def get_open_positions(self) -> list[OpenPosition]:
        """
        Retrieves all open positions from the database and returns them as a list of OpenPosition objects.
        Returns:
            list[OpenPosition]: A list of OpenPosition objects representing the current open positions in the database.
        """
        self.cursor.execute("SELECT * FROM open_position")
        rows = self.cursor.fetchall()
        open_positions = []
        for row in rows:
            open_position = OpenPosition(
                stock=row["stock"],
                direction=Direction(row["direction"]),
                date=pd.Timestamp(row["date"]),
                entry_price=row["entry_price"],
                quantity_type=QuantityType(row["quantity_type"]),
                quantity=row["quantity"],
                entry_signal_id=row["entry_signal_id"],
                open_position_id=row["open_position_id"]
            )
            open_positions.append(open_position)
        return open_positions
    

    def get_signals(self, start_date: pd.Timestamp | None = None, end_date: pd.Timestamp | None = None) -> list[Signal]:
        """
        Retrieves signals from the database within the specified date range and returns them as a list of Signal objects.
        If no date range is provided, all signals will be retrieved.
        Args:
            start_date (pd.Timestamp | None): The start date for filtering signals. If None, no lower bound is applied.
            end_date (pd.Timestamp | None): The end date for filtering signals. If None, no upper bound is applied.
        Returns:
            list[Signal]: A list of Signal objects representing the signals in the specified date range.
        """
        query = "SELECT * FROM signal"
        params = []
        if start_date and end_date:
            query += " WHERE date >= ? AND date <= ?"
            params.extend([start_date.to_pydatetime(), end_date.to_pydatetime()])
        elif start_date:
            query += " WHERE date >= ?"
            params.append(start_date.to_pydatetime())
        elif end_date:
            query += " WHERE date <= ?"
            params.append(end_date.to_pydatetime())

        self.cursor.execute(query, params)
        rows = self.cursor.fetchall()
        signals = []
        for row in rows:
            signal = Signal(
                stock=row["stock"],
                direction=Direction(row["direction"]),
                date=pd.Timestamp(row["date"]),
                signal_type=SignalType(row["signal_type"]),
                price=row["price"],
                confidence=row["confidence"],
                reason=row["reason"],
                signal_id=row["signal_id"]
            )
            signals.append(signal)
        return signals
    

    def get_trades(self, start_date: pd.Timestamp | None = None, end_date: pd.Timestamp | None = None) -> list[Trade]:
        """
        Retrieves trades from the database within the specified date range and returns them as a list of Trade objects.
        If no date range is provided, all trades will be retrieved.
        Args:
            start_date (pd.Timestamp | None): The start date for filtering trades. If None, no lower bound is applied.
            end_date (pd.Timestamp | None): The end date for filtering trades. If None, no upper bound is applied.
        Returns:
            list[Trade]: A list of Trade objects representing the trades in the specified date range.
        """
        query = "SELECT * FROM trade"
        params = []
        if start_date and end_date:
            query += " WHERE entry_date >= ? AND exit_date <= ?"
            params.extend([start_date.to_pydatetime(), end_date.to_pydatetime()])
        elif start_date:
            query += " WHERE entry_date >= ?"
            params.append(start_date.to_pydatetime())
        elif end_date:
            query += " WHERE exit_date <= ?"
            params.append(end_date.to_pydatetime())

        self.cursor.execute(query, params)
        rows = self.cursor.fetchall()
        trades = []
        for row in rows:
            trade = Trade(
                stock=row["stock"],
                direction=Direction(row["direction"]),
                quantity_type=QuantityType(row["quantity_type"]),
                quantity=row["quantity"],
                entry_price=row["entry_price"],
                exit_price=row["exit_price"],
                entry_date=pd.Timestamp(row["entry_date"]),
                exit_date=pd.Timestamp(row["exit_date"]),
                gross_result=row["gross_result"],
                commission=row["commission"],
                net_result=row["net_result"],
                entry_signal_id=row["entry_signal_id"],
                exit_signal_id=row["exit_signal_id"],
                trade_id=row["trade_id"]
            )
            trades.append(trade)
        return trades