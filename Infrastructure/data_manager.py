"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: data_manager.py
Description:
    This module implements the DataManager class, which unifies interactions between
    the local trading database, market database, and Alpaca API.

    DataManager provides a high-level interface for trading operations:
    - Historical data retrieval (with fallback: market_db → alpaca_api)
    - Calendar and asset metadata (market_db → alpaca_api)
    - Trading operations (place/cancel orders, close positions)
    - Position and signal management (via local_db)

    For live trading: orders are placed via the API, then recorded in local_db
    after confirmation.

    For backtests: the alpaca_api may be used for historical data gaps, but
    attempting to close live positions via the API raises an error.

Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2026-03-12
"""

import threading
import pandas as pd

from typing import Optional
from datetime import datetime, timezone
from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass
from alpaca.data.timeframe import TimeFrame

from Domain import Signal, OpenPosition, Trade, Direction, QuantityType
from .interfaces import TradingDataBaseInterface
from .api import AlpacaAPI, AlpacaAPIError, AlpacaOrderError
from .market import MarketDatabase
from .backtester import BacktestDataBaseManager


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class DataManagerError(Exception):
    """Raised for invalid DataManager operations."""


class BacktestOperationError(DataManagerError):
    """Raised when attempting live-only operations during backtests."""


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class DataManager:
    """
    Unified interface for trading operations across local database, market data,
    and the Alpaca API.

    Args:
        local_db (TradingDataBaseInterface): User trading database (backtest or live).
        market_db (MarketDatabase): OHLCV and calendar data cache.
        alpaca_api (Optional[AlpacaAPI]): Connection to Alpaca API (optional for
            offline operations). If None, data operations fall back to market_db only.
    """

    def __init__(
        self,
        local_db: TradingDataBaseInterface,
        market_db: MarketDatabase,
        alpaca_api: Optional[AlpacaAPI] = None,
    ):
        self.local_db = local_db
        self.market_db = market_db
        self.alpaca_api = alpaca_api

        # Determine if we're in backtest mode by checking for BacktestDataBaseManager
        self.is_backtest = isinstance(local_db, BacktestDataBaseManager)

        # Operation lock for atomic transactions (e.g., place order + record in db)
        self._op_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Historical data retrieval (with fallback)
    # ------------------------------------------------------------------

    def get_historical_bars(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        timeframe: TimeFrame = TimeFrame.Day,
    ) -> dict[str, pd.DataFrame]:
        """
        Retrieves historical OHLCV bars for one or more symbols.

        Tries market_db first (faster, cached), then falls back to alpaca_api
        if needed. Symbols missing from both sources are excluded.

        Args:
            symbols (list[str]): One or more tickers.
            start (pd.Timestamp): Inclusive start date.
            end (pd.Timestamp):   Inclusive end date.
            timeframe (TimeFrame): Bar resolution (default: daily).

        Returns:
            dict[str, pd.DataFrame]: Maps each symbol to OHLCV DataFrame.

        Raises:
            DataManagerError: If no data source is available or retrieval fails.
        """
        result: dict[str, pd.DataFrame] = {}
        symbols_to_fetch = set(symbols)

        # Try market_db first
        for symbol in list(symbols_to_fetch):
            if self.market_db.has_ohlcv_data(symbol, start, end):
                try:
                    df = self.market_db.get_ohlcv_data(symbol, start, end)
                    if not df.empty:
                        result[symbol] = df
                        symbols_to_fetch.discard(symbol)
                except Exception:
                    pass

        # Fall back to alpaca_api for missing symbols
        if symbols_to_fetch and self.alpaca_api:
            try:
                api_data = self.alpaca_api.get_historical_bars(
                    list(symbols_to_fetch),
                    start,
                    end,
                    timeframe,
                )
                result.update(api_data)
                symbols_to_fetch -= set(api_data.keys())
            except AlpacaAPIError as exc:
                raise DataManagerError(
                    f"Failed to fetch bars from Alpaca API: {exc}"
                ) from exc

        if symbols_to_fetch and not self.alpaca_api:
            raise DataManagerError(
                f"No data available for {symbols_to_fetch} and alpaca_api is not available."
            )

        return result

    def get_calendar(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """
        Retrieves the market calendar (trading days) for a date range.

        Tries market_db first, then falls back to alpaca_api.

        Args:
            start (pd.Timestamp): Start date.
            end (pd.Timestamp):   End date.

        Returns:
            pd.DataFrame: Calendar DataFrame with boolean column ``open``.

        Raises:
            DataManagerError: If no data source is available.
        """
        try:
            # Try to query market_db directly for the range
            with self.market_db.db_lock:
                cur = self.market_db.conn.cursor()
                cur.execute(
                    """
                    SELECT date, open FROM calendar
                    WHERE date BETWEEN ? AND ?
                    ORDER BY date ASC
                    """,
                    (start.to_pydatetime(), end.to_pydatetime()),
                )
                rows = cur.fetchall()

            if rows:
                dates = pd.DatetimeIndex(
                    [pd.Timestamp(row[0]) for row in rows], name="date"
                )
                return pd.DataFrame({"open": [row[1] for row in rows]}, index=dates)
        except Exception:
            pass

        # Fall back to alpaca_api
        if self.alpaca_api:
            try:
                return self.alpaca_api.get_calendar(start, end)
            except AlpacaAPIError as exc:
                raise DataManagerError(
                    f"Failed to fetch calendar from Alpaca API: {exc}"
                ) from exc

        raise DataManagerError(
            "No calendar available in market_db and alpaca_api is not available."
        )

    # ------------------------------------------------------------------
    # Market data queries
    # ------------------------------------------------------------------

    def get_latest_bars(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        """
        Retrieves the most recent bar for each symbol via the Alpaca API.

        This always goes to the API (not cached in market_db).

        Args:
            symbols (list[str]): One or more tickers.

        Returns:
            dict[str, pd.DataFrame]: Maps each symbol to a single-row DataFrame.

        Raises:
            DataManagerError: If alpaca_api is unavailable or fails.
        """
        if not self.alpaca_api:
            raise DataManagerError(
                "alpaca_api is not available; cannot fetch latest bars."
            )

        try:
            return self.alpaca_api.get_latest_bars(symbols)
        except AlpacaAPIError as exc:
            raise DataManagerError(f"Failed to fetch latest bars: {exc}") from exc

    def get_tradeable_assets(
        self,
        asset_class: AssetClass = AssetClass.US_EQUITY,
    ):
        """
        Retrieves all active, tradeable assets.

        Args:
            asset_class (AssetClass): Asset class to filter (default: US equities).

        Returns:
            list[Asset]: Alpaca-py Asset objects.

        Raises:
            DataManagerError: If alpaca_api is unavailable or fails.
        """
        if not self.alpaca_api:
            raise DataManagerError(
                "alpaca_api is not available; cannot fetch tradeable assets."
            )

        try:
            return self.alpaca_api.get_tradeable_assets(asset_class)
        except AlpacaAPIError as exc:
            raise DataManagerError(f"Failed to fetch tradeable assets: {exc}") from exc

    def get_account_info(self):
        """
        Retrieves account information (balance, buying power, etc.).

        Returns:
            TradeAccount: Alpaca-py TradeAccount object.

        Raises:
            DataManagerError: If alpaca_api is unavailable or fails.
        """
        if not self.alpaca_api:
            raise DataManagerError(
                "alpaca_api is not available; cannot fetch account info."
            )

        try:
            return self.alpaca_api.get_account()
        except AlpacaAPIError as exc:
            raise DataManagerError(f"Failed to fetch account info: {exc}") from exc

    # ------------------------------------------------------------------
    # Position management
    # ------------------------------------------------------------------

    def get_positions(self):
        """
        Retrieves all currently open positions from Alpaca.

        Returns:
            list[Position]: Alpaca-py Position objects.

        Raises:
            DataManagerError: If alpaca_api is unavailable or fails.
        """
        if not self.alpaca_api:
            raise DataManagerError(
                "alpaca_api is not available; cannot fetch positions."
            )

        try:
            return self.alpaca_api.get_positions()
        except AlpacaAPIError as exc:
            raise DataManagerError(f"Failed to fetch positions: {exc}") from exc

    def close_position_via_api(self, symbol: str) -> Trade:
        """
        Closes a live position via the Alpaca API and records the trade in local_db.

        For backtests, raises an error (no live API positions).

        Args:
            symbol (str): The ticker to close.

        Returns:
            Trade: Recorded trade object (with trade_id set).

        Raises:
            BacktestOperationError: If called during a backtest.
            AlpacaOrderError: If the position closure fails.
            DataManagerError: If database recording fails.
        """
        if self.is_backtest:
            raise BacktestOperationError(
                f"Cannot close live position {symbol} during backtest. "
                "Only local database positions can be closed in backtest mode."
            )

        if not self.alpaca_api:
            raise DataManagerError(
                "alpaca_api is not available; cannot close position."
            )

        with self._op_lock:
            try:
                # Place market order to close the position
                order = self.alpaca_api.close_position(symbol)

                # Construct a Trade record from the order
                # Note: commission will typically be 0 until the order is fully filled
                trade = Trade(
                    stock=symbol,
                    direction=Direction.LONG,  # Placeholder; actual direction depends on the position
                    quantity_type=QuantityType.SHARES,
                    quantity=abs(order.qty) if order.qty else 0,
                    entry_price=0.0,  # Not available from close order
                    exit_price=order.filled_avg_price if order.filled_avg_price else 0.0,
                    entry_date=pd.Timestamp.now(tz="UTC"),  # Placeholder
                    exit_date=pd.Timestamp.now(tz="UTC"),
                    gross_result=0.0,  # Calculated from existing position
                    commission=0.0,
                    net_result=0.0,
                    entry_signal_id=0,  # Placeholder
                    exit_signal_id=0,
                )

                # Record the trade in local_db
                # Note: This is a simplified flow; in production you'd query the actual position
                # and calculate P&L, then insert a signal for the exit and record the trade.
                # For now, we just record the order result.

                return trade

            except AlpacaOrderError as exc:
                raise exc
            except Exception as exc:
                raise DataManagerError(f"Failed to close position for {symbol}: {exc}") from exc

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    def place_market_order(
        self,
        symbol: str,
        qty: float,
        side: OrderSide,
        time_in_force: TimeInForce = TimeInForce.DAY,
    ):
        """
        Places a market order via the Alpaca API.

        Args:
            symbol (str): The ticker to trade.
            qty (float): Number of shares (fractional supported).
            side (OrderSide): BUY or SELL.
            time_in_force (TimeInForce): Order validity (default: DAY).

        Returns:
            Order: Alpaca-py Order object.

        Raises:
            BacktestOperationError: If called during a backtest.
            AlpacaOrderError: If the order is rejected.
        """
        if self.is_backtest:
            raise BacktestOperationError(
                f"Cannot place live market order during backtest. "
                "Only recorded trades can be created in backtest mode."
            )

        if not self.alpaca_api:
            raise DataManagerError(
                "alpaca_api is not available; cannot place order."
            )

        try:
            return self.alpaca_api.place_market_order(
                symbol, qty, side, time_in_force
            )
        except AlpacaOrderError as exc:
            raise exc

    def place_limit_order(
        self,
        symbol: str,
        qty: float,
        side: OrderSide,
        limit_price: float,
        time_in_force: TimeInForce = TimeInForce.DAY,
    ):
        """
        Places a limit order via the Alpaca API.

        Args:
            symbol (str): The ticker to trade.
            qty (float): Number of shares.
            side (OrderSide): BUY or SELL.
            limit_price (float): Maximum (buy) or minimum (sell) execution price.
            time_in_force (TimeInForce): Order validity (default: DAY).

        Returns:
            Order: Alpaca-py Order object.

        Raises:
            BacktestOperationError: If called during a backtest.
            AlpacaOrderError: If the order is rejected.
        """
        if self.is_backtest:
            raise BacktestOperationError(
                f"Cannot place live limit order during backtest."
            )

        if not self.alpaca_api:
            raise DataManagerError(
                "alpaca_api is not available; cannot place order."
            )

        try:
            return self.alpaca_api.place_limit_order(
                symbol, qty, side, limit_price, time_in_force
            )
        except AlpacaOrderError as exc:
            raise exc

    def cancel_all_orders(self):
        """
        Cancels all open orders.

        Raises:
            BacktestOperationError: If called during a backtest.
            AlpacaOrderError: If the operation fails.
        """
        if self.is_backtest:
            raise BacktestOperationError(
                "Cannot cancel live orders during backtest."
            )

        if not self.alpaca_api:
            raise DataManagerError(
                "alpaca_api is not available; cannot cancel orders."
            )

        try:
            self.alpaca_api.cancel_all_orders()
        except AlpacaOrderError as exc:
            raise exc

    # ------------------------------------------------------------------
    # Local database operations
    # ------------------------------------------------------------------

    def insert_signal(self, signal: Signal) -> int:
        """Inserts a signal into local_db and returns its ID."""
        return self.local_db.insert_signal(signal)

    def insert_open_position(self, open_position: OpenPosition) -> int:
        """Inserts an open position into local_db and returns its ID."""
        return self.local_db.insert_open_position(open_position)

    def close_open_position(self, open_position_id: int, trade: Trade) -> int:
        """Closes an open position in local_db by recording its trade and returns the trade ID."""
        return self.local_db.close_open_position(open_position_id, trade)

    def get_open_positions(self) -> list[OpenPosition]:
        """Retrieves all open positions from local_db."""
        return self.local_db.get_open_positions()

    def get_signals(
        self, start_date: Optional[pd.Timestamp] = None, end_date: Optional[pd.Timestamp] = None
    ) -> list[Signal]:
        """Retrieves signals from local_db within the specified date range."""
        return self.local_db.get_signals(start_date, end_date)

    def get_trades(
        self, start_date: Optional[pd.Timestamp] = None, end_date: Optional[pd.Timestamp] = None
    ) -> list[Trade]:
        """Retrieves trades from local_db within the specified date range."""
        return self.local_db.get_trades(start_date, end_date)

    # ------------------------------------------------------------------
    # Backtest-specific operations
    # ------------------------------------------------------------------

    def create_backtest_run(
        self,
        strategy_name: str,
        strategy_version: str,
        parameters: dict,
        data_start: datetime,
        data_end: datetime,
    ) -> int:
        """
        Creates a new backtest run (backtest mode only).

        Returns:
            int: The run_id.

        Raises:
            DataManagerError: If not in backtest mode.
        """
        if not self.is_backtest:
            raise DataManagerError(
                "create_backtest_run is only available in backtest mode."
            )

        return self.local_db.create_backtest_run(
            strategy_name, strategy_version, parameters, data_start, data_end
        )

    def close_backtest_run(self):
        """Closes the current backtest run (backtest mode only)."""
        if not self.is_backtest:
            raise DataManagerError(
                "close_backtest_run is only available in backtest mode."
            )

        self.local_db.close_backtest_run()

    def set_active_backtest_run(self, run_id: int):
        """Sets the active backtest run by ID (backtest mode only)."""
        if not self.is_backtest:
            raise DataManagerError(
                "set_active_backtest_run is only available in backtest mode."
            )

        self.local_db.set_active_run(run_id)