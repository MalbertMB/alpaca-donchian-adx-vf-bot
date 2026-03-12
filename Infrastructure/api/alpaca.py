"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: alpaca.py
Description:
    This module implements the AlpacaAPI class, which serves as a thin, error-aware
    wrapper around the alpaca-py SDK.  It exposes all operations required by the
    trading bot: historical OHLCV bars, market calendar, account info, open
    positions, market/limit order placement, position closure, and asset listing.

    Return types are either plain Python objects (alpaca-py models) or pandas
    DataFrames normalised to match the schema expected by MarketDatabase so that
    callers never have to touch the SDK directly.

Author: Albert Marín
Date Created: 2025-06-25
Last Modified: 2026-03-04
"""

import pandas as pd

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    GetAssetsRequest,
    GetCalendarRequest,
    LimitOrderRequest,
    MarketOrderRequest,
)
from alpaca.trading.enums import AssetClass, AssetStatus, OrderSide, TimeInForce
from alpaca.trading.models import Asset, Order, Position, TradeAccount
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestBarRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.common.exceptions import APIError


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class AlpacaAPIError(Exception):
    """Raised when the Alpaca API returns an unexpected error."""


class AlpacaAuthError(AlpacaAPIError):
    """Raised when authentication with the Alpaca API fails."""


class AlpacaDataError(AlpacaAPIError):
    """Raised when a data-retrieval operation fails."""


class AlpacaOrderError(AlpacaAPIError):
    """Raised when an order operation fails."""


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class AlpacaAPI:
    """
    Thin, error-aware wrapper around the alpaca-py SDK.

    All public methods raise a subclass of AlpacaAPIError on failure so that
    callers never have to import alpaca-py directly.

    Args:
        api_key (str): Alpaca API key ID.
        secret_key (str): Alpaca secret key.
        paper (bool): If True (default) uses the paper-trading endpoint.
    """

    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        self.api_key    = api_key
        self.secret_key = secret_key
        self.paper      = paper

        try:
            self._trading = TradingClient(
                api_key    = api_key,
                secret_key = secret_key,
                paper      = paper,
            )
            self._data = StockHistoricalDataClient(
                api_key    = api_key,
                secret_key = secret_key,
            )
        except APIError as exc:
            raise AlpacaAuthError(f"Failed to initialise Alpaca clients: {exc}") from exc


    # ------------------------------------------------------------------
    # Account
    # ------------------------------------------------------------------

    def get_account(self) -> TradeAccount:
        """
        Returns the current account information (balance, buying power, etc.).

        Returns:
            TradeAccount: The alpaca-py TradeAccount object.
        Raises:
            AlpacaAPIError: If the request fails.
        """
        try:
            return self._trading.get_account()
        except APIError as exc:
            raise AlpacaAPIError(f"Failed to retrieve account info: {exc}") from exc


    # ------------------------------------------------------------------
    # Market Data
    # ------------------------------------------------------------------

    def get_historical_bars(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        timeframe: TimeFrame = TimeFrame.Day,
    ) -> dict[str, pd.DataFrame]:
        """
        Fetches OHLCV bars for one or more symbols over a date range.

        The returned DataFrames are indexed by a timezone-aware DatetimeIndex and
        contain exactly the columns expected by MarketDatabase.insert_ohlcv_data:
        ``open``, ``high``, ``low``, ``close``, ``volume``.

        Args:
            symbols (list[str]): One or more ticker symbols.
            start (pd.Timestamp): Inclusive start of the range (tz-aware).
            end (pd.Timestamp):   Inclusive end of the range (tz-aware).
            timeframe (TimeFrame): Bar resolution (default: TimeFrame.Day).

        Returns:
            dict[str, pd.DataFrame]: Maps each symbol to its OHLCV DataFrame.
                                     Missing symbols are absent from the dict.
        Raises:
            AlpacaDataError: If the request fails.
        """
        try:
            request = StockBarsRequest(
                symbol_or_symbols = symbols,
                timeframe         = timeframe,
                start             = start,
                end               = end,
            )
            bar_set = self._data.get_stock_bars(request)
            raw_df  = bar_set.df  # MultiIndex (symbol, timestamp) when >1 symbol

        except APIError as exc:
            raise AlpacaDataError(
                f"Failed to fetch historical bars for {symbols}: {exc}"
            ) from exc

        return self._split_bar_df(raw_df, symbols)


    def get_latest_bars(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        """
        Fetches the most recent bar for each symbol.

        Returns:
            dict[str, pd.DataFrame]: Maps each symbol to a single-row OHLCV DataFrame.
        Raises:
            AlpacaDataError: If the request fails.
        """
        try:
            request = StockLatestBarRequest(symbol_or_symbols=symbols)
            latest  = self._data.get_stock_latest_bar(request)
        except APIError as exc:
            raise AlpacaDataError(
                f"Failed to fetch latest bars for {symbols}: {exc}"
            ) from exc

        result: dict[str, pd.DataFrame] = {}
        for symbol, bar in latest.items():
            result[symbol] = pd.DataFrame(
                [{
                    "open":   bar.open,
                    "high":   bar.high,
                    "low":    bar.low,
                    "close":  bar.close,
                    "volume": bar.volume,
                }],
                index=pd.DatetimeIndex([bar.timestamp], name="date"),
            )
        return result


    def get_calendar(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """
        Returns the market calendar (trading days) for the given range.

        The returned DataFrame is indexed by a DatetimeIndex (date only, no time)
        and has a single boolean column ``open`` — True for every trading day.
        This matches the format consumed by MarketDatabase._insert_stock_calendar.

        Args:
            start (pd.Timestamp): Start date of the range.
            end (pd.Timestamp):   End date of the range.

        Returns:
            pd.DataFrame: Calendar DataFrame with column ``open``.
        Raises:
            AlpacaDataError: If the request fails.
        """
        try:
            calendar = self._trading.get_calendar(
                filters=GetCalendarRequest(
                    start=start.date(),
                    end=end.date(),
                )
            )
        except APIError as exc:
            raise AlpacaDataError(
                f"Failed to fetch market calendar ({start} – {end}): {exc}"
            ) from exc

        if not calendar:
            return pd.DataFrame(columns=["open"])

        dates = pd.DatetimeIndex(
            [pd.Timestamp(day.date) for day in calendar], name="date"
        )
        return pd.DataFrame({"open": True}, index=dates)


    # ------------------------------------------------------------------
    # Assets
    # ------------------------------------------------------------------

    def get_tradeable_assets(
        self,
        asset_class: AssetClass = AssetClass.US_EQUITY,
    ) -> list[Asset]:
        """
        Returns all tradeable, active assets for the given asset class.

        Args:
            asset_class (AssetClass): Asset class to filter by (default: US equities).

        Returns:
            list[Asset]: List of alpaca-py Asset objects.
        Raises:
            AlpacaDataError: If the request fails.
        """
        try:
            return self._trading.get_all_assets(
                GetAssetsRequest(
                    asset_class = asset_class,
                    status      = AssetStatus.ACTIVE,
                )
            )
        except APIError as exc:
            raise AlpacaDataError(
                f"Failed to fetch assets for {asset_class}: {exc}"
            ) from exc


    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    def get_positions(self) -> list[Position]:
        """
        Returns all currently open positions in the account.

        Returns:
            list[Position]: List of alpaca-py Position objects.
        Raises:
            AlpacaAPIError: If the request fails.
        """
        try:
            return self._trading.get_all_positions()
        except APIError as exc:
            raise AlpacaAPIError(f"Failed to retrieve open positions: {exc}") from exc


    def close_position(self, symbol: str) -> Order:
        """
        Closes the entire open position for a given symbol at market price.

        Args:
            symbol (str): Ticker of the position to close.

        Returns:
            Order: The resulting closing order.
        Raises:
            AlpacaOrderError: If the closure fails (e.g. no open position).
        """
        try:
            return self._trading.close_position(symbol)
        except APIError as exc:
            raise AlpacaOrderError(
                f"Failed to close position for {symbol}: {exc}"
            ) from exc


    def close_all_positions(self, cancel_orders: bool = True) -> None:
        """
        Liquidates all open positions at market price.

        Args:
            cancel_orders (bool): Also cancel all open orders first (default: True).
        Raises:
            AlpacaOrderError: If the operation fails.
        """
        try:
            self._trading.close_all_positions(cancel_orders=cancel_orders)
        except APIError as exc:
            raise AlpacaOrderError(f"Failed to close all positions: {exc}") from exc


    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    def place_market_order(
        self,
        symbol: str,
        qty: float,
        side: OrderSide,
        time_in_force: TimeInForce = TimeInForce.DAY,
    ) -> Order:
        """
        Places a market order.

        Args:
            symbol (str): Ticker to trade.
            qty (float): Number of shares (fractional supported).
            side (OrderSide): OrderSide.BUY or OrderSide.SELL.
            time_in_force (TimeInForce): Validity of the order (default: DAY).

        Returns:
            Order: The created alpaca-py Order object.
        Raises:
            AlpacaOrderError: If the order is rejected or the request fails.
        """
        try:
            request = MarketOrderRequest(
                symbol        = symbol,
                qty           = qty,
                side          = side,
                time_in_force = time_in_force,
            )
            return self._trading.submit_order(request)
        except APIError as exc:
            raise AlpacaOrderError(
                f"Market order failed ({side.value} {qty} {symbol}): {exc}"
            ) from exc


    def place_limit_order(
        self,
        symbol: str,
        qty: float,
        side: OrderSide,
        limit_price: float,
        time_in_force: TimeInForce = TimeInForce.DAY,
    ) -> Order:
        """
        Places a limit order.

        Args:
            symbol (str): Ticker to trade.
            qty (float): Number of shares (fractional supported).
            side (OrderSide): OrderSide.BUY or OrderSide.SELL.
            limit_price (float): Maximum (buy) or minimum (sell) execution price.
            time_in_force (TimeInForce): Validity of the order (default: DAY).

        Returns:
            Order: The created alpaca-py Order object.
        Raises:
            AlpacaOrderError: If the order is rejected or the request fails.
        """
        try:
            request = LimitOrderRequest(
                symbol        = symbol,
                qty           = qty,
                side          = side,
                limit_price   = limit_price,
                time_in_force = time_in_force,
            )
            return self._trading.submit_order(request)
        except APIError as exc:
            raise AlpacaOrderError(
                f"Limit order failed ({side.value} {qty} {symbol} @ {limit_price}): {exc}"
            ) from exc
        
    def close_market_order(self, symbol: str) -> Order:
        """
        Closes the entire open position for a given symbol at market price.

        Args:
            symbol (str): Ticker of the position to close.
        Returns:
            Order: The resulting closing order.
        Raises:
            AlpacaOrderError: If the closure fails (e.g. no open position).
        """
        try:
            return self._trading.close_position(symbol)
        except APIError as exc:
            raise AlpacaOrderError(
                f"Failed to close position for {symbol}: {exc}"
            ) from exc
        

    def get_open_orders(self) -> list[Order]:
        """
        Retrieves all currently open orders in the account.

        Returns:
            list[Order]: List of alpaca-py Order objects.
        Raises:
            AlpacaAPIError: If the request fails.
        """
        try:
            return self._trading.get_all_orders(status="open")
        except APIError as exc:
            raise AlpacaAPIError(f"Failed to retrieve open orders: {exc}") from exc


    def cancel_all_orders(self) -> None:
        """
        Cancels all open orders in the account.

        Raises:
            AlpacaOrderError: If the operation fails.
        """
        try:
            self._trading.cancel_orders()
        except APIError as exc:
            raise AlpacaOrderError(f"Failed to cancel all orders: {exc}") from exc


    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _split_bar_df(
        raw_df: pd.DataFrame,
        symbols: list[str],
    ) -> dict[str, pd.DataFrame]:
        """
        Converts the multi-symbol DataFrame returned by alpaca-py into a dict
        mapping each symbol to a normalised OHLCV DataFrame.

        alpaca-py returns a MultiIndex (symbol, timestamp) DataFrame when
        multiple symbols are requested, and a simple timestamp index for one.
        Both cases are handled here.
        """
        keep_cols = ["open", "high", "low", "close", "volume"]
        result: dict[str, pd.DataFrame] = {}

        if raw_df.empty:
            return result

        if isinstance(raw_df.index, pd.MultiIndex):
            for symbol in symbols:
                if symbol not in raw_df.index.get_level_values(0):
                    continue
                df = raw_df.xs(symbol, level=0)[keep_cols].copy()
                df.index.name = "date"
                result[symbol] = df
        else:
            # Single symbol — index is already the timestamp
            symbol = symbols[0]
            df = raw_df[keep_cols].copy()
            df.index.name = "date"
            result[symbol] = df

        return result

    