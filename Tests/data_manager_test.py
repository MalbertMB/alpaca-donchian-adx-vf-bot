"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: data_manager_test.py
Description:
    Comprehensive tests for the DataManager class. Covers:
    - Initialization in backtest and live trading modes
    - Error handling (BacktestOperationError for live operations during backtest)
    - Historical data retrieval with market_db → alpaca_api fallback
    - Context manager support
    - Thread-safe operations
    - Local database integration

Author: Albert Marín
Date Created: 2026-03-12
Last Modified: 2026-03-12
"""

import os
import time
import threading
import pandas as pd
from datetime import datetime, timezone, timedelta

from Infrastructure import (
    DataManager,
    BacktestDataBaseManager,
    LiveTraderDataBaseManager,
    MarketDatabase,
    AlpacaAPI,
    DataManagerError,
    BacktestOperationError,
)
from Domain import Signal, OpenPosition, Trade, Direction, QuantityType, SignalType


# ---------------------------------------------------------------------------
# Test Setup
# ---------------------------------------------------------------------------

BACKTEST_DB_PATH = "Infrastructure/backtester/test_dm_backtest.db"
LIVE_DB_PATH = "Infrastructure/live_trader/test_dm_live.db"
MARKET_DB_PATH = "Infrastructure/market/test_dm_market.db"


def _make_ohlcv(dates: list[str]) -> pd.DataFrame:
    """Create a minimal OHLCV DataFrame."""
    index = pd.DatetimeIndex([pd.Timestamp(d) for d in dates])
    return pd.DataFrame(
        {
            "open": [100.0 + i for i in range(len(dates))],
            "high": [105.0 + i for i in range(len(dates))],
            "low": [95.0 + i for i in range(len(dates))],
            "close": [102.0 + i for i in range(len(dates))],
            "volume": [1_000 * (i + 1) for i in range(len(dates))],
        },
        index=index,
    )


def _make_calendar(dates: list[str]) -> pd.DataFrame:
    """Create a calendar DataFrame."""
    index = pd.DatetimeIndex([pd.Timestamp(d) for d in dates])
    return pd.DataFrame({"open": [True] * len(dates)}, index=index)


def _cleanup():
    """Remove test database files."""
    for path in [BACKTEST_DB_PATH, LIVE_DB_PATH, MARKET_DB_PATH]:
        if os.path.exists(path):
            os.remove(path)


def _ensure_dirs():
    """Create necessary directories."""
    for path in [BACKTEST_DB_PATH, LIVE_DB_PATH, MARKET_DB_PATH]:
        os.makedirs(os.path.dirname(path), exist_ok=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_backtest_mode_initialization():
    print("INFO: Testing DataManager initialization in backtest mode.")

    _ensure_dirs()
    _cleanup()

    backtest_db = BacktestDataBaseManager(BACKTEST_DB_PATH)
    market_db = MarketDatabase(MARKET_DB_PATH)
    dm = DataManager(backtest_db, market_db, alpaca_api=None)

    assert dm.is_backtest is True, "Expected is_backtest=True"
    assert dm.alpaca_api is None, "Expected alpaca_api=None"

    backtest_db.close()
    market_db.close()

    print("SUCCESS: Backtest mode initialization verified.")


def test_live_mode_initialization():
    print("INFO: Testing DataManager initialization in live trading mode.")

    _ensure_dirs()
    _cleanup()

    live_db = LiveTraderDataBaseManager(LIVE_DB_PATH)
    market_db = MarketDatabase(MARKET_DB_PATH)
    dm = DataManager(live_db, market_db, alpaca_api=None)

    assert dm.is_backtest is False, "Expected is_backtest=False"
    assert dm.alpaca_api is None, "Expected alpaca_api=None"

    live_db.close()
    market_db.close()

    print("SUCCESS: Live trading mode initialization verified.")


def test_context_manager():
    print("INFO: Testing DataManager context manager support.")

    _ensure_dirs()
    _cleanup()

    with BacktestDataBaseManager(BACKTEST_DB_PATH) as backtest_db:
        with MarketDatabase(MARKET_DB_PATH) as market_db:
            dm = DataManager(backtest_db, market_db)
            assert dm.is_backtest is True

    # Verify connections are closed
    import sqlite3
    try:
        backtest_db.conn.execute("SELECT 1")
        assert False, "Expected connection to be closed"
    except sqlite3.ProgrammingError:
        pass

    print("SUCCESS: Context manager support verified.")


def test_backtest_operation_error_place_order():
    print("INFO: Testing BacktestOperationError when placing order during backtest.")

    _ensure_dirs()
    _cleanup()

    backtest_db = BacktestDataBaseManager(BACKTEST_DB_PATH)
    market_db = MarketDatabase(MARKET_DB_PATH)
    dm = DataManager(backtest_db, market_db, alpaca_api=None)

    from alpaca.trading.enums import OrderSide

    try:
        dm.place_market_order("AAPL", 10.0, OrderSide.BUY)
        assert False, "Expected BacktestOperationError"
    except BacktestOperationError as e:
        assert "backtest" in str(e).lower()

    backtest_db.close()
    market_db.close()

    print("SUCCESS: BacktestOperationError for order placement verified.")


def test_backtest_operation_error_close_position():
    print("INFO: Testing BacktestOperationError when closing position during backtest.")

    _ensure_dirs()
    _cleanup()

    backtest_db = BacktestDataBaseManager(BACKTEST_DB_PATH)
    market_db = MarketDatabase(MARKET_DB_PATH)
    dm = DataManager(backtest_db, market_db, alpaca_api=None)

    try:
        dm.close_position_via_api("AAPL")
        assert False, "Expected BacktestOperationError"
    except BacktestOperationError as e:
        assert "backtest" in str(e).lower()
        assert "cannot close" in str(e).lower()

    backtest_db.close()
    market_db.close()

    print("SUCCESS: BacktestOperationError for position closure verified.")


def test_historical_bars_from_market_db():
    print("INFO: Testing historical bars retrieval from market_db.")

    _ensure_dirs()
    _cleanup()

    backtest_db = BacktestDataBaseManager(BACKTEST_DB_PATH)
    market_db = MarketDatabase(MARKET_DB_PATH)
    dm = DataManager(backtest_db, market_db, alpaca_api=None)

    # Populate market_db with sample data
    dates = ["2025-01-02", "2025-01-03", "2025-01-06"]
    market_db.insert_ohlcv_data("AAPL", _make_ohlcv(dates))
    market_db._insert_stock_calendar(_make_calendar(dates))

    # Retrieve via DataManager
    start = pd.Timestamp("2025-01-01")
    end = pd.Timestamp("2025-01-10")
    result = dm.get_historical_bars(["AAPL"], start, end)

    assert "AAPL" in result, "Expected AAPL in result"
    assert len(result["AAPL"]) == 3, f"Expected 3 bars, got {len(result['AAPL'])}"
    assert list(result["AAPL"].columns) == ["open", "high", "low", "close", "volume"]

    backtest_db.close()
    market_db.close()

    print("SUCCESS: Historical bars from market_db verified.")


def test_signal_insertion_backtest():
    print("INFO: Testing signal insertion in backtest mode.")

    _ensure_dirs()
    _cleanup()

    backtest_db = BacktestDataBaseManager(BACKTEST_DB_PATH)
    market_db = MarketDatabase(MARKET_DB_PATH)
    dm = DataManager(backtest_db, market_db)

    # Create a backtest run
    run_id = dm.create_backtest_run(
        strategy_name="TestStrat",
        strategy_version="1.0",
        parameters={},
        data_start=datetime.now(timezone.utc),
        data_end=datetime.now(timezone.utc),
    )

    # Insert a signal
    signal = Signal(
        stock="AAPL",
        signal_type=SignalType.ENTRY,
        direction=Direction.LONG,
        date=pd.Timestamp.now(tz="UTC"),
        price=150.0,
        confidence=0.9,
    )
    sig_id = dm.insert_signal(signal)

    assert sig_id > 0, "Expected positive signal_id"
    assert signal.signal_id == sig_id

    # Retrieve the signal
    signals = dm.get_signals()
    assert len(signals) == 1, f"Expected 1 signal, got {len(signals)}"
    assert signals[0].stock == "AAPL"

    backtest_db.close()
    market_db.close()

    print("SUCCESS: Signal insertion in backtest mode verified.")


def test_position_lifecycle_backtest():
    print("INFO: Testing position lifecycle (open → close) in backtest mode.")

    _ensure_dirs()
    _cleanup()

    backtest_db = BacktestDataBaseManager(BACKTEST_DB_PATH)
    market_db = MarketDatabase(MARKET_DB_PATH)
    dm = DataManager(backtest_db, market_db)

    # Create a backtest run
    run_id = dm.create_backtest_run(
        strategy_name="TestStrat",
        strategy_version="1.0",
        parameters={},
        data_start=datetime.now(timezone.utc),
        data_end=datetime.now(timezone.utc),
    )

    # Insert entry signal
    entry_signal = Signal(
        stock="AAPL",
        signal_type=SignalType.ENTRY,
        direction=Direction.LONG,
        date=pd.Timestamp.now(tz="UTC"),
        price=150.0,
        confidence=0.9,
    )
    entry_sig_id = dm.insert_signal(entry_signal)

    # Open position
    open_pos = OpenPosition(
        stock="AAPL",
        direction=Direction.LONG,
        date=pd.Timestamp.now(tz="UTC"),
        entry_price=150.0,
        quantity_type=QuantityType.SHARES,
        quantity=10.0,
        entry_signal_id=entry_sig_id,
    )
    pos_id = dm.insert_open_position(open_pos)

    # Insert exit signal
    exit_signal = Signal(
        stock="AAPL",
        signal_type=SignalType.EXIT,
        direction=Direction.LONG,
        date=pd.Timestamp.now(tz="UTC"),
        price=155.0,
        confidence=0.95,
    )
    exit_sig_id = dm.insert_signal(exit_signal)

    # Close position with trade
    trade = Trade(
        stock="AAPL",
        direction=Direction.LONG,
        quantity_type=QuantityType.SHARES,
        quantity=10.0,
        entry_price=150.0,
        exit_price=155.0,
        entry_date=open_pos.date,
        exit_date=exit_signal.date,
        gross_result=50.0,
        commission=1.0,
        net_result=49.0,
        entry_signal_id=entry_sig_id,
        exit_signal_id=exit_sig_id,
    )
    trade_id = dm.close_open_position(pos_id, trade)

    assert trade_id > 0, "Expected positive trade_id"

    # Verify position is closed
    open_positions = dm.get_open_positions()
    assert len(open_positions) == 0, f"Expected 0 open positions, got {len(open_positions)}"

    # Verify trade is recorded
    trades = dm.get_trades()
    assert len(trades) == 1, f"Expected 1 trade, got {len(trades)}"
    assert trades[0].stock == "AAPL"
    assert trades[0].net_result == 49.0

    backtest_db.close()
    market_db.close()

    print("SUCCESS: Position lifecycle in backtest mode verified.")


def test_concurrent_signal_insertion():
    print("INFO: Testing concurrent signal insertion (thread safety).")

    _ensure_dirs()
    _cleanup()

    backtest_db = BacktestDataBaseManager(BACKTEST_DB_PATH)
    market_db = MarketDatabase(MARKET_DB_PATH)
    dm = DataManager(backtest_db, market_db)

    # Create a backtest run
    run_id = dm.create_backtest_run(
        strategy_name="ConcurrencyTest",
        strategy_version="1.0",
        parameters={},
        data_start=datetime.now(timezone.utc),
        data_end=datetime.now(timezone.utc),
    )

    errors = []
    signal_count = [0]

    def insert_signals(thread_id: int, count: int):
        try:
            for i in range(count):
                signal = Signal(
                    stock=f"STOCK_{thread_id}",
                    signal_type=SignalType.ENTRY,
                    direction=Direction.LONG,
                    date=pd.Timestamp.now(tz="UTC"),
                    price=100.0 + i,
                    confidence=0.9,
                )
                dm.insert_signal(signal)
                signal_count[0] += 1
        except Exception as e:
            errors.append(e)

    threads = [
        threading.Thread(target=insert_signals, args=(i, 50)) for i in range(5)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Concurrency errors: {errors}"
    assert signal_count[0] == 250, f"Expected 250 signals, got {signal_count[0]}"

    signals = dm.get_signals()
    assert len(signals) == 250, f"Expected 250 signals in db, got {len(signals)}"

    backtest_db.close()
    market_db.close()

    print("SUCCESS: Concurrent signal insertion verified.")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_all_tests():
    print("\nINFO: Commencing DataManager tests.\n")

    _ensure_dirs()

    try:
        test_backtest_mode_initialization()
        test_live_mode_initialization()
        test_context_manager()
        test_backtest_operation_error_place_order()
        test_backtest_operation_error_close_position()
        test_historical_bars_from_market_db()
        test_signal_insertion_backtest()
        test_position_lifecycle_backtest()
        test_concurrent_signal_insertion()
    finally:
        _cleanup()

    print("\nINFO: All DataManager tests passed successfully.")


if __name__ == "__main__":
    run_all_tests()
