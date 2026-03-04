"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: db_market_test.py
Description:
    Edge-case and regression tests for MarketDatabase. Covers context manager
    support, insert/retrieve correctness, date filtering, symbol-column exclusion,
    the has_ohlcv_data 0==0 fix, delete_ohlcv_data, persistence across
    reconnections, and concurrent read-while-write safety.

Author: Albert Marín
Date Created: 2026-03-04
Last Modified: 2026-03-04
"""

import os
import time
import threading
import pandas as pd
from datetime import timezone

from Infrastructure.market import MarketDatabase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DB_PATH = "Infrastructure/market/test_market.db"


def _make_ohlcv(dates: list[str]) -> pd.DataFrame:
    """Return a minimal OHLCV DataFrame indexed by DatetimeIndex."""
    index = pd.DatetimeIndex([pd.Timestamp(d) for d in dates])
    return pd.DataFrame(
        {
            "open":   [100.0 + i for i in range(len(dates))],
            "high":   [105.0 + i for i in range(len(dates))],
            "low":    [ 95.0 + i for i in range(len(dates))],
            "close":  [102.0 + i for i in range(len(dates))],
            "volume": [1_000 * (i + 1) for i in range(len(dates))],
        },
        index=index,
    )


def _make_calendar(dates: list[str], is_open: bool = True) -> pd.DataFrame:
    """Return a calendar DataFrame in the format expected by _insert_stock_calendar."""
    index = pd.DatetimeIndex([pd.Timestamp(d) for d in dates])
    return pd.DataFrame({"open": [is_open] * len(dates)}, index=index)


def _fresh_db() -> MarketDatabase:
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    return MarketDatabase(DB_PATH)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_context_manager():
    print("INFO: Testing context manager (__enter__ / __exit__).")

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    with MarketDatabase(DB_PATH) as db:
        db.insert_ohlcv_data("AAPL", _make_ohlcv(["2025-01-02"]))

    # Connection must be closed after the 'with' block – verify by ensuring the
    # conn object itself is not usable (sqlite3 raises ProgrammingError on a
    # closed connection).
    import sqlite3
    try:
        db.conn.execute("SELECT 1")
        assert False, "Expected ProgrammingError for closed connection."
    except sqlite3.ProgrammingError:
        pass

    print("SUCCESS: Context manager closes connection correctly.")


def test_insert_and_retrieve():
    print("INFO: Testing basic insert and retrieve.")

    with _fresh_db() as db:
        dates = ["2025-01-02", "2025-01-03", "2025-01-06"]
        data = _make_ohlcv(dates)
        db.insert_ohlcv_data("AAPL", data)

        start = pd.Timestamp("2025-01-01")
        end   = pd.Timestamp("2025-01-31")
        result = db.get_ohlcv_data("AAPL", start, end)

        assert len(result) == 3, f"Expected 3 rows, got {len(result)}."
        assert list(result.columns) == ["open", "high", "low", "close", "volume"], (
            f"Unexpected columns: {list(result.columns)}"
        )
        assert "symbol" not in result.columns, "Result DataFrame must not contain the 'symbol' column."
        assert result.index.name == "date", "Index name should be 'date'."

        # Spot-check values for the first row
        assert result.iloc[0]["close"] == 102.0

    print("SUCCESS: Insert and retrieve verified.")


def test_empty_insert_is_noop():
    print("INFO: Testing that inserting an empty DataFrame is a no-op.")

    with _fresh_db() as db:
        empty = pd.DataFrame(
            columns=["open", "high", "low", "close", "volume"],
            index=pd.DatetimeIndex([]),
        )
        # Should not raise and should not insert any rows.
        db.insert_ohlcv_data("AAPL", empty)

        start = pd.Timestamp("2000-01-01")
        end   = pd.Timestamp("2030-01-01")
        result = db.get_ohlcv_data("AAPL", start, end)
        assert result.empty, "Empty insert should leave the table empty."

    print("SUCCESS: Empty DataFrame insert is a safe no-op.")


def test_date_filtering():
    print("INFO: Testing date-range filtering in get_ohlcv_data.")

    with _fresh_db() as db:
        dates = ["2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07", "2025-01-08"]
        db.insert_ohlcv_data("MSFT", _make_ohlcv(dates))

        start = pd.Timestamp("2025-01-03")
        end   = pd.Timestamp("2025-01-07")
        result = db.get_ohlcv_data("MSFT", start, end)

        assert len(result) == 3, f"Expected 3 rows in range, got {len(result)}."
        assert result.index[0]  == pd.Timestamp("2025-01-03")
        assert result.index[-1] == pd.Timestamp("2025-01-07")

    print("SUCCESS: Date-range filtering verified.")


def test_delete_ohlcv_data():
    print("INFO: Testing delete_ohlcv_data.")

    with _fresh_db() as db:
        db.insert_ohlcv_data("AAPL", _make_ohlcv(["2025-01-02", "2025-01-03"]))
        db.insert_ohlcv_data("GOOG", _make_ohlcv(["2025-01-02", "2025-01-03"]))

        db.delete_ohlcv_data("AAPL")

        start = pd.Timestamp("2024-01-01")
        end   = pd.Timestamp("2026-01-01")

        assert db.get_ohlcv_data("AAPL", start, end).empty, "AAPL data should be deleted."
        assert len(db.get_ohlcv_data("GOOG", start, end)) == 2, "GOOG data should be untouched."

    print("SUCCESS: delete_ohlcv_data removes only the targeted symbol.")


def test_has_ohlcv_data_empty_calendar_returns_false():
    """Regression test for the 0==0 bug: empty calendar + empty OHLCV must return False."""
    print("INFO: Testing has_ohlcv_data with empty calendar (0==0 regression).")

    with _fresh_db() as db:
        start = pd.Timestamp("2025-01-01")
        end   = pd.Timestamp("2025-01-31")

        # Calendar is empty and OHLCV is empty -> both counts are 0.
        # The old code returned True (0 == 0). The fix must return False.
        result = db.has_ohlcv_data("AAPL", start, end)
        assert result is False, "has_ohlcv_data must return False when calendar is empty."

    print("SUCCESS: has_ohlcv_data correctly returns False for empty calendar.")


def test_has_ohlcv_data_correct_match():
    print("INFO: Testing has_ohlcv_data with matching OHLCV and calendar data.")

    with _fresh_db() as db:
        trading_dates = ["2025-01-02", "2025-01-03", "2025-01-06"]

        db._insert_stock_calendar(_make_calendar(trading_dates, is_open=True))
        db.insert_ohlcv_data("AAPL", _make_ohlcv(trading_dates))

        start = pd.Timestamp("2025-01-01")
        end   = pd.Timestamp("2025-01-10")

        assert db.has_ohlcv_data("AAPL", start, end) is True, (
            "has_ohlcv_data should return True when OHLCV matches calendar trading days."
        )

        # Remove one OHLCV row to break the match.
        db.delete_ohlcv_data("AAPL")
        db.insert_ohlcv_data("AAPL", _make_ohlcv(trading_dates[:2]))  # only 2 of 3

        assert db.has_ohlcv_data("AAPL", start, end) is False, (
            "has_ohlcv_data should return False when OHLCV count doesn't match calendar."
        )

    print("SUCCESS: has_ohlcv_data match logic verified.")


def test_insert_replace_idempotency():
    print("INFO: Testing INSERT OR REPLACE idempotency.")

    with _fresh_db() as db:
        date = "2025-01-02"
        data1 = _make_ohlcv([date])
        db.insert_ohlcv_data("AAPL", data1)

        # Insert again with different values – should replace, not duplicate.
        data2 = data1.copy()
        data2["close"] = 999.0
        db.insert_ohlcv_data("AAPL", data2)

        start  = pd.Timestamp("2025-01-01")
        end    = pd.Timestamp("2025-01-31")
        result = db.get_ohlcv_data("AAPL", start, end)

        assert len(result) == 1, f"Expected exactly 1 row after replace, got {len(result)}."
        assert result.iloc[0]["close"] == 999.0, "Updated close price not reflected."

    print("SUCCESS: INSERT OR REPLACE idempotency verified.")


def test_persistence_and_reconnection():
    print("INFO: Testing data persistence across reconnections.")

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    db = MarketDatabase(DB_PATH)
    db.insert_ohlcv_data("NVDA", _make_ohlcv(["2025-03-01", "2025-03-02"]))
    db.close()

    db2 = MarketDatabase(DB_PATH)
    try:
        start  = pd.Timestamp("2025-01-01")
        end    = pd.Timestamp("2026-01-01")
        result = db2.get_ohlcv_data("NVDA", start, end)
        assert len(result) == 2, f"Expected 2 rows after reconnection, got {len(result)}."
    finally:
        db2.close()

    print("SUCCESS: Data persists correctly across reconnections.")


def test_tickers_retrieval():
    print("INFO: Testing Dow Jones and S&P 500 ticker retrieval.")

    with _fresh_db() as db:
        dow_tickers  = ["AAPL", "MSFT", "JPM"]
        sp500_tickers = ["AAPL", "MSFT", "GOOG", "AMZN"]

        db._insert_dow_jones_tickers(dow_tickers)
        db._insert_sp500_tickers(sp500_tickers)

        dow_result  = db.get_dow_jones_tickers()
        sp500_result = db.get_sp500_tickers()

        assert set(dow_result["symbol"].tolist()) == set(dow_tickers), (
            f"Dow Jones tickers mismatch: {dow_result['symbol'].tolist()}"
        )
        assert set(sp500_result["symbol"].tolist()) == set(sp500_tickers), (
            f"S&P 500 tickers mismatch: {sp500_result['symbol'].tolist()}"
        )

        # Verify INSERT OR IGNORE - re-inserting should not duplicate.
        db._insert_dow_jones_tickers(dow_tickers)
        assert len(db.get_dow_jones_tickers()) == len(dow_tickers), (
            "Duplicate ticker insertion should be silently ignored."
        )

    print("SUCCESS: Ticker retrieval and idempotency verified.")


def test_read_while_write_concurrency():
    print("INFO: Testing read-while-write concurrency.")

    with _fresh_db() as db:
        errors = []
        stop = threading.Event()

        def writer():
            i = 0
            while not stop.is_set():
                try:
                    date = pd.Timestamp("2025-01-01") + pd.Timedelta(days=i)
                    data = _make_ohlcv([date.strftime("%Y-%m-%d")])
                    db.insert_ohlcv_data("TSLA", data)
                    i += 1
                except Exception as e:
                    errors.append(f"Writer: {e}")

        t = threading.Thread(target=writer)
        t.start()
        try:
            start = pd.Timestamp("2025-01-01")
            end   = pd.Timestamp("2030-01-01")
            for _ in range(100):
                db.get_ohlcv_data("TSLA", start, end)
                time.sleep(0.005)
        except Exception as e:
            errors.append(f"Reader: {e}")
        finally:
            stop.set()
            t.join()

        assert not errors, f"Concurrency errors detected: {errors}"

    print("SUCCESS: Read-while-write concurrency verified.")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_all_tests():
    print("\nINFO: Commencing MarketDatabase tests.\n")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    try:
        test_context_manager()
        test_insert_and_retrieve()
        test_empty_insert_is_noop()
        test_date_filtering()
        test_delete_ohlcv_data()
        test_has_ohlcv_data_empty_calendar_returns_false()
        test_has_ohlcv_data_correct_match()
        test_insert_replace_idempotency()
        test_persistence_and_reconnection()
        test_tickers_retrieval()
        test_read_while_write_concurrency()
    finally:
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)

    print("\nINFO: All MarketDatabase tests passed successfully.")


if __name__ == "__main__":
    run_all_tests()
