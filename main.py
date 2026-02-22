"""
Entry point

Reads mode from config.py:

If backtest: load data, run backtester

If live: stream data and trade

if config.MODE == "backtest":
    ...
elif config.MODE == "live":
    ...
"""

from Infrastructure import BacktestDataBaseManager, LiveTraderDataBaseManager
from datetime import datetime, timedelta
import pandas as pd
from Domain import Signal, OpenPosition, Trade, Direction, QuantityType, SignalType

import threading
from Infrastructure import BacktestDataBaseManager, LiveTraderDataBaseManager
from datetime import datetime, timezone
import pandas as pd
from Domain import Signal, OpenPosition, Trade, Direction, QuantityType, SignalType
import sqlite3

def run_updated_stress_test(backtest_db_path: str, live_db_path: str):
    print("🚀 Starting Advanced Database Stress Tests...\n")
    
    # --- Test 1: Context Manager (__exit__ Signature Fix) ---
    print("Test 1: Testing Context Manager (__enter__ and __exit__)...")
    try:
        with LiveTraderDataBaseManager(db_path=live_db_path) as live_db:
            # If __exit__ has the wrong signature, this block will crash when exiting
            pass
        print("✅ PASS: Context manager exited cleanly without crashing.")
    except Exception as e:
        print(f"❌ FAIL: Context manager crashed. Error: {e}")

    # Re-initialize for the rest of the tests
    back_db = BacktestDataBaseManager(db_path=backtest_db_path)
    live_db = LiveTraderDataBaseManager(db_path=live_db_path)

    # --- Test 2: Thread Safety & WAL Mode (Live DB) ---
    print("\nTest 2: Testing Thread Safety (check_same_thread) & WAL Mode...")
    try:
        # Check WAL mode
        live_db.cursor.execute("PRAGMA journal_mode;")
        journal_mode = live_db.cursor.fetchone()[0]
        if journal_mode.lower() == 'wal':
            print("✅ PASS: Live DB is correctly using WAL journal mode.")
        else:
            print(f"❌ FAIL: Expected 'wal', got '{journal_mode}'.")

        # Test cross-thread writing
        def thread_worker():
            live_db.cursor.execute("SELECT 1")
            
        t = threading.Thread(target=thread_worker)
        t.start()
        t.join()
        print("✅ PASS: check_same_thread=False is working. No cross-thread exceptions.")
    except Exception as e:
        print(f"❌ FAIL: Thread safety test failed. Error: {e}")

    # --- Test 3: State Management & LSP (set_active_run) ---
    print("\nTest 3: Testing State Management (set_active_run) and LSP Interface...")
    
    # Create Run A
    run_a_id = back_db.create_backtest_run(
        strategy_name="Strat_A", strategy_version="1.0", parameters={},
        data_start=datetime(2020, 1, 1, tzinfo=timezone.utc),
        data_end=datetime(2021, 1, 1, tzinfo=timezone.utc)
    )
    # Insert a signal into Run A
    sig_a = Signal("AAPL", SignalType.ENTRY, Direction.LONG, pd.Timestamp.now(), 150.0, 0.9, "Run A")
    back_db.insert_signal(sig_a)
    back_db.commit()

    # Create Run B (this automatically shifts the internal state to Run B)
    run_b_id = back_db.create_backtest_run(
        strategy_name="Strat_B", strategy_version="2.0", parameters={},
        data_start=datetime(2021, 1, 1, tzinfo=timezone.utc),
        data_end=datetime(2022, 1, 1, tzinfo=timezone.utc)
    )
    # Insert a signal into Run B
    sig_b = Signal("MSFT", SignalType.EXIT, Direction.SHORT, pd.Timestamp.now(), 300.0, 0.8, "Run B")
    back_db.insert_signal(sig_b)
    back_db.commit()

    # Now, test the LSP interface and state switching!
    back_db.set_active_run(run_a_id)
    signals_a = back_db.get_signals() # NO run_id passed! Perfectly matches interface.
    
    if len(signals_a) == 1 and signals_a[0].stock == "AAPL":
        print("✅ PASS: set_active_run successfully switched context and retrieved Run A data.")
    else:
        print("❌ FAIL: Did not retrieve the correct isolated data for Run A.")

    # --- Test 4: The Ghost Trade (Rowcount / Rollback Fix) ---
    print("\nTest 4: Testing 'Ghost Trade' Rollback on Invalid Position ID...")
    dummy_trade = Trade("TSLA", Direction.LONG, QuantityType.SHARES, 10, 100, 150, pd.Timestamp.now(), pd.Timestamp.now(), 500, 1.5, 498.5, sig_a.signal_id, sig_b.signal_id)
    
    invalid_position_id = 9999999 

    try:
        live_db.close_open_position(open_position_id=invalid_position_id, trade=dummy_trade)
        print("❌ FAIL: No error was raised. The Ghost Trade bug is still present.")
    except ValueError as ve:
        # Check if the trade was actually rolled back
        live_db.cursor.execute("SELECT COUNT(*) FROM trade WHERE stock='TSLA' AND gross_result=500")
        trade_count = live_db.cursor.fetchone()[0]
        if trade_count == 0:
            print("✅ PASS: Caught invalid close attempt and successfully rolled back the trade insertion!")
        else:
            print("❌ FAIL: Error was raised, but the trade was STILL inserted (Rollback failed).")
    except Exception as e:
        print(f"⚠️ WARNING: Unexpected error type caught: {type(e).__name__}: {e}")

    # --- Test 5: Foreign Key Cascade Deletion ---
    print("\nTest 5: Testing ON DELETE CASCADE (Foreign Keys)...")
    
    # We are still in the context of Run A. Let's add an open position to it.
    pos_a = OpenPosition("NVDA", Direction.LONG, pd.Timestamp.now(), 400.0, QuantityType.SHARES, 10.0, sig_a.signal_id, None)
    back_db.insert_open_position(pos_a)
    back_db.commit()

    # Delete Run A via raw SQL to trigger the cascade
    back_db.cursor.execute("DELETE FROM backtest_run WHERE run_id = ?", (run_a_id,))
    back_db.commit()

    # Verify that the open position and signal were wiped out automatically
    back_db.cursor.execute("SELECT COUNT(*) FROM open_position WHERE run_id = ?", (run_a_id,))
    pos_count = back_db.cursor.fetchone()[0]
    back_db.cursor.execute("SELECT COUNT(*) FROM signal WHERE run_id = ?", (run_a_id,))
    sig_count = back_db.cursor.fetchone()[0]

    if pos_count == 0 and sig_count == 0:
        print("✅ PASS: ON DELETE CASCADE successfully wiped child records when the parent run was deleted.")
    else:
        print(f"❌ FAIL: Cascade failed. Found {pos_count} positions and {sig_count} signals remaining.")

    # Cleanup
    back_db.close()
    live_db.close()
    print("\nStress testing complete. Databases safely closed.")

# --- Execute the Test ---
if __name__ == "__main__":
    # Use temporary file paths for testing so you can easily delete them later
    test_back_db = "Infrastructure/backtester/test_backtester.db"
    test_live_db = "Infrastructure/live_trader/test_live_trader.db"
    
    run_updated_stress_test(test_back_db, test_live_db)