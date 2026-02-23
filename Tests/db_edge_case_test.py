"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: live_trader_db_impl.py
Description: 
    This module implements the TradingDataBaseInterface for live trading scenarios using SQLite.
    It provides methods to manage signals, open positions, and trades in a live trading environment,
    ensuring that all operations are immediately reflected in the database to maintain an accurate record of live trading activity.

Author: Albert Marín Blasco
Date Created: 2026-02-22
Last Modified: 2026-02-22
"""

import os
import time
import threading
import sqlite3
import pandas as pd
from datetime import datetime, timezone, timedelta

from Domain import Signal, OpenPosition, Trade, Direction, QuantityType, SignalType
from Infrastructure import BacktestDataBaseManager, LiveTraderDataBaseManager, TradingDataBaseInterface


def get_dummy_signal(date: pd.Timestamp) -> Signal:
    return Signal(
        stock="AAPL", signal_type=SignalType.ENTRY, direction=Direction.LONG,
        date=date, price=150.0, confidence=0.9
    )

def test_persistence_and_reconnection(db_class, db_path: str, is_backtest: bool):
    print("INFO: Executing data persistence and reconnection tests.")
    
    db_manager = db_class(db_path)
    if is_backtest:
        run_id = db_manager.create_backtest_run(
            strategy_name="PersistTest", strategy_version="1.0", parameters={},
            data_start=datetime.now(timezone.utc), data_end=datetime.now(timezone.utc)
        )
    
    sig_id = db_manager.insert_signal(get_dummy_signal(pd.Timestamp.now(tz='UTC')))
    db_manager.commit()
    db_manager.close()
    
    db_manager_reconnected = db_class(db_path)
    if is_backtest:
        db_manager_reconnected.set_active_run(run_id)
        
    signals = db_manager_reconnected.get_signals()
    assert len(signals) >= 1, "Persistence failed: Could not find the inserted signal after reconnecting."
    db_manager_reconnected.close()
    
    print("SUCCESS: Data persistence verified.")


def test_date_filtering(db_manager: TradingDataBaseInterface, is_backtest: bool):
    print("INFO: Executing date filtering logic tests.")
    
    if is_backtest:
        db_manager.create_backtest_run(
            strategy_name="DateFilterTest", strategy_version="1.0", parameters={},
            data_start=datetime.now(timezone.utc), data_end=datetime.now(timezone.utc)
        )
    
    base_date = pd.Timestamp("2026-01-15 10:00:00", tz='UTC')
    
    for i, day_offset in enumerate([-5, 0, 5]):
        trade_date = base_date + pd.Timedelta(days=day_offset)
        sig = get_dummy_signal(trade_date)
        sig_id = db_manager.insert_signal(sig)
        
        trade = Trade(
            stock="AAPL", direction=Direction.LONG, quantity_type=QuantityType.SHARES,
            quantity=10, entry_price=150, exit_price=155, 
            entry_date=trade_date - pd.Timedelta(minutes=5), 
            exit_date=trade_date,
            gross_result=50, commission=1, net_result=49,
            entry_signal_id=sig_id, exit_signal_id=sig_id
        )
        with db_manager.db_lock:
            cur = db_manager.conn.cursor()
            db_manager._insert_trade(trade, cur)
            db_manager.commit()
            
    start_bound = pd.Timestamp("2026-01-14 00:00:00", tz='UTC')
    end_bound = pd.Timestamp("2026-01-16 00:00:00", tz='UTC')
    
    filtered_trades = db_manager.get_trades(start_date=start_bound, end_date=end_bound)
    
    assert len(filtered_trades) == 1, f"Expected 1 trade in range, got {len(filtered_trades)}"
    assert filtered_trades[0].exit_date == base_date, "Filtered trade has the wrong date."
    
    print("SUCCESS: Date filtering constraints verified.")


def test_cascading_deletes(db_manager: TradingDataBaseInterface, is_backtest: bool):
    print("INFO: Executing cascading deletes tests.")
    
    if is_backtest:
        run_id = db_manager.create_backtest_run(
            strategy_name="CascadeTest", strategy_version="1.0", parameters={},
            data_start=datetime.now(timezone.utc), data_end=datetime.now(timezone.utc)
        )
        
        sig_id = db_manager.insert_signal(get_dummy_signal(pd.Timestamp.now(tz='UTC')))
        db_manager.commit()
        
        with db_manager.db_lock:
            cur = db_manager.conn.cursor()
            cur.execute("DELETE FROM backtest_run WHERE run_id = ?", (run_id,))
            db_manager.commit()
            
        signals = db_manager.get_signals()
        assert len(signals) == 0, "Cascading delete failed: Signals remained after backtest_run was deleted."
        print("SUCCESS: Cascading deletes (Backtest Run -> Signal) verified.")
        
    else:
        sig_id = db_manager.insert_signal(get_dummy_signal(pd.Timestamp.now(tz='UTC')))
        
        pos = OpenPosition(
            stock="AAPL", direction=Direction.LONG, date=pd.Timestamp.now(tz='UTC'),
            entry_price=150, quantity_type=QuantityType.SHARES, quantity=10, entry_signal_id=sig_id
        )
        db_manager.insert_open_position(pos)
        db_manager.commit()
        
        with db_manager.db_lock:
            cur = db_manager.conn.cursor()
            cur.execute("DELETE FROM signal WHERE signal_id = ?", (sig_id,))
            db_manager.commit()
            
        positions = db_manager.get_open_positions()
        assert not any(p.entry_signal_id == sig_id for p in positions), "Cascading delete failed: Position remained after signal deleted."
        print("SUCCESS: Cascading deletes (Signal -> Open Position) verified.")


def test_read_while_write_concurrency(db_manager: TradingDataBaseInterface, is_backtest: bool):
    print("INFO: Executing read-while-write concurrency tests.")
    
    if is_backtest:
        db_manager.create_backtest_run(
            strategy_name="RWWTest", strategy_version="1.0", parameters={},
            data_start=datetime.now(timezone.utc), data_end=datetime.now(timezone.utc)
        )

    stop_event = threading.Event()
    error_caught = []

    def background_writer():
        try:
            for _ in range(500):
                if stop_event.is_set(): break
                db_manager.insert_signal(get_dummy_signal(pd.Timestamp.now(tz='UTC')))
                db_manager.commit()
        except Exception as e:
            error_caught.append(e)

    writer_thread = threading.Thread(target=background_writer)
    writer_thread.start()

    try:
        for _ in range(50):
            db_manager.get_signals()
            time.sleep(0.01)
    except Exception as e:
        error_caught.append(e)
        
    stop_event.set()
    writer_thread.join()

    assert not error_caught, f"Concurrency error detected: {error_caught}"
    print("SUCCESS: Read-while-write concurrency verified.")


def run_all_tests(db_class, db_path: str, is_backtest: bool):
    db_type = "Backtest" if is_backtest else "Live Trader"
    print(f"\nINFO: Commencing advanced unit tests for {db_type} database.")

    if os.path.exists(db_path):
        os.remove(db_path)

    test_persistence_and_reconnection(db_class, db_path, is_backtest)
    
    db_manager = db_class(db_path)
    try:
        test_date_filtering(db_manager, is_backtest)
        test_cascading_deletes(db_manager, is_backtest)
        test_read_while_write_concurrency(db_manager, is_backtest)
    finally:
        db_manager.close()
        
    print(f"INFO: All advanced tests passed successfully for {db_type} database.")


if __name__ == "__main__":
    backtest_path = "Infrastructure/backtester/edge_backtest.db"
    live_path = "Infrastructure/live_trader/edge_live.db"

    os.makedirs(os.path.dirname(backtest_path), exist_ok=True)
    os.makedirs(os.path.dirname(live_path), exist_ok=True)

    try:
        run_all_tests(BacktestDataBaseManager, backtest_path, is_backtest=True)
        run_all_tests(LiveTraderDataBaseManager, live_path, is_backtest=False)
    finally:
        if os.path.exists(backtest_path): os.remove(backtest_path)
        if os.path.exists(live_path): os.remove(live_path)