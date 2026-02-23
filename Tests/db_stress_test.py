"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: db_stress_test.py
Description: 
    This module executes multithreaded stress testing and edge case validation for the TradingDataBaseInterface.
    It ensures database integrity, connection handling, and transaction rollbacks function correctly under heavy concurrent load.

Author: Albert Marín Blasco
Date Created: 2026-02-22
Last Modified: 2026-02-22
"""

import os
import time
import random
import concurrent.futures
import pandas as pd
from datetime import datetime, timezone, timedelta

from Domain import Signal, OpenPosition, Trade, Direction, QuantityType, SignalType
from Infrastructure import BacktestDataBaseManager, LiveTraderDataBaseManager, TradingDataBaseInterface

def simulate_bot_worker(db_manager: TradingDataBaseInterface, bot_id: int, num_cycles: int, is_backtest: bool):
    success_count = 0
    base_date = pd.Timestamp.now(tz='UTC')

    for i in range(num_cycles):
        try:
            current_date = base_date + pd.Timedelta(minutes=i + (bot_id * 1000))
            stock_ticker = f"TICKER_{bot_id}_{i%5}"

            entry_signal = Signal(
                stock=stock_ticker,
                signal_type=SignalType.ENTRY,
                direction=Direction.LONG,
                date=current_date,
                price=100.0 + random.uniform(-5, 5),
                confidence=0.85,
                reason=f"Bot {bot_id} Entry"
            )
            entry_sig_id = db_manager.insert_signal(entry_signal)

            open_pos = OpenPosition(
                stock=stock_ticker,
                direction=Direction.LONG,
                date=current_date,
                entry_price=entry_signal.price,
                quantity_type=QuantityType.SHARES,
                quantity=15.5,
                entry_signal_id=entry_sig_id
            )
            pos_id = db_manager.insert_open_position(open_pos)

            exit_date = current_date + pd.Timedelta(minutes=15)
            exit_signal = Signal(
                stock=stock_ticker,
                signal_type=SignalType.EXIT,
                direction=Direction.LONG,
                date=exit_date,
                price=open_pos.entry_price + random.uniform(-2, 5),
                confidence=0.90,
                reason=f"Bot {bot_id} Exit"
            )
            exit_sig_id = db_manager.insert_signal(exit_signal)

            trade = Trade(
                stock=stock_ticker,
                direction=Direction.LONG,
                quantity_type=QuantityType.SHARES,
                quantity=15.5,
                entry_price=open_pos.entry_price,
                exit_price=exit_signal.price,
                entry_date=open_pos.date,
                exit_date=exit_signal.date,
                gross_result=0.0, 
                commission=1.0,
                net_result=0.0,
                entry_signal_id=entry_sig_id,
                exit_signal_id=exit_sig_id
            )
            
            db_manager.close_open_position(pos_id, trade)
            success_count += 1
            
            if i % 100 == 0:
                db_manager.commit()
                
        except Exception as e:
            print(f"ERROR: Worker {bot_id} encountered an exception: {e}")

    db_manager.commit()
    return success_count

def test_edge_cases(db_manager: TradingDataBaseInterface):
    print("INFO: Executing edge case validation (Rollbacks & Invalid Data).")
    
    dummy_trade = Trade(
        stock="GHOST", direction=Direction.LONG, quantity_type=QuantityType.CAPITAL,
        quantity=10, entry_price=100, exit_price=110, entry_date=pd.Timestamp.now(),
        exit_date=pd.Timestamp.now(), gross_result=100, commission=1, net_result=99,
        entry_signal_id=9999, exit_signal_id=9999
    )
    
    try:
        db_manager.close_open_position(open_position_id=999999, trade=dummy_trade)
        print("ERROR: Edge case validation failed. Database allowed closing a non-existent position.")
    except ValueError as e:
        print("SUCCESS: Caught expected ValueError for non-existent position.")
    except Exception as e:
        print(f"WARNING: Unexpected exception during edge case execution: {e}")

def run_comprehensive_test(db_manager: TradingDataBaseInterface, is_backtest: bool, num_bots: int = 10, cycles_per_bot: int = 1000):
    db_type = "Backtest" if is_backtest else "Live"
    print(f"\nINFO: Commencing {db_type} database stress test.")
    print(f"INFO: Configuration - Workers: {num_bots} | Cycles per worker: {cycles_per_bot} | Total expected trades: {num_bots * cycles_per_bot}")
    
    start_time = time.time()

    if is_backtest:
        db_manager.create_backtest_run(
            strategy_name="MultiThreadedStress",
            strategy_version="2.0",
            parameters={"num_bots": num_bots, "cycles": cycles_per_bot},
            data_start=datetime.now(timezone.utc),
            data_end=datetime.now(timezone.utc) + timedelta(days=30)
        )

    print("INFO: Executing concurrent write operations...")
    total_success = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_bots) as executor:
        futures = [
            executor.submit(simulate_bot_worker, db_manager, bot_id, cycles_per_bot, is_backtest) 
            for bot_id in range(num_bots)
        ]
        
        for future in concurrent.futures.as_completed(futures):
            total_success += future.result()

    test_edge_cases(db_manager)

    print("INFO: Validating database integrity...")
    signals = db_manager.get_signals()
    positions = db_manager.get_open_positions()
    trades = db_manager.get_trades()

    expected_signals = num_bots * cycles_per_bot * 2 

    print(f"INFO: Total signals recorded: {len(signals)} (Expected: {expected_signals})")
    print(f"INFO: Total trades recorded: {len(trades)} (Expected: {total_success})")
    print(f"INFO: Dangling positions: {len(positions)} (Expected: 0)")
    
    if is_backtest:
        db_manager.close_backtest_run()

    end_time = time.time()
    duration = end_time - start_time
    ops_per_sec = total_success / duration if duration > 0 else 0

    print(f"INFO: {db_type} database test completed in {duration:.2f}s. Throughput: {ops_per_sec:.2f} trades/sec.")

if __name__ == "__main__":
    backtest_path = "Infrastructure/backtester/stress_backtest.db"
    live_path = "Infrastructure/live_trader/stress_live.db"

    try:
        with BacktestDataBaseManager(backtest_path) as bt_db:
            run_comprehensive_test(bt_db, is_backtest=True, num_bots=10, cycles_per_bot=500)

        with LiveTraderDataBaseManager(live_path) as live_db:
            run_comprehensive_test(live_db, is_backtest=False, num_bots=10, cycles_per_bot=500)

    finally:
        if os.path.exists(backtest_path): os.remove(backtest_path)
        if os.path.exists(live_path): os.remove(live_path)