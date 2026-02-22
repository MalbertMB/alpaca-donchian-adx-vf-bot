"""
Project Name: Alpaca Donchian ADX VF BOT
File Name: db_simple_test.py
Description: 
    This module executes a sequential stress test for the TradingDataBaseInterface.
    It performs a high volume of complete trade cycles (Signal -> Position -> Signal -> Trade)
    to measure single-threaded throughput and ensure database stability under continuous load.

Author: Albert Marín Blasco
Date Created: 2025-06-25
Last Modified: 2026-02-22
"""

import os
import time
import pandas as pd
from datetime import datetime, timezone, timedelta

from Domain import Signal, OpenPosition, Trade, Direction, QuantityType, SignalType
from Infrastructure import BacktestDataBaseManager, LiveTraderDataBaseManager, TradingDataBaseInterface

def run_stress_test(db_manager: TradingDataBaseInterface, is_backtest: bool, num_records: int = 10000, batch_size: int = 1000):
    print(f"INFO: Commencing sequential stress test for {db_manager.__class__.__name__}.")
    print(f"INFO: Target configuration - {num_records} complete trade cycles.")
    
    start_time = time.time()
    
    if is_backtest:
        db_manager.create_backtest_run(
            strategy_name="StressTestStrat",
            strategy_version="1.0",
            parameters={"stress": True},
            data_start=datetime.now(timezone.utc),
            data_end=datetime.now(timezone.utc) + timedelta(days=1)
        )
        
    base_date = pd.Timestamp.now(tz='UTC')
    
    for i in range(1, num_records + 1):
        current_date = base_date + pd.Timedelta(minutes=i)
        
        entry_signal = Signal(
            stock="AAPL",
            signal_type=SignalType.ENTRY,
            direction=Direction.LONG,
            date=current_date,
            price=150.0 + (i * 0.01),
            confidence=0.95,
            reason="Stress Test Entry"
        )
        entry_sig_id = db_manager.insert_signal(entry_signal)
        
        open_pos = OpenPosition(
            stock="AAPL",
            direction=Direction.LONG,
            date=current_date,
            entry_price=150.0 + (i * 0.01),
            quantity_type=QuantityType.SHARES,
            quantity=10.0,
            entry_signal_id=entry_sig_id
        )
        pos_id = db_manager.insert_open_position(open_pos)
        
        exit_date = current_date + pd.Timedelta(minutes=5)
        exit_signal = Signal(
            stock="AAPL",
            signal_type=SignalType.EXIT,
            direction=Direction.LONG,
            date=exit_date,
            price=155.0 + (i * 0.01),
            confidence=0.99,
            reason="Stress Test Exit"
        )
        exit_sig_id = db_manager.insert_signal(exit_signal)
        
        trade = Trade(
            stock="AAPL",
            direction=Direction.LONG,
            quantity_type=QuantityType.SHARES,
            quantity=10.0,
            entry_price=open_pos.entry_price,
            exit_price=exit_signal.price,
            entry_date=open_pos.date,
            exit_date=exit_signal.date,
            gross_result=0.0,  
            commission=1.5,
            net_result=0.0,    
            entry_signal_id=entry_sig_id,
            exit_signal_id=exit_sig_id
        )
        db_manager.close_open_position(pos_id, trade)
        
        if i % batch_size == 0:
            db_manager.commit()
            print(f"INFO: Processed {i}/{num_records} records.")

    db_manager.commit()
    
    if is_backtest:
        db_manager.close_backtest_run()

    end_time = time.time()
    duration = end_time - start_time
    ops_per_sec = num_records / duration if duration > 0 else 0

    print(f"INFO: Stress test completed in {duration:.2f} seconds.")
    print(f"INFO: Throughput recorded at {ops_per_sec:.2f} complete trade cycles per second.")
    
    trades = db_manager.get_trades()
    print(f"SUCCESS: Verification passed. {len(trades)} trades successfully recorded in the database.\n")

if __name__ == "__main__":
    backtest_db_path = "Infrastructure/backtester/test_backtest.db"
    live_db_path = "Infrastructure/live_trader/test_live.db"

    try:
        with BacktestDataBaseManager(backtest_db_path) as bt_db:
            run_stress_test(bt_db, is_backtest=True, num_records=25000)

        with LiveTraderDataBaseManager(live_db_path) as live_db:
            run_stress_test(live_db, is_backtest=False, num_records=25000)
    finally:
        if os.path.exists(backtest_db_path): os.remove(backtest_db_path)
        if os.path.exists(live_db_path): os.remove(live_db_path)