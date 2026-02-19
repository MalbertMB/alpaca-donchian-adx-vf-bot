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


from Infrastructure import BacktestDataBaseManager
from datetime import datetime, timedelta
import pandas as pd
from Domain import Signal, OpenPosition, Trade, Direction, QuantityType, SignalType

""" Example usage of Backtester database"""
# 1. Initialize Database
# Ensure the directory exists or adjust path as needed
db = BacktestDataBaseManager(db_path="Infrastructure/backtester/backtester.db")

print("--- Starting Database Test ---")

# 2. Create a Backtest Run
# We use dummy dates for the data range
run_id = db.create_backtest_run(
    strategy_name="TrendFollower_Test",
    strategy_version="1.0.1",
    parameters={"window": 20, "stop_loss": 0.05},
    data_start=datetime(2023, 1, 1),
    data_end=datetime(2023, 6, 1)
)
print(f"1. Created Backtest Run (ID: {run_id})")

# 3. Create an ENTRY Signal
entry_date = pd.Timestamp("2023-01-05 10:00:00")
entry_signal = Signal(
    stock="AAPL",
    signal_type=SignalType.ENTRY,
    direction=Direction.LONG,
    date=entry_date,
    price=150.0,
    confidence=0.85,
    reason="MA Crossover"
)

# Insert and Commit
db.insert_signal(run_id, entry_signal)
db.commit() # Remember to commit manual insertions!
print(f"2. Inserted Entry Signal (ID: {entry_signal.signal_id})")

# 4. Open a Position based on that signal
position = OpenPosition(
    stock="AAPL",
    direction=Direction.LONG,
    date=entry_date,
    entry_price=150.0,
    quantity_type=QuantityType.SHARES,
    quantity=10.0,
    entry_signal_id=entry_signal.signal_id
)

db.insert_open_position(run_id, position)
db.commit()
print(f"3. Opened Position (ID: {position.open_position_id})")

# Verify position exists
open_positions = db.get_open_positions_for_run(run_id)
print(f"   -> Current Open Positions in DB: {len(open_positions)}")

# 5. Create an EXIT Signal
exit_date = pd.Timestamp("2023-01-10 14:00:00")
exit_signal = Signal(
    stock="AAPL",
    signal_type=SignalType.EXIT,
    direction=Direction.SHORT, # Closing a LONG
    date=exit_date,
    price=160.0,
    confidence=0.90,
    reason="Target Hit"
)

db.insert_signal(run_id, exit_signal)
db.commit()
print(f"4. Inserted Exit Signal (ID: {exit_signal.signal_id})")

# 6. Close the Position (Create Trade)
# We create the Trade object that represents the result
trade_result = Trade(
    stock="AAPL",
    direction=Direction.LONG,
    quantity_type=QuantityType.SHARES,
    quantity=10.0,
    entry_price=150.0,
    exit_price=160.0,
    entry_date=entry_date,
    exit_date=exit_date,
    gross_result=100.0, # (160-150)*10
    commission=2.0,
    net_result=98.0,
    entry_signal_id=entry_signal.signal_id,
    exit_signal_id=exit_signal.signal_id
)

# This method handles inserting the trade AND deleting the open position
db.close_open_position(run_id, position.open_position_id, trade_result)
print(f"5. Closed Position & Recorded Trade (Trade ID: {trade_result.trade_id})")

# 7. Final Verification
print("\n--- Final Results ---")
final_positions = db.get_open_positions_for_run(run_id)
final_trades = db.get_trades_for_run(run_id)

print(f"Open Positions (Should be 0): {len(final_positions)}")
print(f"Recorded Trades (Should be 1): {len(final_trades)}")

if len(final_trades) > 0:
    t = final_trades[0]
    print(f"Trade Details: {t.stock} | Net PnL: {t.net_result}")

# Close the run
db.close_backtest_run(run_id)
print("6. Run Closed.")



""" Example usage of Backtester """
# start_date = datetime(2023, 1, 1)
# end_date = datetime(2023, 12, 31)
# strategy = VolatilityBreakoutStrategy()
# backtester = Backtester(manager, strategy)
# backtester.run(group="Dow Jones", start_date=start_date, end_date=end_date)

# manager.close_trade_by_symbol("NVDA")

""" Example usage of DataManager to open a trade using stock quantity"""
# manager.open_trade_qty("AAPL", 10, "buy")

""" Example usage of DataManager to open a trade using notional value"""
# manager.open_position_notional("NVDA", 1000, "buy")


""" Example usage of DataManager to retrieve open orders """
# open_positions = manager.get_positions()
# for position in open_positions:
#     print(f"Open Position: {position['symbol']} - Qty: {position['qty']} - Side: {position['side']}")
#     manager.close_position_by_symbol(position['symbol'])

""" Example usage of DataManager to update and retrieve OHLCV data """
# start_date = datetime(2023, 11, 1)
# end_date = datetime(2023, 12, 10)
# symbol = "AAPL"
# # # Update OHLCV data for the specified symbol and date range
# # manager.update_ohlcv_data(symbol, start_date, end_date)
# # # Retrieve OHLCV data for the specified symbol and date range
# ohlcv_data = manager.get_ohlcv_data(symbol, start_date, end_date)
# for data in ohlcv_data:
#     print(f"Date: {data['date']}, Open: {data['open']}, High: {data['high']}, Low: {data['low']}, Close: {data['close']}, Volume: {data['volume']}")