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


from data.sqlite_database import SQLiteDatabase
from data.data_manager import DataManager
from datetime import datetime

db = SQLiteDatabase()
manager = DataManager(db)

""" Example usage of DataManager to open a trade using stock quantity"""
# manager.open_trade_qty("AAPL", 10, "buy")

""" Example usage of DataManager to open a trade using notional value"""
# manager.open_trade_notional("NVDA", 1000, "buy")


""" Example usage of DataManager to retrieve open orders """
# open_positions = manager.get_open_positions()
# for position in open_positions:
#     print(f"Open Position: {position['symbol']} - Qty: {position['qty']} - Side: {position['side']}")

""" Example usage of DataManager to update and retrieve OHLCV data """
start_date = datetime(2023, 11, 1)
end_date = datetime(2023, 12, 10)
symbol = "AAPL"
# # Update OHLCV data for the specified symbol and date range
# manager.update_ohlcv_data(symbol, start_date, end_date)
# # Retrieve OHLCV data for the specified symbol and date range
ohlcv_data = manager.get_ohlcv_data(symbol, start_date, end_date)
for data in ohlcv_data:
    print(f"Date: {data['date']}, Open: {data['open']}, High: {data['high']}, Low: {data['low']}, Close: {data['close']}, Volume: {data['volume']}")