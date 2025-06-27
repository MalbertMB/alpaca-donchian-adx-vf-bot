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

start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 31)
symbol = "AAPL"
# Update OHLCV data for the specified symbol and date range
manager.update_ohlcv_data(symbol, start_date, end_date)
# Retrieve OHLCV data for the specified symbol and date range
ohlcv_data = manager.get_ohlcv_data(symbol, start_date, end_date)
for data in ohlcv_data:
    print(f"Date: {data['date']}, Open: {data['open']}, High: {data['high']}, Low: {data['low']}, Close: {data['close']}, Volume: {data['volume']}")