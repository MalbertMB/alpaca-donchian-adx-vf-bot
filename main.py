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

db = SQLiteDatabase()
manager = DataManager(db)

open_trades = manager.get_open_trades()
for trade in open_trades:
    print(trade)
