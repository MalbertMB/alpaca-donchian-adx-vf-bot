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