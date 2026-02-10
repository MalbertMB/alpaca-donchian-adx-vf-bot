# Alpaca Donchian-ADX-VF Trading Bot

A professional-grade real-time trading bot and backtesting engine. This project implements a quantitative strategy leveraging Donchian Channels, Average Directional Index (ADX), and a Volatility Factor (VF) to navigate market trends with precision.

## Project Architecture

The system follows a Clean Architecture pattern to ensure maintainability and testability by decoupling business logic from external dependencies.

* Application: Orchestrates execution logic for live trading and backtesting environments.
* Domain: The core of the project. Includes the trading strategy, mathematical indicators, and business objects independent of external libraries.
* Infrastructure: Handles the details including database implementations, API clients (Alpaca), and data persistence.

---

## Repository Structure

```text
ALPACA-DONCHIAN-ADX-VF-BOT
├── Application/
│   ├── backtest/             # Backtesting execution logic
│   ├── config/               # Configuration and environment management
│   ├── interfaces/           # High-level trader interfaces
│   └── live_trading/         # Real-time execution logic
├── Domain/
│   ├── algorithms/
│   │   └── strategy/         # Strategy implementation (Donchian + ADX + VF)
│   ├── objects/              # Data objects (Signals, Positions)
│   └── utils/                # Helpers, Indicators, and Performance Metrics
├── Infrastructure/
│   ├── backtester/           # Database implementations for backtest results
│   ├── interfaces/           # DB and API abstract interfaces
│   ├── live_trader/          # Live database persistence
│   └── market/               # Market data management and storage
├── main.py                   # Entry point for the application
├── .env                      # API Keys and Secrets (Local only)
└── requirements.txt          # Project dependencies
```

---

## Getting Started

### Prerequisites
* Python 3.10 or higher
* Alpaca Markets account (Paper or Live)

### Installation
1. Clone the repository:
```text
   git clone https://github.com/MalbertMB/alpaca-donchian-adx-vf-bot.git
   cd alpaca-donchian-adx-vf-bot
```

2. Set up a virtual environment:
```text
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
```

3. Install dependencies:
   pip install -r requirements.txt

4. Configure Environment Variables:
   Create a .env file in the root directory:
```text
   ALPACA_API_KEY=your_key_here
   ALPACA_SECRET_KEY=your_secret_here
   BASE_URL=https://paper-api.alpaca.markets
```
---

## Strategy Overview

The system implements a trend-following logic that filters breakouts based on trend strength and local volatility.

### Core Components

* **Donchian Channels**: These establish the trading range. A breakout above the upper channel (n-period high) generates a potential long signal, while a drop below the lower channel (n-period low) generates a potential exit or short signal.
* **Average Directional Index (ADX)**: Serves as the primary trend filter. The bot only considers entries when the ADX is above a specific threshold (typically 25), indicating that the market is in a trending phase rather than a range-bound phase.
* **Volatility Factor (VF)**: Used to normalize entry sensitivity. It compares the current market volatility (Standard Deviation) against a long-term Average True Range (ATR) to adjust position sizing or stop-loss distances dynamically.

### Entry Logic

A Long position is initiated when:
1. The closing price exceeds the Upper Donchian Channel.
2. The ADX is greater than the trend threshold.
3. The Volatility Factor confirms that the current breakout is not an outlier driven by extreme, unsustainable noise.

### Exit Logic

Positions are closed when:
1. The price crosses the mid-point or the opposite Donchian Channel.
2. The ADX falls below the threshold, suggesting the trend has exhausted.
3. A volatility-adjusted trailing stop is triggered.

---

## Tech Stack
* Execution: alpaca-py (v0.43.2)
* Data Analysis: pandas (v3.0.0), numpy (v2.3.5), numba (v0.63.1)
* Technical Analysis: ta (v0.11.0), vectorbt (v0.28.2)
* Database: SQLite (managed via Infrastructure layer)
* Validation: pydantic (v2.12.5)
* Visualization: matplotlib (v3.10.8), plotly (v6.5.2)

---

## Disclaimer
Trading involves significant risk. This software is for educational purposes only. Always test in a paper trading environment before using real capital.