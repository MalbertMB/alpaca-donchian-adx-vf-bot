# TFT-GNN Evolutionary Graph Trading System

A professional-grade autonomous trading system and predictive AI engine developed for a Bachelor's Thesis (TFG). This project implements a cutting-edge quantitative strategy that leverages a hybrid Temporal Fusion Transformer (TFT) and Graph Neural Network (GNN) architecture to model dynamic market relationships and execute trades in real-time.

## Project Architecture

The system follows a Clean Architecture pattern to ensure maintainability, testability, and a clear separation between the deep learning models and the market execution logic.

* **Application**: Orchestrates execution logic for live trading, data pipelines, and Monte Carlo stochastic evaluation.
* **Domain**: The core of the project. Includes the neural network architectures (TFT, GNN), feature engineering logic, and mathematical business objects independent of external libraries.
* **Infrastructure**: Handles external dependencies, including database implementations, the Alpaca API client, and high-fidelity data persistence.

---

## Repository Structure

```text
TFT-GNN-TRADING-SYSTEM
├── Application/
│   ├── evaluation/           # Monte Carlo simulation pipelines
│   ├── config/               # Configuration and environment management
│   ├── interfaces/           # High-level trader and predictor interfaces
│   └── live_trading/         # Real-time execution and paper trading logic
├── Domain/
│   ├── models/
│   │   ├── gnn/              # Dynamic Adjacency Matrix and Graph Networks
│   │   └── tft/              # Temporal Fusion Transformer implementation
│   ├── features/             # Feature engineering (e.g., Fractional Differentiation)
│   └── objects/              # Data objects (Tensors, Signals, Positions)
├── Infrastructure/
│   ├── backtester/           # Database implementations for historical results
│   ├── interfaces/           # DB and API abstract interfaces
│   ├── live_trader/          # Live database persistence
│   └── market/               # Alpaca Market data management and storage
├── main.py                   # Entry point for the application
├── .env                      # API Keys and Secrets (Local only)
└── requirements.txt          # Project dependencies
```

---

## Getting Started

### Prerequisites
* Python 3.10 or higher
* Alpaca Markets account (Paper or Live)
* CUDA-compatible GPU (highly recommended for deep learning model training)

### Installation
1. Clone the repository:
```bash
git clone https://github.com/MalbertMB/tft-gnn-trading-system.git
cd tft-gnn-trading-system
```

2. Set up a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure Environment Variables:
Create a `.env` file in the root directory:
```env
ALPACA_API_KEY=your_key_here
ALPACA_SECRET_KEY=your_secret_here
BASE_URL=https://paper-api.alpaca.markets
```

---

## Model & Strategy Overview

The system treats the financial market as an interconnected ecosystem, aiming to overcome the low signal-to-noise ratio inherent in short-term trading.

### Core AI Engine

* **Spatial Engine (Dynamic GNN)**: Utilizes a Dynamic Adjacency Matrix that recalculates edge weights at each time step using rolling Pearson correlation or Mutual Information metrics. This enables the model to rapidly adapt to sudden changes in market correlations.
* **Temporal Engine (TFT)**: Integrates spatial features into a Temporal Fusion Transformer to achieve robust multi-horizon forecasting.
* **Feature Engineering**: Applies Fractional Differentiation to time series data, ensuring the data remains stationary while preserving crucial historical memory.

### Execution & Integration

* **Alpaca API**: The infrastructure is powered by the Alpaca API to fetch high-fidelity historical and real-time OHLC data.
* **Paper Trading Pipeline**: Executes trades with simulated capital under real market conditions to validate latency, slippage, and model synchronization before live deployment.

### Risk Management & Evaluation

Moving beyond standard historical backtesting, the system relies on stochastic validation:

* **Monte Carlo Simulations**: By collecting paper trading or backtesting operations, the system generates thousands of yield curve permutations.
* **Metrics**: This analysis determines the mathematical expectation of the system across different scenarios, rigorously calculating maximum drawdown risk and the probability of ruin.

---

## Tech Stack
* **Deep Learning**: PyTorch, PyTorch Geometric, PyTorch Forecasting
* **Execution**: alpaca-py
* **Data Science**: pandas, numpy, networkx (for graph generation)
* **Database**: SQLite (managed via Infrastructure layer)
* **Validation**: pydantic
* **Visualization**: matplotlib, plotly (for Monte Carlo distribution charts)

---

## Disclaimer
Trading involves significant risk. This software is for educational purposes only and is developed as part of academic research. Always test thoroughly in a paper trading environment before risking real capital.

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
