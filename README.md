# alpaca-donichan-adx-vf-bot

# System Architecture Documentation

## Domain Layer
The **Domain Layer** contains the core business logic of the trading system.  

It includes:
- **Algorithms**: strategy implementations that define trading behavior.  
- **Strategy Interface**: the contract through which all strategies must be exposed, located in the `strategy` folder.  
- **Utilities**: helper functions and indicators available only to algorithms within this layer.  

The Domain Layer is **independent of infrastructure and external systems**.  
Its sole responsibility is to decide *what action to take* (e.g., buy, sell, hold).  

---

## Infrastructure Layer
The **Infrastructure Layer** provides access to external systems and data storage.  

It includes:
- **Hybrid Database Access**: local and remote database connectivity.  
- **Database Interface**: a repository-style interface that abstracts the underlying storage implementation.  

This layer is responsible for *how data is stored and retrieved*,  
but it has no knowledge of trading strategies or application logic.  

---

## Application Layer
The **Application Layer** orchestrates the interaction between the **Domain Layer** and the **Infrastructure Layer**.  

It includes two distinct systems:
- **Backtester**: runs trading strategies against historical data.  
- **Live Trader**: executes trading strategies in real-time markets.  

This layer is responsible for *running the trading process*,  
feeding data into algorithms, and executing their decisions through the infrastructure.  
