# Quantitative 1-Second Mean-Reversion Trading Engine

A professional-grade algorithmic trading repository for **high-frequency, 1-second interval** mean-reversion research and execution using **Java**, **Spring Boot**, and the **Interactive Brokers (IBKR) TWS API**.

This project targets microscopic bounce opportunities (roughly **0.05%–0.10%**) via a fast scalping framework built around:
- **14-tick RSI**
- **Relative Volume (RVOL)**
- Highly liquid U.S. equities (for example: **TSLA**, **NVDA**, **SPY**, **SHOP**)

---

## Architecture Overview

### 1) Live Trading Engine (`IBKRTrader.java` + `PingPongStrategy.java`)
The live engine executes the strategy in real time and includes a **Synthetic 1-Second Bar Generator** so live data matches backtest assumptions.

- IBKR live feed provides prices and cumulative volume updates in millisecond cadence.
- `tickSize` (Field 8) is interpreted as **cumulative daily volume**.
- The engine computes clean **1-second volume deltas** from cumulative volume and dispatches them with last price into the strategy.
- This aligns live strategy inputs with your historical 1-second bar format and avoids indicator distortion.

Core risk and safety controls include:
- **Hurricane Lockout** (spread/market quality protection during unstable conditions)
- **Circuit Breaker** (max daily drawdown kill-switch behavior)

### 2) Historical Data Downloader (`IBKR1SecSlowScraper.java`)
The scraper builds clean 1-second historical datasets in a pacing-safe, automated way.

- Accepts symbol dynamically (for example: `NVDA`, `TSLA`)
- Generates output files dynamically as: `SYMBOL_1_Sec_Data.csv`
- Auto-generates trading dates from **January 1st (current year)** through **yesterday**
- Uses chunked historical requests with deliberate pacing pauses to stay compliant with IBKR limits (including the practical limit of ~60 requests per 10 minutes)
- Supports resume/append mode and logs whether each run starts fresh or appends

### 3) Historical Backtester (`HistoricalBacktester.java`)
The backtester is designed to mirror live-engine data shape as closely as possible.

- Symbol can be overridden from command line (or read from properties)
- Auto-resolves CSV filename from active symbol (`SYMBOL_1_Sec_Data.csv`)
- Feeds strategy with equivalent 1-second style inputs
- Enables accurate **Implementation Shortfall** style evaluation between simulated and live execution behavior

---

## Prerequisites

Before running the suite, ensure the following are available:

1. **Java 17+** (JDK)
2. **Maven**
3. **IBKR TWS** or **IB Gateway** running locally (`127.0.0.1`)
4. Active **U.S. Equities live market data subscription** in IBKR
5. IBKR account equity requirement for market data/trading permissions (commonly cited at **$500 USD minimum**, but broker policies can change)

---

## Workflow Pipeline (End-to-End)

### Step 1 — Download Historical 1-Second Data
Run the scraper for each symbol you want to trade/research.

```powershell
mvn spring-boot:run '-Dspring-boot.run.main-class=com.calgary.fili.trader.bot.IBKR1SecSlowScraper' '-Dspring-boot.run.arguments="NVDA"'
mvn spring-boot:run '-Dspring-boot.run.main-class=com.calgary.fili.trader.bot.IBKR1SecSlowScraper' '-Dspring-boot.run.arguments="TSLA"'
```

This will create/update files like:
- `NVDA_1_Sec_Data.csv`
- `TSLA_1_Sec_Data.csv`

### Step 2 — Run Historical Backtests
Backtest each symbol using the same strategy engine.

```powershell
./mvnw spring-boot:run '-Dspring-boot.run.main-class=com.calgary.fili.trader.bot.HistoricalBacktester' '-Dspring-boot.run.arguments="NVDA"'
./mvnw spring-boot:run '-Dspring-boot.run.main-class=com.calgary.fili.trader.bot.HistoricalBacktester' '-Dspring-boot.run.arguments="TSLA"'
```

Use resulting trade logs and PnL outputs to compare strategy behavior versus expected live performance.

---

## Scaling Up: Live Multi-Terminal Matrix

To run multiple live bots simultaneously, open split terminals and launch one process per symbol with unique runtime overrides (application name, client ID, server port, trade log file).

Example for running **TSLA** alongside other bots:

```powershell
./mvnw spring-boot:run '-Dspring-boot.run.arguments="--spring.application.name=tsla-bot --trading.symbol=TSLA --trading.client-id=1 --server.port=8080 --trading.log.file=tsla-trades.csv"'

./mvnw spring-boot:run '-Dspring-boot.run.arguments="--spring.application.name=tsla-bot --trading.symbol=TSLA --trading.client-id=1 --server.port=8080 --trading.log.file=tsla-trades.csv --trading.paused=true"' | tee tsla.txt
```

Recommended isolation per bot instance:
- Unique `--trading.client-id`
- Unique `--server.port`
- Unique `--trading.log.file`

---

## Disclaimer

This repository is provided for **educational and research purposes only**.

Algorithmic trading and high-frequency execution involve substantial financial, technical, and operational risk, including the risk of significant loss of capital. Nothing in this project constitutes financial advice, investment advice, broker/dealer activity, or a recommendation to buy/sell any security. You are solely responsible for validating strategy behavior, risk controls, infrastructure stability, and regulatory compliance before any live deployment.
