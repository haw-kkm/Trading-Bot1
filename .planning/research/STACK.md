# Technology Stack

**Project:** Multi-Agent Crypto Trading Bot
**Researched:** 2026-11-19

## Recommended Stack

### Core Framework
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| Node.js / TypeScript | Latest LTS | Core Application | Industry standard for non-blocking I/O, critical for multi-agent WebSocket handling. |
| Redis | 7.x | Message Bus / Cache | Essential for inter-agent communication and low-latency order/position tracking. |
| NestJS | Latest | Microservices | Provides a structured, modular architecture, ideal for separating distinct agent responsibilities. |

### Database
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| PostgreSQL | 16.x | Persistent Logs | Stores trade history, risk rule configs, and audit logs. |
| InfluxDB | 2.x | Time-series Data | Optimal for storing tick data and market snapshots for post-trade analysis. |

### Infrastructure
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| WebSocket | Native | Exchange Connectivity | Used for Binance and Toobit streaming market data and order updates. |
| REST API | Axios / Fetch | Non-critical Ops | For account setup, historical queries, and order placement fail-overs. |

### Supporting Libraries
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| CCXT | Latest | Unified Exchange API | Initial exchange abstraction; for production, wrap in custom connectors to minimize latency. |
| nodejs-order-book | Latest | Order Book Engine | For in-process limit-order book maintenance. |

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Language | TypeScript | Rust | TypeScript provides faster DX and rapid iteration; Rust is better for extreme HFT but increases development time. |
| Bus | Redis | Kafka | Redis is lower latency for simple Pub/Sub; Kafka is over-engineered for this scale. |

## Installation

```bash
# Core Dependencies
npm install nestjs redis pg ccxt nodejs-order-book

# Dev dependencies
npm install -D typescript @types/node
```

## Sources

- [Hummingbot Architecture](https://hummingbot.org/developers/architecture/)
- [Binance/Toobit API Docs](https://api-docs.toobit.com/)
- [Node.js HFT Engine Article](https://vacuumlabs.com/articles/high-frequency-trading-engine-in-node-js/)
