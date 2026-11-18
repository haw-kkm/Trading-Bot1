# Architecture Patterns

**Domain:** Multi-Agent Crypto Trading System
**Researched:** 2026-11-19

## Recommended Architecture

A message-driven, event-sourced architecture utilizing a central message broker to ensure asynchronous communication, auditability, and isolation. Agents are decoupled and communicate via strongly-typed schemas.

### Component Boundaries

| Component | Responsibility | Communicates With |
|-----------|---------------|-------------------|
| **Data Ingestion** | Normalizes market data/order book streams | Market Broker |
| **Strategy Agents** | Analyze data, emit trade signals | Strategy Broker |
| **RiskEngine** | Validates signals against account limits/volatility | OrderGuardian, Market Broker |
| **OrderGuardian** | Acts as final gatekeeper for order execution | Execution Engine, Strategy Agents |
| **Execution Engine**| Manages order placement & life-cycle | Exchange APIs |

### Data Flow

1. **Ingestion** pushes normalized data to the `MarketData` topic.
2. **Strategy Agents** consume market data and emit trade intentions to the `Signals` topic.
3. **RiskEngine** consumes `Signals` and performs pre-trade validation (e.g., exposure checks, wash trading checks).
4. **OrderGuardian** consumes validated signals. It maintains an independent state of open orders and ensures that even if a strategy agent is compromised or buggy, the order is blocked if it violates safety rules.
5. **Execution Engine** receives approved orders and manages interaction with the exchange.

## Isolation Strategies: RiskEngine & OrderGuardian

To ensure safety, the **RiskEngine** and **OrderGuardian** must operate with "Zero Trust" regarding other agents.

- **Immutable Configuration:** Risk rules (max position, max drawdown, etc.) must be defined in a tamper-evident configuration store, not passed dynamically by Strategy Agents.
- **Hardware/Process Isolation:** In high-stakes environments, run these components in separate containers/nodes with restricted network access, communicating only over mTLS-protected RPC or private bus.
- **Circuit Breakers:** The **OrderGuardian** must have a hard-coded "Kill Switch" that can pause all outbound traffic regardless of state.

## Patterns to Follow

### Pattern 1: Event Sourcing
**What:** Store every signal, validation decision, and order event in an append-only log.
**When:** Essential for auditability and post-trade analysis.
**Example:**
```typescript
interface TradeSignal {
  id: string;
  strategyId: string;
  symbol: string;
  side: 'buy' | 'sell';
  amount: number;
  timestamp: number;
}
```

## Anti-Patterns to Avoid

### Anti-Pattern 1: Direct Agent Communication
**What:** Strategy Agents calling the Execution Engine directly.
**Why bad:** Bypasses risk checks; creates tight coupling.
**Instead:** All orders must flow through the RiskEngine/OrderGuardian pipeline.

## Scalability Considerations

| Concern | At 100 orders/sec | At 10K orders/sec |
|---------|--------------|--------------|
| **Latency** | Single Node Broker | Distributed Message Bus (e.g., NATS/Kafka) |
| **Risk Validation** | In-memory lookup | Distributed Cache (e.g., Redis Cluster) |

## Sources

- Distributed Systems Design Patterns
- Financial Engineering Best Practices (Order Validation)
