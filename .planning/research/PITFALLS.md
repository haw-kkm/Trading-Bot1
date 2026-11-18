# Domain Pitfalls: Multi-Agent Trading Systems

**Domain:** Multi-Agent Trading
**Researched:** 2026-11-19

## Critical Pitfalls

Mistakes that cause financial loss, regulatory breaches, or system instability.

### Pitfall 1: Race Conditions between OrderGuardian and Execution
**What goes wrong:** An `OrderGuardian` approves a trade based on state (e.g., account balance or position limits) that changes before the `Execution` agent places the order.
**Why it happens:** Asynchronous message passing or lack of atomic state updates allows the `Execution` agent to act on stale authorization data.
**Consequences:** Over-leveraging, margin calls, or executing trades that violate compliance constraints.
**Prevention:** Implement a "reservation" or "lock" pattern where the `OrderGuardian` reserves the required capital/position margin at the time of approval, which the `Execution` agent must clear.
**Detection:** Monitor discrepancies between "authorized order value" and "executed order value" in post-trade logs.

### Pitfall 2: API Rate Limits
**What goes wrong:** Agents bombard exchange APIs with requests, causing 429 (Too Many Requests) errors during high-volatility events.
**Why it happens:** Independent agents running in parallel do not have a centralized rate-limiting queue, exceeding exchange quotas.
**Consequences:** Missed execution opportunities, account temporary bans, and system-wide stall as agents retry indefinitely.
**Prevention:** Implement a centralized `Gateway` or `Sequencer` that manages all outbound API traffic, enforcing token bucket rate-limiting across all agents.
**Detection:** High frequency of 429 status codes in outbound API logs.

### Pitfall 3: Market Data Lag
**What goes wrong:** Agents make decisions based on delayed price updates, executing at disadvantageous prices ("slippage").
**Why it happens:** Network latency between the exchange and the agent, or computational overhead within the agent's decision loop.
**Consequences:** Adverse selection (trading against sophisticated market participants who have the latest info), consistently poor fill quality.
**Prevention:** Use high-performance network stacks, prioritize market data processing threads, and implement "stale data" circuit breakers that halt trading if data latency exceeds a threshold.
**Detection:** Compare "decision time" vs "market snapshot timestamp"; track high deviation in execution price vs mid-market price at decision time.

## Moderate Pitfalls

### Pitfall 1: Ghost Liquidity / Order Chasing
**What goes wrong:** Multiple agents attempt to capture the same liquidity, causing an artificial "spike" that disappears, leading to filled orders at poor prices.
**Prevention:** Implement a global coordinator agent that aggregates intent before publishing to the `Execution` layer.

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Order Routing | Race Condition | Reservation-based authorization |
| API Integration | Rate Limits | Centralized Gateway pattern |
| Market Analysis | Data Lag | Latency monitoring and circuit breakers |

## Sources

- Distributed Systems Architecture (Atomic Commit Patterns)
- Financial Engineering Best Practices (Execution Latency)
- Exchange API Documentation (Rate Limit Management)
