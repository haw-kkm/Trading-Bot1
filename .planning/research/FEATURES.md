# Feature Landscape

**Domain:** Multi-agent Crypto Trading System
**Researched:** 2026-11-19

## Table Stakes
Features users expect. Missing = product feels incomplete.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Data Normalization | Cross-exchange price/liquidity parity | High | Must handle varying tick sizes, API latency, and formats. |
| Order Execution | VWAP/TWAP and Iceberg algorithms | Medium | Critical for minimizing market impact. |
| Real-time Portfolio | Consolidated view across exchanges | Med | Must handle websocket streams concurrently. |
| Basic Risk Controls | Max position size, daily loss limit | Low | Essential guardrails for any automated system. |

## Differentiators
Features that set product apart. Not expected, but valued.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Multi-Agent Strategy | Independent agents for alpha, hedge, and execution | High | Allows decoupling of logic and parallel scaling. |
| Adversarial Testing | Automated stress testing against bad data | High | Simulates exchange outages, front-running, and spoofing. |
| Dynamic Risk Limits | Real-time volatility-adjusted sizing | High | Adjusts exposure based on market regime. |

## Anti-Features
Features to explicitly NOT build.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Manual Override UI | Adds human latency and emotion to high-frequency loops | Use pre-defined "Circuit Breaker" policies. |
| Single-Agent Monolith | Single point of failure; hard to debug | Distributed agent architecture. |

## Feature Dependencies

```
Data Normalization → Risk Control Types → Adversarial Analysis
```

## MVP Recommendation

Prioritize:
1. Data Normalization (Unified market feed)
2. Basic Risk Controls (Max position/daily loss)
3. Order Execution (TWAP/VWAP)

Defer:
- Full Multi-Agent strategy until normalization and risk framework are stable.
- Advanced Adversarial testing until basic execution flows are tested.

## Sources
- Internal crypto trading architecture standards, risk management guidelines for algorithmic trading.
