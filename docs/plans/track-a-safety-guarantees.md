# Track A safety guarantees (PostgreSQL entry gate) - SINGLE_ACCOUNT_ONLY

| Property | Level |
|---|---|
| Same authorization/token consumed twice locally | **Guaranteed** (conditional UPDATE + UNIQUE constraints, real-PG tested) |
| New ENTRY obtaining submit authority after a SUBMISSION_UNKNOWN has committed (or while frozen) | **Guaranteed, single account only** (advisory lock + fresh re-read + DB trigger backstop) |
| An ENTRY that *already holds* SUBMIT_PENDING being sent after another process commits UNKNOWN | **Mitigated only**: pre-send re-check narrows the window; not closed (no DB lock/tx across API calls) |
| Exactly-once placement at the exchange | **Not guaranteed** (Toobit duplicate-clientOrderId behaviour unverified) |
| Multiple trading accounts | **Not supported** (no account_id; global lock key) |

Unresolved (tracked): PROTECTION_UNKNOWN (SL/TP registration is outside the persisted pipeline);
authority-to-wire window; SINGLE_ACCOUNT_ONLY; no exactly-once at the exchange.
All exchange response shapes used (executedQty, status, reduceOnly, complete/as_of, position
mode, cancel semantics) are an ASSUMED mock/paper contract - real Toobit formats are UNVERIFIED.
