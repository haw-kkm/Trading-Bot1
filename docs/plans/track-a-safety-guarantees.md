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

## manual_operator_reset: RISK MITIGATION, not an atomicity guarantee (P1-2)

The exchange query and the DB freeze release are two different systems and cannot be one atomic
step. The reset therefore **reduces** risk; it does not eliminate the time gap.

Implemented mitigations (all covered by `tests/unit/test_p1_2_reset_risk_mitigation.py`):
- No release while any intent is INTENT_PERSISTED / SUBMIT_PENDING / SUBMISSION_UNKNOWN, or while an
  ACCEPTED order's exchange lifecycle is not known to be terminal (a refresh by query is attempted
  first; unqueryable => stays blocked).
- Any whole-account query that is missing, erroring, incomplete or stale => lock kept
  (`INCOMPLETE_VERIFICATION` / `NOT_IMPLEMENTED`; this does not mean a position exists).
- Two full verification passes before release (optional `reset_settle_s` pause between them).
- The DB release trigger re-checks unresolved intents under the gate lock.
- Post-release re-verification; on any change the freeze is re-applied
  (`POST_RELEASE_CHANGE`, `entries_refrozen`).

**Residual races that remain (NOT solved):**
1. A fill / order that reaches the exchange after the last query of the post-release pass but
   before the next ENTRY decision is invisible to the reset.
2. Between the DB release and the post-release pass the gate is open: an ENTRY that wins it
   already holds submit authority and may be sent even though the pass then re-freezes. Re-freezing
   stops only subsequent ENTRYs.
3. The post-release pass checks intents *before* it queries the exchange; an intent created in
   between is only seen by the next verification.
4. `reset_settle_s` is a heuristic delay, not a proof that no fill is pending.
5. Account drawdown is enforced by RiskEngine on every ENTRY; the reset itself does not re-evaluate
   it (reported as `unverified: account_drawdown_limit`).

## P1-1 lifecycle / fills

Submission result (`order_intents.state`) is separate from exchange lifecycle (`exchange_status`)
and cumulative `filled_qty` (+ `order_fills`, UNIQUE per exchange fill id). CANCELED/EXPIRED are
lifecycle outcomes of an ACCEPTED order, never REJECTED. REJECTED is concluded only from an explicit
REJECTED status with executedQty == 0; unclear responses stay SUBMISSION_UNKNOWN. Duplicate fill
delivery is idempotent (DB UNIQUE + OrderGuardian `fill_id`); `filled_qty` can never decrease (DB
trigger). Fills reported WITHOUT an exchange fill id cannot be de-duplicated by OrderGuardian.
The status vocabulary and `executedQty` / `fills[{fillId,qty,price}]` shape are an ASSUMED contract;
Toobit's real format is UNVERIFIED.

## P0-2 account risk

Drawdown = (high-water-mark equity - current equity) / high-water-mark, baseline persisted in
`account_equity` (migration 003), only moved down by an explicit operator rebase. Equity comes from
`get_account_equity()` (ASSUMED shape, UNVERIFIED on Toobit). No source / failed / stale / malformed
data => new ENTRY rejected; risk-reducing orders (token signed `reduce_only`) are never blocked by it
and a reduce-only token can never authorize an ENTRY.
