"""PaperExchange: a stateful in-memory Toobit stand-in for tests. NO network, NO real API.

Every response shape here (executedQty, status, reduceOnly, complete, as_of, position rows,
position mode, cancel semantics) is an ASSUMED contract. The real Toobit API formats and its
reduce-only / position-mode behaviour are UNVERIFIED; passing tests against this class does
not verify Toobit.
"""

import time


class PaperExchange:
    is_simulated = True

    def __init__(self, position=0.0, symbol="BTCUSDT", mode="ONE_WAY", extreme=False):
        self.symbol = symbol
        self.positions = {symbol: position}       # signed: +long / -short
        self.mode = mode
        self.extreme = extreme
        self.open_orders = []                     # dicts: orderId, symbol, side, reduceOnly, status
        self.conditional_orders = []
        self.orders = {}                          # clientOrderId -> record
        self.sent = []                            # (kind, side, qty, client_order_id, reduce_only)
        # ---- behaviour knobs
        self.ioc_fill_ratio = 1.0
        self.market_fill_ratio = 1.0
        self.unfilled_terminal_status = "EXPIRED"
        self.sl_tp_ok = False
        self.query_mode = "ok"                    # ok | raise | empty | no_qty | no_cid
        self.query_status_override = None
        self.query_executed_override = None
        self.positions_mode = "ok"                # ok | raise | incomplete | stale | malformed
        self.open_orders_mode = "ok"
        self.conditional_mode = "ok"
        self.cancel_confirms = True               # cancel_order removes the order
        self.cancel_conditional_confirms = True
        self.fill_open_order_on_cancel = False    # race: order fills while being cancelled
        self.has_account_api = True
        self.fail_sends = None                    # exception to raise on every order send
        self.equity = 100000.0
        self.equity_mode = "ok"                   # ok | raise | stale | malformed | zero
        self._n = 0

    # ---- orders -----------------------------------------------------------------------
    def _execute(self, kind, symbol, side, qty, cid, reduce_only, ratio):
        self.sent.append((kind, side, qty, cid, reduce_only))
        if self.fail_sends:
            raise self.fail_sends
        self._n += 1
        pos = self.positions.get(symbol, 0.0)
        want = qty
        if reduce_only:
            # exchange-side reduce-only clamp: can only shrink the position, never flip it
            closing = (side == "SELL" and pos > 0) or (side == "BUY" and pos < 0)
            want = min(qty, abs(pos)) if closing else 0.0
        executed = want * ratio
        self.positions[symbol] = pos - executed if side == "SELL" else pos + executed
        status = "FILLED" if executed >= qty - 1e-12 else (
            "PARTIALLY_FILLED" if (executed > 0 and kind == "market") else
            (self.unfilled_terminal_status if executed == 0 else "EXPIRED"))
        self.orders[cid] = {"clientOrderId": cid, "orderId": f"P{self._n}", "status": status,
                            "symbol": symbol, "side": side, "origQty": qty, "executedQty": executed}
        # a bare ACK: deliberately carries NO fill information
        return {"status": "SUCCESS", "orderId": f"P{self._n}", "clientOrderId": cid}

    def execute_market_order(self, symbol, side, quantity, client_order_id=None, reduce_only=False):
        return self._execute("market", symbol, side, quantity, client_order_id, reduce_only,
                             self.market_fill_ratio)

    def execute_ioc_order(self, symbol, side, quantity, price, client_order_id=None, reduce_only=False):
        return self._execute("ioc", symbol, side, quantity, client_order_id, reduce_only,
                             self.ioc_fill_ratio)

    def execute_reduce_only_order(self, symbol, side, quantity, price, order_type="LIMIT",
                                  client_order_id=None, reduce_only=True):
        return self._execute("reduce", symbol, side, quantity, client_order_id, True, 1.0)

    def query_order(self, cid):
        if self.query_mode == "raise":
            raise TimeoutError("query timed out")
        if self.query_mode == "empty":
            return {}
        rec = dict(self.orders.get(cid) or {})
        if not rec:
            return {}
        if self.query_status_override:
            rec["status"] = self.query_status_override
        if self.query_executed_override is not None:
            rec["executedQty"] = self.query_executed_override
        if self.query_mode == "no_qty":
            rec.pop("executedQty", None)
        if self.query_mode == "no_cid":
            rec.pop("clientOrderId", None)
        return rec

    # ---- account queries ---------------------------------------------------------------
    def _wrap(self, mode, key, rows):
        if mode == "raise":
            raise ConnectionError("account query failed")
        if mode == "incomplete":
            return {"complete": False, "as_of": time.time(), key: rows}
        if mode == "stale":
            return {"complete": True, "as_of": time.time() - 3600, key: rows}
        return {"complete": True, "as_of": time.time(), key: rows}

    def get_all_positions(self):
        if not self.has_account_api:
            raise AttributeError
        rows = [{"symbol": s, "side": "LONG" if q > 0 else "SHORT", "quantity": abs(q)}
                for s, q in self.positions.items() if abs(q) > 1e-12]
        if self.positions_mode == "malformed":
            rows = [{"symbol": self.symbol, "side": "???", "quantity": "x"}]
        return self._wrap(self.positions_mode, "positions", rows)

    def get_position_mode(self):
        if self.mode == "raise":
            raise ConnectionError("mode query failed")
        return self.mode

    def get_open_orders(self):
        return self._wrap(self.open_orders_mode, "orders", [dict(o) for o in self.open_orders])

    def get_conditional_orders(self):
        return self._wrap(self.conditional_mode, "orders", [dict(o) for o in self.conditional_orders])

    def cancel_order(self, order_id):
        for o in list(self.open_orders):
            if o["orderId"] == order_id:
                if self.fill_open_order_on_cancel:     # fill beats cancel
                    q = o.get("qty", 1.0)
                    self.positions[o["symbol"]] = self.positions.get(o["symbol"], 0.0) + (
                        q if o["side"] == "BUY" else -q)
                    self.open_orders.remove(o)
                elif self.cancel_confirms:
                    self.open_orders.remove(o)
        return {"status": "SUCCESS"}

    def cancel_conditional_order(self, order_id):
        if self.cancel_conditional_confirms:
            self.conditional_orders = [o for o in self.conditional_orders if o["orderId"] != order_id]
        return {"status": "SUCCESS"}

    def get_account_equity(self):
        """ASSUMED contract: {"equity": float, "as_of": epoch seconds}. Toobit format unverified."""
        if self.equity_mode == "raise":
            raise ConnectionError("equity query failed")
        if self.equity_mode == "stale":
            return {"equity": self.equity, "as_of": time.time() - 3600}
        if self.equity_mode == "malformed":
            return {"balance": self.equity}
        if self.equity_mode == "zero":
            return {"equity": 0, "as_of": time.time()}
        return {"equity": self.equity, "as_of": time.time()}

    # ---- legacy surface used by older tests --------------------------------------------
    def register_sl_tp(self, symbol, side, quantity, price, stop_loss, take_profit):
        return {"status": "SUCCESS" if self.sl_tp_ok else "FAILED"}

    def get_position(self, symbol):
        return {"position_size": abs(self.positions.get(symbol, 0.0))}

    def is_extreme_conditions(self):
        return self.extreme
