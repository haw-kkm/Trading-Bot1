"""Authenticated-account interface (DEFINITION ONLY; nothing here talks to an exchange).

Method names match what the order/risk engines already call on the exchange client
(get_account_equity, get_all_positions, get_open_orders, get_conditional_orders) plus
get_position_mode. With no credentials configured, every method raises NotConfigured; it never
returns fabricated balances or positions. PaperExchange (tests) keeps its own mock values.
"""


class NotConfigured(RuntimeError):
    """NOT_CONFIGURED: no authenticated exchange adapter / credentials are configured."""


class AccountInterface:
    def get_account_equity(self):
        raise NotImplementedError

    def get_all_positions(self):
        raise NotImplementedError

    def get_open_orders(self, *a, **k):
        raise NotImplementedError

    def get_conditional_orders(self, *a, **k):
        raise NotImplementedError

    def get_position_mode(self):
        raise NotImplementedError


class UnconfiguredAccount(AccountInterface):
    def _no(self, name):
        raise NotConfigured(f"NOT_CONFIGURED: {name} requires an authenticated adapter (none configured)")

    def get_account_equity(self):
        self._no("get_account_equity")

    def get_all_positions(self):
        self._no("get_all_positions")

    def get_open_orders(self, *a, **k):
        self._no("get_open_orders")

    def get_conditional_orders(self, *a, **k):
        self._no("get_conditional_orders")

    def get_position_mode(self):
        self._no("get_position_mode")
