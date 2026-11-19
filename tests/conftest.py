"""Shared fixtures. The DB layer is REAL PostgreSQL (embedded server, no mocks); only the
exchange network layer is mocked."""

import tempfile

import pytest

from src.core.execution_engine import ExecutionEngine

_state = {"server": None, "dsn": None, "dir": None}


def _server_dsn() -> str:
    if _state["dsn"] is None:
        import pixeltable_pgserver as pgserver
        from src.orders.pg_store import migrate
        _state["dir"] = tempfile.mkdtemp(prefix="pgtest_")
        _state["server"] = pgserver.get_server(_state["dir"], cleanup_mode="stop")
        _state["dsn"] = _state["server"].get_uri()
        migrate(_state["dsn"])
    return _state["dsn"]


def _reset_tables(dsn: str) -> None:
    import psycopg
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("TRUNCATE order_events, order_intents, authorizations RESTART IDENTITY CASCADE")
        c.execute("UPDATE entry_control SET frozen=FALSE, reason=NULL, frozen_at=NULL WHERE id=1")


@pytest.fixture(scope="session")
def pg_dsn():
    return _server_dsn()


@pytest.fixture
def store(pg_dsn):
    from src.orders.pg_store import OrderStore
    _reset_tables(pg_dsn)
    return OrderStore(pg_dsn)


@pytest.fixture(autouse=True)
def _default_engine_store(request):
    """Legacy engine tests build ExecutionEngine() with no store: give them a clean real-PG one
    (lazily, so tests that never build an engine pay nothing)."""
    from src.orders.pg_store import OrderStore
    fresh = {"done": False}

    def factory():
        dsn = _server_dsn()
        if not fresh["done"]:
            _reset_tables(dsn)
            fresh["done"] = True
        return OrderStore(dsn)

    ExecutionEngine.default_store_factory = factory
    yield
    ExecutionEngine.default_store_factory = None


def pytest_sessionfinish(session, exitstatus):
    if _state["server"] is not None:
        _state["server"].cleanup()
