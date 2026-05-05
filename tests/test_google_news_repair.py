from __future__ import annotations

from curator.google_news_repair import db_operation, transient_mysql_connection_error


class FakeConnection:
    def __init__(self) -> None:
        self.pings = 0

    def ping(self, *, reconnect: bool = False) -> None:
        assert reconnect is True
        self.pings += 1


def test_transient_mysql_connection_error_detects_lost_connection() -> None:
    assert transient_mysql_connection_error(Exception(2013, "Lost connection to MySQL server during query"))
    assert transient_mysql_connection_error(Exception(2006, "MySQL server has gone away"))
    assert not transient_mysql_connection_error(Exception(1062, "Duplicate entry"))


def test_db_operation_retries_transient_mysql_connection_error() -> None:
    conn = FakeConnection()
    calls = 0

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise Exception(2013, "Lost connection to MySQL server during query")
        return "ok"

    assert db_operation(conn, "test operation", operation) == "ok"
    assert calls == 2
    assert conn.pings == 2
