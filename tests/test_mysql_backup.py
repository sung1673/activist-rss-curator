from __future__ import annotations

import gzip
import hashlib
import json
import re
import socket
import threading
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

import curator.mysql_backup as mysql_backup_module
from curator.mysql_backup import (
    ConnectionOptions,
    MySqlBackupError,
    SshDirectTcpipTunnel,
    SshTunnelOptions,
    build_arg_parser,
    connection_options_from_args,
    create_mysql_backup,
    legacy_ssh_rsa_sha1_is_allowed,
    quote_identifier,
    ssh_host_key_sha256,
    ssh_options_from_args,
    sql_literal,
    verify_ssh_host_key,
)


REVISION = "a" * 40


class FakeMySqlOperationalError(Exception):
    pass


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        self.description: list[tuple[str]] | None = None
        self.rows: list[object] = []
        self.position = 0
        self.statement = ""

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: str, params: tuple[object, ...] | None = None) -> None:
        normalized = " ".join(statement.split())
        self.statement = normalized
        self.connection.log.append((self.connection.name, normalized, params))
        self.description = None
        self.rows = []
        self.position = 0
        for prefix, error in self.connection.statement_errors.items():
            if normalized.startswith(prefix):
                raise error
        if normalized.startswith(
            "SELECT (SELECT COUNT(*) FROM information_schema.VIEWS"
        ):
            self.rows = [self.connection.object_counts]
            return
        if normalized.startswith("SELECT TABLE_NAME, ENGINE FROM information_schema"):
            if self.connection.table_snapshots:
                self.rows = list(self.connection.table_snapshots.pop(0))
            else:
                self.rows = list(self.connection.tables)
            return
        if normalized.startswith(
            "SELECT COLUMN_NAME, EXTRA, GENERATION_EXPRESSION "
            "FROM information_schema.COLUMNS"
        ):
            assert params is not None
            table = str(params[1])
            self.rows = list(self.connection.column_metadata[table])
            return
        if normalized.startswith("SHOW CREATE TABLE "):
            table = self.connection.table_from_statement(normalized)
            self.rows = [(table, self.connection.creates[table])]
            return
        if normalized.startswith("SELECT ") and " FROM `" in normalized:
            table = self.connection.table_from_statement(normalized)
            if self.connection.fail_table == table:
                raise RuntimeError("synthetic database read failure")
            columns, rows = self.connection.data[table]
            projection = normalized.split("SELECT ", 1)[1].rsplit(" FROM `", 1)[0]
            if projection == "1":
                selected_columns: list[str] = []
                self.description = [("1",)]
                self.rows = [(1,) for _row in rows]
            else:
                selected_columns = [
                    item[1:-1].replace("``", "`") for item in projection.split(",")
                ]
                indexes = [columns.index(column) for column in selected_columns]
                self.description = [(column,) for column in selected_columns]
                self.rows = [tuple(row[index] for index in indexes) for row in rows]

    def fetchall(self) -> list[object]:
        return list(self.rows)

    def fetchone(self) -> object | None:
        return self.rows[0] if self.rows else None

    def fetchmany(self, size: int) -> list[object]:
        result = self.rows[self.position : self.position + size]
        self.position += len(result)
        return result


class FakeConnection:
    def __init__(
        self,
        name: str,
        *,
        tables: list[tuple[str, str]],
        creates: dict[str, str],
        data: dict[str, tuple[list[str], list[tuple[object, ...]]]],
        log: list[tuple[str, str, object]],
        fail_table: str | None = None,
        column_metadata: dict[str, list[tuple[str, str, str]]] | None = None,
        object_counts: tuple[int, int, int, int] = (0, 0, 0, 0),
        statement_errors: dict[str, Exception] | None = None,
        table_snapshots: list[list[tuple[str, str]]] | None = None,
        close_errors: list[Exception] | None = None,
    ) -> None:
        self.name = name
        self.tables = tables
        self.creates = creates
        self.data = data
        self.column_metadata = column_metadata or {
            table: [(column, "", "") for column in columns]
            for table, (columns, _rows) in data.items()
        }
        self.object_counts = object_counts
        self.log = log
        self.fail_table = fail_table
        self.statement_errors = statement_errors or {}
        self.table_snapshots = table_snapshots or []
        self.close_errors = close_errors or []
        self.rolled_back = False
        self.opened = False
        self.closed = False
        self.close_calls = 0

    def cursor(self, *_args: object, **_kwargs: object) -> FakeCursor:
        return FakeCursor(self)

    def table_from_statement(self, statement: str) -> str:
        quoted = re.findall(r"`((?:``|[^`])*)`", statement)
        if not quoted:
            raise AssertionError(f"statement has no quoted table: {statement}")
        return quoted[-1].replace("``", "`")

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.close_calls += 1
        if self.close_errors:
            raise self.close_errors.pop(0)
        self.closed = True


def fixture_connections(
    *,
    fail_table: str | None = None,
) -> tuple[
    list[FakeConnection],
    list[tuple[str, str, object]],
    Any,
]:
    log: list[tuple[str, str, object]] = []
    tables = [("events", "InnoDB"), ("legacy", "MyISAM")]
    creates = {
        "events": (
            "CREATE TABLE `events` ("
            "`id` bigint NOT NULL,"
            "`title` text,"
            "`payload` blob,"
            "`amount` decimal(20,6),"
            "`created_at` datetime(6),"
            "`event_date` date,"
            "`event_time` time(6),"
            "`duration` time(6),"
            "`optional` text,"
            "`identity_hash` char(64) GENERATED ALWAYS AS (sha2(`title`,256)) STORED,"
            "PRIMARY KEY (`id`)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        ),
        "legacy": (
            "CREATE TABLE `legacy` (`id` int NOT NULL, `enabled` tinyint(1),"
            " PRIMARY KEY (`id`)) ENGINE=MyISAM"
        ),
    }
    data: dict[str, tuple[list[str], list[tuple[object, ...]]]] = {
        "events": (
            [
                "id",
                "title",
                "payload",
                "amount",
                "created_at",
                "event_date",
                "event_time",
                "duration",
                "optional",
            ],
            [
                (
                    1,
                    "O'Reilly\\line\nnext\x00",
                    b"\x00\xff",
                    Decimal("123.450000"),
                    datetime(2026, 7, 25, 12, 30, 1, 123456),
                    date(2026, 7, 25),
                    time(12, 30, 1, 123456),
                    timedelta(hours=25, microseconds=7),
                    None,
                ),
                (
                    2,
                    "second",
                    memoryview(b"\x10"),
                    Decimal("-0.000001"),
                    datetime(
                        2026,
                        7,
                        25,
                        12,
                        tzinfo=timezone(timedelta(hours=9)),
                    ),
                    date(2026, 7, 26),
                    time(1, 2, 3),
                    timedelta(seconds=-1),
                    "ok",
                ),
            ],
        ),
        "legacy": (["id", "enabled"], [(10, True)]),
    }
    dump = FakeConnection(
        "dump",
        tables=tables,
        creates=creates,
        data=data,
        log=log,
        fail_table=fail_table,
        column_metadata={
            "events": [
                *(
                    (
                        column,
                        "DEFAULT_GENERATED" if column == "created_at" else "",
                        "",
                    )
                    for column in data["events"][0]
                ),
                (
                    "identity_hash",
                    "STORED GENERATED",
                    "sha2(`title`,256)",
                ),
            ],
            "legacy": [(column, "", "") for column in data["legacy"][0]],
        },
    )
    lock = FakeConnection(
        "lock",
        tables=tables,
        creates=creates,
        data=data,
        log=log,
        column_metadata={
            "events": [
                *(
                    (
                        column,
                        "DEFAULT_GENERATED" if column == "created_at" else "",
                        "",
                    )
                    for column in data["events"][0]
                ),
                (
                    "identity_hash",
                    "STORED GENERATED",
                    "sha2(`title`,256)",
                ),
            ],
            "legacy": [(column, "", "") for column in data["legacy"][0]],
        },
    )
    verify = FakeConnection(
        "verify",
        tables=tables,
        creates=creates,
        data=data,
        log=log,
        column_metadata={
            "events": [
                *(
                    (
                        column,
                        "DEFAULT_GENERATED" if column == "created_at" else "",
                        "",
                    )
                    for column in data["events"][0]
                ),
                (
                    "identity_hash",
                    "STORED GENERATED",
                    "sha2(`title`,256)",
                ),
            ],
            "legacy": [(column, "", "") for column in data["legacy"][0]],
        },
    )
    connections = [dump, lock, verify]
    autocommit_call = 0

    def connect(_options: ConnectionOptions, autocommit: bool) -> FakeConnection:
        nonlocal autocommit_call
        if autocommit:
            autocommit_call += 1
            expected = connections[autocommit_call]
        else:
            expected = connections[0]
        expected.opened = True
        return expected

    return connections, log, connect


def all_opened_connections_closed(
    connections: list[FakeConnection],
) -> bool:
    return all(not connection.opened or connection.closed for connection in connections)


def options() -> ConnectionOptions:
    return ConnectionOptions(
        host="private.example",
        port=3306,
        user="backup-user",
        password="do-not-print",
        database="production",
    )


def test_identifier_and_value_serialization_is_restore_safe() -> None:
    assert quote_identifier("odd`table") == "`odd``table`"
    assert sql_literal(None) == "NULL"
    assert sql_literal(b"\x00\xff") == "X'00ff'"
    assert sql_literal(Decimal("1E+3")) == "1000"
    assert sql_literal(date(2026, 7, 25)) == "'2026-07-25'"
    assert sql_literal(time(1, 2, 3, 4)) == "'01:02:03.000004'"
    assert sql_literal(timedelta(hours=25, microseconds=7)) == "'25:00:00.000007'"
    assert sql_literal("a'b\\c\t\n") == "'a\\'b\\\\c\\t\\n'"
    with pytest.raises(MySqlBackupError, match="non-finite"):
        sql_literal(float("inf"))
    with pytest.raises(MySqlBackupError, match="timezone-aware time"):
        sql_literal(time(1, tzinfo=timezone.utc))
    with pytest.raises(MySqlBackupError, match="unsupported"):
        sql_literal(object())


def test_backup_streams_restoreable_sql_and_writes_completed_manifest(
    tmp_path: Path,
) -> None:
    connections, log, connect = fixture_connections()
    output = tmp_path / "snapshot.sql.gz"
    result = create_mysql_backup(
        options(),
        output_path=output,
        code_revision=REVISION,
        connect=connect,
        stream_cursor_class=object(),
        fetch_size=1,
        batch_rows=1,
        batch_bytes=100,
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    serialized_manifest = result.manifest_path.read_text(encoding="utf-8")
    assert all(
        private_value not in serialized_manifest
        for private_value in (
            options().host,
            options().user,
            options().password,
            options().database,
        )
    )
    assert manifest["status"] == "completed"
    assert manifest["code_revision"] == REVISION
    assert manifest["table_count"] == 2
    assert manifest["row_count"] == 3
    assert manifest["consistency"] == {
        "consistent_snapshot": True,
        "global_read_lock_used": True,
        "isolation": "REPEATABLE READ",
        "locked_engines": ["InnoDB", "MyISAM"],
        "locked_table_count": 2,
        "nontransactional_engines": ["MyISAM"],
        "nontransactional_table_count": 1,
        "read_lock_strategy": "global_read_lock",
    }
    assert (
        hashlib.sha256(output.read_bytes()).hexdigest() == manifest["backup"]["sha256"]
    )
    assert manifest["backup"]["bytes"] == output.stat().st_size
    assert (
        hashlib.sha256(gzip.decompress(output.read_bytes())).hexdigest()
        == manifest["backup"]["uncompressed_sha256"]
    )
    assert {
        (table["name"], table["engine"], table["row_count"])
        for table in manifest["tables"]
    } == {("events", "InnoDB", 2), ("legacy", "MyISAM", 1)}
    assert all(len(table["sql_sha256"]) == 64 for table in manifest["tables"])
    assert all(table["sql_bytes"] > 0 for table in manifest["tables"])
    events_manifest = next(
        table for table in manifest["tables"] if table["name"] == "events"
    )
    assert events_manifest["generated_column_count"] == 1
    assert events_manifest["insert_column_count"] == 9

    sql = gzip.decompress(output.read_bytes()).decode("utf-8")
    assert "SHOW CREATE" not in sql
    assert "CREATE TABLE `events`" in sql
    assert "DROP TABLE IF EXISTS `legacy`;" in sql
    assert "X'00ff'" in sql
    assert "123.450000" in sql
    assert "'O\\'Reilly\\\\line\\nnext\\0'" in sql
    assert "'2026-07-25 03:00:00'" in sql
    assert "NULL" in sql
    assert "SET @BSIDE_OLD_TIME_ZONE=@@SESSION.TIME_ZONE;" in sql
    assert "SET SESSION TIME_ZONE='+00:00';" in sql
    assert (
        "SET SESSION TIME_ZONE=@BSIDE_OLD_TIME_ZONE;\n"
        "-- BSIDE_BACKUP_COMPLETE\n"
    ) in sql
    assert sql.count("INSERT INTO `events`") == 2
    insert_lines = [
        line for line in sql.splitlines() if line.startswith("INSERT INTO `events`")
    ]
    assert all("identity_hash" not in line for line in insert_lines)
    assert sql.endswith("-- BSIDE_BACKUP_COMPLETE\n")

    statements = [entry[1] for entry in log]
    lock_index = statements.index("FLUSH TABLES WITH READ LOCK")
    snapshot_index = statements.index("START TRANSACTION WITH CONSISTENT SNAPSHOT")
    legacy_index = next(
        index
        for index, statement in enumerate(statements)
        if statement.endswith(" FROM `legacy`")
    )
    unlock_index = statements.index("UNLOCK TABLES")
    events_index = next(
        index
        for index, statement in enumerate(statements)
        if statement.endswith(" FROM `events`")
    )
    assert lock_index < snapshot_index < legacy_index < unlock_index < events_index
    assert connections[0].rolled_back is True
    assert all_opened_connections_closed(connections)
    assert "do-not-print" not in repr(options())


def test_privilege_denied_global_lock_falls_back_to_sorted_quoted_table_locks(
    tmp_path: Path,
) -> None:
    connections, log, connect = fixture_connections()
    dump, lock, _verify = connections
    dump.creates["z`legacy"] = dump.creates.pop("legacy")
    dump.data["z`legacy"] = dump.data.pop("legacy")
    dump.creates["alpha"] = (
        "CREATE TABLE `alpha` (`id` int NOT NULL) ENGINE=MEMORY"
    )
    dump.data["alpha"] = (["id"], [(20,)])
    for connection in connections:
        connection.tables = [
            ("z`legacy", "MyISAM"),
            ("events", "InnoDB"),
            ("alpha", "MEMORY"),
        ]
        connection.column_metadata["z`legacy"] = connection.column_metadata.pop(
            "legacy"
        )
        connection.column_metadata["alpha"] = [("id", "", "")]
    lock.statement_errors["FLUSH TABLES WITH READ LOCK"] = (
        FakeMySqlOperationalError(
            1227,
            "access denied; requires FLUSH_TABLES or RELOAD",
        )
    )
    configured = ConnectionOptions(
        host="private.example",
        port=3306,
        user="backup-user",
        password="do-not-print",
        database="prod`uction",
    )

    result = create_mysql_backup(
        configured,
        output_path=tmp_path / "fallback.sql.gz",
        code_revision=REVISION,
        connect=connect,
        stream_cursor_class=object(),
    )

    statements = [entry[1] for entry in log]
    explicit_lock = (
        "LOCK TABLES "
        "`prod``uction`.`alpha` READ, "
        "`prod``uction`.`events` READ, "
        "`prod``uction`.`z``legacy` READ"
    )
    assert explicit_lock in statements
    snapshot_index = statements.index("START TRANSACTION WITH CONSISTENT SNAPSHOT")
    alpha_index = next(
        index
        for index, statement in enumerate(statements)
        if statement.endswith(" FROM `alpha`")
    )
    legacy_index = next(
        index
        for index, statement in enumerate(statements)
        if statement.endswith(" FROM `z``legacy`")
    )
    unlock_index = statements.index("UNLOCK TABLES")
    events_index = next(
        index
        for index, statement in enumerate(statements)
        if statement.endswith(" FROM `events`")
    )
    inventory_indexes = [
        index
        for index, statement in enumerate(statements)
        if statement.startswith(
            "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES"
        )
    ]
    assert (
        statements.index(explicit_lock)
        < snapshot_index
        < alpha_index
        < legacy_index
        < inventory_indexes[-1]
        < unlock_index
        < events_index
    )
    assert result.manifest["consistency"] == {
        "consistent_snapshot": True,
        "global_read_lock_used": False,
        "isolation": "REPEATABLE READ",
        "locked_engines": ["InnoDB", "MEMORY", "MyISAM"],
        "locked_table_count": 3,
        "nontransactional_engines": ["MEMORY", "MyISAM"],
        "nontransactional_table_count": 2,
        "read_lock_strategy": "explicit_table_read_locks",
    }
    assert all_opened_connections_closed(connections)


@pytest.mark.parametrize(
    "arguments",
    [
        (1044, "access denied; unrelated privilege error 1227"),
        ("1227", "lookalike non-numeric error code"),
    ],
)
def test_global_lock_fallback_rejects_every_error_except_exact_numeric_1227(
    tmp_path: Path,
    arguments: tuple[object, str],
) -> None:
    connections, log, connect = fixture_connections()
    error = FakeMySqlOperationalError(*arguments)
    connections[1].statement_errors["FLUSH TABLES WITH READ LOCK"] = error

    with pytest.raises(FakeMySqlOperationalError) as raised:
        create_mysql_backup(
            options(),
            output_path=tmp_path / f"unsupported-{arguments[0]}.sql.gz",
            code_revision=REVISION,
            connect=connect,
        )

    assert raised.value is error
    assert not any(
        statement.startswith("LOCK TABLES ")
        for _connection, statement, _params in log
    )
    assert all_opened_connections_closed(connections)


def test_explicit_table_lock_failure_aborts_without_completed_backup(
    tmp_path: Path,
) -> None:
    connections, log, connect = fixture_connections()
    connections[1].statement_errors.update(
        {
            "FLUSH TABLES WITH READ LOCK": FakeMySqlOperationalError(
                1227,
                "global lock privilege denied",
            ),
            "LOCK TABLES ": FakeMySqlOperationalError(
                1142,
                "LOCK TABLES denied",
            ),
        }
    )
    output = tmp_path / "lock-denied.sql.gz"

    with pytest.raises(MySqlBackupError, match="could not be acquired safely"):
        create_mysql_backup(
            options(),
            output_path=output,
            code_revision=REVISION,
            connect=connect,
        )

    assert not output.exists()
    assert not (tmp_path / "lock-denied.manifest.json").exists()
    assert "UNLOCK TABLES" not in [entry[1] for entry in log]
    assert all_opened_connections_closed(connections)


def test_fallback_read_locks_release_when_nontransactional_dump_fails(
    tmp_path: Path,
) -> None:
    connections, log, connect = fixture_connections(fail_table="legacy")
    connections[1].statement_errors["FLUSH TABLES WITH READ LOCK"] = (
        FakeMySqlOperationalError(1227, "global lock privilege denied")
    )

    with pytest.raises(RuntimeError, match="synthetic"):
        create_mysql_backup(
            options(),
            output_path=tmp_path / "fallback-failed.sql.gz",
            code_revision=REVISION,
            connect=connect,
            stream_cursor_class=object(),
        )

    statements = [entry[1] for entry in log]
    assert any(statement.startswith("LOCK TABLES ") for statement in statements)
    assert "UNLOCK TABLES" in statements
    assert not (tmp_path / "fallback-failed.sql.gz").exists()
    assert not (tmp_path / "fallback-failed.manifest.json").exists()
    assert all_opened_connections_closed(connections)


@pytest.mark.parametrize(
    "drifted_inventory",
    [
        [
            ("events", "InnoDB"),
            ("legacy", "MyISAM"),
            ("new_table", "MyISAM"),
        ],
        [("events", "InnoDB"), ("legacy", "MEMORY")],
    ],
)
def test_fallback_detects_table_or_engine_drift_before_unlock_and_commit(
    tmp_path: Path,
    drifted_inventory: list[tuple[str, str]],
) -> None:
    connections, log, connect = fixture_connections()
    initial_inventory = list(connections[0].tables)
    connections[0].table_snapshots = [
        initial_inventory,
        initial_inventory,
    ]
    connections[2].table_snapshots = [drifted_inventory]
    connections[1].statement_errors["FLUSH TABLES WITH READ LOCK"] = (
        FakeMySqlOperationalError(1227, "global lock privilege denied")
    )
    output = tmp_path / "schema-drift.sql.gz"

    with pytest.raises(MySqlBackupError, match="metadata changed"):
        create_mysql_backup(
            options(),
            output_path=output,
            code_revision=REVISION,
            connect=connect,
            stream_cursor_class=object(),
        )

    statements = [entry[1] for entry in log]
    legacy_index = next(
        index
        for index, statement in enumerate(statements)
        if statement.endswith(" FROM `legacy`")
    )
    unlock_index = statements.index("UNLOCK TABLES")
    assert legacy_index < unlock_index
    assert not any(statement.endswith(" FROM `events`") for statement in statements)
    assert not output.exists()
    assert not (tmp_path / "schema-drift.manifest.json").exists()
    assert not list(tmp_path.glob("*.partial"))
    assert all_opened_connections_closed(connections)


@pytest.mark.parametrize("failure_stage", ["connect", "query", "close"])
def test_final_inventory_verifier_failure_unlocks_and_removes_partial_output(
    tmp_path: Path,
    failure_stage: str,
) -> None:
    connections, log, base_connect = fixture_connections()
    connections[1].statement_errors["FLUSH TABLES WITH READ LOCK"] = (
        FakeMySqlOperationalError(1227, "global lock privilege denied")
    )
    if failure_stage == "query":
        connections[2].statement_errors[
            "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES"
        ] = RuntimeError("synthetic verifier query failure")
    if failure_stage == "close":
        connections[2].close_errors = [
            RuntimeError("synthetic verifier close failure")
        ]
    autocommit_calls = 0

    def connect(
        configured: ConnectionOptions,
        autocommit: bool,
    ) -> FakeConnection:
        nonlocal autocommit_calls
        if autocommit:
            autocommit_calls += 1
            if failure_stage == "connect" and autocommit_calls == 2:
                raise RuntimeError("synthetic verifier connect failure")
        return base_connect(configured, autocommit)

    output = tmp_path / f"verifier-{failure_stage}.sql.gz"
    expected_error: type[Exception] = (
        MySqlBackupError if failure_stage == "close" else RuntimeError
    )
    with pytest.raises(expected_error):
        create_mysql_backup(
            options(),
            output_path=output,
            code_revision=REVISION,
            connect=connect,
            stream_cursor_class=object(),
        )

    assert "UNLOCK TABLES" in [entry[1] for entry in log]
    assert not output.exists()
    assert not (
        tmp_path / f"verifier-{failure_stage}.manifest.json"
    ).exists()
    assert not list(tmp_path.glob("*.partial"))
    assert all_opened_connections_closed(connections)
    if failure_stage == "close":
        assert connections[2].close_calls == 2


def test_failure_unlocks_myisam_and_never_marks_backup_completed(
    tmp_path: Path,
) -> None:
    connections, log, connect = fixture_connections(fail_table="legacy")
    output = tmp_path / "failed.sql.gz"

    with pytest.raises(RuntimeError, match="synthetic"):
        create_mysql_backup(
            options(),
            output_path=output,
            code_revision=REVISION,
            connect=connect,
            stream_cursor_class=object(),
        )

    assert not output.exists()
    assert not (tmp_path / "failed.manifest.json").exists()
    assert not list(tmp_path.glob("*.partial"))
    assert "UNLOCK TABLES" in [entry[1] for entry in log]
    assert connections[0].rolled_back is True
    assert all_opened_connections_closed(connections)


def test_streaming_failure_is_not_masked_by_cursor_cleanup(
    tmp_path: Path,
) -> None:
    connections, _log, connect = fixture_connections()
    dump_connection = connections[0]
    primary_error = FakeMySqlOperationalError(
        2013,
        "synthetic lost connection during streaming",
    )
    cleanup_error = AttributeError(
        "synthetic SSCursor cleanup touched a missing socket"
    )

    class FailingStreamingCursor(FakeCursor):
        def fetchmany(self, _size: int) -> list[object]:
            raise primary_error

        def close(self) -> None:
            raise cleanup_error

    def cursor(
        *_args: object,
        **_kwargs: object,
    ) -> FailingStreamingCursor:
        return FailingStreamingCursor(dump_connection)

    dump_connection.cursor = cursor  # type: ignore[method-assign]
    output = tmp_path / "stream-failed.sql.gz"

    with pytest.raises(FakeMySqlOperationalError) as captured:
        create_mysql_backup(
            options(),
            output_path=output,
            code_revision=REVISION,
            connect=connect,
            stream_cursor_class=object(),
        )

    assert captured.value is primary_error
    assert not output.exists()
    assert not (tmp_path / "stream-failed.manifest.json").exists()
    assert not list(tmp_path.glob("*.partial"))
    assert all_opened_connections_closed(connections)


def test_innodb_only_backup_does_not_request_a_global_read_lock(
    tmp_path: Path,
) -> None:
    log: list[tuple[str, str, object]] = []
    connection = FakeConnection(
        "dump",
        tables=[("only_table", "InnoDB")],
        creates={
            "only_table": (
                "CREATE TABLE `only_table` (`id` int NOT NULL) ENGINE=InnoDB"
            )
        },
        data={"only_table": (["id"], [(1,)])},
        log=log,
    )
    calls: list[bool] = []

    def connect(_options: ConnectionOptions, autocommit: bool) -> FakeConnection:
        calls.append(autocommit)
        return connection

    result = create_mysql_backup(
        options(),
        output_path=tmp_path / "innodb.sql.gz",
        code_revision=REVISION,
        connect=connect,
    )

    assert calls == [False]
    assert result.manifest["consistency"] == {
        "consistent_snapshot": True,
        "global_read_lock_used": False,
        "isolation": "REPEATABLE READ",
        "locked_engines": [],
        "locked_table_count": 0,
        "nontransactional_engines": [],
        "nontransactional_table_count": 0,
        "read_lock_strategy": "none",
    }
    assert "FLUSH TABLES WITH READ LOCK" not in [entry[1] for entry in log]


def test_unsupported_views_triggers_routines_or_events_fail_closed(
    tmp_path: Path,
) -> None:
    log: list[tuple[str, str, object]] = []
    connection = FakeConnection(
        "dump",
        tables=[],
        creates={},
        data={},
        log=log,
        object_counts=(1, 0, 0, 0),
    )

    with pytest.raises(MySqlBackupError, match="unsupported"):
        create_mysql_backup(
            options(),
            output_path=tmp_path / "unsupported.sql.gz",
            code_revision=REVISION,
            connect=lambda _options, _autocommit: connection,
        )

    assert not (tmp_path / "unsupported.sql.gz").exists()
    assert not (tmp_path / "unsupported.manifest.json").exists()
    assert connection.closed is True


def test_connection_flags_override_environment_without_exposing_password() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(
        [
            "--output",
            "backup.sql.gz",
            "--host",
            "cli-host",
            "--user",
            "cli-user",
            "--database",
            "cli-database",
        ]
    )
    configured = connection_options_from_args(
        args,
        environ={
            "DB_HOST": "env-host",
            "DB_PORT": "3307",
            "DB_USER": "env-user",
            "DB_PASSWORD": "environment-secret",
            "DB_NAME": "env-database",
        },
    )
    assert configured.host == "cli-host"
    assert configured.port == 3307
    assert configured.user == "cli-user"
    assert configured.database == "cli-database"
    assert configured.password == "environment-secret"
    assert "environment-secret" not in repr(configured)


def test_ssh_options_require_and_normalize_a_pinned_host_key() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--output", "backup.sql.gz", "--ssh-tunnel"])
    key_bytes = b"test-public-key-bytes"
    fingerprint = ssh_host_key_sha256(key_bytes)
    configured = ssh_options_from_args(
        args,
        environ={
            "SSH_HOST": "ssh.example",
            "SSH_USER": "ssh-user",
            "SSH_PASSWORD": "ssh-secret",
            "SSH_HOST_KEY_SHA256": fingerprint.removeprefix("SHA256:"),
        },
    )
    assert configured is not None
    assert configured.host_key_sha256 == fingerprint
    assert configured.port == 22
    assert "ssh-secret" not in repr(configured)
    verify_ssh_host_key(key_bytes, fingerprint)
    with pytest.raises(MySqlBackupError, match="does not match"):
        verify_ssh_host_key(b"different-key", fingerprint)


def test_legacy_ssh_rsa_sha1_requires_opt_in_target_and_exact_pin() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(
        [
            "--output",
            "backup.sql.gz",
            "--ssh-tunnel",
            "--ssh-allow-legacy-rsa-sha1",
        ]
    )
    key_bytes = b"gabia-public-host-key"
    fingerprint = ssh_host_key_sha256(key_bytes)
    base_environment = {
        "SSH_HOST": "Legacy.Example.",
        "SSH_USER": "ssh-user",
        "SSH_PASSWORD": "ssh-secret",
        "SSH_HOST_KEY_SHA256": fingerprint,
    }

    with pytest.raises(MySqlBackupError, match="explicit target host"):
        ssh_options_from_args(args, environ=base_environment)

    configured = ssh_options_from_args(
        args,
        environ={
            **base_environment,
            "SSH_LEGACY_RSA_SHA1_HOST": "legacy.example",
        },
    )
    assert configured is not None
    assert configured.allow_legacy_ssh_rsa_sha1 is True
    assert legacy_ssh_rsa_sha1_is_allowed(configured) is True

    environment_configured = ssh_options_from_args(
        parser.parse_args(["--output", "backup.sql.gz", "--ssh-tunnel"]),
        environ={
            **base_environment,
            "SSH_ALLOW_LEGACY_RSA_SHA1": "true",
            "SSH_LEGACY_RSA_SHA1_HOST": "legacy.example",
        },
    )
    assert environment_configured is not None
    assert environment_configured.allow_legacy_ssh_rsa_sha1 is True

    cli_configured = ssh_options_from_args(
        parser.parse_args(
            [
                "--output",
                "backup.sql.gz",
                "--ssh-tunnel",
                "--ssh-host",
                "legacy.example",
                "--ssh-user",
                "ssh-user",
                "--ssh-password",
                "ssh-secret",
                "--ssh-host-key-sha256",
                fingerprint,
                "--ssh-allow-legacy-rsa-sha1",
                "--ssh-legacy-rsa-sha1-host",
                "legacy.example",
            ]
        ),
        environ={},
    )
    assert cli_configured is not None
    assert cli_configured.allow_legacy_ssh_rsa_sha1 is True

    with pytest.raises(MySqlBackupError, match="must match"):
        ssh_options_from_args(
            args,
            environ={
                **base_environment,
                "SSH_LEGACY_RSA_SHA1_HOST": "different.example",
            },
        )

    without_pin = dict(base_environment)
    without_pin.pop("SSH_HOST_KEY_SHA256")
    without_pin["SSH_LEGACY_RSA_SHA1_HOST"] = "legacy.example"
    with pytest.raises(MySqlBackupError, match="missing connection setting"):
        ssh_options_from_args(args, environ=without_pin)


def test_legacy_ssh_rsa_sha1_target_without_opt_in_fails_closed() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--output", "backup.sql.gz", "--ssh-tunnel"])
    fingerprint = ssh_host_key_sha256(b"host-key")

    with pytest.raises(MySqlBackupError, match="requires explicit opt-in"):
        ssh_options_from_args(
            args,
            environ={
                "SSH_HOST": "ssh.example",
                "SSH_USER": "ssh-user",
                "SSH_PASSWORD": "ssh-secret",
                "SSH_HOST_KEY_SHA256": fingerprint,
                "SSH_LEGACY_RSA_SHA1_HOST": "ssh.example",
            },
        )

    with pytest.raises(MySqlBackupError, match="must be a boolean"):
        ssh_options_from_args(
            args,
            environ={
                "SSH_ALLOW_LEGACY_RSA_SHA1": "sometimes",
            },
        )


def test_paramiko_legacy_host_key_exception_is_per_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import paramiko
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    created: list[Any] = []
    authenticated: list[tuple[str, str, bool]] = []
    key_bytes = b"pinned-host-key"
    fingerprint = ssh_host_key_sha256(key_bytes)
    default_key_info = dict(paramiko.Transport._key_info)
    default_preferred_keys = tuple(paramiko.Transport._preferred_keys)

    class FakeSocket:
        def close(self) -> None:
            return None

    class FakeServerKey:
        def asbytes(self) -> bytes:
            return key_bytes

    class FakeTransport:
        def __init__(
            self,
            _socket: object,
            *,
            disabled_algorithms: object,
        ) -> None:
            self.disabled_algorithms = disabled_algorithms
            self._key_info = dict(default_key_info)
            self._preferred_keys = tuple(default_preferred_keys)
            self.auth_timeout = 0
            self.keepalive_interval: int | None = None
            created.append(self)

        def start_client(self, *, timeout: int) -> None:
            assert timeout == 15

        def get_remote_server_key(self) -> FakeServerKey:
            return FakeServerKey()

        def auth_password(
            self,
            *,
            username: str,
            password: str,
            fallback: bool,
        ) -> None:
            authenticated.append((username, password, fallback))

        def is_authenticated(self) -> bool:
            return True

        def set_keepalive(self, interval: int) -> None:
            self.keepalive_interval = interval

        def close(self) -> None:
            return None

    class FakeServer:
        def __init__(self, *_args: object) -> None:
            self.server_address = ("127.0.0.1", 33060)

        def serve_forever(self, *, poll_interval: float) -> None:
            assert poll_interval == 0.1

        def shutdown(self) -> None:
            return None

        def server_close(self) -> None:
            return None

    monkeypatch.setattr("socket.create_connection", lambda *_args, **_kwargs: FakeSocket())
    monkeypatch.setattr(paramiko, "Transport", FakeTransport)
    monkeypatch.setattr("curator.mysql_backup._DirectTcpipServer", FakeServer)

    default_options = SshTunnelOptions(
        host="modern.example",
        port=22,
        user="ssh-user",
        password="ssh-secret",
        host_key_sha256=fingerprint,
    )
    with SshDirectTcpipTunnel(
        default_options,
        destination_host="private-db",
        destination_port=3306,
    ):
        pass
    assert created[0].disabled_algorithms == {"keys": ["ssh-rsa"]}
    assert "ssh-rsa" not in created[0]._preferred_keys
    assert (
        created[0].keepalive_interval
        == mysql_backup_module.SSH_KEEPALIVE_INTERVAL_SECONDS
    )

    legacy_options = SshTunnelOptions(
        host="legacy.example",
        port=22,
        user="ssh-user",
        password="ssh-secret",
        host_key_sha256=fingerprint,
        allow_legacy_ssh_rsa_sha1=True,
        legacy_ssh_rsa_sha1_host="legacy.example",
    )
    with SshDirectTcpipTunnel(
        legacy_options,
        destination_host="private-db",
        destination_port=3306,
    ):
        pass
    assert created[1].disabled_algorithms is None
    assert created[1]._preferred_keys[-1] == "ssh-rsa"
    assert "ssh-rsa" in created[1].preferred_keys
    assert "ssh-rsa-cert-v01@openssh.com" not in created[1].preferred_keys
    assert "ssh-rsa" in created[1]._key_info
    assert "ssh-rsa" in created[1]._key_info["ssh-rsa"].HASHES
    assert (
        created[1].keepalive_interval
        == mysql_backup_module.SSH_KEEPALIVE_INTERVAL_SECONDS
    )
    signed_data = b"local-host-key-verification-fixture"
    private_key = paramiko.RSAKey.generate(bits=1024)
    signature = private_key.key.sign(
        signed_data,
        padding.PKCS1v15(),
        hashes.SHA1(),
    )
    signature_message = paramiko.Message()
    signature_message.add_string("ssh-rsa")
    signature_message.add_string(signature)
    legacy_public_key = created[1]._key_info["ssh-rsa"](
        data=private_key.asbytes()
    )
    assert legacy_public_key.verify_ssh_sig(
        signed_data,
        paramiko.Message(signature_message.asbytes()),
    )
    assert authenticated == [
        ("ssh-user", "ssh-secret", False),
        ("ssh-user", "ssh-secret", False),
    ]


def test_tunnel_forwarder_uses_backpressure_safe_io_timeout() -> None:
    class FakeEndpoint:
        def __init__(self) -> None:
            self.timeouts: list[float] = []
            self.closed = False
            self.shutdown_called = False

        def settimeout(self, value: float) -> None:
            self.timeouts.append(value)

        def recv(self, _size: int) -> bytes:
            return b""

        def send(self, content: bytes | memoryview) -> int:
            return len(content)

        def shutdown(self, _how: int) -> None:
            self.shutdown_called = True

        def close(self) -> None:
            self.closed = True

    request = FakeEndpoint()
    channel = FakeEndpoint()

    class FakeTransport:
        def open_channel(
            self,
            kind: str,
            destination: tuple[str, int],
            source: tuple[str, int],
        ) -> FakeEndpoint:
            assert kind == "direct-tcpip"
            assert destination == ("private-db", 3306)
            assert source == ("127.0.0.1", 45123)
            return channel

    class FakeServer:
        ssh_transport = FakeTransport()
        remote_destination = ("private-db", 3306)

    mysql_backup_module._DirectTcpipHandler(
        request,
        ("127.0.0.1", 45123),
        FakeServer(),
    )

    expected = mysql_backup_module.SSH_FORWARD_IO_TIMEOUT_SECONDS
    assert request.timeouts == [expected]
    assert channel.timeouts == [expected]
    assert request.shutdown_called is True
    assert channel.closed is True


def test_tunnel_forwarder_retries_only_the_unsent_suffix_after_timeout() -> None:
    payload = b"abcdef"

    class Source:
        def __init__(self) -> None:
            self.chunks = [payload, b""]

        def recv(self, _size: int) -> bytes:
            return self.chunks.pop(0)

    class PartialDestination:
        def __init__(self) -> None:
            self.actions: list[int | Exception] = [
                2,
                socket.timeout("synthetic backpressure"),
                4,
            ]
            self.attempts: list[bytes] = []
            self.received = bytearray()

        def send(self, content: bytes | memoryview) -> int:
            remaining = bytes(content)
            self.attempts.append(remaining)
            action = self.actions.pop(0)
            if isinstance(action, Exception):
                raise action
            sent = min(action, len(remaining))
            self.received.extend(remaining[:sent])
            return sent

    source = Source()
    destination = PartialDestination()
    stop = threading.Event()

    mysql_backup_module._DirectTcpipHandler._pump(
        source,
        destination,
        stop,
    )

    assert destination.attempts == [payload, b"cdef", b"cdef"]
    assert bytes(destination.received) == payload
    assert stop.is_set()


def test_tunnel_forwarder_bounds_consecutive_send_timeouts() -> None:
    class StalledDestination:
        def __init__(self) -> None:
            self.calls = 0

        def send(self, _content: bytes | memoryview) -> int:
            self.calls += 1
            raise socket.timeout("synthetic stalled destination")

    destination = StalledDestination()

    with pytest.raises(TimeoutError, match="no write progress"):
        mysql_backup_module._DirectTcpipHandler._send_all(
            destination,
            b"payload",
            threading.Event(),
        )

    assert (
        destination.calls
        == mysql_backup_module.SSH_FORWARD_MAX_CONSECUTIVE_TIMEOUTS
    )


def test_tunnel_forwarder_resets_send_timeout_budget_after_progress() -> None:
    timeout_budget = (
        mysql_backup_module.SSH_FORWARD_MAX_CONSECUTIVE_TIMEOUTS - 1
    )

    class RecoveringDestination:
        def __init__(self) -> None:
            self.actions: list[int | Exception] = [
                *(
                    socket.timeout("first backpressure")
                    for _index in range(timeout_budget)
                ),
                1,
                *(
                    socket.timeout("second backpressure")
                    for _index in range(timeout_budget)
                ),
                1,
            ]
            self.received = bytearray()

        def send(self, content: bytes | memoryview) -> int:
            action = self.actions.pop(0)
            if isinstance(action, Exception):
                raise action
            remaining = bytes(content)
            self.received.extend(remaining[:action])
            return action

    destination = RecoveringDestination()
    mysql_backup_module._DirectTcpipHandler._send_all(
        destination,
        b"ab",
        threading.Event(),
    )

    assert bytes(destination.received) == b"ab"
    assert destination.actions == []


def test_tunnel_forwarder_allows_one_direction_to_remain_idle() -> None:
    stop = threading.Event()
    idle_period = (
        mysql_backup_module.SSH_FORWARD_MAX_CONSECUTIVE_TIMEOUTS + 3
    )

    class IdleSource:
        def __init__(self) -> None:
            self.actions: list[bytes | Exception] = [
                *(
                    socket.timeout("synthetic idle source")
                    for _index in range(idle_period)
                ),
                b"reply",
                b"",
            ]

        def recv(self, _size: int) -> bytes:
            action = self.actions.pop(0)
            if isinstance(action, Exception):
                raise action
            return action

    class Destination:
        def __init__(self) -> None:
            self.received = bytearray()

        def send(self, content: bytes | memoryview) -> int:
            chunk = bytes(content)
            self.received.extend(chunk)
            return len(chunk)

    source = IdleSource()
    destination = Destination()
    mysql_backup_module._DirectTcpipHandler._pump(
        source,
        destination,
        stop,
    )

    assert source.actions == []
    assert bytes(destination.received) == b"reply"
    assert stop.is_set()


def test_tunnel_forwarder_continues_across_receive_idle_periods() -> None:
    timeout_budget = (
        mysql_backup_module.SSH_FORWARD_MAX_CONSECUTIVE_TIMEOUTS - 1
    )

    class RecoveringSource:
        def __init__(self) -> None:
            self.actions: list[bytes | Exception] = [
                *(
                    socket.timeout("first idle period")
                    for _index in range(timeout_budget)
                ),
                b"a",
                *(
                    socket.timeout("second idle period")
                    for _index in range(timeout_budget)
                ),
                b"",
            ]

        def recv(self, _size: int) -> bytes:
            action = self.actions.pop(0)
            if isinstance(action, Exception):
                raise action
            return action

    class Destination:
        def __init__(self) -> None:
            self.received = bytearray()

        def send(self, content: bytes | memoryview) -> int:
            chunk = bytes(content)
            self.received.extend(chunk)
            return len(chunk)

    source = RecoveringSource()
    destination = Destination()
    stop = threading.Event()
    mysql_backup_module._DirectTcpipHandler._pump(
        source,
        destination,
        stop,
    )

    assert bytes(destination.received) == b"a"
    assert source.actions == []
    assert stop.is_set()


def test_wrong_pin_aborts_before_ssh_authentication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import paramiko

    authenticated = False

    class FakeSocket:
        def close(self) -> None:
            return None

    class FakeServerKey:
        def asbytes(self) -> bytes:
            return b"unexpected-host-key"

    class FakeTransport:
        _key_info = dict(paramiko.Transport._key_info)
        _preferred_keys = tuple(paramiko.Transport._preferred_keys)

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.auth_timeout = 0

        def start_client(self, *, timeout: int) -> None:
            return None

        def get_remote_server_key(self) -> FakeServerKey:
            return FakeServerKey()

        def auth_password(self, **_kwargs: object) -> None:
            nonlocal authenticated
            authenticated = True

        def close(self) -> None:
            return None

    monkeypatch.setattr("socket.create_connection", lambda *_args, **_kwargs: FakeSocket())
    monkeypatch.setattr(paramiko, "Transport", FakeTransport)
    options = SshTunnelOptions(
        host="legacy.example",
        port=22,
        user="ssh-user",
        password="ssh-secret",
        host_key_sha256=ssh_host_key_sha256(b"expected-host-key"),
        allow_legacy_ssh_rsa_sha1=True,
        legacy_ssh_rsa_sha1_host="legacy.example",
    )

    with pytest.raises(MySqlBackupError, match="does not match"):
        with SshDirectTcpipTunnel(
            options,
            destination_host="private-db",
            destination_port=3306,
        ):
            pass
    assert authenticated is False


def test_ssh_tunnel_is_disabled_unless_requested() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--output", "backup.sql.gz"])
    assert ssh_options_from_args(args, environ={}) is None


def test_existing_destination_and_invalid_revision_fail_closed(
    tmp_path: Path,
) -> None:
    output = tmp_path / "existing.sql.gz"
    output.write_bytes(b"existing")
    with pytest.raises(MySqlBackupError, match="already exists"):
        create_mysql_backup(
            options(),
            output_path=output,
            code_revision=REVISION,
            connect=lambda *_args: pytest.fail("must not connect"),
        )
    with pytest.raises(MySqlBackupError, match="code_revision"):
        create_mysql_backup(
            options(),
            output_path=tmp_path / "new.sql.gz",
            code_revision="invalid",
            connect=lambda *_args: pytest.fail("must not connect"),
        )
