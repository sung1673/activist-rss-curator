from __future__ import annotations

import gzip
import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from curator.mysql_backup import (
    ConnectionOptions,
    MySqlBackupError,
    build_arg_parser,
    connection_options_from_args,
    create_mysql_backup,
    quote_identifier,
    ssh_host_key_sha256,
    ssh_options_from_args,
    sql_literal,
    verify_ssh_host_key,
)


REVISION = "a" * 40


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
        if normalized.startswith(
            "SELECT (SELECT COUNT(*) FROM information_schema.VIEWS"
        ):
            self.rows = [self.connection.object_counts]
            return
        if normalized.startswith("SELECT TABLE_NAME, ENGINE FROM information_schema"):
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
        self.rolled_back = False
        self.closed = False

    def cursor(self, *_args: object, **_kwargs: object) -> FakeCursor:
        return FakeCursor(self)

    def table_from_statement(self, statement: str) -> str:
        quoted = statement.rsplit("`", 2)
        return quoted[1].replace("``", "`")

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
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
    connections = [dump, lock]

    def connect(_options: ConnectionOptions, autocommit: bool) -> FakeConnection:
        expected = connections[1] if autocommit else connections[0]
        return expected

    return connections, log, connect


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
        "locked_engines": ["MyISAM"],
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
    assert all(connection.closed for connection in connections)
    assert "do-not-print" not in repr(options())


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
    assert all(connection.closed for connection in connections)


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
    assert result.manifest["consistency"]["global_read_lock_used"] is False
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
