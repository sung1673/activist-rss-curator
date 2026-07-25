from __future__ import annotations

import hashlib
from collections.abc import Sequence
from pathlib import Path

import pytest

from curator.mysql_backup import ConnectionOptions
from curator.mysql_migration import (
    DEFAULT_MIGRATION_PATH,
    MIGRATION_LOCK_NAME,
    MIGRATION_NAME,
    MIGRATION_VERSION,
    PREREQUISITE_MIGRATIONS,
    MySqlMigrationError,
    execute_migration_011,
    parse_mysql_script,
    verify_prerequisite_manifest,
)


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.result: list[object] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(
        self,
        query: str,
        args: Sequence[object] | None = None,
    ) -> int:
        self.connection.executions.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT GET_LOCK("):
            self.result = [(1,)]
        elif normalized.startswith("SELECT RELEASE_LOCK("):
            self.result = [(1,)]
            self.connection.lock_released = True
        elif "FROM activist_schema_migrations" in normalized:
            self.result = [
                (version, name, checksum)
                for version, (name, checksum) in sorted(
                    self.connection.manifest.items()
                )
            ]
        elif normalized.startswith("SET @bside_migration_011_sha256"):
            assert args is not None
            self.connection.active_checksum = str(args[0])
            self.result = []
        else:
            if (
                self.connection.fail_on is not None
                and self.connection.fail_on in query
            ):
                raise RuntimeError("credential-adjacent-driver-message")
            if "CALL activist_011_record_migration()" in query:
                assert self.connection.active_checksum is not None
                self.connection.manifest[MIGRATION_VERSION] = (
                    MIGRATION_NAME,
                    self.connection.active_checksum,
                )
            self.result = []
        return 1

    def fetchall(self) -> list[object]:
        return list(self.result)

    def fetchone(self) -> object:
        assert len(self.result) == 1
        return self.result[0]


class FakeConnection:
    def __init__(
        self,
        *,
        manifest: dict[int, tuple[str, str]] | None = None,
        fail_on: str | None = None,
    ) -> None:
        self.manifest = dict(manifest or PREREQUISITE_MIGRATIONS)
        self.fail_on = fail_on
        self.active_checksum: str | None = None
        self.executions: list[tuple[str, Sequence[object] | None]] = []
        self.lock_released = False
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True


def connection_options() -> ConnectionOptions:
    return ConnectionOptions(
        host="private-db",
        port=3306,
        user="migration-user",
        password="never-print-this",
        database="governance",
    )


def test_parser_keeps_real_stored_procedure_bodies_intact() -> None:
    statements = parse_mysql_script(DEFAULT_MIGRATION_PATH.read_bytes())

    assert len(statements) == 62
    assert not any("DELIMITER" in statement for statement in statements)
    preflight = next(
        statement
        for statement in statements
        if "CREATE PROCEDURE activist_011_preflight()" in statement
    )
    assert "SIGNAL SQLSTATE '45000'" in preflight
    assert preflight.rstrip().endswith("END")


def test_parser_ignores_delimiters_inside_strings_identifiers_and_comments() -> None:
    source = b"""\
CREATE TABLE `semi;colon` (`value` VARCHAR(30) DEFAULT 'a;b');
DELIMITER $$
CREATE PROCEDURE sample()
BEGIN
  SELECT '$$;still-string', "double;quote", `semi;colon`;
  -- $$ in a line comment
  SELECT 1 /* $$ in a block comment */;
END$$
DELIMITER ;
# trailing delimiter ; in a comment
SELECT 2;
"""

    statements = parse_mysql_script(source)

    assert len(statements) == 3
    assert statements[0].startswith("CREATE TABLE")
    assert statements[1].startswith("CREATE PROCEDURE")
    assert statements[1].rstrip().endswith("END")
    assert statements[2].endswith("SELECT 2")


@pytest.mark.parametrize(
    "source",
    [
        b"SELECT 'unterminated;",
        b"SELECT 1",
        b"DELIMITER \nSELECT 1;",
        b"DELIMITER $$\nCREATE PROCEDURE p() BEGIN SELECT 1; END",
        b";",
        b"/* unterminated",
    ],
)
def test_parser_rejects_ambiguous_or_incomplete_sql(source: bytes) -> None:
    with pytest.raises(MySqlMigrationError):
        parse_mysql_script(source)


def test_prerequisite_verification_rejects_missing_tampered_and_future_rows() -> None:
    checksum = "a" * 64
    missing = dict(PREREQUISITE_MIGRATIONS)
    missing.pop(7)
    with pytest.raises(MySqlMigrationError, match="007"):
        verify_prerequisite_manifest(
            missing,
            migration_checksum=checksum,
            allow_version_11=True,
        )

    tampered = dict(PREREQUISITE_MIGRATIONS)
    tampered[10] = (tampered[10][0], "0" * 64)
    with pytest.raises(MySqlMigrationError, match="010"):
        verify_prerequisite_manifest(
            tampered,
            migration_checksum=checksum,
            allow_version_11=True,
        )

    future = dict(PREREQUISITE_MIGRATIONS)
    future[12] = ("012_future", "1" * 64)
    with pytest.raises(MySqlMigrationError, match="unsupported"):
        verify_prerequisite_manifest(
            future,
            migration_checksum=checksum,
            allow_version_11=True,
        )


def test_executor_applies_and_replays_exact_bytes_in_one_session() -> None:
    fake = FakeConnection()
    calls: list[tuple[ConnectionOptions, bool]] = []

    def connect(options: ConnectionOptions, autocommit: bool) -> FakeConnection:
        calls.append((options, autocommit))
        return fake

    result = execute_migration_011(
        connection_options(),
        connect_factory=connect,
    )

    expected_checksum = hashlib.sha256(DEFAULT_MIGRATION_PATH.read_bytes()).hexdigest()
    assert result.version == 11
    assert result.name == MIGRATION_NAME
    assert result.checksum == expected_checksum
    assert result.applied_statement_count == 62
    assert result.replayed_statement_count == 62
    assert calls == [(connection_options(), True)]
    assert fake.manifest[11] == (MIGRATION_NAME, expected_checksum)
    checksum_sets = [
        args
        for query, args in fake.executions
        if query.startswith("SET @bside_migration_011_sha256")
    ]
    assert checksum_sets == [(expected_checksum,), (expected_checksum,)]
    lock_calls = [
        (query, args)
        for query, args in fake.executions
        if "GET_LOCK" in query or "RELEASE_LOCK" in query
    ]
    assert lock_calls == [
        ("SELECT GET_LOCK(%s, %s)", (MIGRATION_LOCK_NAME, 30)),
        ("SELECT RELEASE_LOCK(%s)", (MIGRATION_LOCK_NAME,)),
    ]
    assert fake.lock_released is True
    assert fake.closed is True


def test_executor_fails_before_sql_when_prerequisite_manifest_is_wrong() -> None:
    manifest = dict(PREREQUISITE_MIGRATIONS)
    manifest[6] = ("006_wrong", manifest[6][1])
    fake = FakeConnection(manifest=manifest)

    with pytest.raises(MySqlMigrationError, match="006"):
        execute_migration_011(
            connection_options(),
            connect_factory=lambda _options, _autocommit: fake,
        )

    executed_sql = [query for query, _args in fake.executions]
    assert not any(
        "DROP PROCEDURE IF EXISTS activist_011_preflight" in query
        for query in executed_sql
    )
    assert fake.lock_released is True
    assert fake.closed is True


def test_executor_hides_driver_details_and_releases_connection_on_failure(
    tmp_path: Path,
) -> None:
    migration = tmp_path / "011.sql"
    migration.write_text(
        "SELECT 'safe';\nCALL activist_011_record_migration();\n",
        encoding="utf-8",
        newline="\n",
    )
    fake = FakeConnection(fail_on="SELECT 'safe'")

    with pytest.raises(
        MySqlMigrationError,
        match=r"apply failed at statement 1$",
    ) as captured:
        execute_migration_011(
            connection_options(),
            migration_path=migration,
            connect_factory=lambda _options, _autocommit: fake,
        )

    assert "credential-adjacent" not in str(captured.value)
    assert fake.lock_released is True
    assert fake.closed is True
