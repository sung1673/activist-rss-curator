from __future__ import annotations

import hashlib
from collections.abc import Sequence
from pathlib import Path

import pytest

from curator.mysql_backup import ConnectionOptions
from curator.mysql_migration import PREREQUISITE_MIGRATIONS, MySqlMigrationError
from curator.mysql_migration_012 import (
    DEFAULT_MIGRATION_011_PATH,
    DEFAULT_MIGRATION_PATH,
    MIGRATION_LOCK_NAME,
    MIGRATION_NAME,
    MIGRATION_VERSION,
    execute_migration_012,
    parse_mysql_script,
    verify_prerequisite_manifest,
)


MIGRATION_011_CHECKSUM = hashlib.sha256(
    DEFAULT_MIGRATION_011_PATH.read_bytes()
).hexdigest()


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
        elif normalized.startswith("SET @bside_migration_012_sha256"):
            assert args is not None
            self.connection.active_checksum = str(args[0])
            self.result = []
        else:
            if (
                self.connection.fail_on is not None
                and self.connection.fail_on in query
            ):
                raise RuntimeError("credential-adjacent-driver-message")
            if "CALL activist_012_validate_and_record()" in query:
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
        base = dict(PREREQUISITE_MIGRATIONS)
        base[11] = ("011_global_terminal_v2", MIGRATION_011_CHECKSUM)
        self.manifest = dict(manifest or base)
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


def test_parser_preserves_migration_012_procedures() -> None:
    statements = parse_mysql_script(DEFAULT_MIGRATION_PATH.read_bytes())

    assert statements
    assert not any("DELIMITER" in statement for statement in statements)
    assert any(
        "CREATE PROCEDURE activist_012_preflight()" in statement
        for statement in statements
    )
    assert any(
        "CALL activist_012_validate_and_record()" in statement
        for statement in statements
    )


def test_migration_requires_exact_per_credential_attempt_aggregate() -> None:
    migration = DEFAULT_MIGRATION_PATH.read_text(encoding="utf-8")

    assert migration.count(
        "SELECT quota_day,credential_id,SUM(consumed_units) AS attempt_units"
    ) >= 2
    assert migration.count("GROUP BY quota_day,credential_id") >= 2
    assert "WHERE a.credential_id IS NULL" in migration
    assert "OR cd.used_count<>a.attempt_units" in migration
    assert "WHERE cd.credential_id IS NULL" in migration
    assert "cd.used_count < a.consumed_units" not in migration


def test_prerequisite_verification_binds_exact_migration_011_bytes() -> None:
    checksum_012 = "a" * 64
    manifest = dict(PREREQUISITE_MIGRATIONS)
    manifest[11] = ("011_global_terminal_v2", MIGRATION_011_CHECKSUM)

    verify_prerequisite_manifest(
        manifest,
        migration_011_checksum=MIGRATION_011_CHECKSUM,
        migration_012_checksum=checksum_012,
        allow_version_12=True,
    )

    manifest[11] = ("011_global_terminal_v2", "0" * 64)
    with pytest.raises(MySqlMigrationError, match="011"):
        verify_prerequisite_manifest(
            manifest,
            migration_011_checksum=MIGRATION_011_CHECKSUM,
            migration_012_checksum=checksum_012,
            allow_version_12=True,
        )


def test_executor_applies_and_replays_exact_bytes_in_one_session() -> None:
    fake = FakeConnection()
    calls: list[tuple[ConnectionOptions, bool]] = []

    def connect(options: ConnectionOptions, autocommit: bool) -> FakeConnection:
        calls.append((options, autocommit))
        return fake

    result = execute_migration_012(
        connection_options(),
        connect_factory=connect,
    )

    expected_checksum = hashlib.sha256(DEFAULT_MIGRATION_PATH.read_bytes()).hexdigest()
    assert result.version == 12
    assert result.name == MIGRATION_NAME
    assert result.checksum == expected_checksum
    assert result.applied_statement_count == result.replayed_statement_count
    assert calls == [(connection_options(), True)]
    assert fake.manifest[12] == (MIGRATION_NAME, expected_checksum)
    checksum_sets = [
        args
        for query, args in fake.executions
        if query.startswith("SET @bside_migration_012_sha256")
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


def test_executor_hides_driver_details_and_closes_after_failure(
    tmp_path: Path,
) -> None:
    migration = tmp_path / "012.sql"
    migration.write_text(
        "SELECT 'safe';\nCALL activist_012_validate_and_record();\n",
        encoding="utf-8",
        newline="\n",
    )
    fake = FakeConnection(fail_on="SELECT 'safe'")

    with pytest.raises(
        MySqlMigrationError,
        match=r"apply failed at statement 1$",
    ) as captured:
        execute_migration_012(
            connection_options(),
            migration_path=migration,
            connect_factory=lambda _options, _autocommit: fake,
        )

    assert "credential-adjacent" not in str(captured.value)
    assert fake.lock_released is True
    assert fake.closed is True
