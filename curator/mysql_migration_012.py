from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import replace as dataclass_replace
from pathlib import Path
from typing import Mapping, Sequence

from curator.mysql_backup import (
    ConnectionOptions,
    SshDirectTcpipTunnel,
    connection_options_from_args,
    ssh_options_from_args,
)
from curator.mysql_migration import (
    PREREQUISITE_MIGRATIONS,
    ConnectFactory,
    MigrationResult,
    MySqlMigrationError,
    _Connection,
    _pymysql_connect,
    _scalar,
    parse_mysql_script,
    read_migration_manifest,
)


MIGRATION_VERSION = 12
MIGRATION_NAME = "012_dart_credential_pool"
MIGRATION_LOCK_NAME = "bside:migration:012_dart_credential_pool"
DEPLOYMENT_ROOT = Path(__file__).resolve().parents[1] / "deploy" / "activist"
DEFAULT_MIGRATION_PATH = (
    DEPLOYMENT_ROOT / "migrations" / "012_dart_credential_pool.sql"
)
DEFAULT_MIGRATION_011_PATH = (
    DEPLOYMENT_ROOT / "migrations" / "011_global_terminal_v2.sql"
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _bytes_sha256(source_bytes: bytes) -> str:
    checksum = hashlib.sha256(source_bytes).hexdigest()
    if SHA256_PATTERN.fullmatch(checksum) is None:
        raise MySqlMigrationError("migration source SHA-256 is invalid")
    return checksum


def verify_prerequisite_manifest(
    manifest: Mapping[int, tuple[str, str]],
    *,
    migration_011_checksum: str,
    migration_012_checksum: str,
    allow_version_12: bool,
) -> None:
    for version, expected in PREREQUISITE_MIGRATIONS.items():
        if manifest.get(version) != expected:
            raise MySqlMigrationError(
                f"prerequisite migration {version:03d} identity mismatch"
            )
    if manifest.get(11) != (
        "011_global_terminal_v2",
        migration_011_checksum,
    ):
        raise MySqlMigrationError("prerequisite migration 011 identity mismatch")
    unexpected = set(manifest).difference(
        set(PREREQUISITE_MIGRATIONS) | {11, 12}
    )
    if unexpected:
        raise MySqlMigrationError("migration manifest contains unsupported versions")
    version_12 = manifest.get(MIGRATION_VERSION)
    expected_version_12 = (MIGRATION_NAME, migration_012_checksum)
    if version_12 is not None and (
        not allow_version_12 or version_12 != expected_version_12
    ):
        raise MySqlMigrationError("migration 012 identity mismatch")


def _acquire_lock(connection: _Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT GET_LOCK(%s, %s)", (MIGRATION_LOCK_NAME, 30))
        acquired = _scalar(cursor)
    if acquired != 1:
        raise MySqlMigrationError(
            "could not acquire the migration 012 advisory lock"
        )


def _release_lock(connection: _Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT RELEASE_LOCK(%s)", (MIGRATION_LOCK_NAME,))
        released = _scalar(cursor)
    if released != 1:
        raise MySqlMigrationError(
            "could not release the migration 012 advisory lock"
        )


def _execute_statements(
    connection: _Connection,
    statements: Sequence[str],
    *,
    migration_checksum: str,
    phase: str,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            "SET @bside_migration_012_sha256 = %s",
            (migration_checksum,),
        )
        for index, statement in enumerate(statements, start=1):
            try:
                cursor.execute(statement)
            except Exception as error:
                raise MySqlMigrationError(
                    f"migration 012 {phase} failed at statement {index}"
                ) from error
    return len(statements)


def execute_migration_012(
    options: ConnectionOptions,
    *,
    migration_path: Path = DEFAULT_MIGRATION_PATH,
    migration_011_path: Path = DEFAULT_MIGRATION_011_PATH,
    connect_factory: ConnectFactory = _pymysql_connect,
) -> MigrationResult:
    source_bytes = migration_path.read_bytes()
    migration_checksum = _bytes_sha256(source_bytes)
    migration_011_checksum = _bytes_sha256(migration_011_path.read_bytes())
    statements = parse_mysql_script(source_bytes)
    connection: _Connection | None = None
    lock_held = False
    failed = False
    try:
        connection = connect_factory(options, True)
        _acquire_lock(connection)
        lock_held = True
        before = read_migration_manifest(connection)
        verify_prerequisite_manifest(
            before,
            migration_011_checksum=migration_011_checksum,
            migration_012_checksum=migration_checksum,
            allow_version_12=True,
        )
        applied_count = _execute_statements(
            connection,
            statements,
            migration_checksum=migration_checksum,
            phase="apply",
        )
        after_apply = read_migration_manifest(connection)
        verify_prerequisite_manifest(
            after_apply,
            migration_011_checksum=migration_011_checksum,
            migration_012_checksum=migration_checksum,
            allow_version_12=True,
        )
        if after_apply.get(MIGRATION_VERSION) != (
            MIGRATION_NAME,
            migration_checksum,
        ):
            raise MySqlMigrationError(
                "migration 012 was not recorded after apply"
            )
        replayed_count = _execute_statements(
            connection,
            statements,
            migration_checksum=migration_checksum,
            phase="replay",
        )
        after_replay = read_migration_manifest(connection)
        verify_prerequisite_manifest(
            after_replay,
            migration_011_checksum=migration_011_checksum,
            migration_012_checksum=migration_checksum,
            allow_version_12=True,
        )
        if after_replay != after_apply:
            raise MySqlMigrationError(
                "migration manifest changed during replay"
            )
        return MigrationResult(
            version=MIGRATION_VERSION,
            name=MIGRATION_NAME,
            checksum=migration_checksum,
            applied_statement_count=applied_count,
            replayed_statement_count=replayed_count,
            prerequisite_count=len(PREREQUISITE_MIGRATIONS) + 1,
        )
    except Exception:
        failed = True
        raise
    finally:
        cleanup_error: Exception | None = None
        if connection is not None and lock_held:
            try:
                _release_lock(connection)
            except Exception as error:
                cleanup_error = MySqlMigrationError(
                    "could not release the migration 012 advisory lock"
                )
                cleanup_error.__cause__ = error
        if connection is not None:
            try:
                connection.close()
            except Exception as error:
                if cleanup_error is None:
                    cleanup_error = MySqlMigrationError(
                        "could not close the migration 012 connection"
                    )
                    cleanup_error.__cause__ = error
        if cleanup_error is not None and not failed:
            raise cleanup_error


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Apply and replay the exact BSIDE migration 012 bytes in one "
            "verified MySQL session."
        )
    )
    parser.add_argument("--migration", type=Path, default=DEFAULT_MIGRATION_PATH)
    parser.add_argument(
        "--migration-011",
        type=Path,
        default=DEFAULT_MIGRATION_011_PATH,
    )
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--database")
    parser.add_argument("--charset")
    parser.add_argument("--connect-timeout", type=int, default=10)
    parser.add_argument("--read-timeout", type=int, default=300)
    parser.add_argument("--write-timeout", type=int, default=300)
    parser.add_argument("--ssh-tunnel", action="store_true")
    parser.add_argument("--ssh-host")
    parser.add_argument("--ssh-port", type=int)
    parser.add_argument("--ssh-user")
    parser.add_argument("--ssh-password")
    parser.add_argument("--ssh-host-key-sha256")
    parser.add_argument("--ssh-allow-legacy-rsa-sha1", action="store_true")
    parser.add_argument("--ssh-legacy-rsa-sha1-host")
    parser.add_argument("--ssh-remote-db-host")
    parser.add_argument("--ssh-remote-db-port", type=int)
    parser.add_argument("--ssh-connect-timeout", type=int, default=15)
    parser.add_argument("--ssh-auth-timeout", type=int, default=15)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        options = connection_options_from_args(args)
        ssh_options = ssh_options_from_args(args)
        if ssh_options is None:
            result = execute_migration_012(
                options,
                migration_path=args.migration,
                migration_011_path=args.migration_011,
            )
        else:
            with SshDirectTcpipTunnel(
                ssh_options,
                destination_host=options.host,
                destination_port=options.port,
            ) as tunnel:
                if tunnel.local_port is None:
                    raise MySqlMigrationError(
                        "SSH tunnel did not open a local endpoint"
                    )
                tunneled_options = dataclass_replace(
                    options,
                    host=tunnel.local_host,
                    port=tunnel.local_port,
                )
                result = execute_migration_012(
                    tunneled_options,
                    migration_path=args.migration,
                    migration_011_path=args.migration_011,
                )
    except Exception:
        print(
            "MySQL migration 012 failed safely; inspect protected diagnostics.",
            file=sys.stderr,
        )
        return 1
    print(
        "MySQL migration completed and replayed: "
        f"version={result.version} checksum={result.checksum}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
