from __future__ import annotations

import argparse
import hashlib
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import replace as dataclass_replace
from pathlib import Path
from typing import Protocol

from curator.mysql_backup import (
    ConnectionOptions,
    SshDirectTcpipTunnel,
    connection_options_from_args,
    ssh_options_from_args,
)


MIGRATION_VERSION = 11
MIGRATION_NAME = "011_global_terminal_v2"
MIGRATION_LOCK_NAME = "bside:migration:011_global_terminal_v2"
DEFAULT_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "deploy"
    / "activist"
    / "migrations"
    / "011_global_terminal_v2.sql"
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
DELIMITER_DIRECTIVE = re.compile(
    r"^[ \t]*DELIMITER[ \t]+(\S+)[ \t]*$",
    re.IGNORECASE,
)

PREREQUISITE_MIGRATIONS: Mapping[int, tuple[str, str]] = {
    1: (
        "001_governance_v1",
        "2f1f03aa62d733339b79b5bca50e1c480b4f706a5823fd3490bd799421e93afd",
    ),
    2: (
        "002_legacy_source_right_lineage",
        "fdcb2d634a787c7bbe534bd3892470a13aef11254dd75cec1afb54a9f2b61051",
    ),
    3: (
        "003_editorial_governance",
        "906a0071bc11b595eae388a17074bd955f1ebb25f8a7453e3e89534e42ba4f25",
    ),
    4: (
        "004_telegram_signal_rebuild_staging",
        "de64071e117fae70d6849f8191be7267a885e75bf3d498ab7488fa616348fb7f",
    ),
    5: (
        "005_telegram_channel_identity_index",
        "cf1245fe562e583707d821f126562a6f10aa9c8db5e0c9b20afa8ff267d1d903",
    ),
    6: (
        "006_governance_release_guard",
        "f7f7a46f86118316dc21a67bb5b547668d64978b9fe4054b4c86104b85d7ced7",
    ),
    7: (
        "007_governance_identity_and_evidence",
        "074bbb5f066d5f3a20e3b894762ae356fa0a102c61546634fc16be05400f2ebe",
    ),
    8: (
        "008_official_site_snapshot_receipts",
        "b12e5e5290a5901192ddb4c8ec999719aa3dc25596c6c46d16ac383f3be74376",
    ),
    9: (
        "009_dart_global_quota_ledger",
        "9e60867847b7cc2b7d9166c73e395ae872d12a4e91aa62457049468017e5f94d",
    ),
    10: (
        "010_official_slot_claim_ledger",
        "2b8be6264c8a4f3be038729fbf6bbe22e720457874f02c89c82d33db9dc78f51",
    ),
}


class MySqlMigrationError(RuntimeError):
    """Raised when migration 011 cannot be applied and verified safely."""


class _Cursor(Protocol):
    def __enter__(self) -> _Cursor: ...

    def __exit__(self, *args: object) -> None: ...

    def execute(
        self,
        query: str,
        args: Sequence[object] | None = None,
    ) -> object: ...

    def fetchall(self) -> Sequence[object]: ...

    def fetchone(self) -> object: ...


class _Connection(Protocol):
    def cursor(self) -> _Cursor: ...

    def close(self) -> None: ...


ConnectFactory = Callable[[ConnectionOptions, bool], _Connection]


@dataclass(frozen=True)
class MigrationResult:
    version: int
    name: str
    checksum: str
    applied_statement_count: int
    replayed_statement_count: int
    prerequisite_count: int


def _is_sql_trivia(value: str) -> bool:
    position = 0
    length = len(value)
    while position < length:
        if value[position].isspace():
            position += 1
            continue
        if value[position] == "#":
            newline = value.find("\n", position + 1)
            position = length if newline < 0 else newline + 1
            continue
        if (
            value.startswith("--", position)
            and (
                position + 2 == length
                or (
                    position + 2 < length
                    and value[position + 2].isspace()
                )
            )
        ):
            newline = value.find("\n", position + 2)
            position = length if newline < 0 else newline + 1
            continue
        if value.startswith("/*", position):
            end = value.find("*/", position + 2)
            if end < 0:
                return False
            position = end + 2
            continue
        return False
    return True


def _validate_delimiter(value: str, *, line_number: int) -> str:
    if (
        not value
        or len(value) > 32
        or any(character.isspace() or ord(character) < 32 for character in value)
    ):
        raise MySqlMigrationError(
            f"invalid DELIMITER directive at line {line_number}"
        )
    return value


def parse_mysql_script(source_bytes: bytes) -> tuple[str, ...]:
    """Split mysql-client SQL while honoring DELIMITER, strings, and comments."""

    try:
        source = source_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise MySqlMigrationError("migration SQL must be valid UTF-8") from error
    if source.startswith("\ufeff"):
        raise MySqlMigrationError("migration SQL must not contain a UTF-8 BOM")

    statements: list[str] = []
    buffer: list[str] = []
    delimiter = ";"
    state = "normal"
    line_number = 0

    for line_number, line in enumerate(source.splitlines(keepends=True), start=1):
        directive_line = line.rstrip("\r\n")
        if state == "normal" and _is_sql_trivia("".join(buffer)):
            directive = DELIMITER_DIRECTIVE.fullmatch(directive_line)
            if directive is not None:
                buffer.clear()
                delimiter = _validate_delimiter(
                    directive.group(1),
                    line_number=line_number,
                )
                continue
            if re.match(
                r"^[ \t]*DELIMITER(?:[ \t]|$)",
                directive_line,
                re.IGNORECASE,
            ):
                raise MySqlMigrationError(
                    f"invalid DELIMITER directive at line {line_number}"
                )

        position = 0
        while position < len(line):
            if state == "line-comment":
                character = line[position]
                buffer.append(character)
                position += 1
                if character == "\n":
                    state = "normal"
                continue
            if state == "block-comment":
                if line.startswith("*/", position):
                    buffer.append("*/")
                    position += 2
                    state = "normal"
                else:
                    buffer.append(line[position])
                    position += 1
                continue
            if state in {"single-quote", "double-quote", "backtick"}:
                character = line[position]
                buffer.append(character)
                position += 1
                quote = {
                    "single-quote": "'",
                    "double-quote": '"',
                    "backtick": "`",
                }[state]
                if character == "\\" and position < len(line):
                    buffer.append(line[position])
                    position += 1
                elif character == quote:
                    if position < len(line) and line[position] == quote:
                        buffer.append(line[position])
                        position += 1
                    else:
                        state = "normal"
                continue

            if line.startswith(delimiter, position):
                statement = "".join(buffer).strip()
                if not statement or _is_sql_trivia(statement):
                    raise MySqlMigrationError(
                        f"empty SQL statement before delimiter at line {line_number}"
                    )
                statements.append(statement)
                buffer.clear()
                position += len(delimiter)
                continue
            if line.startswith("/*", position):
                buffer.append("/*")
                position += 2
                state = "block-comment"
                continue
            if line[position] == "#":
                buffer.append("#")
                position += 1
                state = "line-comment"
                continue
            if (
                line.startswith("--", position)
                and (
                    position + 2 == len(line)
                    or (
                        position + 2 < len(line)
                        and line[position + 2].isspace()
                    )
                )
            ):
                buffer.append("--")
                position += 2
                state = "line-comment"
                continue
            character = line[position]
            buffer.append(character)
            position += 1
            if character == "'":
                state = "single-quote"
            elif character == '"':
                state = "double-quote"
            elif character == "`":
                state = "backtick"

    if state in {"single-quote", "double-quote", "backtick", "block-comment"}:
        raise MySqlMigrationError(
            f"unterminated SQL token at line {max(line_number, 1)}"
        )
    trailing = "".join(buffer)
    if trailing and not _is_sql_trivia(trailing):
        raise MySqlMigrationError("migration SQL ends without its active delimiter")
    if not statements:
        raise MySqlMigrationError("migration SQL contains no executable statements")
    return tuple(statements)


def _normalize_manifest_row(row: object) -> tuple[int, str, str]:
    if isinstance(row, Mapping):
        lowered = {str(key).casefold(): value for key, value in row.items()}
        values = (
            lowered.get("migration_version"),
            lowered.get("migration_name"),
            lowered.get("migration_checksum"),
        )
    elif isinstance(row, Sequence) and not isinstance(
        row,
        (str, bytes, bytearray, memoryview),
    ):
        if len(row) < 3:
            raise MySqlMigrationError("migration manifest row has an invalid shape")
        values = (row[0], row[1], row[2])
    else:
        raise MySqlMigrationError("migration manifest row has an invalid shape")
    def normalized_text(value: object) -> str:
        if isinstance(value, bytes):
            try:
                return value.decode("ascii")
            except UnicodeDecodeError as error:
                raise MySqlMigrationError(
                    "migration manifest contains non-ASCII identity"
                ) from error
        return str(value)

    try:
        version = int(normalized_text(values[0]))
    except ValueError as error:
        raise MySqlMigrationError("migration version is invalid") from error

    return version, normalized_text(values[1]), normalized_text(values[2])


def read_migration_manifest(connection: _Connection) -> dict[int, tuple[str, str]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT migration_version, migration_name, migration_checksum
            FROM activist_schema_migrations
            WHERE migration_version >= 1
            ORDER BY migration_version
            """
        )
        rows = cursor.fetchall()
    manifest: dict[int, tuple[str, str]] = {}
    for row in rows:
        version, name, checksum = _normalize_manifest_row(row)
        if version in manifest:
            raise MySqlMigrationError("migration manifest contains duplicate versions")
        manifest[version] = (name, checksum)
    return manifest


def verify_prerequisite_manifest(
    manifest: Mapping[int, tuple[str, str]],
    *,
    migration_checksum: str,
    allow_version_11: bool,
) -> None:
    for version, expected in PREREQUISITE_MIGRATIONS.items():
        if manifest.get(version) != expected:
            raise MySqlMigrationError(
                f"prerequisite migration {version:03d} identity mismatch"
            )
    unexpected = set(manifest).difference(PREREQUISITE_MIGRATIONS).difference({11})
    if unexpected:
        raise MySqlMigrationError("migration manifest contains unsupported versions")
    version_11 = manifest.get(MIGRATION_VERSION)
    expected_version_11 = (MIGRATION_NAME, migration_checksum)
    if version_11 is not None and (
        not allow_version_11 or version_11 != expected_version_11
    ):
        raise MySqlMigrationError("migration 011 identity mismatch")


def _scalar(cursor: _Cursor) -> object:
    row = cursor.fetchone()
    if isinstance(row, Mapping):
        if len(row) != 1:
            raise MySqlMigrationError("database scalar result has an invalid shape")
        return next(iter(row.values()))
    if isinstance(row, Sequence) and not isinstance(
        row,
        (str, bytes, bytearray, memoryview),
    ):
        if len(row) != 1:
            raise MySqlMigrationError("database scalar result has an invalid shape")
        return row[0]
    raise MySqlMigrationError("database scalar result has an invalid shape")


def _acquire_lock(connection: _Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT GET_LOCK(%s, %s)", (MIGRATION_LOCK_NAME, 30))
        acquired = _scalar(cursor)
    if acquired != 1:
        raise MySqlMigrationError("could not acquire the migration 011 advisory lock")


def _release_lock(connection: _Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT RELEASE_LOCK(%s)", (MIGRATION_LOCK_NAME,))
        released = _scalar(cursor)
    if released != 1:
        raise MySqlMigrationError("could not release the migration 011 advisory lock")


def _execute_statements(
    connection: _Connection,
    statements: Sequence[str],
    *,
    migration_checksum: str,
    phase: str,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            "SET @bside_migration_011_sha256 = %s",
            (migration_checksum,),
        )
        for index, statement in enumerate(statements, start=1):
            try:
                cursor.execute(statement)
            except Exception as error:
                raise MySqlMigrationError(
                    f"migration 011 {phase} failed at statement {index}"
                ) from error
    return len(statements)


def _pymysql_connect(options: ConnectionOptions, autocommit: bool) -> _Connection:
    try:
        import pymysql  # type: ignore
    except ImportError as error:  # pragma: no cover - declared dependency
        raise MySqlMigrationError(
            "PyMySQL is required to execute migration 011"
        ) from error
    return pymysql.connect(
        host=options.host,
        port=options.port,
        user=options.user,
        password=options.password,
        database=options.database,
        charset=options.charset,
        use_unicode=True,
        binary_prefix=True,
        connect_timeout=options.connect_timeout,
        read_timeout=options.read_timeout,
        write_timeout=options.write_timeout,
        autocommit=autocommit,
    )


def execute_migration_011(
    options: ConnectionOptions,
    *,
    migration_path: Path = DEFAULT_MIGRATION_PATH,
    connect_factory: ConnectFactory = _pymysql_connect,
) -> MigrationResult:
    source_bytes = migration_path.read_bytes()
    migration_checksum = hashlib.sha256(source_bytes).hexdigest()
    if SHA256_PATTERN.fullmatch(migration_checksum) is None:
        raise MySqlMigrationError("migration source SHA-256 is invalid")
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
            migration_checksum=migration_checksum,
            allow_version_11=True,
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
            migration_checksum=migration_checksum,
            allow_version_11=True,
        )
        if after_apply.get(MIGRATION_VERSION) != (
            MIGRATION_NAME,
            migration_checksum,
        ):
            raise MySqlMigrationError("migration 011 was not recorded after apply")
        replayed_count = _execute_statements(
            connection,
            statements,
            migration_checksum=migration_checksum,
            phase="replay",
        )
        after_replay = read_migration_manifest(connection)
        verify_prerequisite_manifest(
            after_replay,
            migration_checksum=migration_checksum,
            allow_version_11=True,
        )
        if after_replay != after_apply:
            raise MySqlMigrationError("migration manifest changed during replay")
        return MigrationResult(
            version=MIGRATION_VERSION,
            name=MIGRATION_NAME,
            checksum=migration_checksum,
            applied_statement_count=applied_count,
            replayed_statement_count=replayed_count,
            prerequisite_count=len(PREREQUISITE_MIGRATIONS),
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
                    "could not release the migration 011 advisory lock"
                )
                cleanup_error.__cause__ = error
        if connection is not None:
            try:
                connection.close()
            except Exception as error:
                if cleanup_error is None:
                    cleanup_error = MySqlMigrationError(
                        "could not close the migration 011 connection"
                    )
                    cleanup_error.__cause__ = error
        if cleanup_error is not None and not failed:
            raise cleanup_error


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Apply and replay the exact BSIDE migration 011 bytes in one "
            "verified MySQL session."
        )
    )
    parser.add_argument("--migration", type=Path, default=DEFAULT_MIGRATION_PATH)
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
    parser.add_argument(
        "--ssh-allow-legacy-rsa-sha1",
        action="store_true",
        help=(
            "Allow the legacy ssh-rsa/SHA-1 host-key algorithm only for the "
            "explicitly pinned --ssh-legacy-rsa-sha1-host"
        ),
    )
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
            result = execute_migration_011(
                options,
                migration_path=args.migration,
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
                result = execute_migration_011(
                    tunneled_options,
                    migration_path=args.migration,
                )
    except Exception:
        print(
            "MySQL migration 011 failed safely; inspect protected diagnostics.",
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
