from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import hmac
import json
import math
import os
import re
import socket
import socketserver
import sys
import tempfile
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from dataclasses import replace as dataclass_replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol


MANIFEST_SCHEMA_VERSION = 1
CODE_REVISION_PATTERN = re.compile(r"^[0-9a-f]{40}$")
DEFAULT_BATCH_ROWS = 500
DEFAULT_BATCH_BYTES = 1024 * 1024
DEFAULT_FETCH_SIZE = 500
SSH_SHA256_PATTERN = re.compile(r"^SHA256:[A-Za-z0-9+/]{43}$")


class MySqlBackupError(RuntimeError):
    """Raised when a safe, completed MySQL backup cannot be produced."""


class _Connection(Protocol):
    def cursor(self, *args: object, **kwargs: object) -> Any: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class ConnectionOptions:
    host: str
    port: int
    user: str
    password: str = field(repr=False)
    database: str
    charset: str = "utf8mb4"
    connect_timeout: int = 10
    read_timeout: int = 300
    write_timeout: int = 300


@dataclass(frozen=True)
class SshTunnelOptions:
    host: str
    port: int
    user: str
    password: str = field(repr=False)
    host_key_sha256: str
    remote_host: str | None = None
    remote_port: int | None = None
    connect_timeout: int = 15
    auth_timeout: int = 15
    allow_legacy_ssh_rsa_sha1: bool = False
    legacy_ssh_rsa_sha1_host: str | None = None


@dataclass(frozen=True)
class TableDefinition:
    name: str
    engine: str
    create_sql: str
    insert_columns: tuple[str, ...]
    generated_column_count: int

    @property
    def transactional(self) -> bool:
        return self.engine.casefold() == "innodb"


@dataclass(frozen=True)
class BackupResult:
    output_path: Path
    manifest_path: Path
    manifest: Mapping[str, object]


ConnectFactory = Callable[[ConnectionOptions, bool], _Connection]


def quote_identifier(value: str) -> str:
    """Quote an arbitrary MySQL identifier without silently changing it."""

    if not isinstance(value, str) or not value or "\x00" in value:
        raise MySqlBackupError(
            "MySQL identifiers must be non-empty strings without NUL"
        )
    return "`" + value.replace("`", "``") + "`"


def _escape_mysql_text(value: str) -> str:
    replacements = {
        "\\": "\\\\",
        "\x00": "\\0",
        "\b": "\\b",
        "\t": "\\t",
        "\n": "\\n",
        "\r": "\\r",
        "\x1a": "\\Z",
        "'": "\\'",
    }
    return "".join(replacements.get(character, character) for character in value)


def _format_datetime(value: datetime) -> str:
    if value.utcoffset() is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    result = value.strftime("%Y-%m-%d %H:%M:%S")
    if value.microsecond:
        result += f".{value.microsecond:06d}"
    return result


def _format_time(value: time) -> str:
    if value.utcoffset() is not None:
        raise MySqlBackupError("timezone-aware time values cannot be serialized safely")
    result = value.strftime("%H:%M:%S")
    if value.microsecond:
        result += f".{value.microsecond:06d}"
    return result


def _format_timedelta(value: timedelta) -> str:
    microseconds = (
        value.days * 86_400 + value.seconds
    ) * 1_000_000 + value.microseconds
    sign = "-" if microseconds < 0 else ""
    microseconds = abs(microseconds)
    seconds, fraction = divmod(microseconds, 1_000_000)
    hours, seconds = divmod(seconds, 3_600)
    minutes, seconds = divmod(seconds, 60)
    result = f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    if fraction:
        result += f".{fraction:06d}"
    return result


def sql_literal(value: object) -> str:
    """Serialize a PyMySQL value to a restore-safe SQL literal."""

    if value is None:
        return "NULL"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "X'" + bytes(value).hex() + "'"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise MySqlBackupError("non-finite Decimal values cannot be serialized")
        return format(value, "f")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise MySqlBackupError("non-finite float values cannot be serialized")
        return repr(value)
    if isinstance(value, datetime):
        return "'" + _format_datetime(value) + "'"
    if isinstance(value, date):
        return "'" + value.isoformat() + "'"
    if isinstance(value, time):
        return "'" + _format_time(value) + "'"
    if isinstance(value, timedelta):
        return "'" + _format_timedelta(value) + "'"
    if isinstance(value, str):
        return "'" + _escape_mysql_text(value) + "'"
    raise MySqlBackupError(f"unsupported MySQL value type: {type(value).__name__}")


def _first_env(environ: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        if name in environ:
            return environ[name]
    return None


def _required_connection_value(
    cli_value: object,
    environ: Mapping[str, str],
    *environment_names: str,
) -> str:
    value = (
        str(cli_value)
        if cli_value is not None
        else _first_env(environ, *environment_names)
    )
    if value is None or not value.strip():
        raise MySqlBackupError(
            f"missing connection setting ({'/'.join(environment_names)})"
        )
    return value.strip()


def connection_options_from_args(
    args: argparse.Namespace,
    *,
    environ: Mapping[str, str] | None = None,
) -> ConnectionOptions:
    environment = os.environ if environ is None else environ
    host = _required_connection_value(args.host, environment, "DB_HOST", "MYSQL_HOST")
    user = _required_connection_value(args.user, environment, "DB_USER", "MYSQL_USER")
    database = _required_connection_value(
        args.database, environment, "DB_NAME", "MYSQL_DATABASE"
    )
    password_value = (
        args.password
        if args.password is not None
        else _first_env(environment, "DB_PASSWORD", "MYSQL_PASSWORD")
    )
    if password_value is None:
        raise MySqlBackupError(
            "missing connection setting (DB_PASSWORD/MYSQL_PASSWORD)"
        )
    raw_port = (
        args.port
        if args.port is not None
        else _first_env(environment, "DB_PORT", "MYSQL_PORT") or "3306"
    )
    raw_charset = (
        args.charset
        if args.charset is not None
        else _first_env(environment, "DB_CHARSET", "MYSQL_CHARSET") or "utf8mb4"
    )
    try:
        port = int(raw_port)
    except (TypeError, ValueError) as error:
        raise MySqlBackupError("database port must be an integer") from error
    if not 1 <= port <= 65535:
        raise MySqlBackupError("database port must be between 1 and 65535")
    return ConnectionOptions(
        host=host,
        port=port,
        user=user,
        password=str(password_value),
        database=database,
        charset=str(raw_charset),
        connect_timeout=args.connect_timeout,
        read_timeout=args.read_timeout,
        write_timeout=args.write_timeout,
    )


def _environment_flag(
    value: str | None,
    *,
    setting: str = "SSH tunnel enablement",
) -> bool:
    if value is None:
        return False
    normalized = value.strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off", ""}:
        return False
    raise MySqlBackupError(f"{setting} must be a boolean")


def _optional_cli_or_env(
    cli_value: object,
    environ: Mapping[str, str],
    *environment_names: str,
) -> str | None:
    if cli_value is not None:
        return str(cli_value).strip()
    value = _first_env(environ, *environment_names)
    return None if value is None else value.strip()


def normalize_ssh_host_key_sha256(value: str) -> str:
    fingerprint = value.strip()
    if not fingerprint.startswith("SHA256:"):
        fingerprint = "SHA256:" + fingerprint
    fingerprint = fingerprint.rstrip("=")
    if SSH_SHA256_PATTERN.fullmatch(fingerprint) is None:
        raise MySqlBackupError("SSH host key SHA-256 fingerprint is invalid")
    return fingerprint


def ssh_host_key_sha256(key_bytes: bytes) -> str:
    encoded = base64.b64encode(hashlib.sha256(key_bytes).digest()).decode("ascii")
    return "SHA256:" + encoded.rstrip("=")


def verify_ssh_host_key(key_bytes: bytes, expected_sha256: str) -> None:
    expected = normalize_ssh_host_key_sha256(expected_sha256)
    actual = ssh_host_key_sha256(key_bytes)
    if not hmac.compare_digest(actual, expected):
        raise MySqlBackupError("SSH server host key does not match the pinned SHA-256")


def _canonical_ssh_host(value: str) -> str:
    """Normalize only syntax that cannot change a host's identity."""

    normalized = value.strip().rstrip(".").casefold()
    if not normalized:
        raise MySqlBackupError("SSH host must be non-empty")
    try:
        return normalized.encode("idna").decode("ascii")
    except UnicodeError as error:
        raise MySqlBackupError("SSH host is invalid") from error


def legacy_ssh_rsa_sha1_is_allowed(options: SshTunnelOptions) -> bool:
    """Validate and return the narrowly scoped legacy host-key exception."""

    normalize_ssh_host_key_sha256(options.host_key_sha256)
    target = options.legacy_ssh_rsa_sha1_host
    if options.allow_legacy_ssh_rsa_sha1:
        if target is None:
            raise MySqlBackupError(
                "legacy ssh-rsa/SHA-1 requires an explicit target host"
            )
        if _canonical_ssh_host(target) != _canonical_ssh_host(options.host):
            raise MySqlBackupError(
                "legacy ssh-rsa/SHA-1 target must match the SSH tunnel host"
            )
        return True
    if target is not None:
        raise MySqlBackupError(
            "legacy ssh-rsa/SHA-1 target requires explicit opt-in"
        )
    return False


def _enable_paramiko_legacy_ssh_rsa_sha1(
    transport: Any,
    paramiko_module: Any,
) -> None:
    """Enable only the legacy ssh-rsa host-signature algorithm on one transport."""

    try:
        from cryptography.hazmat.primitives import hashes

        if str(paramiko_module.__version__).split(".", maxsplit=1)[0] != "5":
            raise MySqlBackupError(
                "legacy ssh-rsa/SHA-1 compatibility requires Paramiko 5"
            )
        base_rsa_key: Any = paramiko_module.RSAKey
        legacy_host_key_class = type(
            "_LegacySshRsaSha1HostKey",
            (base_rsa_key,),
            {
                "HASHES": {
                    **base_rsa_key.HASHES,
                    "ssh-rsa": hashes.SHA1,
                },
            },
        )
        base_transport_class: Any = transport.__class__

        def preferred_keys(instance: Any) -> tuple[str, ...]:
            disabled_algorithms = (
                getattr(instance, "disabled_algorithms", None) or {}
            )
            disabled = disabled_algorithms.get("keys", ())
            filtered = tuple(
                algorithm
                for algorithm in instance._preferred_keys
                if algorithm not in disabled
            )
            certificates = tuple(
                f"{algorithm}-cert-v01@openssh.com"
                for algorithm in filtered
                if algorithm != "ssh-rsa"
            )
            return filtered + certificates

        legacy_transport_class = type(
            "_LegacySshRsaSha1Transport",
            (base_transport_class,),
            {"preferred_keys": property(preferred_keys)},
        )
        transport.__class__ = legacy_transport_class

        transport._key_info = {
            **transport._key_info,
            "ssh-rsa": legacy_host_key_class,
        }
        transport._preferred_keys = tuple(
            algorithm
            for algorithm in transport._preferred_keys
            if algorithm != "ssh-rsa"
        ) + ("ssh-rsa",)
        if "ssh-rsa-cert-v01@openssh.com" in transport.preferred_keys:
            raise MySqlBackupError(
                "legacy ssh-rsa/SHA-1 compatibility could not be isolated"
            )
    except (AttributeError, ImportError, TypeError) as error:
        raise MySqlBackupError(
            "legacy ssh-rsa/SHA-1 compatibility is unavailable"
        ) from error


def ssh_options_from_args(
    args: argparse.Namespace,
    *,
    environ: Mapping[str, str] | None = None,
) -> SshTunnelOptions | None:
    environment = os.environ if environ is None else environ
    legacy_environment_opt_in = _environment_flag(
        _first_env(
            environment,
            "SSH_ALLOW_LEGACY_RSA_SHA1",
            "GABIA_SSH_ALLOW_LEGACY_RSA_SHA1",
        ),
        setting="legacy ssh-rsa/SHA-1 opt-in",
    )
    legacy_cli_opt_in = bool(
        getattr(args, "ssh_allow_legacy_rsa_sha1", False)
    )
    allow_legacy_ssh_rsa_sha1 = (
        legacy_cli_opt_in or legacy_environment_opt_in
    )
    legacy_ssh_rsa_sha1_host = _optional_cli_or_env(
        getattr(args, "ssh_legacy_rsa_sha1_host", None),
        environment,
        "SSH_LEGACY_RSA_SHA1_HOST",
        "GABIA_SSH_LEGACY_RSA_SHA1_HOST",
    )
    requested = bool(args.ssh_tunnel) or _environment_flag(
        _first_env(environment, "DB_SSH_TUNNEL", "MYSQL_SSH_TUNNEL")
    )
    explicit_values = (
        args.ssh_host,
        args.ssh_user,
        args.ssh_password,
        args.ssh_host_key_sha256,
        legacy_ssh_rsa_sha1_host,
    )
    if allow_legacy_ssh_rsa_sha1:
        requested = True
    if not requested and any(value is not None for value in explicit_values):
        requested = True
    if not requested:
        return None
    host = _required_connection_value(
        args.ssh_host,
        environment,
        "SSH_HOST",
        "GABIA_SSH_HOST",
    )
    user = _required_connection_value(
        args.ssh_user,
        environment,
        "SSH_USER",
        "SSH_USERNAME",
        "GABIA_SSH_USER",
    )
    password = _optional_cli_or_env(
        args.ssh_password,
        environment,
        "SSH_PASSWORD",
        "GABIA_SSH_PASSWORD",
    )
    if password is None:
        raise MySqlBackupError("missing SSH password")
    host_key = _required_connection_value(
        args.ssh_host_key_sha256,
        environment,
        "SSH_HOST_KEY_SHA256",
        "GABIA_SSH_HOST_KEY_SHA256",
    )
    raw_port = (
        _optional_cli_or_env(
            args.ssh_port,
            environment,
            "SSH_PORT",
            "GABIA_SSH_PORT",
        )
        or "22"
    )
    raw_remote_host = _optional_cli_or_env(
        args.ssh_remote_db_host,
        environment,
        "SSH_DB_HOST",
    )
    raw_remote_port = _optional_cli_or_env(
        args.ssh_remote_db_port,
        environment,
        "SSH_DB_PORT",
    )
    try:
        port = int(raw_port)
        remote_port = None if raw_remote_port is None else int(raw_remote_port)
    except ValueError as error:
        raise MySqlBackupError("SSH ports must be integers") from error
    if not 1 <= port <= 65535 or (
        remote_port is not None and not 1 <= remote_port <= 65535
    ):
        raise MySqlBackupError("SSH ports must be between 1 and 65535")
    options = SshTunnelOptions(
        host=host,
        port=port,
        user=user,
        password=password,
        host_key_sha256=normalize_ssh_host_key_sha256(host_key),
        remote_host=raw_remote_host or None,
        remote_port=remote_port,
        connect_timeout=args.ssh_connect_timeout,
        auth_timeout=args.ssh_auth_timeout,
        allow_legacy_ssh_rsa_sha1=allow_legacy_ssh_rsa_sha1,
        legacy_ssh_rsa_sha1_host=legacy_ssh_rsa_sha1_host or None,
    )
    legacy_ssh_rsa_sha1_is_allowed(options)
    return options


class _DirectTcpipServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = False
    daemon_threads = True
    ssh_transport: Any
    remote_destination: tuple[str, int]

    def handle_error(
        self,
        _request: object,
        _client_address: object,
    ) -> None:
        # The CLI intentionally does not print endpoints or credential-adjacent data.
        return None


class _DirectTcpipHandler(socketserver.BaseRequestHandler):
    server: Any

    @staticmethod
    def _pump(source: Any, destination: Any, stop: threading.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    chunk = source.recv(64 * 1024)
                except (socket.timeout, TimeoutError):
                    continue
                if not chunk:
                    break
                destination.sendall(chunk)
        except Exception:
            # Forwarding errors fail the DB connection itself; never echo
            # endpoint or transport details from a background thread.
            pass
        finally:
            stop.set()

    def handle(self) -> None:
        channel = self.server.ssh_transport.open_channel(
            "direct-tcpip",
            self.server.remote_destination,
            self.client_address,
        )
        if channel is None:
            return
        stop = threading.Event()
        self.request.settimeout(0.5)
        channel.settimeout(0.5)
        pumps = [
            threading.Thread(
                target=self._pump,
                args=(self.request, channel, stop),
                daemon=True,
            ),
            threading.Thread(
                target=self._pump,
                args=(channel, self.request, stop),
                daemon=True,
            ),
        ]
        try:
            for pump in pumps:
                pump.start()
            stop.wait()
        finally:
            stop.set()
            try:
                channel.close()
            finally:
                try:
                    self.request.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            for pump in pumps:
                pump.join(timeout=1)


class SshDirectTcpipTunnel:
    """Password-authenticated SSH tunnel with a mandatory pinned host key."""

    def __init__(
        self,
        options: SshTunnelOptions,
        *,
        destination_host: str,
        destination_port: int,
    ) -> None:
        self.options = options
        self.destination_host = options.remote_host or destination_host
        self.destination_port = options.remote_port or destination_port
        self._socket: socket.socket | None = None
        self._transport: Any = None
        self._server: _DirectTcpipServer | None = None
        self._server_thread: threading.Thread | None = None
        self.local_host = "127.0.0.1"
        self.local_port: int | None = None

    def __enter__(self) -> "SshDirectTcpipTunnel":
        try:
            import paramiko  # type: ignore
        except ImportError as error:  # pragma: no cover - available in operations venv
            raise MySqlBackupError(
                "Paramiko is required when the SSH tunnel is enabled"
            ) from error
        try:
            allow_legacy_ssh_rsa_sha1 = legacy_ssh_rsa_sha1_is_allowed(
                self.options
            )
            self._socket = socket.create_connection(
                (self.options.host, self.options.port),
                timeout=self.options.connect_timeout,
            )
            disabled_algorithms = (
                None
                if allow_legacy_ssh_rsa_sha1
                else {"keys": ["ssh-rsa"]}
            )
            self._transport = paramiko.Transport(
                self._socket,
                disabled_algorithms=disabled_algorithms,
            )
            if allow_legacy_ssh_rsa_sha1:
                _enable_paramiko_legacy_ssh_rsa_sha1(
                    self._transport,
                    paramiko,
                )
            self._transport.auth_timeout = self.options.auth_timeout
            self._transport.start_client(timeout=self.options.connect_timeout)
            server_key = self._transport.get_remote_server_key()
            verify_ssh_host_key(
                server_key.asbytes(),
                self.options.host_key_sha256,
            )
            self._transport.auth_password(
                username=self.options.user,
                password=self.options.password,
                fallback=False,
            )
            if not self._transport.is_authenticated():
                raise MySqlBackupError("SSH authentication did not complete")
            server = _DirectTcpipServer(
                (self.local_host, 0),
                _DirectTcpipHandler,
            )
            server.ssh_transport = self._transport
            server.remote_destination = (
                self.destination_host,
                self.destination_port,
            )
            self._server = server
            self.local_port = int(server.server_address[1])
            self._server_thread = threading.Thread(
                target=server.serve_forever,
                kwargs={"poll_interval": 0.1},
                daemon=True,
            )
            self._server_thread.start()
            return self
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        if self._server is not None:
            if self._server_thread is not None and self._server_thread.is_alive():
                self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._server_thread is not None:
            self._server_thread.join(timeout=2)
            self._server_thread = None
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        if self._socket is not None:
            self._socket.close()
            self._socket = None
        self.local_port = None

    def __exit__(self, *_args: object) -> None:
        self.close()


def _pymysql_connect(options: ConnectionOptions, autocommit: bool) -> _Connection:
    try:
        import pymysql  # type: ignore
    except ImportError as error:  # pragma: no cover - declared in requirements.txt
        raise MySqlBackupError("PyMySQL is required for MySQL backups") from error
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


def _streaming_cursor_class() -> object | None:
    try:
        import pymysql
    except ImportError:  # pragma: no cover - checked by _pymysql_connect
        return None
    return pymysql.cursors.SSCursor


def _row_item(row: object, index: int, *keys: str) -> object:
    if isinstance(row, Mapping):
        lowered = {str(key).casefold(): value for key, value in row.items()}
        for key in keys:
            if key.casefold() in lowered:
                return lowered[key.casefold()]
        raise MySqlBackupError(f"database metadata row is missing {keys[0]}")
    if isinstance(row, Sequence) and not isinstance(
        row, (str, bytes, bytearray, memoryview)
    ):
        try:
            return row[index]
        except IndexError as error:
            raise MySqlBackupError(
                "database metadata row has an invalid shape"
            ) from error
    raise MySqlBackupError("database metadata row has an unsupported shape")


def _list_tables(connection: _Connection, database: str) -> list[tuple[str, str]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT TABLE_NAME, ENGINE
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """,
            (database,),
        )
        rows = cursor.fetchall()
    result: list[tuple[str, str]] = []
    for row in rows:
        name = str(_row_item(row, 0, "TABLE_NAME"))
        engine_value = _row_item(row, 1, "ENGINE")
        engine = str(engine_value or "UNKNOWN")
        quote_identifier(name)
        result.append((name, engine))
    return result


def _assert_no_unsupported_schema_objects(
    connection: _Connection,
    database: str,
) -> None:
    """Fail closed rather than claiming a full backup with omitted objects."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM information_schema.VIEWS
               WHERE TABLE_SCHEMA = %s) AS view_count,
              (SELECT COUNT(*) FROM information_schema.TRIGGERS
               WHERE TRIGGER_SCHEMA = %s) AS trigger_count,
              (SELECT COUNT(*) FROM information_schema.ROUTINES
               WHERE ROUTINE_SCHEMA = %s) AS routine_count,
              (SELECT COUNT(*) FROM information_schema.EVENTS
               WHERE EVENT_SCHEMA = %s) AS event_count
            """,
            (database, database, database, database),
        )
        row = cursor.fetchone()
    if row is None:
        raise MySqlBackupError("database object preflight returned no result")
    counts = (
        int(str(_row_item(row, 0, "view_count"))),
        int(str(_row_item(row, 1, "trigger_count"))),
        int(str(_row_item(row, 2, "routine_count"))),
        int(str(_row_item(row, 3, "event_count"))),
    )
    if any(counts):
        raise MySqlBackupError(
            "full backup aborted because unsupported views, triggers, "
            "routines, or events exist"
        )


def _show_create_table(connection: _Connection, table_name: str) -> str:
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW CREATE TABLE {quote_identifier(table_name)}")
        row = cursor.fetchone()
    if row is None:
        raise MySqlBackupError(f"SHOW CREATE TABLE returned no row for {table_name!r}")
    value = _row_item(row, 1, "Create Table", "CREATE TABLE")
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if not isinstance(value, str) or not value.strip():
        raise MySqlBackupError(
            f"SHOW CREATE TABLE returned invalid SQL for {table_name!r}"
        )
    return value.strip().rstrip(";")


def _insertable_columns(
    connection: _Connection,
    database: str,
    table_name: str,
) -> tuple[tuple[str, ...], int]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME, EXTRA, GENERATION_EXPRESSION
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (database, table_name),
        )
        rows = cursor.fetchall()
    if not rows:
        raise MySqlBackupError(f"table metadata has no columns for {table_name!r}")
    insertable: list[str] = []
    generated_count = 0
    for row in rows:
        name = str(_row_item(row, 0, "COLUMN_NAME"))
        extra = str(_row_item(row, 1, "EXTRA") or "")
        expression = _row_item(row, 2, "GENERATION_EXPRESSION")
        quote_identifier(name)
        normalized_extra = " ".join(extra.casefold().split())
        if (
            "virtual generated" in normalized_extra
            or "stored generated" in normalized_extra
            or str(expression or "").strip()
        ):
            generated_count += 1
        else:
            insertable.append(name)
    return tuple(insertable), generated_count


def _capture_definitions(
    connection: _Connection,
    database: str,
    tables: Sequence[tuple[str, str]],
) -> list[TableDefinition]:
    result: list[TableDefinition] = []
    for name, engine in tables:
        insert_columns, generated_count = _insertable_columns(
            connection,
            database,
            name,
        )
        result.append(
            TableDefinition(
                name=name,
                engine=engine,
                create_sql=_show_create_table(connection, name),
                insert_columns=insert_columns,
                generated_column_count=generated_count,
            )
        )
    return result


def _execute(connection: _Connection, statement: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(statement)


class _SqlWriter:
    def __init__(self, destination: Any) -> None:
        self.destination = destination
        self.total_uncompressed_bytes = 0
        self.uncompressed_digest = hashlib.sha256()

    def write(self, text: str) -> bytes:
        encoded = text.encode("utf-8")
        self.destination.write(encoded)
        self.total_uncompressed_bytes += len(encoded)
        self.uncompressed_digest.update(encoded)
        return encoded


class _TableWriter:
    def __init__(self, writer: _SqlWriter) -> None:
        self.writer = writer
        self.digest = hashlib.sha256()
        self.byte_count = 0

    def write(self, text: str) -> None:
        encoded = self.writer.write(text)
        self.digest.update(encoded)
        self.byte_count += len(encoded)


def _column_name(description_entry: object) -> str:
    if isinstance(description_entry, Sequence) and not isinstance(
        description_entry, (str, bytes, bytearray, memoryview)
    ):
        if description_entry:
            return str(description_entry[0])
    name = getattr(description_entry, "name", None)
    if name:
        return str(name)
    raise MySqlBackupError("streaming cursor returned an invalid column description")


def _row_values(row: object, columns: Sequence[str]) -> list[object]:
    if isinstance(row, Mapping):
        return [row[column] for column in columns]
    if isinstance(row, Sequence) and not isinstance(
        row, (str, bytes, bytearray, memoryview)
    ):
        if len(row) != len(columns):
            raise MySqlBackupError("database row length does not match its columns")
        return list(row)
    raise MySqlBackupError("database row has an unsupported shape")


def _emit_insert_batch(
    table_writer: _TableWriter,
    *,
    table_name: str,
    columns: Sequence[str],
    rows: Sequence[str],
) -> None:
    if not rows:
        return
    column_sql = ",".join(quote_identifier(column) for column in columns)
    table_writer.write(
        f"INSERT INTO {quote_identifier(table_name)} ({column_sql}) VALUES\n"
        + ",\n".join(rows)
        + ";\n"
    )


def _dump_table(
    connection: _Connection,
    definition: TableDefinition,
    writer: _SqlWriter,
    *,
    stream_cursor_class: object | None,
    fetch_size: int,
    batch_rows: int,
    batch_bytes: int,
) -> dict[str, object]:
    table_writer = _TableWriter(writer)
    table_writer.write(f"DROP TABLE IF EXISTS {quote_identifier(definition.name)};\n")
    table_writer.write(definition.create_sql + ";\n")

    cursor_args = () if stream_cursor_class is None else (stream_cursor_class,)
    row_count = 0
    pending_rows: list[str] = []
    pending_bytes = 0
    with connection.cursor(*cursor_args) as cursor:
        columns = list(definition.insert_columns)
        projection = (
            ",".join(quote_identifier(column) for column in columns) if columns else "1"
        )
        cursor.execute(f"SELECT {projection} FROM {quote_identifier(definition.name)}")
        description = cursor.description
        if description is None:
            raise MySqlBackupError(
                f"streaming SELECT returned no description for {definition.name!r}"
            )
        if columns:
            returned_columns = [_column_name(entry) for entry in description]
            if returned_columns != columns:
                raise MySqlBackupError(
                    f"streaming SELECT columns changed for {definition.name!r}"
                )
        while True:
            fetched = cursor.fetchmany(fetch_size)
            if not fetched:
                break
            for row in fetched:
                values = [] if not columns else _row_values(row, columns)
                serialized = (
                    "(" + ",".join(sql_literal(value) for value in values) + ")"
                )
                serialized_bytes = len(serialized.encode("utf-8"))
                if pending_rows and (
                    len(pending_rows) >= batch_rows
                    or pending_bytes + serialized_bytes > batch_bytes
                ):
                    _emit_insert_batch(
                        table_writer,
                        table_name=definition.name,
                        columns=columns,
                        rows=pending_rows,
                    )
                    pending_rows = []
                    pending_bytes = 0
                pending_rows.append(serialized)
                pending_bytes += serialized_bytes
                row_count += 1
        _emit_insert_batch(
            table_writer,
            table_name=definition.name,
            columns=columns,
            rows=pending_rows,
        )
    table_writer.write("\n")
    return {
        "name": definition.name,
        "engine": definition.engine,
        "row_count": row_count,
        "insert_column_count": len(definition.insert_columns),
        "generated_column_count": definition.generated_column_count,
        "sql_sha256": table_writer.digest.hexdigest(),
        "sql_bytes": table_writer.byte_count,
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_path_for(output_path: Path) -> Path:
    if output_path.name.casefold().endswith(".sql.gz"):
        return output_path.with_name(output_path.name[:-7] + ".manifest.json")
    raise MySqlBackupError("backup output must end with .sql.gz")


def _write_manifest_partial(
    manifest_path: Path, manifest: Mapping[str, object]
) -> Path:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{manifest_path.name}.",
        suffix=".partial",
        dir=str(manifest_path.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        encoded = (
            json.dumps(
                manifest,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        return temporary_path
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        temporary_path.unlink(missing_ok=True)
        raise


def _validate_backup_arguments(
    output_path: Path,
    *,
    code_revision: str,
    fetch_size: int,
    batch_rows: int,
    batch_bytes: int,
) -> tuple[Path, Path]:
    if CODE_REVISION_PATTERN.fullmatch(code_revision) is None:
        raise MySqlBackupError(
            "code_revision must be an exact lowercase 40-character Git SHA"
        )
    if fetch_size <= 0 or batch_rows <= 0 or batch_bytes <= 0:
        raise MySqlBackupError("fetch and batch limits must be positive")
    output_path = output_path.expanduser().resolve()
    manifest_path = _manifest_path_for(output_path)
    if not output_path.parent.is_dir():
        raise MySqlBackupError("backup output directory does not exist")
    if output_path.exists() or manifest_path.exists():
        raise MySqlBackupError("backup output or manifest already exists")
    return output_path, manifest_path


def create_mysql_backup(
    options: ConnectionOptions,
    *,
    output_path: Path,
    code_revision: str,
    connect: ConnectFactory = _pymysql_connect,
    stream_cursor_class: object | None = None,
    fetch_size: int = DEFAULT_FETCH_SIZE,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    batch_bytes: int = DEFAULT_BATCH_BYTES,
    now: Callable[[], datetime] | None = None,
) -> BackupResult:
    """Create a fail-closed, streaming SQL+gzip backup and completion manifest."""

    output_path, manifest_path = _validate_backup_arguments(
        output_path,
        code_revision=code_revision,
        fetch_size=fetch_size,
        batch_rows=batch_rows,
        batch_bytes=batch_bytes,
    )
    clock = now or (lambda: datetime.now(timezone.utc))
    started_at = clock().astimezone(timezone.utc)
    dump_connection: _Connection | None = None
    lock_connection: _Connection | None = None
    read_lock_held = False
    backup_partial: Path | None = None
    manifest_partial: Path | None = None
    output_committed = False
    try:
        dump_connection = connect(options, False)
        _assert_no_unsupported_schema_objects(
            dump_connection,
            options.database,
        )
        tables = _list_tables(dump_connection, options.database)
        nontransactional_tables = [
            item for item in tables if item[1].casefold() != "innodb"
        ]
        if nontransactional_tables:
            lock_connection = connect(options, True)
            _execute(lock_connection, "FLUSH TABLES WITH READ LOCK")
            read_lock_held = True
            locked_tables = _list_tables(dump_connection, options.database)
            if locked_tables != tables:
                raise MySqlBackupError(
                    "database table metadata changed while acquiring the read lock"
                )
            _assert_no_unsupported_schema_objects(
                dump_connection,
                options.database,
            )

        # Metadata SELECTs may start an implicit transaction when autocommit is
        # disabled. End it before configuring the deliberate backup snapshot.
        dump_connection.rollback()
        _execute(
            dump_connection,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        _execute(dump_connection, "SET SESSION time_zone = '+00:00'")
        _execute(dump_connection, "SET TRANSACTION READ ONLY")
        _execute(dump_connection, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        definitions = _capture_definitions(
            dump_connection,
            options.database,
            tables,
        )

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{output_path.name}.",
            suffix=".partial",
            dir=str(output_path.parent),
        )
        backup_partial = Path(temporary_name)
        table_manifest: list[dict[str, object]] = []
        with os.fdopen(descriptor, "wb") as raw_handle:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=raw_handle,
                compresslevel=6,
                mtime=0,
            ) as compressed:
                writer = _SqlWriter(compressed)
                writer.write(
                    "-- BSIDE MySQL backup; restore into the intended empty database.\n"
                    "SET NAMES utf8mb4;\n"
                    "SET @BSIDE_OLD_TIME_ZONE=@@SESSION.TIME_ZONE;\n"
                    "SET SESSION TIME_ZONE='+00:00';\n"
                    "SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO';\n"
                    "SET FOREIGN_KEY_CHECKS=0;\n"
                    "SET UNIQUE_CHECKS=0;\n\n"
                )
                ordered_definitions = sorted(
                    definitions,
                    key=lambda definition: (definition.transactional, definition.name),
                )
                for definition in ordered_definitions:
                    table_manifest.append(
                        _dump_table(
                            dump_connection,
                            definition,
                            writer,
                            stream_cursor_class=stream_cursor_class,
                            fetch_size=fetch_size,
                            batch_rows=batch_rows,
                            batch_bytes=batch_bytes,
                        )
                    )
                    if (
                        read_lock_held
                        and definition
                        is ordered_definitions[len(nontransactional_tables) - 1]
                    ):
                        _execute(lock_connection, "UNLOCK TABLES")  # type: ignore[arg-type]
                        read_lock_held = False
                if read_lock_held:
                    _execute(lock_connection, "UNLOCK TABLES")  # type: ignore[arg-type]
                    read_lock_held = False
                writer.write(
                    "SET UNIQUE_CHECKS=1;\n"
                    "SET FOREIGN_KEY_CHECKS=1;\n"
                    "SET SESSION TIME_ZONE=@BSIDE_OLD_TIME_ZONE;\n"
                    "-- BSIDE_BACKUP_COMPLETE\n"
                )
                uncompressed_bytes = writer.total_uncompressed_bytes
                uncompressed_sha256 = writer.uncompressed_digest.hexdigest()
            raw_handle.flush()
            os.fsync(raw_handle.fileno())

        backup_sha256 = _sha256_file(backup_partial)
        backup_bytes = backup_partial.stat().st_size
        completed_at = clock().astimezone(timezone.utc)
        manifest: dict[str, object] = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "status": "completed",
            "format": "mysql-sql-gzip",
            "code_revision": code_revision,
            "started_at": started_at.isoformat().replace("+00:00", "Z"),
            "completed_at": completed_at.isoformat().replace("+00:00", "Z"),
            "backup": {
                "file": output_path.name,
                "sha256": backup_sha256,
                "bytes": backup_bytes,
                "uncompressed_bytes": uncompressed_bytes,
                "uncompressed_sha256": uncompressed_sha256,
            },
            "consistency": {
                "isolation": "REPEATABLE READ",
                "consistent_snapshot": True,
                "global_read_lock_used": bool(nontransactional_tables),
                "locked_engines": sorted(
                    {engine for _, engine in nontransactional_tables},
                    key=str.casefold,
                ),
            },
            "table_count": len(table_manifest),
            "row_count": sum(int(str(table["row_count"])) for table in table_manifest),
            "tables": sorted(table_manifest, key=lambda table: str(table["name"])),
        }
        manifest_partial = _write_manifest_partial(manifest_path, manifest)

        if output_path.exists() or manifest_path.exists():
            raise MySqlBackupError("backup destination changed before commit")
        os.replace(backup_partial, output_path)
        backup_partial = None
        output_committed = True
        os.replace(manifest_partial, manifest_path)
        manifest_partial = None
        return BackupResult(
            output_path=output_path,
            manifest_path=manifest_path,
            manifest=manifest,
        )
    finally:
        if read_lock_held and lock_connection is not None:
            try:
                _execute(lock_connection, "UNLOCK TABLES")
            except Exception:
                pass
        if dump_connection is not None:
            try:
                dump_connection.rollback()
            except Exception:
                pass
            try:
                dump_connection.close()
            except Exception:
                pass
        if lock_connection is not None:
            try:
                lock_connection.close()
            except Exception:
                pass
        if backup_partial is not None:
            backup_partial.unlink(missing_ok=True)
        if manifest_partial is not None:
            manifest_partial.unlink(missing_ok=True)
        if output_committed and not manifest_path.exists():
            output_path.unlink(missing_ok=True)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create a streaming, restoreable MySQL SQL gzip backup. "
            "Connection flags override DB_*/MYSQL_* environment variables."
        )
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--code-revision")
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
    parser.add_argument("--fetch-size", type=int, default=DEFAULT_FETCH_SIZE)
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS)
    parser.add_argument("--batch-bytes", type=int, default=DEFAULT_BATCH_BYTES)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    revision = str(
        args.code_revision
        or os.environ.get("GITHUB_SHA")
        or os.environ.get("BSIDE_CODE_REVISION", "")
    )
    try:
        options = connection_options_from_args(args)
        ssh_options = ssh_options_from_args(args)
        if ssh_options is None:
            result = create_mysql_backup(
                options,
                output_path=args.output,
                code_revision=revision,
                stream_cursor_class=_streaming_cursor_class(),
                fetch_size=args.fetch_size,
                batch_rows=args.batch_rows,
                batch_bytes=args.batch_bytes,
            )
        else:
            with SshDirectTcpipTunnel(
                ssh_options,
                destination_host=options.host,
                destination_port=options.port,
            ) as tunnel:
                if tunnel.local_port is None:
                    raise MySqlBackupError("SSH tunnel did not open a local endpoint")
                tunneled_options = dataclass_replace(
                    options,
                    host=tunnel.local_host,
                    port=tunnel.local_port,
                )
                result = create_mysql_backup(
                    tunneled_options,
                    output_path=args.output,
                    code_revision=revision,
                    stream_cursor_class=_streaming_cursor_class(),
                    fetch_size=args.fetch_size,
                    batch_rows=args.batch_rows,
                    batch_bytes=args.batch_bytes,
                )
    except Exception as error:
        print(
            "MySQL backup failed safely; no completed backup was produced "
            f"({type(error).__name__}).",
            file=sys.stderr,
        )
        return 1
    print(
        "MySQL backup completed: "
        f"tables={result.manifest['table_count']} rows={result.manifest['row_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
