from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import posixpath
import re
import secrets
import socket
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from urllib.parse import quote, unquote, urlsplit

from curator.deployment_manifest import CORE_API_FILES
from curator.mysql_backup import (
    MySqlBackupError,
    SshTunnelOptions,
    _enable_paramiko_legacy_ssh_rsa_sha1,
    legacy_ssh_rsa_sha1_is_allowed,
    normalize_ssh_host_key_sha256,
    verify_ssh_host_key,
)


BACKUP_SCHEMA_VERSION = 1
DEPLOYMENT_MANIFEST_NAME = "deployment-manifest.json"
V1_OPENAPI_NAME = "openapi.yaml"
DEFAULT_REMOTE_ROOT = "/www_root/activist"
DEFAULT_FILE_MODE = 0o644
PRIVATE_FILE_MODE = 0o600
PRIVATE_DIRECTORY_MODE = 0o700
MAX_REMOTE_FILE_BYTES = 32 * 1024 * 1024
SHA1_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
RELEASE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{7,95}$")
ENVIRONMENT_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
OPCACHE_PROBE_NAME_PATTERN = re.compile(
    r"^\.bside-opcache-([0-9a-f]{64})\.php$"
)
EXCLUSIVE_CLAIM_NAME_PATTERN = re.compile(
    r"^\.bside-exclusive-claim-([0-9a-f]{64})$"
)
PRIVATE_CANARY_NAME_PATTERN = re.compile(
    r"^\.bside-private-canary-([0-9a-f]{64})\.txt$"
)
PUBLIC_CANARY_NAME_PATTERN = re.compile(
    r"^bside-public-canary-([0-9a-f]{64})\.txt$"
)

# Dependencies are installed before their consumers. api.php is the last
# executable dependency, and the deployment manifest is always the final
# commit marker. Every file below except the marker is attested by
# CORE_API_FILES.
DEFAULT_COMMIT_ORDER = (
    ".htaccess",
    "migrations/011_global_terminal_v2.sql",
    "openapi-v2.yaml",
    V1_OPENAPI_NAME,
    "governance_v2_write.php",
    "governance_v2.php",
    "governance_v1.php",
    "api.php",
    DEPLOYMENT_MANIFEST_NAME,
)

STAGE_DENY_RULES = (
    b"Options -Indexes\n"
    b"<IfModule mod_authz_core.c>\n"
    b"  Require all denied\n"
    b"</IfModule>\n"
    b"<IfModule !mod_authz_core.c>\n"
    b"  Order allow,deny\n"
    b"  Deny from all\n"
    b"</IfModule>\n"
)
PRIVATE_ROOT_DENY_RULES = STAGE_DENY_RULES
MAX_PRIVATE_POLICY_BYTES = 256 * 1024
CORE_RELEASE_CONFIRMATION_ENV = "BSIDE_CORE_RELEASE_SHA"
CORE_ROLLBACK_RELEASE_ID_ENV = "BSIDE_CORE_ROLLBACK_RELEASE_ID"
CORE_ROLLBACK_CURRENT_SHA_ENV = "BSIDE_CORE_ROLLBACK_CURRENT_SHA"
GABIA_COMPATIBILITY_SSH_HOST = "alignpartnerscap.com"
GABIA_SSH_HOST_KEY_SHA256 = (
    "SHA256:4Y2J13Nis0NOKupLJCOnr2w5X2UdBZH78TkZMVJCVLo"
)
GABIA_REMOTE_ROOT = "/www_root/activist"
GABIA_PUBLIC_URL_ROOT = "https://alignpe.gabia.io/activist"
GABIA_API_V2_BASE_URL = (
    "https://alignpe.gabia.io/activist/api.php/api/v2"
)
GABIA_ROLLBACK_HEALTH_URL = (
    "https://alignpe.gabia.io/activist/api.php?action=health"
)
GABIA_PRIVATE_DENY_REDIRECT = "http://errdoc.gabia.io/403.html"


class PhpDeploymentError(RuntimeError):
    """Raised when a PHP deployment cannot be completed safely."""


class PhpDeploymentRollbackError(PhpDeploymentError):
    """Raised when deployment failed and the automatic rollback was incomplete."""


class SftpClient(Protocol):
    def lstat(self, path: str) -> Any: ...

    def mkdir(self, path: str, mode: int = ...) -> None: ...

    def chmod(self, path: str, mode: int) -> None: ...

    def open(self, path: str, mode: str = ...) -> Any: ...

    def remove(self, path: str) -> None: ...

    def rmdir(self, path: str) -> None: ...

    def rename(self, oldpath: str, newpath: str) -> None: ...

    def posix_rename(self, oldpath: str, newpath: str) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class LocalArtifact:
    relative_path: str
    path: Path
    sha256: str
    size: int
    mode: int = DEFAULT_FILE_MODE


@dataclass(frozen=True)
class LocalDeploymentPlan:
    local_root: Path
    code_revision: str
    artifacts: tuple[LocalArtifact, ...]

    @property
    def artifact_by_path(self) -> Mapping[str, LocalArtifact]:
        return {artifact.relative_path: artifact for artifact in self.artifacts}


@dataclass(frozen=True)
class RemoteFileSnapshot:
    relative_path: str
    existed: bool
    size: int | None
    mode: int | None
    sha256: str | None
    backup_blob: str | None


@dataclass(frozen=True)
class BackupSnapshot:
    release_id: str
    backup_directory: str
    remote_root: str
    candidate_code_revision: str
    files: tuple[RemoteFileSnapshot, ...]

    @property
    def file_by_path(self) -> Mapping[str, RemoteFileSnapshot]:
        return {item.relative_path: item for item in self.files}


@dataclass(frozen=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes

    def header(self, name: str) -> str:
        expected = name.casefold()
        for key, value in self.headers.items():
            if key.casefold() == expected:
                return value
        return ""


HttpRequester = Callable[
    [str, str, Mapping[str, str], float],
    HttpResponse,
]


class ExclusiveBytesWriter(Protocol):
    def __call__(
        self,
        client: SftpClient,
        path: str,
        content: bytes,
        *,
        mode: int,
    ) -> None: ...


@dataclass(frozen=True)
class GabiaCompatibilityEvidence:
    exclusive_writer_incompatible: bool
    private_mode_0700_directory: bool
    write_readback_verified: bool
    absent_target_rename_verified: bool
    no_replace_collision_verified: bool
    probe_residue_absent: bool


@dataclass(frozen=True)
class GabiaCoreCompatibility:
    """Runtime-bound evidence for the one supported Gabia deployment."""

    client: SftpClient
    ssh_host: str
    ssh_host_key_sha256: str
    remote_root: str
    public_url_root: str
    api_v2_base_url: str
    rollback_health_url: str
    current_release_sha: str | None
    private_policy: bytes = field(repr=False)
    private_policy_mode: int
    evidence: GabiaCompatibilityEvidence
    exclusive_writer: ExclusiveBytesWriter


def _safe_relative_path(value: str) -> str:
    if (
        not value
        or "\\" in value
        or "\x00" in value
        or value.startswith("/")
    ):
        raise PhpDeploymentError("deployment file path is not canonical")
    candidate = PurePosixPath(value)
    if (
        candidate.as_posix() != value
        or any(part in {"", ".", ".."} for part in candidate.parts)
    ):
        raise PhpDeploymentError("deployment file path is not canonical")
    return value


def _remote_absolute_path(value: str, *, label: str) -> str:
    if not value or "\\" in value or "\x00" in value or not value.startswith("/"):
        raise PhpDeploymentError(f"{label} must be a canonical absolute path")
    normalized = posixpath.normpath(value)
    if normalized != value.rstrip("/") or normalized == "/":
        raise PhpDeploymentError(f"{label} must be a canonical absolute path")
    if any(part in {".", ".."} for part in PurePosixPath(normalized).parts):
        raise PhpDeploymentError(f"{label} must be a canonical absolute path")
    return normalized


def _remote_join(root: str, relative: str) -> str:
    safe_root = _remote_absolute_path(root, label="remote root")
    safe_relative = _safe_relative_path(relative)
    joined = posixpath.normpath(posixpath.join(safe_root, safe_relative))
    if not joined.startswith(safe_root + "/"):
        raise PhpDeploymentError("remote path escapes its configured root")
    return joined


def _remote_child(parent: str, child: str, *, label: str) -> str:
    safe_parent = _remote_absolute_path(parent, label="remote parent")
    safe_child = _remote_absolute_path(child, label=label)
    if not safe_child.startswith(safe_parent + "/"):
        raise PhpDeploymentError(f"{label} must remain below {safe_parent}")
    return safe_child


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _read_local_artifact(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise PhpDeploymentError("deployment artifacts must be regular files")
    size = path.stat().st_size
    if size < 1 or size > MAX_REMOTE_FILE_BYTES:
        raise PhpDeploymentError("deployment artifact size is outside the safe limit")
    return path.read_bytes()


def build_local_deployment_plan(
    local_root: Path,
    *,
    expected_sha: str,
) -> LocalDeploymentPlan:
    if SHA1_PATTERN.fullmatch(expected_sha) is None:
        raise PhpDeploymentError(
            "expected release SHA must be 40 lowercase hexadecimal characters"
        )
    if local_root.is_symlink() or not local_root.is_dir():
        raise PhpDeploymentError("local deployment root must be a regular directory")
    root = local_root.resolve()
    manifest_path = root / DEPLOYMENT_MANIFEST_NAME
    manifest_bytes = _read_local_artifact(manifest_path)
    try:
        manifest = json.loads(manifest_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PhpDeploymentError("deployment manifest is invalid") from error
    if (
        not isinstance(manifest, dict)
        or set(manifest) != {"schema_version", "code_revision", "files"}
        or manifest.get("schema_version") != 1
        or manifest.get("code_revision") != expected_sha
        or not isinstance(manifest.get("files"), dict)
    ):
        raise PhpDeploymentError("deployment manifest identity does not match")
    manifest_files = manifest["files"]
    if set(manifest_files) != set(CORE_API_FILES):
        raise PhpDeploymentError("deployment manifest core file set is invalid")

    artifacts: list[LocalArtifact] = []
    for relative_path in DEFAULT_COMMIT_ORDER:
        _safe_relative_path(relative_path)
        candidate = root.joinpath(*PurePosixPath(relative_path).parts)
        try:
            candidate.resolve().relative_to(root)
        except ValueError as error:
            raise PhpDeploymentError("deployment artifact escapes local root") from error
        content = (
            manifest_bytes
            if relative_path == DEPLOYMENT_MANIFEST_NAME
            else _read_local_artifact(candidate)
        )
        digest = _sha256_bytes(content)
        if relative_path in CORE_API_FILES:
            expected_digest = manifest_files.get(relative_path)
            if (
                not isinstance(expected_digest, str)
                or SHA256_PATTERN.fullmatch(expected_digest) is None
                or digest != expected_digest
            ):
                raise PhpDeploymentError(
                    "deployment artifact does not match its manifest"
                )
        artifacts.append(
            LocalArtifact(
                relative_path=relative_path,
                path=candidate,
                sha256=digest,
                size=len(content),
            )
        )
    if artifacts[-1].relative_path != DEPLOYMENT_MANIFEST_NAME:
        raise PhpDeploymentError("deployment manifest must be the final commit marker")
    if artifacts[-2].relative_path != "api.php":
        raise PhpDeploymentError("api.php must be the final executable dependency")
    return LocalDeploymentPlan(
        local_root=root,
        code_revision=expected_sha,
        artifacts=tuple(artifacts),
    )


def _find_repository_root(path: Path) -> Path:
    current = path.resolve()
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    raise PhpDeploymentError("deployment root is not inside a Git checkout")


def _git_output(repository: Path, arguments: Sequence[str]) -> bytes:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repository), *arguments],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise PhpDeploymentError("release checkout Git verification failed") from error
    return completed.stdout


def verify_release_checkout(
    plan: LocalDeploymentPlan,
    *,
    require_repository_clean: bool = False,
) -> None:
    repository = _find_repository_root(plan.local_root)
    head = _git_output(repository, ("rev-parse", "HEAD")).decode("ascii").strip()
    if head != plan.code_revision:
        raise PhpDeploymentError("release checkout HEAD does not match the manifest")
    if require_repository_clean:
        status = _git_output(
            repository,
            ("status", "--porcelain=v1", "--untracked-files=all"),
        )
        if status:
            raise PhpDeploymentError(
                "production release checkout is not clean"
            )
    relative_root = plan.local_root.relative_to(repository)
    tracked_paths = [
        (relative_root / artifact.relative_path).as_posix()
        for artifact in plan.artifacts
        if artifact.relative_path != DEPLOYMENT_MANIFEST_NAME
    ]
    status = _git_output(
        repository,
        ("status", "--porcelain=v1", "--untracked-files=no", "--", *tracked_paths),
    )
    if status:
        raise PhpDeploymentError("release artifacts contain tracked modifications")
    for tracked_path in tracked_paths:
        _git_output(repository, ("ls-files", "--error-unmatch", "--", tracked_path))


def confirm_production_release(
    expected_sha: str,
    confirmation: object,
    *,
    environ: Mapping[str, str] | None = None,
) -> None:
    """Require independent argument and environment confirmation."""

    if SHA1_PATTERN.fullmatch(expected_sha) is None:
        raise PhpDeploymentError("production release SHA is invalid")
    environment = os.environ if environ is None else environ
    environment_confirmation = environment.get(
        CORE_RELEASE_CONFIRMATION_ENV
    )
    if (
        not isinstance(confirmation, str)
        or confirmation != expected_sha
        or environment_confirmation != expected_sha
    ):
        raise PhpDeploymentError(
            "production release confirmation does not match"
        )


def confirm_production_rollback(
    release_id: str,
    release_id_confirmation: object,
    expected_current_sha: str,
    current_sha_confirmation: object,
    *,
    environ: Mapping[str, str] | None = None,
) -> None:
    """Bind a mutating rollback to one backup and current release."""

    safe_release_id = _validate_release_id(release_id)
    if SHA1_PATTERN.fullmatch(expected_current_sha) is None:
        raise PhpDeploymentError(
            "rollback current release SHA is invalid"
        )
    environment = os.environ if environ is None else environ
    if (
        not isinstance(release_id_confirmation, str)
        or release_id_confirmation != safe_release_id
        or environment.get(CORE_ROLLBACK_RELEASE_ID_ENV)
        != safe_release_id
        or not isinstance(current_sha_confirmation, str)
        or current_sha_confirmation != expected_current_sha
        or environment.get(CORE_ROLLBACK_CURRENT_SHA_ENV)
        != expected_current_sha
    ):
        raise PhpDeploymentError(
            "production rollback confirmation does not match"
        )


def _boolean_environment(value: str | None, *, label: str) -> bool:
    if value is None or value.strip() == "":
        return False
    normalized = value.strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise PhpDeploymentError(f"{label} must be a boolean")


def _first_environment(
    environ: Mapping[str, str],
    *names: str,
) -> str | None:
    for name in names:
        value = environ.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def ssh_sftp_options_from_args(
    args: argparse.Namespace,
    *,
    environ: Mapping[str, str] | None = None,
) -> SshTunnelOptions:
    environment = os.environ if environ is None else environ
    host = args.ssh_host or _first_environment(
        environment,
        "SSH_HOST",
        "GABIA_SSH_HOST",
    )
    user = args.ssh_user or _first_environment(
        environment,
        "SSH_USER",
        "SSH_USERNAME",
        "GABIA_SSH_USER",
    )
    fingerprint = args.ssh_host_key_sha256 or _first_environment(
        environment,
        "SSH_HOST_KEY_SHA256",
        "GABIA_SSH_HOST_KEY_SHA256",
    )
    password_environment = str(args.ssh_password_env or "SSH_PASSWORD")
    if ENVIRONMENT_NAME_PATTERN.fullmatch(password_environment) is None:
        raise PhpDeploymentError("SSH password environment name is invalid")
    password = environment.get(password_environment)
    if password is None and password_environment == "SSH_PASSWORD":
        password = environment.get("GABIA_SSH_PASSWORD")
    if not host:
        raise PhpDeploymentError("SSH host is required")
    if not user:
        raise PhpDeploymentError("SSH user is required")
    if not password:
        raise PhpDeploymentError("SSH password environment variable is missing")
    if not fingerprint:
        raise PhpDeploymentError("pinned SSH host fingerprint is required")
    raw_port = args.ssh_port or _first_environment(
        environment,
        "SSH_PORT",
        "GABIA_SSH_PORT",
    )
    try:
        port = int(raw_port or "22")
    except ValueError as error:
        raise PhpDeploymentError("SSH port must be an integer") from error
    if port < 1 or port > 65535:
        raise PhpDeploymentError("SSH port is outside the valid range")
    allow_legacy = bool(args.ssh_allow_legacy_rsa_sha1) or _boolean_environment(
        _first_environment(
            environment,
            "SSH_ALLOW_LEGACY_RSA_SHA1",
            "GABIA_SSH_ALLOW_LEGACY_RSA_SHA1",
        ),
        label="legacy ssh-rsa/SHA-1 opt-in",
    )
    legacy_host = args.ssh_legacy_rsa_sha1_host or _first_environment(
        environment,
        "SSH_LEGACY_RSA_SHA1_HOST",
        "GABIA_SSH_LEGACY_RSA_SHA1_HOST",
    )
    try:
        normalized_fingerprint = normalize_ssh_host_key_sha256(fingerprint)
    except MySqlBackupError as error:
        raise PhpDeploymentError("pinned SSH host fingerprint is invalid") from error
    options = SshTunnelOptions(
        host=host,
        port=port,
        user=user,
        password=password,
        host_key_sha256=normalized_fingerprint,
        connect_timeout=int(args.connect_timeout),
        auth_timeout=int(args.auth_timeout),
        allow_legacy_ssh_rsa_sha1=allow_legacy,
        legacy_ssh_rsa_sha1_host=legacy_host or None,
    )
    try:
        legacy_ssh_rsa_sha1_is_allowed(options)
    except MySqlBackupError as error:
        raise PhpDeploymentError(
            "legacy ssh-rsa/SHA-1 policy is invalid"
        ) from error
    return options


class ParamikoPinnedSftpSession:
    """Pinned, password-authenticated SFTP session without trust-on-first-use."""

    def __init__(self, options: SshTunnelOptions) -> None:
        self.options = options
        self._socket: socket.socket | None = None
        self._transport: Any = None
        self._sftp: Any = None

    def __enter__(self) -> SftpClient:
        try:
            import paramiko  # type: ignore
        except ImportError as error:  # pragma: no cover - declared dependency
            raise PhpDeploymentError("Paramiko is required for SFTP deployment") from error
        try:
            try:
                allow_legacy = legacy_ssh_rsa_sha1_is_allowed(self.options)
            except MySqlBackupError as error:
                raise PhpDeploymentError(
                    "legacy ssh-rsa/SHA-1 policy is invalid"
                ) from error
            self._socket = socket.create_connection(
                (self.options.host, self.options.port),
                timeout=self.options.connect_timeout,
            )
            self._transport = paramiko.Transport(
                self._socket,
                disabled_algorithms=(
                    None if allow_legacy else {"keys": ["ssh-rsa"]}
                ),
            )
            if allow_legacy:
                try:
                    _enable_paramiko_legacy_ssh_rsa_sha1(
                        self._transport,
                        paramiko,
                    )
                except MySqlBackupError as error:
                    raise PhpDeploymentError(
                        "legacy ssh-rsa/SHA-1 compatibility is unavailable"
                    ) from error
            self._transport.auth_timeout = self.options.auth_timeout
            self._transport.start_client(timeout=self.options.connect_timeout)
            server_key = self._transport.get_remote_server_key()
            try:
                verify_ssh_host_key(
                    server_key.asbytes(),
                    self.options.host_key_sha256,
                )
            except MySqlBackupError as error:
                raise PhpDeploymentError(
                    "SSH server host key does not match the pinned fingerprint"
                ) from error
            self._transport.auth_password(
                username=self.options.user,
                password=self.options.password,
                fallback=False,
            )
            if not self._transport.is_authenticated():
                raise PhpDeploymentError("SSH authentication did not complete")
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            if self._sftp is None:
                raise PhpDeploymentError("SFTP subsystem did not start")
            return self._sftp
        except PhpDeploymentError:
            self.close()
            raise
        except Exception as error:
            self.close()
            raise PhpDeploymentError(
                "SSH/SFTP session establishment failed"
            ) from error

    def close(self) -> None:
        if self._sftp is not None:
            try:
                self._sftp.close()
            except Exception:
                pass
            self._sftp = None
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception:
                pass
            self._transport = None
        if self._socket is not None:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None

    def __exit__(self, *_args: object) -> None:
        self.close()


def _is_not_found(error: OSError) -> bool:
    return getattr(error, "errno", None) in {errno.ENOENT, 2}


def _lstat_or_none(client: SftpClient, path: str) -> Any | None:
    try:
        return client.lstat(path)
    except OSError as error:
        if _is_not_found(error):
            return None
        raise PhpDeploymentError("remote metadata read failed") from error


def _mode(attributes: Any) -> int:
    value = getattr(attributes, "st_mode", None)
    if not isinstance(value, int):
        raise PhpDeploymentError("remote file mode is unavailable")
    return value


def _require_remote_directory(
    client: SftpClient,
    path: str,
    *,
    create: bool,
    mode: int = PRIVATE_DIRECTORY_MODE,
) -> None:
    safe_path = _remote_absolute_path(path, label="remote directory")
    current = ""
    for part in PurePosixPath(safe_path).parts:
        if part == "/":
            current = "/"
            continue
        current = posixpath.join(current, part)
        attributes = _lstat_or_none(client, current)
        if attributes is None:
            if not create:
                raise PhpDeploymentError("required remote directory is missing")
            try:
                client.mkdir(current, mode=mode)
            except OSError as error:
                raise PhpDeploymentError("remote directory creation failed") from error
            attributes = _lstat_or_none(client, current)
        current_mode = _mode(attributes)
        if stat.S_ISLNK(current_mode) or not stat.S_ISDIR(current_mode):
            raise PhpDeploymentError("remote path contains a non-directory component")


def _read_remote_bytes(
    client: SftpClient,
    path: str,
    *,
    maximum_bytes: int = MAX_REMOTE_FILE_BYTES,
) -> bytes:
    attributes = _lstat_or_none(client, path)
    if attributes is None:
        raise PhpDeploymentError("required remote file is missing")
    remote_mode = _mode(attributes)
    if stat.S_ISLNK(remote_mode) or not stat.S_ISREG(remote_mode):
        raise PhpDeploymentError("remote deployment target is not a regular file")
    declared_size = getattr(attributes, "st_size", None)
    if isinstance(declared_size, int) and (
        declared_size < 0 or declared_size > maximum_bytes
    ):
        raise PhpDeploymentError("remote file exceeds the safe size limit")
    chunks: list[bytes] = []
    total = 0
    try:
        with client.open(path, "rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    chunk = bytes(chunk)
                total += len(chunk)
                if total > maximum_bytes:
                    raise PhpDeploymentError(
                        "remote file exceeds the safe size limit"
                    )
                chunks.append(chunk)
    except PhpDeploymentError:
        raise
    except OSError as error:
        raise PhpDeploymentError("remote file read failed") from error
    content = b"".join(chunks)
    if isinstance(declared_size, int) and len(content) != declared_size:
        raise PhpDeploymentError("remote file size changed during read")
    return content


def _write_remote_bytes(
    client: SftpClient,
    path: str,
    content: bytes,
    *,
    mode: int,
    require_absent: bool = True,
) -> None:
    if require_absent and _lstat_or_none(client, path) is not None:
        raise PhpDeploymentError("remote staging file unexpectedly exists")
    try:
        with client.open(path, "wb") as handle:
            handle.write(content)
            handle.flush()
        client.chmod(path, mode)
    except OSError as error:
        raise PhpDeploymentError("remote file upload failed") from error
    readback = _read_remote_bytes(client, path)
    if not secrets.compare_digest(_sha256_bytes(readback), _sha256_bytes(content)):
        raise PhpDeploymentError("remote upload readback hash mismatch")


def _write_remote_exclusive_bytes(
    client: SftpClient,
    path: str,
    content: bytes,
    *,
    mode: int,
) -> None:
    """Create a new remote file without replacing a concurrent writer."""

    if _lstat_or_none(client, path) is not None:
        raise PhpDeploymentError("exclusive remote target already exists")
    created = False
    operation_error: BaseException | None = None
    cleanup_error: BaseException | None = None
    try:
        handle = client.open(path, "x")
        created = True
        with handle:
            handle.write(content)
            handle.flush()
        client.chmod(path, mode)
        readback = _read_remote_bytes(client, path)
        if not secrets.compare_digest(
            _sha256_bytes(readback),
            _sha256_bytes(content),
        ):
            raise PhpDeploymentError(
                "exclusive remote upload readback hash mismatch"
            )
    except BaseException as error:
        operation_error = error
    if operation_error is None:
        return
    if created:
        try:
            _remove_remote_file_if_present(client, path)
            if _lstat_or_none(client, path) is not None:
                raise PhpDeploymentError(
                    "exclusive remote upload cleanup failed"
                )
        except BaseException as error:
            cleanup_error = error
    if cleanup_error is not None:
        raise PhpDeploymentError(
            "exclusive remote upload failed and cleanup did not complete"
        ) from cleanup_error
    if isinstance(operation_error, PhpDeploymentError):
        raise operation_error
    raise PhpDeploymentError("exclusive remote upload failed") from operation_error


def _regular_file_identity_matches(
    client: SftpClient,
    path: str,
    content: bytes,
    *,
    mode: int,
) -> bool:
    attributes = _lstat_or_none(client, path)
    if attributes is None:
        return False
    current_mode = _mode(attributes)
    if (
        stat.S_ISLNK(current_mode)
        or not stat.S_ISREG(current_mode)
        or stat.S_IMODE(current_mode) != mode
    ):
        return False
    try:
        actual = _read_remote_bytes(client, path)
    except Exception:
        return False
    return secrets.compare_digest(
        _sha256_bytes(actual),
        _sha256_bytes(content),
    )


def _write_wb_and_verify(
    client: SftpClient,
    path: str,
    content: bytes,
    *,
    mode: int,
) -> None:
    if _lstat_or_none(client, path) is not None:
        raise PhpDeploymentError(
            "compatibility staging target unexpectedly exists"
        )
    try:
        with client.open(path, "wb") as handle:
            handle.write(content)
            handle.flush()
        client.chmod(path, mode)
    except OSError as error:
        raise PhpDeploymentError(
            "compatibility staging write failed"
        ) from error
    if not _regular_file_identity_matches(
        client,
        path,
        content,
        mode=mode,
    ):
        raise PhpDeploymentError(
            "compatibility staging readback did not match"
        )


def _standard_no_replace_rename(
    client: SftpClient,
    source: str,
    target: str,
) -> None:
    rename = getattr(client, "rename", None)
    if not callable(rename):
        raise PhpDeploymentError(
            "standard SFTP rename capability is unavailable"
        )
    try:
        rename(source, target)
    except OSError:
        raise
    except Exception as error:
        raise PhpDeploymentError(
            "standard SFTP rename capability failed"
        ) from error


def _claim_directory_path(parent: str) -> str:
    filename = f".bside-exclusive-claim-{secrets.token_hex(32)}"
    if EXCLUSIVE_CLAIM_NAME_PATTERN.fullmatch(filename) is None:
        raise PhpDeploymentError(
            "compatibility claim directory name is invalid"
        )
    return _remote_join(parent, filename)


def _create_private_claim_directory(
    client: SftpClient,
    parent: str,
) -> str:
    safe_parent = _remote_absolute_path(parent, label="claim parent")
    _require_remote_directory(client, safe_parent, create=False)
    claim_path = _claim_directory_path(safe_parent)
    if _lstat_or_none(client, claim_path) is not None:
        raise PhpDeploymentError(
            "compatibility claim directory unexpectedly exists"
        )
    try:
        client.mkdir(claim_path, mode=PRIVATE_DIRECTORY_MODE)
    except OSError as error:
        raise PhpDeploymentError(
            "compatibility claim directory creation failed"
        ) from error
    attributes = _lstat_or_none(client, claim_path)
    if attributes is None:
        raise PhpDeploymentError(
            "compatibility claim directory was not created"
        )
    current_mode = _mode(attributes)
    if (
        stat.S_ISLNK(current_mode)
        or not stat.S_ISDIR(current_mode)
        or stat.S_IMODE(current_mode) != PRIVATE_DIRECTORY_MODE
    ):
        try:
            client.rmdir(claim_path)
        except OSError as error:
            raise PhpDeploymentError(
                "non-private compatibility claim cleanup failed"
            ) from error
        if _lstat_or_none(client, claim_path) is not None:
            raise PhpDeploymentError(
                "non-private compatibility claim cleanup was not durable"
            )
        raise PhpDeploymentError(
            "compatibility claim directory is not private"
        )
    return claim_path


def _remove_claim_file_if_present(
    client: SftpClient,
    *,
    claim_path: str,
    file_path: str,
) -> None:
    attributes = _lstat_or_none(client, file_path)
    if attributes is None:
        return
    claim_attributes = _lstat_or_none(client, claim_path)
    if claim_attributes is None:
        raise PhpDeploymentError(
            "compatibility claim ownership cannot be proven"
        )
    claim_mode = _mode(claim_attributes)
    file_mode = _mode(attributes)
    if (
        stat.S_ISLNK(claim_mode)
        or not stat.S_ISDIR(claim_mode)
        or stat.S_IMODE(claim_mode) != PRIVATE_DIRECTORY_MODE
        or stat.S_ISLNK(file_mode)
        or not stat.S_ISREG(file_mode)
        or posixpath.dirname(file_path) != claim_path
    ):
        raise PhpDeploymentError(
            "compatibility staging ownership cannot be proven"
        )
    try:
        client.remove(file_path)
    except OSError as error:
        raise PhpDeploymentError(
            "compatibility staging cleanup failed"
        ) from error
    if _lstat_or_none(client, file_path) is not None:
        raise PhpDeploymentError(
            "compatibility staging cleanup was not durable"
        )


def _remove_claim_directory(
    client: SftpClient,
    claim_path: str,
) -> None:
    attributes = _lstat_or_none(client, claim_path)
    if attributes is None:
        return
    current_mode = _mode(attributes)
    if (
        stat.S_ISLNK(current_mode)
        or not stat.S_ISDIR(current_mode)
        or stat.S_IMODE(current_mode) != PRIVATE_DIRECTORY_MODE
    ):
        raise PhpDeploymentError(
            "compatibility claim ownership cannot be proven"
        )
    try:
        client.rmdir(claim_path)
    except OSError as error:
        raise PhpDeploymentError(
            "compatibility claim cleanup failed"
        ) from error
    if _lstat_or_none(client, claim_path) is not None:
        raise PhpDeploymentError(
            "compatibility claim cleanup was not durable"
        )


def _remove_exact_file_if_present(
    client: SftpClient,
    path: str,
    content: bytes,
    *,
    mode: int,
) -> None:
    if _lstat_or_none(client, path) is None:
        return
    if not _regular_file_identity_matches(
        client,
        path,
        content,
        mode=mode,
    ):
        raise PhpDeploymentError(
            "compatibility target ownership cannot be proven"
        )
    try:
        client.remove(path)
    except OSError as error:
        raise PhpDeploymentError(
            "compatibility target cleanup failed"
        ) from error
    if _lstat_or_none(client, path) is not None:
        raise PhpDeploymentError(
            "compatibility target cleanup was not durable"
        )


def _gabia_compatibility_writer(
    bound_client: SftpClient,
    *,
    allowed_root: str,
) -> ExclusiveBytesWriter:
    safe_allowed_root = _remote_absolute_path(
        allowed_root,
        label="compatibility root",
    )

    def write(
        client: SftpClient,
        path: str,
        content: bytes,
        *,
        mode: int,
    ) -> None:
        if client is not bound_client:
            raise PhpDeploymentError(
                "compatibility writer is bound to a different SFTP session"
            )
        if not isinstance(content, bytes) or mode < 0 or mode > 0o777:
            raise PhpDeploymentError(
                "compatibility writer input is invalid"
            )
        safe_path = _remote_absolute_path(path, label="exclusive target")
        if not safe_path.startswith(safe_allowed_root + "/"):
            raise PhpDeploymentError(
                "compatibility target escapes the deployment root"
            )
        parent = posixpath.dirname(safe_path)
        _require_remote_directory(client, parent, create=False)
        if _lstat_or_none(client, safe_path) is not None:
            raise PhpDeploymentError(
                "exclusive remote target already exists"
            )

        claim_path = _create_private_claim_directory(client, parent)
        stage_path = _remote_join(claim_path, "payload.blob")
        committed = False
        operation_error: BaseException | None = None
        cleanup_errors: list[BaseException] = []
        try:
            _write_wb_and_verify(
                client,
                stage_path,
                content,
                mode=mode,
            )
            if _lstat_or_none(client, safe_path) is not None:
                raise PhpDeploymentError(
                    "exclusive remote target appeared before commit"
                )
            _standard_no_replace_rename(client, stage_path, safe_path)
            committed = True
            if _lstat_or_none(client, stage_path) is not None:
                raise PhpDeploymentError(
                    "compatibility rename left its staging file"
                )
            if not _regular_file_identity_matches(
                client,
                safe_path,
                content,
                mode=mode,
            ):
                raise PhpDeploymentError(
                    "compatibility committed file did not match"
                )
            _remove_claim_directory(client, claim_path)
            return
        except BaseException as error:
            operation_error = error

        target_is_ours = (
            committed
            and _regular_file_identity_matches(
                client,
                safe_path,
                content,
                mode=mode,
            )
            and _lstat_or_none(client, stage_path) is None
        )
        if target_is_ours:
            try:
                _remove_exact_file_if_present(
                    client,
                    safe_path,
                    content,
                    mode=mode,
                )
            except BaseException as error:
                cleanup_errors.append(error)
        try:
            _remove_claim_file_if_present(
                client,
                claim_path=claim_path,
                file_path=stage_path,
            )
        except BaseException as error:
            cleanup_errors.append(error)
        try:
            _remove_claim_directory(client, claim_path)
        except BaseException as error:
            cleanup_errors.append(error)
        if cleanup_errors:
            raise PhpDeploymentError(
                "compatibility upload failed and cleanup did not complete"
            ) from cleanup_errors[-1]
        if isinstance(operation_error, PhpDeploymentError):
            raise operation_error
        raise PhpDeploymentError(
            "compatibility exclusive upload failed"
        ) from operation_error

    return write


def _is_confirmed_gabia_exclusive_writer_error(error: OSError) -> bool:
    return (
        type(error) is OSError
        and getattr(error, "errno", None) is None
        and getattr(error, "code", None) is None
        and error.args == ("File not open for writing",)
        and str(error) == "File not open for writing"
    )


def _probe_gabia_sftp_capabilities(
    client: SftpClient,
    *,
    private_root: str,
) -> GabiaCompatibilityEvidence:
    safe_private_root = _remote_absolute_path(
        private_root,
        label="private root",
    )
    _require_remote_directory(client, safe_private_root, create=False)
    probe_name = f".bside-exclusive-x-probe-{secrets.token_hex(32)}.blob"
    probe_path = _remote_join(safe_private_root, probe_name)
    probe_content = b"bside-exclusive-writer-capability-probe"
    if _lstat_or_none(client, probe_path) is not None:
        raise PhpDeploymentError(
            "exclusive-writer probe path unexpectedly exists"
        )
    handle_opened = False
    write_attempted = False
    write_completed = False
    operation_error: BaseException | None = None
    try:
        handle = client.open(probe_path, "x")
        handle_opened = True
        with handle:
            write_attempted = True
            handle.write(probe_content)
            write_completed = True
            handle.flush()
        client.chmod(probe_path, PRIVATE_FILE_MODE)
    except BaseException as error:
        operation_error = error

    exclusive_writer_incompatible = False
    if handle_opened:
        attributes = _lstat_or_none(client, probe_path)
        if attributes is None:
            raise PhpDeploymentError(
                "exclusive-writer handle left no attributable probe file"
            ) from operation_error
        current_mode = _mode(attributes)
        residue_matches_gabia = (
            write_attempted
            and not write_completed
            and isinstance(operation_error, OSError)
            and _is_confirmed_gabia_exclusive_writer_error(
                operation_error
            )
            and not stat.S_ISLNK(current_mode)
            and stat.S_ISREG(current_mode)
            and getattr(attributes, "st_size", None) == 0
            and stat.S_IMODE(current_mode) == DEFAULT_FILE_MODE
            and _read_remote_bytes(
                client,
                probe_path,
                maximum_bytes=1,
            )
            == b""
        )
        if residue_matches_gabia:
            _remove_exact_file_if_present(
                client,
                probe_path,
                b"",
                mode=DEFAULT_FILE_MODE,
            )
            exclusive_writer_incompatible = True
            operation_error = None
        if operation_error is not None:
            raise PhpDeploymentError(
                "exclusive-writer capability probe failed"
            ) from operation_error
        if not exclusive_writer_incompatible:
            _remove_exact_file_if_present(
                client,
                probe_path,
                probe_content,
                mode=PRIVATE_FILE_MODE,
            )
            raise PhpDeploymentError(
                "exclusive-writer compatibility is not required"
            )
    if operation_error is not None:
        raise PhpDeploymentError(
            "exclusive-writer capability probe failed"
        ) from operation_error
    if not exclusive_writer_incompatible:
        raise PhpDeploymentError(
            "exclusive-writer incompatibility was not established"
        )
    if _lstat_or_none(client, probe_path) is not None:
        raise PhpDeploymentError(
            "exclusive-writer probe produced unattributed remote residue"
        )

    claim_path = _create_private_claim_directory(
        client,
        safe_private_root,
    )
    probe_identity = secrets.token_hex(32)
    source_path = _remote_join(claim_path, "rename-source.blob")
    target_path = _remote_join(
        safe_private_root,
        f".bside-cross-dir-target-{probe_identity}.blob",
    )
    collision_source = _remote_join(
        claim_path,
        "collision-source.blob",
    )
    collision_target = _remote_join(
        safe_private_root,
        f".bside-cross-dir-collision-{probe_identity}.blob",
    )
    source_content = b"bside-standard-rename-source"
    collision_source_content = b"bside-collision-source"
    collision_target_content = b"bside-collision-target"
    capability_error: BaseException | None = None
    cleanup_errors: list[BaseException] = []
    evidence: GabiaCompatibilityEvidence | None = None
    try:
        try:
            client.mkdir(claim_path, mode=PRIVATE_DIRECTORY_MODE)
        except OSError:
            pass
        else:
            raise PhpDeploymentError(
                "claim directory creation is not exclusive"
            )
        _write_wb_and_verify(
            client,
            source_path,
            source_content,
            mode=PRIVATE_FILE_MODE,
        )
        _standard_no_replace_rename(client, source_path, target_path)
        if (
            _lstat_or_none(client, source_path) is not None
            or not _regular_file_identity_matches(
                client,
                target_path,
                source_content,
                mode=PRIVATE_FILE_MODE,
            )
        ):
            raise PhpDeploymentError(
                "target-absent standard rename did not match"
            )
        _write_wb_and_verify(
            client,
            collision_source,
            collision_source_content,
            mode=PRIVATE_FILE_MODE,
        )
        _write_wb_and_verify(
            client,
            collision_target,
            collision_target_content,
            mode=PRIVATE_FILE_MODE,
        )
        try:
            _standard_no_replace_rename(
                client,
                collision_source,
                collision_target,
            )
        except OSError:
            pass
        else:
            raise PhpDeploymentError(
                "standard rename can replace an existing target"
            )
        if (
            not _regular_file_identity_matches(
                client,
                collision_source,
                collision_source_content,
                mode=PRIVATE_FILE_MODE,
            )
            or not _regular_file_identity_matches(
                client,
                collision_target,
                collision_target_content,
                mode=PRIVATE_FILE_MODE,
            )
        ):
            raise PhpDeploymentError(
                "rename collision did not preserve both files"
            )
        evidence = GabiaCompatibilityEvidence(
            exclusive_writer_incompatible=True,
            private_mode_0700_directory=True,
            write_readback_verified=True,
            absent_target_rename_verified=True,
            no_replace_collision_verified=True,
            probe_residue_absent=True,
        )
    except BaseException as error:
        capability_error = error
    finally:
        for claim_file in (source_path, collision_source):
            try:
                _remove_claim_file_if_present(
                    client,
                    claim_path=claim_path,
                    file_path=claim_file,
                )
            except BaseException as error:
                cleanup_errors.append(error)
        for target, possible_contents in (
            (target_path, (source_content,)),
            (
                collision_target,
                (collision_target_content, collision_source_content),
            ),
        ):
            try:
                if _lstat_or_none(client, target) is None:
                    continue
                matching_content = next(
                    (
                        candidate
                        for candidate in possible_contents
                        if _regular_file_identity_matches(
                            client,
                            target,
                            candidate,
                            mode=PRIVATE_FILE_MODE,
                        )
                    ),
                    None,
                )
                if matching_content is None:
                    raise PhpDeploymentError(
                        "SFTP capability probe target ownership is unclear"
                    )
                _remove_exact_file_if_present(
                    client,
                    target,
                    matching_content,
                    mode=PRIVATE_FILE_MODE,
                )
            except BaseException as error:
                cleanup_errors.append(error)
        try:
            _remove_claim_directory(client, claim_path)
        except BaseException as error:
            cleanup_errors.append(error)
    if cleanup_errors:
        raise PhpDeploymentError(
            "SFTP capability probe cleanup did not complete"
        ) from cleanup_errors[-1]
    if capability_error is not None:
        if isinstance(capability_error, PhpDeploymentError):
            raise capability_error
        raise PhpDeploymentError(
            "SFTP capability probe failed"
        ) from capability_error
    if evidence is None:
        raise PhpDeploymentError(
            "SFTP capability probe did not produce evidence"
        )
    return evidence


def _remove_remote_file_if_present(client: SftpClient, path: str) -> None:
    attributes = _lstat_or_none(client, path)
    if attributes is None:
        return
    current_mode = _mode(attributes)
    if stat.S_ISLNK(current_mode) or not stat.S_ISREG(current_mode):
        raise PhpDeploymentError("refusing to remove a non-regular remote path")
    try:
        client.remove(path)
    except OSError as error:
        raise PhpDeploymentError("remote file removal failed") from error
    if _lstat_or_none(client, path) is not None:
        raise PhpDeploymentError("remote file removal was not durable")


def _posix_replace(
    client: SftpClient,
    source: str,
    target: str,
    *,
    expected_sha256: str,
    mode: int,
) -> None:
    target_attributes = _lstat_or_none(client, target)
    if target_attributes is not None:
        target_mode = _mode(target_attributes)
        if stat.S_ISLNK(target_mode) or not stat.S_ISREG(target_mode):
            raise PhpDeploymentError(
                "refusing to replace a non-regular deployment target"
            )
    try:
        client.posix_rename(source, target)
        client.chmod(target, mode)
    except OSError as error:
        raise PhpDeploymentError(
            "atomic SFTP POSIX rename is unavailable or failed"
        ) from error
    content = _read_remote_bytes(client, target)
    if not secrets.compare_digest(_sha256_bytes(content), expected_sha256):
        raise PhpDeploymentError("installed remote file hash mismatch")


def _make_release_id(code_revision: str, *, prefix: str = "php-v2") -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ").casefold()
    return (
        f"{prefix}-{code_revision[:12]}-{timestamp}-{secrets.token_hex(8)}"
    )


def _validate_release_id(release_id: str) -> str:
    normalized = release_id.strip().casefold()
    if RELEASE_ID_PATTERN.fullmatch(normalized) is None:
        raise PhpDeploymentError("release ID is invalid")
    return normalized


def _private_roots(
    remote_root: str,
    stage_root: str | None,
    backup_root: str | None,
) -> tuple[str, str, str]:
    root = _remote_absolute_path(remote_root, label="remote root")
    private_root = _remote_join(root, "_private")
    configured_stage = stage_root or _remote_join(
        private_root,
        "deployment-staging",
    )
    configured_backup = backup_root or _remote_join(
        private_root,
        "deployment-backups",
    )
    safe_stage = _remote_child(
        private_root,
        configured_stage,
        label="stage root",
    )
    safe_backup = _remote_child(
        private_root,
        configured_backup,
        label="backup root",
    )
    if safe_stage == safe_backup:
        raise PhpDeploymentError("stage and backup roots must be different")
    return private_root, safe_stage, safe_backup


def _create_private_workspace(
    client: SftpClient,
    parent: str,
    name: str,
) -> str:
    _require_remote_directory(client, parent, create=True)
    workspace = _remote_join(parent, name)
    if _lstat_or_none(client, workspace) is not None:
        raise PhpDeploymentError("remote private workspace already exists")
    workspace_created = False
    try:
        client.mkdir(workspace, mode=PRIVATE_DIRECTORY_MODE)
        workspace_created = True
        client.chmod(workspace, PRIVATE_DIRECTORY_MODE)
    except OSError as error:
        creation_cleanup_error: OSError | None = None
        if workspace_created:
            try:
                client.rmdir(workspace)
            except OSError as error_during_cleanup:
                creation_cleanup_error = error_during_cleanup
        if creation_cleanup_error is not None:
            raise PhpDeploymentError(
                "remote private workspace creation failed and partial "
                "workspace cleanup did not complete"
            ) from creation_cleanup_error
        raise PhpDeploymentError("remote private workspace creation failed") from error
    try:
        _write_remote_bytes(
            client,
            _remote_join(workspace, ".htaccess"),
            STAGE_DENY_RULES,
            mode=PRIVATE_FILE_MODE,
        )
    except BaseException:
        policy_cleanup_error: BaseException | None = None
        try:
            _remove_remote_file_if_present(
                client,
                _remote_join(workspace, ".htaccess"),
            )
            client.rmdir(workspace)
        except (OSError, PhpDeploymentError) as error_during_cleanup:
            policy_cleanup_error = error_during_cleanup
        if policy_cleanup_error is not None:
            raise PhpDeploymentError(
                "private workspace policy setup failed and partial workspace "
                "cleanup did not complete"
            ) from policy_cleanup_error
        raise
    return workspace


def _verify_posix_rename_capability(
    client: SftpClient,
    workspace: str,
) -> None:
    first = _remote_join(workspace, "rename-a.blob")
    second = _remote_join(workspace, "rename-b.blob")
    _write_remote_bytes(client, first, b"bside-atomic-a\n", mode=PRIVATE_FILE_MODE)
    _write_remote_bytes(client, second, b"bside-atomic-b\n", mode=PRIVATE_FILE_MODE)
    try:
        client.posix_rename(first, second)
    except OSError as error:
        raise PhpDeploymentError(
            "server does not support atomic POSIX rename replacement"
        ) from error
    if _lstat_or_none(client, first) is not None:
        raise PhpDeploymentError("atomic rename left the source path behind")
    if _read_remote_bytes(client, second) != b"bside-atomic-a\n":
        raise PhpDeploymentError("atomic rename replacement changed bytes")
    _remove_remote_file_if_present(client, second)


def _artifact_content(artifact: LocalArtifact) -> bytes:
    content = _read_local_artifact(artifact.path)
    if (
        len(content) != artifact.size
        or not secrets.compare_digest(_sha256_bytes(content), artifact.sha256)
    ):
        raise PhpDeploymentError("local deployment artifact changed after planning")
    return content


def _stage_local_artifacts(
    client: SftpClient,
    workspace: str,
    plan: LocalDeploymentPlan,
    result: dict[str, str],
) -> Mapping[str, str]:
    for index, artifact in enumerate(plan.artifacts):
        blob_name = f"{index:03d}-{artifact.sha256}.blob"
        blob_path = _remote_join(workspace, blob_name)
        # Register the intended path before the upload so partial creation is
        # still removed if write/chmod/readback fails.
        result[artifact.relative_path] = blob_path
        content = _artifact_content(artifact)
        _write_remote_bytes(
            client,
            blob_path,
            content,
            mode=PRIVATE_FILE_MODE,
        )
    return result


def _backup_manifest_payload(snapshot: BackupSnapshot) -> dict[str, object]:
    files: dict[str, object] = {}
    for item in snapshot.files:
        files[item.relative_path] = {
            "existed": item.existed,
            "size": item.size,
            "mode": item.mode,
            "sha256": item.sha256,
            "backup_blob": item.backup_blob,
        }
    return {
        "schema_version": BACKUP_SCHEMA_VERSION,
        "release_id": snapshot.release_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "remote_root": snapshot.remote_root,
        "candidate_code_revision": snapshot.candidate_code_revision,
        "files": files,
    }


def _encode_json(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def capture_remote_backup(
    client: SftpClient,
    *,
    plan: LocalDeploymentPlan,
    remote_root: str,
    backup_root: str,
    release_id: str,
) -> BackupSnapshot:
    safe_release_id = _validate_release_id(release_id)
    backup_directory = _create_private_workspace(
        client,
        backup_root,
        safe_release_id,
    )
    snapshots: list[RemoteFileSnapshot] = []
    backup_files: list[str] = []
    try:
        for index, artifact in enumerate(plan.artifacts):
            target = _remote_join(remote_root, artifact.relative_path)
            attributes = _lstat_or_none(client, target)
            if attributes is None:
                snapshots.append(
                    RemoteFileSnapshot(
                        relative_path=artifact.relative_path,
                        existed=False,
                        size=None,
                        mode=None,
                        sha256=None,
                        backup_blob=None,
                    )
                )
                continue
            target_mode = _mode(attributes)
            if stat.S_ISLNK(target_mode) or not stat.S_ISREG(target_mode):
                raise PhpDeploymentError(
                    "deployment target is not a regular file before backup"
                )
            content = _read_remote_bytes(client, target)
            digest = _sha256_bytes(content)
            blob_name = f"{index:03d}-{digest}.blob"
            blob_path = _remote_join(backup_directory, blob_name)
            backup_files.append(blob_path)
            _write_remote_bytes(
                client,
                blob_path,
                content,
                mode=PRIVATE_FILE_MODE,
            )
            snapshots.append(
                RemoteFileSnapshot(
                    relative_path=artifact.relative_path,
                    existed=True,
                    size=len(content),
                    mode=stat.S_IMODE(target_mode),
                    sha256=digest,
                    backup_blob=blob_name,
                )
            )
        snapshot = BackupSnapshot(
            release_id=safe_release_id,
            backup_directory=backup_directory,
            remote_root=_remote_absolute_path(remote_root, label="remote root"),
            candidate_code_revision=plan.code_revision,
            files=tuple(snapshots),
        )
        manifest_bytes = _encode_json(_backup_manifest_payload(snapshot))
        manifest_path = _remote_join(
            backup_directory,
            "backup-manifest.json",
        )
        backup_files.append(manifest_path)
        _write_remote_bytes(
            client,
            manifest_path,
            manifest_bytes,
            mode=PRIVATE_FILE_MODE,
        )
        return snapshot
    except BaseException as operation_error:
        try:
            _cleanup_workspace(
                client,
                backup_directory,
                known_files=tuple(backup_files),
            )
        except BaseException as cleanup_error:
            raise PhpDeploymentError(
                "remote backup capture failed and incomplete backup cleanup "
                "did not complete"
            ) from cleanup_error
        raise operation_error


def _parse_backup_file(
    relative_path: str,
    raw: object,
) -> RemoteFileSnapshot:
    _safe_relative_path(relative_path)
    if not isinstance(raw, dict) or set(raw) != {
        "existed",
        "size",
        "mode",
        "sha256",
        "backup_blob",
    }:
        raise PhpDeploymentError("remote backup manifest file entry is invalid")
    existed = raw["existed"]
    if not isinstance(existed, bool):
        raise PhpDeploymentError("remote backup existence marker is invalid")
    if not existed:
        if any(raw[field] is not None for field in ("size", "mode", "sha256", "backup_blob")):
            raise PhpDeploymentError("absent backup entry contains unexpected data")
        return RemoteFileSnapshot(
            relative_path=relative_path,
            existed=False,
            size=None,
            mode=None,
            sha256=None,
            backup_blob=None,
        )
    size = raw["size"]
    mode = raw["mode"]
    digest = raw["sha256"]
    blob_name = raw["backup_blob"]
    if (
        not isinstance(size, int)
        or size < 0
        or size > MAX_REMOTE_FILE_BYTES
        or not isinstance(mode, int)
        or mode < 0
        or mode > 0o7777
        or not isinstance(digest, str)
        or SHA256_PATTERN.fullmatch(digest) is None
        or not isinstance(blob_name, str)
        or "/" in blob_name
        or "\\" in blob_name
        or not blob_name.endswith(".blob")
    ):
        raise PhpDeploymentError("remote backup file metadata is invalid")
    return RemoteFileSnapshot(
        relative_path=relative_path,
        existed=True,
        size=size,
        mode=mode,
        sha256=digest,
        backup_blob=blob_name,
    )


def load_remote_backup(
    client: SftpClient,
    *,
    backup_root: str,
    release_id: str,
    expected_remote_root: str,
) -> BackupSnapshot:
    safe_release_id = _validate_release_id(release_id)
    backup_directory = _remote_join(backup_root, safe_release_id)
    manifest_path = _remote_join(backup_directory, "backup-manifest.json")
    raw = _read_remote_bytes(client, manifest_path)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PhpDeploymentError("remote backup manifest is invalid") from error
    if (
        not isinstance(payload, dict)
        or set(payload)
        != {
            "schema_version",
            "release_id",
            "created_at",
            "remote_root",
            "candidate_code_revision",
            "files",
        }
        or payload.get("schema_version") != BACKUP_SCHEMA_VERSION
        or payload.get("release_id") != safe_release_id
        or payload.get("remote_root") != expected_remote_root
        or not isinstance(payload.get("candidate_code_revision"), str)
        or SHA1_PATTERN.fullmatch(payload["candidate_code_revision"]) is None
        or not isinstance(payload.get("files"), dict)
        or set(payload["files"]) != set(DEFAULT_COMMIT_ORDER)
    ):
        raise PhpDeploymentError("remote backup identity is invalid")
    files = tuple(
        _parse_backup_file(relative_path, payload["files"][relative_path])
        for relative_path in DEFAULT_COMMIT_ORDER
    )
    snapshot = BackupSnapshot(
        release_id=safe_release_id,
        backup_directory=backup_directory,
        remote_root=expected_remote_root,
        candidate_code_revision=payload["candidate_code_revision"],
        files=files,
    )
    for item in snapshot.files:
        if not item.existed:
            continue
        if item.backup_blob is None or item.sha256 is None or item.size is None:
            raise PhpDeploymentError("remote backup is incomplete")
        content = _read_remote_bytes(
            client,
            _remote_join(backup_directory, item.backup_blob),
        )
        if (
            len(content) != item.size
            or not secrets.compare_digest(_sha256_bytes(content), item.sha256)
        ):
            raise PhpDeploymentError("remote backup blob identity mismatch")
    return snapshot


def verify_existing_remote_release_identity(
    client: SftpClient,
    *,
    remote_root: str,
) -> str | None:
    """Verify the current v2 manifest and bytes, or attest a first deploy."""

    root = _remote_absolute_path(remote_root, label="remote root")
    manifest_path = _remote_join(root, DEPLOYMENT_MANIFEST_NAME)
    attributes = _lstat_or_none(client, manifest_path)
    if attributes is None:
        for relative_path in (
            "governance_v2.php",
            "governance_v2_write.php",
            "openapi-v2.yaml",
        ):
            if _lstat_or_none(client, _remote_join(root, relative_path)) is not None:
                raise PhpDeploymentError(
                    "existing v2 files are missing a deployment manifest"
                )
        return None
    manifest_mode = _mode(attributes)
    if stat.S_ISLNK(manifest_mode) or not stat.S_ISREG(manifest_mode):
        raise PhpDeploymentError(
            "existing deployment manifest is not a regular file"
        )
    raw = _read_remote_bytes(client, manifest_path)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PhpDeploymentError(
            "existing deployment manifest is invalid"
        ) from error
    if (
        not isinstance(payload, dict)
        or set(payload) != {"schema_version", "code_revision", "files"}
        or payload.get("schema_version") != 1
        or not isinstance(payload.get("code_revision"), str)
        or SHA1_PATTERN.fullmatch(payload["code_revision"]) is None
        or not isinstance(payload.get("files"), dict)
        or set(payload["files"]) != set(CORE_API_FILES)
    ):
        raise PhpDeploymentError(
            "existing deployment manifest identity is invalid"
        )
    for relative_path in CORE_API_FILES:
        expected_digest = payload["files"].get(relative_path)
        if (
            not isinstance(expected_digest, str)
            or SHA256_PATTERN.fullmatch(expected_digest) is None
        ):
            raise PhpDeploymentError(
                "existing deployment manifest file hash is invalid"
            )
        content = _read_remote_bytes(
            client,
            _remote_join(root, relative_path),
        )
        if not secrets.compare_digest(
            _sha256_bytes(content),
            expected_digest,
        ):
            raise PhpDeploymentError(
                "existing deployment bytes do not match the manifest"
            )
    return payload["code_revision"]


def verify_remote_targets_match_snapshot(
    client: SftpClient,
    *,
    snapshot: BackupSnapshot,
) -> None:
    """Fail before commit if a non-cooperating writer changed any target."""

    for item in snapshot.files:
        target = _remote_join(snapshot.remote_root, item.relative_path)
        before = _lstat_or_none(client, target)
        if not item.existed:
            if before is not None:
                raise PhpDeploymentError(
                    "deployment target changed after backup capture"
                )
            continue
        if (
            before is None
            or item.size is None
            or item.mode is None
            or item.sha256 is None
        ):
            raise PhpDeploymentError(
                "deployment target changed after backup capture"
            )
        before_mode = _mode(before)
        if (
            stat.S_ISLNK(before_mode)
            or not stat.S_ISREG(before_mode)
            or stat.S_IMODE(before_mode) != item.mode
            or getattr(before, "st_size", None) != item.size
        ):
            raise PhpDeploymentError(
                "deployment target changed after backup capture"
            )
        content = _read_remote_bytes(client, target)
        after = _lstat_or_none(client, target)
        if after is None:
            raise PhpDeploymentError(
                "deployment target changed after backup capture"
            )
        after_mode = _mode(after)
        if (
            stat.S_ISLNK(after_mode)
            or not stat.S_ISREG(after_mode)
            or stat.S_IMODE(after_mode) != item.mode
            or getattr(after, "st_size", None) != item.size
            or len(content) != item.size
            or not secrets.compare_digest(_sha256_bytes(content), item.sha256)
        ):
            raise PhpDeploymentError(
                "deployment target changed after backup capture"
            )


def _acquire_deployment_lock(
    client: SftpClient,
    private_root: str,
    release_id: str,
) -> str:
    _require_remote_directory(client, private_root, create=False)
    lock_path = _remote_join(private_root, "deployment-lock")
    if _lstat_or_none(client, lock_path) is not None:
        raise PhpDeploymentError("another deployment lock already exists")
    lock_created = False
    try:
        client.mkdir(lock_path, mode=PRIVATE_DIRECTORY_MODE)
        lock_created = True
        client.chmod(lock_path, PRIVATE_DIRECTORY_MODE)
    except OSError as error:
        cleanup_error: OSError | None = None
        if lock_created:
            try:
                client.rmdir(lock_path)
            except OSError as error_during_cleanup:
                cleanup_error = error_during_cleanup
        if cleanup_error is not None:
            raise PhpDeploymentError(
                "could not acquire the deployment lock and partial lock "
                "cleanup did not complete"
            ) from cleanup_error
        raise PhpDeploymentError("could not acquire the deployment lock") from error
    metadata = _encode_json(
        {
            "schema_version": 1,
            "release_id": release_id,
            "acquired_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    try:
        _write_remote_bytes(
            client,
            _remote_join(lock_path, "owner.json"),
            metadata,
            mode=PRIVATE_FILE_MODE,
        )
    except BaseException:
        try:
            _remove_remote_file_if_present(
                client,
                _remote_join(lock_path, "owner.json"),
            )
            client.rmdir(lock_path)
        except (OSError, PhpDeploymentError):
            pass
        raise
    return lock_path


def _release_deployment_lock(
    client: SftpClient,
    lock_path: str,
    *,
    exclusive_writer: ExclusiveBytesWriter | None = None,
) -> None:
    owner_path = _remote_join(lock_path, "owner.json")
    owner_attributes = _lstat_or_none(client, owner_path)
    if owner_attributes is None:
        raise PhpDeploymentError(
            "deployment lock owner evidence is missing"
        )
    owner_mode = _mode(owner_attributes)
    if stat.S_ISLNK(owner_mode) or not stat.S_ISREG(owner_mode):
        raise PhpDeploymentError(
            "deployment lock owner evidence is not a regular file"
        )
    owner_content = _read_remote_bytes(client, owner_path)
    _remove_remote_file_if_present(client, owner_path)
    try:
        client.rmdir(lock_path)
    except OSError as error:
        try:
            writer = (
                _write_remote_exclusive_bytes
                if exclusive_writer is None
                else exclusive_writer
            )
            writer(
                client,
                owner_path,
                owner_content,
                mode=stat.S_IMODE(owner_mode),
            )
        except BaseException as restore_error:
            raise PhpDeploymentError(
                "could not release the deployment lock and owner evidence "
                "could not be restored"
            ) from restore_error
        raise PhpDeploymentError(
            "could not release the deployment lock; owner evidence was preserved"
        ) from error


def _cleanup_workspace(
    client: SftpClient,
    workspace: str,
    *,
    known_files: Sequence[str],
) -> None:
    cleanup_failed = False
    for path in known_files:
        try:
            _remove_remote_file_if_present(client, path)
        except PhpDeploymentError:
            cleanup_failed = True
    try:
        _remove_remote_file_if_present(client, _remote_join(workspace, ".htaccess"))
        client.rmdir(workspace)
    except (OSError, PhpDeploymentError):
        cleanup_failed = True
    if cleanup_failed or _lstat_or_none(client, workspace) is not None:
        raise PhpDeploymentError(
            "remote private workspace cleanup did not complete"
        )


def _commit_staged_artifacts(
    client: SftpClient,
    *,
    plan: LocalDeploymentPlan,
    staged: Mapping[str, str],
    remote_root: str,
) -> None:
    for artifact in plan.artifacts:
        source = staged.get(artifact.relative_path)
        if source is None:
            raise PhpDeploymentError("staged deployment artifact is missing")
        target = _remote_join(remote_root, artifact.relative_path)
        parent = posixpath.dirname(target)
        _require_remote_directory(client, parent, create=False)
        _posix_replace(
            client,
            source,
            target,
            expected_sha256=artifact.sha256,
            mode=artifact.mode,
        )


def _stage_backup_files_for_restore(
    client: SftpClient,
    *,
    snapshot: BackupSnapshot,
    workspace: str,
) -> Mapping[str, str]:
    result: dict[str, str] = {}
    for index, item in enumerate(snapshot.files):
        if not item.existed:
            continue
        if item.backup_blob is None or item.sha256 is None:
            raise PhpDeploymentError("rollback backup metadata is incomplete")
        content = _read_remote_bytes(
            client,
            _remote_join(snapshot.backup_directory, item.backup_blob),
        )
        if not secrets.compare_digest(_sha256_bytes(content), item.sha256):
            raise PhpDeploymentError("rollback backup blob hash mismatch")
        stage_path = _remote_join(
            workspace,
            f"restore-{index:03d}-{item.sha256}.blob",
        )
        _write_remote_bytes(
            client,
            stage_path,
            content,
            mode=PRIVATE_FILE_MODE,
        )
        result[item.relative_path] = stage_path
    return result


def _restore_order(snapshot: BackupSnapshot) -> tuple[str, ...]:
    files = snapshot.file_by_path
    previous_v2 = bool(
        files["governance_v2.php"].existed
        and files["governance_v2_write.php"].existed
    )
    without_manifest = tuple(
        path
        for path in DEFAULT_COMMIT_ORDER
        if path != DEPLOYMENT_MANIFEST_NAME
    )
    if previous_v2:
        return (*without_manifest, DEPLOYMENT_MANIFEST_NAME)
    first_deploy_order = (
        "api.php",
        "governance_v1.php",
        V1_OPENAPI_NAME,
        ".htaccess",
        "governance_v2.php",
        "governance_v2_write.php",
        "openapi-v2.yaml",
        "migrations/011_global_terminal_v2.sql",
        DEPLOYMENT_MANIFEST_NAME,
    )
    if set(first_deploy_order) != set(DEFAULT_COMMIT_ORDER):
        raise PhpDeploymentError("internal first-deployment rollback order is invalid")
    return first_deploy_order


def restore_remote_backup(
    client: SftpClient,
    *,
    snapshot: BackupSnapshot,
    workspace: str,
) -> None:
    staged = _stage_backup_files_for_restore(
        client,
        snapshot=snapshot,
        workspace=workspace,
    )
    manifest_target = _remote_join(
        snapshot.remote_root,
        DEPLOYMENT_MANIFEST_NAME,
    )
    _remove_remote_file_if_present(client, manifest_target)
    file_by_path = snapshot.file_by_path
    for relative_path in _restore_order(snapshot):
        item = file_by_path[relative_path]
        target = _remote_join(snapshot.remote_root, relative_path)
        if not item.existed:
            _remove_remote_file_if_present(client, target)
            continue
        source = staged.get(relative_path)
        if (
            source is None
            or item.sha256 is None
            or item.mode is None
        ):
            raise PhpDeploymentError("rollback staged file is incomplete")
        _posix_replace(
            client,
            source,
            target,
            expected_sha256=item.sha256,
            mode=item.mode,
        )
    for item in snapshot.files:
        target = _remote_join(snapshot.remote_root, item.relative_path)
        attributes = _lstat_or_none(client, target)
        if not item.existed:
            if attributes is not None:
                raise PhpDeploymentError("rollback left a newly introduced file")
            continue
        if attributes is None or item.sha256 is None or item.mode is None:
            raise PhpDeploymentError("rollback did not restore an existing file")
        content = _read_remote_bytes(client, target)
        if (
            not secrets.compare_digest(_sha256_bytes(content), item.sha256)
            or stat.S_IMODE(_mode(attributes)) != item.mode
        ):
            raise PhpDeploymentError("rollback file identity mismatch")


def _validate_https_url(
    value: str,
    *,
    label: str,
    allow_http: bool = False,
    allow_query: bool = False,
) -> str:
    normalized = value.strip().rstrip("/")
    parsed = urlsplit(normalized)
    allowed_schemes = {"https", "http"} if allow_http else {"https"}
    if (
        parsed.scheme.casefold() not in allowed_schemes
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or (parsed.query and not allow_query)
        or parsed.fragment
    ):
        raise PhpDeploymentError(f"{label} must be a credential-free HTTPS URL")
    return normalized


def _canonical_url_path(value: str, *, label: str) -> str:
    try:
        decoded = unquote(value, errors="strict")
    except UnicodeDecodeError as error:
        raise PhpDeploymentError(f"{label} path is invalid") from error
    if "\\" in decoded or "\x00" in decoded or not decoded.startswith("/"):
        raise PhpDeploymentError(f"{label} path is invalid")
    normalized = posixpath.normpath(decoded)
    if normalized != decoded.rstrip("/"):
        raise PhpDeploymentError(f"{label} path is not canonical")
    return normalized


def validate_http_endpoint_binding(
    *,
    public_url_root: str,
    api_v2_base_url: str,
    rollback_health_url: str,
    allow_http: bool = False,
) -> None:
    public = _validate_https_url(
        public_url_root,
        label="public URL root",
        allow_http=allow_http,
    )
    api = _validate_https_url(
        api_v2_base_url,
        label="API v2 base URL",
        allow_http=allow_http,
    )
    rollback = _validate_https_url(
        rollback_health_url,
        label="rollback health URL",
        allow_http=allow_http,
        allow_query=True,
    )
    public_parts = urlsplit(public)
    public_origin = (
        public_parts.scheme.casefold(),
        public_parts.netloc.casefold(),
    )
    public_path = _canonical_url_path(
        public_parts.path or "/",
        label="public URL root",
    )
    for label, candidate in (
        ("API v2 base URL", api),
        ("rollback health URL", rollback),
    ):
        parts = urlsplit(candidate)
        if (
            parts.scheme.casefold(),
            parts.netloc.casefold(),
        ) != public_origin:
            raise PhpDeploymentError(
                "deployment verification URLs must use the public URL origin"
            )
        candidate_path = _canonical_url_path(parts.path or "/", label=label)
        if (
            candidate_path != public_path
            and not candidate_path.startswith(public_path.rstrip("/") + "/")
        ):
            raise PhpDeploymentError(
                "deployment verification URL escapes the public URL path"
            )


def _validate_gabia_core_binding(
    compatibility: GabiaCoreCompatibility,
    *,
    client: SftpClient,
    remote_root: str,
    public_url_root: str,
    api_v2_base_url: str,
    rollback_health_url: str,
) -> None:
    evidence = compatibility.evidence
    if (
        compatibility.client is not client
        or compatibility.ssh_host != GABIA_COMPATIBILITY_SSH_HOST
        or compatibility.ssh_host_key_sha256
        != GABIA_SSH_HOST_KEY_SHA256
        or remote_root != GABIA_REMOTE_ROOT
        or compatibility.remote_root != GABIA_REMOTE_ROOT
        or public_url_root != GABIA_PUBLIC_URL_ROOT
        or compatibility.public_url_root != GABIA_PUBLIC_URL_ROOT
        or api_v2_base_url != GABIA_API_V2_BASE_URL
        or compatibility.api_v2_base_url != GABIA_API_V2_BASE_URL
        or rollback_health_url != GABIA_ROLLBACK_HEALTH_URL
        or compatibility.rollback_health_url
        != GABIA_ROLLBACK_HEALTH_URL
        or (
            compatibility.current_release_sha is not None
            and SHA1_PATTERN.fullmatch(
                compatibility.current_release_sha
            )
            is None
        )
        or compatibility.private_policy_mode < 0
        or compatibility.private_policy_mode > 0o7777
        or not compatibility.private_policy
        or len(compatibility.private_policy) > MAX_PRIVATE_POLICY_BYTES
        or not callable(compatibility.exclusive_writer)
        or not all(
            (
                evidence.exclusive_writer_incompatible,
                evidence.private_mode_0700_directory,
                evidence.write_readback_verified,
                evidence.absent_target_rename_verified,
                evidence.no_replace_collision_verified,
                evidence.probe_residue_absent,
            )
        )
    ):
        raise PhpDeploymentError(
            "Gabia core compatibility binding does not match"
        )


def _verify_gabia_private_policy(
    client: SftpClient,
    compatibility: GabiaCoreCompatibility,
) -> None:
    deny_path = _remote_join(
        _remote_join(compatibility.remote_root, "_private"),
        ".htaccess",
    )
    if not _regular_file_identity_matches(
        client,
        deny_path,
        compatibility.private_policy,
        mode=compatibility.private_policy_mode,
    ):
        raise PhpDeploymentError(
            "Gabia private HTTP policy preservation check failed"
        )


def prepare_gabia_core_compatibility(
    client: SftpClient,
    *,
    ssh_options: SshTunnelOptions,
    compatibility_host: str,
    remote_root: str,
    public_url_root: str,
    api_v2_base_url: str,
    rollback_health_url: str,
    expected_current_sha: str | None = None,
) -> GabiaCoreCompatibility:
    """Probe the exact Gabia server and bind a fallback to this session."""

    if (
        compatibility_host != GABIA_COMPATIBILITY_SSH_HOST
        or ssh_options.host != GABIA_COMPATIBILITY_SSH_HOST
        or not secrets.compare_digest(
            compatibility_host,
            ssh_options.host,
        )
        or remote_root != GABIA_REMOTE_ROOT
        or public_url_root != GABIA_PUBLIC_URL_ROOT
        or api_v2_base_url != GABIA_API_V2_BASE_URL
        or rollback_health_url != GABIA_ROLLBACK_HEALTH_URL
    ):
        raise PhpDeploymentError(
            "Gabia core compatibility target does not match"
        )
    try:
        fingerprint = normalize_ssh_host_key_sha256(
            ssh_options.host_key_sha256
        )
        legacy_allowed = legacy_ssh_rsa_sha1_is_allowed(ssh_options)
    except Exception as error:
        raise PhpDeploymentError(
            "pinned Gabia SSH security policy is invalid"
        ) from error
    if (
        fingerprint != GABIA_SSH_HOST_KEY_SHA256
        or ssh_options.host_key_sha256 != GABIA_SSH_HOST_KEY_SHA256
        or legacy_allowed is not True
        or ssh_options.allow_legacy_ssh_rsa_sha1 is not True
        or ssh_options.legacy_ssh_rsa_sha1_host
        != GABIA_COMPATIBILITY_SSH_HOST
    ):
        raise PhpDeploymentError(
            "pinned Gabia SSH security policy does not match"
        )
    if (
        expected_current_sha is not None
        and SHA1_PATTERN.fullmatch(expected_current_sha) is None
    ):
        raise PhpDeploymentError(
            "expected current Gabia release SHA is invalid"
        )

    private_root = _remote_join(GABIA_REMOTE_ROOT, "_private")
    _require_remote_directory(client, GABIA_REMOTE_ROOT, create=False)
    _require_remote_directory(client, private_root, create=False)
    current_release_sha = verify_existing_remote_release_identity(
        client,
        remote_root=GABIA_REMOTE_ROOT,
    )
    if (
        expected_current_sha is not None
        and current_release_sha != expected_current_sha
    ):
        raise PhpDeploymentError(
            "current Gabia release SHA does not match"
        )
    evidence = _probe_gabia_sftp_capabilities(
        client,
        private_root=private_root,
    )
    deny_path = _remote_join(private_root, ".htaccess")
    deny_attributes = _lstat_or_none(client, deny_path)
    if deny_attributes is None:
        raise PhpDeploymentError(
            "existing private HTTP policy is missing"
        )
    deny_mode = _mode(deny_attributes)
    if stat.S_ISLNK(deny_mode) or not stat.S_ISREG(deny_mode):
        raise PhpDeploymentError(
            "existing private HTTP policy is not a regular file"
        )
    private_policy = _read_remote_bytes(
        client,
        deny_path,
        maximum_bytes=MAX_PRIVATE_POLICY_BYTES,
    )
    if not private_policy:
        raise PhpDeploymentError(
            "existing private HTTP policy is empty"
        )
    compatibility = GabiaCoreCompatibility(
        client=client,
        ssh_host=ssh_options.host,
        ssh_host_key_sha256=fingerprint,
        remote_root=remote_root,
        public_url_root=public_url_root,
        api_v2_base_url=api_v2_base_url,
        rollback_health_url=rollback_health_url,
        current_release_sha=current_release_sha,
        private_policy=private_policy,
        private_policy_mode=stat.S_IMODE(deny_mode),
        evidence=evidence,
        exclusive_writer=_gabia_compatibility_writer(
            client,
            allowed_root=GABIA_REMOTE_ROOT,
        ),
    )
    _validate_gabia_core_binding(
        compatibility,
        client=client,
        remote_root=remote_root,
        public_url_root=public_url_root,
        api_v2_base_url=api_v2_base_url,
        rollback_health_url=rollback_health_url,
    )
    return compatibility


def ensure_private_root_http_protection(
    client: SftpClient,
    *,
    remote_root: str,
    private_root: str,
    public_url_root: str,
    http_request: HttpRequester | None = None,
    timeout: float = 20.0,
    allow_http: bool = False,
    exclusive_writer: ExclusiveBytesWriter | None = None,
    expected_policy: bytes | None = None,
    expected_policy_mode: int | None = None,
    allowed_private_redirect: str | None = None,
) -> None:
    """Prove that permanent private backups cannot be read over HTTP."""

    safe_remote_root = _remote_absolute_path(
        remote_root,
        label="remote root",
    )
    safe_private_root = _remote_absolute_path(
        private_root,
        label="private root",
    )
    if safe_private_root != _remote_join(safe_remote_root, "_private"):
        raise PhpDeploymentError(
            "private root is not bound to the public document root"
        )
    _require_remote_directory(client, safe_remote_root, create=False)
    _require_remote_directory(client, safe_private_root, create=False)
    writer = (
        _write_remote_exclusive_bytes
        if exclusive_writer is None
        else exclusive_writer
    )
    deny_path = _remote_join(safe_private_root, ".htaccess")
    deny_attributes = _lstat_or_none(client, deny_path)
    if expected_policy is not None:
        if (
            not expected_policy
            or len(expected_policy) > MAX_PRIVATE_POLICY_BYTES
            or expected_policy_mode is None
            or deny_attributes is None
            or not _regular_file_identity_matches(
                client,
                deny_path,
                expected_policy,
                mode=expected_policy_mode,
            )
        ):
            raise PhpDeploymentError(
                "existing private HTTP policy changed"
            )
    elif deny_attributes is None:
        writer(
            client,
            deny_path,
            PRIVATE_ROOT_DENY_RULES,
            mode=DEFAULT_FILE_MODE,
        )
    else:
        deny_mode = _mode(deny_attributes)
        if stat.S_ISLNK(deny_mode) or not stat.S_ISREG(deny_mode):
            raise PhpDeploymentError(
                "private root deny file is not a regular file"
            )
        if _read_remote_bytes(client, deny_path) != PRIVATE_ROOT_DENY_RULES:
            raise PhpDeploymentError(
                "private root deny file does not match the expected policy"
            )

    web_root = _validate_https_url(
        public_url_root,
        label="public URL root",
        allow_http=allow_http,
    )
    canary_id = secrets.token_hex(32)
    public_filename = f"bside-public-canary-{canary_id}.txt"
    private_filename = f".bside-private-canary-{canary_id}.txt"
    if (
        PUBLIC_CANARY_NAME_PATTERN.fullmatch(public_filename) is None
        or PRIVATE_CANARY_NAME_PATTERN.fullmatch(private_filename) is None
    ):
        raise PhpDeploymentError("private root canary filename is invalid")
    public_canary_path = _remote_join(safe_remote_root, public_filename)
    private_canary_path = _remote_join(safe_private_root, private_filename)
    sentinel = b"bside-private-canary:" + secrets.token_bytes(48)
    operation_error: BaseException | None = None
    cleanup_error: BaseException | None = None
    request = _default_http_request if http_request is None else http_request
    try:
        writer(
            client,
            public_canary_path,
            sentinel,
            mode=DEFAULT_FILE_MODE,
        )
        writer(
            client,
            private_canary_path,
            sentinel,
            mode=PRIVATE_FILE_MODE,
        )
        public_response = request(
            "GET",
            web_root + "/" + quote(public_filename, safe=""),
            {
                "Accept": "application/octet-stream",
                "Cache-Control": "no-store",
            },
            timeout,
        )
        if (
            public_response.status != 200
            or not secrets.compare_digest(public_response.body, sentinel)
        ):
            raise PhpDeploymentError(
                "public document root HTTP mapping canary failed"
            )
        private_response = request(
            "GET",
            web_root + "/_private/" + quote(private_filename, safe=""),
            {
                "Accept": "application/octet-stream",
                "Cache-Control": "no-store",
            },
            timeout,
        )
        private_denied = private_response.status in {403, 404}
        if (
            private_response.status == 302
            and allowed_private_redirect is not None
        ):
            private_denied = secrets.compare_digest(
                private_response.header("Location"),
                allowed_private_redirect,
            )
        if (
            not private_denied
            or sentinel in private_response.body
            or len(private_response.body) > 250000
        ):
            raise PhpDeploymentError(
                "private root HTTP isolation canary failed"
            )
    except BaseException as error:
        operation_error = error
    finally:
        try:
            if expected_policy is not None:
                _remove_exact_file_if_present(
                    client,
                    private_canary_path,
                    sentinel,
                    mode=PRIVATE_FILE_MODE,
                )
                _remove_exact_file_if_present(
                    client,
                    public_canary_path,
                    sentinel,
                    mode=DEFAULT_FILE_MODE,
                )
            else:
                _remove_remote_file_if_present(
                    client,
                    private_canary_path,
                )
                _remove_remote_file_if_present(
                    client,
                    public_canary_path,
                )
            if (
                _lstat_or_none(client, private_canary_path) is not None
                or _lstat_or_none(client, public_canary_path) is not None
            ):
                raise PhpDeploymentError(
                    "HTTP isolation canary cleanup failed"
                )
        except BaseException as error:
            cleanup_error = error
    if cleanup_error is not None:
        if operation_error is not None:
            raise PhpDeploymentError(
                "private root isolation failed and canary cleanup did not complete"
            ) from cleanup_error
        raise PhpDeploymentError(
            "HTTP isolation canary cleanup failed"
        ) from cleanup_error
    if operation_error is not None:
        raise operation_error
    if expected_policy is not None and not _regular_file_identity_matches(
        client,
        deny_path,
        expected_policy,
        mode=expected_policy_mode or -1,
    ):
        raise PhpDeploymentError(
            "private HTTP policy changed during verification"
        )


def _default_http_request(
    method: str,
    url: str,
    headers: Mapping[str, str],
    timeout: float,
) -> HttpResponse:
    try:
        import httpx

        with httpx.Client(
            follow_redirects=False,
            timeout=timeout,
            trust_env=False,
        ) as client:
            response = client.request(method, url, headers=dict(headers))
    except Exception as error:
        raise PhpDeploymentError("deployment HTTP verification failed") from error
    return HttpResponse(
        status=response.status_code,
        headers=dict(response.headers),
        body=response.content,
    )


def _json_response(response: HttpResponse, *, label: str) -> Mapping[str, object]:
    content_type = response.header("content-type").casefold()
    if not content_type.startswith("application/json"):
        raise PhpDeploymentError(f"{label} did not return JSON")
    if len(response.body) > 250000:
        raise PhpDeploymentError(f"{label} exceeded the response budget")
    try:
        payload = json.loads(response.body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PhpDeploymentError(f"{label} returned invalid JSON") from error
    if not isinstance(payload, dict):
        raise PhpDeploymentError(f"{label} returned a non-object JSON body")
    return payload


def _validated_protected_token(value: str) -> str:
    if not isinstance(value, str):
        raise PhpDeploymentError("protected Bearer token is required")
    encoded = value.encode("utf-8")
    if (
        len(encoded) < 32
        or len(encoded) > 4096
        or any(character < 0x21 or character == 0x7F for character in encoded)
    ):
        raise PhpDeploymentError(
            "protected Bearer token must be at least 32 non-whitespace bytes"
        )
    return value


def verify_protected_closed_state(
    *,
    base_url: str,
    protected_token: str,
    http_request: HttpRequester = _default_http_request,
    timeout: float = 20.0,
    allow_http: bool = False,
) -> None:
    base = _validate_https_url(
        base_url,
        label="API v2 base URL",
        allow_http=allow_http,
    )
    if not base.endswith("/api/v2"):
        raise PhpDeploymentError("API v2 base URL must end with /api/v2")
    token = _validated_protected_token(protected_token)
    response = http_request(
        "GET",
        base + "/ops/release-state",
        {
            "Accept": "application/json",
            "Authorization": "Bearer " + token,
            "Cache-Control": "no-store",
        },
        timeout,
    )
    if response.status != 200:
        raise PhpDeploymentError(
            "authenticated protected route did not return HTTP 200"
        )
    payload = _json_response(response, label="authenticated protected route")
    data = payload.get("data")
    if (
        payload.get("ok") is not True
        or payload.get("api_version") != "v2"
        or not isinstance(data, dict)
        or data.get("release_state") != "closed"
    ):
        raise PhpDeploymentError(
            "authenticated protected route is not in the closed state"
        )


def _opcache_probe_source(*, token_hash: str, probe_id: str) -> bytes:
    if SHA256_PATTERN.fullmatch(token_hash) is None:
        raise PhpDeploymentError("OPcache token hash is invalid")
    if SHA256_PATTERN.fullmatch(probe_id) is None:
        raise PhpDeploymentError("OPcache probe ID is invalid")
    source = f"""<?php
declare(strict_types=1);
ini_set('display_errors', '0');
error_reporting(E_ALL);
header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');
header('X-Content-Type-Options: nosniff');
$expected = '{token_hash}';
$provided = isset($_SERVER['HTTP_X_BSIDE_OPCACHE_TOKEN'])
    ? (string)$_SERVER['HTTP_X_BSIDE_OPCACHE_TOKEN'] : '';
if ($_SERVER['REQUEST_METHOD'] !== 'POST'
    || $provided === ''
    || !hash_equals($expected, hash('sha256', $provided))) {{
    http_response_code(404);
    echo '{{"ok":false,"error":"not_found"}}';
    exit;
}}
$available = function_exists('opcache_reset');
$reset = $available ? opcache_reset() : false;
if (!$available || !$reset) {{
    http_response_code(503);
    echo '{{"ok":false,"error":"opcache_reset_failed"}}';
    exit;
}}
http_response_code(200);
echo '{{"ok":true,"opcache_reset":true,"probe_id":"{probe_id}"}}';
"""
    return source.encode("utf-8")


def _strict_opcache_probe_source(
    *,
    token_hash: str,
    probe_id: str,
) -> bytes:
    if SHA256_PATTERN.fullmatch(token_hash) is None:
        raise PhpDeploymentError("OPcache token hash is invalid")
    if SHA256_PATTERN.fullmatch(probe_id) is None:
        raise PhpDeploymentError("OPcache probe ID is invalid")
    source = f"""<?php
declare(strict_types=1);
ini_set('display_errors', '0');
error_reporting(E_ALL);
header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');
header('X-Content-Type-Options: nosniff');
$expected = '{token_hash}';
$provided = isset($_SERVER['HTTP_X_BSIDE_OPCACHE_TOKEN'])
    ? (string)$_SERVER['HTTP_X_BSIDE_OPCACHE_TOKEN'] : '';
if ($_SERVER['REQUEST_METHOD'] !== 'POST'
    || $provided === ''
    || !hash_equals($expected, hash('sha256', $provided))) {{
    http_response_code(404);
    echo '{{"ok":false,"error":"not_found"}}';
    exit;
}}
$extensionLoaded = extension_loaded('Zend OPcache');
$resetFunction = function_exists('opcache_reset');
$statusFunction = function_exists('opcache_get_status');
$iniEnable = ini_get('opcache.enable');
$validateTimestamps = ini_get('opcache.validate_timestamps');
$revalidateFreq = ini_get('opcache.revalidate_freq');
if (!$extensionLoaded
    || !$resetFunction
    || !$statusFunction
    || $iniEnable !== '1'
    || $validateTimestamps !== '1'
    || $revalidateFreq !== '2') {{
    http_response_code(503);
    echo '{{"ok":false,"error":"opcache_contract_mismatch"}}';
    exit;
}}
$status = opcache_get_status(false);
if (!is_array($status)
    || !array_key_exists('opcache_enabled', $status)
    || !is_bool($status['opcache_enabled'])) {{
    http_response_code(503);
    echo '{{"ok":false,"error":"opcache_status_ambiguous"}}';
    exit;
}}
$statusEnabled = $status['opcache_enabled'];
$resetResult = null;
$action = 'disabled_verified';
if ($statusEnabled === true) {{
    $resetResult = opcache_reset();
    if ($resetResult !== true) {{
        http_response_code(503);
        echo '{{"ok":false,"error":"opcache_reset_failed"}}';
        exit;
    }}
    $action = 'reset_verified';
}}
http_response_code(200);
echo json_encode(array(
    'ok' => true,
    'opcache_action' => $action,
    'probe_id' => '{probe_id}',
    'extension_loaded' => $extensionLoaded,
    'reset_function' => $resetFunction,
    'status_function' => $statusFunction,
    'status_available' => true,
    'status_enabled' => $statusEnabled,
    'ini_enable' => $iniEnable,
    'validate_timestamps' => $validateTimestamps,
    'revalidate_freq' => $revalidateFreq,
    'reset_result' => $resetResult,
));
"""
    return source.encode("ascii")


def reset_opcache_with_ephemeral_probe(
    client: SftpClient,
    *,
    remote_root: str,
    public_url_root: str,
    http_request: HttpRequester = _default_http_request,
    timeout: float = 20.0,
    allow_http: bool = False,
    exclusive_writer: ExclusiveBytesWriter | None = None,
    require_strict_state: bool = False,
) -> str:
    web_root = _validate_https_url(
        public_url_root,
        label="public URL root",
        allow_http=allow_http,
    )
    probe_id = secrets.token_hex(32)
    token = secrets.token_urlsafe(48)
    token_hash = _sha256_bytes(token.encode("utf-8"))
    filename = f".bside-opcache-{probe_id}.php"
    if OPCACHE_PROBE_NAME_PATTERN.fullmatch(filename) is None:
        raise PhpDeploymentError("OPcache probe filename is invalid")
    remote_path = _remote_join(remote_root, filename)
    probe_source = (
        _strict_opcache_probe_source(
            token_hash=token_hash,
            probe_id=probe_id,
        )
        if require_strict_state
        else _opcache_probe_source(
            token_hash=token_hash,
            probe_id=probe_id,
        )
    )
    if token.encode("utf-8") in probe_source:
        raise PhpDeploymentError("OPcache probe source contains the raw token")
    operation_error: BaseException | None = None
    cleanup_error: BaseException | None = None
    opcache_action: str | None = None
    writer = (
        _write_remote_bytes
        if exclusive_writer is None
        else exclusive_writer
    )
    try:
        writer(
            client,
            remote_path,
            probe_source,
            mode=PRIVATE_FILE_MODE,
        )
        try:
            response = http_request(
                "POST",
                web_root + "/" + quote(filename, safe=""),
                {
                    "Accept": "application/json",
                    "Cache-Control": "no-store",
                    "X-BSIDE-OPcache-Token": token,
                },
                timeout,
            )
            if response.status != 200:
                raise PhpDeploymentError(
                    "OPcache reset probe did not return HTTP 200"
                )
            payload = _json_response(response, label="OPcache reset probe")
            if require_strict_state:
                expected_keys = {
                    "ok",
                    "opcache_action",
                    "probe_id",
                    "extension_loaded",
                    "reset_function",
                    "status_function",
                    "status_available",
                    "status_enabled",
                    "ini_enable",
                    "validate_timestamps",
                    "revalidate_freq",
                    "reset_result",
                }
                if (
                    set(payload) != expected_keys
                    or payload.get("ok") is not True
                    or payload.get("probe_id") != probe_id
                    or payload.get("extension_loaded") is not True
                    or payload.get("reset_function") is not True
                    or payload.get("status_function") is not True
                    or payload.get("status_available") is not True
                    or payload.get("ini_enable") != "1"
                    or payload.get("validate_timestamps") != "1"
                    or payload.get("revalidate_freq") != "2"
                ):
                    raise PhpDeploymentError(
                        "strict OPcache probe response is invalid"
                    )
                candidate_action = payload.get("opcache_action")
                status_enabled = payload.get("status_enabled")
                reset_result = payload.get("reset_result")
                if candidate_action == "disabled_verified":
                    if status_enabled is not False or reset_result is not None:
                        raise PhpDeploymentError(
                            "disabled OPcache evidence is invalid"
                        )
                    opcache_action = "disabled_verified"
                elif candidate_action == "reset_verified":
                    if status_enabled is not True or reset_result is not True:
                        raise PhpDeploymentError(
                            "reset OPcache evidence is invalid"
                        )
                    opcache_action = "reset_verified"
                else:
                    raise PhpDeploymentError(
                        "strict OPcache action is invalid"
                    )
            else:
                if (
                    set(payload) != {"ok", "opcache_reset", "probe_id"}
                    or payload.get("ok") is not True
                    or payload.get("opcache_reset") is not True
                    or payload.get("probe_id") != probe_id
                ):
                    raise PhpDeploymentError(
                        "OPcache reset probe response is invalid"
                    )
                opcache_action = "reset_verified"
        except BaseException as error:
            operation_error = error
    except BaseException as error:
        operation_error = error
    finally:
        try:
            if require_strict_state:
                _remove_exact_file_if_present(
                    client,
                    remote_path,
                    probe_source,
                    mode=PRIVATE_FILE_MODE,
                )
            else:
                _remove_remote_file_if_present(client, remote_path)
            if _lstat_or_none(client, remote_path) is not None:
                raise PhpDeploymentError(
                    "OPcache reset probe cleanup failed"
                )
        except BaseException as error:
            cleanup_error = error
    if cleanup_error is not None:
        if operation_error is not None:
            raise PhpDeploymentError(
                "OPcache reset failed and probe cleanup did not complete"
            ) from cleanup_error
        raise PhpDeploymentError(
            "OPcache reset probe cleanup failed"
        ) from cleanup_error
    if operation_error is not None:
        raise operation_error
    if opcache_action not in {"disabled_verified", "reset_verified"}:
        raise PhpDeploymentError(
            "OPcache verification produced no action"
        )
    return opcache_action


def verify_closed_v2_api(
    *,
    base_url: str,
    expected_sha: str,
    protected_token: str,
    http_request: HttpRequester = _default_http_request,
    timeout: float = 20.0,
    allow_http: bool = False,
) -> None:
    base = _validate_https_url(
        base_url,
        label="API v2 base URL",
        allow_http=allow_http,
    )
    if not base.endswith("/api/v2"):
        raise PhpDeploymentError("API v2 base URL must end with /api/v2")
    health = http_request(
        "GET",
        base + "/health",
        {"Accept": "application/json", "Cache-Control": "no-store"},
        timeout,
    )
    if health.status != 200:
        raise PhpDeploymentError("v2 health did not return HTTP 200")
    health_payload = _json_response(health, label="v2 health")
    expected_health = {
        "ok": True,
        "service": "bside-global-market-terminal",
        "code_revision": expected_sha,
        "schema_version": 11,
        "api_version": "v2",
    }
    if any(health_payload.get(key) != value for key, value in expected_health.items()):
        raise PhpDeploymentError("v2 health identity is invalid")
    if health.header("x-bside-api-version") != "v2":
        raise PhpDeploymentError("v2 health response header is invalid")

    openapi = http_request(
        "GET",
        base + "/openapi.yaml",
        {"Accept": "application/yaml", "Cache-Control": "no-store"},
        timeout,
    )
    if (
        openapi.status != 200
        or not openapi.header("content-type").casefold().startswith(
            "application/yaml"
        )
        or openapi.header("x-bside-api-version") != "v2"
        or b"x-schema-version: 11" not in openapi.body
    ):
        raise PhpDeploymentError("v2 OpenAPI smoke failed")

    missing = http_request(
        "GET",
        base + "/__bside_sftp_deploy_not_found__",
        {"Accept": "application/json", "Cache-Control": "no-store"},
        timeout,
    )
    missing_payload = _json_response(missing, label="v2 unknown route")
    if (
        missing.status != 404
        or missing_payload.get("api_version") != "v2"
        or missing_payload.get("error") != "not_found"
    ):
        raise PhpDeploymentError("v2 unknown-route smoke failed")

    events = http_request(
        "GET",
        base + "/events?limit=1",
        {"Accept": "application/json", "Cache-Control": "no-store"},
        timeout,
    )
    events_payload = _json_response(events, label="v2 closed events")
    if (
        events.status != 503
        or events_payload.get("api_version") != "v2"
        or events_payload.get("error") != "global_terminal_release_closed"
    ):
        raise PhpDeploymentError("v2 public data is not fail-closed")

    admin = http_request(
        "GET",
        base + "/admin/release-state",
        {"Accept": "application/json", "Cache-Control": "no-store"},
        timeout,
    )
    admin_payload = _json_response(admin, label="v2 unauthenticated admin")
    if (
        admin.status != 401
        or admin_payload.get("api_version") != "v2"
        or admin_payload.get("error") != "bearer_token_required"
    ):
        raise PhpDeploymentError("v2 admin authentication smoke failed")
    verify_protected_closed_state(
        base_url=base,
        protected_token=protected_token,
        http_request=http_request,
        timeout=timeout,
        allow_http=allow_http,
    )


def verify_rollback_health(
    *,
    url: str,
    http_request: HttpRequester = _default_http_request,
    timeout: float = 20.0,
    allow_http: bool = False,
) -> None:
    health_url = _validate_https_url(
        url,
        label="rollback health URL",
        allow_http=allow_http,
        allow_query=True,
    )
    response = http_request(
        "GET",
        health_url,
        {"Accept": "application/json", "Cache-Control": "no-store"},
        timeout,
    )
    payload = _json_response(response, label="rollback health")
    if response.status != 200 or payload.get("ok") is not True:
        raise PhpDeploymentError("rollback health smoke failed")


def inspect_remote_deployment(
    client: SftpClient,
    *,
    plan: LocalDeploymentPlan,
    remote_root: str,
) -> Mapping[str, object]:
    files: list[dict[str, object]] = []
    for artifact in plan.artifacts:
        target = _remote_join(remote_root, artifact.relative_path)
        attributes = _lstat_or_none(client, target)
        if attributes is None:
            files.append(
                {
                    "path": artifact.relative_path,
                    "action": "create",
                    "candidate_sha256": artifact.sha256,
                    "candidate_bytes": artifact.size,
                    "remote_exists": False,
                    "remote_sha256": None,
                    "remote_mode": None,
                }
            )
            continue
        current_mode = _mode(attributes)
        if stat.S_ISLNK(current_mode) or not stat.S_ISREG(current_mode):
            raise PhpDeploymentError("deployment target is not a regular file")
        current = _read_remote_bytes(client, target)
        current_sha256 = _sha256_bytes(current)
        action = "unchanged" if current_sha256 == artifact.sha256 else "replace"
        files.append(
            {
                "path": artifact.relative_path,
                "action": action,
                "candidate_sha256": artifact.sha256,
                "candidate_bytes": artifact.size,
                "remote_exists": True,
                "remote_sha256": current_sha256,
                "remote_mode": oct(stat.S_IMODE(current_mode)),
            }
        )
    return {
        "ok": True,
        "operation": "deploy-dry-run",
        "mutated_remote": False,
        "code_revision": plan.code_revision,
        "files": files,
    }


def local_plan_report(plan: LocalDeploymentPlan) -> Mapping[str, object]:
    return {
        "ok": True,
        "operation": "plan",
        "mutated_remote": False,
        "code_revision": plan.code_revision,
        "files": [
            {
                "position": index + 1,
                "path": artifact.relative_path,
                "sha256": artifact.sha256,
                "bytes": artifact.size,
                "mode": oct(artifact.mode),
                "commit_marker": (
                    artifact.relative_path == DEPLOYMENT_MANIFEST_NAME
                ),
            }
            for index, artifact in enumerate(plan.artifacts)
        ],
    }


def _stage_paths_for_cleanup(
    workspace: str,
    staged: Mapping[str, str],
) -> tuple[str, ...]:
    return tuple(staged.values()) + (
        _remote_join(workspace, "rename-a.blob"),
        _remote_join(workspace, "rename-b.blob"),
    )


def deploy_release(
    client: SftpClient,
    *,
    plan: LocalDeploymentPlan,
    remote_root: str = DEFAULT_REMOTE_ROOT,
    stage_root: str | None = None,
    backup_root: str | None = None,
    release_id: str | None = None,
    public_url_root: str,
    api_v2_base_url: str,
    rollback_health_url: str,
    protected_token: str,
    http_request: HttpRequester = _default_http_request,
    http_timeout: float = 20.0,
    allow_http: bool = False,
    gabia_compatibility: GabiaCoreCompatibility | None = None,
) -> Mapping[str, object]:
    _validated_protected_token(protected_token)
    validate_http_endpoint_binding(
        public_url_root=public_url_root,
        api_v2_base_url=api_v2_base_url,
        rollback_health_url=rollback_health_url,
        allow_http=allow_http,
    )
    root = _remote_absolute_path(remote_root, label="remote root")
    if gabia_compatibility is not None:
        if allow_http:
            raise PhpDeploymentError(
                "Gabia core compatibility requires HTTPS"
            )
        _validate_gabia_core_binding(
            gabia_compatibility,
            client=client,
            remote_root=root,
            public_url_root=public_url_root,
            api_v2_base_url=api_v2_base_url,
            rollback_health_url=rollback_health_url,
        )
    exclusive_writer = (
        None
        if gabia_compatibility is None
        else gabia_compatibility.exclusive_writer
    )
    private_root, safe_stage_root, safe_backup_root = _private_roots(
        root,
        stage_root,
        backup_root,
    )
    selected_release_id = _validate_release_id(
        release_id or _make_release_id(plan.code_revision)
    )
    _require_remote_directory(client, root, create=False)
    existing_revision = verify_existing_remote_release_identity(
        client,
        remote_root=root,
    )
    if existing_revision is not None:
        verify_closed_v2_api(
            base_url=api_v2_base_url,
            expected_sha=existing_revision,
            protected_token=protected_token,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=allow_http,
        )
    ensure_private_root_http_protection(
        client,
        remote_root=root,
        private_root=private_root,
        public_url_root=public_url_root,
        http_request=http_request,
        timeout=http_timeout,
        allow_http=allow_http,
        exclusive_writer=exclusive_writer,
        expected_policy=(
            None
            if gabia_compatibility is None
            else gabia_compatibility.private_policy
        ),
        expected_policy_mode=(
            None
            if gabia_compatibility is None
            else gabia_compatibility.private_policy_mode
        ),
        allowed_private_redirect=(
            None
            if gabia_compatibility is None
            else GABIA_PRIVATE_DENY_REDIRECT
        ),
    )
    lock_path = _acquire_deployment_lock(
        client,
        private_root,
        selected_release_id,
    )
    workspace = ""
    staged: dict[str, str] = {}
    backup: BackupSnapshot | None = None
    commit_started = False
    deployment_error: BaseException | None = None
    rollback_error: BaseException | None = None
    workspace_cleanup_error: BaseException | None = None
    lock_release_error: BaseException | None = None
    policy_preservation_error: BaseException | None = None
    opcache_action: str | None = None
    try:
        workspace = _create_private_workspace(
            client,
            safe_stage_root,
            selected_release_id,
        )
        _verify_posix_rename_capability(client, workspace)
        _stage_local_artifacts(
            client,
            workspace,
            plan,
            staged,
        )
        backup = capture_remote_backup(
            client,
            plan=plan,
            remote_root=root,
            backup_root=safe_backup_root,
            release_id=selected_release_id,
        )
        verify_remote_targets_match_snapshot(
            client,
            snapshot=backup,
        )
        if existing_revision is not None:
            verify_closed_v2_api(
                base_url=api_v2_base_url,
                expected_sha=existing_revision,
                protected_token=protected_token,
                http_request=http_request,
                timeout=http_timeout,
                allow_http=allow_http,
            )
            verify_remote_targets_match_snapshot(
                client,
                snapshot=backup,
            )
        commit_started = True
        _commit_staged_artifacts(
            client,
            plan=plan,
            staged=staged,
            remote_root=root,
        )
        opcache_action = reset_opcache_with_ephemeral_probe(
            client,
            remote_root=root,
            public_url_root=public_url_root,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=allow_http,
            exclusive_writer=exclusive_writer,
            require_strict_state=gabia_compatibility is not None,
        )
        verify_closed_v2_api(
            base_url=api_v2_base_url,
            expected_sha=plan.code_revision,
            protected_token=protected_token,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=allow_http,
        )
    except BaseException as error:
        deployment_error = error
        if commit_started and backup is not None and workspace:
            try:
                restore_remote_backup(
                    client,
                    snapshot=backup,
                    workspace=workspace,
                )
                reset_opcache_with_ephemeral_probe(
                    client,
                    remote_root=root,
                    public_url_root=public_url_root,
                    http_request=http_request,
                    timeout=http_timeout,
                    allow_http=allow_http,
                    exclusive_writer=exclusive_writer,
                    require_strict_state=gabia_compatibility is not None,
                )
                verify_rollback_health(
                    url=rollback_health_url,
                    http_request=http_request,
                    timeout=http_timeout,
                    allow_http=allow_http,
                )
            except BaseException as error_during_rollback:
                rollback_error = error_during_rollback
    finally:
        if workspace:
            try:
                _cleanup_workspace(
                    client,
                    workspace,
                    known_files=_stage_paths_for_cleanup(workspace, staged),
                )
            except BaseException as error:
                workspace_cleanup_error = error
        try:
            _release_deployment_lock(
                client,
                lock_path,
                exclusive_writer=exclusive_writer,
            )
        except BaseException as error:
            lock_release_error = error
        if gabia_compatibility is not None:
            try:
                _verify_gabia_private_policy(
                    client,
                    gabia_compatibility,
                )
            except BaseException as error:
                policy_preservation_error = error
    if deployment_error is not None:
        if rollback_error is not None:
            raise PhpDeploymentRollbackError(
                "deployment failed and automatic rollback did not complete"
            ) from rollback_error
        if not commit_started:
            if (
                workspace_cleanup_error is not None
                or lock_release_error is not None
                or policy_preservation_error is not None
            ):
                raise PhpDeploymentError(
                    "deployment stopped before commit, but private cleanup "
                    "or policy preservation did not complete"
                ) from (
                    workspace_cleanup_error
                    or lock_release_error
                    or policy_preservation_error
                )
            raise PhpDeploymentError(
                "deployment stopped before commit; remote targets were not changed"
            ) from deployment_error
        if (
            workspace_cleanup_error is not None
            or lock_release_error is not None
            or policy_preservation_error is not None
        ):
            raise PhpDeploymentError(
                "deployment failed and previous files were restored, "
                "but private cleanup or policy preservation did not complete"
            ) from (
                workspace_cleanup_error
                or lock_release_error
                or policy_preservation_error
            )
        raise PhpDeploymentError(
            "deployment failed safely; the previous files were restored"
        ) from deployment_error
    if workspace_cleanup_error is not None:
        raise PhpDeploymentError(
            "deployment files were applied and verified, "
            "but staging cleanup failed"
        ) from workspace_cleanup_error
    if lock_release_error is not None:
        raise PhpDeploymentError(
            "deployment files were applied and verified, "
            "but deployment lock cleanup failed"
        ) from lock_release_error
    if policy_preservation_error is not None:
        raise PhpDeploymentError(
            "deployment files were applied and verified, but the Gabia "
            "private HTTP policy was not preserved"
        ) from policy_preservation_error
    if backup is None:
        raise PhpDeploymentError("deployment completed without a durable backup")
    if opcache_action not in {"disabled_verified", "reset_verified"}:
        raise PhpDeploymentError(
            "deployment completed without verified OPcache state"
        )
    return {
        "ok": True,
        "operation": "deploy",
        "code_revision": plan.code_revision,
        "release_id": selected_release_id,
        "backup_manifest": (
            safe_backup_root
            + "/"
            + selected_release_id
            + "/backup-manifest.json"
        ),
        "files_deployed": len(plan.artifacts),
        "manifest_committed_last": True,
        "opcache_action": opcache_action,
        "opcache_reset": opcache_action == "reset_verified",
        "closed_smoke": True,
        "private_root_http_protected": True,
    }


def rollback_release(
    client: SftpClient,
    *,
    release_id: str,
    remote_root: str = DEFAULT_REMOTE_ROOT,
    stage_root: str | None = None,
    backup_root: str | None = None,
    public_url_root: str,
    api_v2_base_url: str,
    rollback_health_url: str,
    protected_token: str,
    http_request: HttpRequester = _default_http_request,
    http_timeout: float = 20.0,
    allow_http: bool = False,
    dry_run: bool = False,
    gabia_compatibility: GabiaCoreCompatibility | None = None,
    expected_current_sha: str | None = None,
) -> Mapping[str, object]:
    root = _remote_absolute_path(remote_root, label="remote root")
    if gabia_compatibility is not None:
        if allow_http or dry_run:
            raise PhpDeploymentError(
                "Gabia core compatibility is invalid for this operation"
            )
        _validate_gabia_core_binding(
            gabia_compatibility,
            client=client,
            remote_root=root,
            public_url_root=public_url_root,
            api_v2_base_url=api_v2_base_url,
            rollback_health_url=rollback_health_url,
        )
        if (
            expected_current_sha is None
            or SHA1_PATTERN.fullmatch(expected_current_sha) is None
            or gabia_compatibility.current_release_sha
            != expected_current_sha
        ):
            raise PhpDeploymentError(
                "Gabia rollback current release binding does not match"
            )
        current_release_sha = verify_existing_remote_release_identity(
            client,
            remote_root=root,
        )
        if current_release_sha != expected_current_sha:
            raise PhpDeploymentError(
                "current Gabia release SHA does not match"
            )
    exclusive_writer = (
        None
        if gabia_compatibility is None
        else gabia_compatibility.exclusive_writer
    )
    private_root, safe_stage_root, safe_backup_root = _private_roots(
        root,
        stage_root,
        backup_root,
    )
    safe_release_id = _validate_release_id(release_id)
    target_snapshot = load_remote_backup(
        client,
        backup_root=safe_backup_root,
        release_id=safe_release_id,
        expected_remote_root=root,
    )
    if dry_run:
        return {
            "ok": True,
            "operation": "rollback-dry-run",
            "mutated_remote": False,
            "release_id": safe_release_id,
            "candidate_code_revision": target_snapshot.candidate_code_revision,
            "files": [
                {
                    "path": item.relative_path,
                    "action": "restore" if item.existed else "delete",
                    "sha256": item.sha256,
                    "mode": None if item.mode is None else oct(item.mode),
                }
                for item in target_snapshot.files
            ],
        }
    _validated_protected_token(protected_token)
    validate_http_endpoint_binding(
        public_url_root=public_url_root,
        api_v2_base_url=api_v2_base_url,
        rollback_health_url=rollback_health_url,
        allow_http=allow_http,
    )
    ensure_private_root_http_protection(
        client,
        remote_root=root,
        private_root=private_root,
        public_url_root=public_url_root,
        http_request=http_request,
        timeout=http_timeout,
        allow_http=allow_http,
        exclusive_writer=exclusive_writer,
        expected_policy=(
            None
            if gabia_compatibility is None
            else gabia_compatibility.private_policy
        ),
        expected_policy_mode=(
            None
            if gabia_compatibility is None
            else gabia_compatibility.private_policy_mode
        ),
        allowed_private_redirect=(
            None
            if gabia_compatibility is None
            else GABIA_PRIVATE_DENY_REDIRECT
        ),
    )
    verify_protected_closed_state(
        base_url=api_v2_base_url,
        protected_token=protected_token,
        http_request=http_request,
        timeout=http_timeout,
        allow_http=allow_http,
    )
    rollback_operation_id = _validate_release_id(
        _make_release_id(
            target_snapshot.candidate_code_revision,
            prefix="rollback",
        )
    )
    lock_path = _acquire_deployment_lock(
        client,
        private_root,
        rollback_operation_id,
    )
    workspace = ""
    recovery_workspace = ""
    emergency: BackupSnapshot | None = None
    rollback_mutation_started = False
    rollback_error: BaseException | None = None
    recovery_error: BaseException | None = None
    workspace_cleanup_error: BaseException | None = None
    recovery_cleanup_error: BaseException | None = None
    lock_release_error: BaseException | None = None
    policy_preservation_error: BaseException | None = None
    opcache_action: str | None = None
    try:
        workspace = _create_private_workspace(
            client,
            safe_stage_root,
            rollback_operation_id,
        )
        _verify_posix_rename_capability(client, workspace)
        synthetic_plan = LocalDeploymentPlan(
            local_root=Path("."),
            code_revision=target_snapshot.candidate_code_revision,
            artifacts=tuple(
                LocalArtifact(
                    relative_path=path,
                    path=Path("."),
                    sha256="0" * 64,
                    size=1,
                )
                for path in DEFAULT_COMMIT_ORDER
            ),
        )
        emergency_id = _validate_release_id(
            _make_release_id(
                target_snapshot.candidate_code_revision,
                prefix="pre-rollback",
            )
        )
        emergency = capture_remote_backup(
            client,
            plan=synthetic_plan,
            remote_root=root,
            backup_root=safe_backup_root,
            release_id=emergency_id,
        )
        verify_protected_closed_state(
            base_url=api_v2_base_url,
            protected_token=protected_token,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=allow_http,
        )
        verify_remote_targets_match_snapshot(
            client,
            snapshot=emergency,
        )
        if gabia_compatibility is not None:
            final_current_release_sha = (
                verify_existing_remote_release_identity(
                    client,
                    remote_root=root,
                )
            )
            if final_current_release_sha != expected_current_sha:
                raise PhpDeploymentError(
                    "current Gabia release SHA changed before rollback"
                )
            verify_remote_targets_match_snapshot(
                client,
                snapshot=emergency,
            )
        rollback_mutation_started = True
        restore_remote_backup(
            client,
            snapshot=target_snapshot,
            workspace=workspace,
        )
        opcache_action = reset_opcache_with_ephemeral_probe(
            client,
            remote_root=root,
            public_url_root=public_url_root,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=allow_http,
            exclusive_writer=exclusive_writer,
            require_strict_state=gabia_compatibility is not None,
        )
        verify_rollback_health(
            url=rollback_health_url,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=allow_http,
        )
    except BaseException as error:
        rollback_error = error
        if rollback_mutation_started and emergency is not None and workspace:
            try:
                recovery_id = _validate_release_id(
                    _make_release_id(
                        target_snapshot.candidate_code_revision,
                        prefix="recovery",
                    )
                )
                recovery_workspace = _create_private_workspace(
                    client,
                    safe_stage_root,
                    recovery_id,
                )
                _verify_posix_rename_capability(
                    client,
                    recovery_workspace,
                )
                restore_remote_backup(
                    client,
                    snapshot=emergency,
                    workspace=recovery_workspace,
                )
                reset_opcache_with_ephemeral_probe(
                    client,
                    remote_root=root,
                    public_url_root=public_url_root,
                    http_request=http_request,
                    timeout=http_timeout,
                    allow_http=allow_http,
                    exclusive_writer=exclusive_writer,
                    require_strict_state=gabia_compatibility is not None,
                )
            except BaseException as error_during_recovery:
                recovery_error = error_during_recovery
    finally:
        if workspace:
            try:
                _cleanup_workspace(client, workspace, known_files=())
            except BaseException as error:
                workspace_cleanup_error = error
        if recovery_workspace:
            try:
                _cleanup_workspace(
                    client,
                    recovery_workspace,
                    known_files=(),
                )
            except BaseException as error:
                recovery_cleanup_error = error
        try:
            _release_deployment_lock(
                client,
                lock_path,
                exclusive_writer=exclusive_writer,
            )
        except BaseException as error:
            lock_release_error = error
        if gabia_compatibility is not None:
            try:
                _verify_gabia_private_policy(
                    client,
                    gabia_compatibility,
                )
            except BaseException as error:
                policy_preservation_error = error
    if rollback_error is not None:
        if not rollback_mutation_started:
            if (
                workspace_cleanup_error is not None
                or recovery_cleanup_error is not None
                or lock_release_error is not None
                or policy_preservation_error is not None
            ):
                raise PhpDeploymentError(
                    "rollback stopped before mutation, but private cleanup "
                    "or policy preservation did not complete"
                ) from (
                    workspace_cleanup_error
                    or recovery_cleanup_error
                    or lock_release_error
                    or policy_preservation_error
                )
            raise PhpDeploymentError(
                "rollback stopped before mutation; current targets were not "
                "overwritten"
            ) from rollback_error
        if recovery_error is not None:
            raise PhpDeploymentRollbackError(
                "rollback failed and the pre-rollback files could not be restored"
            ) from recovery_error
        if (
            workspace_cleanup_error is not None
            or recovery_cleanup_error is not None
            or lock_release_error is not None
            or policy_preservation_error is not None
        ):
            raise PhpDeploymentError(
                "rollback failed and pre-rollback files were restored, "
                "but private cleanup or policy preservation did not complete"
            ) from (
                workspace_cleanup_error
                or recovery_cleanup_error
                or lock_release_error
                or policy_preservation_error
            )
        raise PhpDeploymentError(
            "rollback failed safely; pre-rollback files were restored"
        ) from rollback_error
    if (
        workspace_cleanup_error is not None
        or recovery_cleanup_error is not None
    ):
        raise PhpDeploymentError(
            "rollback was applied and verified, but staging cleanup failed"
        ) from (workspace_cleanup_error or recovery_cleanup_error)
    if lock_release_error is not None:
        raise PhpDeploymentError(
            "rollback was applied and verified, but lock cleanup failed"
        ) from lock_release_error
    if policy_preservation_error is not None:
        raise PhpDeploymentError(
            "rollback was applied and verified, but the Gabia private "
            "HTTP policy was not preserved"
        ) from policy_preservation_error
    if opcache_action not in {"disabled_verified", "reset_verified"}:
        raise PhpDeploymentError(
            "rollback completed without verified OPcache state"
        )
    return {
        "ok": True,
        "operation": "rollback",
        "release_id": safe_release_id,
        "restored_files": sum(
            1 for item in target_snapshot.files if item.existed
        ),
        "removed_new_files": sum(
            1 for item in target_snapshot.files if not item.existed
        ),
        "opcache_action": opcache_action,
        "opcache_reset": opcache_action == "reset_verified",
        "rollback_smoke": True,
        "private_root_http_protected": True,
        "emergency_backup_release_id": (
            None if emergency is None else emergency.release_id
        ),
    }


def _add_local_plan_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--local-root",
        type=Path,
        default=Path("deploy/activist"),
    )
    parser.add_argument("--expected-sha", required=True)


def _add_sftp_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--ssh-host")
    parser.add_argument("--ssh-port")
    parser.add_argument("--ssh-user")
    parser.add_argument(
        "--ssh-password-env",
        default="SSH_PASSWORD",
        help="Name of the environment variable containing the SSH password",
    )
    parser.add_argument("--ssh-host-key-sha256")
    parser.add_argument(
        "--ssh-allow-legacy-rsa-sha1",
        "--allow-legacy-ssh-rsa",
        dest="ssh_allow_legacy_rsa_sha1",
        action="store_true",
        help=(
            "Explicitly allow pinned legacy ssh-rsa/SHA-1 for one exact host"
        ),
    )
    parser.add_argument(
        "--ssh-legacy-rsa-sha1-host",
        help="Exact host receiving the legacy ssh-rsa/SHA-1 exception",
    )
    parser.add_argument("--connect-timeout", type=int, default=15)
    parser.add_argument("--auth-timeout", type=int, default=15)
    parser.add_argument("--remote-root", default=DEFAULT_REMOTE_ROOT)
    parser.add_argument("--stage-root")
    parser.add_argument("--backup-root")
    parser.add_argument(
        "--gabia-core-compatibility-host",
        help=(
            "Explicit exact-host opt-in for the probed Gabia SFTP "
            "exclusive-create compatibility path"
        ),
    )


def _add_http_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--public-url-root")
    parser.add_argument("--api-v2-base-url")
    parser.add_argument("--rollback-health-url")
    parser.add_argument(
        "--protected-token-env",
        help=(
            "Explicit environment variable containing the >=32-byte ops/admin "
            "Bearer token"
        ),
    )
    parser.add_argument("--http-timeout", type=float, default=20.0)
    parser.add_argument(
        "--allow-http",
        action="store_true",
        help="Test-only opt-in for non-TLS local endpoints",
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Pinned SFTP PHP deployment with byte verification, automatic "
            "rollback, and an ephemeral OPcache reset probe"
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)
    plan_parser = commands.add_parser(
        "plan",
        help="Validate and print the local immutable deployment plan",
    )
    _add_local_plan_arguments(plan_parser)

    deploy_parser = commands.add_parser(
        "deploy",
        help="Deploy or inspect the exact release through pinned SFTP",
    )
    _add_local_plan_arguments(deploy_parser)
    _add_sftp_arguments(deploy_parser)
    _add_http_arguments(deploy_parser)
    deploy_parser.add_argument("--release-id")
    deploy_parser.add_argument("--dry-run", action="store_true")
    deploy_parser.add_argument(
        "--confirm-production-write",
        help="Must exactly match --expected-sha for a mutating deployment",
    )

    rollback_parser = commands.add_parser(
        "rollback",
        help="Restore an exact remote backup through pinned SFTP",
    )
    _add_sftp_arguments(rollback_parser)
    _add_http_arguments(rollback_parser)
    rollback_parser.add_argument("--release-id", required=True)
    rollback_parser.add_argument("--dry-run", action="store_true")
    rollback_parser.add_argument("--expected-current-sha")
    rollback_parser.add_argument("--confirm-rollback-release-id")
    rollback_parser.add_argument("--confirm-rollback-current-sha")
    return parser


def _print_report(report: Mapping[str, object]) -> None:
    print(
        json.dumps(
            report,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def _required_cli_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PhpDeploymentError(f"{label} is required for a mutating operation")
    return value.strip()


def _protected_token_from_environment(
    environment_name: object,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    if (
        not isinstance(environment_name, str)
        or ENVIRONMENT_NAME_PATTERN.fullmatch(environment_name) is None
    ):
        raise PhpDeploymentError(
            "--protected-token-env is required and must name an environment variable"
        )
    environment = os.environ if environ is None else environ
    value = environment.get(environment_name)
    if value is None:
        raise PhpDeploymentError(
            "protected Bearer token environment variable is missing"
        )
    return _validated_protected_token(value)


def _prepare_cli_gabia_compatibility(
    client: SftpClient,
    *,
    args: argparse.Namespace,
    options: SshTunnelOptions,
    expected_current_sha: str | None = None,
) -> GabiaCoreCompatibility | None:
    compatibility_host = getattr(
        args,
        "gabia_core_compatibility_host",
        None,
    )
    if compatibility_host is None:
        if options.host == GABIA_COMPATIBILITY_SSH_HOST:
            raise PhpDeploymentError(
                "Gabia core compatibility requires explicit exact-host opt-in"
            )
        return None
    return prepare_gabia_core_compatibility(
        client,
        ssh_options=options,
        compatibility_host=_required_cli_text(
            compatibility_host,
            label="Gabia core compatibility host",
        ),
        remote_root=args.remote_root,
        public_url_root=_required_cli_text(
            args.public_url_root,
            label="public URL root",
        ),
        api_v2_base_url=_required_cli_text(
            args.api_v2_base_url,
            label="API v2 base URL",
        ),
        rollback_health_url=_required_cli_text(
            args.rollback_health_url,
            label="rollback health URL",
        ),
        expected_current_sha=expected_current_sha,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        if args.command == "plan":
            plan = build_local_deployment_plan(
                args.local_root,
                expected_sha=args.expected_sha,
            )
            verify_release_checkout(plan)
            _print_report(local_plan_report(plan))
            return 0

        if args.command == "deploy":
            if (
                args.dry_run
                and args.gabia_core_compatibility_host is not None
            ):
                raise PhpDeploymentError(
                    "Gabia compatibility probe is not available in dry-run"
                )
            plan = build_local_deployment_plan(
                args.local_root,
                expected_sha=args.expected_sha,
            )
            verify_release_checkout(
                plan,
                require_repository_clean=not args.dry_run,
            )
            if not args.dry_run:
                confirm_production_release(
                    plan.code_revision,
                    args.confirm_production_write,
                )
            options = ssh_sftp_options_from_args(args)
            protected_token = (
                ""
                if args.dry_run
                else _protected_token_from_environment(
                    args.protected_token_env
                )
            )
            with ParamikoPinnedSftpSession(options) as client:
                if args.dry_run:
                    report = inspect_remote_deployment(
                        client,
                        plan=plan,
                        remote_root=args.remote_root,
                    )
                else:
                    compatibility = _prepare_cli_gabia_compatibility(
                        client,
                        args=args,
                        options=options,
                    )
                    report = deploy_release(
                        client,
                        plan=plan,
                        remote_root=args.remote_root,
                        stage_root=args.stage_root,
                        backup_root=args.backup_root,
                        release_id=args.release_id,
                        public_url_root=_required_cli_text(
                            args.public_url_root,
                            label="public URL root",
                        ),
                        api_v2_base_url=_required_cli_text(
                            args.api_v2_base_url,
                            label="API v2 base URL",
                        ),
                        rollback_health_url=_required_cli_text(
                            args.rollback_health_url,
                            label="rollback health URL",
                        ),
                        protected_token=protected_token,
                        http_timeout=args.http_timeout,
                        allow_http=args.allow_http,
                        gabia_compatibility=compatibility,
                    )
            _print_report(report)
            return 0

        if (
            args.dry_run
            and args.gabia_core_compatibility_host is not None
        ):
            raise PhpDeploymentError(
                "Gabia compatibility probe is not available in dry-run"
            )
        expected_current_sha: str | None = None
        if (
            not args.dry_run
            and args.gabia_core_compatibility_host is not None
        ):
            expected_current_sha = _required_cli_text(
                args.expected_current_sha,
                label="expected current release SHA",
            )
            confirm_production_rollback(
                args.release_id,
                args.confirm_rollback_release_id,
                expected_current_sha,
                args.confirm_rollback_current_sha,
            )
        options = ssh_sftp_options_from_args(args)
        protected_token = (
            ""
            if args.dry_run
            else _protected_token_from_environment(
                args.protected_token_env
            )
        )
        with ParamikoPinnedSftpSession(options) as client:
            compatibility = (
                None
                if args.dry_run
                else _prepare_cli_gabia_compatibility(
                    client,
                    args=args,
                    options=options,
                    expected_current_sha=expected_current_sha,
                )
            )
            report = rollback_release(
                client,
                release_id=args.release_id,
                remote_root=args.remote_root,
                stage_root=args.stage_root,
                backup_root=args.backup_root,
                public_url_root=(
                    ""
                    if args.dry_run
                    else _required_cli_text(
                        args.public_url_root,
                        label="public URL root",
                    )
                ),
                api_v2_base_url=(
                    ""
                    if args.dry_run
                    else _required_cli_text(
                        getattr(args, "api_v2_base_url", None),
                        label="API v2 base URL",
                    )
                ),
                rollback_health_url=(
                    ""
                    if args.dry_run
                    else _required_cli_text(
                        args.rollback_health_url,
                        label="rollback health URL",
                    )
                ),
                protected_token=protected_token,
                http_timeout=args.http_timeout,
                allow_http=args.allow_http,
                dry_run=args.dry_run,
                gabia_compatibility=compatibility,
                expected_current_sha=expected_current_sha,
            )
        _print_report(report)
        return 0
    except PhpDeploymentError:
        print(
            "PHP SFTP operation failed safely; inspect protected diagnostics.",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "PHP SFTP operation failed safely; inspect protected diagnostics.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
