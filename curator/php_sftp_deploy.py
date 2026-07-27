from __future__ import annotations

import argparse
import errno
import hashlib
import json
import math
import os
import posixpath
import re
import secrets
import socket
import stat
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
    "migrations/012_dart_credential_pool.sql",
    "openapi-v2.yaml",
    V1_OPENAPI_NAME,
    "governance_v2_write.php",
    "governance_v2.php",
    "governance_v1.php",
    "api.php",
    DEPLOYMENT_MANIFEST_NAME,
)

# Exact manifest shape deployed by the schema 11 release. This literal must not
# be derived from CORE_API_FILES: adding a future core file must not silently
# widen the one-time schema 11 -> 12 bridge.
LEGACY_SCHEMA_11_CORE_API_FILES = (
    ".htaccess",
    "api.php",
    "governance_v1.php",
    "governance_v2.php",
    "governance_v2_write.php",
    "openapi.yaml",
    "openapi-v2.yaml",
    "migrations/011_global_terminal_v2.sql",
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
SCHEMA_BRIDGE_ROLLBACK_RELEASE_ID_ENV = (
    "BSIDE_SCHEMA_BRIDGE_ROLLBACK_RELEASE_ID"
)
SCHEMA_BRIDGE_ROLLBACK_CURRENT_SHA_ENV = (
    "BSIDE_SCHEMA_BRIDGE_ROLLBACK_CURRENT_SHA"
)
SCHEMA_BRIDGE_ROLLBACK_PREVIOUS_SHA_ENV = (
    "BSIDE_SCHEMA_BRIDGE_ROLLBACK_PREVIOUS_SHA"
)
SCHEMA_BRIDGE_ROLLBACK_BACKUP_SHA256_ENV = (
    "BSIDE_SCHEMA_BRIDGE_ROLLBACK_BACKUP_SHA256"
)
SCHEMA_BRIDGE_DEPLOY_PREVIOUS_SHA_ENV = (
    "BSIDE_SCHEMA_BRIDGE_DEPLOY_PREVIOUS_SHA"
)
SCHEMA_BRIDGE_DART_DISABLED_EVIDENCE_ENV = (
    "BSIDE_SCHEMA_BRIDGE_DART_DISABLED_EVIDENCE"
)
SCHEMA_BRIDGE_STALE_LOCK_OWNER_ENV = (
    "BSIDE_SCHEMA_BRIDGE_STALE_LOCK_OWNER_RELEASE_ID"
)
SCHEMA_BRIDGE_STALE_LOCK_WRITER_ABSENCE_ENV = (
    "BSIDE_SCHEMA_BRIDGE_STALE_LOCK_WRITER_ABSENCE_EVIDENCE"
)
SCHEMA_BRIDGE_STALE_LOCK_FIRST_OBSERVED_ENV = (
    "BSIDE_SCHEMA_BRIDGE_STALE_LOCK_FIRST_OBSERVED_AT"
)
PRIVATE_REPORT_ROOT_ENV = "BSIDE_PRIVATE_REPORT_ROOT"
DART_DISABLED_EVIDENCE_PATTERN = re.compile(
    r"^github-variable:DART_OFFICIAL_INGEST_ENABLED=false@"
    r"[A-Za-z0-9][A-Za-z0-9:._/@=+-]{7,191}$"
)
STALE_LOCK_WRITER_ABSENCE_PATTERN = re.compile(
    r"^github-actions:no-running-php-writers@"
    r"(?P<issued_at>[0-9]{8}T[0-9]{6}Z):"
    r"owner_sha256=(?P<owner_sha256>[0-9a-f]{64}):"
    r"acquired_at_sha256=(?P<acquired_at_sha256>[0-9a-f]{64}):"
    r"nonce=(?P<nonce>[0-9a-f]{32})$"
)
STALE_LOCK_OWNERLESS = "ownerless"
STALE_LOCK_MINIMUM_AGE = timedelta(minutes=15)
STALE_LOCK_EVIDENCE_MAX_AGE = timedelta(minutes=10)
STALE_LOCK_EVIDENCE_FUTURE_SKEW = timedelta(minutes=1)
SCHEMA_BRIDGE_STALE_LOCK_PREFIXES = (
    "php-v2-",
    "schema11-bridge-rollback-",
    "schema11-bridge-abort-",
)
# This recovery path restores the exact production predecessor. The candidate
# is not hard-coded because this recovery implementation itself changes the
# final release SHA; it is instead bound to a clean local checkout, its
# manifest and the exact remote DeployCore backup.
ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA = (
    "c06b374d09e18b29a14cce46719fe4f1842f9047"
)
GABIA_COMPATIBILITY_SSH_HOST = "alignpartnerscap.com"
GABIA_SSH_HOST_KEY_SHA256 = (
    "SHA256:4Y2J13Nis0NOKupLJCOnr2w5X2UdBZH78TkZMVJCVLo"
)
GABIA_REMOTE_ROOT = "/www_root/activist"
GABIA_PUBLIC_URL_ROOT = "https://alignpe.gabia.io/activist"
GABIA_API_V2_BASE_URL = (
    "https://alignpe.gabia.io/activist/api.php/api/v2"
)
GABIA_API_V1_BASE_URL = (
    "https://alignpe.gabia.io/activist/api.php/api/v1"
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

    def listdir(self, path: str) -> list[str]: ...

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
    manifest_path: str
    manifest_sha256: str
    manifest_size: int

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
BridgeReportUpdater = Callable[[str, Mapping[str, object]], None]
BridgeReportLoader = Callable[[], Mapping[str, object]]


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


def confirm_one_time_schema_bridge_rollback(
    *,
    release_id: str,
    release_id_confirmation: object,
    expected_current_sha: str,
    current_sha_confirmation: object,
    expected_previous_sha: str,
    previous_sha_confirmation: object,
    expected_backup_manifest_sha256: str | None,
    backup_sha256_confirmation: object,
    dart_disabled_evidence: str | None = None,
    environ: Mapping[str, str] | None = None,
    allow_missing_backup: bool = False,
) -> None:
    """Bind the one-time schema 12-over-11 PHP recovery to exact evidence."""

    safe_release_id = _validate_release_id(release_id)
    if (
        SHA1_PATTERN.fullmatch(expected_current_sha) is None
        or expected_previous_sha != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
        or (
            expected_backup_manifest_sha256 is not None
            and SHA256_PATTERN.fullmatch(
                expected_backup_manifest_sha256
            )
            is None
        )
        or (
            expected_backup_manifest_sha256 is None
            and not allow_missing_backup
        )
        or dart_disabled_evidence is None
        or DART_DISABLED_EVIDENCE_PATTERN.fullmatch(dart_disabled_evidence)
        is None
    ):
        raise PhpDeploymentError(
            "one-time schema bridge rollback identity is invalid"
        )
    environment = os.environ if environ is None else environ
    backup_confirmation_valid = (
        backup_sha256_confirmation == expected_backup_manifest_sha256
        and (
            environment.get(
                SCHEMA_BRIDGE_ROLLBACK_BACKUP_SHA256_ENV
            )
            == expected_backup_manifest_sha256
            if expected_backup_manifest_sha256 is not None
            else environment.get(
                SCHEMA_BRIDGE_ROLLBACK_BACKUP_SHA256_ENV
            )
            in {None, ""}
        )
    )
    if (
        release_id_confirmation != safe_release_id
        or environment.get(SCHEMA_BRIDGE_ROLLBACK_RELEASE_ID_ENV)
        != safe_release_id
        or current_sha_confirmation != expected_current_sha
        or environment.get(SCHEMA_BRIDGE_ROLLBACK_CURRENT_SHA_ENV)
        != expected_current_sha
        or previous_sha_confirmation != expected_previous_sha
        or environment.get(SCHEMA_BRIDGE_ROLLBACK_PREVIOUS_SHA_ENV)
        != expected_previous_sha
        or not backup_confirmation_valid
        or environment.get(SCHEMA_BRIDGE_DART_DISABLED_EVIDENCE_ENV)
        != dart_disabled_evidence
    ):
        raise PhpDeploymentError(
            "one-time schema bridge rollback confirmation does not match"
        )


def _parse_utc_timestamp(value: str, *, compact: bool = False) -> datetime:
    try:
        if compact:
            parsed = datetime.strptime(
                value,
                "%Y%m%dT%H%M%SZ",
            ).replace(tzinfo=timezone.utc)
        else:
            parsed = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
    except ValueError as error:
        raise PhpDeploymentError("stale lock timestamp is invalid") from error
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() != timedelta(0)
    ):
        raise PhpDeploymentError("stale lock timestamp is not UTC")
    return parsed


def _validated_stale_lock_writer_absence(
    evidence: str,
    *,
    expected_owner_sha256: str | None = None,
    acquired_at_reference: str | None = None,
    now: datetime | None = None,
) -> Mapping[str, str]:
    match = STALE_LOCK_WRITER_ABSENCE_PATTERN.fullmatch(evidence)
    if match is None:
        raise PhpDeploymentError(
            "stale lock writer-absence evidence syntax is invalid"
        )
    observed_now = datetime.now(timezone.utc) if now is None else now
    issued_at = _parse_utc_timestamp(
        match.group("issued_at"),
        compact=True,
    )
    if (
        issued_at < observed_now - STALE_LOCK_EVIDENCE_MAX_AGE
        or issued_at > observed_now + STALE_LOCK_EVIDENCE_FUTURE_SKEW
    ):
        raise PhpDeploymentError(
            "stale lock writer-absence evidence is not fresh"
        )
    if (
        expected_owner_sha256 is not None
        and not secrets.compare_digest(
            match.group("owner_sha256"),
            expected_owner_sha256,
        )
    ):
        raise PhpDeploymentError(
            "stale lock evidence owner digest does not match"
        )
    if acquired_at_reference is not None:
        acquired_at = _parse_utc_timestamp(acquired_at_reference)
        if acquired_at > observed_now - STALE_LOCK_MINIMUM_AGE:
            raise PhpDeploymentError(
                "deployment lock has not reached the minimum stale age"
            )
        expected_acquired_digest = _sha256_bytes(
            acquired_at_reference.encode("ascii")
        )
        if not secrets.compare_digest(
            match.group("acquired_at_sha256"),
            expected_acquired_digest,
        ):
            raise PhpDeploymentError(
                "stale lock evidence acquisition digest does not match"
            )
    return dict(match.groupdict())


def confirm_schema_bridge_stale_lock_takeover(
    *,
    owner_release_id: str | None,
    owner_release_id_confirmation: str | None,
    writer_absence_evidence: str | None,
    first_observed_at: str | None = None,
    environ: Mapping[str, str] | None = None,
    allow_recorded_evidence: bool = False,
) -> None:
    """Require independent operator and Actions evidence before lock takeover."""

    values = (
        owner_release_id,
        owner_release_id_confirmation,
        writer_absence_evidence,
        first_observed_at,
    )
    if all(value is None for value in values[:3]) and first_observed_at is None:
        return
    if any(value is None for value in values[:3]):
        raise PhpDeploymentError(
            "stale lock takeover confirmation is incomplete"
        )
    assert owner_release_id is not None
    assert owner_release_id_confirmation is not None
    assert writer_absence_evidence is not None
    safe_owner = (
        STALE_LOCK_OWNERLESS
        if owner_release_id == STALE_LOCK_OWNERLESS
        else _validate_release_id(owner_release_id)
    )
    environment = os.environ if environ is None else environ
    if (
        safe_owner != owner_release_id
        or owner_release_id_confirmation != safe_owner
        or (
            safe_owner != STALE_LOCK_OWNERLESS
            and not safe_owner.startswith(
                SCHEMA_BRIDGE_STALE_LOCK_PREFIXES
            )
        )
        or environment.get(SCHEMA_BRIDGE_STALE_LOCK_OWNER_ENV)
        != safe_owner
        or environment.get(
            SCHEMA_BRIDGE_STALE_LOCK_WRITER_ABSENCE_ENV
        )
        != writer_absence_evidence
        or environment.get(
            SCHEMA_BRIDGE_STALE_LOCK_FIRST_OBSERVED_ENV,
            "",
        )
        != (first_observed_at or "")
        or (
            safe_owner == STALE_LOCK_OWNERLESS
            and first_observed_at is None
        )
        or (
            safe_owner != STALE_LOCK_OWNERLESS
            and first_observed_at is not None
        )
    ):
        raise PhpDeploymentError(
            "stale lock takeover confirmation does not match"
        )
    if (
        STALE_LOCK_WRITER_ABSENCE_PATTERN.fullmatch(
            writer_absence_evidence
        )
        is None
    ):
        raise PhpDeploymentError(
            "schema bridge stale lock evidence is invalid"
        )
    if allow_recorded_evidence:
        return
    if safe_owner == STALE_LOCK_OWNERLESS:
        assert first_observed_at is not None
        _validated_stale_lock_writer_absence(
            writer_absence_evidence,
            acquired_at_reference=first_observed_at,
        )
    else:
        _validated_stale_lock_writer_absence(
            writer_absence_evidence
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


def _read_stable_remote_regular_bytes(
    client: SftpClient,
    path: str,
    *,
    required_mode: int | None = None,
    expected_size: int | None = None,
    label: str,
) -> bytes:
    """Read a regular file while proving its public metadata stayed fixed."""

    before = _lstat_or_none(client, path)
    if before is None:
        raise PhpDeploymentError(f"{label} is missing")
    before_mode = _mode(before)
    before_size = getattr(before, "st_size", None)
    if (
        stat.S_ISLNK(before_mode)
        or not stat.S_ISREG(before_mode)
        or not isinstance(before_size, int)
        or before_size < 0
        or (
            required_mode is not None
            and stat.S_IMODE(before_mode) != required_mode
        )
        or (expected_size is not None and before_size != expected_size)
    ):
        raise PhpDeploymentError(f"{label} metadata is invalid")
    content = _read_remote_bytes(client, path)
    after = _lstat_or_none(client, path)
    if after is None:
        raise PhpDeploymentError(f"{label} changed during verification")
    after_mode = _mode(after)
    after_size = getattr(after, "st_size", None)
    if (
        stat.S_ISLNK(after_mode)
        or not stat.S_ISREG(after_mode)
        or stat.S_IMODE(after_mode) != stat.S_IMODE(before_mode)
        or after_size != before_size
        or len(content) != before_size
        or (
            required_mode is not None
            and stat.S_IMODE(after_mode) != required_mode
        )
        or (expected_size is not None and after_size != expected_size)
    ):
        raise PhpDeploymentError(f"{label} changed during verification")
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
            manifest_path="",
            manifest_sha256="",
            manifest_size=0,
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
        return BackupSnapshot(
            release_id=snapshot.release_id,
            backup_directory=snapshot.backup_directory,
            remote_root=snapshot.remote_root,
            candidate_code_revision=snapshot.candidate_code_revision,
            files=snapshot.files,
            manifest_path=manifest_path,
            manifest_sha256=_sha256_bytes(manifest_bytes),
            manifest_size=len(manifest_bytes),
        )
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
        manifest_path=manifest_path,
        manifest_sha256=_sha256_bytes(raw),
        manifest_size=len(raw),
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


def verify_one_time_schema_bridge_backup(
    client: SftpClient,
    *,
    snapshot: BackupSnapshot,
    expected_current_sha: str,
    expected_previous_sha: str,
    expected_manifest_sha256: str,
) -> None:
    """Verify the exact DeployCore backup that precedes the schema bridge."""

    if (
        SHA1_PATTERN.fullmatch(expected_current_sha) is None
        or expected_previous_sha != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
        or snapshot.candidate_code_revision != expected_current_sha
        or SHA256_PATTERN.fullmatch(expected_manifest_sha256) is None
        or snapshot.manifest_path
        != _remote_join(snapshot.backup_directory, "backup-manifest.json")
        or snapshot.manifest_size < 1
    ):
        raise PhpDeploymentError(
            "one-time schema bridge backup identity does not match"
        )
    if not secrets.compare_digest(
        snapshot.manifest_sha256,
        expected_manifest_sha256,
    ):
        raise PhpDeploymentError(
            "one-time schema bridge backup manifest hash does not match"
        )
    previous_release = inspect_backup_previous_release(
        client,
        snapshot=snapshot,
    )
    if (
        previous_release is None
        or previous_release[0] != expected_previous_sha
        or previous_release[1] != frozenset(LEGACY_SCHEMA_11_CORE_API_FILES)
    ):
        raise PhpDeploymentError(
            "one-time schema bridge previous manifest identity is invalid"
        )
    files = snapshot.file_by_path
    for relative_path in (
        *LEGACY_SCHEMA_11_CORE_API_FILES,
        DEPLOYMENT_MANIFEST_NAME,
    ):
        predecessor = files.get(relative_path)
        if (
            predecessor is None
            or not predecessor.existed
            or predecessor.mode != DEFAULT_FILE_MODE
        ):
            raise PhpDeploymentError(
                "one-time schema bridge predecessor mode is not exact 0644"
            )
    migration_012 = files.get("migrations/012_dart_credential_pool.sql")
    if migration_012 is None or migration_012.existed:
        raise PhpDeploymentError(
            "one-time schema bridge backup is not a schema 11 predecessor"
        )


def confirm_one_time_schema_bridge_deploy(
    *,
    expected_previous_sha: str,
    previous_sha_confirmation: object,
    dart_disabled_evidence: str,
    environ: Mapping[str, str] | None = None,
) -> None:
    """Bind the schema bridge to c06 and durable DART-off evidence."""

    if (
        expected_previous_sha != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
        or previous_sha_confirmation != expected_previous_sha
        or DART_DISABLED_EVIDENCE_PATTERN.fullmatch(dart_disabled_evidence)
        is None
    ):
        raise PhpDeploymentError(
            "one-time schema bridge deploy identity is invalid"
        )
    environment = os.environ if environ is None else environ
    if (
        environment.get(SCHEMA_BRIDGE_DEPLOY_PREVIOUS_SHA_ENV)
        != expected_previous_sha
        or environment.get(SCHEMA_BRIDGE_DART_DISABLED_EVIDENCE_ENV)
        != dart_disabled_evidence
    ):
        raise PhpDeploymentError(
            "one-time schema bridge deploy confirmation does not match"
        )


def inspect_backup_previous_release(
    client: SftpClient,
    *,
    snapshot: BackupSnapshot,
) -> tuple[str, frozenset[str]] | None:
    """Validate and identify the deployment manifest embedded in a backup."""

    files = snapshot.file_by_path
    previous_manifest = files.get(DEPLOYMENT_MANIFEST_NAME)
    if previous_manifest is None:
        raise PhpDeploymentError("remote backup has no manifest entry")
    if not previous_manifest.existed:
        return None
    if (
        previous_manifest.backup_blob is None
        or previous_manifest.sha256 is None
    ):
        raise PhpDeploymentError("remote backup previous manifest is incomplete")
    raw = _read_remote_bytes(
        client,
        _remote_join(
            snapshot.backup_directory,
            previous_manifest.backup_blob,
        ),
    )
    if not secrets.compare_digest(
        _sha256_bytes(raw),
        previous_manifest.sha256,
    ):
        raise PhpDeploymentError("remote backup previous manifest bytes changed")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PhpDeploymentError(
            "remote backup previous manifest is invalid"
        ) from error
    if (
        not isinstance(payload, dict)
        or set(payload) != {"schema_version", "code_revision", "files"}
        or payload.get("schema_version") != 1
        or not isinstance(payload.get("code_revision"), str)
        or SHA1_PATTERN.fullmatch(payload["code_revision"]) is None
        or not isinstance(payload.get("files"), dict)
    ):
        raise PhpDeploymentError(
            "remote backup previous manifest identity is invalid"
        )
    manifest_files = frozenset(payload["files"])
    if manifest_files not in {
        frozenset(CORE_API_FILES),
        frozenset(LEGACY_SCHEMA_11_CORE_API_FILES),
    }:
        raise PhpDeploymentError(
            "remote backup previous manifest file set is unsupported"
        )
    for relative_path in manifest_files:
        item = files.get(relative_path)
        expected_digest = payload["files"].get(relative_path)
        if (
            item is None
            or not item.existed
            or item.sha256 is None
            or expected_digest != item.sha256
        ):
            raise PhpDeploymentError(
                "remote backup previous release bytes are incomplete"
            )
    return payload["code_revision"], manifest_files


def verify_existing_remote_release_identity(
    client: SftpClient,
    *,
    remote_root: str,
    expected_core_files: Sequence[str] = CORE_API_FILES,
    required_mode: int | None = None,
) -> str | None:
    """Verify the current v2 manifest and bytes, or attest a first deploy."""

    selected_core_files = tuple(expected_core_files)
    if selected_core_files not in (
        CORE_API_FILES,
        LEGACY_SCHEMA_11_CORE_API_FILES,
    ):
        raise PhpDeploymentError(
            "existing release core-file expectation is unsupported"
        )
    root = _remote_absolute_path(remote_root, label="remote root")
    if (
        selected_core_files == LEGACY_SCHEMA_11_CORE_API_FILES
        and _lstat_or_none(
            client,
            _remote_join(
                root,
                "migrations/012_dart_credential_pool.sql",
            ),
        )
        is not None
    ):
        raise PhpDeploymentError(
            "schema 11 release contains a stray migration 012 file"
        )
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
    if (
        stat.S_ISLNK(manifest_mode)
        or not stat.S_ISREG(manifest_mode)
        or (
            required_mode is not None
            and stat.S_IMODE(manifest_mode) != required_mode
        )
    ):
        raise PhpDeploymentError(
            "existing deployment manifest is not a regular file"
        )
    raw = _read_stable_remote_regular_bytes(
        client,
        manifest_path,
        required_mode=required_mode,
        label="existing deployment manifest",
    )
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
        or set(payload["files"]) != set(selected_core_files)
    ):
        raise PhpDeploymentError(
            "existing deployment manifest identity is invalid"
        )
    for relative_path in selected_core_files:
        expected_digest = payload["files"].get(relative_path)
        if (
            not isinstance(expected_digest, str)
            or SHA256_PATTERN.fullmatch(expected_digest) is None
        ):
            raise PhpDeploymentError(
                "existing deployment manifest file hash is invalid"
            )
        content = _read_stable_remote_regular_bytes(
            client,
            _remote_join(root, relative_path),
            required_mode=required_mode,
            label="existing deployment artifact",
        )
        if not secrets.compare_digest(
            _sha256_bytes(content),
            expected_digest,
        ):
            raise PhpDeploymentError(
                "existing deployment bytes do not match the manifest"
            )
    return payload["code_revision"]


def verify_remote_release_matches_plan(
    client: SftpClient,
    *,
    remote_root: str,
    plan: LocalDeploymentPlan,
) -> None:
    """Bind a deployed release to the exact clean local candidate bytes."""

    root = _remote_absolute_path(remote_root, label="remote root")
    for artifact in plan.artifacts:
        content = _read_stable_remote_regular_bytes(
            client,
            _remote_join(root, artifact.relative_path),
            required_mode=artifact.mode,
            label="remote candidate artifact",
        )
        if (
            len(content) != artifact.size
            or not secrets.compare_digest(
                _sha256_bytes(content),
                artifact.sha256,
            )
        ):
            raise PhpDeploymentError(
                "remote candidate bytes do not match the exact local release"
            )


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


def classify_schema_bridge_targets(
    client: SftpClient,
    *,
    candidate_plan: LocalDeploymentPlan,
    predecessor: BackupSnapshot,
) -> str:
    """Classify exact candidate/predecessor mixtures after an interrupted commit."""

    candidate_files = candidate_plan.artifact_by_path
    predecessor_files = predecessor.file_by_path
    if set(candidate_files) != set(predecessor_files):
        raise PhpDeploymentError("schema bridge recovery target set changed")
    saw_candidate_only = False
    saw_predecessor_only = False
    saw_restore_transition = False
    for relative_path in DEFAULT_COMMIT_ORDER:
        artifact = candidate_files[relative_path]
        previous = predecessor_files[relative_path]
        target = _remote_join(predecessor.remote_root, relative_path)
        attributes = _lstat_or_none(client, target)
        if attributes is None:
            candidate_match = False
            previous_match = not previous.existed
            # restore_remote_backup deliberately removes the commit marker
            # before it restores dependencies and writes the marker last.
            transition_match = (
                relative_path == DEPLOYMENT_MANIFEST_NAME
                and previous.existed
            )
        else:
            target_mode = _mode(attributes)
            if stat.S_ISLNK(target_mode) or not stat.S_ISREG(target_mode):
                raise PhpDeploymentError(
                    "schema bridge recovery target is not a regular file"
                )
            content = _read_stable_remote_regular_bytes(
                client,
                target,
                label="schema bridge recovery target",
            )
            after = _lstat_or_none(client, target)
            if after is None:
                raise PhpDeploymentError(
                    "schema bridge recovery target changed during verification"
                )
            after_mode = _mode(after)
            if (
                stat.S_ISLNK(after_mode)
                or not stat.S_ISREG(after_mode)
                or stat.S_IMODE(after_mode)
                != stat.S_IMODE(target_mode)
                or getattr(after, "st_size", None) != len(content)
            ):
                raise PhpDeploymentError(
                    "schema bridge recovery target changed during verification"
                )
            digest = _sha256_bytes(content)
            mode = stat.S_IMODE(target_mode)
            candidate_match = (
                len(content) == artifact.size
                and mode == artifact.mode
                and secrets.compare_digest(digest, artifact.sha256)
            )
            previous_match = (
                previous.existed
                and previous.size == len(content)
                and previous.mode == mode
                and previous.sha256 is not None
                and secrets.compare_digest(digest, previous.sha256)
            )
            transition_match = False
        if candidate_match and previous_match:
            continue
        if candidate_match:
            saw_candidate_only = True
        elif previous_match:
            saw_predecessor_only = True
        elif transition_match:
            saw_restore_transition = True
        else:
            raise PhpDeploymentError(
                "schema bridge recovery found an unrecognized target identity"
            )
    if saw_candidate_only and not (
        saw_predecessor_only or saw_restore_transition
    ):
        return "candidate"
    if not saw_candidate_only:
        return (
            "predecessor"
            if not saw_restore_transition
            else "predecessor_restore_transition"
        )
    return "mixed"


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


def _read_exact_deployment_lock_owner(
    client: SftpClient,
    *,
    lock_path: str,
    expected_release_id: str,
) -> tuple[bytes, Mapping[str, object]]:
    lock_attributes = _lstat_or_none(client, lock_path)
    if lock_attributes is None:
        raise PhpDeploymentError("expected stale deployment lock is missing")
    lock_mode = _mode(lock_attributes)
    if (
        stat.S_ISLNK(lock_mode)
        or not stat.S_ISDIR(lock_mode)
        or stat.S_IMODE(lock_mode) != PRIVATE_DIRECTORY_MODE
    ):
        raise PhpDeploymentError(
            "stale deployment lock directory identity is invalid"
        )
    owner_path = _remote_join(lock_path, "owner.json")
    owner_attributes = _lstat_or_none(client, owner_path)
    if owner_attributes is None:
        raise PhpDeploymentError("stale deployment lock owner is missing")
    owner_mode = _mode(owner_attributes)
    if (
        stat.S_ISLNK(owner_mode)
        or not stat.S_ISREG(owner_mode)
        or stat.S_IMODE(owner_mode) != PRIVATE_FILE_MODE
    ):
        raise PhpDeploymentError(
            "stale deployment lock owner mode is invalid"
        )
    owner_content = _read_stable_remote_regular_bytes(
        client,
        owner_path,
        required_mode=PRIVATE_FILE_MODE,
        label="stale deployment lock owner",
    )
    try:
        owner = json.loads(owner_content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PhpDeploymentError(
            "stale deployment lock owner JSON is invalid"
        ) from error
    if (
        not isinstance(owner, dict)
        or set(owner) != {"schema_version", "release_id", "acquired_at"}
        or owner.get("schema_version") != 1
        or owner.get("release_id") != expected_release_id
        or not expected_release_id.startswith(
            SCHEMA_BRIDGE_STALE_LOCK_PREFIXES
        )
        or not isinstance(owner.get("acquired_at"), str)
    ):
        raise PhpDeploymentError(
            "stale deployment lock owner identity does not match"
        )
    try:
        acquired_at = datetime.fromisoformat(str(owner["acquired_at"]))
    except ValueError as error:
        raise PhpDeploymentError(
            "stale deployment lock acquisition time is invalid"
        ) from error
    if (
        acquired_at.tzinfo is None
        or acquired_at.utcoffset() is None
        or acquired_at > datetime.now(timezone.utc)
    ):
        raise PhpDeploymentError(
            "stale deployment lock acquisition time is invalid"
        )
    return owner_content, owner


def _ownerless_lock_remote_identity(
    attributes: Any,
    *,
    lock_path: str,
    now: datetime | None = None,
) -> tuple[bytes, Mapping[str, object], str]:
    lock_mode = _mode(attributes)
    lock_size = getattr(attributes, "st_size", None)
    lock_mtime = getattr(attributes, "st_mtime", None)
    if (
        stat.S_ISLNK(lock_mode)
        or not stat.S_ISDIR(lock_mode)
        or stat.S_IMODE(lock_mode) != PRIVATE_DIRECTORY_MODE
        or type(lock_size) is not int
        or not isinstance(lock_mtime, (int, float))
        or isinstance(lock_mtime, bool)
        or not math.isfinite(float(lock_mtime))
        or float(lock_mtime) < 0
    ):
        raise PhpDeploymentError(
            "ownerless deployment lock remote identity is invalid"
        )
    observed_now = datetime.now(timezone.utc) if now is None else now
    remote_mtime = datetime.fromtimestamp(
        float(lock_mtime),
        timezone.utc,
    )
    if (
        remote_mtime
        > observed_now + STALE_LOCK_EVIDENCE_FUTURE_SKEW
        or remote_mtime > observed_now - STALE_LOCK_MINIMUM_AGE
    ):
        raise PhpDeploymentError(
            "ownerless deployment lock has not reached the minimum "
            "remote stale age"
        )
    identity: dict[str, object] = {
        "schema_version": 1,
        "lock_path": lock_path,
        "st_mode": PRIVATE_DIRECTORY_MODE,
        "st_size": lock_size,
        "st_mtime": lock_mtime,
        "st_uid": (
            getattr(attributes, "st_uid")
            if type(getattr(attributes, "st_uid", None)) is int
            else None
        ),
        "st_gid": (
            getattr(attributes, "st_gid")
            if type(getattr(attributes, "st_gid", None)) is int
            else None
        ),
        "st_ino": (
            getattr(attributes, "st_ino")
            if type(getattr(attributes, "st_ino", None)) is int
            else None
        ),
        "st_dev": (
            getattr(attributes, "st_dev")
            if type(getattr(attributes, "st_dev", None)) is int
            else None
        ),
    }
    encoded = _encode_json(identity)
    return encoded, identity, remote_mtime.isoformat()


def _read_exact_ownerless_deployment_lock(
    client: SftpClient,
    *,
    lock_path: str,
) -> tuple[bytes, Mapping[str, object], str]:
    lock_attributes = _lstat_or_none(client, lock_path)
    if lock_attributes is None:
        raise PhpDeploymentError("expected ownerless lock is missing")
    if (
        _lstat_or_none(
            client,
            _remote_join(lock_path, "owner.json"),
        )
        is not None
    ):
        raise PhpDeploymentError(
            "ownerless deployment lock identity is invalid"
        )
    try:
        entries = client.listdir(lock_path)
    except OSError as error:
        raise PhpDeploymentError(
            "ownerless deployment lock contents could not be verified"
        ) from error
    if entries:
        raise PhpDeploymentError(
            "ownerless deployment lock is not empty"
        )
    encoded, identity, remote_mtime = _ownerless_lock_remote_identity(
        lock_attributes,
        lock_path=lock_path,
    )
    after = _lstat_or_none(client, lock_path)
    if after is None:
        raise PhpDeploymentError(
            "ownerless deployment lock changed during verification"
        )
    after_encoded, _after_identity, _after_mtime = (
        _ownerless_lock_remote_identity(
            after,
            lock_path=lock_path,
        )
    )
    try:
        after_entries = client.listdir(lock_path)
    except OSError as error:
        raise PhpDeploymentError(
            "ownerless deployment lock contents changed during "
            "verification"
        ) from error
    if (
        not secrets.compare_digest(encoded, after_encoded)
        or after_entries
        or _lstat_or_none(
            client,
            _remote_join(lock_path, "owner.json"),
        )
        is not None
    ):
        raise PhpDeploymentError(
            "ownerless deployment lock changed during verification"
        )
    return encoded, identity, remote_mtime


def inspect_ownerless_deployment_lock(
    client: SftpClient,
    *,
    remote_root: str,
) -> Mapping[str, object]:
    """Read and attest the exact stale ownerless lock without mutating it."""

    safe_remote_root = _remote_absolute_path(
        remote_root,
        label="remote root",
    )
    lock_path = _remote_join(
        _remote_join(safe_remote_root, "_private"),
        "deployment-lock",
    )
    encoded, identity, remote_mtime = (
        _read_exact_ownerless_deployment_lock(
            client,
            lock_path=lock_path,
        )
    )
    return {
        "schema_version": 1,
        "inspection": "schema_bridge_ownerless_lock",
        "owner_release_id": STALE_LOCK_OWNERLESS,
        "lock_path": lock_path,
        "remote_identity": dict(identity),
        "owner_sha256": _sha256_bytes(encoded),
        "stale_lock_first_observed_at": remote_mtime,
        "remote_mtime": remote_mtime,
        "minimum_remote_age_seconds": int(
            STALE_LOCK_MINIMUM_AGE.total_seconds()
        ),
        "eligible_for_writer_absence_attestation": True,
        "remote_files_mutated": False,
    }


def _remove_exact_ownerless_deployment_lock(
    client: SftpClient,
    *,
    lock_path: str,
    expected_remote_identity: bytes,
) -> None:
    current_identity, _identity, _mtime = (
        _read_exact_ownerless_deployment_lock(
            client,
            lock_path=lock_path,
        )
    )
    if not secrets.compare_digest(
        current_identity,
        expected_remote_identity,
    ):
        raise PhpDeploymentError(
            "ownerless deployment lock remote identity changed before "
            "takeover"
        )
    try:
        client.rmdir(lock_path)
    except OSError as error:
        raise PhpDeploymentError(
            "ownerless lock takeover refused a non-empty lock"
        ) from error
    if _lstat_or_none(client, lock_path) is not None:
        raise PhpDeploymentError(
            "ownerless deployment lock cleanup could not be verified"
        )


def _remove_exact_stale_deployment_lock(
    client: SftpClient,
    *,
    lock_path: str,
    expected_owner_content: bytes,
    expected_release_id: str,
    exclusive_writer: ExclusiveBytesWriter,
) -> None:
    current_content, _owner = _read_exact_deployment_lock_owner(
        client,
        lock_path=lock_path,
        expected_release_id=expected_release_id,
    )
    if not secrets.compare_digest(current_content, expected_owner_content):
        raise PhpDeploymentError(
            "stale deployment lock owner changed before takeover"
        )
    owner_path = _remote_join(lock_path, "owner.json")
    _remove_remote_file_if_present(client, owner_path)
    try:
        client.rmdir(lock_path)
    except OSError as error:
        try:
            exclusive_writer(
                client,
                owner_path,
                expected_owner_content,
                mode=PRIVATE_FILE_MODE,
            )
        except BaseException as restore_error:
            raise PhpDeploymentError(
                "stale lock takeover failed and owner evidence could not "
                "be restored"
            ) from restore_error
        raise PhpDeploymentError(
            "stale lock takeover refused a non-empty or changed lock"
        ) from error
    if _lstat_or_none(client, lock_path) is not None:
        raise PhpDeploymentError(
            "stale deployment lock cleanup could not be verified"
        )


def _acquire_schema_bridge_abort_lock(
    client: SftpClient,
    *,
    private_root: str,
    replacement_release_id: str,
    stale_owner_release_id: str | None,
    writer_absence_evidence: str | None,
    stale_lock_first_observed_at: str | None,
    exclusive_writer: ExclusiveBytesWriter,
    bridge_report_update: BridgeReportUpdater,
) -> tuple[str, Mapping[str, object] | None]:
    lock_path = _remote_join(private_root, "deployment-lock")
    state_loader = getattr(
        bridge_report_update,
        "_bside_bridge_state_loader",
        None,
    )
    durable_state = state_loader() if callable(state_loader) else None
    durable_takeover = (
        durable_state.get("stale_lock_takeover")
        if isinstance(durable_state, dict)
        and durable_state.get("status")
        in (
            "stale_lock_takeover_ready",
            "stale_lock_takeover_complete",
        )
        else None
    )
    if durable_takeover is not None:
        assert isinstance(durable_state, dict)
        durable_status = durable_state.get("status")
        if (
            not _valid_stale_lock_takeover_identity(durable_takeover)
            or (
                durable_status
                == "stale_lock_takeover_complete"
                and durable_state.get(
                    "stale_lock_cleanup_verified"
                )
                is not True
            )
            or stale_owner_release_id
            != durable_takeover.get("stale_owner_release_id")
            or writer_absence_evidence
            != durable_takeover.get("writer_absence_evidence")
        ):
            raise PhpDeploymentError(
                "durable stale lock takeover identity changed"
            )
        persisted_replacement = str(
            durable_takeover["replacement_release_id"]
        )
        lock_attributes = _lstat_or_none(client, lock_path)
        if lock_attributes is None:
            replacement = _acquire_deployment_lock(
                client,
                private_root,
                persisted_replacement,
            )
        else:
            replacement = lock_path
            replacement_present = False
            try:
                _read_exact_deployment_lock_owner(
                    client,
                    lock_path=lock_path,
                    expected_release_id=persisted_replacement,
                )
                replacement_present = True
            except PhpDeploymentError:
                replacement_present = False
            if not replacement_present:
                if durable_status == "stale_lock_takeover_complete":
                    raise PhpDeploymentError(
                        "completed stale lock takeover found an unrelated "
                        "replacement lock"
                    )
                stale_state = durable_takeover.get("stale_owner_state")
                if stale_state == "ownerless":
                    raise PhpDeploymentError(
                        "ownerless takeover-ready lock cannot be "
                        "distinguished from a new writer"
                    )
                elif stale_state == "owner_present":
                    owner_content, _owner = (
                        _read_exact_deployment_lock_owner(
                            client,
                            lock_path=lock_path,
                            expected_release_id=str(
                                durable_takeover[
                                    "stale_owner_release_id"
                                ]
                            ),
                        )
                    )
                    if not secrets.compare_digest(
                        _sha256_bytes(owner_content),
                        str(
                            durable_takeover[
                                "stale_owner_sha256"
                            ]
                        ),
                    ):
                        raise PhpDeploymentError(
                            "durable stale lock owner digest changed"
                        )
                    _remove_exact_stale_deployment_lock(
                        client,
                        lock_path=lock_path,
                        expected_owner_content=owner_content,
                        expected_release_id=str(
                            durable_takeover[
                                "stale_owner_release_id"
                            ]
                        ),
                        exclusive_writer=exclusive_writer,
                    )
                else:
                    raise PhpDeploymentError(
                        "durable stale lock owner state is invalid"
                    )
                replacement = _acquire_deployment_lock(
                    client,
                    private_root,
                    persisted_replacement,
                )
        bridge_report_update(
            "stale_lock_takeover_complete",
            {
                "stale_lock_takeover": durable_takeover,
                "stale_lock_cleanup_verified": True,
            },
        )
        return replacement, dict(durable_takeover)
    if _lstat_or_none(client, lock_path) is None:
        return (
            _acquire_deployment_lock(
                client,
                private_root,
                replacement_release_id,
            ),
            None,
        )
    if (
        stale_owner_release_id is None
        or writer_absence_evidence is None
        or (
            stale_owner_release_id != STALE_LOCK_OWNERLESS
            and not stale_owner_release_id.startswith(
                SCHEMA_BRIDGE_STALE_LOCK_PREFIXES
            )
        )
        or (
            stale_owner_release_id == STALE_LOCK_OWNERLESS
            and stale_lock_first_observed_at is None
        )
        or (
            stale_owner_release_id != STALE_LOCK_OWNERLESS
            and stale_lock_first_observed_at is not None
        )
    ):
        raise PhpDeploymentError(
            "schema bridge abort found a lock without takeover evidence"
        )
    ownerless = stale_owner_release_id == STALE_LOCK_OWNERLESS
    ownerless_remote_identity: Mapping[str, object] | None = None
    ownerless_remote_mtime: str | None = None
    if ownerless:
        assert stale_lock_first_observed_at is not None
        (
            owner_content,
            ownerless_remote_identity,
            ownerless_remote_mtime,
        ) = _read_exact_ownerless_deployment_lock(
            client,
            lock_path=lock_path,
        )
        if (
            ownerless_remote_mtime is None
            or not secrets.compare_digest(
                ownerless_remote_mtime,
                stale_lock_first_observed_at,
            )
        ):
            raise PhpDeploymentError(
                "ownerless lock attested remote mtime does not match"
            )
        acquired_at_reference = ownerless_remote_mtime
    else:
        owner_content, owner = _read_exact_deployment_lock_owner(
            client,
            lock_path=lock_path,
            expected_release_id=stale_owner_release_id,
        )
        acquired_at_reference = str(owner["acquired_at"])
    evidence_identity = _validated_stale_lock_writer_absence(
        writer_absence_evidence,
        expected_owner_sha256=_sha256_bytes(owner_content),
        acquired_at_reference=acquired_at_reference,
    )
    takeover: dict[str, object] = {
        "stale_owner_release_id": stale_owner_release_id,
        "stale_owner_state": (
            "ownerless" if ownerless else "owner_present"
        ),
        "stale_owner_acquired_at": acquired_at_reference,
        "stale_owner_sha256": _sha256_bytes(owner_content),
        "writer_absence_evidence": writer_absence_evidence,
        "writer_absence_nonce": evidence_identity["nonce"],
        "writer_absence_issued_at": evidence_identity["issued_at"],
        "replacement_release_id": replacement_release_id,
        "database_mutated": False,
    }
    if ownerless:
        takeover.update(
            {
                "stale_owner_remote_identity": dict(
                    ownerless_remote_identity or {}
                ),
                "stale_owner_remote_mtime": ownerless_remote_mtime,
            }
        )
    takeover["identity_sha256"] = _sha256_bytes(_encode_json(takeover))
    bridge_report_update(
        "stale_lock_takeover_ready",
        {"stale_lock_takeover": takeover},
    )
    if ownerless:
        _remove_exact_ownerless_deployment_lock(
            client,
            lock_path=lock_path,
            expected_remote_identity=owner_content,
        )
    else:
        _remove_exact_stale_deployment_lock(
            client,
            lock_path=lock_path,
            expected_owner_content=owner_content,
            expected_release_id=stale_owner_release_id,
            exclusive_writer=exclusive_writer,
        )
    replacement = _acquire_deployment_lock(
        client,
        private_root,
        replacement_release_id,
    )
    try:
        bridge_report_update(
            "stale_lock_takeover_complete",
            {
                "stale_lock_takeover": takeover,
                "stale_lock_cleanup_verified": True,
            },
        )
    except BaseException as error:
        try:
            _release_deployment_lock(
                client,
                replacement,
                exclusive_writer=exclusive_writer,
            )
        except BaseException as cleanup_error:
            raise PhpDeploymentError(
                "replacement lock journal failed and cleanup was incomplete"
            ) from cleanup_error
        raise PhpDeploymentError(
            "replacement lock journal could not be committed"
        ) from error
    return replacement, takeover


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


def _restore_stage_paths(
    snapshot: BackupSnapshot,
    workspace: str,
) -> tuple[str, ...]:
    return tuple(
        _remote_join(
            workspace,
            f"restore-{index:03d}-{item.sha256}.blob",
        )
        for index, item in enumerate(snapshot.files)
        if item.existed and item.sha256 is not None
    )


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
        "migrations/012_dart_credential_pool.sql",
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
        content = _read_stable_remote_regular_bytes(
            client,
            target,
            required_mode=item.mode,
            expected_size=item.size,
            label="rollback restored artifact",
        )
        after = _lstat_or_none(client, target)
        if (
            after is None
            or not secrets.compare_digest(
                _sha256_bytes(content),
                item.sha256,
            )
            or stat.S_IMODE(_mode(after)) != item.mode
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
    expected_core_files: Sequence[str] = CORE_API_FILES,
    allow_partial_schema_bridge: bool = False,
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
    if allow_partial_schema_bridge and expected_current_sha is not None:
        raise PhpDeploymentError(
            "partial schema bridge compatibility cannot attest a current SHA"
        )

    private_root = _remote_join(GABIA_REMOTE_ROOT, "_private")
    _require_remote_directory(client, GABIA_REMOTE_ROOT, create=False)
    _require_remote_directory(client, private_root, create=False)
    current_release_sha = (
        None
        if allow_partial_schema_bridge
        else verify_existing_remote_release_identity(
            client,
            remote_root=GABIA_REMOTE_ROOT,
            expected_core_files=expected_core_files,
            required_mode=(
                DEFAULT_FILE_MODE
                if tuple(expected_core_files)
                == LEGACY_SCHEMA_11_CORE_API_FILES
                else None
            ),
        )
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


def verify_protected_schema_mismatch(
    *,
    base_url: str,
    protected_token: str,
    expected_schema_version: int,
    actual_schema_version: int,
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
    payload = _json_response(
        response,
        label="authenticated pending-schema route",
    )
    if (
        response.status != 503
        or payload.get("api_version") != "v2"
        or payload.get("error") != "schema_version_mismatch"
        or payload.get("expected_schema_version") != expected_schema_version
        or payload.get("actual_schema_version") != actual_schema_version
    ):
        raise PhpDeploymentError(
            "authenticated protected route did not prove the pending "
            "schema upgrade"
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
    expected_schema_version: int = 12,
    pending_actual_schema_version: int | None = None,
) -> None:
    if expected_schema_version not in {11, 12}:
        raise PhpDeploymentError("unsupported v2 schema smoke version")
    if (
        pending_actual_schema_version is not None
        and (
            expected_schema_version != 12
            or pending_actual_schema_version != 11
        )
    ):
        raise PhpDeploymentError("unsupported pending schema transition")
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
        "schema_version": expected_schema_version,
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
        or (
            f"x-schema-version: {expected_schema_version}".encode()
            not in openapi.body
        )
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
    if pending_actual_schema_version is None:
        if (
            events.status != 503
            or events_payload.get("api_version") != "v2"
            or events_payload.get("error") != "global_terminal_release_closed"
        ):
            raise PhpDeploymentError("v2 public data is not fail-closed")
    elif (
        events.status != 503
        or events_payload.get("api_version") != "v2"
        or events_payload.get("error") != "schema_version_mismatch"
        or events_payload.get("expected_schema_version")
        != expected_schema_version
        or events_payload.get("actual_schema_version")
        != pending_actual_schema_version
    ):
        raise PhpDeploymentError(
            "v2 public data did not prove the pending schema upgrade"
        )

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
    if pending_actual_schema_version is None:
        verify_protected_closed_state(
            base_url=base,
            protected_token=protected_token,
            http_request=http_request,
            timeout=timeout,
            allow_http=allow_http,
        )
    else:
        verify_protected_schema_mismatch(
            base_url=base,
            protected_token=protected_token,
            expected_schema_version=expected_schema_version,
            actual_schema_version=pending_actual_schema_version,
            http_request=http_request,
            timeout=timeout,
            allow_http=allow_http,
        )


def _v1_base_from_v2(base_url: str, *, allow_http: bool = False) -> str:
    base = _validate_https_url(
        base_url,
        label="API v2 base URL",
        allow_http=allow_http,
    )
    if not base.endswith("/api/v2"):
        raise PhpDeploymentError("API v2 base URL must end with /api/v2")
    derived = base[: -len("/api/v2")] + "/api/v1"
    if not allow_http and base == GABIA_API_V2_BASE_URL:
        if derived != GABIA_API_V1_BASE_URL:
            raise PhpDeploymentError("derived Gabia API v1 URL is invalid")
    return derived


def verify_closed_v1_api(
    *,
    v2_base_url: str,
    protected_token: str,
    http_request: HttpRequester = _default_http_request,
    timeout: float = 20.0,
    allow_http: bool = False,
) -> None:
    """Prove the independent v1 release row is closed and authenticated."""

    token = _validated_protected_token(protected_token)
    base = _v1_base_from_v2(v2_base_url, allow_http=allow_http)
    common = {"Accept": "application/json", "Cache-Control": "no-store"}
    health = http_request("GET", base + "/health", common, timeout)
    health_payload = _json_response(health, label="v1 health")
    if (
        health.status != 200
        or health.header("x-bside-api-version") != "v1"
        or health_payload.get("ok") is not True
        or health_payload.get("service") != "bside-governance-intelligence"
    ):
        raise PhpDeploymentError("v1 health identity is invalid")

    events = http_request("GET", base + "/events?limit=1", common, timeout)
    events_payload = _json_response(events, label="v1 closed events")
    if (
        events.status != 503
        or events.header("x-bside-api-version") != "v1"
        or events_payload.get("error") != "governance_release_closed"
    ):
        raise PhpDeploymentError("v1 public data is not fail-closed")

    admin = http_request("GET", base + "/admin/release-state", common, timeout)
    admin_payload = _json_response(admin, label="v1 unauthenticated admin")
    if admin.status != 401 or admin_payload.get("error") != "bearer_token_required":
        raise PhpDeploymentError("v1 admin authentication smoke failed")

    protected_ops = http_request(
        "GET",
        base + "/ops/runtime-state?resource=runs&limit=1",
        {
            **common,
            "Authorization": "Bearer " + token,
        },
        timeout,
    )
    protected_ops_payload = _json_response(
        protected_ops,
        label="v1 protected ops runtime state",
    )
    protected_ops_data = protected_ops_payload.get("data")
    if (
        protected_ops.status != 200
        or protected_ops.header("x-bside-api-version") != "v1"
        or protected_ops_payload.get("ok") is not True
        or protected_ops_payload.get("api_version") != "v1"
        or not isinstance(protected_ops_data, dict)
        or protected_ops_data.get("resource") != "runs"
        or not isinstance(protected_ops_data.get("records"), list)
    ):
        raise PhpDeploymentError(
            "authenticated v1 ops route is invalid"
        )

    protected_admin = http_request(
        "GET",
        base + "/admin/release-state",
        {
            **common,
            "Authorization": "Bearer " + token,
        },
        timeout,
    )
    protected_admin_payload = _json_response(
        protected_admin,
        label="v1 ops token admin boundary",
    )
    if (
        protected_admin.status != 403
        or protected_admin.header("x-bside-api-version") != "v1"
        or protected_admin_payload.get("api_version") != "v1"
        or protected_admin_payload.get("error") != "insufficient_role"
    ):
        raise PhpDeploymentError(
            "v1 ops token crossed the admin privilege boundary"
        )


def observe_closed_candidate_schema(
    *,
    base_url: str,
    expected_sha: str,
    protected_token: str,
    http_request: HttpRequester = _default_http_request,
    timeout: float = 20.0,
    allow_http: bool = False,
) -> int:
    """Accept only the two bridge states: candidate over DB 11 or DB 12."""

    failures: list[BaseException] = []
    for actual_schema in (12, 11):
        try:
            verify_closed_v2_api(
                base_url=base_url,
                expected_sha=expected_sha,
                protected_token=protected_token,
                http_request=http_request,
                timeout=timeout,
                allow_http=allow_http,
                expected_schema_version=12,
                pending_actual_schema_version=(
                    None if actual_schema == 12 else 11
                ),
            )
            verify_closed_v1_api(
                v2_base_url=base_url,
                protected_token=protected_token,
                http_request=http_request,
                timeout=timeout,
                allow_http=allow_http,
            )
            return actual_schema
        except PhpDeploymentError as error:
            failures.append(error)
    raise PhpDeploymentError(
        "candidate did not prove a closed schema-11 or schema-12 bridge state"
    ) from failures[-1]


def verify_c06_forward_compatible_closed(
    *,
    base_url: str,
    protected_token: str,
    http_request: HttpRequester = _default_http_request,
    timeout: float = 20.0,
    allow_http: bool = False,
) -> None:
    """Prove c06's schema-11 API contract with both release rows closed."""

    verify_closed_v2_api(
        base_url=base_url,
        expected_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        protected_token=protected_token,
        http_request=http_request,
        timeout=timeout,
        allow_http=allow_http,
        expected_schema_version=11,
    )
    verify_closed_v1_api(
        v2_base_url=base_url,
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
    schema_upgrade_from: int | None = None,
    expected_previous_sha: str | None = None,
    dart_disabled_evidence: str | None = None,
    bridge_report_update: BridgeReportUpdater | None = None,
) -> Mapping[str, object]:
    if schema_upgrade_from not in {None, 11}:
        raise PhpDeploymentError(
            "only the explicit v2 schema 11 to 12 upgrade is supported"
        )
    if schema_upgrade_from == 11 and (
        expected_previous_sha != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
        or dart_disabled_evidence is None
        or DART_DISABLED_EVIDENCE_PATTERN.fullmatch(dart_disabled_evidence)
        is None
        or not callable(bridge_report_update)
    ):
        raise PhpDeploymentError(
            "schema bridge requires exact c06, DART-off evidence, and a "
            "durable bridge journal"
        )
    selected_release_id = _validate_release_id(
        release_id or _make_release_id(plan.code_revision)
    )
    if (
        schema_upgrade_from == 11
        and not selected_release_id.startswith("php-v2-")
    ):
        raise PhpDeploymentError(
            "schema bridge release ID must use the php-v2 prefix"
        )
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
        if (
            schema_upgrade_from == 11
            and gabia_compatibility.current_release_sha
            != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
        ):
            raise PhpDeploymentError(
                "schema bridge compatibility predecessor is not exact c06"
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
    _require_remote_directory(client, root, create=False)
    existing_core_files = (
        LEGACY_SCHEMA_11_CORE_API_FILES
        if schema_upgrade_from == 11
        else CORE_API_FILES
    )
    existing_revision = verify_existing_remote_release_identity(
        client,
        remote_root=root,
        expected_core_files=existing_core_files,
        required_mode=(
            DEFAULT_FILE_MODE if schema_upgrade_from == 11 else None
        ),
    )
    if schema_upgrade_from is not None and existing_revision is None:
        raise PhpDeploymentError(
            "schema upgrade deployment requires an attested existing release"
        )
    if (
        schema_upgrade_from == 11
        and existing_revision != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
    ):
        raise PhpDeploymentError(
            "schema bridge predecessor is not the exact c06 release"
        )
    if schema_upgrade_from == 11:
        migration_path = "migrations/011_global_terminal_v2.sql"
        migration_artifact = plan.artifact_by_path[migration_path]
        existing_migration = _read_stable_remote_regular_bytes(
            client,
            _remote_join(root, migration_path),
            required_mode=DEFAULT_FILE_MODE,
            label="schema 11 migration 011",
        )
        if (
            len(existing_migration) != migration_artifact.size
            or not secrets.compare_digest(
                _sha256_bytes(existing_migration),
                migration_artifact.sha256,
            )
        ):
            raise PhpDeploymentError(
                "schema 11 upgrade requires unchanged migration 011 bytes"
            )
    existing_schema_version = (
        12 if schema_upgrade_from is None else schema_upgrade_from
    )
    if existing_revision is not None:
        verify_closed_v2_api(
            base_url=api_v2_base_url,
            expected_sha=existing_revision,
            protected_token=protected_token,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=allow_http,
            expected_schema_version=existing_schema_version,
        )
        if schema_upgrade_from == 11:
            verify_closed_v1_api(
                v2_base_url=api_v2_base_url,
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
        if schema_upgrade_from == 11:
            verify_one_time_schema_bridge_backup(
                client,
                snapshot=backup,
                expected_current_sha=plan.code_revision,
                expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
                expected_manifest_sha256=backup.manifest_sha256,
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
                expected_schema_version=existing_schema_version,
            )
            if schema_upgrade_from == 11:
                verify_closed_v1_api(
                    v2_base_url=api_v2_base_url,
                    protected_token=protected_token,
                    http_request=http_request,
                    timeout=http_timeout,
                    allow_http=allow_http,
                )
            verify_remote_targets_match_snapshot(
                client,
                snapshot=backup,
            )
        if schema_upgrade_from == 11:
            if dart_disabled_evidence is None:
                raise PhpDeploymentError("schema bridge DART-off evidence is missing")
            ready = _bridge_backup_ready_identity(
                backup=backup,
                candidate_code_revision=plan.code_revision,
                previous_code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
                dart_disabled_evidence=dart_disabled_evidence,
            )
            if bridge_report_update is not None:
                bridge_report_update("backup_ready", {"backup_ready": ready})
                bridge_report_update("commit_started", {})
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
            expected_schema_version=12,
            pending_actual_schema_version=schema_upgrade_from,
        )
        if schema_upgrade_from == 11:
            verify_closed_v1_api(
                v2_base_url=api_v2_base_url,
                protected_token=protected_token,
                http_request=http_request,
                timeout=http_timeout,
                allow_http=allow_http,
            )
            verify_remote_release_matches_plan(
                client,
                remote_root=root,
                plan=plan,
            )
            if bridge_report_update is not None:
                bridge_report_update(
                    "restored",
                    {
                        "candidate_database_schema_version_before": 11,
                        "database_mutated": False,
                    },
                )
                bridge_report_update(
                    "verified",
                    {
                        "candidate_database_schema_version_before": 11,
                        "database_mutated": False,
                        "v1_closed_smoke": True,
                        "v2_closed_smoke": True,
                    },
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
                if schema_upgrade_from == 11:
                    restored_release = verify_existing_remote_release_identity(
                        client,
                        remote_root=root,
                        expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
                        required_mode=DEFAULT_FILE_MODE,
                    )
                    if restored_release != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA:
                        raise PhpDeploymentError(
                            "automatic bridge abort did not restore exact c06"
                        )
                    verify_c06_forward_compatible_closed(
                        base_url=api_v2_base_url,
                        protected_token=protected_token,
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
    result: dict[str, object] = {
        "ok": True,
        "operation": (
            "schema-bridge-deploy"
            if schema_upgrade_from == 11
            else "deploy"
        ),
        "code_revision": plan.code_revision,
        "release_id": selected_release_id,
        "backup_manifest": backup.manifest_path,
        "backup_manifest_sha256": backup.manifest_sha256,
        "backup_identity": {
            "schema_version": BACKUP_SCHEMA_VERSION,
            "release_id": backup.release_id,
            "backup_directory": backup.backup_directory,
            "manifest_path": backup.manifest_path,
            "manifest_sha256": backup.manifest_sha256,
            "manifest_bytes": backup.manifest_size,
            "remote_root": backup.remote_root,
            "candidate_code_revision": backup.candidate_code_revision,
        },
        "files_deployed": len(plan.artifacts),
        "manifest_committed_last": True,
        "opcache_action": opcache_action,
        "opcache_reset": opcache_action == "reset_verified",
        "closed_smoke": schema_upgrade_from is None,
        "fail_closed_smoke": True,
        "deployment_smoke_mode": (
            "closed"
            if schema_upgrade_from is None
            else "pending_schema_upgrade_11_to_12"
        ),
        "schema_upgrade_from": schema_upgrade_from,
        "private_root_http_protected": True,
    }
    if schema_upgrade_from == 11:
        result.update(
            {
                "previous_code_revision": ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
                "database_mutated": False,
                "dart_disabled_evidence": dart_disabled_evidence,
                "byte_verification": True,
                "v1_closed_smoke": True,
                "v2_closed_smoke": True,
            }
        )
    return result


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
    previous_release = inspect_backup_previous_release(
        client,
        snapshot=target_snapshot,
    )
    if (
        previous_release is not None
        and previous_release[1]
        == frozenset(LEGACY_SCHEMA_11_CORE_API_FILES)
    ):
        raise PhpDeploymentError(
            "normal rollback cannot restore a schema 11 predecessor; "
            "use schema-bridge-rollback so the database remains intact"
        )
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


def rollback_one_time_schema_bridge(
    client: SftpClient,
    *,
    candidate_plan: LocalDeploymentPlan,
    release_id: str,
    expected_current_sha: str,
    expected_previous_sha: str,
    expected_backup_manifest_sha256: str,
    remote_root: str = DEFAULT_REMOTE_ROOT,
    stage_root: str | None = None,
    backup_root: str | None = None,
    public_url_root: str,
    api_v2_base_url: str,
    rollback_health_url: str,
    protected_token: str,
    http_request: HttpRequester = _default_http_request,
    http_timeout: float = 20.0,
    gabia_compatibility: GabiaCoreCompatibility | None = None,
    dart_disabled_evidence: str | None = None,
    bridge_report_update: BridgeReportUpdater | None = None,
) -> Mapping[str, object]:
    """Restore exact c06 PHP while leaving a schema-11 or schema-12 DB intact."""

    if gabia_compatibility is None:
        raise PhpDeploymentError(
            "one-time schema bridge rollback requires pinned Gabia compatibility"
        )
    if (
        expected_current_sha != candidate_plan.code_revision
        or expected_previous_sha != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
        or gabia_compatibility.current_release_sha != expected_current_sha
        or dart_disabled_evidence is None
        or DART_DISABLED_EVIDENCE_PATTERN.fullmatch(dart_disabled_evidence)
        is None
        or not callable(bridge_report_update)
    ):
        raise PhpDeploymentError(
            "one-time schema bridge release identity does not match"
        )
    _validated_protected_token(protected_token)
    validate_http_endpoint_binding(
        public_url_root=public_url_root,
        api_v2_base_url=api_v2_base_url,
        rollback_health_url=rollback_health_url,
        allow_http=False,
    )
    root = _remote_absolute_path(remote_root, label="remote root")
    _validate_gabia_core_binding(
        gabia_compatibility,
        client=client,
        remote_root=root,
        public_url_root=public_url_root,
        api_v2_base_url=api_v2_base_url,
        rollback_health_url=rollback_health_url,
    )
    private_root, safe_stage_root, safe_backup_root = _private_roots(
        root,
        stage_root,
        backup_root,
    )
    safe_release_id = _validate_release_id(release_id)

    current_release_sha = verify_existing_remote_release_identity(
        client,
        remote_root=root,
    )
    if current_release_sha != expected_current_sha:
        raise PhpDeploymentError(
            "one-time schema bridge current release does not match"
        )
    verify_remote_release_matches_plan(
        client,
        remote_root=root,
        plan=candidate_plan,
    )
    target_snapshot = load_remote_backup(
        client,
        backup_root=safe_backup_root,
        release_id=safe_release_id,
        expected_remote_root=root,
    )
    verify_one_time_schema_bridge_backup(
        client,
        snapshot=target_snapshot,
        expected_current_sha=expected_current_sha,
        expected_previous_sha=expected_previous_sha,
        expected_manifest_sha256=expected_backup_manifest_sha256,
    )
    ready = _bridge_backup_ready_identity(
        backup=target_snapshot,
        candidate_code_revision=expected_current_sha,
        previous_code_revision=expected_previous_sha,
        dart_disabled_evidence=dart_disabled_evidence,
    )
    bridge_report_update("backup_ready", {"backup_ready": ready})
    candidate_database_schema_version_before = observe_closed_candidate_schema(
        base_url=api_v2_base_url,
        expected_sha=expected_current_sha,
        protected_token=protected_token,
        http_request=http_request,
        timeout=http_timeout,
    )
    ensure_private_root_http_protection(
        client,
        remote_root=root,
        private_root=private_root,
        public_url_root=public_url_root,
        http_request=http_request,
        timeout=http_timeout,
        allow_http=False,
        exclusive_writer=gabia_compatibility.exclusive_writer,
        expected_policy=gabia_compatibility.private_policy,
        expected_policy_mode=gabia_compatibility.private_policy_mode,
        allowed_private_redirect=GABIA_PRIVATE_DENY_REDIRECT,
    )

    operation_id = _validate_release_id(
        _make_release_id(
            expected_previous_sha,
            prefix="schema11-bridge-rollback",
        )
    )
    lock_path = _acquire_deployment_lock(
        client,
        private_root,
        operation_id,
    )
    workspace = ""
    recovery_workspace = ""
    emergency: BackupSnapshot | None = None
    mutation_started = False
    operation_error: BaseException | None = None
    recovery_error: BaseException | None = None
    cleanup_error: BaseException | None = None
    recovery_cleanup_error: BaseException | None = None
    lock_release_error: BaseException | None = None
    policy_error: BaseException | None = None
    opcache_action: str | None = None
    try:
        workspace = _create_private_workspace(
            client,
            safe_stage_root,
            operation_id,
        )
        _verify_posix_rename_capability(client, workspace)
        synthetic_plan = LocalDeploymentPlan(
            local_root=Path("."),
            code_revision=expected_current_sha,
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
                expected_current_sha,
                prefix="pre-schema11-bridge-rollback",
            )
        )
        emergency = capture_remote_backup(
            client,
            plan=synthetic_plan,
            remote_root=root,
            backup_root=safe_backup_root,
            release_id=emergency_id,
        )
        emergency_identity = _bridge_emergency_identity(
            emergency,
            candidate_code_revision=expected_current_sha,
        )
        bridge_report_update(
            "emergency_ready",
            {"emergency_backup": emergency_identity},
        )
        locked_database_schema_version = observe_closed_candidate_schema(
            base_url=api_v2_base_url,
            expected_sha=expected_current_sha,
            protected_token=protected_token,
            http_request=http_request,
            timeout=http_timeout,
        )
        if (
            locked_database_schema_version
            != candidate_database_schema_version_before
        ):
            raise PhpDeploymentError(
                "candidate database schema changed before restore"
            )
        final_current_release_sha = verify_existing_remote_release_identity(
            client,
            remote_root=root,
        )
        if final_current_release_sha != expected_current_sha:
            raise PhpDeploymentError(
                "one-time schema bridge current release changed before restore"
            )
        verify_remote_release_matches_plan(
            client,
            remote_root=root,
            plan=candidate_plan,
        )
        verify_remote_targets_match_snapshot(
            client,
            snapshot=emergency,
        )
        locked_target_snapshot = load_remote_backup(
            client,
            backup_root=safe_backup_root,
            release_id=safe_release_id,
            expected_remote_root=root,
        )
        verify_one_time_schema_bridge_backup(
            client,
            snapshot=locked_target_snapshot,
            expected_current_sha=expected_current_sha,
            expected_previous_sha=expected_previous_sha,
            expected_manifest_sha256=expected_backup_manifest_sha256,
        )
        bridge_report_update(
            "commit_started",
            {
                "candidate_database_schema_version_before": (
                    candidate_database_schema_version_before
                ),
                "database_mutated": False,
            },
        )
        mutation_started = True
        restore_remote_backup(
            client,
            snapshot=locked_target_snapshot,
            workspace=workspace,
        )
        opcache_action = reset_opcache_with_ephemeral_probe(
            client,
            remote_root=root,
            public_url_root=public_url_root,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=False,
            exclusive_writer=gabia_compatibility.exclusive_writer,
            require_strict_state=True,
        )
        restored_release_sha = verify_existing_remote_release_identity(
            client,
            remote_root=root,
            expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
            required_mode=DEFAULT_FILE_MODE,
        )
        if restored_release_sha != expected_previous_sha:
            raise PhpDeploymentError(
                "one-time schema bridge previous release was not restored"
            )
        bridge_report_update(
            "restored",
            {
                "candidate_database_schema_version_before": (
                    candidate_database_schema_version_before
                ),
                "database_mutated": False,
            },
        )
        verify_c06_forward_compatible_closed(
            base_url=api_v2_base_url,
            protected_token=protected_token,
            http_request=http_request,
            timeout=http_timeout,
        )
        verify_rollback_health(
            url=rollback_health_url,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=False,
        )
        bridge_report_update(
            "verified",
            {
                "candidate_database_schema_version_before": (
                    candidate_database_schema_version_before
                ),
                "database_schema_observation": (
                    "candidate_schema_"
                    + str(candidate_database_schema_version_before)
                ),
                "database_mutated": False,
                "v1_closed_smoke": True,
                "v2_closed_smoke": True,
                "legacy_health_smoke": True,
                "forward_compatibility_verified": True,
            },
        )
    except BaseException as error:
        operation_error = error
        if mutation_started and emergency is not None:
            try:
                recovery_id = _validate_release_id(
                    _make_release_id(
                        expected_current_sha,
                        prefix="recover-schema11-bridge-rollback",
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
                    allow_http=False,
                    exclusive_writer=gabia_compatibility.exclusive_writer,
                    require_strict_state=True,
                )
                recovered_database_schema_version = (
                    observe_closed_candidate_schema(
                        base_url=api_v2_base_url,
                        expected_sha=expected_current_sha,
                        protected_token=protected_token,
                        http_request=http_request,
                        timeout=http_timeout,
                    )
                )
                if (
                    recovered_database_schema_version
                    != candidate_database_schema_version_before
                ):
                    raise PhpDeploymentError(
                        "candidate recovery changed the database schema"
                    )
            except BaseException as error_during_recovery:
                recovery_error = error_during_recovery
    finally:
        if workspace:
            try:
                _cleanup_workspace(
                    client,
                    workspace,
                    known_files=_restore_stage_paths(
                        target_snapshot,
                        workspace,
                    ),
                )
            except BaseException as error:
                cleanup_error = error
        if recovery_workspace:
            try:
                _cleanup_workspace(
                    client,
                    recovery_workspace,
                    known_files=(
                        ()
                        if emergency is None
                        else _restore_stage_paths(
                            emergency,
                            recovery_workspace,
                        )
                    ),
                )
            except BaseException as error:
                recovery_cleanup_error = error
        try:
            _release_deployment_lock(
                client,
                lock_path,
                exclusive_writer=gabia_compatibility.exclusive_writer,
            )
        except BaseException as error:
            lock_release_error = error
        try:
            _verify_gabia_private_policy(
                client,
                gabia_compatibility,
            )
        except BaseException as error:
            policy_error = error

    if operation_error is not None:
        if mutation_started and recovery_error is not None:
            raise PhpDeploymentRollbackError(
                "one-time schema bridge rollback failed and candidate recovery failed"
            ) from recovery_error
        if (
            cleanup_error is not None
            or recovery_cleanup_error is not None
            or lock_release_error is not None
            or policy_error is not None
        ):
            raise PhpDeploymentError(
                "one-time schema bridge rollback failed with incomplete "
                "private cleanup or policy verification"
            ) from (
                cleanup_error
                or recovery_cleanup_error
                or lock_release_error
                or policy_error
            )
        if mutation_started:
            raise PhpDeploymentError(
                "one-time schema bridge rollback failed safely; "
                "the candidate was restored"
            ) from operation_error
        raise PhpDeploymentError(
            "one-time schema bridge rollback stopped before production mutation"
        ) from operation_error
    if (
        cleanup_error is not None
        or recovery_cleanup_error is not None
        or lock_release_error is not None
        or policy_error is not None
    ):
        raise PhpDeploymentError(
            "one-time schema bridge rollback was applied, but private "
            "cleanup or policy verification failed"
        ) from (
            cleanup_error
            or recovery_cleanup_error
            or lock_release_error
            or policy_error
        )
    if opcache_action not in {"disabled_verified", "reset_verified"}:
        raise PhpDeploymentError(
            "one-time schema bridge rollback lacks verified OPcache state"
        )
    return {
        "ok": True,
        "operation": "schema-bridge-rollback",
        "release_id": safe_release_id,
        "backup_manifest": target_snapshot.manifest_path,
        "backup_manifest_sha256": target_snapshot.manifest_sha256,
        "restored_code_revision": expected_previous_sha,
        "restored_api_schema_contract_version": 11,
        "candidate_code_revision": expected_current_sha,
        "candidate_expected_schema_version": 12,
        "candidate_database_schema_version_before": (
            candidate_database_schema_version_before
        ),
        "database_schema_observation": (
            "candidate_schema_"
            + str(candidate_database_schema_version_before)
        ),
        "database_mutated": False,
        "dart_disabled_evidence": dart_disabled_evidence,
        "manifest_committed_last": True,
        "byte_verification": True,
        "v1_closed_smoke": True,
        "v2_closed_smoke": True,
        "legacy_health_smoke": True,
        "forward_compatibility_verified": True,
        "closed_smoke": True,
        "opcache_action": opcache_action,
        "private_root_http_protected": True,
        "emergency_backup_release_id": (
            None if emergency is None else emergency.release_id
        ),
        "emergency_backup_identity": (
            None
            if emergency is None
            else _bridge_emergency_identity(
                emergency,
                candidate_code_revision=expected_current_sha,
            )
        ),
    }


def _verify_prebackup_c06_public_state(
    client: SftpClient,
    *,
    candidate_plan: LocalDeploymentPlan,
    remote_root: str,
    api_v2_base_url: str,
    protected_token: str,
    http_request: HttpRequester,
    http_timeout: float,
) -> None:
    """Prove a prepared deploy crashed before changing any public byte."""

    current_release = verify_existing_remote_release_identity(
        client,
        remote_root=remote_root,
        expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
        required_mode=DEFAULT_FILE_MODE,
    )
    if current_release != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA:
        raise PhpDeploymentError(
            "schema bridge prebackup recovery is not exact c06"
        )
    migration = candidate_plan.artifact_by_path[
        "migrations/011_global_terminal_v2.sql"
    ]
    existing_migration = _read_stable_remote_regular_bytes(
        client,
        _remote_join(
            remote_root,
            "migrations/011_global_terminal_v2.sql",
        ),
        required_mode=DEFAULT_FILE_MODE,
        expected_size=migration.size,
        label="schema bridge prebackup migration 011",
    )
    if not secrets.compare_digest(
        _sha256_bytes(existing_migration),
        migration.sha256,
    ):
        raise PhpDeploymentError(
            "schema bridge prebackup migration 011 differs from candidate"
        )
    verify_c06_forward_compatible_closed(
        base_url=api_v2_base_url,
        protected_token=protected_token,
        http_request=http_request,
        timeout=http_timeout,
    )


def _abort_prebackup_schema_bridge(
    client: SftpClient,
    *,
    candidate_plan: LocalDeploymentPlan,
    release_id: str,
    prepared_checkpoint: Mapping[str, object],
    bridge_deploy_report_load: BridgeReportLoader,
    bridge_report_update: BridgeReportUpdater,
    remote_root: str,
    private_root: str,
    stage_root: str,
    backup_root: str,
    public_url_root: str,
    api_v2_base_url: str,
    rollback_health_url: str,
    protected_token: str,
    http_request: HttpRequester,
    http_timeout: float,
    gabia_compatibility: GabiaCoreCompatibility,
    stale_lock_owner_release_id: str | None,
    stale_lock_writer_absence_evidence: str | None,
    stale_lock_first_observed_at: str | None,
) -> Mapping[str, object]:
    """Release a crashed pre-backup lock only after exact c06 attestation."""

    prebackup_identity = _bridge_prebackup_c06_identity(
        prepared_checkpoint
    )
    if not _valid_bridge_prebackup_c06_identity(
        prebackup_identity,
        candidate_code_revision=candidate_plan.code_revision,
        release_id=release_id,
    ):
        raise PhpDeploymentError(
            "schema bridge prepared checkpoint is invalid"
        )
    _verify_prebackup_c06_public_state(
        client,
        candidate_plan=candidate_plan,
        remote_root=remote_root,
        api_v2_base_url=api_v2_base_url,
        protected_token=protected_token,
        http_request=http_request,
        http_timeout=http_timeout,
    )
    bridge_report_update(
        "prebackup_c06_ready",
        {"prebackup_c06": prebackup_identity},
    )
    ensure_private_root_http_protection(
        client,
        remote_root=remote_root,
        private_root=private_root,
        public_url_root=public_url_root,
        http_request=http_request,
        timeout=http_timeout,
        allow_http=False,
        exclusive_writer=gabia_compatibility.exclusive_writer,
        expected_policy=gabia_compatibility.private_policy,
        expected_policy_mode=gabia_compatibility.private_policy_mode,
        allowed_private_redirect=GABIA_PRIVATE_DENY_REDIRECT,
    )
    operation_id = _validate_release_id(
        _make_release_id(
            candidate_plan.code_revision,
            prefix="schema11-bridge-abort",
        )
    )
    lock_path, stale_lock_takeover = _acquire_schema_bridge_abort_lock(
        client,
        private_root=private_root,
        replacement_release_id=operation_id,
        stale_owner_release_id=stale_lock_owner_release_id,
        writer_absence_evidence=stale_lock_writer_absence_evidence,
        stale_lock_first_observed_at=stale_lock_first_observed_at,
        exclusive_writer=gabia_compatibility.exclusive_writer,
        bridge_report_update=bridge_report_update,
    )
    operation_error: BaseException | None = None
    lock_release_error: BaseException | None = None
    policy_error: BaseException | None = None
    opcache_action: str | None = None
    try:
        if bridge_deploy_report_load() != prepared_checkpoint:
            raise PhpDeploymentError(
                "schema bridge prepared journal changed under recovery lock"
            )
        _verify_prebackup_c06_public_state(
            client,
            candidate_plan=candidate_plan,
            remote_root=remote_root,
            api_v2_base_url=api_v2_base_url,
            protected_token=protected_token,
            http_request=http_request,
            http_timeout=http_timeout,
        )
        bridge_report_update(
            "commit_started",
            {
                "initial_php_state": "prebackup_c06",
                "candidate_database_schema_version_before": None,
                "database_mutated": False,
            },
        )
        opcache_action = reset_opcache_with_ephemeral_probe(
            client,
            remote_root=remote_root,
            public_url_root=public_url_root,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=False,
            exclusive_writer=gabia_compatibility.exclusive_writer,
            require_strict_state=True,
        )
        _verify_prebackup_c06_public_state(
            client,
            candidate_plan=candidate_plan,
            remote_root=remote_root,
            api_v2_base_url=api_v2_base_url,
            protected_token=protected_token,
            http_request=http_request,
            http_timeout=http_timeout,
        )
        verify_rollback_health(
            url=rollback_health_url,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=False,
        )
        bridge_report_update(
            "restored",
            {
                "initial_php_state": "prebackup_c06",
                "candidate_database_schema_version_before": None,
                "database_mutated": False,
            },
        )
        bridge_report_update(
            "verified",
            {
                "initial_php_state": "prebackup_c06",
                "candidate_database_schema_version_before": None,
                "database_schema_observation": (
                    "unavailable_due_c06_contract"
                ),
                "database_mutated": False,
                "v1_closed_smoke": True,
                "v2_closed_smoke": True,
                "legacy_health_smoke": True,
                "forward_compatibility_verified": True,
            },
        )
    except BaseException as error:
        operation_error = error
    finally:
        try:
            _release_deployment_lock(
                client,
                lock_path,
                exclusive_writer=gabia_compatibility.exclusive_writer,
            )
        except BaseException as error:
            lock_release_error = error
        try:
            _verify_gabia_private_policy(client, gabia_compatibility)
        except BaseException as error:
            policy_error = error
    if operation_error is not None:
        if lock_release_error is not None or policy_error is not None:
            raise PhpDeploymentError(
                "schema bridge prebackup abort failed with incomplete cleanup"
            ) from (lock_release_error or policy_error)
        raise PhpDeploymentError(
            "schema bridge prebackup abort did not prove an unchanged c06 "
            "release"
        ) from operation_error
    if lock_release_error is not None or policy_error is not None:
        raise PhpDeploymentError(
            "schema bridge prebackup abort verified c06 but cleanup was "
            "incomplete"
        ) from (lock_release_error or policy_error)
    if opcache_action not in {"disabled_verified", "reset_verified"}:
        raise PhpDeploymentError(
            "schema bridge prebackup abort lacks verified OPcache state"
        )
    preserved_private_paths = [
        path
        for path in (
            _remote_join(stage_root, release_id),
            _remote_join(backup_root, release_id),
        )
        if _lstat_or_none(client, path) is not None
    ]
    return {
        "ok": True,
        "operation": "schema-bridge-abort",
        "release_id": release_id,
        "backup_manifest": None,
        "backup_manifest_sha256": None,
        "restored_code_revision": ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        "restored_api_schema_contract_version": 11,
        "candidate_code_revision": candidate_plan.code_revision,
        "candidate_expected_schema_version": 12,
        "candidate_database_schema_version_before": None,
        "database_schema_observation": (
            "unavailable_due_c06_contract"
        ),
        "database_mutated": False,
        "dart_disabled_evidence": prepared_checkpoint.get(
            "dart_disabled_evidence"
        ),
        "initial_php_state": "prebackup_c06",
        "manifest_committed_last": False,
        "manifest_commit_not_applicable": True,
        "public_release_files_mutated": False,
        "ephemeral_opcache_probe_created_and_removed": True,
        "byte_verification": True,
        "v1_closed_smoke": True,
        "v2_closed_smoke": True,
        "legacy_health_smoke": True,
        "forward_compatibility_verified": True,
        "closed_smoke": True,
        "opcache_action": opcache_action,
        "private_root_http_protected": True,
        "emergency_backup_release_id": None,
        "emergency_backup_identity": None,
        "stale_lock_takeover": stale_lock_takeover,
        "stale_lock_cleanup_verified": (
            stale_lock_takeover is None
            or _lstat_or_none(
                client,
                _remote_join(private_root, "deployment-lock"),
            )
            is None
        ),
        "prebackup_c06_identity": prebackup_identity,
        "orphan_private_artifacts_preserved": True,
        "preserved_private_artifact_paths": preserved_private_paths,
    }


def abort_one_time_schema_bridge(
    client: SftpClient,
    *,
    candidate_plan: LocalDeploymentPlan,
    release_id: str,
    expected_backup_manifest_sha256: str | None,
    dart_disabled_evidence: str,
    bridge_deploy_report_load: BridgeReportLoader,
    bridge_report_update: BridgeReportUpdater,
    remote_root: str = DEFAULT_REMOTE_ROOT,
    stage_root: str | None = None,
    backup_root: str | None = None,
    public_url_root: str,
    api_v2_base_url: str,
    rollback_health_url: str,
    protected_token: str,
    http_request: HttpRequester = _default_http_request,
    http_timeout: float = 20.0,
    gabia_compatibility: GabiaCoreCompatibility | None = None,
    stale_lock_owner_release_id: str | None = None,
    stale_lock_writer_absence_evidence: str | None = None,
    stale_lock_first_observed_at: str | None = None,
) -> Mapping[str, object]:
    """Resume an interrupted bridge commit and converge exact files to c06."""

    if (
        gabia_compatibility is None
        or gabia_compatibility.current_release_sha is not None
        or DART_DISABLED_EVIDENCE_PATTERN.fullmatch(dart_disabled_evidence)
        is None
        or not callable(bridge_deploy_report_load)
        or not callable(bridge_report_update)
        or (
            (stale_lock_owner_release_id is None)
            != (stale_lock_writer_absence_evidence is None)
        )
        or (
            stale_lock_owner_release_id is None
            and stale_lock_first_observed_at is not None
        )
    ):
        raise PhpDeploymentError(
            "schema bridge abort requires partial-state Gabia compatibility"
        )
    if stale_lock_owner_release_id is not None:
        safe_stale_owner = (
            STALE_LOCK_OWNERLESS
            if stale_lock_owner_release_id == STALE_LOCK_OWNERLESS
            else _validate_release_id(stale_lock_owner_release_id)
        )
        if (
            safe_stale_owner != stale_lock_owner_release_id
            or (
                safe_stale_owner != STALE_LOCK_OWNERLESS
                and not safe_stale_owner.startswith(
                    SCHEMA_BRIDGE_STALE_LOCK_PREFIXES
                )
            )
            or stale_lock_writer_absence_evidence is None
            or (
                safe_stale_owner == STALE_LOCK_OWNERLESS
                and stale_lock_first_observed_at is None
            )
            or (
                safe_stale_owner != STALE_LOCK_OWNERLESS
                and stale_lock_first_observed_at is not None
            )
        ):
            raise PhpDeploymentError(
            "schema bridge abort stale lock evidence is invalid"
        )
        if (
            STALE_LOCK_WRITER_ABSENCE_PATTERN.fullmatch(
                stale_lock_writer_absence_evidence
            )
            is None
        ):
            raise PhpDeploymentError(
                "schema bridge abort stale lock evidence is invalid"
            )
    _validated_protected_token(protected_token)
    validate_http_endpoint_binding(
        public_url_root=public_url_root,
        api_v2_base_url=api_v2_base_url,
        rollback_health_url=rollback_health_url,
        allow_http=False,
    )
    root = _remote_absolute_path(remote_root, label="remote root")
    _validate_gabia_core_binding(
        gabia_compatibility,
        client=client,
        remote_root=root,
        public_url_root=public_url_root,
        api_v2_base_url=api_v2_base_url,
        rollback_health_url=rollback_health_url,
    )
    private_root, safe_stage_root, safe_backup_root = _private_roots(
        root,
        stage_root,
        backup_root,
    )
    safe_release_id = _validate_release_id(release_id)
    ready = bridge_deploy_report_load()
    if ready.get("checkpoint_state") == "prepared_no_backup":
        if expected_backup_manifest_sha256 is not None:
            raise PhpDeploymentError(
                "prebackup abort must not claim a backup manifest"
            )
        return _abort_prebackup_schema_bridge(
            client,
            candidate_plan=candidate_plan,
            release_id=safe_release_id,
            prepared_checkpoint=ready,
            bridge_deploy_report_load=bridge_deploy_report_load,
            bridge_report_update=bridge_report_update,
            remote_root=root,
            private_root=private_root,
            stage_root=safe_stage_root,
            backup_root=safe_backup_root,
            public_url_root=public_url_root,
            api_v2_base_url=api_v2_base_url,
            rollback_health_url=rollback_health_url,
            protected_token=protected_token,
            http_request=http_request,
            http_timeout=http_timeout,
            gabia_compatibility=gabia_compatibility,
            stale_lock_owner_release_id=stale_lock_owner_release_id,
            stale_lock_writer_absence_evidence=(
                stale_lock_writer_absence_evidence
            ),
            stale_lock_first_observed_at=stale_lock_first_observed_at,
        )
    if (
        expected_backup_manifest_sha256 is None
        or SHA256_PATTERN.fullmatch(
            expected_backup_manifest_sha256
        )
        is None
    ):
        raise PhpDeploymentError(
            "schema bridge abort backup manifest hash is invalid"
        )
    target_snapshot = load_remote_backup(
        client,
        backup_root=safe_backup_root,
        release_id=safe_release_id,
        expected_remote_root=root,
    )
    verify_one_time_schema_bridge_backup(
        client,
        snapshot=target_snapshot,
        expected_current_sha=candidate_plan.code_revision,
        expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        expected_manifest_sha256=expected_backup_manifest_sha256,
    )
    if (
        ready.get("backup_manifest") != target_snapshot.manifest_path
        or ready.get("backup_manifest_sha256")
        != target_snapshot.manifest_sha256
        or ready.get("identity_sha256")
        != _bridge_backup_ready_identity(
            backup=target_snapshot,
            candidate_code_revision=candidate_plan.code_revision,
            previous_code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            dart_disabled_evidence=dart_disabled_evidence,
        ).get("identity_sha256")
    ):
        raise PhpDeploymentError(
            "schema bridge abort backup differs from its durable journal"
        )
    bridge_report_update("backup_ready", {"backup_ready": dict(ready)})
    ensure_private_root_http_protection(
        client,
        remote_root=root,
        private_root=private_root,
        public_url_root=public_url_root,
        http_request=http_request,
        timeout=http_timeout,
        allow_http=False,
        exclusive_writer=gabia_compatibility.exclusive_writer,
        expected_policy=gabia_compatibility.private_policy,
        expected_policy_mode=gabia_compatibility.private_policy_mode,
        allowed_private_redirect=GABIA_PRIVATE_DENY_REDIRECT,
    )

    operation_id = _validate_release_id(
        _make_release_id(
            candidate_plan.code_revision,
            prefix="schema11-bridge-abort",
        )
    )
    lock_path, stale_lock_takeover = _acquire_schema_bridge_abort_lock(
        client,
        private_root=private_root,
        replacement_release_id=operation_id,
        stale_owner_release_id=stale_lock_owner_release_id,
        writer_absence_evidence=(
            stale_lock_writer_absence_evidence
        ),
        stale_lock_first_observed_at=stale_lock_first_observed_at,
        exclusive_writer=gabia_compatibility.exclusive_writer,
        bridge_report_update=bridge_report_update,
    )
    workspace = ""
    recovery_workspace = ""
    emergency: BackupSnapshot | None = None
    mutation_started = False
    operation_error: BaseException | None = None
    recovery_error: BaseException | None = None
    cleanup_error: BaseException | None = None
    recovery_cleanup_error: BaseException | None = None
    lock_release_error: BaseException | None = None
    policy_error: BaseException | None = None
    opcache_action: str | None = None
    initial_state = "unknown"
    database_schema_before: int | None = None
    try:
        locked_ready = bridge_deploy_report_load()
        if locked_ready != ready:
            raise PhpDeploymentError(
                "schema bridge deploy journal changed under recovery lock"
            )
        locked_snapshot = load_remote_backup(
            client,
            backup_root=safe_backup_root,
            release_id=safe_release_id,
            expected_remote_root=root,
        )
        verify_one_time_schema_bridge_backup(
            client,
            snapshot=locked_snapshot,
            expected_current_sha=candidate_plan.code_revision,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            expected_manifest_sha256=expected_backup_manifest_sha256,
        )
        initial_state = classify_schema_bridge_targets(
            client,
            candidate_plan=candidate_plan,
            predecessor=locked_snapshot,
        )
        if initial_state == "candidate":
            verify_remote_release_matches_plan(
                client,
                remote_root=root,
                plan=candidate_plan,
            )
            database_schema_before = observe_closed_candidate_schema(
                base_url=api_v2_base_url,
                expected_sha=candidate_plan.code_revision,
                protected_token=protected_token,
                http_request=http_request,
                timeout=http_timeout,
            )
            synthetic_plan = LocalDeploymentPlan(
                local_root=Path("."),
                code_revision=candidate_plan.code_revision,
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
                    candidate_plan.code_revision,
                    prefix="pre-schema11-bridge-abort",
                )
            )
            emergency = capture_remote_backup(
                client,
                plan=synthetic_plan,
                remote_root=root,
                backup_root=safe_backup_root,
                release_id=emergency_id,
            )
            verify_remote_targets_match_snapshot(
                client,
                snapshot=emergency,
            )
            bridge_report_update(
                "emergency_ready",
                {
                    "emergency_backup": _bridge_emergency_identity(
                        emergency,
                        candidate_code_revision=candidate_plan.code_revision,
                    )
                },
            )
        bridge_report_update(
            "commit_started",
            {
                "initial_php_state": initial_state,
                "candidate_database_schema_version_before": (
                    database_schema_before
                ),
                "database_mutated": False,
            },
        )
        if initial_state != "predecessor":
            workspace = _create_private_workspace(
                client,
                safe_stage_root,
                operation_id,
            )
            _verify_posix_rename_capability(client, workspace)
            mutation_started = True
            restore_remote_backup(
                client,
                snapshot=locked_snapshot,
                workspace=workspace,
            )
        opcache_action = reset_opcache_with_ephemeral_probe(
            client,
            remote_root=root,
            public_url_root=public_url_root,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=False,
            exclusive_writer=gabia_compatibility.exclusive_writer,
            require_strict_state=True,
        )
        restored_release = verify_existing_remote_release_identity(
            client,
            remote_root=root,
            expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
            required_mode=DEFAULT_FILE_MODE,
        )
        if restored_release != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA:
            raise PhpDeploymentError(
                "schema bridge abort did not restore exact c06"
            )
        bridge_report_update(
            "restored",
            {
                "initial_php_state": initial_state,
                "candidate_database_schema_version_before": (
                    database_schema_before
                ),
                "database_mutated": False,
            },
        )
        verify_c06_forward_compatible_closed(
            base_url=api_v2_base_url,
            protected_token=protected_token,
            http_request=http_request,
            timeout=http_timeout,
        )
        verify_rollback_health(
            url=rollback_health_url,
            http_request=http_request,
            timeout=http_timeout,
            allow_http=False,
        )
        bridge_report_update(
            "verified",
            {
                "initial_php_state": initial_state,
                "candidate_database_schema_version_before": (
                    database_schema_before
                ),
                "database_schema_observation": (
                    "candidate_schema_" + str(database_schema_before)
                    if database_schema_before in {11, 12}
                    else (
                        "unavailable_due_c06_contract"
                        if initial_state == "predecessor"
                        else "unavailable_due_partial_php"
                    )
                ),
                "database_mutated": False,
                "v1_closed_smoke": True,
                "v2_closed_smoke": True,
                "legacy_health_smoke": True,
                "forward_compatibility_verified": True,
            },
        )
    except BaseException as error:
        operation_error = error
        if mutation_started and emergency is not None:
            try:
                recovery_id = _validate_release_id(
                    _make_release_id(
                        candidate_plan.code_revision,
                        prefix="recover-schema11-bridge-abort",
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
                    allow_http=False,
                    exclusive_writer=gabia_compatibility.exclusive_writer,
                    require_strict_state=True,
                )
                recovered_schema = observe_closed_candidate_schema(
                    base_url=api_v2_base_url,
                    expected_sha=candidate_plan.code_revision,
                    protected_token=protected_token,
                    http_request=http_request,
                    timeout=http_timeout,
                )
                if recovered_schema != database_schema_before:
                    raise PhpDeploymentError(
                        "schema bridge abort candidate recovery changed DB state"
                    )
            except BaseException as recovery_failure:
                recovery_error = recovery_failure
    finally:
        if workspace:
            try:
                _cleanup_workspace(
                    client,
                    workspace,
                    known_files=_restore_stage_paths(
                        target_snapshot,
                        workspace,
                    ),
                )
            except BaseException as error:
                cleanup_error = error
        if recovery_workspace:
            try:
                _cleanup_workspace(
                    client,
                    recovery_workspace,
                    known_files=(
                        ()
                        if emergency is None
                        else _restore_stage_paths(
                            emergency,
                            recovery_workspace,
                        )
                    ),
                )
            except BaseException as error:
                recovery_cleanup_error = error
        try:
            _release_deployment_lock(
                client,
                lock_path,
                exclusive_writer=gabia_compatibility.exclusive_writer,
            )
        except BaseException as error:
            lock_release_error = error
        try:
            _verify_gabia_private_policy(client, gabia_compatibility)
        except BaseException as error:
            policy_error = error

    if operation_error is not None:
        if recovery_error is not None:
            raise PhpDeploymentRollbackError(
                "schema bridge abort failed and candidate recovery failed"
            ) from recovery_error
        if (
            cleanup_error is not None
            or recovery_cleanup_error is not None
            or lock_release_error is not None
            or policy_error is not None
        ):
            raise PhpDeploymentError(
                "schema bridge abort failed with incomplete cleanup"
            ) from (
                cleanup_error
                or recovery_cleanup_error
                or lock_release_error
                or policy_error
            )
        if emergency is not None and mutation_started:
            raise PhpDeploymentError(
                "schema bridge abort failed safely; candidate was restored"
            ) from operation_error
        raise PhpDeploymentError(
            "schema bridge abort is incomplete; use the original deploy "
            "journal with a new recovery report"
        ) from operation_error
    if (
        cleanup_error is not None
        or recovery_cleanup_error is not None
        or lock_release_error is not None
        or policy_error is not None
    ):
        raise PhpDeploymentError(
            "schema bridge abort restored c06 but cleanup was incomplete"
        ) from (
            cleanup_error
            or recovery_cleanup_error
            or lock_release_error
            or policy_error
        )
    if opcache_action not in {"disabled_verified", "reset_verified"}:
        raise PhpDeploymentError(
            "schema bridge abort lacks verified OPcache state"
        )
    return {
        "ok": True,
        "operation": "schema-bridge-abort",
        "release_id": safe_release_id,
        "backup_manifest": target_snapshot.manifest_path,
        "backup_manifest_sha256": target_snapshot.manifest_sha256,
        "restored_code_revision": ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        "restored_api_schema_contract_version": 11,
        "candidate_code_revision": candidate_plan.code_revision,
        "candidate_expected_schema_version": 12,
        "candidate_database_schema_version_before": database_schema_before,
        "database_schema_observation": (
            "candidate_schema_" + str(database_schema_before)
            if database_schema_before in {11, 12}
            else (
                "unavailable_due_c06_contract"
                if initial_state == "predecessor"
                else "unavailable_due_partial_php"
            )
        ),
        "database_mutated": False,
        "dart_disabled_evidence": dart_disabled_evidence,
        "initial_php_state": initial_state,
        "manifest_committed_last": True,
        "byte_verification": True,
        "v1_closed_smoke": True,
        "v2_closed_smoke": True,
        "legacy_health_smoke": True,
        "forward_compatibility_verified": True,
        "closed_smoke": True,
        "opcache_action": opcache_action,
        "private_root_http_protected": True,
        "emergency_backup_release_id": (
            None if emergency is None else emergency.release_id
        ),
        "emergency_backup_identity": (
            None
            if emergency is None
            else _bridge_emergency_identity(
                emergency,
                candidate_code_revision=candidate_plan.code_revision,
            )
        ),
        "stale_lock_takeover": stale_lock_takeover,
        "stale_lock_cleanup_verified": (
            stale_lock_takeover is None
            or _lstat_or_none(
                client,
                _remote_join(private_root, "deployment-lock"),
            )
            is None
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

    ownerless_inspect_parser = commands.add_parser(
        "inspect-ownerless-lock",
        help=(
            "Read and attest an exact stale ownerless deployment lock "
            "without mutating it"
        ),
    )
    _add_sftp_arguments(ownerless_inspect_parser)

    deploy_parser = commands.add_parser(
        "deploy",
        help="Deploy or inspect the exact release through pinned SFTP",
    )
    _add_local_plan_arguments(deploy_parser)
    _add_sftp_arguments(deploy_parser)
    _add_http_arguments(deploy_parser)
    deploy_parser.add_argument("--release-id")
    deploy_parser.add_argument(
        "--report-output",
        type=Path,
        help=(
            "Absolute new owner-only JSON checkpoint/report path; required "
            "for a mutating Gabia deployment"
        ),
    )
    deploy_parser.add_argument("--private-report-root", type=Path)
    deploy_parser.add_argument("--dry-run", action="store_true")
    deploy_parser.add_argument(
        "--confirm-production-write",
        help="Must exactly match --expected-sha for a mutating deployment",
    )
    deploy_parser.add_argument(
        "--schema-upgrade-from",
        type=int,
        choices=(11,),
        help=(
            "Explicit one-time bridge that deploys schema-12 PHP while the "
            "attested existing release and database remain on schema 11"
        ),
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
    rollback_parser.add_argument("--report-output", type=Path)
    rollback_parser.add_argument("--private-report-root", type=Path)

    bridge_rollback_parser = commands.add_parser(
        "schema-bridge-rollback",
        help=(
            "One-time restore of the exact schema-11 PHP predecessor while "
            "the database remains on schema 11 or schema 12"
        ),
    )
    deploy_parser.add_argument("--expected-previous-sha")
    deploy_parser.add_argument("--confirm-previous-sha")
    deploy_parser.add_argument("--dart-disabled-evidence")
    _add_local_plan_arguments(bridge_rollback_parser)
    _add_sftp_arguments(bridge_rollback_parser)
    _add_http_arguments(bridge_rollback_parser)
    bridge_rollback_parser.add_argument("--release-id", required=True)
    bridge_rollback_parser.add_argument(
        "--expected-current-sha",
        required=True,
    )
    bridge_rollback_parser.add_argument(
        "--expected-previous-sha",
        required=True,
    )
    bridge_rollback_parser.add_argument(
        "--expected-backup-manifest-sha256",
        required=True,
    )
    bridge_rollback_parser.add_argument(
        "--confirm-rollback-release-id",
        required=True,
    )
    bridge_rollback_parser.add_argument(
        "--confirm-rollback-current-sha",
        required=True,
    )
    bridge_rollback_parser.add_argument(
        "--confirm-rollback-previous-sha",
        required=True,
    )
    bridge_rollback_parser.add_argument(
        "--confirm-backup-manifest-sha256",
        required=True,
    )
    bridge_rollback_parser.add_argument(
        "--dart-disabled-evidence",
        required=True,
    )
    bridge_rollback_parser.add_argument(
        "--report-output",
        type=Path,
        required=True,
        help="Absolute new owner-only JSON checkpoint/report path",
    )
    bridge_rollback_parser.add_argument(
        "--private-report-root",
        type=Path,
        required=True,
        help=(
            "Absolute owner-only private directory, independently confirmed "
            "by BSIDE_PRIVATE_REPORT_ROOT"
        ),
    )

    bridge_abort_parser = commands.add_parser(
        "schema-bridge-abort",
        help=(
            "Resume an interrupted schema bridge PHP commit and converge "
            "exactly to c06 without changing MySQL"
        ),
    )
    _add_local_plan_arguments(bridge_abort_parser)
    _add_sftp_arguments(bridge_abort_parser)
    _add_http_arguments(bridge_abort_parser)
    bridge_abort_parser.add_argument("--release-id", required=True)
    bridge_abort_parser.add_argument(
        "--expected-current-sha",
        required=True,
    )
    bridge_abort_parser.add_argument(
        "--expected-previous-sha",
        required=True,
    )
    bridge_abort_parser.add_argument(
        "--expected-backup-manifest-sha256",
        help=(
            "Exact backup manifest SHA-256; omit only when the durable "
            "deploy journal is still prepared with no backup"
        ),
    )
    bridge_abort_parser.add_argument(
        "--confirm-rollback-release-id",
        required=True,
    )
    bridge_abort_parser.add_argument(
        "--confirm-rollback-current-sha",
        required=True,
    )
    bridge_abort_parser.add_argument(
        "--confirm-rollback-previous-sha",
        required=True,
    )
    bridge_abort_parser.add_argument(
        "--confirm-backup-manifest-sha256",
        help=(
            "Must repeat --expected-backup-manifest-sha256; omit only for "
            "a prepared-no-backup recovery"
        ),
    )
    bridge_abort_parser.add_argument(
        "--dart-disabled-evidence",
        required=True,
    )
    bridge_abort_parser.add_argument(
        "--bridge-deploy-report",
        type=Path,
        required=True,
        help=(
            "Owner-only schema-bridge-deploy journal containing either the "
            "fixed prepared precondition or backup_ready"
        ),
    )
    bridge_abort_parser.add_argument(
        "--stale-lock-owner-release-id",
        help="Exact owner.json release_id approved for abort-only takeover",
    )
    bridge_abort_parser.add_argument(
        "--confirm-stale-lock-owner-release-id",
        help="Must exactly repeat --stale-lock-owner-release-id",
    )
    bridge_abort_parser.add_argument(
        "--stale-lock-writer-absence-evidence",
        help=(
            "github-actions:no-running-php-writers@... evidence; "
            "also required in the pinned environment variable"
        ),
    )
    bridge_abort_parser.add_argument(
        "--stale-lock-first-observed-at",
        help=(
            "UTC timestamp required only for an explicit ownerless lock "
            "takeover"
        ),
    )
    bridge_abort_parser.add_argument(
        "--report-output",
        type=Path,
        required=True,
        help=(
            "Absolute owner-only recovery report path; reuse the same path "
            "only for a durable stale-lock takeover resume"
        ),
    )
    bridge_abort_parser.add_argument(
        "--private-report-root",
        type=Path,
        required=True,
    )
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


def _fsync_parent_directory(path: Path) -> None:
    """Persist a rename where the host OS exposes directory fsync."""

    if os.name == "nt":
        return
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path.parent, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _load_durable_report_payload(destination: Path) -> dict[str, object]:
    try:
        if destination.is_symlink() or not destination.is_file():
            raise PhpDeploymentError("durable deployment report is not a regular file")
        payload = json.loads(destination.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PhpDeploymentError("durable deployment report is invalid") from error
    if not isinstance(payload, dict):
        raise PhpDeploymentError("durable deployment report is invalid")
    return payload


def load_schema_bridge_backup_ready(
    destination: Path,
    *,
    private_root: Path,
    expected_candidate_sha: str,
    expected_release_id: str,
    expected_backup_manifest_sha256: str | None,
    expected_dart_disabled_evidence: str,
) -> Mapping[str, object]:
    """Load an immutable predecessor identity from the deploy journal."""

    if (
        not destination.is_absolute()
        or not private_root.is_absolute()
        or destination != destination.resolve(strict=False)
        or private_root != private_root.resolve(strict=True)
        or destination.parent.is_symlink()
        or private_root.is_symlink()
    ):
        raise PhpDeploymentError("schema bridge journal path is not canonical")
    try:
        destination.parent.resolve(strict=True).relative_to(private_root)
    except (OSError, ValueError) as error:
        raise PhpDeploymentError(
            "schema bridge journal is outside its private root"
        ) from error
    payload = _load_durable_report_payload(destination)
    if not _valid_bridge_journal_base_identity(
        payload,
        operation="schema-bridge-deploy",
        code_revision=expected_candidate_sha,
        release_id=expected_release_id,
    ):
        raise PhpDeploymentError(
            "schema bridge deploy journal base identity does not match"
        )
    if (
        payload.get("status") == "prepared"
        and payload.get("backup_ready") is None
    ):
        if expected_backup_manifest_sha256 is not None:
            raise PhpDeploymentError(
                "prepared schema bridge journal must not claim a backup"
            )
        precondition = payload.get("bridge_precondition")
        if not _valid_bridge_prepared_identity(
            precondition,
            candidate_code_revision=expected_candidate_sha,
            release_id=expected_release_id,
            journal_nonce=payload.get("journal_nonce"),
            dart_disabled_evidence=expected_dart_disabled_evidence,
        ):
            raise PhpDeploymentError(
                "schema bridge prepared journal identity does not match"
            )
        assert isinstance(precondition, dict)
        return {
            "checkpoint_state": "prepared_no_backup",
            **dict(precondition),
        }
    ready = payload.get("backup_ready")
    if (
        expected_backup_manifest_sha256 is None
        or payload.get("status")
        not in {
            "backup_ready",
            "commit_started",
            "restored",
            "verified",
            "completed",
        }
        or not isinstance(ready, dict)
        or ready.get("candidate_code_revision") != expected_candidate_sha
        or ready.get("previous_code_revision")
        != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
        or ready.get("release_id") != expected_release_id
        or ready.get("backup_manifest_sha256")
        != expected_backup_manifest_sha256
        or ready.get("dart_disabled_evidence")
        != expected_dart_disabled_evidence
        or ready.get("database_mutated") is not False
        or not isinstance(ready.get("identity_sha256"), str)
    ):
        raise PhpDeploymentError(
            "schema bridge deploy journal identity does not match"
        )
    recomputed = dict(ready)
    claimed = recomputed.pop("identity_sha256")
    if not secrets.compare_digest(
        str(claimed),
        _sha256_bytes(_encode_json(recomputed)),
    ):
        raise PhpDeploymentError(
            "schema bridge deploy journal backup identity is invalid"
        )
    return dict(ready)


def _bridge_prepared_identity(
    *,
    candidate_code_revision: str,
    release_id: str,
    journal_nonce: str,
    dart_disabled_evidence: str,
) -> dict[str, object]:
    identity: dict[str, object] = {
        "candidate_code_revision": candidate_code_revision,
        "previous_code_revision": ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        "release_id": release_id,
        "journal_nonce": journal_nonce,
        "dart_disabled_evidence": dart_disabled_evidence,
        "database_mutated": False,
    }
    identity["identity_sha256"] = _sha256_bytes(_encode_json(identity))
    return identity


def _valid_bridge_prepared_identity(
    value: object,
    *,
    candidate_code_revision: object,
    release_id: object,
    journal_nonce: object,
    dart_disabled_evidence: object,
) -> bool:
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "candidate_code_revision",
            "previous_code_revision",
            "release_id",
            "journal_nonce",
            "dart_disabled_evidence",
            "database_mutated",
            "identity_sha256",
        }
        or value.get("candidate_code_revision")
        != candidate_code_revision
        or value.get("previous_code_revision")
        != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
        or value.get("release_id") != release_id
        or value.get("journal_nonce") != journal_nonce
        or value.get("dart_disabled_evidence")
        != dart_disabled_evidence
        or value.get("database_mutated") is not False
        or not isinstance(value.get("identity_sha256"), str)
    ):
        return False
    recomputed = dict(value)
    claimed = recomputed.pop("identity_sha256")
    return secrets.compare_digest(
        str(claimed),
        _sha256_bytes(_encode_json(recomputed)),
    )


def _bridge_prebackup_c06_identity(
    prepared: Mapping[str, object],
) -> dict[str, object]:
    identity: dict[str, object] = {
        "checkpoint_state": "prepared_no_backup",
        "candidate_code_revision": prepared.get(
            "candidate_code_revision"
        ),
        "previous_code_revision": ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        "release_id": prepared.get("release_id"),
        "deploy_journal_nonce": prepared.get("journal_nonce"),
        "dart_disabled_evidence": prepared.get(
            "dart_disabled_evidence"
        ),
        "public_file_mode": DEFAULT_FILE_MODE,
        "database_mutated": False,
    }
    identity["identity_sha256"] = _sha256_bytes(_encode_json(identity))
    return identity


def _valid_bridge_prebackup_c06_identity(
    value: object,
    *,
    candidate_code_revision: object,
    release_id: object,
) -> bool:
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "checkpoint_state",
            "candidate_code_revision",
            "previous_code_revision",
            "release_id",
            "deploy_journal_nonce",
            "dart_disabled_evidence",
            "public_file_mode",
            "database_mutated",
            "identity_sha256",
        }
        or value.get("checkpoint_state") != "prepared_no_backup"
        or value.get("candidate_code_revision")
        != candidate_code_revision
        or value.get("previous_code_revision")
        != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
        or value.get("release_id") != release_id
        or not isinstance(value.get("deploy_journal_nonce"), str)
        or re.fullmatch(
            r"[0-9a-f]{32}",
            str(value["deploy_journal_nonce"]),
        )
        is None
        or not isinstance(value.get("dart_disabled_evidence"), str)
        or DART_DISABLED_EVIDENCE_PATTERN.fullmatch(
            str(value["dart_disabled_evidence"])
        )
        is None
        or value.get("public_file_mode") != DEFAULT_FILE_MODE
        or value.get("database_mutated") is not False
        or not isinstance(value.get("identity_sha256"), str)
    ):
        return False
    recomputed = dict(value)
    claimed = recomputed.pop("identity_sha256")
    return secrets.compare_digest(
        str(claimed),
        _sha256_bytes(_encode_json(recomputed)),
    )


def _replace_durable_report(
    destination: Path,
    payload: Mapping[str, object],
) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".checkpoint",
        dir=str(destination.parent),
    )
    temporary = Path(temporary_name)
    try:
        os.chmod(temporary, PRIVATE_FILE_MODE)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(_encode_json(payload))
            handle.flush()
            os.fsync(handle.fileno())
        if destination.is_symlink() or not destination.is_file():
            raise PhpDeploymentError(
                "durable deployment report changed before checkpoint commit"
            )
        os.replace(temporary, destination)
        os.chmod(destination, PRIVATE_FILE_MODE)
        _fsync_parent_directory(destination)
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise


def _bridge_backup_ready_identity(
    *,
    backup: BackupSnapshot,
    candidate_code_revision: str,
    previous_code_revision: str,
    dart_disabled_evidence: str,
) -> dict[str, object]:
    identity = {
        "release_id": backup.release_id,
        "candidate_code_revision": candidate_code_revision,
        "previous_code_revision": previous_code_revision,
        "backup_directory": backup.backup_directory,
        "backup_manifest": backup.manifest_path,
        "backup_manifest_sha256": backup.manifest_sha256,
        "backup_manifest_bytes": backup.manifest_size,
        "remote_root": backup.remote_root,
        "dart_disabled_evidence": dart_disabled_evidence,
        "database_mutated": False,
    }
    identity["identity_sha256"] = _sha256_bytes(_encode_json(identity))
    return identity


def _valid_bridge_backup_ready_identity(
    value: object,
    *,
    candidate_code_revision: object,
    release_id: object,
) -> bool:
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "release_id",
            "candidate_code_revision",
            "previous_code_revision",
            "backup_directory",
            "backup_manifest",
            "backup_manifest_sha256",
            "backup_manifest_bytes",
            "remote_root",
            "dart_disabled_evidence",
            "database_mutated",
            "identity_sha256",
        }
        or value.get("candidate_code_revision")
        != candidate_code_revision
        or value.get("previous_code_revision")
        != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
        or value.get("release_id") != release_id
        or not isinstance(value.get("backup_directory"), str)
        or not isinstance(value.get("backup_manifest"), str)
        or not isinstance(value.get("backup_manifest_sha256"), str)
        or SHA256_PATTERN.fullmatch(
            str(value["backup_manifest_sha256"])
        )
        is None
        or type(value.get("backup_manifest_bytes")) is not int
        or int(value["backup_manifest_bytes"]) < 1
        or not isinstance(value.get("remote_root"), str)
        or not isinstance(value.get("dart_disabled_evidence"), str)
        or DART_DISABLED_EVIDENCE_PATTERN.fullmatch(
            str(value["dart_disabled_evidence"])
        )
        is None
        or value.get("database_mutated") is not False
        or not isinstance(value.get("identity_sha256"), str)
    ):
        return False
    try:
        backup_directory = _remote_absolute_path(
            str(value["backup_directory"]),
            label="bridge backup directory",
        )
        _remote_absolute_path(
            str(value["remote_root"]),
            label="bridge remote root",
        )
        if value.get("backup_manifest") != _remote_join(
            backup_directory,
            "backup-manifest.json",
        ):
            return False
    except PhpDeploymentError:
        return False
    recomputed = dict(value)
    claimed = recomputed.pop("identity_sha256")
    return secrets.compare_digest(
        str(claimed),
        _sha256_bytes(_encode_json(recomputed)),
    )


def _bridge_emergency_identity(
    backup: BackupSnapshot,
    *,
    candidate_code_revision: str,
) -> dict[str, object]:
    identity: dict[str, object] = {
        "release_id": backup.release_id,
        "candidate_code_revision": candidate_code_revision,
        "backup_directory": backup.backup_directory,
        "backup_manifest": backup.manifest_path,
        "backup_manifest_sha256": backup.manifest_sha256,
        "backup_manifest_bytes": backup.manifest_size,
        "remote_root": backup.remote_root,
        "database_mutated": False,
    }
    identity["identity_sha256"] = _sha256_bytes(_encode_json(identity))
    return identity


def _valid_bridge_emergency_identity(
    value: object,
    *,
    candidate_code_revision: object,
) -> bool:
    if (
        not isinstance(value, dict)
        or value.get("candidate_code_revision") != candidate_code_revision
        or value.get("database_mutated") is not False
        or not isinstance(value.get("identity_sha256"), str)
        or not isinstance(value.get("backup_manifest_sha256"), str)
        or SHA256_PATTERN.fullmatch(str(value["backup_manifest_sha256"]))
        is None
    ):
        return False
    recomputed = dict(value)
    claimed = recomputed.pop("identity_sha256")
    return secrets.compare_digest(
        str(claimed),
        _sha256_bytes(_encode_json(recomputed)),
    )


def _valid_ownerless_remote_identity(
    value: object,
    *,
    remote_mtime: object,
) -> bool:
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "lock_path",
            "st_mode",
            "st_size",
            "st_mtime",
            "st_uid",
            "st_gid",
            "st_ino",
            "st_dev",
        }
        or value.get("schema_version") != 1
        or not isinstance(value.get("lock_path"), str)
        or value.get("st_mode") != PRIVATE_DIRECTORY_MODE
        or type(value.get("st_size")) is not int
        or not isinstance(value.get("st_mtime"), (int, float))
        or isinstance(value.get("st_mtime"), bool)
        or not math.isfinite(float(value["st_mtime"]))
        or any(
            value.get(field) is not None
            and type(value.get(field)) is not int
            for field in ("st_uid", "st_gid", "st_ino", "st_dev")
        )
        or not isinstance(remote_mtime, str)
    ):
        return False
    try:
        _remote_absolute_path(
            str(value["lock_path"]),
            label="ownerless lock path",
        )
        expected_mtime = datetime.fromtimestamp(
            float(value["st_mtime"]),
            timezone.utc,
        ).isoformat()
        _parse_utc_timestamp(remote_mtime)
    except (OSError, OverflowError, ValueError, PhpDeploymentError):
        return False
    return secrets.compare_digest(expected_mtime, remote_mtime)


def _valid_stale_lock_takeover_identity(value: object) -> bool:
    evidence = (
        None
        if not isinstance(value, dict)
        else value.get("writer_absence_evidence")
    )
    match = (
        None
        if not isinstance(evidence, str)
        else STALE_LOCK_WRITER_ABSENCE_PATTERN.fullmatch(evidence)
    )
    remote_identity = (
        value.get("stale_owner_remote_identity")
        if isinstance(value, dict)
        else None
    )
    if (
        not isinstance(value, dict)
        or not isinstance(value.get("stale_owner_release_id"), str)
        or (
            value.get("stale_owner_release_id") != STALE_LOCK_OWNERLESS
            and RELEASE_ID_PATTERN.fullmatch(
                str(value["stale_owner_release_id"])
            )
            is None
        )
        or (
            value.get("stale_owner_release_id") != STALE_LOCK_OWNERLESS
            and not str(value["stale_owner_release_id"]).startswith(
                SCHEMA_BRIDGE_STALE_LOCK_PREFIXES
            )
        )
        or value.get("stale_owner_state")
        not in ("owner_present", "ownerless")
        or not isinstance(value.get("stale_owner_sha256"), str)
        or SHA256_PATTERN.fullmatch(str(value["stale_owner_sha256"]))
        is None
        or not isinstance(value.get("stale_owner_acquired_at"), str)
        or match is None
        or match.group("owner_sha256")
        != value.get("stale_owner_sha256")
        or match.group("acquired_at_sha256")
        != _sha256_bytes(
            str(value["stale_owner_acquired_at"]).encode("ascii")
        )
        or match.group("nonce") != value.get("writer_absence_nonce")
        or match.group("issued_at")
        != value.get("writer_absence_issued_at")
        or (
            value.get("stale_owner_state") == "ownerless"
            and value.get("stale_owner_release_id")
            != STALE_LOCK_OWNERLESS
        )
        or (
            value.get("stale_owner_state") == "owner_present"
            and value.get("stale_owner_release_id")
            == STALE_LOCK_OWNERLESS
        )
        or (
            value.get("stale_owner_state") == "ownerless"
            and (
                not isinstance(remote_identity, dict)
                or not _valid_ownerless_remote_identity(
                    remote_identity,
                    remote_mtime=value.get(
                        "stale_owner_remote_mtime"
                    ),
                )
                or value.get("stale_owner_acquired_at")
                != value.get("stale_owner_remote_mtime")
                or value.get("stale_owner_sha256")
                != _sha256_bytes(
                    _encode_json(remote_identity)
                )
            )
        )
        or (
            value.get("stale_owner_state") == "owner_present"
            and (
                value.get("stale_owner_remote_identity") is not None
                or value.get("stale_owner_remote_mtime") is not None
            )
        )
        or not isinstance(value.get("replacement_release_id"), str)
        or RELEASE_ID_PATTERN.fullmatch(
            str(value["replacement_release_id"])
        )
        is None
        or not str(value["replacement_release_id"]).startswith(
            "schema11-bridge-abort-"
        )
        or value.get("database_mutated") is not False
        or not isinstance(value.get("identity_sha256"), str)
    ):
        return False
    try:
        assert match is not None
        _parse_utc_timestamp(
            match.group("issued_at"),
            compact=True,
        )
        _parse_utc_timestamp(
            str(value["stale_owner_acquired_at"])
        )
    except PhpDeploymentError:
        return False
    recomputed = dict(value)
    claimed = recomputed.pop("identity_sha256")
    return secrets.compare_digest(
        str(claimed),
        _sha256_bytes(_encode_json(recomputed)),
    )


def _bridge_journal_base_identity(
    value: Mapping[str, object],
) -> dict[str, object]:
    return {
        "schema_version": value.get("schema_version"),
        "operation": value.get("operation"),
        "code_revision": value.get("code_revision"),
        "release_id": value.get("release_id"),
        "journal_nonce": value.get("journal_nonce"),
        "prepared_at": value.get("prepared_at"),
    }


def _valid_bridge_journal_base_identity(
    value: object,
    *,
    operation: object,
    code_revision: object,
    release_id: object,
) -> bool:
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != 2
        or value.get("operation") != operation
        or value.get("code_revision") != code_revision
        or value.get("release_id") != release_id
        or not isinstance(value.get("journal_nonce"), str)
        or re.fullmatch(
            r"[0-9a-f]{32}",
            str(value["journal_nonce"]),
        )
        is None
        or not isinstance(value.get("prepared_at"), str)
        or not isinstance(
            value.get("journal_identity_sha256"),
            str,
        )
        or SHA256_PATTERN.fullmatch(
            str(value["journal_identity_sha256"])
        )
        is None
    ):
        return False
    try:
        _parse_utc_timestamp(str(value["prepared_at"]))
    except PhpDeploymentError:
        return False
    return secrets.compare_digest(
        str(value["journal_identity_sha256"]),
        _sha256_bytes(
            _encode_json(_bridge_journal_base_identity(value))
        ),
    )


def _valid_bridge_fixed_recovery_pair(
    value: Mapping[str, object],
    *,
    code_revision: object,
    release_id: object,
) -> bool:
    ready_valid = _valid_bridge_backup_ready_identity(
        value.get("backup_ready"),
        candidate_code_revision=code_revision,
        release_id=release_id,
    )
    prebackup_valid = _valid_bridge_prebackup_c06_identity(
        value.get("prebackup_c06"),
        candidate_code_revision=code_revision,
        release_id=release_id,
    )
    return ready_valid != prebackup_valid


def _advance_bridge_report(
    destination: Path,
    *,
    operation: str,
    code_revision: str,
    release_id: str,
    expected_statuses: Sequence[str],
    status: str,
    evidence: Mapping[str, object],
) -> None:
    """Atomically advance a crash-recoverable bridge journal."""

    payload = _load_durable_report_payload(destination)
    if (
        not _valid_bridge_journal_base_identity(
            payload,
            operation=operation,
            code_revision=code_revision,
            release_id=release_id,
        )
        or payload.get("status") not in set(expected_statuses)
    ):
        raise PhpDeploymentError("schema bridge journal identity is invalid")
    existing_ready = payload.get("backup_ready")
    supplied_ready = evidence.get("backup_ready")
    if existing_ready is not None and (
        not isinstance(existing_ready, dict)
        or supplied_ready is not None and supplied_ready != existing_ready
    ):
        raise PhpDeploymentError("schema bridge backup_ready identity changed")
    existing_prebackup = payload.get("prebackup_c06")
    supplied_prebackup = evidence.get("prebackup_c06")
    if existing_prebackup is not None and (
        not isinstance(existing_prebackup, dict)
        or (
            supplied_prebackup is not None
            and supplied_prebackup != existing_prebackup
        )
    ):
        raise PhpDeploymentError(
            "schema bridge prebackup c06 identity changed"
        )
    next_payload = dict(payload)
    next_payload["status"] = status
    next_payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    for key, value in evidence.items():
        if key in {
            "schema_version",
            "operation",
            "code_revision",
            "release_id",
            "journal_nonce",
            "prepared_at",
        }:
            raise PhpDeploymentError("schema bridge journal field is reserved")
        next_payload[key] = value
    prebackup = next_payload.get("prebackup_c06")
    if status != "prepared":
        if (
            operation != "schema-bridge-abort"
            and prebackup is not None
        ) or not _valid_bridge_fixed_recovery_pair(
            next_payload,
            code_revision=code_revision,
            release_id=release_id,
        ):
            raise PhpDeploymentError(
                "schema bridge journal requires exactly one fixed recovery "
                "identity"
            )
    _replace_durable_report(destination, next_payload)


def _bridge_report_updater(
    destination: Path,
    *,
    operation: str,
    code_revision: str,
    release_id: str,
) -> BridgeReportUpdater:
    transitions = {
        "backup_ready": ("prepared",),
        "prebackup_c06_ready": ("prepared",),
        "stale_lock_takeover_ready": (
            "backup_ready",
            "prebackup_c06_ready",
        ),
        "stale_lock_takeover_complete": (
            "stale_lock_takeover_ready",
        ),
        "emergency_ready": (
            "prepared",
            "backup_ready",
            "stale_lock_takeover_complete",
        ),
        "commit_started": (
            "backup_ready",
            "prebackup_c06_ready",
            "emergency_ready",
            "stale_lock_takeover_complete",
        ),
        "restored": ("commit_started",),
        "verified": ("restored", "commit_started"),
    }

    def update(status: str, evidence: Mapping[str, object]) -> None:
        allowed = transitions.get(status)
        if allowed is None:
            raise PhpDeploymentError("unsupported schema bridge journal transition")
        current = _load_durable_report_payload(destination)
        if (
            not _valid_bridge_journal_base_identity(
                current,
                operation=operation,
                code_revision=code_revision,
                release_id=release_id,
            )
        ):
            raise PhpDeploymentError(
                "schema bridge journal identity is invalid"
            )
        if current.get("status") != "prepared" and (
            (
                operation != "schema-bridge-abort"
                and current.get("prebackup_c06") is not None
            )
            or not _valid_bridge_fixed_recovery_pair(
                current,
                code_revision=code_revision,
                release_id=release_id,
            )
        ):
            raise PhpDeploymentError(
                "schema bridge journal fixed recovery identity is invalid"
            )
        if (
            current.get("status") == status
            and all(current.get(key) == value for key, value in evidence.items())
        ):
            return
        fixed_identity_key = (
            "backup_ready"
            if status == "backup_ready"
            else (
                "prebackup_c06"
                if status == "prebackup_c06_ready"
                else None
            )
        )
        if (
            fixed_identity_key is not None
            and current.get(fixed_identity_key)
            == evidence.get(fixed_identity_key)
            and current.get("status") != "prepared"
        ):
            return
        _advance_bridge_report(
            destination,
            operation=operation,
            code_revision=code_revision,
            release_id=release_id,
            expected_statuses=allowed,
            status=status,
            evidence=evidence,
        )

    setattr(
        update,
        "_bside_bridge_state_loader",
        lambda: _load_durable_report_payload(destination),
    )
    return update


def _prepare_durable_report(
    output_path: Path | None,
    *,
    private_root: Path | None,
    operation: str,
    code_revision: str,
    release_id: str | None,
    environ: Mapping[str, str] | None = None,
    schema_bridge_dart_disabled_evidence: str | None = None,
    allow_bridge_abort_resume: bool = False,
) -> Path | None:
    """Reserve an owner-only report path before a production mutation."""

    if output_path is None:
        return None
    if not output_path.is_absolute() or private_root is None:
        raise PhpDeploymentError(
            "report output and its private root must be absolute paths"
        )
    if not private_root.is_absolute():
        raise PhpDeploymentError("private report root must be an absolute path")
    destination = output_path
    if destination != destination.resolve(strict=False):
        raise PhpDeploymentError("report output path must be canonical")
    parent = destination.parent.resolve()
    safe_private_root = private_root.resolve()
    environment = os.environ if environ is None else environ
    configured_private_root = environment.get(PRIVATE_REPORT_ROOT_ENV)
    try:
        confirmed_private_root = (
            None
            if configured_private_root is None
            else Path(configured_private_root).resolve()
        )
        parent.relative_to(safe_private_root)
    except (OSError, ValueError) as error:
        raise PhpDeploymentError(
            "report output is outside its private root"
        ) from error
    if (
        confirmed_private_root != safe_private_root
        or private_root.is_symlink()
        or not safe_private_root.is_dir()
        or destination.parent.is_symlink()
        or not parent.is_dir()
        or destination.is_symlink()
    ):
        raise PhpDeploymentError(
            "report output requires the independently confirmed private root"
        )
    if os.name != "nt" and stat.S_IMODE(safe_private_root.stat().st_mode) & 0o077:
        raise PhpDeploymentError(
            "private report root must not grant group or other access"
        )
    if destination.exists():
        if (
            not allow_bridge_abort_resume
            or operation != "schema-bridge-abort"
        ):
            raise PhpDeploymentError(
                "durable deployment report already exists"
            )
        existing = _load_durable_report_payload(destination)
        destination_mode = stat.S_IMODE(destination.stat().st_mode)
        ready_valid = _valid_bridge_backup_ready_identity(
            existing.get("backup_ready"),
            candidate_code_revision=code_revision,
            release_id=release_id,
        )
        prebackup_valid = _valid_bridge_prebackup_c06_identity(
            existing.get("prebackup_c06"),
            candidate_code_revision=code_revision,
            release_id=release_id,
        )
        if (
            not _valid_bridge_journal_base_identity(
                existing,
                operation=operation,
                code_revision=code_revision,
                release_id=release_id,
            )
            or existing.get("status")
            not in (
                "stale_lock_takeover_ready",
                "stale_lock_takeover_complete",
            )
            or not _valid_stale_lock_takeover_identity(
                existing.get("stale_lock_takeover")
            )
            or ready_valid == prebackup_valid
            or (
                existing.get("status")
                == "stale_lock_takeover_complete"
                and existing.get("stale_lock_cleanup_verified")
                is not True
            )
            or (os.name != "nt" and destination_mode != PRIVATE_FILE_MODE)
        ):
            raise PhpDeploymentError(
                "durable bridge abort report is not resumable"
            )
        return destination
    schema_version = (
        2
        if operation
        in {
            "schema-bridge-deploy",
            "schema-bridge-rollback",
            "schema-bridge-abort",
        }
        else 1
    )
    prepared_payload: dict[str, object] = {
        "schema_version": schema_version,
        "status": "prepared",
        "operation": operation,
        "code_revision": code_revision,
        "release_id": release_id,
        "prepared_at": datetime.now(timezone.utc).isoformat(),
    }
    if schema_version == 2:
        prepared_payload["journal_nonce"] = secrets.token_hex(16)
        prepared_payload["journal_identity_sha256"] = _sha256_bytes(
            _encode_json(
                _bridge_journal_base_identity(prepared_payload)
            )
        )
    if operation == "schema-bridge-deploy":
        if (
            release_id is None
            or not release_id.startswith("php-v2-")
            or schema_bridge_dart_disabled_evidence is None
            or DART_DISABLED_EVIDENCE_PATTERN.fullmatch(
                schema_bridge_dart_disabled_evidence
            )
            is None
        ):
            raise PhpDeploymentError(
                "schema bridge deploy report requires a php-v2 release ID "
                "and fixed DART-off evidence"
            )
        prepared_payload["bridge_precondition"] = (
            _bridge_prepared_identity(
                candidate_code_revision=code_revision,
                release_id=release_id,
                journal_nonce=str(prepared_payload["journal_nonce"]),
                dart_disabled_evidence=(
                    schema_bridge_dart_disabled_evidence
                ),
            )
        )
    prepared = _encode_json(
        prepared_payload
    )
    descriptor = -1
    try:
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            PRIVATE_FILE_MODE,
        )
        os.write(descriptor, prepared)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.chmod(destination, PRIVATE_FILE_MODE)
        _fsync_parent_directory(destination)
    except OSError as error:
        if descriptor >= 0:
            os.close(descriptor)
        destination.unlink(missing_ok=True)
        raise PhpDeploymentError(
            "durable deployment report could not be prepared"
        ) from error
    return destination


def _commit_durable_report(
    destination: Path | None,
    report: Mapping[str, object],
) -> None:
    if destination is None:
        return
    prepared = _load_durable_report_payload(destination)
    operation = prepared.get("operation")
    bridge_operation = operation in {
        "schema-bridge-deploy",
        "schema-bridge-rollback",
        "schema-bridge-abort",
    }
    if (
        prepared.get("schema_version") != (2 if bridge_operation else 1)
        or (
            bridge_operation
            and not _valid_bridge_journal_base_identity(
                prepared,
                operation=operation,
                code_revision=prepared.get("code_revision"),
                release_id=prepared.get("release_id"),
            )
        )
        or (
            prepared.get("status") != "verified"
            if bridge_operation
            else prepared.get("status") != "prepared"
        )
        or operation not in {
            "deploy",
            "rollback",
            "schema-bridge-deploy",
            "schema-bridge-rollback",
            "schema-bridge-abort",
        }
        or report.get("ok") is not True
        or report.get("operation") != operation
        or report.get("release_id") != prepared.get("release_id")
    ):
        raise PhpDeploymentError(
            "deployment report does not match its prepared checkpoint"
        )
    if operation == "deploy":
        backup_identity = report.get("backup_identity")
        if (
            report.get("code_revision") != prepared.get("code_revision")
            or not isinstance(backup_identity, dict)
            or backup_identity.get("release_id") != report.get("release_id")
            or backup_identity.get("candidate_code_revision")
            != report.get("code_revision")
            or backup_identity.get("manifest_path")
            != report.get("backup_manifest")
            or backup_identity.get("manifest_sha256")
            != report.get("backup_manifest_sha256")
            or not isinstance(report.get("backup_manifest_sha256"), str)
            or SHA256_PATTERN.fullmatch(
                str(report["backup_manifest_sha256"])
            )
            is None
        ):
            raise PhpDeploymentError(
                "deployment report backup identity is incomplete"
            )
    elif operation == "schema-bridge-deploy":
        ready = prepared.get("backup_ready")
        backup_identity = report.get("backup_identity")
        required_true: tuple[str, ...] = (
            "manifest_committed_last",
            "byte_verification",
            "v1_closed_smoke",
            "v2_closed_smoke",
            "private_root_http_protected",
        )
        if (
            not isinstance(ready, dict)
            or not isinstance(backup_identity, dict)
            or report.get("code_revision") != prepared.get("code_revision")
            or report.get("previous_code_revision")
            != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
            or report.get("database_mutated") is not False
            or report.get("dart_disabled_evidence")
            != ready.get("dart_disabled_evidence")
            or report.get("backup_manifest")
            != ready.get("backup_manifest")
            or report.get("backup_manifest_sha256")
            != ready.get("backup_manifest_sha256")
            or backup_identity.get("manifest_sha256")
            != ready.get("backup_manifest_sha256")
            or backup_identity.get("candidate_code_revision")
            != prepared.get("code_revision")
            or any(report.get(field) is not True for field in required_true)
            or report.get("opcache_action")
            not in {"disabled_verified", "reset_verified"}
        ):
            raise PhpDeploymentError(
                "schema bridge deploy report evidence is incomplete"
            )
    elif operation in {"schema-bridge-rollback", "schema-bridge-abort"}:
        ready = prepared.get("backup_ready")
        prebackup = prepared.get("prebackup_c06")
        emergency = prepared.get("emergency_backup")
        report_emergency = report.get("emergency_backup_identity")
        stale_takeover = prepared.get("stale_lock_takeover")
        initial_state = report.get("initial_php_state")
        database_before = report.get(
            "candidate_database_schema_version_before"
        )
        database_observation = report.get(
            "database_schema_observation"
        )
        prebackup_recovery = (
            operation == "schema-bridge-abort"
            and initial_state == "prebackup_c06"
        )
        ready_valid = isinstance(ready, dict)
        if ready_valid:
            assert isinstance(ready, dict)
            recomputed_ready = dict(ready)
            ready_claimed = recomputed_ready.pop(
                "identity_sha256",
                None,
            )
            ready_valid = (
                ready.get("candidate_code_revision")
                == prepared.get("code_revision")
                and ready.get("previous_code_revision")
                == ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
                and ready.get("release_id")
                == prepared.get("release_id")
                and ready.get("database_mutated") is False
                and isinstance(ready_claimed, str)
                and secrets.compare_digest(
                    ready_claimed,
                    _sha256_bytes(_encode_json(recomputed_ready)),
                )
            )
        prebackup_valid = _valid_bridge_prebackup_c06_identity(
            prebackup,
            candidate_code_revision=prepared.get("code_revision"),
            release_id=prepared.get("release_id"),
        )
        if operation == "schema-bridge-rollback":
            state_matrix_valid = (
                initial_state is None
                and database_before in (11, 12)
                and database_observation
                == "candidate_schema_" + str(database_before)
                and ready_valid
                and not prebackup_valid
            )
            emergency_required = True
        elif initial_state == "candidate":
            state_matrix_valid = (
                database_before in (11, 12)
                and database_observation
                == "candidate_schema_" + str(database_before)
                and ready_valid
                and not prebackup_valid
            )
            emergency_required = True
        elif initial_state in (
            "mixed",
            "predecessor_restore_transition",
        ):
            state_matrix_valid = (
                database_before is None
                and database_observation
                == "unavailable_due_partial_php"
                and ready_valid
                and not prebackup_valid
            )
            emergency_required = False
        elif initial_state == "predecessor":
            state_matrix_valid = (
                database_before is None
                and database_observation
                == "unavailable_due_c06_contract"
                and ready_valid
                and not prebackup_valid
            )
            emergency_required = False
        elif prebackup_recovery:
            state_matrix_valid = (
                database_before is None
                and database_observation
                == "unavailable_due_c06_contract"
                and prebackup_valid
                and not ready_valid
                and report.get("prebackup_c06_identity")
                == prebackup
            )
            emergency_required = False
        else:
            state_matrix_valid = False
            emergency_required = False
        required_true = (
            "byte_verification",
            "v1_closed_smoke",
            "v2_closed_smoke",
            "legacy_health_smoke",
            "private_root_http_protected",
            "forward_compatibility_verified",
        )
        if (
            not state_matrix_valid
            or report.get("candidate_code_revision")
            != prepared.get("code_revision")
            or report.get("restored_code_revision")
            != ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
            or report.get("restored_api_schema_contract_version") != 11
            or report.get("database_mutated") is not False
            or (
                prebackup_recovery
                and (
                    report.get("manifest_committed_last") is not False
                    or report.get("manifest_commit_not_applicable")
                    is not True
                    or report.get("public_release_files_mutated")
                    is not False
                    or report.get(
                        "ephemeral_opcache_probe_created_and_removed"
                    )
                    is not True
                    or report.get(
                        "orphan_private_artifacts_preserved"
                    )
                    is not True
                    or not isinstance(
                        report.get(
                            "preserved_private_artifact_paths"
                        ),
                        list,
                    )
                )
            )
            or (
                not prebackup_recovery
                and report.get("manifest_committed_last") is not True
            )
            or (
                ready_valid
                and (
                    not isinstance(ready, dict)
                    or report.get("backup_manifest")
                    != ready.get("backup_manifest")
                    or report.get("backup_manifest_sha256")
                    != ready.get("backup_manifest_sha256")
                    or report.get("dart_disabled_evidence")
                    != ready.get("dart_disabled_evidence")
                )
            )
            or (
                prebackup_valid
                and (
                    not isinstance(prebackup, dict)
                    or report.get("backup_manifest") is not None
                    or report.get("backup_manifest_sha256") is not None
                    or report.get("dart_disabled_evidence")
                    != prebackup.get("dart_disabled_evidence")
                )
            )
            or (
                emergency_required
                and (
                    not _valid_bridge_emergency_identity(
                        emergency,
                        candidate_code_revision=prepared.get(
                            "code_revision"
                        ),
                    )
                    or report_emergency != emergency
                    or not isinstance(emergency, dict)
                    or not isinstance(ready, dict)
                    or emergency.get("remote_root")
                    != ready.get("remote_root")
                    or report.get("emergency_backup_release_id")
                    != emergency.get("release_id")
                )
            )
            or (
                not emergency_required
                and (
                    emergency is not None
                    or report_emergency is not None
                    or report.get("emergency_backup_release_id")
                    is not None
                )
            )
            or (
                stale_takeover is not None
                and (
                    not _valid_stale_lock_takeover_identity(
                        stale_takeover
                    )
                    or report.get("stale_lock_takeover")
                    != stale_takeover
                    or report.get("stale_lock_cleanup_verified") is not True
                    or prepared.get("stale_lock_cleanup_verified")
                    is not True
                )
            )
            or (
                stale_takeover is None
                and report.get("stale_lock_takeover") is not None
            )
            or any(report.get(field) is not True for field in required_true)
            or report.get("opcache_action")
            not in {"disabled_verified", "reset_verified"}
        ):
            raise PhpDeploymentError(
                "schema bridge recovery report identity is incomplete"
            )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".complete",
        dir=str(destination.parent),
    )
    temporary = Path(temporary_name)
    try:
        os.chmod(temporary, PRIVATE_FILE_MODE)
        completed_payload: dict[str, object]
        if bridge_operation:
            completed_payload = dict(prepared)
            completed_payload.update(
                {
                    "status": "completed",
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "report": dict(report),
                }
            )
        else:
            completed_payload = {
                "schema_version": 1,
                "status": "completed",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "report": dict(report),
            }
        encoded = _encode_json(completed_payload)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        os.chmod(destination, PRIVATE_FILE_MODE)
        _fsync_parent_directory(destination)
    except OSError as error:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise PhpDeploymentError(
            "deployment completed but its durable report was not committed"
        ) from error


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
    schema_upgrade_from = getattr(args, "schema_upgrade_from", None)
    partial_schema_bridge = getattr(args, "command", None) == (
        "schema-bridge-abort"
    )
    expected_core_files = (
        LEGACY_SCHEMA_11_CORE_API_FILES
        if schema_upgrade_from == 11
        else CORE_API_FILES
    )
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
        expected_core_files=expected_core_files,
        allow_partial_schema_bridge=partial_schema_bridge,
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

        if args.command == "inspect-ownerless-lock":
            options = ssh_sftp_options_from_args(args)
            with ParamikoPinnedSftpSession(options) as client:
                report = inspect_ownerless_deployment_lock(
                    client,
                    remote_root=args.remote_root,
                )
            _print_report(report)
            return 0

        if args.command == "deploy":
            if (
                args.dry_run
                and args.gabia_core_compatibility_host is not None
            ):
                raise PhpDeploymentError(
                    "Gabia compatibility probe is not available in dry-run"
                )
            if args.dry_run and args.schema_upgrade_from is not None:
                raise PhpDeploymentError(
                    "schema upgrade bridge is not available in dry-run"
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
                if args.schema_upgrade_from == 11:
                    confirm_one_time_schema_bridge_deploy(
                        expected_previous_sha=_required_cli_text(
                            args.expected_previous_sha,
                            label="schema bridge previous SHA",
                        ),
                        previous_sha_confirmation=args.confirm_previous_sha,
                        dart_disabled_evidence=_required_cli_text(
                            args.dart_disabled_evidence,
                            label="DART disabled evidence",
                        ),
                    )
            if (
                not args.dry_run
                and args.gabia_core_compatibility_host is not None
                and (
                    args.report_output is None
                    or args.private_report_root is None
                    or args.release_id is None
                )
            ):
                raise PhpDeploymentError(
                    "mutating Gabia deployment requires an explicit release "
                    "ID, private report root, and durable report output"
                )
            durable_report = (
                None
                if args.dry_run
                else _prepare_durable_report(
                    args.report_output,
                    private_root=args.private_report_root,
                    operation=(
                        "schema-bridge-deploy"
                        if args.schema_upgrade_from == 11
                        else "deploy"
                    ),
                    code_revision=plan.code_revision,
                    release_id=args.release_id,
                    schema_bridge_dart_disabled_evidence=(
                        args.dart_disabled_evidence
                        if args.schema_upgrade_from == 11
                        else None
                    ),
                )
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
                        expected_current_sha=(
                            ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
                            if args.schema_upgrade_from == 11
                            else None
                        ),
                    )
                    bridge_report_update = (
                        None
                        if args.schema_upgrade_from != 11
                        or durable_report is None
                        else _bridge_report_updater(
                            durable_report,
                            operation="schema-bridge-deploy",
                            code_revision=plan.code_revision,
                            release_id=_required_cli_text(
                                args.release_id,
                                label="release ID",
                            ),
                        )
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
                        schema_upgrade_from=args.schema_upgrade_from,
                        expected_previous_sha=(
                            args.expected_previous_sha
                            if args.schema_upgrade_from == 11
                            else None
                        ),
                        dart_disabled_evidence=(
                            args.dart_disabled_evidence
                            if args.schema_upgrade_from == 11
                            else None
                        ),
                        bridge_report_update=bridge_report_update,
                    )
            _commit_durable_report(durable_report, report)
            _print_report(report)
            return 0

        if args.command == "schema-bridge-abort":
            if (
                args.allow_http
                or args.gabia_core_compatibility_host
                != GABIA_COMPATIBILITY_SSH_HOST
            ):
                raise PhpDeploymentError(
                    "schema bridge abort requires the exact pinned Gabia "
                    "HTTPS compatibility target"
                )
            candidate_plan = build_local_deployment_plan(
                args.local_root,
                expected_sha=args.expected_sha,
            )
            verify_release_checkout(
                candidate_plan,
                require_repository_clean=True,
            )
            if args.expected_current_sha != candidate_plan.code_revision:
                raise PhpDeploymentError(
                    "schema bridge current SHA does not match the local release"
                )
            recorded_abort_resume = False
            if args.report_output.exists():
                try:
                    existing_abort = _load_durable_report_payload(
                        args.report_output
                    )
                    recorded_abort_resume = (
                        _valid_bridge_journal_base_identity(
                            existing_abort,
                            operation="schema-bridge-abort",
                            code_revision=candidate_plan.code_revision,
                            release_id=args.release_id,
                        )
                        and existing_abort.get("status")
                        in (
                            "stale_lock_takeover_ready",
                            "stale_lock_takeover_complete",
                        )
                        and _valid_stale_lock_takeover_identity(
                            existing_abort.get(
                                "stale_lock_takeover"
                            )
                        )
                        and _valid_bridge_fixed_recovery_pair(
                            existing_abort,
                            code_revision=candidate_plan.code_revision,
                            release_id=args.release_id,
                        )
                        and (
                            existing_abort.get("status")
                            == "stale_lock_takeover_ready"
                            or existing_abort.get(
                                "stale_lock_cleanup_verified"
                            )
                            is True
                        )
                    )
                except PhpDeploymentError:
                    recorded_abort_resume = False
            confirm_one_time_schema_bridge_rollback(
                release_id=args.release_id,
                release_id_confirmation=args.confirm_rollback_release_id,
                expected_current_sha=args.expected_current_sha,
                current_sha_confirmation=args.confirm_rollback_current_sha,
                expected_previous_sha=args.expected_previous_sha,
                previous_sha_confirmation=args.confirm_rollback_previous_sha,
                expected_backup_manifest_sha256=(
                    args.expected_backup_manifest_sha256
                ),
                backup_sha256_confirmation=(
                    args.confirm_backup_manifest_sha256
                ),
                dart_disabled_evidence=args.dart_disabled_evidence,
                allow_missing_backup=(
                    args.expected_backup_manifest_sha256 is None
                ),
            )
            confirm_schema_bridge_stale_lock_takeover(
                owner_release_id=args.stale_lock_owner_release_id,
                owner_release_id_confirmation=(
                    args.confirm_stale_lock_owner_release_id
                ),
                writer_absence_evidence=(
                    args.stale_lock_writer_absence_evidence
                ),
                first_observed_at=args.stale_lock_first_observed_at,
                allow_recorded_evidence=recorded_abort_resume,
            )
            if args.bridge_deploy_report == args.report_output:
                raise PhpDeploymentError(
                    "schema bridge deploy and abort journals must differ"
                )
            def deploy_report_load() -> Mapping[str, object]:
                return load_schema_bridge_backup_ready(
                    args.bridge_deploy_report,
                    private_root=args.private_report_root,
                    expected_candidate_sha=candidate_plan.code_revision,
                    expected_release_id=args.release_id,
                    expected_backup_manifest_sha256=(
                        args.expected_backup_manifest_sha256
                    ),
                    expected_dart_disabled_evidence=(
                        args.dart_disabled_evidence
                    ),
                )
            deploy_report_load()
            durable_report = _prepare_durable_report(
                args.report_output,
                private_root=args.private_report_root,
                operation="schema-bridge-abort",
                code_revision=candidate_plan.code_revision,
                release_id=args.release_id,
                allow_bridge_abort_resume=True,
            )
            if durable_report is None:
                raise PhpDeploymentError(
                    "schema bridge abort durable report is missing"
                )
            bridge_report_update = _bridge_report_updater(
                durable_report,
                operation="schema-bridge-abort",
                code_revision=candidate_plan.code_revision,
                release_id=args.release_id,
            )
            options = ssh_sftp_options_from_args(args)
            protected_token = _protected_token_from_environment(
                args.protected_token_env
            )
            with ParamikoPinnedSftpSession(options) as client:
                compatibility = _prepare_cli_gabia_compatibility(
                    client,
                    args=args,
                    options=options,
                    expected_current_sha=None,
                )
                report = abort_one_time_schema_bridge(
                    client,
                    candidate_plan=candidate_plan,
                    release_id=args.release_id,
                    expected_backup_manifest_sha256=(
                        args.expected_backup_manifest_sha256
                    ),
                    dart_disabled_evidence=args.dart_disabled_evidence,
                    bridge_deploy_report_load=deploy_report_load,
                    bridge_report_update=bridge_report_update,
                    remote_root=args.remote_root,
                    stage_root=args.stage_root,
                    backup_root=args.backup_root,
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
                    gabia_compatibility=compatibility,
                    stale_lock_owner_release_id=(
                        args.stale_lock_owner_release_id
                    ),
                    stale_lock_writer_absence_evidence=(
                        args.stale_lock_writer_absence_evidence
                    ),
                    stale_lock_first_observed_at=(
                        args.stale_lock_first_observed_at
                    ),
                )
            _commit_durable_report(durable_report, report)
            _print_report(report)
            return 0

        if args.command == "schema-bridge-rollback":
            if (
                args.allow_http
                or args.gabia_core_compatibility_host
                != GABIA_COMPATIBILITY_SSH_HOST
            ):
                raise PhpDeploymentError(
                    "schema bridge rollback requires the exact pinned Gabia "
                    "HTTPS compatibility target"
                )
            candidate_plan = build_local_deployment_plan(
                args.local_root,
                expected_sha=args.expected_sha,
            )
            verify_release_checkout(
                candidate_plan,
                require_repository_clean=True,
            )
            if args.expected_current_sha != candidate_plan.code_revision:
                raise PhpDeploymentError(
                    "schema bridge current SHA does not match the local release"
                )
            confirm_one_time_schema_bridge_rollback(
                release_id=args.release_id,
                release_id_confirmation=args.confirm_rollback_release_id,
                expected_current_sha=args.expected_current_sha,
                current_sha_confirmation=args.confirm_rollback_current_sha,
                expected_previous_sha=args.expected_previous_sha,
                previous_sha_confirmation=args.confirm_rollback_previous_sha,
                expected_backup_manifest_sha256=(
                    args.expected_backup_manifest_sha256
                ),
                backup_sha256_confirmation=(
                    args.confirm_backup_manifest_sha256
                ),
                dart_disabled_evidence=args.dart_disabled_evidence,
            )
            durable_report = _prepare_durable_report(
                args.report_output,
                private_root=args.private_report_root,
                operation="schema-bridge-rollback",
                code_revision=candidate_plan.code_revision,
                release_id=args.release_id,
            )
            options = ssh_sftp_options_from_args(args)
            protected_token = _protected_token_from_environment(
                args.protected_token_env
            )
            with ParamikoPinnedSftpSession(options) as client:
                compatibility = _prepare_cli_gabia_compatibility(
                    client,
                    args=args,
                    options=options,
                    expected_current_sha=args.expected_current_sha,
                )
                if durable_report is None:
                    raise PhpDeploymentError(
                        "schema bridge rollback durable report is missing"
                    )
                bridge_report_update = _bridge_report_updater(
                    durable_report,
                    operation="schema-bridge-rollback",
                    code_revision=candidate_plan.code_revision,
                    release_id=args.release_id,
                )
                report = rollback_one_time_schema_bridge(
                    client,
                    candidate_plan=candidate_plan,
                    release_id=args.release_id,
                    expected_current_sha=args.expected_current_sha,
                    expected_previous_sha=args.expected_previous_sha,
                    expected_backup_manifest_sha256=(
                        args.expected_backup_manifest_sha256
                    ),
                    remote_root=args.remote_root,
                    stage_root=args.stage_root,
                    backup_root=args.backup_root,
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
                    gabia_compatibility=compatibility,
                    dart_disabled_evidence=args.dart_disabled_evidence,
                    bridge_report_update=bridge_report_update,
                )
            _commit_durable_report(durable_report, report)
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
        durable_report = (
            None
            if args.dry_run
            else _prepare_durable_report(
                args.report_output,
                private_root=args.private_report_root,
                operation="rollback",
                code_revision=(
                    expected_current_sha
                    or "0" * 40
                ),
                release_id=args.release_id,
            )
        )
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
        _commit_durable_report(durable_report, report)
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
