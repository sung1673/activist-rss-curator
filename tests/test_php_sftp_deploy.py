from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import posixpath
import re
import stat
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote, urlsplit

import pytest

import curator.php_sftp_deploy as php_deploy
from curator.deployment_manifest import CORE_API_FILES, write_deployment_manifest
from curator.mysql_backup import legacy_ssh_rsa_sha1_is_allowed
from curator.php_sftp_deploy import (
    CORE_RELEASE_CONFIRMATION_ENV,
    CORE_ROLLBACK_CURRENT_SHA_ENV,
    CORE_ROLLBACK_RELEASE_ID_ENV,
    DEFAULT_COMMIT_ORDER,
    DEFAULT_REMOTE_ROOT,
    DEPLOYMENT_MANIFEST_NAME,
    GABIA_COMPATIBILITY_SSH_HOST,
    GABIA_PRIVATE_DENY_REDIRECT,
    GABIA_SSH_HOST_KEY_SHA256,
    HttpResponse,
    LEGACY_SCHEMA_11_CORE_API_FILES,
    ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
    ParamikoPinnedSftpSession,
    PhpDeploymentError,
    build_local_deployment_plan,
    confirm_production_release,
    confirm_production_rollback,
    confirm_one_time_schema_bridge_rollback,
    deploy_release,
    inspect_remote_deployment,
    load_remote_backup,
    local_plan_report,
    main,
    prepare_gabia_core_compatibility,
    reset_opcache_with_ephemeral_probe,
    rollback_one_time_schema_bridge,
    rollback_release,
    ssh_sftp_options_from_args,
    verify_closed_v2_api,
    verify_existing_remote_release_identity,
)


RELEASE_SHA = "a" * 40
LEGACY_RELEASE_SHA = "b" * 40
RELEASE_ID = "php-v2-aaaaaaaaaaaa-20260725t000000z-12345678"
PUBLIC_ROOT = "https://alignpe.gabia.io/activist"
API_V2 = PUBLIC_ROOT + "/api.php/api/v2"
API_V1 = PUBLIC_ROOT + "/api.php/api/v1"
ROLLBACK_HEALTH = PUBLIC_ROOT + "/api.php?action=health"
PROTECTED_TOKEN = "ops-protected-token-" + "z" * 40
DART_DISABLED_EVIDENCE = (
    "github-variable:DART_OFFICIAL_INGEST_ENABLED=false@run-20260727"
)


def _stale_writer_absence_evidence(
    *,
    owner_content: bytes,
    acquired_at_reference: str,
    nonce: str = "1" * 32,
    issued_at: datetime | None = None,
) -> str:
    issued = (
        datetime.now(timezone.utc)
        if issued_at is None
        else issued_at
    ).strftime("%Y%m%dT%H%M%SZ")
    return (
        f"github-actions:no-running-php-writers@{issued}:"
        f"owner_sha256={hashlib.sha256(owner_content).hexdigest()}:"
        "acquired_at_sha256="
        f"{hashlib.sha256(acquired_at_reference.encode('ascii')).hexdigest()}:"
        f"nonce={nonce}"
    )


def _age_test_lock_owner(
    client: MemorySftp,
    lock_path: str,
) -> tuple[str, str]:
    owner_path = lock_path + "/owner.json"
    payload = json.loads(client.files[owner_path])
    acquired_at = (
        datetime.now(timezone.utc) - timedelta(minutes=30)
    ).isoformat()
    payload["acquired_at"] = acquired_at
    owner_content = php_deploy._encode_json(payload)
    client.files[owner_path] = owner_content
    return acquired_at, _stale_writer_absence_evidence(
        owner_content=owner_content,
        acquired_at_reference=acquired_at,
    )


def _age_test_ownerless_lock(
    client: MemorySftp,
    lock_path: str,
    *,
    first_observed_delta: int = 30,
    remote_mtime_delta: int = 30,
    issued_delta: int = 0,
    owner_content_override: bytes | None = None,
) -> tuple[str, str, bytes]:
    now = datetime.now(timezone.utc)
    client.mtimes[lock_path] = int(
        (now - timedelta(minutes=remote_mtime_delta)).timestamp()
    )
    remote_identity, _identity, remote_mtime = (
        php_deploy._read_exact_ownerless_deployment_lock(
            client,
            lock_path=lock_path,
        )
    )
    first_observed = (
        remote_mtime
        if first_observed_delta == remote_mtime_delta
        else (
            now - timedelta(minutes=first_observed_delta)
        ).isoformat()
    )
    evidence = _stale_writer_absence_evidence(
        owner_content=(
            remote_identity
            if owner_content_override is None
            else owner_content_override
        ),
        acquired_at_reference=first_observed,
        issued_at=now - timedelta(minutes=issued_delta),
    )
    return first_observed, evidence, remote_identity


def _recording_bridge_updater() -> object:
    states: list[str] = []

    def update(status: str, _evidence: object) -> None:
        states.append(status)

    return update


class _Writer(io.BytesIO):
    def __init__(self, callback: object) -> None:
        super().__init__()
        self.callback = callback

    def close(self) -> None:
        if not self.closed:
            callback = self.callback
            assert callable(callback)
            callback(self.getvalue())
        super().close()


class MemorySftp:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.modes: dict[str, int] = {}
        self.directories: dict[str, int] = {"/": 0o755}
        self.mtimes: dict[str, int] = {
            "/": int(datetime.now(timezone.utc).timestamp())
        }
        self.inodes: dict[str, int] = {"/": 1}
        self.next_inode = 2
        self.symlinks: set[str] = set()
        self.mutations = 0
        self.mkdir_calls: list[str] = []

    @staticmethod
    def _path(path: str) -> str:
        normalized = posixpath.normpath(path)
        assert normalized.startswith("/")
        return normalized

    def add_directory(self, path: str, mode: int = 0o755) -> None:
        normalized = self._path(path)
        parent = posixpath.dirname(normalized)
        assert parent in self.directories
        self.directories[normalized] = mode
        self.mtimes[normalized] = int(
            datetime.now(timezone.utc).timestamp()
        )
        self.inodes[normalized] = self.next_inode
        self.next_inode += 1

    def add_file(self, path: str, content: bytes, mode: int = 0o644) -> None:
        normalized = self._path(path)
        assert posixpath.dirname(normalized) in self.directories
        self.files[normalized] = content
        self.modes[normalized] = mode
        self.mtimes[normalized] = int(
            datetime.now(timezone.utc).timestamp()
        )
        self.inodes[normalized] = self.next_inode
        self.next_inode += 1

    def lstat(self, path: str) -> object:
        normalized = self._path(path)
        if normalized in self.symlinks:
            return SimpleNamespace(st_mode=stat.S_IFLNK | 0o777, st_size=0)
        if normalized in self.files:
            return SimpleNamespace(
                st_mode=stat.S_IFREG | self.modes[normalized],
                st_size=len(self.files[normalized]),
                st_mtime=self.mtimes.get(normalized, 0),
                st_uid=1000,
                st_gid=1000,
                st_ino=self.inodes.get(normalized, 0),
                st_dev=1,
            )
        if normalized in self.directories:
            return SimpleNamespace(
                st_mode=stat.S_IFDIR | self.directories[normalized],
                st_size=0,
                st_mtime=self.mtimes.get(normalized, 0),
                st_uid=1000,
                st_gid=1000,
                st_ino=self.inodes.get(normalized, 0),
                st_dev=1,
            )
        raise FileNotFoundError(2, "not found")

    def listdir(self, path: str) -> list[str]:
        normalized = self._path(path)
        if normalized not in self.directories:
            raise FileNotFoundError(2, "not found")
        prefix = normalized.rstrip("/") + "/"
        children = {
            candidate[len(prefix) :].split("/", 1)[0]
            for candidate in (
                *self.files,
                *self.directories,
                *self.symlinks,
            )
            if candidate.startswith(prefix)
            and candidate != normalized
        }
        return sorted(children)

    def mkdir(self, path: str, mode: int = 0o777) -> None:
        normalized = self._path(path)
        self.mkdir_calls.append(normalized)
        if (
            normalized in self.files
            or normalized in self.directories
            or normalized in self.symlinks
        ):
            raise FileExistsError(17, "exists")
        if posixpath.dirname(normalized) not in self.directories:
            raise FileNotFoundError(2, "parent missing")
        self.directories[normalized] = mode
        self.mtimes[normalized] = int(
            datetime.now(timezone.utc).timestamp()
        )
        self.inodes[normalized] = self.next_inode
        self.next_inode += 1
        self.mutations += 1

    def chmod(self, path: str, mode: int) -> None:
        normalized = self._path(path)
        if normalized in self.files:
            self.modes[normalized] = mode
        elif normalized in self.directories:
            self.directories[normalized] = mode
        else:
            raise FileNotFoundError(2, "not found")
        self.mutations += 1

    def open(self, path: str, mode: str = "r") -> object:
        normalized = self._path(path)
        if "r" in mode:
            if normalized not in self.files:
                raise FileNotFoundError(2, "not found")
            return io.BytesIO(self.files[normalized])
        if "x" in mode and (
            normalized in self.files
            or normalized in self.directories
            or normalized in self.symlinks
        ):
            raise FileExistsError(17, "exists")
        if "w" not in mode and "x" not in mode:
            raise ValueError("unsupported test mode")
        if posixpath.dirname(normalized) not in self.directories:
            raise FileNotFoundError(2, "parent missing")

        def commit(content: bytes) -> None:
            created = normalized not in self.files
            self.files[normalized] = content
            self.modes.setdefault(normalized, 0o666)
            self.mtimes[normalized] = int(
                datetime.now(timezone.utc).timestamp()
            )
            if created:
                self.inodes[normalized] = self.next_inode
                self.next_inode += 1
            self.mutations += 1

        return _Writer(commit)

    def remove(self, path: str) -> None:
        normalized = self._path(path)
        if normalized not in self.files:
            raise FileNotFoundError(2, "not found")
        del self.files[normalized]
        self.modes.pop(normalized, None)
        self.mtimes.pop(normalized, None)
        self.inodes.pop(normalized, None)
        self.mutations += 1

    def rmdir(self, path: str) -> None:
        normalized = self._path(path)
        if normalized not in self.directories:
            raise FileNotFoundError(2, "not found")
        prefix = normalized + "/"
        if any(
            candidate.startswith(prefix)
            for candidate in (*self.files, *self.directories)
            if candidate != normalized
        ):
            raise OSError(39, "not empty")
        del self.directories[normalized]
        self.mtimes.pop(normalized, None)
        self.inodes.pop(normalized, None)
        self.mutations += 1

    def posix_rename(self, oldpath: str, newpath: str) -> None:
        source = self._path(oldpath)
        target = self._path(newpath)
        if source not in self.files:
            raise FileNotFoundError(2, "not found")
        if posixpath.dirname(target) not in self.directories:
            raise FileNotFoundError(2, "parent missing")
        if target in self.directories or target in self.symlinks:
            raise OSError(21, "target is not a file")
        self.files[target] = self.files.pop(source)
        self.modes[target] = self.modes.pop(source)
        self.mtimes[target] = self.mtimes.pop(source)
        self.inodes[target] = self.inodes.pop(source)
        self.mutations += 1

    def rename(self, oldpath: str, newpath: str) -> None:
        source = self._path(oldpath)
        target = self._path(newpath)
        if source not in self.files:
            raise FileNotFoundError(2, "not found")
        if (
            target in self.files
            or target in self.directories
            or target in self.symlinks
        ):
            raise FileExistsError(17, "exists")
        if posixpath.dirname(target) not in self.directories:
            raise FileNotFoundError(2, "parent missing")
        self.files[target] = self.files.pop(source)
        self.modes[target] = self.modes.pop(source)
        self.mtimes[target] = self.mtimes.pop(source)
        self.inodes[target] = self.inodes.pop(source)
        self.mutations += 1

    def close(self) -> None:
        return


class _GabiaExclusiveHandle:
    def __enter__(self) -> _GabiaExclusiveHandle:
        return self

    def __exit__(self, *_args: object) -> None:
        return

    def write(self, _content: bytes) -> int:
        raise OSError("File not open for writing")

    def flush(self) -> None:
        return


class GabiaMemorySftp(MemorySftp):
    def open(self, path: str, mode: str = "r") -> object:
        normalized = self._path(path)
        if "x" not in mode:
            return super().open(path, mode)
        if (
            normalized in self.files
            or normalized in self.directories
            or normalized in self.symlinks
        ):
            raise FileExistsError(17, "exists")
        if posixpath.dirname(normalized) not in self.directories:
            raise FileNotFoundError(2, "parent missing")
        self.files[normalized] = b""
        self.modes[normalized] = 0o644
        self.mtimes[normalized] = int(
            datetime.now(timezone.utc).timestamp()
        )
        self.inodes[normalized] = self.next_inode
        self.next_inode += 1
        self.mutations += 1
        return _GabiaExclusiveHandle()


class ReplacingRenameGabiaSftp(GabiaMemorySftp):
    def rename(self, oldpath: str, newpath: str) -> None:
        self.posix_rename(oldpath, newpath)


class HttpRouter:
    def __init__(
        self,
        sftp: MemorySftp,
        *,
        code_revision: str,
        fail_v2_health: bool = False,
        fail_probe: bool = False,
        strip_authorization: bool = False,
        protected_release_state: str = "closed",
        private_canary_mode: str = "blocked",
        public_canary_mode: str = "mapped",
        strict_opcache_action: str | None = None,
        schema_version: int = 12,
        pending_actual_schema_version: int | None = None,
    ) -> None:
        self.sftp = sftp
        self.code_revision = code_revision
        self.fail_v2_health = fail_v2_health
        self.fail_probe = fail_probe
        self.strip_authorization = strip_authorization
        self.protected_release_state = protected_release_state
        self.private_canary_mode = private_canary_mode
        self.public_canary_mode = public_canary_mode
        self.strict_opcache_action = strict_opcache_action
        self.schema_version = schema_version
        self.pending_actual_schema_version = pending_actual_schema_version
        self.calls: list[tuple[str, str]] = []
        self.probe_tokens: list[str] = []
        self.private_canary_paths: list[str] = []
        self.public_canary_paths: list[str] = []
        self.public_canary_modes: list[int] = []

    @staticmethod
    def _json(status: int, payload: object) -> HttpResponse:
        return HttpResponse(
            status=status,
            headers={
                "Content-Type": "application/json",
                "X-BSIDE-API-Version": "v2",
            },
            body=json.dumps(payload, separators=(",", ":")).encode(),
        )

    @staticmethod
    def _v1_json(status: int, payload: object) -> HttpResponse:
        return HttpResponse(
            status=status,
            headers={
                "Content-Type": "application/json",
                "X-BSIDE-API-Version": "v1",
            },
            body=json.dumps(payload, separators=(",", ":")).encode(),
        )

    def __call__(
        self,
        method: str,
        url: str,
        headers: object,
        _timeout: float,
    ) -> HttpResponse:
        assert isinstance(headers, dict)
        self.calls.append((method, url))
        parsed = urlsplit(url)
        filename = unquote(posixpath.basename(parsed.path))
        public_match = re.fullmatch(
            r"bside-public-canary-([0-9a-f]{64})\.txt",
            filename,
        )
        if method == "GET" and public_match is not None:
            canary_path = DEFAULT_REMOTE_ROOT + "/" + filename
            self.public_canary_paths.append(canary_path)
            self.public_canary_modes.append(self.sftp.modes[canary_path])
            source = self.sftp.files[canary_path]
            if self.public_canary_mode == "missing":
                return HttpResponse(
                    status=404,
                    headers={"Content-Type": "text/plain"},
                    body=b"not found",
                )
            if self.public_canary_mode == "wrong-body":
                return HttpResponse(
                    status=200,
                    headers={"Content-Type": "application/octet-stream"},
                    body=b"wrong public root",
                )
            assert self.public_canary_mode == "mapped"
            return HttpResponse(
                status=200,
                headers={"Content-Type": "application/octet-stream"},
                body=source,
            )
        private_match = re.fullmatch(
            r"\.bside-private-canary-([0-9a-f]{64})\.txt",
            filename,
        )
        if method == "GET" and private_match is not None:
            canary_path = DEFAULT_REMOTE_ROOT + "/_private/" + filename
            self.private_canary_paths.append(canary_path)
            source = self.sftp.files[canary_path]
            if self.private_canary_mode == "exposed":
                return HttpResponse(
                    status=200,
                    headers={"Content-Type": "application/octet-stream"},
                    body=source,
                )
            if self.private_canary_mode == "redirect":
                return HttpResponse(
                    status=302,
                    headers={"Location": "https://other.example/private"},
                    body=b"redirect",
                )
            if self.private_canary_mode == "gabia-redirect":
                return HttpResponse(
                    status=302,
                    headers={"Location": GABIA_PRIVATE_DENY_REDIRECT},
                    body=b"Gabia access-denied document",
                )
            assert self.private_canary_mode == "blocked"
            return HttpResponse(
                status=403,
                headers={"Content-Type": "text/plain"},
                body=b"forbidden",
            )
        match = re.fullmatch(r"\.bside-opcache-([0-9a-f]{64})\.php", filename)
        if method == "POST" and match is not None:
            token = str(headers["X-BSIDE-OPcache-Token"])
            self.probe_tokens.append(token)
            probe_path = DEFAULT_REMOTE_ROOT + "/" + filename
            source = self.sftp.files[probe_path]
            assert token.encode() not in source
            assert hashlib.sha256(token.encode()).hexdigest().encode() in source
            if self.fail_probe:
                return self._json(503, {"ok": False, "error": "failed"})
            if self.strict_opcache_action is not None:
                assert b"opcache_get_status(false)" in source
                enabled = self.strict_opcache_action == "reset_verified"
                return self._json(
                    200,
                    {
                        "ok": True,
                        "opcache_action": self.strict_opcache_action,
                        "probe_id": match.group(1),
                        "extension_loaded": True,
                        "reset_function": True,
                        "status_function": True,
                        "status_available": True,
                        "status_enabled": enabled,
                        "ini_enable": "1",
                        "validate_timestamps": "1",
                        "revalidate_freq": "2",
                        "reset_result": True if enabled else None,
                    },
                )
            return self._json(
                200,
                {
                    "ok": True,
                    "opcache_reset": True,
                    "probe_id": match.group(1),
                },
            )
        if url == API_V1 + "/health":
            return self._v1_json(
                200,
                {
                    "ok": True,
                    "service": "bside-governance-intelligence",
                    "api_version": "v1",
                },
            )
        if url == API_V1 + "/events?limit=1":
            return self._v1_json(
                503,
                {
                    "ok": False,
                    "error": "governance_release_closed",
                    "api_version": "v1",
                },
            )
        if url == API_V1 + "/admin/release-state":
            if headers.get("Authorization") != "Bearer " + PROTECTED_TOKEN:
                return self._v1_json(
                    401,
                    {
                        "ok": False,
                        "error": "bearer_token_required",
                        "api_version": "v1",
                    },
                )
            return self._v1_json(
                200,
                {
                    "ok": True,
                    "release_state": "closed",
                    "schema_version": 10,
                    "api_version": "v1",
                },
            )
        if url == API_V2 + "/health":
            if self.fail_v2_health:
                return self._json(500, {"ok": False, "api_version": "v2"})
            return self._json(
                200,
                {
                    "ok": True,
                    "service": "bside-global-market-terminal",
                    "code_revision": self.code_revision,
                    "schema_version": self.schema_version,
                    "api_version": "v2",
                },
            )
        if url == API_V2 + "/openapi.yaml":
            return HttpResponse(
                status=200,
                headers={
                    "Content-Type": "application/yaml; charset=utf-8",
                    "X-BSIDE-API-Version": "v2",
                },
                body=(
                    "openapi: 3.1.0\n"
                    f"x-schema-version: {self.schema_version}\n"
                ).encode(),
            )
        if url == API_V2 + "/__bside_sftp_deploy_not_found__":
            return self._json(
                404,
                {"ok": False, "error": "not_found", "api_version": "v2"},
            )
        if url == API_V2 + "/events?limit=1":
            if self.pending_actual_schema_version is not None:
                return self._json(
                    503,
                    {
                        "ok": False,
                        "error": "schema_version_mismatch",
                        "expected_schema_version": self.schema_version,
                        "actual_schema_version": (
                            self.pending_actual_schema_version
                        ),
                        "api_version": "v2",
                    },
                )
            return self._json(
                503,
                {
                    "ok": False,
                    "error": "global_terminal_release_closed",
                    "api_version": "v2",
                },
            )
        if url == API_V2 + "/admin/release-state":
            return self._json(
                401,
                {
                    "ok": False,
                    "error": "bearer_token_required",
                    "api_version": "v2",
                },
            )
        if url == API_V2 + "/ops/release-state":
            if self.strip_authorization:
                return self._json(
                    401,
                    {
                        "ok": False,
                        "error": "bearer_token_required",
                        "api_version": "v2",
                    },
                )
            assert headers.get("Authorization") == "Bearer " + PROTECTED_TOKEN
            if self.pending_actual_schema_version is not None:
                return self._json(
                    503,
                    {
                        "ok": False,
                        "error": "schema_version_mismatch",
                        "expected_schema_version": self.schema_version,
                        "actual_schema_version": (
                            self.pending_actual_schema_version
                        ),
                        "api_version": "v2",
                    },
                )
            return self._json(
                200,
                {
                    "ok": True,
                    "data": {"release_state": self.protected_release_state},
                    "api_version": "v2",
                },
            )
        if url == ROLLBACK_HEALTH:
            return self._json(200, {"ok": True})
        raise AssertionError(f"unexpected request: {method} {url}")


class SchemaBridgeHttpRouter:
    def __init__(
        self,
        sftp: MemorySftp,
        *,
        previous_release_sha: str = LEGACY_RELEASE_SHA,
        private_canary_mode: str = "blocked",
        strict_opcache_action: str | None = None,
        candidate_actual_schema_version: int = 11,
    ) -> None:
        self.sftp = sftp
        self.legacy = HttpRouter(
            sftp,
            code_revision=previous_release_sha,
            schema_version=11,
            private_canary_mode=private_canary_mode,
            strict_opcache_action=strict_opcache_action,
        )
        self.candidate = HttpRouter(
            sftp,
            code_revision=RELEASE_SHA,
            schema_version=12,
            pending_actual_schema_version=(
                None
                if candidate_actual_schema_version == 12
                else candidate_actual_schema_version
            ),
            private_canary_mode=private_canary_mode,
            strict_opcache_action=strict_opcache_action,
        )
        self.previous_release_sha = previous_release_sha

    def __call__(
        self,
        method: str,
        url: str,
        headers: object,
        timeout: float,
    ) -> HttpResponse:
        manifest_path = DEFAULT_REMOTE_ROOT + "/" + DEPLOYMENT_MANIFEST_NAME
        if manifest_path not in self.sftp.files:
            return self.legacy(method, url, headers, timeout)
        manifest = json.loads(self.sftp.files[manifest_path])
        revision = manifest["code_revision"]
        if revision == self.previous_release_sha:
            return self.legacy(method, url, headers, timeout)
        if revision == RELEASE_SHA:
            return self.candidate(method, url, headers, timeout)
        raise AssertionError(f"unexpected deployed revision: {revision}")


@pytest.fixture
def local_plan(tmp_path: Path) -> object:
    root = tmp_path / "deploy" / "activist"
    (root / "migrations").mkdir(parents=True)
    for relative_path in CORE_API_FILES:
        path = root.joinpath(*relative_path.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"candidate:{relative_path}\n".encode())
    write_deployment_manifest(
        root,
        code_revision=RELEASE_SHA,
        output=root / DEPLOYMENT_MANIFEST_NAME,
    )
    return build_local_deployment_plan(root, expected_sha=RELEASE_SHA)


@pytest.fixture
def production_sftp() -> MemorySftp:
    client = MemorySftp()
    client.add_directory("/www_root")
    client.add_directory(DEFAULT_REMOTE_ROOT)
    client.add_directory(DEFAULT_REMOTE_ROOT + "/_private", 0o700)
    client.add_directory(DEFAULT_REMOTE_ROOT + "/migrations")
    client.add_file(DEFAULT_REMOTE_ROOT + "/.htaccess", b"old htaccess\n", 0o640)
    client.add_file(DEFAULT_REMOTE_ROOT + "/api.php", b"old api\n", 0o640)
    client.add_file(
        DEFAULT_REMOTE_ROOT + "/governance_v1.php",
        b"old v1\n",
        0o644,
    )
    client.add_file(
        DEFAULT_REMOTE_ROOT + "/openapi.yaml",
        b"old openapi\n",
        0o644,
    )
    client.mutations = 0
    return client


def _artifact_bytes(plan: object, relative_path: str) -> bytes:
    artifact_by_path = getattr(plan, "artifact_by_path")
    return artifact_by_path[relative_path].path.read_bytes()


def _install_attested_release(
    client: MemorySftp,
    *,
    code_revision: str,
    core_files: tuple[str, ...],
    file_overrides: dict[str, bytes] | None = None,
) -> None:
    hashes: dict[str, str] = {}
    overrides = {} if file_overrides is None else file_overrides
    for relative_path in core_files:
        target = DEFAULT_REMOTE_ROOT + "/" + relative_path
        content = overrides.get(
            relative_path,
            f"existing:{relative_path}\n".encode(),
        )
        client.files[target] = content
        client.modes[target] = 0o644
        hashes[relative_path] = hashlib.sha256(content).hexdigest()
    for relative_path in set(CORE_API_FILES) - set(core_files):
        target = DEFAULT_REMOTE_ROOT + "/" + relative_path
        client.files.pop(target, None)
        client.modes.pop(target, None)
    manifest = {
        "schema_version": 1,
        "code_revision": code_revision,
        "files": hashes,
    }
    client.files[
        DEFAULT_REMOTE_ROOT + "/" + DEPLOYMENT_MANIFEST_NAME
    ] = json.dumps(
        manifest,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    client.modes[
        DEFAULT_REMOTE_ROOT + "/" + DEPLOYMENT_MANIFEST_NAME
    ] = 0o644
    client.mutations = 0


def test_local_plan_attests_all_core_files_and_commits_manifest_last(
    local_plan: object,
) -> None:
    report = local_plan_report(local_plan)
    paths = [item["path"] for item in report["files"]]  # type: ignore[index]

    assert set(CORE_API_FILES) == set(DEFAULT_COMMIT_ORDER[:-1])
    assert len(CORE_API_FILES) == 9
    assert paths == list(DEFAULT_COMMIT_ORDER)
    assert paths[-2:] == ["api.php", DEPLOYMENT_MANIFEST_NAME]
    assert report["mutated_remote"] is False


def test_schema_11_manifest_shape_is_an_exact_one_file_predecessor() -> None:
    assert len(LEGACY_SCHEMA_11_CORE_API_FILES) == 8
    assert len(CORE_API_FILES) == 9
    assert set(LEGACY_SCHEMA_11_CORE_API_FILES) | {
        "migrations/012_dart_credential_pool.sql"
    } == set(CORE_API_FILES)


def test_deploy_creates_verified_backup_and_closed_release(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    http = HttpRouter(production_sftp, code_revision=RELEASE_SHA)

    result = deploy_release(
        production_sftp,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=http,
    )

    assert result["ok"] is True
    assert result["manifest_committed_last"] is True
    for relative_path in DEFAULT_COMMIT_ORDER:
        assert production_sftp.files[
            DEFAULT_REMOTE_ROOT + "/" + relative_path
        ] == _artifact_bytes(local_plan, relative_path)
    backup_manifest = (
        DEFAULT_REMOTE_ROOT
        + "/_private/deployment-backups/"
        + RELEASE_ID
        + "/backup-manifest.json"
    )
    assert backup_manifest in production_sftp.files
    backup = json.loads(production_sftp.files[backup_manifest])
    assert backup["files"]["api.php"]["existed"] is True
    assert backup["files"]["governance_v2.php"]["existed"] is False
    assert DEFAULT_REMOTE_ROOT + "/_private/deployment-lock" not in (
        production_sftp.directories
    )
    assert not any("deployment-staging" in path for path in production_sftp.files)
    assert not any(".bside-opcache-" in path for path in production_sftp.files)
    assert production_sftp.files[
        DEFAULT_REMOTE_ROOT + "/_private/.htaccess"
    ] == php_deploy.PRIVATE_ROOT_DENY_RULES
    assert production_sftp.modes[
        DEFAULT_REMOTE_ROOT + "/_private/.htaccess"
    ] == 0o644
    assert len(http.private_canary_paths) == 1
    assert http.private_canary_paths[0] not in production_sftp.files
    assert len(http.public_canary_paths) == 1
    assert http.public_canary_paths[0] not in production_sftp.files
    assert http.public_canary_modes == [0o644]
    assert len(http.probe_tokens) == 1


def test_failed_closed_smoke_automatically_restores_first_deployment(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    old_files = dict(production_sftp.files)
    old_modes = dict(production_sftp.modes)
    http = HttpRouter(
        production_sftp,
        code_revision=RELEASE_SHA,
        fail_v2_health=True,
    )

    with pytest.raises(
        PhpDeploymentError,
        match="previous files were restored",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=http,
        )

    for relative_path in DEFAULT_COMMIT_ORDER:
        target = DEFAULT_REMOTE_ROOT + "/" + relative_path
        if target in old_files:
            assert production_sftp.files[target] == old_files[target]
            assert production_sftp.modes[target] == old_modes[target]
        else:
            assert target not in production_sftp.files
    assert len(http.probe_tokens) == 2
    assert not any(".bside-opcache-" in path for path in production_sftp.files)
    assert DEFAULT_REMOTE_ROOT + "/_private/deployment-lock" not in (
        production_sftp.directories
    )


def test_explicit_rollback_restores_bytes_modes_and_removes_new_files(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    old_files = dict(production_sftp.files)
    old_modes = dict(production_sftp.modes)
    deploy_http = HttpRouter(production_sftp, code_revision=RELEASE_SHA)
    deploy_release(
        production_sftp,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=deploy_http,
    )
    rollback_http = HttpRouter(production_sftp, code_revision=RELEASE_SHA)

    result = rollback_release(
        production_sftp,
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=rollback_http,
    )

    assert result["ok"] is True
    assert result["removed_new_files"] > 0
    for relative_path in DEFAULT_COMMIT_ORDER:
        target = DEFAULT_REMOTE_ROOT + "/" + relative_path
        if target in old_files:
            assert production_sftp.files[target] == old_files[target]
            assert production_sftp.modes[target] == old_modes[target]
        else:
            assert target not in production_sftp.files
    emergency_id = result["emergency_backup_release_id"]
    assert isinstance(emergency_id, str)
    emergency = load_remote_backup(
        production_sftp,
        backup_root=DEFAULT_REMOTE_ROOT + "/_private/deployment-backups",
        release_id=emergency_id,
        expected_remote_root=DEFAULT_REMOTE_ROOT,
    )
    assert all(item.existed for item in emergency.files)


def test_remote_dry_run_is_read_only(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    before_files = dict(production_sftp.files)
    before_directories = dict(production_sftp.directories)
    before_mutations = production_sftp.mutations

    report = inspect_remote_deployment(
        production_sftp,
        plan=local_plan,  # type: ignore[arg-type]
        remote_root=DEFAULT_REMOTE_ROOT,
    )

    assert report["operation"] == "deploy-dry-run"
    assert report["mutated_remote"] is False
    assert production_sftp.files == before_files
    assert production_sftp.directories == before_directories
    assert production_sftp.mutations == before_mutations


def test_rollback_dry_run_does_not_acquire_lock(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    http = HttpRouter(production_sftp, code_revision=RELEASE_SHA)
    deploy_release(
        production_sftp,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=http,
    )
    before_mutations = production_sftp.mutations

    report = rollback_release(
        production_sftp,
        release_id=RELEASE_ID,
        public_url_root="",
        api_v2_base_url="",
        rollback_health_url="",
        protected_token="",
        dry_run=True,
    )

    assert report["operation"] == "rollback-dry-run"
    assert report["mutated_remote"] is False
    assert production_sftp.mutations == before_mutations


def test_opcache_probe_uses_header_secret_and_is_always_deleted(
    production_sftp: MemorySftp,
) -> None:
    http = HttpRouter(
        production_sftp,
        code_revision=RELEASE_SHA,
        fail_probe=True,
    )

    with pytest.raises(PhpDeploymentError, match="did not return HTTP 200"):
        reset_opcache_with_ephemeral_probe(
            production_sftp,
            remote_root=DEFAULT_REMOTE_ROOT,
            public_url_root=PUBLIC_ROOT,
            http_request=http,
        )

    assert len(http.probe_tokens) == 1
    assert len(http.probe_tokens[0]) >= 48
    assert not any(".bside-opcache-" in path for path in production_sftp.files)


def test_opcache_probe_is_deleted_when_upload_verification_fails(
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_chmod = production_sftp.chmod

    def fail_probe_chmod(path: str, mode: int) -> None:
        original_chmod(path, mode)
        if "/.bside-opcache-" in path:
            raise OSError(5, "sensitive remote failure detail")

    monkeypatch.setattr(production_sftp, "chmod", fail_probe_chmod)

    with pytest.raises(PhpDeploymentError, match="remote file upload failed"):
        reset_opcache_with_ephemeral_probe(
            production_sftp,
            remote_root=DEFAULT_REMOTE_ROOT,
            public_url_root=PUBLIC_ROOT,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert not any(".bside-opcache-" in path for path in production_sftp.files)


def test_remote_symlink_target_is_rejected(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    target = DEFAULT_REMOTE_ROOT + "/governance_v2.php"
    production_sftp.symlinks.add(target)

    with pytest.raises(PhpDeploymentError, match="regular file"):
        inspect_remote_deployment(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            remote_root=DEFAULT_REMOTE_ROOT,
        )


def _ssh_args(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "ssh_host": "ssh.example",
        "ssh_port": "22",
        "ssh_user": "deploy",
        "ssh_password_env": "DEPLOY_PASSWORD",
        "ssh_host_key_sha256": "SHA256:" + "A" * 43,
        "ssh_allow_legacy_rsa_sha1": False,
        "ssh_legacy_rsa_sha1_host": None,
        "connect_timeout": 15,
        "auth_timeout": 15,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _gabia_options() -> object:
    return ssh_sftp_options_from_args(
        _ssh_args(
            ssh_host=GABIA_COMPATIBILITY_SSH_HOST,
            ssh_host_key_sha256=GABIA_SSH_HOST_KEY_SHA256,
            ssh_allow_legacy_rsa_sha1=True,
            ssh_legacy_rsa_sha1_host=GABIA_COMPATIBILITY_SSH_HOST,
        ),
        environ={"DEPLOY_PASSWORD": "never-print-this-password"},
    )


def _gabia_sftp() -> GabiaMemorySftp:
    client = GabiaMemorySftp()
    client.add_directory("/www_root")
    client.add_directory(DEFAULT_REMOTE_ROOT)
    client.add_directory(DEFAULT_REMOTE_ROOT + "/_private", 0o700)
    client.add_directory(DEFAULT_REMOTE_ROOT + "/migrations")
    client.add_file(
        DEFAULT_REMOTE_ROOT + "/_private/.htaccess",
        b"Gabia-current-private-deny-policy\n",
        0o644,
    )
    client.add_file(DEFAULT_REMOTE_ROOT + "/.htaccess", b"old htaccess\n", 0o640)
    client.add_file(DEFAULT_REMOTE_ROOT + "/api.php", b"old api\n", 0o640)
    client.add_file(
        DEFAULT_REMOTE_ROOT + "/governance_v1.php",
        b"old v1\n",
        0o644,
    )
    client.add_file(
        DEFAULT_REMOTE_ROOT + "/openapi.yaml",
        b"old openapi\n",
        0o644,
    )
    client.mutations = 0
    client.mkdir_calls.clear()
    return client


def _copy_sftp_state(
    source: MemorySftp,
    destination: MemorySftp,
) -> None:
    destination.files = dict(source.files)
    destination.modes = dict(source.modes)
    destination.directories = dict(source.directories)
    destination.mtimes = dict(source.mtimes)
    destination.inodes = dict(source.inodes)
    destination.next_inode = source.next_inode
    destination.symlinks = set(source.symlinks)
    destination.mutations = 0
    destination.mkdir_calls.clear()


def test_production_release_requires_argument_and_environment_confirmation() -> None:
    with pytest.raises(PhpDeploymentError, match="does not match"):
        confirm_production_release(
            RELEASE_SHA,
            RELEASE_SHA,
            environ={},
        )
    with pytest.raises(PhpDeploymentError, match="does not match"):
        confirm_production_release(
            RELEASE_SHA,
            "b" * 40,
            environ={CORE_RELEASE_CONFIRMATION_ENV: RELEASE_SHA},
        )

    confirm_production_release(
        RELEASE_SHA,
        RELEASE_SHA,
        environ={CORE_RELEASE_CONFIRMATION_ENV: RELEASE_SHA},
    )


def test_production_rollback_requires_backup_and_current_sha_confirmations() -> None:
    environment = {
        CORE_ROLLBACK_RELEASE_ID_ENV: RELEASE_ID,
        CORE_ROLLBACK_CURRENT_SHA_ENV: RELEASE_SHA,
    }
    confirm_production_rollback(
        RELEASE_ID,
        RELEASE_ID,
        RELEASE_SHA,
        RELEASE_SHA,
        environ=environment,
    )

    for bad_environment, release_confirmation, sha_confirmation in (
        ({}, RELEASE_ID, RELEASE_SHA),
        (environment, "other-release-id", RELEASE_SHA),
        (environment, RELEASE_ID, "b" * 40),
        (
            {
                **environment,
                CORE_ROLLBACK_RELEASE_ID_ENV: "other-release-id",
            },
            RELEASE_ID,
            RELEASE_SHA,
        ),
        (
            {
                **environment,
                CORE_ROLLBACK_CURRENT_SHA_ENV: "b" * 40,
            },
            RELEASE_ID,
            RELEASE_SHA,
        ),
    ):
        with pytest.raises(PhpDeploymentError, match="does not match"):
            confirm_production_rollback(
                RELEASE_ID,
                release_confirmation,
                RELEASE_SHA,
                sha_confirmation,
                environ=bad_environment,
            )


def test_production_checkout_requires_repository_wide_clean_state(
    local_plan: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = local_plan
    repository = getattr(plan, "local_root").parents[1]
    calls: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        php_deploy,
        "_find_repository_root",
        lambda _path: repository,
    )

    def git_output(_repository: Path, arguments: object) -> bytes:
        assert isinstance(arguments, tuple)
        calls.append(arguments)
        if arguments == ("rev-parse", "HEAD"):
            return (RELEASE_SHA + "\n").encode()
        if arguments == (
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ):
            return b" M README.md\n"
        raise AssertionError(arguments)

    monkeypatch.setattr(php_deploy, "_git_output", git_output)

    with pytest.raises(PhpDeploymentError, match="is not clean"):
        php_deploy.verify_release_checkout(
            plan,
            require_repository_clean=True,
        )

    assert calls[-1] == (
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"compatibility_host": "other.example"}, "target does not match"),
        ({"remote_root": "/www_root/other"}, "target does not match"),
        (
            {"public_url_root": "https://alignpe.gabia.io/other"},
            "target does not match",
        ),
        (
            {
                "api_v2_base_url": (
                    "https://alignpe.gabia.io/other/api.php/api/v2"
                )
            },
            "target does not match",
        ),
        (
            {
                "rollback_health_url": (
                    "https://alignpe.gabia.io/other/api.php?action=health"
                )
            },
            "target does not match",
        ),
        (
            {"ssh_host_key_sha256": "SHA256:" + "A" * 43},
            "security policy does not match",
        ),
    ],
)
def test_gabia_compatibility_is_exact_target_pinned(
    override: dict[str, str],
    message: str,
) -> None:
    client = _gabia_sftp()
    options = _gabia_options()
    values = {
        "ssh_options": options,
        "compatibility_host": GABIA_COMPATIBILITY_SSH_HOST,
        "remote_root": DEFAULT_REMOTE_ROOT,
        "public_url_root": PUBLIC_ROOT,
        "api_v2_base_url": API_V2,
        "rollback_health_url": ROLLBACK_HEALTH,
    }
    if "ssh_host_key_sha256" in override:
        values["ssh_options"] = SimpleNamespace(
            **{
                **vars(options),
                "host_key_sha256": override["ssh_host_key_sha256"],
            }
        )
    else:
        values.update(override)

    with pytest.raises(PhpDeploymentError, match=message):
        prepare_gabia_core_compatibility(
            client,
            **values,  # type: ignore[arg-type]
        )

    assert client.mutations == 0


def test_gabia_core_deploy_uses_probed_exclusive_fallback_and_strict_opcache(
    local_plan: object,
) -> None:
    client = _gabia_sftp()
    private_policy = client.files[
        DEFAULT_REMOTE_ROOT + "/_private/.htaccess"
    ]
    compatibility = prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
    )
    http = HttpRouter(
        client,
        code_revision=RELEASE_SHA,
        private_canary_mode="gabia-redirect",
        strict_opcache_action="disabled_verified",
    )

    result = deploy_release(
        client,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=http,
        gabia_compatibility=compatibility,
    )

    assert result["opcache_action"] == "disabled_verified"
    assert result["opcache_reset"] is False
    assert client.files[
        DEFAULT_REMOTE_ROOT + "/_private/.htaccess"
    ] == private_policy
    assert client.mkdir_calls.count(
        DEFAULT_REMOTE_ROOT + "/_private/deployment-lock"
    ) == 1
    assert not any(
        ".bside-exclusive-" in path or ".bside-opcache-" in path
        for path in (*client.files, *client.directories)
    )


def test_gabia_deploy_finally_rejects_private_policy_drift(
    local_plan: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _gabia_sftp()
    compatibility = prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
    )
    original_release = php_deploy._release_deployment_lock

    def release_then_drift(
        locked_client: MemorySftp,
        lock_path: str,
        **kwargs: object,
    ) -> None:
        original_release(
            locked_client,
            lock_path,
            **kwargs,  # type: ignore[arg-type]
        )
        policy_path = DEFAULT_REMOTE_ROOT + "/_private/.htaccess"
        locked_client.files[policy_path] = b"unexpected-policy-drift\n"

    monkeypatch.setattr(
        php_deploy,
        "_release_deployment_lock",
        release_then_drift,
    )

    with pytest.raises(PhpDeploymentError, match="was not preserved"):
        deploy_release(
            client,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                client,
                code_revision=RELEASE_SHA,
                private_canary_mode="gabia-redirect",
                strict_opcache_action="disabled_verified",
            ),
            gabia_compatibility=compatibility,
        )

    assert client.files[
        DEFAULT_REMOTE_ROOT + "/deployment-manifest.json"
    ] == _artifact_bytes(local_plan, "deployment-manifest.json")


def test_gabia_prepare_rejects_current_sha_mismatch_before_probe(
    local_plan: object,
) -> None:
    client = _gabia_sftp()
    first_compatibility = prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
    )
    deploy_release(
        client,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=HttpRouter(
            client,
            code_revision=RELEASE_SHA,
            private_canary_mode="gabia-redirect",
            strict_opcache_action="disabled_verified",
        ),
        gabia_compatibility=first_compatibility,
    )
    mutations_before = client.mutations

    with pytest.raises(PhpDeploymentError, match="does not match"):
        prepare_gabia_core_compatibility(
            client,
            ssh_options=_gabia_options(),  # type: ignore[arg-type]
            compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
            remote_root=DEFAULT_REMOTE_ROOT,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            expected_current_sha="b" * 40,
        )

    assert client.mutations == mutations_before


def test_gabia_rollback_finally_rejects_private_policy_drift(
    local_plan: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _gabia_sftp()
    deploy_compatibility = prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
    )
    deploy_release(
        client,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=HttpRouter(
            client,
            code_revision=RELEASE_SHA,
            private_canary_mode="gabia-redirect",
            strict_opcache_action="disabled_verified",
        ),
        gabia_compatibility=deploy_compatibility,
    )
    rollback_compatibility = prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        expected_current_sha=RELEASE_SHA,
    )
    original_release = php_deploy._release_deployment_lock

    def release_then_drift(
        locked_client: MemorySftp,
        lock_path: str,
        **kwargs: object,
    ) -> None:
        original_release(
            locked_client,
            lock_path,
            **kwargs,  # type: ignore[arg-type]
        )
        policy_path = DEFAULT_REMOTE_ROOT + "/_private/.htaccess"
        locked_client.modes[policy_path] = 0o600

    monkeypatch.setattr(
        php_deploy,
        "_release_deployment_lock",
        release_then_drift,
    )

    with pytest.raises(PhpDeploymentError, match="was not preserved"):
        rollback_release(
            client,
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                client,
                code_revision=RELEASE_SHA,
                private_canary_mode="gabia-redirect",
                strict_opcache_action="disabled_verified",
            ),
            gabia_compatibility=rollback_compatibility,
            expected_current_sha=RELEASE_SHA,
        )

    assert DEFAULT_REMOTE_ROOT + "/deployment-manifest.json" not in client.files


def test_gabia_rollback_rechecks_current_release_immediately_before_restore(
    local_plan: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _gabia_sftp()
    deploy_compatibility = prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
    )
    deploy_release(
        client,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=HttpRouter(
            client,
            code_revision=RELEASE_SHA,
            private_canary_mode="gabia-redirect",
            strict_opcache_action="disabled_verified",
        ),
        gabia_compatibility=deploy_compatibility,
    )
    rollback_compatibility = prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        expected_current_sha=RELEASE_SHA,
    )
    manifest_path = DEFAULT_REMOTE_ROOT + "/deployment-manifest.json"
    alternate_sha = "b" * 40
    alternate_manifest = json.loads(client.files[manifest_path])
    alternate_manifest["code_revision"] = alternate_sha
    alternate_manifest_bytes = json.dumps(
        alternate_manifest,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    deployed_api = client.files[DEFAULT_REMOTE_ROOT + "/api.php"]
    original_verify_snapshot = (
        php_deploy.verify_remote_targets_match_snapshot
    )
    snapshot_checks = 0

    def verify_then_switch_release(
        checked_client: MemorySftp,
        *,
        snapshot: object,
    ) -> None:
        nonlocal snapshot_checks
        original_verify_snapshot(
            checked_client,
            snapshot=snapshot,  # type: ignore[arg-type]
        )
        snapshot_checks += 1
        if snapshot_checks == 1:
            checked_client.files[manifest_path] = alternate_manifest_bytes

    monkeypatch.setattr(
        php_deploy,
        "verify_remote_targets_match_snapshot",
        verify_then_switch_release,
    )

    with pytest.raises(
        PhpDeploymentError,
        match="rollback stopped before mutation",
    ):
        rollback_release(
            client,
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                client,
                code_revision=RELEASE_SHA,
                private_canary_mode="gabia-redirect",
                strict_opcache_action="disabled_verified",
            ),
            gabia_compatibility=rollback_compatibility,
            expected_current_sha=RELEASE_SHA,
        )

    assert snapshot_checks == 1
    assert client.files[manifest_path] == alternate_manifest_bytes
    assert client.files[DEFAULT_REMOTE_ROOT + "/api.php"] == deployed_api


def test_gabia_probe_rejects_replacing_standard_rename_without_residue() -> None:
    baseline = _gabia_sftp()
    client = ReplacingRenameGabiaSftp()
    _copy_sftp_state(baseline, client)

    with pytest.raises(PhpDeploymentError, match="can replace"):
        prepare_gabia_core_compatibility(
            client,
            ssh_options=_gabia_options(),  # type: ignore[arg-type]
            compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
            remote_root=DEFAULT_REMOTE_ROOT,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
        )

    assert not any(
        ".bside-exclusive-" in path
        or ".bside-cross-dir-" in path
        for path in (*client.files, *client.directories)
    )


def test_gabia_strict_opcache_rejects_ambiguous_disabled_evidence(
) -> None:
    client = _gabia_sftp()
    compatibility = prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
    )
    http = HttpRouter(
        client,
        code_revision=RELEASE_SHA,
        private_canary_mode="gabia-redirect",
        strict_opcache_action="ambiguous",
    )

    with pytest.raises(PhpDeploymentError, match="action is invalid"):
        reset_opcache_with_ephemeral_probe(
            client,
            remote_root=DEFAULT_REMOTE_ROOT,
            public_url_root=PUBLIC_ROOT,
            http_request=http,
            exclusive_writer=compatibility.exclusive_writer,
            require_strict_state=True,
        )

    assert not any(
        ".bside-exclusive-" in path or ".bside-opcache-" in path
        for path in (*client.files, *client.directories)
    )


def test_ssh_options_require_explicit_exact_legacy_host() -> None:
    environment = {"DEPLOY_PASSWORD": "never-print-this-password"}

    with pytest.raises(PhpDeploymentError, match="legacy ssh-rsa"):
        ssh_sftp_options_from_args(
            _ssh_args(ssh_allow_legacy_rsa_sha1=True),
            environ=environment,
        )
    with pytest.raises(PhpDeploymentError, match="legacy ssh-rsa"):
        ssh_sftp_options_from_args(
            _ssh_args(
                ssh_allow_legacy_rsa_sha1=True,
                ssh_legacy_rsa_sha1_host="other.example",
            ),
            environ=environment,
        )

    options = ssh_sftp_options_from_args(
        _ssh_args(
            ssh_allow_legacy_rsa_sha1=True,
            ssh_legacy_rsa_sha1_host="ssh.example",
        ),
        environ=environment,
    )

    assert legacy_ssh_rsa_sha1_is_allowed(options) is True
    assert "never-print-this-password" not in repr(options)


def test_tampered_backup_blob_is_rejected(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    http = HttpRouter(production_sftp, code_revision=RELEASE_SHA)
    deploy_release(
        production_sftp,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=http,
    )
    backup_directory = (
        DEFAULT_REMOTE_ROOT + "/_private/deployment-backups/" + RELEASE_ID
    )
    manifest = json.loads(
        production_sftp.files[backup_directory + "/backup-manifest.json"]
    )
    blob = manifest["files"]["api.php"]["backup_blob"]
    production_sftp.files[backup_directory + "/" + blob] = b"tampered"

    with pytest.raises(PhpDeploymentError, match="backup blob identity"):
        load_remote_backup(
            production_sftp,
            backup_root=DEFAULT_REMOTE_ROOT + "/_private/deployment-backups",
            release_id=RELEASE_ID,
            expected_remote_root=DEFAULT_REMOTE_ROOT,
        )


def test_incomplete_backup_is_removed_when_blob_upload_fails(
    local_plan: object,
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup_root = DEFAULT_REMOTE_ROOT + "/_private/deployment-backups"
    release_id = "backup-failure-20260725t000000z-12345678"
    backup_directory = backup_root + "/" + release_id
    original_chmod = production_sftp.chmod

    def fail_backup_blob_chmod(path: str, mode: int) -> None:
        original_chmod(path, mode)
        if path.startswith(backup_directory + "/") and path.endswith(".blob"):
            raise OSError(5, "backup blob chmod failed")

    monkeypatch.setattr(
        production_sftp,
        "chmod",
        fail_backup_blob_chmod,
    )

    with pytest.raises(PhpDeploymentError, match="remote file upload failed"):
        php_deploy.capture_remote_backup(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            remote_root=DEFAULT_REMOTE_ROOT,
            backup_root=backup_root,
            release_id=release_id,
        )

    assert backup_directory not in production_sftp.directories
    assert not any(
        path.startswith(backup_directory + "/")
        for path in production_sftp.files
    )


def test_incomplete_backup_cleanup_failure_is_reported_without_blobs(
    local_plan: object,
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup_root = DEFAULT_REMOTE_ROOT + "/_private/deployment-backups"
    release_id = "backup-cleanup-failure-20260725t000000z-12345678"
    backup_directory = backup_root + "/" + release_id
    original_chmod = production_sftp.chmod
    original_rmdir = production_sftp.rmdir

    def fail_backup_blob_chmod(path: str, mode: int) -> None:
        original_chmod(path, mode)
        if path.startswith(backup_directory + "/") and path.endswith(".blob"):
            raise OSError(5, "backup blob chmod failed")

    def fail_backup_directory_rmdir(path: str) -> None:
        if path == backup_directory:
            raise OSError(5, "backup directory rmdir failed")
        original_rmdir(path)

    monkeypatch.setattr(
        production_sftp,
        "chmod",
        fail_backup_blob_chmod,
    )
    monkeypatch.setattr(
        production_sftp,
        "rmdir",
        fail_backup_directory_rmdir,
    )

    with pytest.raises(
        PhpDeploymentError,
        match="incomplete backup cleanup did not complete",
    ):
        php_deploy.capture_remote_backup(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            remote_root=DEFAULT_REMOTE_ROOT,
            backup_root=backup_root,
            release_id=release_id,
        )

    assert backup_directory in production_sftp.directories
    assert not any(
        path.startswith(backup_directory + "/")
        for path in production_sftp.files
    )


@pytest.mark.parametrize(
    ("router_options", "expected_error"),
    [
        (
            {"strip_authorization": True},
            "authenticated protected route did not return HTTP 200",
        ),
        (
            {"protected_release_state": "preview"},
            "authenticated protected route is not in the closed state",
        ),
    ],
)
def test_closed_smoke_requires_forwarded_bearer_and_closed_protected_state(
    production_sftp: MemorySftp,
    router_options: dict[str, object],
    expected_error: str,
) -> None:
    http = HttpRouter(
        production_sftp,
        code_revision=RELEASE_SHA,
        **router_options,
    )

    with pytest.raises(PhpDeploymentError, match=expected_error):
        verify_closed_v2_api(
            base_url=API_V2,
            expected_sha=RELEASE_SHA,
            protected_token=PROTECTED_TOKEN,
            http_request=http,
        )


def test_closed_smoke_supports_attested_schema_11_release(
    production_sftp: MemorySftp,
) -> None:
    verify_closed_v2_api(
        base_url=API_V2,
        expected_sha=RELEASE_SHA,
        protected_token=PROTECTED_TOKEN,
        http_request=HttpRouter(
            production_sftp,
            code_revision=RELEASE_SHA,
            schema_version=11,
        ),
        expected_schema_version=11,
    )


def test_pending_schema_smoke_requires_exact_12_over_11_mismatch(
    production_sftp: MemorySftp,
) -> None:
    verify_closed_v2_api(
        base_url=API_V2,
        expected_sha=RELEASE_SHA,
        protected_token=PROTECTED_TOKEN,
        http_request=HttpRouter(
            production_sftp,
            code_revision=RELEASE_SHA,
            schema_version=12,
            pending_actual_schema_version=11,
        ),
        expected_schema_version=12,
        pending_actual_schema_version=11,
    )

    with pytest.raises(
        PhpDeploymentError,
        match="did not prove the pending schema upgrade",
    ):
        verify_closed_v2_api(
            base_url=API_V2,
            expected_sha=RELEASE_SHA,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
                schema_version=12,
                pending_actual_schema_version=10,
            ),
            expected_schema_version=12,
            pending_actual_schema_version=11,
        )


def test_schema_upgrade_bridge_requires_attested_existing_release(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    before_files = dict(production_sftp.files)
    before_directories = dict(production_sftp.directories)
    before_mutations = production_sftp.mutations

    with pytest.raises(
        PhpDeploymentError,
        match="requires an attested existing release",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
            schema_upgrade_from=11,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
        )

    assert production_sftp.files == before_files
    assert production_sftp.directories == before_directories
    assert production_sftp.mutations == before_mutations


def test_schema_upgrade_bridge_rejects_non_c06_schema11_predecessor(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    _install_attested_release(
        production_sftp,
        code_revision=LEGACY_RELEASE_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    )
    before_files = dict(production_sftp.files)
    before_mutations = production_sftp.mutations

    with pytest.raises(
        PhpDeploymentError,
        match="predecessor is not the exact c06 release",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=LEGACY_RELEASE_SHA,
                schema_version=11,
            ),
            schema_upgrade_from=11,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
        )

    assert production_sftp.files == before_files
    assert production_sftp.mutations == before_mutations


def test_schema_upgrade_bridge_requires_journal_before_remote_mutation(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    _install_attested_release(
        production_sftp,
        code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
        file_overrides={
            "migrations/011_global_terminal_v2.sql": _artifact_bytes(
                local_plan,
                "migrations/011_global_terminal_v2.sql",
            )
        },
    )
    before_files = dict(production_sftp.files)
    before_directories = dict(production_sftp.directories)
    before_mutations = production_sftp.mutations

    with pytest.raises(
        PhpDeploymentError,
        match="durable bridge journal",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=SchemaBridgeHttpRouter(
                production_sftp,
                previous_release_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            ),
            schema_upgrade_from=11,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=None,
        )

    assert production_sftp.files == before_files
    assert production_sftp.directories == before_directories
    assert production_sftp.mutations == before_mutations


def test_schema_upgrade_bridge_verifies_old_then_pending_candidate(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    _install_attested_release(
        production_sftp,
        code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
        file_overrides={
            "migrations/011_global_terminal_v2.sql": _artifact_bytes(
                local_plan,
                "migrations/011_global_terminal_v2.sql",
            )
        },
    )
    result = deploy_release(
        production_sftp,
        plan=local_plan,  # type: ignore[arg-type]
        release_id="php-v2-schema-bridge-20260726t000000z-12345678",
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=SchemaBridgeHttpRouter(
            production_sftp,
            previous_release_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        ),
        schema_upgrade_from=11,
        expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        dart_disabled_evidence=DART_DISABLED_EVIDENCE,
        bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
    )

    manifest = json.loads(
        production_sftp.files[
            DEFAULT_REMOTE_ROOT + "/" + DEPLOYMENT_MANIFEST_NAME
        ]
    )
    backup = json.loads(
        production_sftp.files[str(result["backup_manifest"])]
    )
    assert set(manifest["files"]) == set(CORE_API_FILES)
    assert manifest["code_revision"] == RELEASE_SHA
    assert (
        DEFAULT_REMOTE_ROOT + "/migrations/012_dart_credential_pool.sql"
        in production_sftp.files
    )
    assert (
        backup["files"]["migrations/012_dart_credential_pool.sql"]["existed"]
        is False
    )
    assert result["closed_smoke"] is False
    assert result["fail_closed_smoke"] is True
    assert result["deployment_smoke_mode"] == (
        "pending_schema_upgrade_11_to_12"
    )
    assert result["schema_upgrade_from"] == 11


def test_schema_11_manifest_is_rejected_without_explicit_bridge(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    _install_attested_release(
        production_sftp,
        code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    )
    before_files = dict(production_sftp.files)
    before_directories = dict(production_sftp.directories)
    before_mutations = production_sftp.mutations

    with pytest.raises(
        PhpDeploymentError,
        match="existing deployment manifest identity is invalid",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
                schema_version=11,
            ),
        )

    assert production_sftp.files == before_files
    assert production_sftp.directories == before_directories
    assert production_sftp.mutations == before_mutations


def test_schema_11_bridge_rejects_changed_migration_011_before_mutation(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    _install_attested_release(
        production_sftp,
        code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    )
    before_files = dict(production_sftp.files)
    before_directories = dict(production_sftp.directories)
    before_mutations = production_sftp.mutations

    with pytest.raises(
        PhpDeploymentError,
        match="requires unchanged migration 011 bytes",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
                schema_version=11,
            ),
            schema_upgrade_from=11,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
        )

    assert production_sftp.files == before_files
    assert production_sftp.directories == before_directories
    assert production_sftp.mutations == before_mutations


def test_schema_11_bridge_rejects_stray_migration_012_before_mutation(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    _install_attested_release(
        production_sftp,
        code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
        file_overrides={
            "migrations/011_global_terminal_v2.sql": _artifact_bytes(
                local_plan,
                "migrations/011_global_terminal_v2.sql",
            )
        },
    )
    stray_path = (
        DEFAULT_REMOTE_ROOT + "/migrations/012_dart_credential_pool.sql"
    )
    production_sftp.files[stray_path] = b"partial previous deployment\n"
    production_sftp.modes[stray_path] = 0o644
    before_files = dict(production_sftp.files)
    before_directories = dict(production_sftp.directories)
    before_mutations = production_sftp.mutations

    with pytest.raises(
        PhpDeploymentError,
        match="stray migration 012",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
                schema_version=11,
            ),
            schema_upgrade_from=11,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
        )

    assert production_sftp.files == before_files
    assert production_sftp.directories == before_directories
    assert production_sftp.mutations == before_mutations


@pytest.mark.parametrize("drift", ["missing", "extra", "tampered"])
def test_schema_11_bridge_rejects_manifest_or_byte_drift(
    production_sftp: MemorySftp,
    drift: str,
) -> None:
    _install_attested_release(
        production_sftp,
        code_revision=LEGACY_RELEASE_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    )
    manifest_path = DEFAULT_REMOTE_ROOT + "/" + DEPLOYMENT_MANIFEST_NAME
    manifest = json.loads(production_sftp.files[manifest_path])
    if drift == "missing":
        del manifest["files"]["openapi-v2.yaml"]
        production_sftp.files[manifest_path] = json.dumps(manifest).encode()
    elif drift == "extra":
        manifest["files"]["unexpected.php"] = "0" * 64
        production_sftp.files[manifest_path] = json.dumps(manifest).encode()
    else:
        production_sftp.files[
            DEFAULT_REMOTE_ROOT + "/openapi-v2.yaml"
        ] += b"tampered"
    before_mutations = production_sftp.mutations

    with pytest.raises(
        PhpDeploymentError,
        match=(
            "existing deployment manifest identity is invalid"
            if drift != "tampered"
            else "existing deployment bytes do not match the manifest"
        ),
    ):
        verify_existing_remote_release_identity(
            production_sftp,
            remote_root=DEFAULT_REMOTE_ROOT,
            expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
        )

    assert production_sftp.mutations == before_mutations


def test_deploy_parser_exposes_only_explicit_schema_11_bridge() -> None:
    args = php_deploy.build_arg_parser().parse_args(
        [
            "deploy",
            "--local-root",
            "deploy/activist",
            "--expected-sha",
            RELEASE_SHA,
            "--schema-upgrade-from",
            "11",
        ]
    )
    assert args.schema_upgrade_from == 11
    with pytest.raises(SystemExit):
        php_deploy.build_arg_parser().parse_args(
            [
                "deploy",
                "--expected-sha",
                RELEASE_SHA,
                "--schema-upgrade-from",
                "10",
            ]
        )


@pytest.mark.parametrize(
    ("schema_upgrade_from", "expected_core_files"),
    [
        (11, LEGACY_SCHEMA_11_CORE_API_FILES),
        (None, CORE_API_FILES),
    ],
)
def test_cli_gabia_preflight_uses_exact_manifest_shape_for_bridge(
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
    schema_upgrade_from: int | None,
    expected_core_files: tuple[str, ...],
) -> None:
    captured: dict[str, object] = {}
    sentinel = object()

    def capture_prepare(
        _client: MemorySftp,
        **kwargs: object,
    ) -> object:
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(
        php_deploy,
        "prepare_gabia_core_compatibility",
        capture_prepare,
    )
    args = argparse.Namespace(
        gabia_core_compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        schema_upgrade_from=schema_upgrade_from,
    )

    result = php_deploy._prepare_cli_gabia_compatibility(
        production_sftp,
        args=args,
        options=_gabia_options(),  # type: ignore[arg-type]
    )

    assert result is sentinel
    assert captured["expected_core_files"] == expected_core_files


@pytest.mark.parametrize(
    ("api_url", "rollback_url", "expected_error"),
    [
        (
            "https://other.example/activist/api.php/api/v2",
            ROLLBACK_HEALTH,
            "public URL origin",
        ),
        (
            "https://alignpe.gabia.io/other/api.php/api/v2",
            ROLLBACK_HEALTH,
            "escapes the public URL path",
        ),
        (
            API_V2,
            "https://other.example/activist/api.php?action=health",
            "public URL origin",
        ),
    ],
)
def test_deploy_rejects_unbound_http_endpoints_before_remote_mutation(
    local_plan: object,
    production_sftp: MemorySftp,
    api_url: str,
    rollback_url: str,
    expected_error: str,
) -> None:
    before_files = dict(production_sftp.files)
    before_directories = dict(production_sftp.directories)
    before_mutations = production_sftp.mutations

    with pytest.raises(PhpDeploymentError, match=expected_error):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=api_url,
            rollback_health_url=rollback_url,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert production_sftp.files == before_files
    assert production_sftp.directories == before_directories
    assert production_sftp.mutations == before_mutations


@pytest.mark.parametrize("token", ["too-short", "x" * 31, "x" * 31 + "\n"])
def test_deploy_rejects_invalid_protected_token_before_remote_mutation(
    local_plan: object,
    production_sftp: MemorySftp,
    token: str,
) -> None:
    before_files = dict(production_sftp.files)
    before_directories = dict(production_sftp.directories)
    before_mutations = production_sftp.mutations

    with pytest.raises(PhpDeploymentError, match="protected Bearer token"):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=token,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert production_sftp.files == before_files
    assert production_sftp.directories == before_directories
    assert production_sftp.mutations == before_mutations


def test_deploy_detects_non_cooperating_writer_before_first_commit(
    local_plan: object,
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_capture = php_deploy.capture_remote_backup
    original_targets = {
        path: content
        for path, content in production_sftp.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    }
    external_content = b"external-writer-update\n"

    def capture_then_mutate(*args: object, **kwargs: object) -> object:
        snapshot = original_capture(*args, **kwargs)  # type: ignore[arg-type]
        production_sftp.files[DEFAULT_REMOTE_ROOT + "/api.php"] = external_content
        return snapshot

    monkeypatch.setattr(
        php_deploy,
        "capture_remote_backup",
        capture_then_mutate,
    )

    with pytest.raises(PhpDeploymentError, match="stopped before commit"):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert production_sftp.files[DEFAULT_REMOTE_ROOT + "/api.php"] == external_content
    for path, content in original_targets.items():
        if path != DEFAULT_REMOTE_ROOT + "/api.php":
            assert production_sftp.files[path] == content
    assert DEFAULT_REMOTE_ROOT + "/deployment-manifest.json" not in (
        production_sftp.files
    )
    assert DEFAULT_REMOTE_ROOT + "/governance_v2.php" not in production_sftp.files


def test_session_wraps_unexpected_transport_errors_without_sensitive_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "sensitive-password-user-host-endpoint"
    options = ssh_sftp_options_from_args(
        _ssh_args(),
        environ={"DEPLOY_PASSWORD": secret},
    )
    monkeypatch.setitem(sys.modules, "paramiko", SimpleNamespace())

    def fail_connection(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError(secret)

    monkeypatch.setattr(
        php_deploy.socket,
        "create_connection",
        fail_connection,
    )

    with pytest.raises(
        PhpDeploymentError,
        match="SSH/SFTP session establishment failed",
    ) as caught:
        ParamikoPinnedSftpSession(options).__enter__()

    assert secret not in str(caught.value)
    assert secret not in repr(caught.value)


def test_cli_unexpected_error_prints_only_generic_safe_line(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sensitive_values = (
        "password-do-not-print",
        "token-do-not-print",
        "https://secret-endpoint.invalid/private",
    )

    def fail_options(_args: argparse.Namespace) -> object:
        raise RuntimeError(" ".join(sensitive_values))

    monkeypatch.setattr(
        php_deploy,
        "ssh_sftp_options_from_args",
        fail_options,
    )

    exit_code = main(
        [
            "rollback",
            "--dry-run",
            "--release-id",
            RELEASE_ID,
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == (
        "PHP SFTP operation failed safely; inspect protected diagnostics.\n"
    )
    for value in sensitive_values:
        assert value not in captured.out
        assert value not in captured.err


@pytest.mark.parametrize(
    "arguments",
    [
        [
            "deploy",
            "--dry-run",
            "--expected-sha",
            RELEASE_SHA,
            "--gabia-core-compatibility-host",
            GABIA_COMPATIBILITY_SSH_HOST,
        ],
        [
            "rollback",
            "--dry-run",
            "--release-id",
            RELEASE_ID,
            "--gabia-core-compatibility-host",
            GABIA_COMPATIBILITY_SSH_HOST,
        ],
    ],
)
def test_cli_rejects_gabia_compatibility_dry_run_before_sftp(
    arguments: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_options(_args: argparse.Namespace) -> object:
        raise AssertionError("SFTP options must not be read")

    monkeypatch.setattr(
        php_deploy,
        "ssh_sftp_options_from_args",
        unexpected_options,
    )

    assert main(arguments) == 1


def test_cli_rejects_unconfirmed_gabia_rollback_before_sftp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(CORE_ROLLBACK_RELEASE_ID_ENV, raising=False)
    monkeypatch.delenv(CORE_ROLLBACK_CURRENT_SHA_ENV, raising=False)

    def unexpected_options(_args: argparse.Namespace) -> object:
        raise AssertionError("SFTP options must not be read")

    monkeypatch.setattr(
        php_deploy,
        "ssh_sftp_options_from_args",
        unexpected_options,
    )

    assert main(
        [
            "rollback",
            "--release-id",
            RELEASE_ID,
            "--confirm-rollback-release-id",
            RELEASE_ID,
            "--expected-current-sha",
            RELEASE_SHA,
            "--confirm-rollback-current-sha",
            RELEASE_SHA,
            "--gabia-core-compatibility-host",
            GABIA_COMPATIBILITY_SSH_HOST,
        ]
    ) == 1


@pytest.mark.parametrize("canary_mode", ["exposed", "redirect"])
def test_private_root_canary_blocks_exposure_and_redirects_before_lock(
    local_plan: object,
    production_sftp: MemorySftp,
    canary_mode: str,
) -> None:
    http = HttpRouter(
        production_sftp,
        code_revision=RELEASE_SHA,
        private_canary_mode=canary_mode,
    )

    with pytest.raises(
        PhpDeploymentError,
        match="private root HTTP isolation canary failed",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=http,
        )

    assert production_sftp.files[
        DEFAULT_REMOTE_ROOT + "/_private/.htaccess"
    ] == php_deploy.PRIVATE_ROOT_DENY_RULES
    assert len(http.private_canary_paths) == 1
    assert http.private_canary_paths[0] not in production_sftp.files
    assert len(http.public_canary_paths) == 1
    assert http.public_canary_paths[0] not in production_sftp.files
    assert http.public_canary_modes == [0o644]
    assert DEFAULT_REMOTE_ROOT + "/_private/deployment-lock" not in (
        production_sftp.directories
    )
    assert not any("deployment-backups" in path for path in production_sftp.files)


@pytest.mark.parametrize("public_mode", ["missing", "wrong-body"])
def test_public_root_positive_control_rejects_false_private_404_success(
    local_plan: object,
    production_sftp: MemorySftp,
    public_mode: str,
) -> None:
    http = HttpRouter(
        production_sftp,
        code_revision=RELEASE_SHA,
        public_canary_mode=public_mode,
    )

    with pytest.raises(
        PhpDeploymentError,
        match="public document root HTTP mapping canary failed",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=http,
        )

    assert len(http.public_canary_paths) == 1
    assert http.public_canary_paths[0] not in production_sftp.files
    assert http.public_canary_modes == [0o644]
    assert http.private_canary_paths == []
    assert not any(
        ".bside-private-canary-" in path for path in production_sftp.files
    )
    assert DEFAULT_REMOTE_ROOT + "/_private/deployment-lock" not in (
        production_sftp.directories
    )


def test_private_root_existing_deny_policy_is_never_overwritten(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    deny_path = DEFAULT_REMOTE_ROOT + "/_private/.htaccess"
    unexpected_policy = b"Require all granted\n"
    production_sftp.add_file(deny_path, unexpected_policy, 0o644)
    production_sftp.mutations = 0

    with pytest.raises(
        PhpDeploymentError,
        match="does not match the expected policy",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert production_sftp.files[deny_path] == unexpected_policy
    assert production_sftp.mutations == 0
    assert not any(
        ".bside-private-canary-" in path for path in production_sftp.files
    )


def test_existing_v2_rechecks_sha_and_closed_state_immediately_before_commit(
    local_plan: object,
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deploy_release(
        production_sftp,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=HttpRouter(
            production_sftp,
            code_revision=RELEASE_SHA,
        ),
    )
    target_bytes = {
        relative_path: production_sftp.files[
            DEFAULT_REMOTE_ROOT + "/" + relative_path
        ]
        for relative_path in DEFAULT_COMMIT_ORDER
    }
    router = HttpRouter(production_sftp, code_revision=RELEASE_SHA)
    protected_calls = 0

    def changing_release_state(
        method: str,
        url: str,
        headers: object,
        timeout: float,
    ) -> HttpResponse:
        nonlocal protected_calls
        if url == API_V2 + "/ops/release-state":
            protected_calls += 1
            router.protected_release_state = (
                "closed" if protected_calls == 1 else "preview"
            )
        return router(method, url, headers, timeout)

    commit_called = False

    def unexpected_commit(*_args: object, **_kwargs: object) -> None:
        nonlocal commit_called
        commit_called = True

    monkeypatch.setattr(
        php_deploy,
        "_commit_staged_artifacts",
        unexpected_commit,
    )

    with pytest.raises(PhpDeploymentError, match="stopped before commit"):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id="php-v2-recheck-20260725t000000z-12345678",
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=changing_release_state,
        )

    assert protected_calls == 2
    assert commit_called is False
    for relative_path, content in target_bytes.items():
        assert production_sftp.files[
            DEFAULT_REMOTE_ROOT + "/" + relative_path
        ] == content


def test_deploy_success_with_lock_cleanup_failure_reports_applied_state(
    local_plan: object,
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_rmdir = production_sftp.rmdir

    def fail_lock_rmdir(path: str) -> None:
        if path.endswith("/deployment-lock"):
            raise OSError(5, "lock cleanup failed")
        original_rmdir(path)

    monkeypatch.setattr(production_sftp, "rmdir", fail_lock_rmdir)

    with pytest.raises(
        PhpDeploymentError,
        match="files were applied and verified.*lock cleanup failed",
    ) as caught:
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert production_sftp.files[
        DEFAULT_REMOTE_ROOT + "/api.php"
    ] == _artifact_bytes(local_plan, "api.php")
    assert production_sftp.files[
        DEFAULT_REMOTE_ROOT + "/deployment-manifest.json"
    ] == _artifact_bytes(local_plan, "deployment-manifest.json")
    assert production_sftp.files[
        DEFAULT_REMOTE_ROOT + "/_private/deployment-lock/owner.json"
    ]
    assert caught.value.__cause__ is not None
    assert "owner evidence was preserved" in str(caught.value.__cause__)


def test_lock_chmod_failure_removes_partially_created_lock(
    local_plan: object,
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_chmod = production_sftp.chmod
    lock_path = DEFAULT_REMOTE_ROOT + "/_private/deployment-lock"

    def fail_lock_chmod(path: str, mode: int) -> None:
        if path == lock_path:
            raise OSError(5, "lock chmod failed")
        original_chmod(path, mode)

    monkeypatch.setattr(production_sftp, "chmod", fail_lock_chmod)

    with pytest.raises(PhpDeploymentError, match="acquire the deployment lock"):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert lock_path not in production_sftp.directories
    assert lock_path + "/owner.json" not in production_sftp.files


def test_workspace_chmod_failure_removes_partially_created_workspace(
    local_plan: object,
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_chmod = production_sftp.chmod
    workspace = (
        DEFAULT_REMOTE_ROOT
        + "/_private/deployment-staging/"
        + RELEASE_ID
    )

    def fail_workspace_chmod(path: str, mode: int) -> None:
        if path == workspace:
            raise OSError(5, "workspace chmod failed")
        original_chmod(path, mode)

    monkeypatch.setattr(
        production_sftp,
        "chmod",
        fail_workspace_chmod,
    )

    with pytest.raises(PhpDeploymentError, match="stopped before commit"):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert workspace not in production_sftp.directories
    assert not any(path.startswith(workspace + "/") for path in production_sftp.files)


def test_partial_stage_upload_is_tracked_and_removed(
    local_plan: object,
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_chmod = production_sftp.chmod
    workspace = (
        DEFAULT_REMOTE_ROOT
        + "/_private/deployment-staging/"
        + RELEASE_ID
    )

    def fail_second_stage_blob(path: str, mode: int) -> None:
        original_chmod(path, mode)
        if posixpath.basename(path).startswith("001-"):
            raise OSError(5, "partial stage failure")

    monkeypatch.setattr(
        production_sftp,
        "chmod",
        fail_second_stage_blob,
    )

    with pytest.raises(PhpDeploymentError, match="stopped before commit"):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert workspace not in production_sftp.directories
    assert not any(path.startswith(workspace + "/") for path in production_sftp.files)


def test_rollback_success_with_lock_cleanup_failure_reports_applied_state(
    local_plan: object,
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_api = production_sftp.files[DEFAULT_REMOTE_ROOT + "/api.php"]
    deploy_release(
        production_sftp,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=HttpRouter(
            production_sftp,
            code_revision=RELEASE_SHA,
        ),
    )
    original_rmdir = production_sftp.rmdir

    def fail_lock_rmdir(path: str) -> None:
        if path.endswith("/deployment-lock"):
            raise OSError(5, "lock cleanup failed")
        original_rmdir(path)

    monkeypatch.setattr(production_sftp, "rmdir", fail_lock_rmdir)

    with pytest.raises(
        PhpDeploymentError,
        match="rollback was applied and verified.*lock cleanup failed",
    ):
        rollback_release(
            production_sftp,
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert production_sftp.files[DEFAULT_REMOTE_ROOT + "/api.php"] == old_api
    assert DEFAULT_REMOTE_ROOT + "/governance_v2.php" not in production_sftp.files
    assert production_sftp.files[
        DEFAULT_REMOTE_ROOT + "/_private/deployment-lock/owner.json"
    ]


def test_failed_rollback_uses_separate_recovery_workspace(
    local_plan: object,
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deploy_release(
        production_sftp,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=HttpRouter(
            production_sftp,
            code_revision=RELEASE_SHA,
        ),
    )
    deployed_api = production_sftp.files[DEFAULT_REMOTE_ROOT + "/api.php"]
    original_restore = php_deploy.restore_remote_backup
    workspaces: list[str] = []
    candidate_digest = getattr(local_plan, "artifact_by_path")[
        ".htaccess"
    ].sha256

    def fail_once_then_restore(
        client: MemorySftp,
        *,
        snapshot: object,
        workspace: str,
    ) -> None:
        workspaces.append(workspace)
        if len(workspaces) == 1:
            client.add_file(
                workspace + f"/restore-000-{candidate_digest}.blob",
                b"leftover restore blob",
                0o600,
            )
            raise PhpDeploymentError("simulated rollback restore failure")
        original_restore(
            client,
            snapshot=snapshot,  # type: ignore[arg-type]
            workspace=workspace,
        )

    monkeypatch.setattr(
        php_deploy,
        "restore_remote_backup",
        fail_once_then_restore,
    )

    with pytest.raises(
        PhpDeploymentError,
        match="pre-rollback files were restored",
    ):
        rollback_release(
            production_sftp,
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert len(workspaces) == 2
    assert workspaces[0] != workspaces[1]
    assert production_sftp.files[DEFAULT_REMOTE_ROOT + "/api.php"] == deployed_api


def test_rollback_rechecks_emergency_snapshot_before_target_restore(
    local_plan: object,
    production_sftp: MemorySftp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deploy_release(
        production_sftp,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=HttpRouter(
            production_sftp,
            code_revision=RELEASE_SHA,
        ),
    )
    deployed_api = production_sftp.files[DEFAULT_REMOTE_ROOT + "/api.php"]
    original_capture = php_deploy.capture_remote_backup
    original_restore = php_deploy.restore_remote_backup
    restore_release_ids: list[str] = []

    def capture_then_external_write(
        *args: object,
        **kwargs: object,
    ) -> object:
        snapshot = original_capture(*args, **kwargs)  # type: ignore[arg-type]
        release_id = getattr(snapshot, "release_id")
        if str(release_id).startswith("pre-rollback-"):
            production_sftp.files[
                DEFAULT_REMOTE_ROOT + "/api.php"
            ] = b"non-cooperating-writer\n"
        return snapshot

    def record_restore(
        client: MemorySftp,
        *,
        snapshot: object,
        workspace: str,
    ) -> None:
        restore_release_ids.append(str(getattr(snapshot, "release_id")))
        original_restore(
            client,
            snapshot=snapshot,  # type: ignore[arg-type]
            workspace=workspace,
        )

    monkeypatch.setattr(
        php_deploy,
        "capture_remote_backup",
        capture_then_external_write,
    )
    monkeypatch.setattr(
        php_deploy,
        "restore_remote_backup",
        record_restore,
    )

    with pytest.raises(
        PhpDeploymentError,
        match="rollback stopped before mutation",
    ):
        rollback_release(
            production_sftp,
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
            ),
        )

    assert restore_release_ids == []
    assert production_sftp.files[
        DEFAULT_REMOTE_ROOT + "/api.php"
    ] == b"non-cooperating-writer\n"
    assert production_sftp.files[
        DEFAULT_REMOTE_ROOT + "/api.php"
    ] != deployed_api


def test_existing_v2_requires_protected_closed_smoke_before_remote_mutation(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    deploy_release(
        production_sftp,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=HttpRouter(
            production_sftp,
            code_revision=RELEASE_SHA,
        ),
    )
    production_sftp.mutations = 0
    before_files = dict(production_sftp.files)
    before_directories = dict(production_sftp.directories)

    with pytest.raises(
        PhpDeploymentError,
        match="authenticated protected route did not return HTTP 200",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id="php-v2-existing-20260725t000000z-12345678",
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                production_sftp,
                code_revision=RELEASE_SHA,
                strip_authorization=True,
            ),
        )

    assert production_sftp.files == before_files
    assert production_sftp.directories == before_directories
    assert production_sftp.mutations == 0


def _deploy_test_schema_bridge(
    local_plan: object,
) -> tuple[GabiaMemorySftp, SchemaBridgeHttpRouter, dict[str, object]]:
    client = _gabia_sftp()
    _install_attested_release(
        client,
        code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
        file_overrides={
            "migrations/011_global_terminal_v2.sql": _artifact_bytes(
                local_plan,
                "migrations/011_global_terminal_v2.sql",
            )
        },
    )
    compatibility = prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        expected_current_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    )
    router = SchemaBridgeHttpRouter(
        client,
        previous_release_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        private_canary_mode="gabia-redirect",
        strict_opcache_action="disabled_verified",
    )
    result = dict(
        deploy_release(
            client,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=router,
            gabia_compatibility=compatibility,
            schema_upgrade_from=11,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
        )
    )
    return client, router, result


def test_schema_bridge_deploy_rejects_non_takeover_release_id_without_io(
    local_plan: object,
    production_sftp: MemorySftp,
) -> None:
    before_files = dict(production_sftp.files)
    before_modes = dict(production_sftp.modes)
    before_directories = dict(production_sftp.directories)
    before_mutations = production_sftp.mutations

    with pytest.raises(
        PhpDeploymentError,
        match="must use the php-v2 prefix",
    ):
        deploy_release(
            production_sftp,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=(
                "schema11-bridge-abort-test-"
                "20260727t000000z-12345678"
            ),
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            schema_upgrade_from=11,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
        )

    assert production_sftp.files == before_files
    assert production_sftp.modes == before_modes
    assert production_sftp.directories == before_directories
    assert production_sftp.mutations == before_mutations


def test_schema_bridge_deploy_rejects_non_0644_c06_before_mutation(
    local_plan: object,
) -> None:
    client = _gabia_sftp()
    _install_attested_release(
        client,
        code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
        file_overrides={
            "migrations/011_global_terminal_v2.sql": _artifact_bytes(
                local_plan,
                "migrations/011_global_terminal_v2.sql",
            )
        },
    )
    manifest_path = DEFAULT_REMOTE_ROOT + "/" + DEPLOYMENT_MANIFEST_NAME
    client.modes[manifest_path] = 0o600
    before_files = dict(client.files)
    before_modes = dict(client.modes)
    before_directories = dict(client.directories)
    before_mutations = client.mutations

    with pytest.raises(PhpDeploymentError):
        deploy_release(
            client,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                client,
                code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
                schema_version=11,
            ),
            schema_upgrade_from=11,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
        )

    assert client.files == before_files
    assert client.modes == before_modes
    assert client.directories == before_directories
    assert client.mutations == before_mutations


def test_release_identity_rejects_mode_change_after_read(
    local_plan: object,
) -> None:
    class ModeFlipAfterReadSftp(GabiaMemorySftp):
        flip_path: str | None = None

        def open(self, path: str, mode: str = "r") -> object:
            handle = super().open(path, mode)
            normalized = self._path(path)
            if "r" not in mode or normalized != self.flip_path:
                return handle

            client = self

            class FlipReader(io.BytesIO):
                def close(self) -> None:
                    if not self.closed:
                        client.modes[normalized] = 0o600
                    super().close()

            assert isinstance(handle, io.BytesIO)
            return FlipReader(handle.getvalue())

    client = ModeFlipAfterReadSftp()
    client.add_directory("/www_root")
    client.add_directory(DEFAULT_REMOTE_ROOT)
    client.add_directory(DEFAULT_REMOTE_ROOT + "/migrations")
    _install_attested_release(
        client,
        code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
        file_overrides={
            "migrations/011_global_terminal_v2.sql": _artifact_bytes(
                local_plan,
                "migrations/011_global_terminal_v2.sql",
            )
        },
    )
    client.flip_path = DEFAULT_REMOTE_ROOT + "/api.php"

    with pytest.raises(
        PhpDeploymentError,
        match="changed during verification",
    ):
        verify_existing_remote_release_identity(
            client,
            remote_root=DEFAULT_REMOTE_ROOT,
            expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
            required_mode=0o644,
        )


def _candidate_compatibility(client: GabiaMemorySftp) -> object:
    return prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        expected_current_sha=RELEASE_SHA,
    )


def _partial_bridge_compatibility(client: GabiaMemorySftp) -> object:
    return prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        allow_partial_schema_bridge=True,
    )


def _bridge_ready(
    client: GabiaMemorySftp,
    deploy_result: dict[str, object],
) -> dict[str, object]:
    snapshot = load_remote_backup(
        client,
        backup_root=(
            DEFAULT_REMOTE_ROOT + "/_private/deployment-backups"
        ),
        release_id=RELEASE_ID,
        expected_remote_root=DEFAULT_REMOTE_ROOT,
    )
    assert snapshot.manifest_sha256 == deploy_result["backup_manifest_sha256"]
    return php_deploy._bridge_backup_ready_identity(
        backup=snapshot,
        candidate_code_revision=RELEASE_SHA,
        previous_code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        dart_disabled_evidence=DART_DISABLED_EVIDENCE,
    )


def test_deploy_report_exposes_exact_remote_backup_identity(
    local_plan: object,
) -> None:
    client, _router, result = _deploy_test_schema_bridge(local_plan)
    identity = result["backup_identity"]
    assert isinstance(identity, dict)
    manifest_path = str(identity["manifest_path"])
    assert result["release_id"] == RELEASE_ID
    assert identity["release_id"] == RELEASE_ID
    assert identity["candidate_code_revision"] == RELEASE_SHA
    assert identity["remote_root"] == DEFAULT_REMOTE_ROOT
    assert identity["backup_directory"].endswith("/" + RELEASE_ID)
    assert hashlib.sha256(client.files[manifest_path]).hexdigest() == (
        identity["manifest_sha256"]
    )
    assert result["backup_manifest_sha256"] == identity["manifest_sha256"]


def test_one_time_schema_bridge_confirmation_binds_all_four_identities() -> None:
    backup_sha256 = "f" * 64
    environment = {
        php_deploy.SCHEMA_BRIDGE_ROLLBACK_RELEASE_ID_ENV: RELEASE_ID,
        php_deploy.SCHEMA_BRIDGE_ROLLBACK_CURRENT_SHA_ENV: RELEASE_SHA,
        php_deploy.SCHEMA_BRIDGE_ROLLBACK_PREVIOUS_SHA_ENV: (
            ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
        ),
        php_deploy.SCHEMA_BRIDGE_ROLLBACK_BACKUP_SHA256_ENV: backup_sha256,
        php_deploy.SCHEMA_BRIDGE_DART_DISABLED_EVIDENCE_ENV: (
            DART_DISABLED_EVIDENCE
        ),
    }
    confirm_one_time_schema_bridge_rollback(
        release_id=RELEASE_ID,
        release_id_confirmation=RELEASE_ID,
        expected_current_sha=RELEASE_SHA,
        current_sha_confirmation=RELEASE_SHA,
        expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        previous_sha_confirmation=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        expected_backup_manifest_sha256=backup_sha256,
        backup_sha256_confirmation=backup_sha256,
        dart_disabled_evidence=DART_DISABLED_EVIDENCE,
        environ=environment,
    )
    with pytest.raises(PhpDeploymentError, match="confirmation does not match"):
        confirm_one_time_schema_bridge_rollback(
            release_id=RELEASE_ID,
            release_id_confirmation=RELEASE_ID,
            expected_current_sha=RELEASE_SHA,
            current_sha_confirmation=RELEASE_SHA,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            previous_sha_confirmation=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            expected_backup_manifest_sha256=backup_sha256,
            backup_sha256_confirmation="e" * 64,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            environ=environment,
        )


def test_schema_bridge_stale_lock_confirmation_requires_cli_and_env() -> None:
    acquired_at = (
        datetime.now(timezone.utc) - timedelta(minutes=30)
    ).isoformat()
    owner_content = php_deploy._encode_json(
        {
            "schema_version": 1,
            "release_id": RELEASE_ID,
            "acquired_at": acquired_at,
        }
    )
    evidence = _stale_writer_absence_evidence(
        owner_content=owner_content,
        acquired_at_reference=acquired_at,
    )
    environment = {
        php_deploy.SCHEMA_BRIDGE_STALE_LOCK_OWNER_ENV: RELEASE_ID,
        php_deploy.SCHEMA_BRIDGE_STALE_LOCK_WRITER_ABSENCE_ENV: (
            evidence
        ),
    }
    php_deploy.confirm_schema_bridge_stale_lock_takeover(
        owner_release_id=RELEASE_ID,
        owner_release_id_confirmation=RELEASE_ID,
        writer_absence_evidence=evidence,
        environ=environment,
    )
    with pytest.raises(
        PhpDeploymentError,
        match="confirmation does not match",
    ):
        php_deploy.confirm_schema_bridge_stale_lock_takeover(
            owner_release_id=RELEASE_ID,
            owner_release_id_confirmation=RELEASE_ID,
            writer_absence_evidence=evidence,
            environ={
                **environment,
                php_deploy.SCHEMA_BRIDGE_STALE_LOCK_OWNER_ENV: (
                    "php-v2-other-20260727t000000z-12345678"
                ),
            },
        )
    stale_recorded = _stale_writer_absence_evidence(
        owner_content=owner_content,
        acquired_at_reference=acquired_at,
        issued_at=(
            datetime.now(timezone.utc) - timedelta(minutes=30)
        ),
    )
    recorded_environment = {
        php_deploy.SCHEMA_BRIDGE_STALE_LOCK_OWNER_ENV: RELEASE_ID,
        php_deploy.SCHEMA_BRIDGE_STALE_LOCK_WRITER_ABSENCE_ENV: (
            stale_recorded
        ),
    }
    with pytest.raises(PhpDeploymentError):
        php_deploy.confirm_schema_bridge_stale_lock_takeover(
            owner_release_id=RELEASE_ID,
            owner_release_id_confirmation=RELEASE_ID,
            writer_absence_evidence=stale_recorded,
            environ=recorded_environment,
        )
    php_deploy.confirm_schema_bridge_stale_lock_takeover(
        owner_release_id=RELEASE_ID,
        owner_release_id_confirmation=RELEASE_ID,
        writer_absence_evidence=stale_recorded,
        environ=recorded_environment,
        allow_recorded_evidence=True,
    )


def test_schema_bridge_rollback_restores_manifest_last_and_closed_schema11(
    local_plan: object,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    compatibility = _candidate_compatibility(client)
    result = rollback_one_time_schema_bridge(
        client,
        candidate_plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        expected_current_sha=RELEASE_SHA,
        expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        expected_backup_manifest_sha256=str(
            deploy_result["backup_manifest_sha256"]
        ),
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=router,
        gabia_compatibility=compatibility,  # type: ignore[arg-type]
        dart_disabled_evidence=DART_DISABLED_EVIDENCE,
        bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
    )

    assert result["restored_code_revision"] == (
        ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
    )
    assert result["restored_api_schema_contract_version"] == 11
    assert result["candidate_database_schema_version_before"] == 11
    assert result["database_mutated"] is False
    assert result["manifest_committed_last"] is True
    assert result["byte_verification"] is True
    assert (
        DEFAULT_REMOTE_ROOT + "/migrations/012_dart_credential_pool.sql"
        not in client.files
    )
    assert verify_existing_remote_release_identity(
        client,
        remote_root=DEFAULT_REMOTE_ROOT,
        expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    ) == ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA


def test_schema_bridge_rollback_accepts_closed_candidate_on_database_schema12(
    local_plan: object,
) -> None:
    client, _router, deploy_result = _deploy_test_schema_bridge(local_plan)
    compatibility = _candidate_compatibility(client)
    target_files = {
        path: content
        for path, content in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    }
    healthy_schema12 = SchemaBridgeHttpRouter(
        client,
        previous_release_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        private_canary_mode="gabia-redirect",
        strict_opcache_action="disabled_verified",
        candidate_actual_schema_version=12,
    )

    result = rollback_one_time_schema_bridge(
        client,
        candidate_plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        expected_current_sha=RELEASE_SHA,
        expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        expected_backup_manifest_sha256=str(
            deploy_result["backup_manifest_sha256"]
        ),
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=healthy_schema12,
        gabia_compatibility=compatibility,  # type: ignore[arg-type]
        dart_disabled_evidence=DART_DISABLED_EVIDENCE,
        bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
    )

    assert result["candidate_database_schema_version_before"] == 12
    assert result["database_mutated"] is False
    assert {
        path: content
        for path, content in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    } != target_files


def test_schema_bridge_rollback_refuses_current_c06_schema11_state(
    local_plan: object,
) -> None:
    client = _gabia_sftp()
    _install_attested_release(
        client,
        code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    )
    compatibility = prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        expected_current_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    )
    before_files = dict(client.files)

    with pytest.raises(
        PhpDeploymentError,
        match="release identity does not match",
    ):
        rollback_one_time_schema_bridge(
            client,
            candidate_plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            expected_current_sha=RELEASE_SHA,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            expected_backup_manifest_sha256="f" * 64,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                client,
                code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
                schema_version=11,
                private_canary_mode="gabia-redirect",
                strict_opcache_action="disabled_verified",
            ),
            gabia_compatibility=compatibility,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
        )

    assert client.files == before_files


def test_schema_bridge_rollback_requires_exact_local_candidate_bytes(
    local_plan: object,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    manifest_path = DEFAULT_REMOTE_ROOT + "/" + DEPLOYMENT_MANIFEST_NAME
    api_path = DEFAULT_REMOTE_ROOT + "/api.php"
    alternate_api = b"self-attested-but-not-local-candidate\n"
    client.files[api_path] = alternate_api
    alternate_manifest = json.loads(client.files[manifest_path])
    alternate_manifest["files"]["api.php"] = hashlib.sha256(
        alternate_api
    ).hexdigest()
    client.files[manifest_path] = json.dumps(
        alternate_manifest,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    compatibility = _candidate_compatibility(client)
    before_files = dict(client.files)

    with pytest.raises(
        PhpDeploymentError,
        match="do not match the exact local release",
    ):
        rollback_one_time_schema_bridge(
            client,
            candidate_plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            expected_current_sha=RELEASE_SHA,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            expected_backup_manifest_sha256=str(
                deploy_result["backup_manifest_sha256"]
            ),
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=router,
            gabia_compatibility=compatibility,  # type: ignore[arg-type]
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
        )

    assert client.files == before_files


def test_schema_bridge_rollback_rejects_candidate_mode_drift_before_mutation(
    local_plan: object,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    compatibility = _candidate_compatibility(client)
    api_path = DEFAULT_REMOTE_ROOT + "/api.php"
    client.modes[api_path] = 0o600
    before_files = dict(client.files)
    before_modes = dict(client.modes)

    with pytest.raises(
        PhpDeploymentError,
        match="remote candidate artifact metadata is invalid",
    ):
        rollback_one_time_schema_bridge(
            client,
            candidate_plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            expected_current_sha=RELEASE_SHA,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            expected_backup_manifest_sha256=str(
                deploy_result["backup_manifest_sha256"]
            ),
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=router,
            gabia_compatibility=compatibility,  # type: ignore[arg-type]
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
        )

    assert client.files == before_files
    assert client.modes == before_modes


def test_normal_rollback_refuses_schema11_predecessor_backup(
    local_plan: object,
) -> None:
    client, _router, _deploy_result = _deploy_test_schema_bridge(local_plan)
    compatibility = _candidate_compatibility(client)
    target_files = {
        path: content
        for path, content in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    }

    with pytest.raises(
        PhpDeploymentError,
        match="normal rollback cannot restore a schema 11 predecessor",
    ):
        rollback_release(
            client,
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=HttpRouter(
                client,
                code_revision=RELEASE_SHA,
                schema_version=12,
                private_canary_mode="gabia-redirect",
                strict_opcache_action="disabled_verified",
            ),
            gabia_compatibility=compatibility,  # type: ignore[arg-type]
            expected_current_sha=RELEASE_SHA,
        )

    assert {
        path: content
        for path, content in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    } == target_files


def test_schema_bridge_rollback_reloads_top_manifest_under_lock(
    local_plan: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    compatibility = _candidate_compatibility(client)
    backup_manifest_path = str(deploy_result["backup_manifest"])
    target_manifest_path = DEFAULT_REMOTE_ROOT + "/" + DEPLOYMENT_MANIFEST_NAME
    candidate_manifest = client.files[target_manifest_path]
    original_capture = php_deploy.capture_remote_backup

    def capture_then_tamper(*args: object, **kwargs: object) -> object:
        snapshot = original_capture(*args, **kwargs)  # type: ignore[arg-type]
        if str(getattr(snapshot, "release_id")).startswith(
            "pre-schema11-bridge-rollback-"
        ):
            client.files[backup_manifest_path] += b" "
        return snapshot

    monkeypatch.setattr(
        php_deploy,
        "capture_remote_backup",
        capture_then_tamper,
    )

    with pytest.raises(
        PhpDeploymentError,
        match="stopped before production mutation",
    ):
        rollback_one_time_schema_bridge(
            client,
            candidate_plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            expected_current_sha=RELEASE_SHA,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            expected_backup_manifest_sha256=str(
                deploy_result["backup_manifest_sha256"]
            ),
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=router,
            gabia_compatibility=compatibility,  # type: ignore[arg-type]
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=_recording_bridge_updater(),  # type: ignore[arg-type]
        )

    assert client.files[target_manifest_path] == candidate_manifest


def _abort_test_schema_bridge(
    client: GabiaMemorySftp,
    router: SchemaBridgeHttpRouter,
    local_plan: object,
    deploy_result: dict[str, object],
    *,
    stale_lock_owner_release_id: str | None = None,
    stale_lock_writer_absence_evidence: str | None = None,
    stale_lock_first_observed_at: str | None = None,
    bridge_report_update: object | None = None,
) -> dict[str, object]:
    ready = _bridge_ready(client, deploy_result)
    return dict(
        php_deploy.abort_one_time_schema_bridge(
            client,
            candidate_plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            expected_backup_manifest_sha256=str(
                deploy_result["backup_manifest_sha256"]
            ),
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_deploy_report_load=lambda: dict(ready),
            bridge_report_update=(
                _recording_bridge_updater()
                if bridge_report_update is None
                else bridge_report_update
            ),  # type: ignore[arg-type]
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=router,
            gabia_compatibility=_partial_bridge_compatibility(client),  # type: ignore[arg-type]
            stale_lock_owner_release_id=stale_lock_owner_release_id,
            stale_lock_writer_absence_evidence=(
                stale_lock_writer_absence_evidence
            ),
            stale_lock_first_observed_at=stale_lock_first_observed_at,
        )
    )


def test_schema_bridge_abort_converges_complete_candidate_to_c06(
    local_plan: object,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)

    result = _abort_test_schema_bridge(
        client,
        router,
        local_plan,
        deploy_result,
    )

    assert result["initial_php_state"] == "candidate"
    assert result["candidate_database_schema_version_before"] == 11
    assert result["database_mutated"] is False
    assert verify_existing_remote_release_identity(
        client,
        remote_root=DEFAULT_REMOTE_ROOT,
        expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    ) == ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA


def test_schema_bridge_abort_converges_exact_mixed_files_to_c06(
    local_plan: object,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    predecessor = load_remote_backup(
        client,
        backup_root=(
            DEFAULT_REMOTE_ROOT + "/_private/deployment-backups"
        ),
        release_id=RELEASE_ID,
        expected_remote_root=DEFAULT_REMOTE_ROOT,
    )
    api_snapshot = predecessor.file_by_path["api.php"]
    assert api_snapshot.backup_blob is not None
    api_path = DEFAULT_REMOTE_ROOT + "/api.php"
    client.files[api_path] = client.files[
        predecessor.backup_directory + "/" + api_snapshot.backup_blob
    ]
    assert api_snapshot.mode is not None
    client.modes[api_path] = api_snapshot.mode

    result = _abort_test_schema_bridge(
        client,
        router,
        local_plan,
        deploy_result,
    )

    assert result["initial_php_state"] == "mixed"
    assert result["candidate_database_schema_version_before"] is None
    assert (
        result["database_schema_observation"]
        == "unavailable_due_partial_php"
    )


def test_schema_bridge_abort_resumes_manifest_absent_c06_restore(
    local_plan: object,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    predecessor = load_remote_backup(
        client,
        backup_root=(
            DEFAULT_REMOTE_ROOT + "/_private/deployment-backups"
        ),
        release_id=RELEASE_ID,
        expected_remote_root=DEFAULT_REMOTE_ROOT,
    )
    for relative_path in DEFAULT_COMMIT_ORDER:
        item = predecessor.file_by_path[relative_path]
        target = DEFAULT_REMOTE_ROOT + "/" + relative_path
        if relative_path == DEPLOYMENT_MANIFEST_NAME:
            client.files.pop(target)
            client.modes.pop(target)
            continue
        if not item.existed:
            client.files.pop(target, None)
            client.modes.pop(target, None)
            continue
        assert item.backup_blob is not None
        assert item.mode is not None
        client.files[target] = client.files[
            predecessor.backup_directory + "/" + item.backup_blob
        ]
        client.modes[target] = item.mode

    result = _abort_test_schema_bridge(
        client,
        router,
        local_plan,
        deploy_result,
    )

    assert result["initial_php_state"] == "predecessor_restore_transition"
    assert verify_existing_remote_release_identity(
        client,
        remote_root=DEFAULT_REMOTE_ROOT,
        expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    ) == ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA


def test_schema_bridge_abort_rejects_third_party_bytes_before_mutation(
    local_plan: object,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    api_path = DEFAULT_REMOTE_ROOT + "/api.php"
    client.files[api_path] = b"unrecognized third-party bytes\n"
    before = dict(client.files)

    with pytest.raises(
        PhpDeploymentError,
        match="schema bridge abort is incomplete",
    ):
        _abort_test_schema_bridge(
            client,
            router,
            local_plan,
            deploy_result,
        )

    assert client.files == before


def test_schema_bridge_abort_takes_over_only_confirmed_stale_bridge_lock(
    local_plan: object,
    tmp_path: Path,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    lock_path = php_deploy._acquire_deployment_lock(
        client,
        DEFAULT_REMOTE_ROOT + "/_private",
        RELEASE_ID,
    )
    assert lock_path in client.directories
    _acquired_at, stale_evidence = _age_test_lock_owner(
        client,
        lock_path,
    )
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    report_path = (tmp_path / "bridge-abort.json").resolve()
    php_deploy._prepare_durable_report(
        report_path,
        private_root=private_root,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    updater = php_deploy._bridge_report_updater(
        report_path,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )

    result = _abort_test_schema_bridge(
        client,
        router,
        local_plan,
        deploy_result,
        stale_lock_owner_release_id=RELEASE_ID,
        stale_lock_writer_absence_evidence=stale_evidence,
        bridge_report_update=updater,
    )
    php_deploy._commit_durable_report(report_path, result)

    takeover = result["stale_lock_takeover"]
    assert isinstance(takeover, dict)
    assert takeover["stale_owner_release_id"] == RELEASE_ID
    assert takeover["writer_absence_evidence"] == stale_evidence
    assert result["stale_lock_cleanup_verified"] is True
    assert lock_path not in client.directories
    durable = json.loads(report_path.read_text(encoding="utf-8"))
    assert durable["status"] == "completed"
    assert durable["stale_lock_takeover"] == takeover
    assert durable["stale_lock_cleanup_verified"] is True


@pytest.mark.parametrize(
    "crash_point",
    ("before_replacement_lock", "after_takeover_complete"),
)
def test_schema_bridge_abort_resumes_durable_takeover_crash_windows(
    local_plan: object,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    lock_path = php_deploy._acquire_deployment_lock(
        client,
        DEFAULT_REMOTE_ROOT + "/_private",
        RELEASE_ID,
    )
    _acquired_at, stale_evidence = _age_test_lock_owner(
        client,
        lock_path,
    )
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    report_path = (tmp_path / "bridge-abort.json").resolve()
    php_deploy._prepare_durable_report(
        report_path,
        private_root=private_root,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    updater = php_deploy._bridge_report_updater(
        report_path,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )
    first_updater: object = updater
    original_acquire = php_deploy._acquire_deployment_lock
    failed = False
    if crash_point == "before_replacement_lock":

        def fail_replacement_once(
            selected_client: object,
            private_path: str,
            selected_release_id: str,
        ) -> str:
            nonlocal failed
            if (
                not failed
                and selected_release_id.startswith(
                    "schema11-bridge-abort-"
                )
            ):
                failed = True
                raise RuntimeError("simulated crash before replacement lock")
            return original_acquire(
                selected_client,  # type: ignore[arg-type]
                private_path,
                selected_release_id,
            )

        monkeypatch.setattr(
            php_deploy,
            "_acquire_deployment_lock",
            fail_replacement_once,
        )
    else:

        def fail_after_complete(
            status: str,
            evidence: object,
        ) -> None:
            nonlocal failed
            assert callable(updater)
            updater(status, evidence)  # type: ignore[arg-type]
            if status == "stale_lock_takeover_complete" and not failed:
                failed = True
                raise RuntimeError("simulated crash after takeover complete")

        first_updater = fail_after_complete

    with pytest.raises((PhpDeploymentError, RuntimeError)):
        _abort_test_schema_bridge(
            client,
            router,
            local_plan,
            deploy_result,
            stale_lock_owner_release_id=RELEASE_ID,
            stale_lock_writer_absence_evidence=stale_evidence,
            bridge_report_update=first_updater,
        )

    assert failed is True
    assert (
        php_deploy._prepare_durable_report(
            report_path,
            private_root=private_root,
            operation="schema-bridge-abort",
            code_revision=RELEASE_SHA,
            release_id=RELEASE_ID,
            allow_bridge_abort_resume=True,
            environ={
                php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
            },
        )
        == report_path
    )
    result = _abort_test_schema_bridge(
        client,
        router,
        local_plan,
        deploy_result,
        stale_lock_owner_release_id=RELEASE_ID,
        stale_lock_writer_absence_evidence=stale_evidence,
        bridge_report_update=updater,
    )
    php_deploy._commit_durable_report(report_path, result)

    assert result["stale_lock_cleanup_verified"] is True
    assert lock_path not in client.directories
    assert json.loads(report_path.read_text(encoding="utf-8"))[
        "status"
    ] == "completed"


def test_completed_takeover_preserves_new_ownerless_writer_lock(
    local_plan: object,
    tmp_path: Path,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    lock_path = php_deploy._acquire_deployment_lock(
        client,
        DEFAULT_REMOTE_ROOT + "/_private",
        RELEASE_ID,
    )
    _acquired_at, stale_evidence = _age_test_lock_owner(
        client,
        lock_path,
    )
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    report_path = (tmp_path / "bridge-abort.json").resolve()
    php_deploy._prepare_durable_report(
        report_path,
        private_root=private_root,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    updater = php_deploy._bridge_report_updater(
        report_path,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )
    failed = False

    def stop_after_complete(status: str, evidence: object) -> None:
        nonlocal failed
        updater(status, evidence)  # type: ignore[arg-type]
        if status == "stale_lock_takeover_complete" and not failed:
            failed = True
            raise RuntimeError("simulated stop after takeover completion")

    with pytest.raises(PhpDeploymentError):
        _abort_test_schema_bridge(
            client,
            router,
            local_plan,
            deploy_result,
            stale_lock_owner_release_id=RELEASE_ID,
            stale_lock_writer_absence_evidence=stale_evidence,
            bridge_report_update=stop_after_complete,
        )
    assert failed is True
    assert lock_path not in client.directories
    client.mkdir(lock_path, mode=0o700)
    client.chmod(lock_path, 0o700)

    with pytest.raises(
        PhpDeploymentError,
        match="unrelated replacement lock",
    ):
        _abort_test_schema_bridge(
            client,
            router,
            local_plan,
            deploy_result,
            stale_lock_owner_release_id=RELEASE_ID,
            stale_lock_writer_absence_evidence=stale_evidence,
            bridge_report_update=updater,
        )

    assert lock_path in client.directories
    assert lock_path + "/owner.json" not in client.files
    valid = json.loads(report_path.read_text(encoding="utf-8"))
    for substituted_nonce in (
        "not-a-valid-journal-nonce",
        "f" * 32,
    ):
        invalid_nonce = dict(valid)
        invalid_nonce["journal_nonce"] = substituted_nonce
        report_path.write_bytes(php_deploy._encode_json(invalid_nonce))
        with pytest.raises(
            PhpDeploymentError,
            match="not resumable",
        ):
            php_deploy._prepare_durable_report(
                report_path,
                private_root=private_root,
                operation="schema-bridge-abort",
                code_revision=RELEASE_SHA,
                release_id=RELEASE_ID,
                allow_bridge_abort_resume=True,
                environ={
                    php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
                },
            )
        invalid_nonce_updater = php_deploy._bridge_report_updater(
            report_path,
            operation="schema-bridge-abort",
            code_revision=RELEASE_SHA,
            release_id=RELEASE_ID,
        )
        with pytest.raises(
            PhpDeploymentError,
            match="journal identity is invalid",
        ):
            invalid_nonce_updater(
                str(valid["status"]),
                {
                    "stale_lock_takeover": valid["stale_lock_takeover"],
                    "stale_lock_cleanup_verified": True,
                },
            )

    forged = dict(valid)
    forged["backup_ready"] = None
    report_path.write_bytes(php_deploy._encode_json(forged))
    forged_fixed_updater = php_deploy._bridge_report_updater(
        report_path,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )
    with pytest.raises(
        PhpDeploymentError,
        match="fixed recovery identity is invalid",
    ):
        forged_fixed_updater(
            str(valid["status"]),
            {
                "stale_lock_takeover": valid["stale_lock_takeover"],
                "stale_lock_cleanup_verified": True,
            },
        )
    with pytest.raises(
        PhpDeploymentError,
        match="not resumable",
    ):
        php_deploy._prepare_durable_report(
            report_path,
            private_root=private_root,
            operation="schema-bridge-abort",
            code_revision=RELEASE_SHA,
            release_id=RELEASE_ID,
            allow_bridge_abort_resume=True,
            environ={
                php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
            },
        )


def test_ownerless_lock_inspector_cli_attests_without_remote_mutation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = GabiaMemorySftp()
    client.add_directory("/www_root")
    client.add_directory(DEFAULT_REMOTE_ROOT)
    client.add_directory(DEFAULT_REMOTE_ROOT + "/_private", 0o700)
    lock_path = DEFAULT_REMOTE_ROOT + "/_private/deployment-lock"
    client.mkdir(lock_path, mode=0o700)
    client.chmod(lock_path, 0o700)
    first_observed, _evidence, remote_identity = (
        _age_test_ownerless_lock(
            client,
            lock_path,
        )
    )
    mutations_before = client.mutations

    class TestSession:
        def __init__(self, _options: object) -> None:
            return

        def __enter__(self) -> MemorySftp:
            return client

        def __exit__(self, *_args: object) -> None:
            return

    monkeypatch.setattr(
        php_deploy,
        "ParamikoPinnedSftpSession",
        TestSession,
    )
    monkeypatch.setenv("SSH_PASSWORD", "test-password")

    assert (
        main(
            [
                "inspect-ownerless-lock",
                "--ssh-host",
                "ssh.example",
                "--ssh-port",
                "22",
                "--ssh-user",
                "deploy",
                "--ssh-host-key-sha256",
                "SHA256:" + "A" * 43,
                "--remote-root",
                DEFAULT_REMOTE_ROOT,
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert captured.err == ""
    assert report["inspection"] == "schema_bridge_ownerless_lock"
    assert report["owner_release_id"] == php_deploy.STALE_LOCK_OWNERLESS
    assert report["remote_identity"] == json.loads(remote_identity)
    assert report["owner_sha256"] == hashlib.sha256(
        remote_identity
    ).hexdigest()
    assert report["stale_lock_first_observed_at"] == first_observed
    assert report["remote_mtime"] == first_observed
    assert report["eligible_for_writer_absence_attestation"] is True
    assert report["remote_files_mutated"] is False
    assert client.mutations == mutations_before


@pytest.mark.parametrize(
    "crash_window",
    ("mkdir_before_owner", "owner_deleted_before_rmdir"),
)
def test_schema_bridge_abort_recovers_exact_ownerless_lock_windows(
    local_plan: object,
    tmp_path: Path,
    crash_window: str,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    lock_path = DEFAULT_REMOTE_ROOT + "/_private/deployment-lock"
    if crash_window == "mkdir_before_owner":
        client.mkdir(lock_path, mode=0o700)
        client.chmod(lock_path, 0o700)
    else:
        php_deploy._acquire_deployment_lock(
            client,
            DEFAULT_REMOTE_ROOT + "/_private",
            RELEASE_ID,
        )
        client.remove(lock_path + "/owner.json")
    first_observed, evidence, remote_identity = (
        _age_test_ownerless_lock(
            client,
            lock_path,
        )
    )
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    report_path = (tmp_path / f"{crash_window}.json").resolve()
    php_deploy._prepare_durable_report(
        report_path,
        private_root=private_root,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    updater = php_deploy._bridge_report_updater(
        report_path,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )

    result = _abort_test_schema_bridge(
        client,
        router,
        local_plan,
        deploy_result,
        stale_lock_owner_release_id=php_deploy.STALE_LOCK_OWNERLESS,
        stale_lock_writer_absence_evidence=evidence,
        stale_lock_first_observed_at=first_observed,
        bridge_report_update=updater,
    )
    php_deploy._commit_durable_report(report_path, result)

    assert lock_path not in client.directories
    takeover = result["stale_lock_takeover"]
    assert isinstance(takeover, dict)
    assert takeover["stale_owner_state"] == "ownerless"
    assert takeover["stale_owner_remote_mtime"] == first_observed
    assert takeover["stale_owner_sha256"] == hashlib.sha256(
        remote_identity
    ).hexdigest()
    assert (
        verify_existing_remote_release_identity(
            client,
            remote_root=DEFAULT_REMOTE_ROOT,
            expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
            required_mode=0o644,
        )
        == ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
    )


@pytest.mark.parametrize(
    (
        "failure",
        "first_observed_delta",
        "issued_delta",
        "owner_content_override",
    ),
    (
        ("too_young", 5, 0, None),
        ("stale_evidence", 30, 30, None),
        ("forged_owner_digest", 30, 0, b"forged-owner"),
        ("attested_mtime_mismatch", 31, 0, None),
    ),
)
def test_schema_bridge_abort_preserves_ownerless_lock_on_invalid_evidence(
    local_plan: object,
    failure: str,
    first_observed_delta: int,
    issued_delta: int,
    owner_content_override: bytes | None,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    lock_path = DEFAULT_REMOTE_ROOT + "/_private/deployment-lock"
    client.mkdir(lock_path, mode=0o700)
    client.chmod(lock_path, 0o700)
    first_observed, evidence, _remote_identity = (
        _age_test_ownerless_lock(
            client,
            lock_path,
            first_observed_delta=first_observed_delta,
            issued_delta=issued_delta,
            owner_content_override=owner_content_override,
        )
    )
    public_before = {
        path: value
        for path, value in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    }

    with pytest.raises(PhpDeploymentError):
        _abort_test_schema_bridge(
            client,
            router,
            local_plan,
            deploy_result,
            stale_lock_owner_release_id=(
                php_deploy.STALE_LOCK_OWNERLESS
            ),
            stale_lock_writer_absence_evidence=evidence,
            stale_lock_first_observed_at=first_observed,
        )

    assert failure
    assert lock_path in client.directories
    assert {
        path: value
        for path, value in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    } == public_before


def test_ownerless_takeover_preserves_lock_recreated_before_delete(
    local_plan: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    lock_path = DEFAULT_REMOTE_ROOT + "/_private/deployment-lock"
    client.mkdir(lock_path, mode=0o700)
    client.chmod(lock_path, 0o700)
    first_observed, evidence, old_remote_identity = (
        _age_test_ownerless_lock(
            client,
            lock_path,
        )
    )
    public_before = {
        path: value
        for path, value in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    }
    original_remove = php_deploy._remove_exact_ownerless_deployment_lock
    replaced = False

    def replace_before_remove(
        selected_client: MemorySftp,
        *,
        lock_path: str,
        expected_remote_identity: bytes,
    ) -> None:
        nonlocal replaced
        assert expected_remote_identity == old_remote_identity
        selected_client.rmdir(lock_path)
        selected_client.mkdir(lock_path, mode=0o700)
        selected_client.chmod(lock_path, 0o700)
        replaced = True
        original_remove(
            selected_client,
            lock_path=lock_path,
            expected_remote_identity=expected_remote_identity,
        )

    monkeypatch.setattr(
        php_deploy,
        "_remove_exact_ownerless_deployment_lock",
        replace_before_remove,
    )

    with pytest.raises(
        PhpDeploymentError,
        match=(
            "minimum remote stale age|remote identity changed before takeover"
        ),
    ):
        _abort_test_schema_bridge(
            client,
            router,
            local_plan,
            deploy_result,
            stale_lock_owner_release_id=php_deploy.STALE_LOCK_OWNERLESS,
            stale_lock_writer_absence_evidence=evidence,
            stale_lock_first_observed_at=first_observed,
        )

    assert replaced is True
    assert lock_path in client.directories
    assert lock_path + "/owner.json" not in client.files
    assert client.mtimes[lock_path] != json.loads(
        old_remote_identity
    )["st_mtime"]
    assert {
        path: value
        for path, value in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    } == public_before


def test_schema_bridge_abort_preserves_nonempty_ownerless_lock(
    local_plan: object,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    lock_path = DEFAULT_REMOTE_ROOT + "/_private/deployment-lock"
    client.mkdir(lock_path, mode=0o700)
    client.chmod(lock_path, 0o700)
    client.add_file(lock_path + "/unexpected-entry", b"foreign\n", 0o600)
    client.mtimes[lock_path] = int(
        (
            datetime.now(timezone.utc) - timedelta(minutes=30)
        ).timestamp()
    )
    remote_identity, _identity, first_observed = (
        php_deploy._ownerless_lock_remote_identity(
            client.lstat(lock_path),
            lock_path=lock_path,
        )
    )
    evidence = _stale_writer_absence_evidence(
        owner_content=remote_identity,
        acquired_at_reference=first_observed,
    )

    with pytest.raises(
        PhpDeploymentError,
        match="not empty",
    ):
        _abort_test_schema_bridge(
            client,
            router,
            local_plan,
            deploy_result,
            stale_lock_owner_release_id=(
                php_deploy.STALE_LOCK_OWNERLESS
            ),
            stale_lock_writer_absence_evidence=evidence,
            stale_lock_first_observed_at=first_observed,
        )

    assert lock_path in client.directories
    assert client.files[lock_path + "/unexpected-entry"] == b"foreign\n"


def test_schema_bridge_abort_preserves_unconfirmed_third_party_lock(
    local_plan: object,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    third_owner = "php-v2-thirdparty-20260727t000000z-12345678"
    lock_path = php_deploy._acquire_deployment_lock(
        client,
        DEFAULT_REMOTE_ROOT + "/_private",
        third_owner,
    )
    owner_path = lock_path + "/owner.json"
    _acquired_at, stale_evidence = _age_test_lock_owner(
        client,
        lock_path,
    )
    owner_before = client.files[owner_path]
    candidate_before = {
        path: content
        for path, content in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    }

    with pytest.raises(
        PhpDeploymentError,
        match="owner identity does not match",
    ):
        _abort_test_schema_bridge(
            client,
            router,
            local_plan,
            deploy_result,
            stale_lock_owner_release_id=RELEASE_ID,
            stale_lock_writer_absence_evidence=stale_evidence,
        )

    assert client.files[owner_path] == owner_before
    assert lock_path in client.directories
    assert {
        path: content
        for path, content in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    } == candidate_before


def test_schema_bridge_abort_preserves_nonempty_stale_lock(
    local_plan: object,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    lock_path = php_deploy._acquire_deployment_lock(
        client,
        DEFAULT_REMOTE_ROOT + "/_private",
        RELEASE_ID,
    )
    _acquired_at, stale_evidence = _age_test_lock_owner(
        client,
        lock_path,
    )
    owner_path = lock_path + "/owner.json"
    owner_before = client.files[owner_path]
    client.add_file(lock_path + "/unexpected-entry", b"foreign\n", 0o600)

    with pytest.raises(
        PhpDeploymentError,
        match="non-empty or changed lock",
    ):
        _abort_test_schema_bridge(
            client,
            router,
            local_plan,
            deploy_result,
            stale_lock_owner_release_id=RELEASE_ID,
            stale_lock_writer_absence_evidence=stale_evidence,
        )

    assert client.files[owner_path] == owner_before
    assert client.files[lock_path + "/unexpected-entry"] == b"foreign\n"
    assert lock_path in client.directories


def test_durable_report_is_reserved_then_atomically_completed(
    tmp_path: Path,
) -> None:
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    destination = (tmp_path / "deploy-report.json").resolve()
    prepared = php_deploy._prepare_durable_report(
        destination,
        private_root=private_root,
        operation="deploy",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    assert prepared == destination
    prepared_payload = json.loads(destination.read_text(encoding="utf-8"))
    assert prepared_payload["status"] == "prepared"
    assert "report" not in prepared_payload

    report = {
        "ok": True,
        "operation": "deploy",
        "release_id": RELEASE_ID,
        "backup_manifest_sha256": "f" * 64,
        "backup_manifest": "/private/backup-manifest.json",
        "code_revision": RELEASE_SHA,
        "backup_identity": {
            "release_id": RELEASE_ID,
            "candidate_code_revision": RELEASE_SHA,
            "manifest_path": "/private/backup-manifest.json",
            "manifest_sha256": "f" * 64,
        },
    }
    php_deploy._commit_durable_report(destination, report)
    completed = json.loads(destination.read_text(encoding="utf-8"))
    assert completed["status"] == "completed"
    assert completed["report"] == report


def test_durable_prepared_checkpoint_cannot_commit_failed_report(
    tmp_path: Path,
) -> None:
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    destination = (tmp_path / "deploy-report.json").resolve()
    php_deploy._prepare_durable_report(
        destination,
        private_root=private_root,
        operation="deploy",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )

    with pytest.raises(
        PhpDeploymentError,
        match="does not match its prepared checkpoint",
    ):
        php_deploy._commit_durable_report(
            destination,
            {
                "ok": False,
                "operation": "deploy",
                "release_id": RELEASE_ID,
                "code_revision": RELEASE_SHA,
            },
        )

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["status"] == "prepared"
    assert "report" not in payload


def _prepared_bridge_deploy(
    local_plan: object,
    destination: Path,
    private_root: Path,
) -> tuple[
    GabiaMemorySftp,
    SchemaBridgeHttpRouter,
    object,
    object,
]:
    client = _gabia_sftp()
    _install_attested_release(
        client,
        code_revision=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
        file_overrides={
            "migrations/011_global_terminal_v2.sql": _artifact_bytes(
                local_plan,
                "migrations/011_global_terminal_v2.sql",
            )
        },
    )
    compatibility = prepare_gabia_core_compatibility(
        client,
        ssh_options=_gabia_options(),  # type: ignore[arg-type]
        compatibility_host=GABIA_COMPATIBILITY_SSH_HOST,
        remote_root=DEFAULT_REMOTE_ROOT,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        expected_current_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    )
    php_deploy._prepare_durable_report(
        destination,
        private_root=private_root,
        operation="schema-bridge-deploy",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        schema_bridge_dart_disabled_evidence=(
            DART_DISABLED_EVIDENCE
        ),
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    updater = php_deploy._bridge_report_updater(
        destination,
        operation="schema-bridge-deploy",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )
    router = SchemaBridgeHttpRouter(
        client,
        previous_release_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        private_canary_mode="gabia-redirect",
        strict_opcache_action="disabled_verified",
    )
    return client, router, compatibility, updater


def test_schema_bridge_abort_recovers_prebackup_crash_without_public_write(
    local_plan: object,
    tmp_path: Path,
) -> None:
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    deploy_report = (tmp_path / "bridge-deploy.json").resolve()
    abort_report = (tmp_path / "bridge-abort.json").resolve()
    client, router, _compatibility, _updater = (
        _prepared_bridge_deploy(
            local_plan,
            deploy_report,
            private_root,
        )
    )
    compatibility = _partial_bridge_compatibility(client)
    lock_path = php_deploy._acquire_deployment_lock(
        client,
        DEFAULT_REMOTE_ROOT + "/_private",
        RELEASE_ID,
    )
    _acquired_at, stale_evidence = _age_test_lock_owner(
        client,
        lock_path,
    )
    public_before = {
        path: (value, client.modes[path])
        for path, value in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    }

    def load_prepared() -> object:
        return php_deploy.load_schema_bridge_backup_ready(
            deploy_report,
            private_root=private_root,
            expected_candidate_sha=RELEASE_SHA,
            expected_release_id=RELEASE_ID,
            expected_backup_manifest_sha256=None,
            expected_dart_disabled_evidence=DART_DISABLED_EVIDENCE,
        )

    php_deploy._prepare_durable_report(
        abort_report,
        private_root=private_root,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    updater = php_deploy._bridge_report_updater(
        abort_report,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )
    result = php_deploy.abort_one_time_schema_bridge(
        client,
        candidate_plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        expected_backup_manifest_sha256=None,
        dart_disabled_evidence=DART_DISABLED_EVIDENCE,
        bridge_deploy_report_load=load_prepared,  # type: ignore[arg-type]
        bridge_report_update=updater,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=router,
        gabia_compatibility=compatibility,  # type: ignore[arg-type]
        stale_lock_owner_release_id=RELEASE_ID,
        stale_lock_writer_absence_evidence=stale_evidence,
    )
    php_deploy._commit_durable_report(abort_report, result)

    assert result["initial_php_state"] == "prebackup_c06"
    assert result["public_release_files_mutated"] is False
    assert (
        result["ephemeral_opcache_probe_created_and_removed"] is True
    )
    assert result["manifest_commit_not_applicable"] is True
    assert result["backup_manifest"] is None
    assert result["candidate_database_schema_version_before"] is None
    assert (
        result["database_schema_observation"]
        == "unavailable_due_c06_contract"
    )
    assert {
        path: (value, client.modes[path])
        for path, value in client.files.items()
        if path.startswith(DEFAULT_REMOTE_ROOT + "/")
        and "/_private/" not in path
    } == public_before
    assert lock_path not in client.directories
    assert json.loads(abort_report.read_text(encoding="utf-8"))[
        "status"
    ] == "completed"


@pytest.mark.parametrize(
    "corruption",
    ("mode", "bytes", "stray_migration_012"),
)
def test_schema_bridge_prebackup_recovery_preserves_lock_on_public_drift(
    local_plan: object,
    tmp_path: Path,
    corruption: str,
) -> None:
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    deploy_report = (tmp_path / "bridge-deploy.json").resolve()
    abort_report = (tmp_path / "bridge-abort.json").resolve()
    client, router, _compatibility, _updater = (
        _prepared_bridge_deploy(
            local_plan,
            deploy_report,
            private_root,
        )
    )
    compatibility = _partial_bridge_compatibility(client)
    lock_path = php_deploy._acquire_deployment_lock(
        client,
        DEFAULT_REMOTE_ROOT + "/_private",
        RELEASE_ID,
    )
    _acquired_at, stale_evidence = _age_test_lock_owner(
        client,
        lock_path,
    )
    if corruption == "mode":
        client.modes[DEFAULT_REMOTE_ROOT + "/api.php"] = 0o600
    elif corruption == "bytes":
        client.files[DEFAULT_REMOTE_ROOT + "/api.php"] = b"changed\n"
    else:
        client.add_file(
            DEFAULT_REMOTE_ROOT
            + "/migrations/012_dart_credential_pool.sql",
            b"stray\n",
            0o644,
        )
    owner_before = client.files[lock_path + "/owner.json"]

    def load_prepared() -> object:
        return php_deploy.load_schema_bridge_backup_ready(
            deploy_report,
            private_root=private_root,
            expected_candidate_sha=RELEASE_SHA,
            expected_release_id=RELEASE_ID,
            expected_backup_manifest_sha256=None,
            expected_dart_disabled_evidence=DART_DISABLED_EVIDENCE,
        )

    php_deploy._prepare_durable_report(
        abort_report,
        private_root=private_root,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    updater = php_deploy._bridge_report_updater(
        abort_report,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )

    with pytest.raises(PhpDeploymentError):
        php_deploy.abort_one_time_schema_bridge(
            client,
            candidate_plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            expected_backup_manifest_sha256=None,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_deploy_report_load=load_prepared,  # type: ignore[arg-type]
            bridge_report_update=updater,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=router,
            gabia_compatibility=compatibility,  # type: ignore[arg-type]
            stale_lock_owner_release_id=RELEASE_ID,
            stale_lock_writer_absence_evidence=stale_evidence,
        )

    assert client.files[lock_path + "/owner.json"] == owner_before
    assert lock_path in client.directories


def test_bridge_deploy_crash_before_first_file_leaves_backup_ready_journal(
    local_plan: object,
    tmp_path: Path,
) -> None:
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    destination = (tmp_path / "bridge-deploy.json").resolve()
    client, router, compatibility, updater = _prepared_bridge_deploy(
        local_plan,
        destination,
        private_root,
    )

    def crash_before_commit(status: str, evidence: object) -> None:
        if status == "commit_started":
            raise RuntimeError("simulated process stop before first file")
        assert callable(updater)
        updater(status, evidence)  # type: ignore[arg-type]

    with pytest.raises(PhpDeploymentError, match="stopped before commit"):
        deploy_release(
            client,
            plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=router,
            gabia_compatibility=compatibility,  # type: ignore[arg-type]
            schema_upgrade_from=11,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=crash_before_commit,
        )

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["status"] == "backup_ready"
    assert payload["backup_ready"]["previous_code_revision"] == (
        ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA
    )
    assert verify_existing_remote_release_identity(
        client,
        remote_root=DEFAULT_REMOTE_ROOT,
        expected_core_files=LEGACY_SCHEMA_11_CORE_API_FILES,
    ) == ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA


def test_bridge_completed_report_preserves_backup_ready_for_later_abort(
    local_plan: object,
    tmp_path: Path,
) -> None:
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    destination = (tmp_path / "bridge-deploy.json").resolve()
    client, router, compatibility, updater = _prepared_bridge_deploy(
        local_plan,
        destination,
        private_root,
    )
    result = deploy_release(
        client,
        plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=router,
        gabia_compatibility=compatibility,  # type: ignore[arg-type]
        schema_upgrade_from=11,
        expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        dart_disabled_evidence=DART_DISABLED_EVIDENCE,
        bridge_report_update=updater,  # type: ignore[arg-type]
    )
    php_deploy._commit_durable_report(destination, result)

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 2
    assert payload["status"] == "completed"
    assert payload["backup_ready"]["backup_manifest_sha256"] == (
        result["backup_manifest_sha256"]
    )
    loaded = php_deploy.load_schema_bridge_backup_ready(
        destination,
        private_root=private_root,
        expected_candidate_sha=RELEASE_SHA,
        expected_release_id=RELEASE_ID,
        expected_backup_manifest_sha256=str(
            result["backup_manifest_sha256"]
        ),
        expected_dart_disabled_evidence=DART_DISABLED_EVIDENCE,
    )
    assert loaded == payload["backup_ready"]


def test_bridge_rollback_completed_report_binds_emergency_snapshot(
    local_plan: object,
    tmp_path: Path,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    destination = (tmp_path / "bridge-rollback.json").resolve()
    php_deploy._prepare_durable_report(
        destination,
        private_root=private_root,
        operation="schema-bridge-rollback",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    updater = php_deploy._bridge_report_updater(
        destination,
        operation="schema-bridge-rollback",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )
    result = rollback_one_time_schema_bridge(
        client,
        candidate_plan=local_plan,  # type: ignore[arg-type]
        release_id=RELEASE_ID,
        expected_current_sha=RELEASE_SHA,
        expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
        expected_backup_manifest_sha256=str(
            deploy_result["backup_manifest_sha256"]
        ),
        public_url_root=PUBLIC_ROOT,
        api_v2_base_url=API_V2,
        rollback_health_url=ROLLBACK_HEALTH,
        protected_token=PROTECTED_TOKEN,
        http_request=router,
        gabia_compatibility=_candidate_compatibility(client),  # type: ignore[arg-type]
        dart_disabled_evidence=DART_DISABLED_EVIDENCE,
        bridge_report_update=updater,
    )
    php_deploy._commit_durable_report(destination, result)

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["status"] == "completed"
    assert payload["emergency_backup"] == (
        payload["report"]["emergency_backup_identity"]
    )


def test_bridge_rollback_completed_report_rejects_cross_state_database(
    local_plan: object,
    tmp_path: Path,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    destination = (tmp_path / "bridge-rollback.json").resolve()
    php_deploy._prepare_durable_report(
        destination,
        private_root=private_root,
        operation="schema-bridge-rollback",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    updater = php_deploy._bridge_report_updater(
        destination,
        operation="schema-bridge-rollback",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )
    result = dict(
        rollback_one_time_schema_bridge(
            client,
            candidate_plan=local_plan,  # type: ignore[arg-type]
            release_id=RELEASE_ID,
            expected_current_sha=RELEASE_SHA,
            expected_previous_sha=ONE_TIME_SCHEMA_BRIDGE_PREVIOUS_SHA,
            expected_backup_manifest_sha256=str(
                deploy_result["backup_manifest_sha256"]
            ),
            public_url_root=PUBLIC_ROOT,
            api_v2_base_url=API_V2,
            rollback_health_url=ROLLBACK_HEALTH,
            protected_token=PROTECTED_TOKEN,
            http_request=router,
            gabia_compatibility=_candidate_compatibility(client),  # type: ignore[arg-type]
            dart_disabled_evidence=DART_DISABLED_EVIDENCE,
            bridge_report_update=updater,
        )
    )
    checkpoint = destination.read_bytes()
    variants = (
        {
            **result,
            "candidate_database_schema_version_before": None,
            "database_schema_observation": (
                "unavailable_due_partial_php"
            ),
        },
        {
            **result,
            "database_schema_observation": "candidate_schema_12",
        },
        {**result, "initial_php_state": "candidate"},
    )
    for index, variant in enumerate(variants):
        forged = (tmp_path / f"forged-rollback-{index}.json").resolve()
        forged.write_bytes(checkpoint)
        with pytest.raises(
            PhpDeploymentError,
            match="recovery report identity is incomplete",
        ):
            php_deploy._commit_durable_report(forged, variant)


def test_bridge_abort_completed_report_rejects_cross_state_evidence(
    local_plan: object,
    tmp_path: Path,
) -> None:
    client, router, deploy_result = _deploy_test_schema_bridge(local_plan)
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    destination = (tmp_path / "bridge-abort.json").resolve()
    php_deploy._prepare_durable_report(
        destination,
        private_root=private_root,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    updater = php_deploy._bridge_report_updater(
        destination,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )
    result = _abort_test_schema_bridge(
        client,
        router,
        local_plan,
        deploy_result,
        bridge_report_update=updater,
    )
    checkpoint = destination.read_bytes()
    variants = (
        {
            **result,
            "candidate_database_schema_version_before": None,
            "database_schema_observation": (
                "unavailable_due_partial_php"
            ),
        },
        {
            **result,
            "emergency_backup_release_id": None,
            "emergency_backup_identity": None,
        },
        {
            **result,
            "initial_php_state": "mixed",
        },
        {
            **result,
            "initial_php_state": "predecessor",
            "candidate_database_schema_version_before": None,
            "database_schema_observation": (
                "unavailable_due_partial_php"
            ),
            "emergency_backup_release_id": None,
            "emergency_backup_identity": None,
        },
    )
    for index, variant in enumerate(variants):
        forged = (tmp_path / f"forged-abort-{index}.json").resolve()
        forged.write_bytes(checkpoint)
        with pytest.raises(
            PhpDeploymentError,
            match="recovery report identity is incomplete",
        ):
            php_deploy._commit_durable_report(forged, variant)


def test_bridge_recovery_report_rejects_minimal_ok_payload(
    local_plan: object,
    tmp_path: Path,
) -> None:
    client, _router, deploy_result = _deploy_test_schema_bridge(local_plan)
    ready = _bridge_ready(client, deploy_result)
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    destination = (tmp_path / "bridge-rollback.json").resolve()
    php_deploy._prepare_durable_report(
        destination,
        private_root=private_root,
        operation="schema-bridge-rollback",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    updater = php_deploy._bridge_report_updater(
        destination,
        operation="schema-bridge-rollback",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
    )
    updater("backup_ready", {"backup_ready": ready})
    updater("commit_started", {})
    updater("restored", {})
    updater("verified", {})

    with pytest.raises(
        PhpDeploymentError,
        match="recovery report identity is incomplete",
    ):
        php_deploy._commit_durable_report(
            destination,
            {
                "ok": True,
                "operation": "schema-bridge-rollback",
                "release_id": RELEASE_ID,
                "candidate_code_revision": RELEASE_SHA,
            },
        )


def test_durable_report_rejects_unconfirmed_private_parent(
    tmp_path: Path,
) -> None:
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    destination = (tmp_path / "deploy-report.json").resolve()
    with pytest.raises(
        PhpDeploymentError,
        match="independently confirmed private root",
    ):
        php_deploy._prepare_durable_report(
            destination,
            private_root=private_root,
            operation="deploy",
            code_revision=RELEASE_SHA,
            release_id=RELEASE_ID,
            environ={
                php_deploy.PRIVATE_REPORT_ROOT_ENV: str(
                    (tmp_path / "other").resolve()
                ),
            },
        )
    assert not destination.exists()


@pytest.mark.skipif(
    os.name == "nt",
    reason="POSIX owner-only mode enforcement",
)
def test_bridge_abort_resume_rechecks_private_root_mode(
    tmp_path: Path,
) -> None:
    private_root = tmp_path.resolve()
    os.chmod(private_root, 0o700)
    destination = (tmp_path / "bridge-abort.json").resolve()
    php_deploy._prepare_durable_report(
        destination,
        private_root=private_root,
        operation="schema-bridge-abort",
        code_revision=RELEASE_SHA,
        release_id=RELEASE_ID,
        environ={
            php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
        },
    )
    takeover = {
        "stale_owner_release_id": RELEASE_ID,
        "stale_owner_state": "owner_present",
        "stale_owner_acquired_at": (
            datetime.now(timezone.utc) - timedelta(minutes=30)
        ).isoformat(),
        "stale_owner_sha256": "a" * 64,
        "writer_absence_evidence": "",
        "writer_absence_nonce": "1" * 32,
        "writer_absence_issued_at": "",
        "replacement_release_id": (
            "schema11-bridge-abort-test-"
            "20260727t000000z-12345678"
        ),
        "database_mutated": False,
    }
    acquired_at = str(takeover["stale_owner_acquired_at"])
    evidence = _stale_writer_absence_evidence(
        owner_content=b"placeholder-owner",
        acquired_at_reference=acquired_at,
    )
    evidence_match = php_deploy.STALE_LOCK_WRITER_ABSENCE_PATTERN.fullmatch(
        evidence
    )
    assert evidence_match is not None
    takeover["stale_owner_sha256"] = evidence_match.group(
        "owner_sha256"
    )
    takeover["writer_absence_evidence"] = evidence
    takeover["writer_absence_issued_at"] = evidence_match.group(
        "issued_at"
    )
    takeover["identity_sha256"] = hashlib.sha256(
        php_deploy._encode_json(takeover)
    ).hexdigest()
    payload = json.loads(destination.read_text(encoding="utf-8"))
    payload["status"] = "stale_lock_takeover_ready"
    payload["stale_lock_takeover"] = takeover
    destination.write_bytes(php_deploy._encode_json(payload))
    os.chmod(destination, 0o600)
    os.chmod(private_root, 0o755)

    with pytest.raises(
        PhpDeploymentError,
        match="must not grant group or other access",
    ):
        php_deploy._prepare_durable_report(
            destination,
            private_root=private_root,
            operation="schema-bridge-abort",
            code_revision=RELEASE_SHA,
            release_id=RELEASE_ID,
            allow_bridge_abort_resume=True,
            environ={
                php_deploy.PRIVATE_REPORT_ROOT_ENV: str(private_root),
            },
        )
