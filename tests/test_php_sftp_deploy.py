from __future__ import annotations

import argparse
import hashlib
import io
import json
import posixpath
import re
import stat
import sys
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote, urlsplit

import pytest

import curator.php_sftp_deploy as php_deploy
from curator.deployment_manifest import CORE_API_FILES, write_deployment_manifest
from curator.mysql_backup import legacy_ssh_rsa_sha1_is_allowed
from curator.php_sftp_deploy import (
    DEFAULT_COMMIT_ORDER,
    DEFAULT_REMOTE_ROOT,
    DEPLOYMENT_MANIFEST_NAME,
    HttpResponse,
    ParamikoPinnedSftpSession,
    PhpDeploymentError,
    build_local_deployment_plan,
    deploy_release,
    inspect_remote_deployment,
    load_remote_backup,
    local_plan_report,
    main,
    reset_opcache_with_ephemeral_probe,
    rollback_release,
    ssh_sftp_options_from_args,
    verify_closed_v2_api,
)


RELEASE_SHA = "a" * 40
RELEASE_ID = "php-v2-aaaaaaaaaaaa-20260725t000000z-12345678"
PUBLIC_ROOT = "https://alignpe.gabia.io/activist"
API_V2 = PUBLIC_ROOT + "/api.php/api/v2"
ROLLBACK_HEALTH = PUBLIC_ROOT + "/api.php?action=health"
PROTECTED_TOKEN = "ops-protected-token-" + "z" * 40


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
        self.symlinks: set[str] = set()
        self.mutations = 0

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

    def add_file(self, path: str, content: bytes, mode: int = 0o644) -> None:
        normalized = self._path(path)
        assert posixpath.dirname(normalized) in self.directories
        self.files[normalized] = content
        self.modes[normalized] = mode

    def lstat(self, path: str) -> object:
        normalized = self._path(path)
        if normalized in self.symlinks:
            return SimpleNamespace(st_mode=stat.S_IFLNK | 0o777, st_size=0)
        if normalized in self.files:
            return SimpleNamespace(
                st_mode=stat.S_IFREG | self.modes[normalized],
                st_size=len(self.files[normalized]),
            )
        if normalized in self.directories:
            return SimpleNamespace(
                st_mode=stat.S_IFDIR | self.directories[normalized],
                st_size=0,
            )
        raise FileNotFoundError(2, "not found")

    def mkdir(self, path: str, mode: int = 0o777) -> None:
        normalized = self._path(path)
        if (
            normalized in self.files
            or normalized in self.directories
            or normalized in self.symlinks
        ):
            raise FileExistsError(17, "exists")
        if posixpath.dirname(normalized) not in self.directories:
            raise FileNotFoundError(2, "parent missing")
        self.directories[normalized] = mode
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
            self.files[normalized] = content
            self.modes.setdefault(normalized, 0o666)
            self.mutations += 1

        return _Writer(commit)

    def remove(self, path: str) -> None:
        normalized = self._path(path)
        if normalized not in self.files:
            raise FileNotFoundError(2, "not found")
        del self.files[normalized]
        self.modes.pop(normalized, None)
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
        self.mutations += 1

    def close(self) -> None:
        return


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
    ) -> None:
        self.sftp = sftp
        self.code_revision = code_revision
        self.fail_v2_health = fail_v2_health
        self.fail_probe = fail_probe
        self.strip_authorization = strip_authorization
        self.protected_release_state = protected_release_state
        self.private_canary_mode = private_canary_mode
        self.public_canary_mode = public_canary_mode
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
            return self._json(
                200,
                {
                    "ok": True,
                    "opcache_reset": True,
                    "probe_id": match.group(1),
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
                    "schema_version": 11,
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
                body=b"openapi: 3.1.0\nx-schema-version: 11\n",
            )
        if url == API_V2 + "/__bside_sftp_deploy_not_found__":
            return self._json(
                404,
                {"ok": False, "error": "not_found", "api_version": "v2"},
            )
        if url == API_V2 + "/events?limit=1":
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


def test_local_plan_attests_all_eight_core_files_and_commits_manifest_last(
    local_plan: object,
) -> None:
    report = local_plan_report(local_plan)
    paths = [item["path"] for item in report["files"]]  # type: ignore[index]

    assert set(CORE_API_FILES) == set(DEFAULT_COMMIT_ORDER[:-1])
    assert len(CORE_API_FILES) == 8
    assert paths == list(DEFAULT_COMMIT_ORDER)
    assert paths[-2:] == ["api.php", DEPLOYMENT_MANIFEST_NAME]
    assert report["mutated_remote"] is False


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
