from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Mapping, cast


SCHEMA_VERSION = 1
BINDING_KIND = "bside-global-alpha-pages-artifact-binding"
CONTENT_KIND = "bside-global-alpha-pages-content-identity"
PRODUCER_WORKFLOW = ".github/workflows/daily.yml"
CONTENT_ALGORITHM = "sha256-canonical-file-manifest-v1"
TERMINAL_ASSETS = ("app.js", "config.js", "index.html", "styles.css")
MAX_TERMINAL_ASSET_BYTES = 250_000
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ARTIFACT_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class PagesArtifactIdentityError(ValueError):
    """Raised when a Pages artifact cannot be bound to observed terminal bytes."""


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _file_entry(path: str, content: bytes) -> dict[str, object]:
    if not path or path.startswith("/") or "\\" in path or ".." in path.split("/"):
        raise PagesArtifactIdentityError(f"unsafe content identity path: {path!r}")
    return {
        "path": path,
        "byte_length": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def _manifest_digest(entries: list[dict[str, object]]) -> str:
    return hashlib.sha256(
        _canonical_json(
            {
                "algorithm": CONTENT_ALGORITHM,
                "files": entries,
            }
        )
    ).hexdigest()


def build_terminal_content_identity(
    assets: Mapping[str, bytes],
) -> dict[str, object]:
    if set(assets) != set(TERMINAL_ASSETS):
        raise PagesArtifactIdentityError(
            f"terminal assets must be exactly {list(TERMINAL_ASSETS)}"
        )
    entries: list[dict[str, object]] = []
    for name in sorted(TERMINAL_ASSETS):
        content = assets[name]
        if not isinstance(content, bytes):
            raise PagesArtifactIdentityError(f"terminal asset is not bytes: {name}")
        if not content or len(content) > MAX_TERMINAL_ASSET_BYTES:
            raise PagesArtifactIdentityError(
                f"terminal asset size is invalid: {name}"
            )
        entries.append(_file_entry(name, content))
    return {
        "algorithm": CONTENT_ALGORITHM,
        "file_count": len(entries),
        "total_bytes": sum(
            cast(int, entry["byte_length"]) for entry in entries
        ),
        "sha256": _manifest_digest(entries),
        "files": entries,
    }


def _site_files(root: Path) -> list[tuple[str, bytes]]:
    resolved = root.resolve()
    if root.is_symlink() or not resolved.is_dir():
        raise PagesArtifactIdentityError("Pages site must be a regular directory")
    files: list[tuple[str, bytes]] = []
    candidates = sorted(
        resolved.rglob("*"),
        key=lambda candidate: candidate.relative_to(resolved).as_posix(),
    )
    casefolded_paths: set[str] = set()
    for candidate in candidates:
        if candidate.is_symlink():
            raise PagesArtifactIdentityError(
                f"Pages site contains a symbolic link: {candidate}"
            )
        if candidate.is_dir():
            continue
        if not candidate.is_file():
            raise PagesArtifactIdentityError(
                f"Pages site contains a non-regular entry: {candidate}"
            )
        relative = candidate.relative_to(resolved).as_posix()
        folded = relative.casefold()
        if folded in casefolded_paths:
            raise PagesArtifactIdentityError(
                f"Pages site contains a case-colliding path: {relative}"
            )
        casefolded_paths.add(folded)
        files.append((relative, candidate.read_bytes()))
    if not files:
        raise PagesArtifactIdentityError("Pages site is empty")
    return files


def build_pages_content_identity(root: Path) -> dict[str, object]:
    site_files = _site_files(root)
    site_entries = [_file_entry(path, content) for path, content in site_files]
    by_path = dict(site_files)
    terminal_assets: dict[str, bytes] = {}
    for name in TERMINAL_ASSETS:
        root_path = name
        nested_path = f"governance/{name}"
        if root_path not in by_path or nested_path not in by_path:
            raise PagesArtifactIdentityError(
                f"Pages site is missing root or governance terminal asset: {name}"
            )
        if by_path[root_path] != by_path[nested_path]:
            raise PagesArtifactIdentityError(
                f"root and governance terminal assets differ: {name}"
            )
        terminal_assets[name] = by_path[nested_path]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": CONTENT_KIND,
        "algorithm": CONTENT_ALGORITHM,
        "site": {
            "file_count": len(site_entries),
            "total_bytes": sum(
                cast(int, entry["byte_length"]) for entry in site_entries
            ),
            "sha256": _manifest_digest(site_entries),
        },
        "terminal": build_terminal_content_identity(terminal_assets),
    }


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise PagesArtifactIdentityError(f"{field} must be a positive integer")
    return value


def validate_terminal_content_identity(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise PagesArtifactIdentityError("terminal content identity must be an object")
    expected_keys = {
        "algorithm",
        "file_count",
        "total_bytes",
        "sha256",
        "files",
    }
    if set(value) != expected_keys or value.get("algorithm") != CONTENT_ALGORITHM:
        raise PagesArtifactIdentityError("terminal content identity contract mismatch")
    if value.get("file_count") != len(TERMINAL_ASSETS):
        raise PagesArtifactIdentityError("terminal file_count is invalid")
    total_bytes = value.get("total_bytes")
    if isinstance(total_bytes, bool) or not isinstance(total_bytes, int) or total_bytes < 1:
        raise PagesArtifactIdentityError("terminal total_bytes is invalid")
    digest = str(value.get("sha256") or "").casefold()
    if SHA256_RE.fullmatch(digest) is None:
        raise PagesArtifactIdentityError("terminal sha256 is invalid")
    raw_files = value.get("files")
    if not isinstance(raw_files, list) or len(raw_files) != len(TERMINAL_ASSETS):
        raise PagesArtifactIdentityError("terminal files are invalid")
    entries: list[dict[str, object]] = []
    for index, raw_entry in enumerate(raw_files):
        if not isinstance(raw_entry, dict) or set(raw_entry) != {
            "path",
            "byte_length",
            "sha256",
        }:
            raise PagesArtifactIdentityError(
                f"terminal files[{index}] contract mismatch"
            )
        path = raw_entry.get("path")
        size = raw_entry.get("byte_length")
        file_digest = str(raw_entry.get("sha256") or "").casefold()
        if path != sorted(TERMINAL_ASSETS)[index]:
            raise PagesArtifactIdentityError(
                f"terminal files[{index}].path is invalid"
            )
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or size < 1
            or size > MAX_TERMINAL_ASSET_BYTES
        ):
            raise PagesArtifactIdentityError(
                f"terminal files[{index}].byte_length is invalid"
            )
        if SHA256_RE.fullmatch(file_digest) is None:
            raise PagesArtifactIdentityError(
                f"terminal files[{index}].sha256 is invalid"
            )
        entries.append(
            {"path": path, "byte_length": size, "sha256": file_digest}
        )
    if sum(cast(int, entry["byte_length"]) for entry in entries) != total_bytes:
        raise PagesArtifactIdentityError("terminal total_bytes does not match files")
    if _manifest_digest(entries) != digest:
        raise PagesArtifactIdentityError("terminal sha256 does not match files")
    return {
        "algorithm": CONTENT_ALGORITHM,
        "file_count": len(entries),
        "total_bytes": total_bytes,
        "sha256": digest,
        "files": entries,
    }


def validate_pages_content_identity(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise PagesArtifactIdentityError("Pages content identity must be an object")
    if set(value) != {
        "schema_version",
        "kind",
        "algorithm",
        "site",
        "terminal",
    }:
        raise PagesArtifactIdentityError("Pages content identity contract mismatch")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise PagesArtifactIdentityError("Pages content schema_version is invalid")
    if value.get("kind") != CONTENT_KIND:
        raise PagesArtifactIdentityError("Pages content kind is invalid")
    if value.get("algorithm") != CONTENT_ALGORITHM:
        raise PagesArtifactIdentityError("Pages content algorithm is invalid")
    site = value.get("site")
    if not isinstance(site, dict) or set(site) != {
        "file_count",
        "total_bytes",
        "sha256",
    }:
        raise PagesArtifactIdentityError("site content identity contract mismatch")
    file_count = _positive_integer(site.get("file_count"), "site.file_count")
    total_bytes = _positive_integer(site.get("total_bytes"), "site.total_bytes")
    site_digest = str(site.get("sha256") or "").casefold()
    if SHA256_RE.fullmatch(site_digest) is None:
        raise PagesArtifactIdentityError("site.sha256 is invalid")
    terminal = validate_terminal_content_identity(value.get("terminal"))
    if file_count < cast(int, terminal["file_count"]):
        raise PagesArtifactIdentityError("site file_count is smaller than terminal")
    if total_bytes < cast(int, terminal["total_bytes"]):
        raise PagesArtifactIdentityError("site total_bytes is smaller than terminal")
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": CONTENT_KIND,
        "algorithm": CONTENT_ALGORITHM,
        "site": {
            "file_count": file_count,
            "total_bytes": total_bytes,
            "sha256": site_digest,
        },
        "terminal": terminal,
    }


def build_pages_artifact_binding(
    root: Path,
    *,
    code_revision: str,
    producer_run_id: int,
    producer_run_attempt: int,
    artifact_id: int,
    artifact_name: str,
    artifact_digest: str,
) -> dict[str, object]:
    revision = code_revision.strip().casefold()
    digest = artifact_digest.strip().casefold()
    run_id = _positive_integer(producer_run_id, "producer_run_id")
    run_attempt = _positive_integer(producer_run_attempt, "producer_run_attempt")
    resolved_artifact_id = _positive_integer(artifact_id, "artifact_id")
    if SHA_RE.fullmatch(revision) is None:
        raise PagesArtifactIdentityError("code_revision is invalid")
    if ARTIFACT_DIGEST_RE.fullmatch(digest) is None:
        raise PagesArtifactIdentityError("artifact_digest is invalid")
    expected_name = f"pages-{run_id}-{run_attempt}"
    if artifact_name != expected_name:
        raise PagesArtifactIdentityError(
            f"artifact_name must be exactly {expected_name}"
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": BINDING_KIND,
        "code_revision": revision,
        "producer_workflow": PRODUCER_WORKFLOW,
        "producer_run_id": run_id,
        "producer_run_attempt": run_attempt,
        "artifact_id": resolved_artifact_id,
        "artifact_name": artifact_name,
        "artifact_digest": digest,
        "content_identity": build_pages_content_identity(root),
    }


def validate_pages_artifact_binding(
    value: object,
    *,
    expected_revision: str | None = None,
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise PagesArtifactIdentityError("Pages artifact binding must be an object")
    if set(value) != {
        "schema_version",
        "kind",
        "code_revision",
        "producer_workflow",
        "producer_run_id",
        "producer_run_attempt",
        "artifact_id",
        "artifact_name",
        "artifact_digest",
        "content_identity",
    }:
        raise PagesArtifactIdentityError("Pages artifact binding contract mismatch")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise PagesArtifactIdentityError("Pages artifact schema_version is invalid")
    if value.get("kind") != BINDING_KIND:
        raise PagesArtifactIdentityError("Pages artifact kind is invalid")
    if value.get("producer_workflow") != PRODUCER_WORKFLOW:
        raise PagesArtifactIdentityError("Pages artifact producer_workflow is invalid")
    revision = str(value.get("code_revision") or "").casefold()
    if SHA_RE.fullmatch(revision) is None:
        raise PagesArtifactIdentityError("Pages artifact code_revision is invalid")
    if expected_revision is not None and revision != expected_revision.casefold():
        raise PagesArtifactIdentityError(
            "Pages artifact code_revision does not match release candidate"
        )
    run_id = _positive_integer(value.get("producer_run_id"), "producer_run_id")
    run_attempt = _positive_integer(
        value.get("producer_run_attempt"), "producer_run_attempt"
    )
    artifact_id = _positive_integer(value.get("artifact_id"), "artifact_id")
    artifact_name = str(value.get("artifact_name") or "")
    if artifact_name != f"pages-{run_id}-{run_attempt}":
        raise PagesArtifactIdentityError("Pages artifact name is not run-bound")
    artifact_digest = str(value.get("artifact_digest") or "").casefold()
    if ARTIFACT_DIGEST_RE.fullmatch(artifact_digest) is None:
        raise PagesArtifactIdentityError("Pages artifact digest is invalid")
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": BINDING_KIND,
        "code_revision": revision,
        "producer_workflow": PRODUCER_WORKFLOW,
        "producer_run_id": run_id,
        "producer_run_attempt": run_attempt,
        "artifact_id": artifact_id,
        "artifact_name": artifact_name,
        "artifact_digest": artifact_digest,
        "content_identity": validate_pages_content_identity(
            value.get("content_identity")
        ),
    }


def _load_binding(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PagesArtifactIdentityError("Pages artifact binding is invalid JSON") from exc
    return validate_pages_artifact_binding(value)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bind an immutable daily Pages artifact to observed terminal bytes"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create")
    create.add_argument("--site", type=Path, required=True)
    create.add_argument("--code-revision", required=True)
    create.add_argument("--producer-run-id", type=int, required=True)
    create.add_argument("--producer-run-attempt", type=int, required=True)
    create.add_argument("--artifact-id", type=int, required=True)
    create.add_argument("--artifact-name", required=True)
    create.add_argument("--artifact-digest", required=True)
    create.add_argument("--output", type=Path, required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--site", type=Path, required=True)
    verify.add_argument("--binding", type=Path, required=True)
    verify.add_argument("--expected-revision", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.command == "create":
        binding = build_pages_artifact_binding(
            args.site,
            code_revision=args.code_revision,
            producer_run_id=args.producer_run_id,
            producer_run_attempt=args.producer_run_attempt,
            artifact_id=args.artifact_id,
            artifact_name=args.artifact_name,
            artifact_digest=args.artifact_digest,
        )
        _write_json(args.output, binding)
        return 0
    binding = validate_pages_artifact_binding(
        _load_binding(args.binding),
        expected_revision=args.expected_revision,
    )
    content = build_pages_content_identity(args.site)
    if content != binding["content_identity"]:
        raise PagesArtifactIdentityError(
            "downloaded Pages contents do not match the evidence-bound artifact"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
