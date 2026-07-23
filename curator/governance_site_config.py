from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Mapping

from .governance_ui import normalize_api_base, normalize_build_sha, normalize_web_base


CONFIG_PREFIX = "window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze("
CONFIG_SUFFIX = ");\n"
CONFIG_KEYS = frozenset({"apiBase", "webBase", "buildSha"})
MAX_CONFIG_BYTES = 16 * 1024


class GovernanceSiteConfigError(ValueError):
    """Raised when a staged or deployed UI config is not the approved release config."""


def _read_config(path: Path) -> tuple[dict[str, str], bytes]:
    if path.is_symlink() or not path.is_file():
        raise GovernanceSiteConfigError(f"config must be a regular file: {path}")
    raw = path.read_bytes()
    if not raw or len(raw) > MAX_CONFIG_BYTES:
        raise GovernanceSiteConfigError(f"config size is invalid: {path}")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise GovernanceSiteConfigError(f"config is not UTF-8: {path}") from exc
    if not text.startswith(CONFIG_PREFIX) or not text.endswith(CONFIG_SUFFIX):
        raise GovernanceSiteConfigError(f"config wrapper is invalid: {path}")
    encoded = text[len(CONFIG_PREFIX) : -len(CONFIG_SUFFIX)]
    try:
        value = json.loads(encoded)
    except json.JSONDecodeError as exc:
        raise GovernanceSiteConfigError(f"config JSON is invalid: {path}") from exc
    if not isinstance(value, dict) or set(value) != CONFIG_KEYS:
        raise GovernanceSiteConfigError(f"config must contain exactly {sorted(CONFIG_KEYS)}: {path}")
    if any(not isinstance(value[key], str) or not value[key] for key in CONFIG_KEYS):
        raise GovernanceSiteConfigError(f"config values must be non-empty strings: {path}")
    return {key: value[key] for key in CONFIG_KEYS}, raw


def verify_governance_site_config(
    site: Path,
    *,
    expected_api_base: str,
    expected_web_base: str,
    expected_build_sha: str,
) -> Mapping[str, str]:
    root = site.resolve()
    if site.is_symlink() or not root.is_dir():
        raise GovernanceSiteConfigError("site must be a regular directory")
    primary, primary_raw = _read_config(root / "config.js")
    nested, nested_raw = _read_config(root / "governance" / "config.js")
    if primary_raw != nested_raw or primary != nested:
        raise GovernanceSiteConfigError("root and /governance config.js must be byte-identical")

    expected = {
        "apiBase": normalize_api_base(expected_api_base),
        "webBase": normalize_web_base(expected_web_base),
        "buildSha": normalize_build_sha(expected_build_sha),
    }
    if len(expected["buildSha"]) != 40:
        raise GovernanceSiteConfigError("release build SHA must be a full 40-character Git SHA")
    if primary["apiBase"] != normalize_api_base(primary["apiBase"]):
        raise GovernanceSiteConfigError("embedded apiBase is not canonical")
    if primary["webBase"] != normalize_web_base(primary["webBase"]):
        raise GovernanceSiteConfigError("embedded webBase is not canonical")
    if primary["buildSha"] != normalize_build_sha(primary["buildSha"]):
        raise GovernanceSiteConfigError("embedded buildSha is not canonical")
    for key, expected_value in expected.items():
        if primary[key] != expected_value:
            raise GovernanceSiteConfigError(
                f"embedded {key} {primary[key]!r} does not match approved {expected_value!r}"
            )
    return primary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify immutable governance Pages config")
    parser.add_argument("--site", required=True)
    parser.add_argument("--expected-api-base", required=True)
    parser.add_argument("--expected-web-base", required=True)
    parser.add_argument("--expected-build-sha", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    config = verify_governance_site_config(
        Path(args.site),
        expected_api_base=args.expected_api_base,
        expected_web_base=args.expected_web_base,
        expected_build_sha=args.expected_build_sha,
    )
    print(
        "Governance site config verified: "
        f"api_base={config['apiBase']}, web_base={config['webBase']}, "
        f"build_sha={config['buildSha']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
