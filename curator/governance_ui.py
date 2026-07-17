from __future__ import annotations

import argparse
import gzip
import json
import os
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_API_BASE = "/api/v1"
HTML_BUDGET_BYTES = 250_000
ASSET_GZIP_BUDGET_BYTES = 250_000


def configured_api_base(explicit: str | None = None) -> str:
    if explicit is not None and explicit.strip():
        return explicit.strip()
    for name in ("GOVERNANCE_API_BASE_URL", "BSIDE_PUBLIC_API_V1_URL", "ACTIVIST_PUBLIC_API_URL"):
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return DEFAULT_API_BASE


def normalize_api_base(value: str) -> str:
    raw = str(value or DEFAULT_API_BASE).strip()
    if not raw:
        raw = DEFAULT_API_BASE
    parsed = urlsplit(raw)
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError("governance API base must not contain credentials, a query, or a fragment")
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        raise ValueError("governance API base must use http or https")
    if parsed.scheme and not parsed.netloc:
        raise ValueError("governance API base requires a host")
    if not parsed.scheme and parsed.netloc:
        raise ValueError("protocol-relative governance API bases are not allowed")
    path = parsed.path.rstrip("/") or ""
    if not path.startswith("/"):
        path = "/" + path
    if not path.endswith("/api/v1"):
        path += "/api/v1"
    normalized = urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
    return normalized or DEFAULT_API_BASE


def config_javascript(api_base: str) -> str:
    payload = json.dumps(
        {"apiBase": normalize_api_base(api_base)},
        ensure_ascii=False,
        separators=(",", ":"),
    ).replace("</", "<\\/").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    return f"window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze({payload});\n"


def asset_budget(governance_dir: Path) -> dict[str, int]:
    index_path = governance_dir / "index.html"
    asset_paths = [governance_dir / "app.js", governance_dir / "styles.css", governance_dir / "config.js"]
    missing = [path.name for path in [index_path, *asset_paths] if not path.is_file()]
    if missing:
        raise FileNotFoundError("missing governance UI assets: " + ", ".join(missing))
    html_bytes = len(index_path.read_bytes())
    gzip_bytes = sum(len(gzip.compress(path.read_bytes(), compresslevel=9)) for path in asset_paths)
    return {"html_bytes": html_bytes, "asset_gzip_bytes": gzip_bytes}


def assert_asset_budget(governance_dir: Path) -> dict[str, int]:
    budget = asset_budget(governance_dir)
    if budget["html_bytes"] > HTML_BUDGET_BYTES:
        raise RuntimeError(f"governance HTML exceeds {HTML_BUDGET_BYTES} bytes")
    if budget["asset_gzip_bytes"] > ASSET_GZIP_BUDGET_BYTES:
        raise RuntimeError(f"governance JS/CSS exceeds {ASSET_GZIP_BUDGET_BYTES} gzip bytes")
    return budget


def build_governance_ui(root: Path = PROJECT_ROOT, api_base: str | None = None) -> dict[str, object]:
    governance_dir = root / "public" / "governance"
    governance_dir.mkdir(parents=True, exist_ok=True)
    normalized = normalize_api_base(configured_api_base(api_base))
    config_path = governance_dir / "config.js"
    config_path.write_text(config_javascript(normalized), encoding="utf-8", newline="\n")
    return {"api_base": normalized, **assert_asset_budget(governance_dir)}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Configure and validate the public governance intelligence UI.")
    parser.add_argument("--root", default=str(PROJECT_ROOT))
    parser.add_argument("--api-base", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = build_governance_ui(Path(args.root).resolve(), args.api_base)
    print("Governance UI ready: " + ", ".join(f"{key}={value}" for key, value in result.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
