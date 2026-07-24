from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path

from .governance_ui import assert_asset_budget, config_javascript, configured_api_base, normalize_api_base
from .legacy_feed_compat import MANIFEST_NAME, verify_legacy_feed_compatibility


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PUBLIC_ASSETS = ("index.html", "app.js", "styles.css")
ROOT_COMPAT_ASSETS = ("CNAME", "404.html", "feed.xml", MANIFEST_NAME)
DENIED_NAME_PARTS = ("telegram", "story-review", "admin")
DATED_FEED_ASSET = re.compile(r"^\d{4}-\d{2}-\d{2}\.html$")


def _require_regular_file(path: Path, *, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"{label} must be a regular file: {path}")


def _safe_copy(source: Path, destination: Path) -> None:
    _require_regular_file(source, label="site asset")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)


def _copy_legacy_compatibility(legacy_root: Path, output: Path) -> tuple[int, dict[str, object]]:
    manifest = verify_legacy_feed_compatibility(legacy_root)
    copied = 0
    for name in ROOT_COMPAT_ASSETS:
        source = legacy_root / name
        if source.exists():
            _safe_copy(source, output / name)
            copied += 1

    feed_root = legacy_root / "feed"
    if not feed_root.exists():
        raise RuntimeError("verified legacy compatibility feed directory disappeared")
    if feed_root.is_symlink() or not feed_root.is_dir():
        raise RuntimeError("legacy feed must be a regular directory")
    for source in sorted(feed_root.iterdir()):
        lowered = source.name.casefold()
        if any(part in lowered for part in DENIED_NAME_PARTS):
            raise RuntimeError(f"denied legacy asset: {source.name}")
        if source.is_dir() or not DATED_FEED_ASSET.fullmatch(source.name):
            continue
        _safe_copy(source, output / "feed" / source.name)
        copied += 1
    return copied, manifest


def _validate_output(output: Path) -> None:
    for path in output.rglob("*"):
        if path.is_symlink():
            raise RuntimeError(f"site staging contains a symbolic link: {path}")
        if path.is_file():
            relative = path.relative_to(output).as_posix().casefold()
            if any(part in relative for part in DENIED_NAME_PARTS):
                raise RuntimeError(f"site staging contains a denied public asset: {relative}")


def build_governance_site(
    root: Path = PROJECT_ROOT,
    *,
    output: Path,
    api_base: str | None = None,
    legacy_root: Path | None = None,
) -> dict[str, object]:
    source = root / "public" / "governance"
    for name in PUBLIC_ASSETS:
        _require_regular_file(source / name, label="governance source asset")

    resolved_output = output.resolve()
    resolved_root = root.resolve()
    if resolved_output == resolved_root or resolved_output == (resolved_root / "public"):
        raise RuntimeError("governance site output must be a dedicated staging directory")
    if resolved_output.exists():
        shutil.rmtree(resolved_output)
    resolved_output.mkdir(parents=True)

    normalized_api_base = normalize_api_base(configured_api_base(api_base))
    config = config_javascript(normalized_api_base)
    for prefix in (Path(), Path("governance")):
        target = resolved_output / prefix
        target.mkdir(parents=True, exist_ok=True)
        for name in PUBLIC_ASSETS:
            _safe_copy(source / name, target / name)
        (target / "config.js").write_text(config, encoding="utf-8", newline="\n")
        assert_asset_budget(target)

    compatibility_count = 0
    compatibility_manifest: dict[str, object] | None = None
    if legacy_root is not None:
        compatibility_source = legacy_root
        if not compatibility_source.exists():
            raise RuntimeError("legacy compatibility source is missing")
        compatibility_count, compatibility_manifest = _copy_legacy_compatibility(
            compatibility_source, resolved_output
        )

    _validate_output(resolved_output)
    files = [path for path in resolved_output.rglob("*") if path.is_file()]
    result: dict[str, object] = {
        "api_base": normalized_api_base,
        "output": str(resolved_output),
        "file_count": len(files),
        "compatibility_file_count": compatibility_count,
    }
    if compatibility_manifest is not None:
        result.update(
            {
                "compatibility_window_start": compatibility_manifest["window_start"],
                "compatibility_window_end": compatibility_manifest["window_end"],
                "compatibility_report_count": compatibility_manifest["dated_report_count"],
            }
        )
    return result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a clean governance-only GitHub Pages staging directory.")
    parser.add_argument("--root", default=str(PROJECT_ROOT))
    parser.add_argument("--output", required=True)
    parser.add_argument("--api-base", default=None)
    parser.add_argument("--legacy-root", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = build_governance_site(
        Path(args.root).resolve(),
        output=Path(args.output),
        api_base=args.api_base,
        legacy_root=Path(args.legacy_root) if args.legacy_root else None,
    )
    print("Governance site staged: " + ", ".join(f"{key}={value}" for key, value in result.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
