from __future__ import annotations

import argparse
import html
import json
import re
import sys
from pathlib import Path
from typing import Any


TELEGRAM_PAGE_PATHS = frozenset(
    {
        "feed/telegram-admin.html",
        "feed/telegram.html",
    }
)
DATED_REPORT = re.compile(r"^feed/\d{4}-\d{2}-\d{2}\.html$")
TELEGRAM_URL = re.compile(
    r"(?:(?:https?:)?//(?:www\.)?(?:t\.me|telegram\.me)(?:[:/\\?#]|$))"
    r"|(?:tg://)",
    re.IGNORECASE,
)
TELEGRAM_MENTIONS_OPEN = re.compile(
    rb"<script\b(?=[^>]*\bdata-story-telegram-mentions\b)[^>]*>",
    re.IGNORECASE,
)
TELEGRAM_MENTIONS_SCRIPT = re.compile(
    rb"(<script\b(?=[^>]*\bdata-story-telegram-mentions\b)[^>]*>)"
    rb"(.*?)"
    rb"(</script\s*>)",
    re.IGNORECASE | re.DOTALL,
)
LEGACY_TELEGRAM_CHANNEL_TEMPLATE = b"`https://t.me/${handle}`"


class LegacyTelegramSafetyError(RuntimeError):
    """A safe-to-print legacy Telegram exposure validation failure."""


def _telegram_scan_text(payload: bytes) -> str:
    text = payload.decode("utf-8", errors="ignore")
    for _ in range(2):
        text = html.unescape(text)
        text = re.sub(
            r"\\u([0-9a-fA-F]{4})",
            lambda match: (
                chr(int(match.group(1), 16))
                if int(match.group(1), 16) <= 0x7F
                else match.group(0)
            ),
            text,
        )
        text = text.replace("\\/", "/")
    return text


def validate_public_payload(payload: bytes, *, path: str) -> None:
    if TELEGRAM_URL.search(_telegram_scan_text(payload)):
        raise LegacyTelegramSafetyError(
            f"legacy public artifact contains a Telegram URL: {path}"
        )
    if not path.casefold().endswith(".html"):
        return
    scripts = list(TELEGRAM_MENTIONS_SCRIPT.finditer(payload))
    if len(list(TELEGRAM_MENTIONS_OPEN.finditer(payload))) != len(scripts):
        raise LegacyTelegramSafetyError(
            f"legacy public artifact contains malformed Telegram mention data: {path}"
        )
    for match in scripts:
        if match.group(2).strip() != b"[]":
            raise LegacyTelegramSafetyError(
                f"legacy public artifact contains Telegram mention data: {path}"
            )


def redact_telegram_mentions(payload: bytes, *, path: str) -> bytes:
    redacted = TELEGRAM_MENTIONS_SCRIPT.sub(
        lambda match: match.group(1) + b"[]" + match.group(3),
        payload,
    )
    # Historical dated reports include this inert helper even after their
    # mention payload is emptied. Neutralize only the exact known template;
    # every other Telegram URL remains a fail-closed validation error.
    redacted = redacted.replace(LEGACY_TELEGRAM_CHANNEL_TEMPLATE, b"''")
    validate_public_payload(redacted, path=path)
    return redacted


def verify_public_site(
    root: Path,
    *,
    minimum_dated_reports: int = 1,
) -> dict[str, Any]:
    if minimum_dated_reports < 1:
        raise LegacyTelegramSafetyError(
            "minimum dated report count must be positive"
        )
    if root.is_symlink() or not root.is_dir():
        raise LegacyTelegramSafetyError(
            "legacy public site must be a regular directory"
        )
    resolved_root = root.resolve()
    files = 0
    dated_reports = 0
    for candidate in resolved_root.rglob("*"):
        if candidate.is_symlink():
            raise LegacyTelegramSafetyError(
                "legacy public site contains a symbolic link"
            )
        if candidate.is_dir():
            continue
        if not candidate.is_file():
            raise LegacyTelegramSafetyError(
                "legacy public site contains a non-regular file"
            )
        resolved = candidate.resolve()
        try:
            relative = resolved.relative_to(resolved_root).as_posix()
        except ValueError as exc:
            raise LegacyTelegramSafetyError(
                "legacy public site path escaped its root"
            ) from exc
        if relative in TELEGRAM_PAGE_PATHS:
            raise LegacyTelegramSafetyError(
                f"legacy public site contains a forbidden Telegram page: {relative}"
            )
        payload = candidate.read_bytes()
        validate_public_payload(payload, path=relative)
        files += 1
        if DATED_REPORT.fullmatch(relative):
            dated_reports += 1
    if dated_reports < minimum_dated_reports:
        raise LegacyTelegramSafetyError(
            "legacy public site does not contain the required dated reports"
        )
    return {
        "schema_version": 1,
        "kind": "bside-legacy-telegram-exposure-check",
        "file_count": files,
        "dated_report_count": dated_reports,
        "forbidden_page_count": 0,
        "telegram_url_count": 0,
        "nonempty_mention_payload_count": 0,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify that a legacy public site contains no Telegram exposure"
    )
    parser.add_argument("command", choices=("verify-site",))
    parser.add_argument("--site", type=Path, required=True)
    parser.add_argument("--minimum-dated-reports", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        result = verify_public_site(
            args.site,
            minimum_dated_reports=args.minimum_dated_reports,
        )
    except LegacyTelegramSafetyError as exc:
        print(f"legacy_telegram_safety_error={exc}", file=sys.stderr)
        return 1
    print(
        "legacy_telegram_safety="
        + json.dumps(result, separators=(",", ":"), sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
