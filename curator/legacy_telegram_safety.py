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
TELEGRAM_LITERAL = re.compile(r"telegram", re.IGNORECASE)
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
LEGACY_SEARCH_PATH = "feed/search.html"
LEGACY_SEARCH_FALLBACK = """<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>BSIDE 아카이브 안내</title>
  <style>
    :root { color-scheme: light; font-family: system-ui, sans-serif; }
    body { margin: 0; background: #f6f7f9; color: #111827; }
    main { box-sizing: border-box; max-width: 680px; margin: 12vh auto; padding: 32px; background: #fff; border: 1px solid #e5e7eb; border-radius: 16px; }
    .eyebrow { margin: 0 0 10px; color: #6d28d9; font-weight: 800; letter-spacing: .08em; }
    h1 { margin: 0 0 14px; font-size: clamp(26px, 5vw, 34px); }
    p { margin: 0; color: #4b5563; line-height: 1.65; }
    nav { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 24px; }
    a { min-height: 44px; display: inline-flex; align-items: center; padding: 0 16px; border-radius: 10px; background: #6d28d9; color: #fff; font-weight: 700; text-decoration: none; }
    a + a { background: #ede9fe; color: #4c1d95; }
  </style>
</head>
<body>
  <main>
    <p class="eyebrow">BSIDE</p>
    <h1>검색 화면을 준비 중입니다</h1>
    <p>안전한 이전 버전 복구 중에는 검색 기능을 제공하지 않습니다. 최신 일보와 날짜별 아카이브는 계속 이용할 수 있습니다.</p>
    <nav aria-label="아카이브 이동">
      <a href="latest.html">최신 일보</a>
      <a href="index.html">날짜별 아카이브</a>
      <a href="../index.html">홈</a>
    </nav>
  </main>
</body>
</html>
""".encode("utf-8")
HTML_SCRIPT = re.compile(
    rb"<script\b[^>]*>.*?</script\s*>",
    re.IGNORECASE | re.DOTALL,
)
HTML_ANCHOR = re.compile(
    rb"<a\b[^>]*>.*?</a\s*>",
    re.IGNORECASE | re.DOTALL,
)
HTML_PARAGRAPH = re.compile(
    rb"<p\b[^>]*>.*?</p\s*>",
    re.IGNORECASE | re.DOTALL,
)
HTML_STYLE = re.compile(
    rb"(<style\b[^>]*>)(.*?)(</style\s*>)",
    re.IGNORECASE | re.DOTALL,
)
HREF_ATTRIBUTE = re.compile(
    rb"\bhref\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^\s>]+))",
    re.IGNORECASE,
)
TELEGRAM_CSS_RULE = re.compile(
    rb"(?im)(?:^[ \t]*|(?<=\})[ \t]*)"
    rb"[^{}\r\n]*telegram[^{}\r\n]*\{[^{}]*\}[ \t]*(?:\r?\n|$)?",
)
KNOWN_TELEGRAM_SCRIPT_MARKERS = (
    b"fetchtelegrammentions",
    b"rendertelegrammentions",
    b"statictelegrammentions",
    b"telegramchannelurl",
    b"telegram_reactions",
    b"telegram_messages",
)
KNOWN_TELEGRAM_TEMPLATE_TEXT = frozenset(
    {
        "기사·이슈·telegram 신호를 함께 보려면 별도 검색 화면에서 확인하세요.",
        (
            "뉴스 기사, 이슈 묶음, telegram 공개 채널 신호를 한 화면에서 함께 "
            "확인합니다. 검색 결과는 투자 추천이 아니라 시장 언급과 공개 출처를 "
            "정리한 보조 정보입니다."
        ),
    }
)


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


def _markup_text(payload: bytes) -> str:
    text = _telegram_scan_text(payload)
    text = re.sub(r"<[^>]*>", " ", text)
    return re.sub(r"\s+", " ", text).strip().casefold()


def _is_telegram_page_anchor(payload: bytes) -> bool:
    start_tag = payload.split(b">", 1)[0] + b">"
    match = HREF_ATTRIBUTE.search(start_tag)
    if match is None:
        return False
    raw = next((value for value in match.groups() if value is not None), b"")
    href = html.unescape(raw.decode("utf-8", errors="ignore")).strip()
    path = href.split("#", 1)[0].split("?", 1)[0].replace("\\", "/")
    return path.rstrip("/").rsplit("/", 1)[-1].casefold() in {
        "telegram.html",
        "telegram-admin.html",
    }


def _remove_telegram_scripts(match: re.Match[bytes]) -> bytes:
    payload = match.group(0)
    lowered = payload.lower()
    if b"data-story-telegram-mentions" in lowered:
        return b""
    if any(marker in lowered for marker in KNOWN_TELEGRAM_SCRIPT_MARKERS):
        return b""
    return payload


def _remove_telegram_anchor(match: re.Match[bytes]) -> bytes:
    return b"" if _is_telegram_page_anchor(match.group(0)) else match.group(0)


def _remove_telegram_template_paragraph(match: re.Match[bytes]) -> bytes:
    return (
        b""
        if _markup_text(match.group(0)) in KNOWN_TELEGRAM_TEMPLATE_TEXT
        else match.group(0)
    )


def _remove_telegram_css(match: re.Match[bytes]) -> bytes:
    body = TELEGRAM_CSS_RULE.sub(b"", match.group(2))
    return match.group(1) + body + match.group(3)


def validate_public_payload(payload: bytes, *, path: str) -> None:
    scan_text = _telegram_scan_text(payload)
    if TELEGRAM_URL.search(scan_text):
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
    if TELEGRAM_LITERAL.search(scan_text):
        raise LegacyTelegramSafetyError(
            f"legacy public artifact contains a Telegram literal: {path}"
        )


def redact_telegram_mentions(payload: bytes, *, path: str) -> bytes:
    if path.casefold() == LEGACY_SEARCH_PATH:
        # The historical search application mixes article search with the
        # retired signal dashboard throughout one tightly coupled script.
        # Surgical token removal would leave a misleading or broken public
        # surface, so recovery artifacts replace only this known route with a
        # deterministic, data-free compatibility page.
        validate_public_payload(LEGACY_SEARCH_FALLBACK, path=path)
        return LEGACY_SEARCH_FALLBACK
    redacted = payload
    if path.casefold().endswith(".html"):
        # Compatibility pages are deliberately static. Removing a known
        # Telegram-coupled script as a complete DOM node avoids leaving
        # syntactically broken helpers or hidden network paths behind.
        redacted = HTML_SCRIPT.sub(_remove_telegram_scripts, redacted)
        redacted = HTML_ANCHOR.sub(_remove_telegram_anchor, redacted)
        redacted = HTML_PARAGRAPH.sub(
            _remove_telegram_template_paragraph,
            redacted,
        )
        redacted = HTML_STYLE.sub(_remove_telegram_css, redacted)
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
        "telegram_literal_count": 0,
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
