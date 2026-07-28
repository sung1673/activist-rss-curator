from __future__ import annotations

from pathlib import Path

import pytest

from curator.legacy_telegram_safety import (
    LegacyTelegramSafetyError,
    redact_telegram_mentions,
    validate_public_payload,
    verify_public_site,
)


def _html(body: str) -> bytes:
    return f"<!doctype html><html><body>{body}</body></html>".encode()


def test_redaction_is_deterministic_and_preserves_only_an_empty_payload() -> None:
    source = _html(
        '<script type="application/json" data-story-telegram-mentions>'
        '[{"message_url":"https://t.me/private/42","text":"signal"}]'
        "</script>"
        "<script>"
        "function telegramChannelUrl(handle) {"
        "return handle ? `https://t.me/${handle}` : '';"
        "}"
        "</script>"
    )

    first = redact_telegram_mentions(source, path="feed/2026-07-29.html")
    second = redact_telegram_mentions(source, path="feed/2026-07-29.html")

    assert first == second
    assert b"https://t.me/" not in first
    assert b"return handle ? '' : '';" in first
    assert b"data-story-telegram-mentions>[]</script>" in first
    validate_public_payload(first, path="feed/2026-07-29.html")


def test_redaction_rejects_an_unknown_telegram_template() -> None:
    source = _html(
        "<script>"
        "function unexpected(value) {"
        "return `https://t.me/${value}`;"
        "}"
        "</script>"
    )

    with pytest.raises(LegacyTelegramSafetyError, match="Telegram URL"):
        redact_telegram_mentions(source, path="feed/2026-07-29.html")


@pytest.mark.parametrize(
    "payload",
    [
        _html(
            "<script data-story-telegram-mentions>"
            '[{"channel":"private"}]'
            "</script>"
        ),
        _html('<a href="https:\\/\\/t.me\\/private\\/42">signal</a>'),
        _html('<a href="//t.me/private/42">signal</a>'),
        _html('<a href="https:&#47;&#47;t.me&#47;private&#47;42">signal</a>'),
        _html(
            '<script>const url="\\u0068\\u0074\\u0074\\u0070\\u0073'
            ':\\u002f\\u002ft.me\\u002fprivate\\u002f42";</script>'
        ),
        _html('<a href="https://t.me?domain=private">signal</a>'),
        _html('<a href="https://t.me:443/private/42">signal</a>'),
        _html('<a href="tg://resolve?domain=private">signal</a>'),
        _html('<a href="https://telegram.me/private/42">signal</a>'),
        _html("<script data-story-telegram-mentions>["),
    ],
)
def test_validator_rejects_nonempty_malformed_or_linked_telegram_content(
    payload: bytes,
) -> None:
    with pytest.raises(LegacyTelegramSafetyError, match="Telegram"):
        validate_public_payload(payload, path="feed/2026-07-29.html")


def test_site_verifier_rejects_forbidden_pages_and_preserves_dated_count(
    tmp_path: Path,
) -> None:
    site = tmp_path / "site"
    feed = site / "feed"
    feed.mkdir(parents=True)
    (site / "index.html").write_bytes(_html("root"))
    (site / "feed.xml").write_text("<rss/>", encoding="utf-8")
    (feed / "2026-07-29.html").write_bytes(_html("dated"))

    result = verify_public_site(site)
    assert result["dated_report_count"] == 1
    assert result["telegram_url_count"] == 0

    (feed / "telegram.html").write_bytes(_html("forbidden"))
    with pytest.raises(LegacyTelegramSafetyError, match="forbidden Telegram page"):
        verify_public_site(site)
