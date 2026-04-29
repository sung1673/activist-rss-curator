from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo


def test_digest_preview_includes_duplicate_records_without_clusters(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import digest_preview, summaries

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test-channel")
    monkeypatch.setenv("DIGEST_PREVIEW_HOURS", "24")
    monkeypatch.setattr(
        digest_preview,
        "now_in_timezone",
        lambda _tz: datetime(2026, 4, 26, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )
    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 주주제안 이슈 지속")

    sent_messages = []

    def fake_send(_bot_token, _chat_id, text, _config):  # type: ignore[no-untyped-def]
        sent_messages.append(text)
        return {"ok": True}

    monkeypatch.setattr(digest_preview, "send_telegram_message", fake_send)
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "state.json").write_text(
        json.dumps(
            {
                "published_clusters": [],
                "pending_clusters": [],
                "articles": [
                    {
                        "status": "duplicate",
                        "title": "주주제안 관련 중복 기사",
                        "canonical_url": "https://example.com/duplicate",
                        "published_at": "2026-04-26T09:30:00+09:00",
                        "seen_at": "2026-04-26T09:35:00+09:00",
                        "duplicate_matches": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    assert digest_preview.send_digest_preview(tmp_path) == {
        "digest_preview_sent": 1,
        "digest_preview_failed": 0,
    }
    assert 'href="https://example.com/duplicate"' in sent_messages[0]
