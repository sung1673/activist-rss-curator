from __future__ import annotations

import json

from conftest import make_article


def test_resend_last_digest_uses_record_window_and_clusters(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_resend_digest

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test-channel")

    article = make_article("소액주주 주주제안 브리핑", "https://example.com/a")
    cluster = {
        "guid": "cluster:test:20260430:1",
        "representative_title": "소액주주 주주제안",
        "published_at": article["published_at"],
        "articles": [article],
    }
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "state.json").write_text(
        json.dumps(
            {
                "published_clusters": [cluster],
                "telegram_digest_records": [
                    {
                        "sent_at": "2026-04-30T20:31:00+09:00",
                        "window_start": "2026-04-30T20:00:00+09:00",
                        "window_end": "2026-04-30T20:30:00+09:00",
                        "cluster_guids": ["cluster:test:20260430:1"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    built: list[str] = []

    def fake_build(clusters, _config, now, start_at):  # type: ignore[no-untyped-def]
        built.append(f"{start_at.isoformat()}|{now.isoformat()}|{clusters[0]['guid']}")
        return ["rebuilt message"]

    def fake_send(_bot_token, _chat_id, text, _config, **_kwargs):  # type: ignore[no-untyped-def]
        built.append(text)
        return {"ok": True, "message_id": 99}

    monkeypatch.setattr(telegram_resend_digest, "build_hourly_update_messages", fake_build)
    monkeypatch.setattr(telegram_resend_digest, "send_telegram_message", fake_send)

    assert telegram_resend_digest.resend_last_digest(tmp_path) == {
        "telegram_digest_resend_sent": 1,
        "telegram_digest_resend_failed": 0,
    }
    assert built == [
        "2026-04-30T20:00:00+09:00|2026-04-30T20:30:00+09:00|cluster:test:20260430:1",
        "rebuilt message",
    ]
