from __future__ import annotations

from curator.telegram_dashboard import write_telegram_dashboard


def test_telegram_dashboard_writes_public_safe_status_page(tmp_path, config, now) -> None:  # type: ignore[no-untyped-def]
    state = {
        "telegram_source_channels": [
            {
                "handle": "marketnews",
                "title": "경제 증권 뉴스",
                "enabled": True,
                "source_type": "public_channel",
                "is_public_channel": True,
                "quality_score": 86,
            }
        ],
        "telegram_source_messages": [
            {
                "handle": "marketnews",
                "channel_title": "경제 증권 뉴스",
                "telegram_message_id": 10,
                "posted_at": now.isoformat(),
                "text": "행동주의 주주 공시 뉴스",
                "normalized_text": "행동주의 주주 공시 뉴스",
                "message_url": "https://t.me/marketnews/10",
            }
        ],
        "telegram_article_matches": [],
        "telegram_channel_candidates": [{"handle": "candidate", "status": "pending"}],
        "telegram_issue_signals": [],
    }

    path = write_telegram_dashboard(tmp_path, state, config, now)
    html = path.read_text(encoding="utf-8")

    assert "Telegram 수집 운영 대시보드" in html
    assert "공개 broadcast 채널만" in html
    assert "marketnews" in html
    assert "TELEGRAM_API_HASH" not in html
