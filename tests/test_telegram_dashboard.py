from __future__ import annotations

import hashlib

from curator.telegram_dashboard import (
    build_telegram_admin_access_message,
    send_telegram_admin_access_message,
    telegram_admin_access_token_hash,
    telegram_dashboard_model,
    write_telegram_dashboard,
)


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

    assert "Telegram 시장 시그널 대시보드" in html
    assert "공개 broadcast 채널" in html
    # Static Pages contains aggregate status only; channel/message details are
    # fetched from the authenticated, paginated API after unlock.
    assert "marketnews" not in html
    assert "매칭 품질" in html
    assert "시장 시그널 분석" in html
    assert "New/Rising" in html
    assert "Watch 후보" in html
    assert "Risk watch" in html
    assert "상장사 시그널 분석" in html
    assert "상승 상장사" in html
    assert "company_signal_overview" in html
    assert "signal_quality_score" in html
    assert "TELEGRAM_API_HASH" not in html


def test_telegram_dashboard_model_builds_investor_signal_sections(config, now) -> None:  # type: ignore[no-untyped-def]
    state = {
        "telegram_source_channels": [
            {
                "handle": "first",
                "title": "경제 증권 뉴스",
                "enabled": True,
                "source_type": "public_channel",
                "is_public_channel": True,
                "quality_score": 80,
            },
            {
                "handle": "second",
                "title": "공시 채널",
                "enabled": True,
                "source_type": "public_channel",
                "is_public_channel": True,
                "quality_score": 75,
            },
        ],
        "telegram_source_messages": [
            {
                "handle": "first",
                "channel_title": "경제 증권 뉴스",
                "telegram_message_id": 1,
                "posted_at": now.isoformat(),
                "text": "삼성전자 자사주 소각 주주환원 이슈",
                "normalized_text": "삼성전자 자사주 소각 주주환원 이슈",
                "message_url": "https://t.me/first/1",
            },
            {
                "handle": "second",
                "channel_title": "공시 채널",
                "telegram_message_id": 2,
                "posted_at": now.isoformat(),
                "text": "삼성전자 자사주 소각 확대 보도",
                "normalized_text": "삼성전자 자사주 소각 확대 보도",
                "message_url": "https://t.me/second/2",
            },
        ],
        "telegram_article_matches": [],
        "telegram_channel_candidates": [],
        "telegram_issue_signals": [],
    }
    config["telegram_sources"] = {
        "signal_window_hours": 72,
        "signal_min_messages": 2,
        "signal_min_channels": 2,
        "signal_limit": 10,
    }
    config["source_rights"] = {
        "enforce": True,
        "records": [
            {
                "source_right_id": f"telegram:{handle}",
                "source_category": "authorized_telegram",
                "source_identity": handle,
                "scope": "collection,ai,redistribution",
                "evidence_ref": f"evidence://telegram/{handle}",
                "valid_from": "2021-01-01",
                "allow_ai": True,
                "allow_redistribution": True,
            }
            for handle in ("first", "second")
        ],
    }

    model = telegram_dashboard_model(state, config, now)

    assert model["signal_overview"]["top_score"] > 0  # type: ignore[index]
    assert model["signal_overview"]["watchlist_candidates"] >= 1  # type: ignore[index]
    assert model["watchlist_candidates"]
    assert model["watchlist_candidates"][0]["signal_score"] > 0  # type: ignore[index]
    assert model["watchlist_candidates"][0]["lifecycle"] in {"new", "rising", "active"}  # type: ignore[index]
    assert model["company_signal_overview"]["companies_total"] >= 1  # type: ignore[index]
    assert model["top_company_signals"][0]["company"] == "삼성전자"  # type: ignore[index]
    assert model["top_company_signals"][0]["mentions_24h"] == 2  # type: ignore[index]
    assert model["top_company_signals"][0]["channels_count"] == 2  # type: ignore[index]
    assert model["top_company_signals"][0]["signal_score"] > 0  # type: ignore[index]


def test_telegram_dashboard_requires_token_without_embedding_data(tmp_path, config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_ADMIN_ACCESS_TOKEN", "admin-secret-token")
    state = {
        "telegram_source_channels": [
            {
                "handle": "sensitivechannel",
                "title": "Sensitive Channel",
                "enabled": True,
                "source_type": "public_channel",
                "is_public_channel": True,
                "quality_score": 90,
            }
        ],
        "telegram_source_messages": [],
        "telegram_article_matches": [],
        "telegram_channel_candidates": [],
        "telegram_issue_signals": [],
    }

    path = write_telegram_dashboard(tmp_path, state, config, now)
    html = path.read_text(encoding="utf-8")

    expected_hash = hashlib.sha256(b"admin-secret-token").hexdigest()
    assert "Telegram admin" in html
    assert expected_hash in html
    assert "admin-secret-token" not in html
    assert "sensitivechannel" not in html


def test_telegram_admin_access_message_contains_token_link(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_ADMIN_ACCESS_TOKEN", "admin-secret-token")

    message = build_telegram_admin_access_message(config, now)

    assert "Telegram admin" in message
    assert "telegram-admin.html#token=admin-secret-token" in message
    assert telegram_admin_access_token_hash() == hashlib.sha256(b"admin-secret-token").hexdigest()


def test_telegram_admin_access_uses_private_destination(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_dashboard

    captured: dict[str, object] = {}
    monkeypatch.setenv("TELEGRAM_ADMIN_ACCESS_TOKEN", "admin-secret-token")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:secret")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@public_channel")
    monkeypatch.setenv("TELEGRAM_ADMIN_CHAT_ID", "424242")

    def fake_send(_token, chat_id, text, _config, **_kwargs):  # type: ignore[no-untyped-def]
        captured.update({"chat_id": chat_id, "text": text})
        return {"ok": True, "message_id": 8}

    monkeypatch.setattr(telegram_dashboard, "send_telegram_message", fake_send)

    response = send_telegram_admin_access_message(config, now)

    assert response["ok"] is True
    assert captured["chat_id"] == "424242"
    assert "#token=admin-secret-token" in str(captured["text"])


def test_telegram_admin_access_rejects_public_destination(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_ADMIN_ACCESS_TOKEN", "admin-secret-token")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:secret")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "424242")
    monkeypatch.setenv("TELEGRAM_ADMIN_CHAT_ID", "424242")

    response = send_telegram_admin_access_message(config, now)

    assert response == {"ok": False, "error": "telegram_admin_chat_matches_public_destination"}
