from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from curator.source_rights import partition_authorized_records, source_is_authorized


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))


def rights_config(*, revoked_at: str | None = None, allow_ai: bool = True, allow_public: bool = True) -> dict[str, object]:
    return {
        "source_rights": {
            "enforce": True,
            "records": [
                {
                    "source_right_id": "telegram:licensed",
                    "source_category": "authorized_telegram",
                    "source_identity": "licensed",
                    "scope": "collection,ai,redistribution",
                    "evidence_ref": "evidence://telegram/license-1",
                    "valid_from": "2021-01-01",
                    "revoked_at": revoked_at,
                    "allow_ai": allow_ai,
                    "allow_redistribution": allow_public,
                }
            ],
        }
    }


def test_telegram_requires_registered_active_right() -> None:
    record = {"source_kind": "telegram_signal", "handle": "licensed"}
    assert source_is_authorized(record, rights_config(), NOW)
    assert not source_is_authorized({**record, "handle": "unknown"}, rights_config(), NOW)
    assert not source_is_authorized(record, rights_config(revoked_at="2026-07-16"), NOW)


def test_right_scope_blocks_ai_and_public_independently() -> None:
    record = {"source_kind": "telegram_reference", "channel_handle": "@licensed"}
    assert not source_is_authorized(record, rights_config(allow_ai=False), NOW, purpose="ai")
    assert not source_is_authorized(record, rights_config(allow_public=False), NOW, purpose="public")
    assert source_is_authorized(record, rights_config(), NOW, purpose="public")


def test_official_and_media_do_not_require_telegram_license() -> None:
    assert source_is_authorized({"source_kind": "official", "source": "DART"}, rights_config(), NOW)
    assert source_is_authorized({"source_kind": "direct", "source": "Reuters"}, rights_config(), NOW)


def test_explicit_lineage_never_falls_back_to_unlicensed_media() -> None:
    record = {
        "source_kind": "direct",
        "source_right_id": "telegram:missing",
        "source": "Derived record",
    }
    assert not source_is_authorized(record, rights_config(), NOW, purpose="public")


def test_missing_explicit_lineage_never_falls_back_to_same_handle() -> None:
    record = {
        "source_kind": "telegram_signal",
        "source_right_id": "telegram:withdrawn-license",
        "handle": "licensed",
    }
    assert not source_is_authorized(record, rights_config(), NOW, purpose="public")


def test_partition_marks_block_reason_without_mutating_input() -> None:
    licensed = {"source_kind": "telegram", "handle": "licensed", "title": "원문"}
    missing = {"source_kind": "telegram", "handle": "missing", "title": "원문2"}
    allowed, blocked = partition_authorized_records([licensed, missing], rights_config(), NOW, purpose="ai")
    assert allowed == [licensed]
    assert blocked[0]["source_right_status"] == "source_right_missing"
    assert "source_right_status" not in missing


def test_pending_status_and_string_false_permissions_fail_closed() -> None:
    config: dict[str, object] = {
        "source_rights": {
            "enforce": True,
            "records": [
                {
                    "source_right_id": "telegram:pending",
                    "source_category": "authorized_telegram",
                    "source_identity": "pending",
                    "scope": "read",
                    "evidence_ref": "evidence:pending",
                    "valid_from": "2025-01-01",
                    "status": "pending",
                    "allow_ai": "false",
                    "allow_redistribution": "0",
                }
            ],
        }
    }
    record = {"source_kind": "telegram", "handle": "pending"}
    assert not source_is_authorized(record, config, NOW, purpose="collect")
    assert not source_is_authorized(record, config, NOW, purpose="ai")
    assert not source_is_authorized(record, config, NOW, purpose="public")
