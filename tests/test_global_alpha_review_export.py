from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from curator.global_alpha_evidence_bundle import (
    _parse_review_export,
    _select_review_candidates,
)
from curator.global_alpha_review_export import (
    ReviewExportConfig,
    ReviewExportError,
    export_review_candidates,
    normalize_api_base,
    write_export,
)


REVISION = "a" * 40
OTHER_REVISION = "b" * 40
API_BASE = "https://api.example.test/activist/api.php/api/v2"
TOKEN = "preview-secret-that-must-not-be-exported"


def _event(index: int) -> dict[str, object]:
    issuer_group = index // 6
    return {
        "event_id": f"event:{index:03d}",
        "issuer_id": f"issuer:{issuer_group:02d}",
        "issuer_name": f"Issuer {issuer_group:02d}",
        "ticker": f"T{issuer_group:02d}",
        "market": "TEST",
        "country": ("KR", "US", "JP", "GB", "CA", "AU")[index % 6],
        "event_family": f"family_{issuer_group % 3}",
        "importance": ("high", "medium", "low")[index % 3],
        "verification_status": "official",
        "change_type": "new",
        "title": f"Original official event {index:03d}",
        "original_language": "en",
        "change_summary": f"Change {index}",
        "current_status": "official",
        "actor_name": None,
        "actor_role": None,
        "occurred_at": "2026-07-25T00:00:00Z",
        "filed_at": "2026-07-25T00:00:00Z",
        "first_observed_at": "2026-07-25T00:01:00Z",
        "updated_at": "2026-07-25T00:02:00Z",
        "deadline_at": None,
        "official_evidence_count": 2,
        "media_count": 1,
        "coverage_mode": "market-wide",
        "source_url": f"https://official.example/event/{index}",
    }


def _documents(
    index: int,
    *,
    documents_per_event: int,
    telegram: bool = False,
) -> list[dict[str, object]]:
    documents = [
        {
            "document_id": f"document:{index:03d}:{document_index}",
            "document_type": "official",
            "source_class": "official_disclosure",
            "source_key": "not-exported",
            "original_language": "en",
            "title": f"Official document {index:03d}/{document_index}",
            "original_url": (
                f"https://official.example/doc/{index}/{document_index}"
            ),
            "filed_at": "2026-07-25T00:00:00Z",
            "published_at": "2026-07-25T00:00:00Z",
            "verification_status": "official",
            "correction_of_document_id": None,
        }
        for document_index in range(documents_per_event)
    ]
    documents.append(
        {
            "document_id": f"media:{index:03d}",
            "document_type": "news",
            "source_class": "media_report",
            "source_key": "must-be-filtered",
            "original_language": "en",
            "title": f"Media report {index:03d}",
            "original_url": f"https://media.example/story/{index}",
            "filed_at": None,
            "published_at": "2026-07-25T00:03:00Z",
            "verification_status": "confirmed",
            "correction_of_document_id": None,
        }
    )
    if telegram and index == 0:
        documents.append(
            {
                "document_id": "telegram:exposure",
                "document_type": "signal",
                "source_class": "authorized_telegram",
                "source_key": "forbidden",
                "original_language": "ko",
                "title": "Forbidden Telegram signal",
                "original_url": "https://t.me/forbidden/1",
                "filed_at": None,
                "published_at": "2026-07-25T00:04:00Z",
                "verification_status": "signal",
                "correction_of_document_id": None,
            }
        )
    return documents


def _envelope(data: object, **extra: object) -> dict[str, object]:
    return {"ok": True, "data": data, "api_version": "v2", **extra}


def _handler(
    *,
    revision: str = REVISION,
    brief_revision: str = REVISION,
    event_count: int = 60,
    documents_per_event: int = 2,
    telegram: bool = False,
    mutate_brief: bool = False,
    mutate_health: bool = False,
    expose_preview_token: bool = False,
) -> tuple[
    Callable[[httpx.Request], httpx.Response],
    list[tuple[str, str]],
]:
    events = [_event(index) for index in range(event_count)]
    if expose_preview_token:
        events[0]["title"] = TOKEN
    calls: list[tuple[str, str]] = []
    brief_calls = 0
    health_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal brief_calls, health_calls
        path = request.url.path
        authorization = request.headers.get("authorization", "")
        calls.append((path, authorization))
        if path.endswith("/health"):
            health_calls += 1
            assert authorization == ""
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "service": "bside-global-market-terminal",
                    "code_revision": (
                        OTHER_REVISION
                        if mutate_health and health_calls > 1
                        else revision
                    ),
                    "time": "2026-07-25T12:00:00Z",
                    "api_version": "v2",
                },
            )
        assert authorization == f"Bearer {TOKEN}"
        if path.endswith("/briefs/latest"):
            brief_calls += 1
            assert request.url.params["edition"] == "global"
            top_items = [dict(item) for item in events[:5]]
            if mutate_brief and brief_calls > 1:
                top_items[0]["title"] = "Changed while export was running"
            return httpx.Response(
                200,
                json=_envelope(
                    {
                        "schema_version": 1,
                        "brief_id": "brief:global:20260725",
                        "edition": "global",
                        "cutoff_at": "2026-07-25T11:00:00Z",
                        "published_at": "2026-07-25T11:05:00Z",
                        "last_updated_at": "2026-07-25T11:05:00Z",
                        "build_sha": brief_revision,
                        "stale": False,
                        "coverage_notice": None,
                        "top": top_items,
                        "watch": [],
                        "deadlines": [],
                        "source_status": [],
                        "empty_reason": None,
                    }
                ),
            )
        if path.endswith("/events"):
            offset = int(request.url.params["offset"])
            limit = int(request.url.params["limit"])
            items = events[offset : offset + limit]
            has_more = offset + len(items) < len(events)
            return httpx.Response(
                200,
                json={
                    **_envelope({"items": items}),
                    "meta": {
                        "page": None,
                        "offset": offset,
                        "limit": limit,
                        "returned": len(items),
                        "has_more": has_more,
                        "next_page": None,
                        "next_offset": (
                            offset + len(items) if has_more else None
                        ),
                        "continuation_limited": False,
                    },
                },
            )
        marker = "/events/event:"
        if marker in path:
            index = int(path.split(marker, 1)[1])
            return httpx.Response(
                200,
                json=_envelope(
                    {
                        "event": events[index],
                        "actors": [],
                        "documents": _documents(
                            index,
                            documents_per_event=documents_per_event,
                            telegram=telegram,
                        ),
                        "observations": [],
                    }
                ),
            )
        raise AssertionError(f"unexpected request: {request.url}")

    return handler, calls


def _config() -> ReviewExportConfig:
    return ReviewExportConfig(
        api_base_url=API_BASE,
        preview_token=TOKEN,
        expected_revision=REVISION,
    )


def _export(
    handler: Callable[[httpx.Request], httpx.Response],
) -> dict[str, object]:
    with httpx.Client(
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
    ) as client:
        return export_review_candidates(_config(), client=client)


def test_export_is_deterministic_blank_and_prepare_compatible() -> None:
    first_handler, first_calls = _handler()
    second_handler, _ = _handler()
    first = _export(first_handler)
    second = _export(second_handler)

    assert first == second
    assert first["kind"] == "bside-global-alpha-review-candidate-export"
    assert first["code_revision"] == REVISION
    assert first["is_synthetic"] is False
    assert len(first["event_candidates"]) == 60
    assert len(first["same_event_pair_candidates"]) == 120
    assert len(first["top5_candidates"]) == 5
    assert {
        pair["stratum"]
        for pair in first["same_event_pair_candidates"]
    } == {"predicted_same", "hard_negative", "easy_negative"}
    assert {
        stratum: sum(
            pair["stratum"] == stratum
            for pair in first["same_event_pair_candidates"]
        )
        for stratum in ("predicted_same", "hard_negative", "easy_negative")
    } == {
        "predicted_same": 40,
        "hard_negative": 40,
        "easy_negative": 40,
    }
    serialized = json.dumps(first, ensure_ascii=False)
    assert TOKEN not in serialized
    assert "source_key" not in serialized
    assert '"decision"' not in serialized
    assert '"label"' not in serialized
    assert "telegram" not in serialized.casefold()
    assert sum(path.endswith("/health") for path, _ in first_calls) == 2

    encoded = json.dumps(first, ensure_ascii=False).encode("utf-8")
    with patch.object(Path, "read_bytes", return_value=encoded):
        parsed, _ = _parse_review_export(
            Path("review-export.json"),
            expected_revision=REVISION,
        )
        events, pairs, top5 = _select_review_candidates(parsed)
        assert len(events) == 60
        assert len(pairs) == 120
        assert [item["position_no"] for item in top5] == [1, 2, 3, 4, 5]


def test_revision_and_brief_build_are_bound_to_expected_sha() -> None:
    mismatched_health, calls = _handler(revision=OTHER_REVISION)
    with pytest.raises(ReviewExportError, match="code_revision mismatch"):
        _export(mismatched_health)
    assert len(calls) == 1

    mismatched_brief, _ = _handler(brief_revision=OTHER_REVISION)
    with pytest.raises(ReviewExportError, match="build_sha mismatch"):
        _export(mismatched_brief)

    changing_brief, calls = _handler(mutate_brief=True)
    with pytest.raises(ReviewExportError, match="Top 5 changed during export"):
        _export(changing_brief)
    assert sum(path.endswith("/briefs/latest") for path, _ in calls) == 2
    assert sum(path.endswith("/health") for path, _ in calls) == 1

    changing_health, calls = _handler(mutate_health=True)
    with pytest.raises(ReviewExportError, match="code_revision mismatch"):
        _export(changing_health)
    assert sum(path.endswith("/briefs/latest") for path, _ in calls) == 2
    assert sum(path.endswith("/health") for path, _ in calls) == 2


def test_insufficient_events_or_pair_strata_fail_closed() -> None:
    too_few_events, _ = _handler(event_count=59)
    with pytest.raises(ReviewExportError, match="insufficient official-evidence"):
        _export(too_few_events)

    no_cross_document_pairs, _ = _handler(documents_per_event=1)
    with pytest.raises(ReviewExportError, match="insufficient predicted_same"):
        _export(no_cross_document_pairs)


def test_telegram_document_exposure_fails_even_when_not_official() -> None:
    handler, _ = _handler(telegram=True)
    with pytest.raises(ReviewExportError, match="Telegram source exposure"):
        _export(handler)


def test_preview_token_reflection_fails_closed() -> None:
    handler, _ = _handler(expose_preview_token=True)
    with pytest.raises(ReviewExportError, match="preview token exposure"):
        _export(handler)


def test_output_is_create_only_and_api_base_rejects_credentials() -> None:
    handler, _ = _handler()
    value = _export(handler)
    output = Path("review.json")
    with (
        patch.object(Path, "exists", return_value=False),
        patch.object(Path, "mkdir") as mkdir,
        patch.object(Path, "write_text") as write_text,
    ):
        write_export(output, value)
        mkdir.assert_called_once()
        write_text.assert_called_once()
    with patch.object(Path, "exists", return_value=True):
        with pytest.raises(ReviewExportError, match="already exists"):
            write_export(output, value)
    assert (
        normalize_api_base("https://api.example.test/activist/api.php/api/v1")
        == API_BASE
    )
    with pytest.raises(ReviewExportError, match="invalid_api_base_url"):
        normalize_api_base(
            "https://user:password@api.example.test/activist/api.php/api/v2"
        )
