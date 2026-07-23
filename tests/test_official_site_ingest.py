from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import httpx
import pytest
import yaml

from curator import official_site_ingest
from curator.official_site_ingest import (
    Candidate,
    Connector,
    OfficialSiteIngestError,
    apply_artifact,
    build_apply_payloads,
    collect,
    fetch_adapter,
    parse_allowlist_manifest,
    stable_connector_id,
    stable_source_right_id,
    validate_https_url,
    validate_source_rights,
)


NOW = datetime(2026, 7, 22, 12, 0, tzinfo=timezone.utc)
REVISION = "a" * 40


def company_candidates() -> dict[str, object]:
    return {
        "ok": True,
        "api_version": "v1",
        "generated_at": "2026-07-22T11:59:00+00:00",
        "score_version": "official-site-candidates-v1",
        "selection": {
            "companies_limit": 20,
            "actors_limit": 10,
            "official_evidence_required": True,
        },
        "companies": [
            {
                "company_id": "00123456",
                "company_name": "테스트 주식회사",
                "rank": 1,
                "event_count": 4,
                "raw_score": 320,
                "latest_event_at": "2026-07-21T00:00:00Z",
            }
        ],
        "actors": [],
    }


def source_right(*, status: str = "active", redistribution: object = 1) -> dict[str, object]:
    return {
        "source_right_id": "right:company-site:00123456",
        "source_type": "company_statement",
        "source_key": "company-site:00123456",
        "source_name": "테스트 주식회사 공식 사이트",
        "permission_scope": "store and redistribute original official statements",
        "evidence_uri": "https://company.example/permission-record",
        "evidence_hash": None,
        "valid_from": "2026-01-01 00:00:00",
        "valid_until": "2027-01-01 00:00:00",
        "revoked_at": None,
        "ai_allowed": 0,
        "redistribution_allowed": redistribution,
        "status": status,
        "notes": "printed permission record retained offline",
        "created_at": "2026-07-01 00:00:00",
        "updated_at": "2026-07-01 00:00:00",
    }


def manifest() -> dict[str, object]:
    return {
        "schema_version": 1,
        "connectors": [
            {
                "connector_id": "company-site:00123456",
                "entity_type": "company",
                "entity_id": "00123456",
                "source_class": "company_statement",
                "source_right_id": "right:company-site:00123456",
                "endpoint": "https://company.example/feed.json",
                "allowed_hosts": ["company.example"],
                "page_size": 100,
                "active": True,
            }
        ],
    }


def complete_item(**overrides: object) -> dict[str, object]:
    item: dict[str, object] = {
        "operation": "upsert",
        "external_id": "notice-2026-001",
        "title": "  원문 제목은 공백도 보존  ",
        "body": "첫 문단\n\nOriginal English paragraph.  ",
        "language": "ko",
        "original_url": "https://company.example/notices/notice-2026-001?view=full",
        "published_at": "2026-07-22T09:00:00+09:00",
        "identity": {
            "company_id": "00123456",
            "event_type": "shareholder_proposal",
            "action": "제안",
            "target": "정관 변경",
            "actor_id": "actor:verified-proponent",
            "effective_at": "2026-07-22T09:00:00+09:00",
            "deadline_at": "2027-03-31T09:00:00+09:00",
        },
    }
    item.update(overrides)
    return item


def adapter_payload(
    items: list[dict[str, object]],
    *,
    page: int = 1,
    total_pages: int = 1,
    total_count: int | None = None,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "connector_id": "company-site:00123456",
        "page": page,
        "total_pages": total_pages,
        "total_count": len(items) if total_count is None else total_count,
        "items": items,
    }


def api_page(rows: list[dict[str, object]], *, page: int = 1, has_more: bool = False) -> dict[str, object]:
    return {
        "ok": True,
        "api_version": "v1",
        "data": rows,
        "pagination": {
            "page": page,
            "limit": 25,
            "returned": len(rows),
            "has_more": has_more,
            "next_page": page + 1 if has_more else None,
        },
    }


def collecting_client(
    adapter_items: list[dict[str, object]],
    *,
    observed: list[httpx.Request] | None = None,
) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        if observed is not None:
            observed.append(request)
        if request.url.host == "api.example":
            assert request.headers["Authorization"] == "Bearer " + "x" * 32
            if request.url.path.endswith("/ops/official-site-candidates"):
                return httpx.Response(200, json=company_candidates())
            if request.url.path.endswith("/ops/official-site-rights"):
                assert request.url.params["page"] == "1"
                return httpx.Response(200, json=api_page([source_right()]))
        if request.url.host == "company.example":
            return httpx.Response(200, json=adapter_payload(adapter_items))
        raise AssertionError(f"unexpected request: {request.url}")

    return httpx.Client(transport=httpx.MockTransport(handler))


def run_collect(
    adapter_items: list[dict[str, object]],
    *,
    observed: list[httpx.Request] | None = None,
) -> dict[str, object]:
    with collecting_client(adapter_items, observed=observed) as client:
        return collect(
            api_base_url="https://api.example/api.php/api/v1",
            api_token="x" * 32,
            manifest_payload=manifest(),
            now=NOW,
            code_revision=REVISION,
            client=client,
        )


def test_collects_exact_candidates_rights_and_original_content_into_drafts() -> None:
    requests: list[httpx.Request] = []
    item = complete_item()
    artifact = run_collect([item], observed=requests)

    payload = artifact["draft_payload"]
    assert isinstance(payload, dict)
    documents = payload["documents"]
    events = payload["events"]
    assert isinstance(documents, list) and isinstance(events, list)
    assert len(documents) == len(events) == 1
    document = documents[0]
    event = events[0]
    assert document["title"] == item["title"]
    assert document["body_text"] == item["body"]
    assert document["original_language"] == item["language"]
    assert document["publication_status"] == "draft"
    assert event["publication_status"] == "draft"
    assert event["review_status"] == "pending"
    assert event["review_required"] is True
    assert event["identity_status"] == "complete"
    assert artifact["review_items"] == []
    assert artifact["tombstones"] == []
    assert artifact["apply"] == {
        "mode": "not_requested",
        "remote_mutation_performed": False,
        "reason": "collection_and_apply_are_separate_fail_closed_steps",
    }
    assert all(
        "authorization" not in request.headers and "cookie" not in request.headers
        for request in requests
        if request.url.host == "company.example"
    )
    serialized = json.dumps(artifact, ensure_ascii=False)
    assert "https://company.example/feed.json" not in serialized
    assert "x" * 32 not in serialized


def test_idempotent_ids_and_content_change_remains_a_reviewable_draft() -> None:
    first = run_collect([complete_item()])
    repeated = run_collect([complete_item()])
    assert first == repeated

    changed = run_collect([complete_item(body="변경된 원문 본문")])
    first_document = first["draft_payload"]["documents"][0]  # type: ignore[index]
    changed_document = changed["draft_payload"]["documents"][0]  # type: ignore[index]
    assert first_document["document_id"] != changed_document["document_id"]
    assert first_document["content_hash"] != changed_document["content_hash"]
    assert changed_document["publication_status"] == "draft"


def test_builds_one_bounded_atomic_payload_with_exact_review_and_tombstone_counts() -> None:
    incomplete_identity = copy.deepcopy(complete_item()["identity"])
    assert isinstance(incomplete_identity, dict)
    incomplete_identity["deadline_at"] = ""
    artifact = run_collect(
        [
            complete_item(),
            complete_item(external_id="notice-2026-002", identity=incomplete_identity),
            {
                "operation": "delete",
                "external_id": "notice-2026-003",
                "deleted_at": "2026-07-23T10:00:00+09:00",
                "original_url": "https://company.example/notices/notice-2026-003",
            },
        ]
    )

    payloads = build_apply_payloads(artifact)

    assert len(payloads) == 1
    payload = payloads[0]
    assert payload["expected"] == {
        "companies": 1,
        "documents": 1,
        "events": 1,
        "event_observations": 1,
        "review_items": 1,
        "tombstones": 1,
    }
    assert payload["connector"]["connector_id"] == "company-site:00123456"  # type: ignore[index]
    assert payload["snapshot_id"] and payload["payload_sha256"]
    assert json.dumps(payload, ensure_ascii=False).find("company.example/feed.json") == -1


def test_apply_requires_exact_hmac_ack_and_reports_idempotent_replay(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    artifact = run_collect([complete_item()])
    submitted: list[dict[str, object]] = []

    monkeypatch.setattr(official_site_ingest, "remote_api_configured", lambda: True)

    def exact_post(action: str, payload: dict[str, object], *, timeout: float) -> dict[str, object]:
        assert action == "upsert_official_site_snapshot"
        assert timeout == 60.0
        submitted.append(payload)
        return {
            "ok": True,
            "snapshot_id": payload["snapshot_id"],
            "receipt_sha256": payload["receipt_sha256"],
            "accepted": payload["expected"],
            "rejected": 0,
            "idempotent": True,
        }

    monkeypatch.setattr(official_site_ingest, "post_remote_action", exact_post)
    applied = apply_artifact(artifact)

    assert len(submitted) == 1
    assert applied["apply"] == {
        "mode": "remote_atomic_per_connector",
        "remote_mutation_performed": True,
        "connector_count": 1,
        "idempotent_connector_count": 1,
        "accepted": {
            "companies": 1,
            "documents": 1,
            "events": 1,
            "event_observations": 1,
            "review_items": 0,
            "tombstones": 0,
        },
    }

    def partial_post(action: str, payload: dict[str, object], *, timeout: float) -> dict[str, object]:
        response = exact_post(action, payload, timeout=timeout)
        accepted = dict(response["accepted"])  # type: ignore[arg-type]
        accepted["documents"] = 0
        response["accepted"] = accepted
        return response

    monkeypatch.setattr(official_site_ingest, "post_remote_action", partial_post)
    with pytest.raises(OfficialSiteIngestError, match="ACK mismatch"):
        apply_artifact(artifact)


def test_delete_is_only_a_tombstone_and_never_a_remote_delete() -> None:
    deleted = {
        "operation": "delete",
        "external_id": "notice-2026-001",
        "deleted_at": "2026-07-22T20:00:00+09:00",
        "original_url": "https://company.example/notices/notice-2026-001",
    }
    artifact = run_collect([deleted])
    payload = artifact["draft_payload"]
    assert payload["documents"] == []  # type: ignore[index]
    assert payload["events"] == []  # type: ignore[index]
    tombstones = artifact["tombstones"]
    assert isinstance(tombstones, list) and len(tombstones) == 1
    assert tombstones[0]["action"] == "review_only_no_automatic_delete"
    assert artifact["apply"]["remote_mutation_performed"] is False  # type: ignore[index]


def test_incomplete_identity_goes_only_to_review_artifact() -> None:
    identity = copy.deepcopy(complete_item()["identity"])
    assert isinstance(identity, dict)
    identity["deadline_at"] = ""
    artifact = run_collect([complete_item(identity=identity)])
    payload = artifact["draft_payload"]
    assert payload["documents"] == []  # type: ignore[index]
    assert payload["events"] == []  # type: ignore[index]
    reviews = artifact["review_items"]
    assert isinstance(reviews, list) and len(reviews) == 1
    assert "missing_deadline_at" in reviews[0]["review_reasons"]
    assert reviews[0]["draft_document"]["publication_status"] == "draft"


def test_manifest_requires_one_active_connector_per_candidate_and_no_extras() -> None:
    candidates = [Candidate("company", "00123456", "회사")]
    missing = {"schema_version": 1, "connectors": []}
    with pytest.raises(OfficialSiteIngestError, match="mismatch"):
        parse_allowlist_manifest(missing, candidates=candidates)

    duplicated = manifest()
    duplicated["connectors"] = [
        *duplicated["connectors"],  # type: ignore[misc]
        copy.deepcopy(duplicated["connectors"][0]),  # type: ignore[index]
    ]
    with pytest.raises(OfficialSiteIngestError, match="duplicate connector_id|multiple connectors"):
        parse_allowlist_manifest(duplicated, candidates=candidates)

    inactive = manifest()
    inactive["connectors"][0]["active"] = False  # type: ignore[index]
    with pytest.raises(OfficialSiteIngestError, match="active connector"):
        parse_allowlist_manifest(inactive, candidates=candidates)

    unstable = manifest()
    unstable["connectors"][0]["connector_id"] = "company-site:renamed"  # type: ignore[index]
    with pytest.raises(OfficialSiteIngestError, match="stable connector_id"):
        parse_allowlist_manifest(unstable, candidates=candidates)


def test_long_actor_ids_receive_deterministic_bounded_connector_and_right_ids() -> None:
    actor_id = "actor:" + "a" * 56
    connector_id = stable_connector_id("actor", actor_id)
    right_id = stable_source_right_id(connector_id)
    assert len(connector_id) <= 64 and len(right_id) <= 64
    raw = {
        "schema_version": 1,
        "connectors": [
            {
                "connector_id": connector_id,
                "entity_type": "actor",
                "entity_id": actor_id,
                "source_class": "activist_statement",
                "source_right_id": right_id,
                "endpoint": "https://activist.example/feed.json",
                "allowed_hosts": ["activist.example"],
                "page_size": 50,
                "active": True,
            }
        ],
    }
    parsed = parse_allowlist_manifest(
        raw,
        candidates=[Candidate("actor", actor_id, "행동주주")],
    )
    assert parsed[0].connector_id == connector_id
    assert parsed[0].source_class == "activist_statement"


@pytest.mark.parametrize(
    "url",
    [
        "http://company.example/feed.json",
        "https://user:password@company.example/feed.json",
        "https://company.example/feed.json?token=secret",
        "https://127.0.0.1/feed.json",
        "https://localhost/feed.json",
        "https://company.example:8443/feed.json",
    ],
)
def test_adapter_endpoint_requires_credential_free_public_https(url: str) -> None:
    raw = manifest()
    raw["connectors"][0]["endpoint"] = url  # type: ignore[index]
    with pytest.raises(OfficialSiteIngestError):
        parse_allowlist_manifest(
            raw,
            candidates=[Candidate("company", "00123456", "회사")],
        )


def test_valid_source_right_must_be_active_unrevoked_and_redistributable() -> None:
    connector = parse_allowlist_manifest(
        manifest(),
        candidates=[Candidate("company", "00123456", "회사")],
    )[0]
    validate_source_rights(
        [connector],
        {"right:company-site:00123456": source_right()},
        now=NOW,
    )
    for invalid in (
        source_right(status="revoked"),
        source_right(redistribution=0),
        {**source_right(), "revoked_at": "2026-07-22 11:59:59"},
        {**source_right(), "valid_until": "2026-07-22 11:59:59"},
        {**source_right(), "source_key": "another-connector"},
        {**source_right(), "evidence_uri": None, "evidence_hash": None},
    ):
        with pytest.raises(OfficialSiteIngestError, match="lacks an active"):
            validate_source_rights(
                [connector],
                {"right:company-site:00123456": invalid},
                now=NOW,
            )


def test_minimal_ops_right_projection_uses_evidence_presence_without_disclosing_evidence() -> None:
    expected = connector()
    projected = {
        "source_right_id": expected.source_right_id,
        "source_type": expected.source_class,
        "source_key": expected.connector_id,
        "source_name": "verified official site",
        "permission_scope": "store and redistribute original official statements",
        "valid_from": "2026-01-01 00:00:00",
        "valid_until": "2027-01-01 00:00:00",
        "ai_allowed": 0,
        "redistribution_allowed": 1,
        "status": "active",
        "rights_revision": 3,
        "evidence_present": True,
    }
    validate_source_rights([expected], {expected.source_right_id: projected}, now=NOW)
    projected["evidence_present"] = False
    with pytest.raises(OfficialSiteIngestError, match="lacks an active"):
        validate_source_rights([expected], {expected.source_right_id: projected}, now=NOW)


def connector() -> Connector:
    return parse_allowlist_manifest(
        manifest(),
        candidates=[Candidate("company", "00123456", "회사")],
    )[0]


def adapter_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


@pytest.mark.parametrize("case", ["page_drift", "empty", "count", "duplicate", "totals"])
def test_adapter_rejects_pagination_drift_empty_count_mismatch_and_duplicates(case: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        if case == "page_drift":
            payload = adapter_payload([complete_item()], page=2, total_pages=2, total_count=2)
        elif case == "empty":
            payload = adapter_payload(
                [complete_item()] if page == 1 else [],
                page=page,
                total_pages=2,
                total_count=1,
            )
        elif case == "count":
            payload = adapter_payload([complete_item()], total_count=2)
        elif case == "duplicate":
            payload = adapter_payload(
                [complete_item()],
                page=page,
                total_pages=2,
                total_count=2,
            )
        else:
            payload = adapter_payload(
                [complete_item(external_id=f"notice-{page}")],
                page=page,
                total_pages=2,
                total_count=2 if page == 1 else 3,
            )
        return httpx.Response(200, json=payload)

    with adapter_client(handler) as client:
        with pytest.raises(OfficialSiteIngestError):
            fetch_adapter(client, connector())


def test_adapter_rejects_redirect_wrong_content_type_and_original_url_host_escape() -> None:
    handlers: list[Callable[[httpx.Request], httpx.Response]] = [
        lambda request: httpx.Response(
            302,
            headers={"location": "https://other.example/feed.json"},
        ),
        lambda request: httpx.Response(
            200,
            text=json.dumps(adapter_payload([complete_item()])),
            headers={"content-type": "text/plain"},
        ),
        lambda request: httpx.Response(
            200,
            json=adapter_payload(
                [complete_item(original_url="https://other.example/notice")]
            ),
        ),
    ]
    for handler in handlers:
        with adapter_client(handler) as client:
            with pytest.raises(OfficialSiteIngestError):
                fetch_adapter(client, connector())


def test_source_right_pagination_is_exercised_and_duplicate_pages_fail() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/ops/official-site-candidates"):
            return httpx.Response(200, json=company_candidates())
        if request.url.path.endswith("/ops/official-site-rights"):
            page = int(request.url.params["page"])
            if page == 1:
                unrelated = {**source_right(), "source_right_id": "right:unrelated"}
                return httpx.Response(200, json=api_page([unrelated], page=1, has_more=True))
            return httpx.Response(200, json=api_page([source_right()], page=2))
        return httpx.Response(200, json=adapter_payload([complete_item()]))

    with adapter_client(handler) as client:
        artifact = collect(
            api_base_url="https://api.example/api.php/api/v1",
            api_token="x" * 32,
            manifest_payload=manifest(),
            now=NOW,
            code_revision=REVISION,
            client=client,
        )
    right_pages = [
        request.url.params["page"]
        for request in requests
        if request.url.path.endswith("/ops/official-site-rights")
    ]
    assert right_pages == ["1", "2"]
    assert artifact["counts"]["connector_count"] == 1  # type: ignore[index]


def test_valid_https_url_keeps_public_item_queries_but_rejects_fragments() -> None:
    value, host = validate_https_url(
        "https://company.example/notice?id=123",
        location="test URL",
        allow_query=True,
    )
    assert value.endswith("?id=123") and host == "company.example"
    with pytest.raises(OfficialSiteIngestError, match="fragment"):
        validate_https_url(
            "https://company.example/notice#secret",
            location="test URL",
            allow_query=True,
        )
    with pytest.raises(OfficialSiteIngestError, match="credential-like"):
        validate_https_url(
            "https://company.example/notice?token=secret",
            location="test URL",
            allow_query=True,
        )


def test_official_site_workflow_is_shadow_live_only_and_atomically_applies_drafts() -> None:
    path = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ingest-official-sites.yml"
    text = path.read_text(encoding="utf-8")
    parsed = yaml.load(text, Loader=yaml.BaseLoader)
    assert isinstance(parsed, dict) and "jobs" in parsed
    assert "GOVERNANCE_PIPELINE_MODE == 'shadow'" in text
    assert "GOVERNANCE_PIPELINE_MODE == 'live'" in text
    assert "dart_canary" not in text
    assert "OFFICIAL_SITE_ALLOWLIST_B64: ${{ secrets.OFFICIAL_SITE_ALLOWLIST_B64 }}" in text
    assert "BSIDE_OPS_TOKEN: ${{ secrets.BSIDE_OPS_TOKEN }}" in text
    assert "ENABLE_TELEGRAM_DELIVERY: \"false\"" in text
    assert "ENABLE_GOVERNANCE_DELIVERY: \"false\"" in text
    assert "upload-artifact@v7" in text
    assert "retention-days: 90" in text
    assert "ACTIVIST_API_SECRET: ${{ secrets.ACTIVIST_API_SECRET }}" in text
    assert "--apply" in text
    assert "BSIDE_ADMIN_TOKEN" not in text
