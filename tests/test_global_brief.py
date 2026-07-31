from __future__ import annotations

import base64
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import yaml

from curator.global_brief import (
    BUNDLE_KIND,
    RECEIPT_KIND,
    GlobalBriefApiError,
    GlobalBriefConfigurationError,
    GlobalBriefValidationError,
    V2GlobalBriefClient,
    _approval_from_sources,
    _validate_pipeline_mode,
    build_candidate_bundle,
    expected_brief_id,
    publish_human_approval,
    validate_candidate_bundle,
    validate_human_approval,
    validate_publication,
)


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "global-brief.yml"
REVISION = "a" * 40
EDITOR_TOKEN = "editor-" + "e" * 40
PREVIEW_TOKEN = "preview-" + "p" * 40
NOW = datetime(2026, 7, 24, 0, 0, tzinfo=timezone.utc)
CUTOFF = "2026-07-23T20:45:00+00:00"


def _event(
    event_id: str,
    *,
    country: str = "US",
    evidence: int = 1,
    updated_at: str = "2026-07-23 20:30:00",
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "issuer_id": "issuer:us:cik:0000320193",
        "issuer_name": "Example Corp",
        "ticker": "EX",
        "market": "US",
        "country": country,
        "event_family": "meeting_and_vote",
        "importance": "high",
        "verification_status": "official",
        "change_type": "new",
        "title": "Original source title",
        "title_provenance": "source",
        "original_language": "en",
        "change_summary": "A governance event changed.",
        "current_status": "official",
        "actor_name": "Example Actor",
        "occurred_at": "2026-07-23 18:00:00",
        "filed_at": "2026-07-23 18:00:00",
        "first_observed_at": "2026-07-23 18:05:00",
        "updated_at": updated_at,
        "deadline_at": None,
        "official_evidence_count": evidence,
        "media_count": 2,
        "coverage_mode": "market-wide",
        "source_url": "https://www.sec.gov/Archives/example",
        "internal_queue_score": 999,
    }


def _source(
    country: str,
    *,
    ready: bool = True,
    public_ready: bool | None = None,
) -> dict[str, object]:
    is_public_ready = ready if public_ready is None else public_ready
    return {
        "connector_id": f"connector:{country.casefold()}:official",
        "country": country,
        "source_name": f"{country} official",
        "coverage_mode": "market-wide",
        "status": "active" if ready else "degraded",
        "collect_status": "active" if ready else "degraded",
        "public_status": (
            "active"
            if is_public_ready
            else ("redistribution_blocked" if ready else "degraded")
        ),
        "last_success_at": "2026-07-23 23:55:00",
        "last_checked_at": "2026-07-23 23:56:00",
        "last_error_class": None if ready else "source_unavailable",
        "public_note": "Official source",
        "lag_minutes": 5,
        "expected_cadence_minutes": 30,
        "fresh": ready,
        "collect_fresh": ready,
        "public_ready": is_public_ready,
        "raw_count": 3,
        "acknowledged_count": 3,
        "private_debug": "must not be copied",
    }


def _bundle(
    *,
    candidates: list[dict[str, object]] | None = None,
    statuses: list[dict[str, object]] | None = None,
    edition: str = "US",
) -> dict[str, object]:
    return build_candidate_bundle(
        edition=edition,
        build_sha=REVISION,
        candidates=candidates if candidates is not None else [_event("event:one")],
        source_checked_at="2026-07-24T00:00:00+00:00",
        source_statuses=statuses if statuses is not None else [_source("US")],
        now=NOW,
        cutoff_at=CUTOFF,
    )


def _approve(bundle: dict[str, object]) -> dict[str, object]:
    approval = deepcopy(bundle["approval_template"])
    approval["approval"] = {
        "status": "approved",
        "approved_by": "oversight-owner",
        "approved_at": "2026-07-24T00:00:00+00:00",
    }
    publication = approval["publication"]
    for item in publication["items"]:
        item["selection_reason"] = "Human-confirmed material governance event"
    return approval


def _candidate_response(items: list[dict[str, object]]) -> dict[str, object]:
    return {
        "ok": True,
        "api_version": "v2",
        "data": {"items": items},
        "meta": {"returned": len(items), "limit": 100},
    }


def _status_response(items: list[dict[str, object]]) -> dict[str, object]:
    return {
        "ok": True,
        "api_version": "v2",
        "data": {
            "items": items,
            "checked_at": "2026-07-24T00:00:00+00:00",
        },
        "meta": {"returned": len(items)},
    }


def test_candidate_bundle_is_secret_free_pending_and_caps_official_top() -> None:
    candidates = [
        _event(f"event:{index}", evidence=0 if index == 2 else 1)
        for index in range(1, 9)
    ]
    bundle = _bundle(candidates=candidates)
    assert bundle["kind"] == BUNDLE_KIND
    assert bundle["contains_secrets"] is False
    assert bundle["auto_publish"] is False
    assert bundle["human_approval_required"] is True
    approval = bundle["approval_template"]
    assert approval["approval"]["status"] == "pending"
    assert approval["approval"]["approved_by"] == ""
    publication = approval["publication"]
    assert len(publication["items"]) == 5
    assert all(item["lane"] == "top" for item in publication["items"])
    assert "event:2" not in {
        item["event_id"] for item in publication["items"]
    }
    assert publication["empty_reason"] is None
    serialized = json.dumps(bundle, ensure_ascii=False)
    assert EDITOR_TOKEN not in serialized
    assert PREVIEW_TOKEN not in serialized
    assert "internal_queue_score" not in serialized
    assert "private_debug" not in serialized
    assert bundle["basis"]["candidates"][0]["importance"] == "high"
    assert bundle["basis"]["candidates"][0]["title_provenance"] == "source"


@pytest.mark.parametrize("importance", [None, "unknown", 7, True])
def test_candidate_importance_is_required_and_categorical(importance: object) -> None:
    event = _event("event:importance")
    if importance is None:
        event.pop("importance")
    else:
        event["importance"] = importance
    with pytest.raises(
        GlobalBriefValidationError,
        match="invalid_brief_candidate_importance",
    ):
        _bundle(candidates=[event])


@pytest.mark.parametrize(
    "title_provenance",
    [None, "", "inferred", 7, True],
)
def test_candidate_title_provenance_is_required(
    title_provenance: object,
) -> None:
    event = _event("event:title-provenance")
    if title_provenance is None:
        event.pop("title_provenance")
    else:
        event["title_provenance"] = title_provenance
    with pytest.raises(
        GlobalBriefValidationError,
        match="invalid_brief_candidate_title_provenance",
    ):
        _bundle(candidates=[event])


def test_empty_bundle_distinguishes_no_event_from_coverage_failure() -> None:
    ready = _bundle(candidates=[])
    unavailable = _bundle(candidates=[], statuses=[_source("US", ready=False)])
    assert (
        ready["approval_template"]["publication"]["empty_reason"]
        == "no_confirmed_material_events"
    )
    assert (
        unavailable["approval_template"]["publication"]["empty_reason"]
        == "coverage_unavailable"
    )
    assert ready["basis"]["source_snapshot"]["readiness"]["ready"] is True
    assert unavailable["basis"]["source_snapshot"]["readiness"]["ready"] is False


def test_collection_freshness_does_not_imply_public_brief_readiness() -> None:
    status = _source("US", ready=True, public_ready=False)
    bundle = _bundle(candidates=[], statuses=[status])
    snapshot = bundle["basis"]["source_snapshot"]
    item = snapshot["items"][0]
    assert item["collect_fresh"] is True
    assert item["fresh"] is True
    assert item["public_status"] == "redistribution_blocked"
    assert item["public_ready"] is False
    assert snapshot["readiness"]["ready"] is False
    assert (
        bundle["approval_template"]["publication"]["empty_reason"]
        == "coverage_unavailable"
    )


def test_global_readiness_requires_all_four_alpha_countries() -> None:
    statuses = [_source(country) for country in ("KR", "US", "JP", "GB", "CA")]
    bundle = _bundle(candidates=[], statuses=statuses, edition="global")
    readiness = bundle["basis"]["source_snapshot"]["readiness"]
    assert readiness["ready"] is False
    assert readiness["unavailable_countries"] == ["AU"]
    assert (
        bundle["approval_template"]["publication"]["empty_reason"]
        == "coverage_unavailable"
    )


def test_optional_jp_gb_coverage_unavailable_does_not_block_global_brief() -> None:
    statuses = [_source(country) for country in ("KR", "US", "CA", "AU")]
    for country in ("JP", "GB"):
        item = _source(country, ready=False, public_ready=False)
        item.update(
            {
                "coverage_mode": "link-only",
                "status": "inactive",
                "collect_status": "inactive",
                "public_status": "coverage_unavailable",
                "fresh": False,
                "collect_fresh": False,
                "public_ready": False,
                "raw_count": 0,
                "acknowledged_count": 0,
            }
        )
        statuses.append(item)
    bundle = _bundle(candidates=[], statuses=statuses, edition="global")
    readiness = bundle["basis"]["source_snapshot"]["readiness"]
    assert readiness["ready"] is True
    assert readiness["required_countries"] == ["KR", "US", "CA", "AU"]
    assert (
        bundle["approval_template"]["publication"]["empty_reason"]
        == "no_confirmed_material_events"
    )


@pytest.mark.parametrize(
    "public_status",
    ("delayed", "blocked_identity", "blocked_policy_activity"),
)
def test_api_public_status_variants_are_preserved_fail_closed(
    public_status: str,
) -> None:
    status = _source("US", ready=False, public_ready=False)
    status["public_status"] = public_status
    bundle = _bundle(candidates=[], statuses=[status], edition="US")
    item = bundle["basis"]["source_snapshot"]["items"][0]
    assert item["public_status"] == public_status
    assert item["public_ready"] is False
    assert bundle["basis"]["source_snapshot"]["readiness"]["ready"] is False
    assert (
        bundle["approval_template"]["publication"]["empty_reason"]
        == "coverage_unavailable"
    )


def test_client_uses_separate_editor_and_preview_tokens_and_strict_v2() -> None:
    requests: list[httpx.Request] = []
    event = _event("event:one")
    source = _source("US")

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/admin/brief-candidates"):
            assert request.headers["authorization"] == f"Bearer {EDITOR_TOKEN}"
            assert request.url.params["country"] == "US"
            assert request.url.params["limit"] == "100"
            return httpx.Response(200, json=_candidate_response([event]))
        if request.url.path.endswith("/sources/status"):
            assert request.headers["authorization"] == f"Bearer {PREVIEW_TOKEN}"
            assert request.url.params["country"] == "US"
            return httpx.Response(200, json=_status_response([source]))
        raise AssertionError(request.url)

    client = V2GlobalBriefClient(
        base_url="https://example.test/activist/api.php/api/v1",
        editor_token=EDITOR_TOKEN,
        preview_token=PREVIEW_TOKEN,
        transport=httpx.MockTransport(handler),
    )
    assert client.fetch_candidates(edition="US")[0]["event_id"] == "event:one"
    checked_at, statuses = client.fetch_source_status(edition="US")
    assert checked_at == "2026-07-24T00:00:00+00:00"
    assert statuses[0]["fresh"] is True
    assert len(requests) == 2


@pytest.mark.parametrize("status", [401, 403, 409, 503])
def test_client_fails_closed_on_auth_conflict_or_unavailable(status: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status,
            json={
                "ok": False,
                "api_version": "v2",
                "error": "request_rejected",
            },
        )

    client = V2GlobalBriefClient(
        base_url="https://example.test/api/v2",
        editor_token=EDITOR_TOKEN,
        preview_token=PREVIEW_TOKEN,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(GlobalBriefApiError) as raised:
        client.fetch_candidates(edition="global")
    assert raised.value.http_status == status


def test_local_candidate_and_source_responses_cannot_cross_markets() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/admin/brief-candidates"):
            return httpx.Response(
                200,
                json=_candidate_response([_event("event:jp", country="JP")]),
            )
        return httpx.Response(200, json=_status_response([_source("JP")]))

    client = V2GlobalBriefClient(
        base_url="https://example.test/api/v2",
        editor_token=EDITOR_TOKEN,
        preview_token=PREVIEW_TOKEN,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(GlobalBriefApiError, match="candidate_country_mismatch"):
        client.fetch_candidates(edition="US")
    with pytest.raises(GlobalBriefApiError, match="source_status_country_mismatch"):
        client.fetch_source_status(edition="US")


def test_human_approval_must_be_explicit_current_and_same_revision() -> None:
    bundle = _bundle()
    pending = deepcopy(bundle["approval_template"])
    with pytest.raises(GlobalBriefValidationError, match="human_approval_required"):
        validate_human_approval(
            pending,
            expected_revision=REVISION,
            now=NOW,
        )
    approved = _approve(bundle)
    with pytest.raises(GlobalBriefValidationError, match="approved_revision_mismatch"):
        validate_human_approval(
            approved,
            expected_revision="b" * 40,
            now=NOW,
        )
    approved["approval"]["approved_at"] = "2026-07-22T00:00:00+00:00"
    with pytest.raises(GlobalBriefValidationError, match="human_approval_expired"):
        validate_human_approval(
            approved,
            expected_revision=REVISION,
            now=NOW,
        )


def test_publication_rejects_one_event_across_multiple_lanes() -> None:
    with pytest.raises(
        GlobalBriefValidationError,
        match="duplicate_brief_item",
    ):
        validate_publication(
            {
                "edition": "US",
                "cutoff_at": CUTOFF,
                "build_sha": REVISION,
                "empty_reason": None,
                "items": [
                    {
                        "event_id": "event:one",
                        "lane": "top",
                        "position_no": 1,
                        "selection_reason": "Top selection.",
                    },
                    {
                        "event_id": "event:one",
                        "lane": "watch",
                        "position_no": 1,
                        "selection_reason": "Duplicate watch selection.",
                    },
                ],
            }
        )


def test_candidate_bundle_hash_and_revision_are_verified_before_publish() -> None:
    bundle = _bundle()
    validated = validate_candidate_bundle(
        bundle,
        expected_revision=REVISION,
    )
    assert validated["candidate_bundle_sha256"] == bundle[
        "candidate_bundle_sha256"
    ]
    tampered = deepcopy(bundle)
    tampered["basis"]["candidates"][0]["title"] = "Tampered after generation"
    with pytest.raises(
        GlobalBriefValidationError,
        match="candidate_bundle_hash_mismatch",
    ):
        validate_candidate_bundle(tampered, expected_revision=REVISION)
    with pytest.raises(
        GlobalBriefValidationError,
        match="candidate_bundle_revision_mismatch",
    ):
        validate_candidate_bundle(bundle, expected_revision="b" * 40)


def test_approval_must_be_bound_to_bundle_candidates_and_versions() -> None:
    bundle = _bundle()
    approved = _approve(bundle)
    approved["selected_event_versions"]["event:one"] = (
        "2026-07-23T20:29:00+00:00"
    )
    client = V2GlobalBriefClient(
        base_url="https://example.test/api/v2",
        editor_token=EDITOR_TOKEN,
        transport=httpx.MockTransport(
            lambda request: (_ for _ in ()).throw(AssertionError(request.url))
        ),
    )
    with pytest.raises(
        GlobalBriefValidationError,
        match="approved_version_not_in_candidate_bundle",
    ):
        publish_human_approval(
            client=client,
            candidate_bundle=bundle,
            approval=approved,
            expected_revision=REVISION,
            now=NOW,
        )


def test_publish_rechecks_event_version_evidence_status_and_exact_receipt() -> None:
    bundle = _bundle()
    approved = _approve(bundle)
    expected_id = expected_brief_id(edition="US", cutoff_at=CUTOFF)
    posted: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/admin/brief-candidates"):
            return httpx.Response(200, json=_candidate_response([_event("event:one")]))
        if request.url.path.endswith("/sources/status"):
            return httpx.Response(200, json=_status_response([_source("US")]))
        if request.url.path.endswith("/admin/briefs"):
            assert request.headers["authorization"] == f"Bearer {EDITOR_TOKEN}"
            posted.update(json.loads(request.content))
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "api_version": "v2",
                    "data": {
                        "brief_id": expected_id,
                        "edition": "US",
                        "published": True,
                        "idempotent": False,
                        "top_count": 1,
                        "item_count": 1,
                        "empty_reason": None,
                    },
                },
            )
        raise AssertionError(request.url)

    client = V2GlobalBriefClient(
        base_url="https://example.test/api/v2",
        editor_token=EDITOR_TOKEN,
        preview_token=PREVIEW_TOKEN,
        transport=httpx.MockTransport(handler),
    )
    receipt = publish_human_approval(
        client=client,
        candidate_bundle=bundle,
        approval=approved,
        expected_revision=REVISION,
        now=NOW,
    )
    assert receipt["kind"] == RECEIPT_KIND
    assert receipt["contains_secrets"] is False
    assert receipt["publication"]["brief_id"] == expected_id
    assert receipt["publication"]["api_version"] == "v2"
    assert posted == approved["publication"]


def test_publish_refuses_changed_event_before_post() -> None:
    bundle = _bundle()
    approved = _approve(bundle)
    posted = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal posted
        if request.url.path.endswith("/admin/brief-candidates"):
            changed = _event(
                "event:one",
                updated_at="2026-07-23 20:31:00",
            )
            return httpx.Response(200, json=_candidate_response([changed]))
        if request.url.path.endswith("/admin/briefs"):
            posted = True
        raise AssertionError(request.url)

    client = V2GlobalBriefClient(
        base_url="https://example.test/api/v2",
        editor_token=EDITOR_TOKEN,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(
        GlobalBriefValidationError,
        match="approved_event_version_changed",
    ):
        publish_human_approval(
            client=client,
            candidate_bundle=bundle,
            approval=approved,
            expected_revision=REVISION,
            now=NOW,
        )
    assert posted is False


def test_empty_publication_is_rechecked_against_current_source_state() -> None:
    bundle = _bundle(candidates=[], statuses=[_source("US", ready=False)])
    approved = _approve(bundle)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/admin/brief-candidates"):
            return httpx.Response(200, json=_candidate_response([]))
        if request.url.path.endswith("/sources/status"):
            return httpx.Response(200, json=_status_response([_source("US")]))
        raise AssertionError(request.url)

    client = V2GlobalBriefClient(
        base_url="https://example.test/api/v2",
        editor_token=EDITOR_TOKEN,
        preview_token=PREVIEW_TOKEN,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(
        GlobalBriefValidationError,
        match="brief_coverage_is_available",
    ):
        publish_human_approval(
            client=client,
            candidate_bundle=bundle,
            approval=approved,
            expected_revision=REVISION,
            now=NOW,
        )


def test_publication_response_must_contain_the_exact_expected_brief_id() -> None:
    publication = _approve(_bundle())["publication"]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "ok": True,
                "api_version": "v2",
                "data": {
                    "brief_id": "brief:wrong",
                    "edition": "US",
                    "published": True,
                    "idempotent": True,
                },
            },
        )

    client = V2GlobalBriefClient(
        base_url="https://example.test/api/v2",
        editor_token=EDITOR_TOKEN,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(
        GlobalBriefApiError,
        match="brief_publication_acknowledgment_mismatch",
    ):
        client.publish(publication)


@pytest.mark.parametrize("mode", ["", "off", "dart_canary", "preview"])
def test_pipeline_mode_fails_closed_outside_shadow_or_live(mode: str) -> None:
    with pytest.raises(
        GlobalBriefConfigurationError,
        match="global_brief_pipeline_inactive",
    ):
        _validate_pipeline_mode(mode)
    assert _validate_pipeline_mode("shadow") == "shadow"
    assert _validate_pipeline_mode("LIVE") == "live"


def test_approval_input_requires_exactly_one_json_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    approval = _approve(_bundle())
    encoded = base64.b64encode(json.dumps(approval).encode()).decode()
    monkeypatch.setenv("APPROVAL_B64", encoded)
    monkeypatch.delenv("APPROVAL_JSON", raising=False)
    parsed = _approval_from_sources(
        file_path="",
        json_env_name="APPROVAL_JSON",
        base64_env_name="APPROVAL_B64",
    )
    assert parsed == approval
    path = tmp_path / "approval.json"
    path.write_text(json.dumps(approval), encoding="utf-8")
    with pytest.raises(
        GlobalBriefConfigurationError,
        match="exactly_one_approval_payload_required",
    ):
        _approval_from_sources(
            file_path=str(path),
            json_env_name="APPROVAL_JSON",
            base64_env_name="APPROVAL_B64",
        )


def test_workflow_schedules_candidate_only_at_0545_kst_and_manual_publish() -> None:
    text = WORKFLOW_PATH.read_text(encoding="utf-8")
    workflow = yaml.safe_load(text)
    trigger = workflow.get("on") or workflow.get(True)
    assert trigger["schedule"] == [{"cron": "45 20 * * *"}]
    assert set(workflow["jobs"]) == {"candidates", "publish"}
    assert "github.event_name == 'schedule'" in workflow["jobs"]["candidates"]["if"]
    assert "github.event_name == 'workflow_dispatch'" in workflow["jobs"]["publish"]["if"]
    assert "inputs.operation == 'publish'" in workflow["jobs"]["publish"]["if"]
    assert workflow["concurrency"]["cancel-in-progress"] is False
    assert workflow["jobs"]["publish"]["concurrency"] == {
        "group": (
            "governance-production-official-write-"
            "${{ github.repository }}-${{ github.ref }}"
        ),
        "queue": "max",
        "cancel-in-progress": False,
    }
    assert "vars.GOVERNANCE_PIPELINE_MODE == 'shadow'" in text
    assert "vars.GOVERNANCE_PIPELINE_MODE == 'live'" in text
    assert "BSIDE_EDITOR_TOKEN" in text
    assert "GOVERNANCE_PREVIEW_TOKEN" in text
    assert "approval-json-env" in text
    assert "approval-base64-env" in text
    assert trigger["workflow_dispatch"]["inputs"]["candidate_run_id"][
        "default"
    ] == ""
    assert "actions: read" in text
    assert "candidate-bundle-file" in text
    assert "CANDIDATE_RUN_ID" in text
    assert '.head_sha == $sha' in text
    assert '.head_branch == $branch' in text
    assert '.path == ".github/workflows/global-brief.yml"' in text
    assert ".run_attempt" in text
    assert 'expected_name="global-brief-candidates-' in text
    assert ".name == $name" in text
    assert "exactly one candidate artifact is required" in text
    assert (
        "actions/download-artifact@"
        "3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    ) in text
    assert text.count(
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
    ) == 2
    assert "telegram" not in text.casefold()
    candidate_commands = json.dumps(workflow["jobs"]["candidates"], ensure_ascii=False)
    assert "global_brief candidates" in candidate_commands
    assert "global_brief publish" not in candidate_commands
    publish_commands = json.dumps(workflow["jobs"]["publish"], ensure_ascii=False)
    assert "global_brief publish" in publish_commands
    assert workflow["jobs"]["publish"]["environment"]["name"] == "governance-release"
