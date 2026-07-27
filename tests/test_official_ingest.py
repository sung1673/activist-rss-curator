from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from curator import official_ingest


BACKEND_BINDING_ID = "b" * 64
DART_RIGHTS_REVISION = "c" * 64
DART_CONTRACT_REVISION = "d" * 64
DART_DEPLOYMENT_REVISION = "e" * 40


def _allow_dart_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_SHA", DART_DEPLOYMENT_REVISION)
    monkeypatch.setenv("GOVERNANCE_PIPELINE_MODE", "dart_canary")

    class EligibleDartRightsClient:
        def preflight(
            self,
            _expected_release_sha: str | None = None,
        ) -> official_ingest.OfficialSourceRightEligibility:
            assert _expected_release_sha == DART_DEPLOYMENT_REVISION
            return official_ingest.OfficialSourceRightEligibility(
                source_right_id="official:dart",
                use="collect",
                rights_revision=DART_RIGHTS_REVISION,
                source_type="official_disclosure",
                source_key="dart",
                redistribution_allowed=True,
                ai_allowed=False,
                contract_revision=DART_CONTRACT_REVISION,
                release_state="closed",
            )

    monkeypatch.setattr(
        official_ingest,
        "DartOfficialSourceRightClient",
        EligibleDartRightsClient,
    )


def _set_scheduled_claim_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    schedule: str,
    slot: str,
    trigger: str,
    claimed: str,
    next_slot: str,
    trigger_lag: int,
    claim_lag: int,
    late: bool,
) -> None:
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("CURATOR_EVENT_SCHEDULE", schedule)
    monkeypatch.setenv("GITHUB_RUN_ID", "123456789")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.setenv("CURATOR_GITHUB_RUN_CREATED_AT", trigger)
    monkeypatch.setenv("CURATOR_OFFICIAL_SLOT_CLAIM_ID", "official-slot:claim-1")
    monkeypatch.setenv("CURATOR_OFFICIAL_SCHEDULED_SLOT_AT", slot)
    monkeypatch.setenv("CURATOR_OFFICIAL_SLOT_CLAIMED_AT", claimed)
    monkeypatch.setenv("CURATOR_OFFICIAL_NEXT_CADENCE_SLOT_AT", next_slot)
    monkeypatch.setenv("CURATOR_OFFICIAL_TRIGGER_LAG_SECONDS", str(trigger_lag))
    monkeypatch.setenv("CURATOR_OFFICIAL_CLAIM_LAG_SECONDS", str(claim_lag))
    monkeypatch.setenv("CURATOR_OFFICIAL_SLOT_LATE", "1" if late else "0")
    monkeypatch.setenv("CURATOR_GITHUB_RUN_ID", "123456789")
    monkeypatch.setenv("CURATOR_GITHUB_RUN_ATTEMPT", "1")


def test_remote_sync_persists_one_final_failed_run_after_all_data_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_post(action: str, payload: dict[str, object], *, timeout: float) -> dict[str, object]:
        assert action == "upsert_governance_snapshot_dart_guarded"
        assert timeout == 45.0
        assert payload["expected_backend_binding_id"] == BACKEND_BINDING_ID
        calls.append(payload)
        if len(calls) == 2:
            return {"ok": False}
        return {
            "ok": True,
            "backend_binding_id": BACKEND_BINDING_ID,
            "upserted": {
                key: len(payload[key])
                for key in ("companies", "documents", "events", "source_rights")
            }
            | {
                "source_rights_rejected": 0,
                "runs": int(bool(payload["run"])),
            },
        }

    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", BACKEND_BINDING_ID)
    monkeypatch.setattr(official_ingest, "remote_api_configured", lambda: True)
    monkeypatch.setattr(official_ingest, "post_remote_action", fake_post)
    payload: dict[str, object] = {
        "companies": [],
        "documents": [{"document_id": f"dart:{index}"} for index in range(3601)],
        "events": [],
        "source_rights": [],
        "expected_source_right_revisions": {
            "official:dart": {
                "rights_revision": DART_RIGHTS_REVISION,
                "contract_revision": DART_CONTRACT_REVISION,
            }
        },
        "expected_deployment_code_revision": DART_DEPLOYMENT_REVISION,
        "expected_release_state": "closed",
    }
    run = {
        "run_id": "run:official-test",
        "status": "succeeded",
        "error_count": 2,
        "source_outcomes": {"dart": {"status": "succeeded"}},
    }

    summary = official_ingest.sync_governance_payload(payload, run=run)

    assert len(calls) == 4
    assert all(call["run"] == {} for call in calls[:-1])
    assert all(call["source_rights"] == [] for call in calls)
    assert all(
        call["expected_source_right_revisions"]
        == payload["expected_source_right_revisions"]
        for call in calls
    )
    assert all(
        call["expected_deployment_code_revision"]
        == DART_DEPLOYMENT_REVISION
        for call in calls
    )
    assert all(call["expected_release_state"] == "closed" for call in calls)
    final_run = calls[-1]["run"]
    assert isinstance(final_run, dict)
    assert final_run["status"] == "failed"
    assert final_run["error_count"] == 3
    assert final_run["remote_data_batches_attempted"] == 3
    assert final_run["remote_data_batches_succeeded"] == 2
    assert final_run["remote_data_batches_failed"] == 1
    assert final_run["raw_count"] == 3601
    assert final_run["ack_count"] == 1801
    assert final_run["source_ack_counts"] == {"unknown": 1801}
    assert final_run["source_outcomes"] == run["source_outcomes"]
    assert summary == {
        "official_remote_synced": 2,
        "official_remote_failed": 1,
        "official_remote_skipped": 0,
        "official_remote_batches_attempted": 3,
        "official_remote_run_persisted": 1,
        "official_remote_ack_mismatches": 0,
        "official_remote_raw_count": 3601,
        "official_remote_ack_count": 1801,
    }


def test_remote_sync_company_master_only_chunk_keeps_exact_dart_guards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_post(
        action: str,
        payload: dict[str, object],
        *,
        timeout: float,
    ) -> dict[str, object]:
        assert timeout == 45.0
        calls.append((action, payload))
        return {
            "ok": True,
            "backend_binding_id": BACKEND_BINDING_ID,
            "upserted": {
                key: len(payload[key])
                for key in ("companies", "documents", "events", "source_rights")
            }
            | {
                "source_rights_rejected": 0,
                "runs": int(bool(payload["run"])),
            },
        }

    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", BACKEND_BINDING_ID)
    monkeypatch.setattr(official_ingest, "remote_api_configured", lambda: True)
    monkeypatch.setattr(official_ingest, "post_remote_action", fake_post)
    payload: dict[str, object] = {
        "companies": [
            {
                "company_id": "00126380",
                "legal_name": "DART company master only",
            }
        ],
        "documents": [],
        "events": [],
        "source_rights": [],
        "expected_source_right_revisions": {
            "official:dart": {
                "rights_revision": DART_RIGHTS_REVISION,
                "contract_revision": DART_CONTRACT_REVISION,
            }
        },
        "expected_deployment_code_revision": DART_DEPLOYMENT_REVISION,
        "expected_release_state": "closed",
    }

    summary = official_ingest.sync_governance_payload(
        payload,
        run={
            "run_id": "run:dart-company-master-only",
            "source_key": "dart",
            "status": "succeeded",
            "error_count": 0,
        },
    )

    company_only_calls = [
        submitted
        for action, submitted in calls
        if submitted["companies"]
        and submitted["documents"] == []
        and submitted["events"] == []
        and submitted["run"] == {}
    ]
    assert len(company_only_calls) == 1
    company_only = company_only_calls[0]
    assert all(
        action == "upsert_governance_snapshot_dart_guarded"
        for action, _submitted in calls
    )
    assert (
        company_only["expected_source_right_revisions"]
        == payload["expected_source_right_revisions"]
    )
    assert (
        company_only["expected_deployment_code_revision"]
        == DART_DEPLOYMENT_REVISION
    )
    assert company_only["expected_release_state"] == "closed"
    assert company_only["expected_backend_binding_id"] == BACKEND_BINDING_ID
    assert summary["official_remote_failed"] == 0
    assert summary["official_remote_synced"] == 2
    assert summary["official_remote_run_persisted"] == 1


def test_remote_sync_persists_explicit_zero_ack_for_selected_empty_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_post(
        _action: str, payload: dict[str, object], *, timeout: float
    ) -> dict[str, object]:
        assert timeout == 45.0
        assert payload["expected_backend_binding_id"] == BACKEND_BINDING_ID
        calls.append(payload)
        return {
            "ok": True,
            "backend_binding_id": BACKEND_BINDING_ID,
            "upserted": {
                key: len(payload[key])
                for key in ("companies", "documents", "events", "source_rights")
            }
            | {"source_rights_rejected": 0, "runs": int(bool(payload["run"]))},
        }

    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", BACKEND_BINDING_ID)
    monkeypatch.setattr(official_ingest, "remote_api_configured", lambda: True)
    monkeypatch.setattr(official_ingest, "post_remote_action", fake_post)

    summary = official_ingest.sync_governance_payload(
        {"companies": [], "documents": [], "events": [], "source_rights": []},
        run={
            "run_id": "run:empty-official-test",
            "source_key": "dart+kind",
            "status": "succeeded",
            "raw_count": 0,
            "source_outcomes": {
                "dart": {"status": "succeeded", "raw_count": 0},
                "kind": {"status": "succeeded", "raw_count": 0},
            },
        },
    )

    assert summary["official_remote_failed"] == 0
    final_run = calls[-1]["run"]
    assert isinstance(final_run, dict)
    assert final_run["source_ack_counts"] == {"dart": 0, "kind": 0}


@pytest.mark.parametrize(
    "bad_upserted",
    (
        {"companies": 1, "documents": 0, "events": 0, "source_rights": 1, "source_rights_rejected": 0, "runs": 0},
        {"companies": 1, "documents": 1, "events": 0, "source_rights": 1, "source_rights_rejected": 1, "runs": 0},
        {"companies": 1, "documents": 1, "events": 0, "source_rights": 1},
    ),
)
def test_remote_sync_fails_closed_on_partial_or_rejected_ack(
    monkeypatch: pytest.MonkeyPatch,
    bad_upserted: dict[str, int],
) -> None:
    call_count = 0

    def fake_post(_action: str, payload: dict[str, object], *, timeout: float) -> dict[str, object]:
        nonlocal call_count
        assert timeout == 45.0
        call_count += 1
        if call_count == 1:
            return {
                "ok": True,
                "backend_binding_id": BACKEND_BINDING_ID,
                "upserted": bad_upserted,
            }
        return {
            "ok": True,
            "backend_binding_id": BACKEND_BINDING_ID,
            "upserted": {
                "companies": 0,
                "documents": 0,
                "events": 0,
                "source_rights": 0,
                "source_rights_rejected": 0,
                "runs": int(bool(payload["run"])),
            },
        }

    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", BACKEND_BINDING_ID)
    monkeypatch.setattr(official_ingest, "remote_api_configured", lambda: True)
    monkeypatch.setattr(official_ingest, "post_remote_action", fake_post)
    summary = official_ingest.sync_governance_payload(
        {
            "companies": [{"company_id": "00126380"}],
            "documents": [{"document_id": "dart:1", "company_id": "00126380"}],
            "events": [],
            "source_rights": [{"source_right_id": "official:dart"}],
        },
        run={"run_id": "run:ack-test", "status": "succeeded", "error_count": 0},
    )

    assert summary["official_remote_synced"] == 0
    assert summary["official_remote_failed"] == 1
    assert summary["official_remote_ack_mismatches"] == 1
    assert summary["official_remote_run_persisted"] == 1


@pytest.mark.parametrize(
    "acknowledged_binding_id",
    (None, "c" * 64, "한" * 64),
)
def test_remote_governance_ack_requires_matching_backend_binding(
    monkeypatch: pytest.MonkeyPatch,
    acknowledged_binding_id: str | None,
) -> None:
    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", BACKEND_BINDING_ID)
    payload: dict[str, object] = {
        "companies": [],
        "documents": [],
        "events": [],
        "source_rights": [],
        "run": {},
        "expected_backend_binding_id": BACKEND_BINDING_ID,
    }
    response: dict[str, object] = {
        "ok": True,
        "upserted": {
            "companies": 0,
            "documents": 0,
            "events": 0,
            "source_rights": 0,
            "source_rights_rejected": 0,
            "runs": 0,
        },
    }
    if acknowledged_binding_id is not None:
        response["backend_binding_id"] = acknowledged_binding_id

    assert official_ingest._remote_acknowledges_payload(response, payload) is False


def test_remote_sync_stops_after_backend_binding_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_post(
        _action: str,
        payload: dict[str, object],
        *,
        timeout: float,
    ) -> dict[str, object]:
        assert timeout == 45.0
        calls.append(payload)
        return {"ok": False, "error": "backend_binding_mismatch"}

    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", BACKEND_BINDING_ID)
    monkeypatch.setattr(official_ingest, "remote_api_configured", lambda: True)
    monkeypatch.setattr(official_ingest, "post_remote_action", fake_post)

    summary = official_ingest.sync_governance_payload(
        {
            "companies": [],
            "documents": [
                {"document_id": f"dart:{index}"}
                for index in range(3_001)
            ],
            "events": [],
            "source_rights": [],
        },
        run={"run_id": "run:binding-rejection", "status": "succeeded"},
    )

    assert len(calls) == 1
    assert calls[0]["expected_backend_binding_id"] == BACKEND_BINDING_ID
    assert summary["official_remote_batches_attempted"] == 1
    assert summary["official_remote_failed"] == 1
    assert summary["official_remote_run_persisted"] == 0
    assert summary["official_remote_ack_count"] == 0


def test_remote_sync_rejects_invalid_backend_binding_before_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", "INVALID")
    monkeypatch.setattr(official_ingest, "remote_api_configured", lambda: True)

    def unexpected_post(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise AssertionError("invalid backend binding must block the network request")

    monkeypatch.setattr(official_ingest, "post_remote_action", unexpected_post)

    with pytest.raises(
        official_ingest.GovernanceBackendBindingError,
        match="64 lowercase hexadecimal",
    ):
        official_ingest.sync_governance_payload(
            {
                "companies": [],
                "documents": [],
                "events": [],
                "source_rights": [],
            },
            run={"run_id": "run:invalid-binding", "status": "succeeded"},
        )


def test_required_kind_without_endpoint_fails_and_records_source_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_run: dict[str, object] = {}

    def fake_sync(payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        assert payload["documents"] == []
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    monkeypatch.setenv("CURATOR_REQUIRE_KIND", "1")
    monkeypatch.delenv("KIND_DISCLOSURE_ENDPOINT", raising=False)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": False, "kind_enabled": True},
    )

    assert summary["official_failed"] == 1
    assert summary["official_kind_required"] == 1
    assert summary["official_kind_configured"] == 0
    assert summary["official_kind_errors"] == 1
    assert captured_run["status"] == "failed"
    assert captured_run["source_key"] == "kind"
    assert captured_run["fetched_count"] == 0
    assert captured_run["raw_count"] == 0
    outcomes = captured_run["source_outcomes"]
    assert isinstance(outcomes, dict)
    assert {key: outcomes["kind"][key] for key in (
        "enabled",
        "required",
        "configured",
        "fetched",
        "accepted",
        "error_count",
        "status",
    )} == {
        "enabled": True,
        "required": True,
        "configured": False,
        "fetched": 0,
        "accepted": 0,
        "error_count": 1,
        "status": "failed",
    }
    assert outcomes["kind"]["failure_kinds"]["configuration"] == 1


def test_explicit_kind_disable_skips_configured_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payload: dict[str, object] = {}
    captured_run: dict[str, object] = {}

    class UnexpectedKindConnector:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("KIND connector must not run in explicit DART-only mode")

    def fake_sync(payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        captured_payload.update(payload)
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    monkeypatch.setenv("KIND_DISCLOSURE_ENDPOINT", "https://kind-adapter.invalid/v1/disclosures")
    monkeypatch.setenv("CURATOR_ENABLE_KIND", "0")
    monkeypatch.setenv("CURATOR_REQUIRE_KIND", "0")
    monkeypatch.setattr(official_ingest, "KindConnector", UnexpectedKindConnector)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": False, "kind_enabled": True},
    )

    assert summary["official_failed"] == 0
    assert summary["official_kind_enabled"] == 0
    assert summary["official_kind_configured"] == 1
    assert captured_payload["source_rights"] == []
    outcomes = captured_run["source_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["kind"]["requested"] is True
    assert outcomes["kind"]["enabled"] is False
    assert outcomes["kind"]["status"] == "disabled"


def test_kind_preflight_missing_runtime_config_fails_before_connector_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_run: dict[str, object] = {}

    class UnexpectedKindConnector:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("KIND connector must not be constructed before rights approval")

    def fake_sync(_payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    monkeypatch.setenv("KIND_DISCLOSURE_ENDPOINT", "https://kind-adapter.example/v1/disclosures")
    monkeypatch.setenv("CURATOR_ENABLE_KIND", "1")
    monkeypatch.setenv("CURATOR_REQUIRE_KIND", "1")
    for name in (
        "BSIDE_API_BASE_URL",
        "GOVERNANCE_API_BASE_URL",
        "ACTIVIST_API_URL",
        "BSIDE_OPS_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(official_ingest, "KindConnector", UnexpectedKindConnector)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": False, "kind_enabled": True},
    )

    assert summary["official_failed"] == 1
    assert summary["official_kind_rights_verified"] == 0
    assert summary["official_kind_fetched"] == 0
    outcomes = captured_run["source_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["kind"]["rights_checked"] is True
    assert outcomes["kind"]["rights_eligible"] is False
    assert outcomes["kind"]["rights_revision"] is None
    assert outcomes["kind"]["failure_kinds"]["rights"] == 1


@pytest.mark.parametrize(
    "failure",
    (
        "network failure",
        "invalid JSON",
        "HTTP 500",
        "KIND SourceRight is ineligible",
    ),
)
def test_kind_preflight_failure_never_constructs_connector(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    class FailedRightsClient:
        def check_kind_ingest(self) -> object:
            raise official_ingest.OfficialSourceRightError(failure)

    class UnexpectedKindConnector:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("KIND connector must not be constructed after preflight failure")

    monkeypatch.setenv("KIND_DISCLOSURE_ENDPOINT", "https://kind-adapter.example/v1/disclosures")
    monkeypatch.setenv("CURATOR_ENABLE_KIND", "1")
    monkeypatch.setenv("CURATOR_REQUIRE_KIND", "1")
    monkeypatch.setattr(official_ingest, "OfficialSourceRightClient", FailedRightsClient)
    monkeypatch.setattr(official_ingest, "KindConnector", UnexpectedKindConnector)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": False, "kind_enabled": True},
        dry_run=True,
    )

    assert summary["official_failed"] == 1
    assert summary["official_kind_rights_verified"] == 0
    assert summary["official_kind_fetched"] == 0


def test_kind_preflight_revision_is_stored_without_collector_managed_rights(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    revision = "b" * 64
    captured_payload: dict[str, object] = {}
    captured_run: dict[str, object] = {}

    class EligibleRightsClient:
        def check_kind_ingest(self) -> official_ingest.OfficialSourceRightEligibility:
            return official_ingest.OfficialSourceRightEligibility(
                source_right_id="official:kind",
                use="ingest",
                rights_revision=revision,
                checked_at="2026-07-16T00:00:00+00:00",
            )

    class EmptyKindConnector:
        list_requests = 1
        pages_fetched = 1
        rows_fetched = 0

        def __init__(self, endpoint: str, *, api_key: str) -> None:
            assert endpoint == "https://kind-adapter.example/v1/disclosures"
            assert api_key == "adapter-key"

        def iter_disclosure_rows(self, *_args: object, **_kwargs: object):  # type: ignore[no-untyped-def]
            return iter(())

    def fake_sync(payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        captured_payload.update(payload)
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    monkeypatch.setenv("KIND_DISCLOSURE_ENDPOINT", "https://kind-adapter.example/v1/disclosures")
    monkeypatch.setenv("KIND_API_KEY", "adapter-key")
    monkeypatch.setenv("CURATOR_ENABLE_KIND", "1")
    monkeypatch.setenv("CURATOR_REQUIRE_KIND", "1")
    monkeypatch.setattr(official_ingest, "OfficialSourceRightClient", EligibleRightsClient)
    monkeypatch.setattr(official_ingest, "KindConnector", EmptyKindConnector)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": False, "kind_enabled": True},
    )

    assert summary["official_failed"] == 0
    assert summary["official_kind_rights_verified"] == 1
    assert captured_payload["source_rights"] == []
    outcomes = captured_run["source_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["kind"]["rights_eligible"] is True
    assert outcomes["kind"]["rights_revision"] == revision
    metrics = captured_run["metrics"]
    assert isinstance(metrics, dict)
    assert metrics["kind_rights_revision"] == revision


def test_connector_failure_discards_partial_dart_window_before_remote_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payload: dict[str, object] = {}
    captured_run: dict[str, object] = {}

    class PartialDartConnector:
        list_requests = 2
        pages_fetched = 1
        rows_fetched = 1

        def __init__(self, _api_key: str) -> None:
            pass

        def iter_disclosure_rows(self, *_args: object, **_kwargs: object):  # type: ignore[no-untyped-def]
            yield {
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "stock_code": "005930",
                "corp_cls": "Y",
                "report_nm": "주요사항보고서(자기주식취득결정)",
                "rcept_no": "20260716000123",
                "flr_nm": "삼성전자",
                "rcept_dt": "20260716",
                "rm": "",
            }
            raise RuntimeError("page 2 failed")

        def fetch_company_master(self) -> list[dict[str, object]]:
            return []

    def fake_sync(payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        captured_payload.update(payload)
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    monkeypatch.setenv("DART_API_KEY", "a" * 40)
    _allow_dart_apply(monkeypatch)
    monkeypatch.setattr(official_ingest, "DartConnector", PartialDartConnector)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"kind_enabled": False},
    )

    assert captured_payload["documents"] == []
    assert captured_payload["events"] == []
    assert summary["official_fetched"] == 1
    assert summary["official_dart_accepted"] == 0
    assert summary["official_dart_discarded"] == 1
    assert summary["official_dart_errors"] == 1
    assert summary["official_failed"] == 1
    assert captured_run["fetched_count"] == 1
    assert captured_run["raw_count"] == 0
    outcomes = captured_run["source_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["dart"]["fetched"] == 1
    assert outcomes["dart"]["raw_count"] == 0
    assert outcomes["dart"]["failure_kinds"]["connector"] == 1
    assert outcomes["dart"]["pages_fetched"] == 1


def test_direct_dart_apply_requires_protected_preflight_before_connector_or_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnexpectedDartConnector:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("OpenDART must not run before protected preflight")

    def unexpected_sync(
        _payload: dict[str, object],
        *,
        run: dict[str, object],
    ) -> dict[str, int]:
        raise AssertionError(f"remote write was attempted: {run}")

    monkeypatch.setenv("DART_API_KEY", "a" * 40)
    for name in (
        "BSIDE_API_BASE_URL",
        "GOVERNANCE_API_BASE_URL",
        "ACTIVIST_API_URL",
        "BSIDE_OPS_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(official_ingest, "DartConnector", UnexpectedDartConnector)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", unexpected_sync)

    with pytest.raises(
        official_ingest.OfficialSourceRightError,
        match="BSIDE_OPS_TOKEN",
    ):
        official_ingest.run(
            now=datetime(2026, 7, 16, tzinfo=timezone.utc),
            start=date(2026, 7, 15),
            end=date(2026, 7, 16),
            settings_overrides={"dart_enabled": True, "kind_enabled": False},
        )


def test_inactive_dart_connector_preflight_blocks_opendart_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class InactiveDartRightsClient:
        def preflight(
            self,
            _expected_release_sha: str | None = None,
        ) -> official_ingest.OfficialSourceRightEligibility:
            assert _expected_release_sha == DART_DEPLOYMENT_REVISION
            raise official_ingest.OfficialSourceRightError(
                "OpenDART connector is not configured for collection"
            )

    class UnexpectedDartConnector:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError(
                "OpenDART network client must not be constructed while inactive"
            )

    monkeypatch.setenv("GITHUB_SHA", DART_DEPLOYMENT_REVISION)
    monkeypatch.setenv("DART_API_KEY", "a" * 40)
    monkeypatch.setattr(
        official_ingest,
        "DartOfficialSourceRightClient",
        InactiveDartRightsClient,
    )
    monkeypatch.setattr(
        official_ingest,
        "DartConnector",
        UnexpectedDartConnector,
    )

    with pytest.raises(
        official_ingest.OfficialSourceRightError,
        match="connector is not configured",
    ):
        official_ingest.run(
            now=datetime(2026, 7, 16, tzinfo=timezone.utc),
            start=date(2026, 7, 15),
            end=date(2026, 7, 16),
            settings_overrides={"dart_enabled": True, "kind_enabled": False},
        )


def test_dart_dry_run_never_constructs_protected_preflight_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnexpectedRightsClient:
        def __init__(self) -> None:
            raise AssertionError("dry-run must not call the protected API")

    class EmptyDartConnector:
        list_requests = 0
        pages_fetched = 0
        rows_fetched = 0
        requests_made = 0

        def __init__(self, _credentials: object) -> None:
            pass

        def iter_disclosure_rows(
            self,
            *_args: object,
            **_kwargs: object,
        ):  # type: ignore[no-untyped-def]
            return iter(())

        def fetch_company_master(self) -> list[dict[str, object]]:
            return []

    monkeypatch.setenv("DART_API_KEY", "a" * 40)
    monkeypatch.setattr(
        official_ingest,
        "DartOfficialSourceRightClient",
        UnexpectedRightsClient,
    )
    monkeypatch.setattr(official_ingest, "DartConnector", EmptyDartConnector)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": True, "kind_enabled": False},
        dry_run=True,
    )
    assert summary["official_failed"] == 0
    assert summary["official_source_rights"] == 0


def test_dart_right_change_after_collection_blocks_remote_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_SHA", DART_DEPLOYMENT_REVISION)
    monkeypatch.setenv("GOVERNANCE_PIPELINE_MODE", "dart_canary")

    class ChangingRightsClient:
        def __init__(self) -> None:
            self.calls = 0

        def preflight(
            self,
            _expected_release_sha: str | None = None,
        ) -> official_ingest.OfficialSourceRightEligibility:
            assert _expected_release_sha == DART_DEPLOYMENT_REVISION
            self.calls += 1
            return official_ingest.OfficialSourceRightEligibility(
                source_right_id="official:dart",
                use="collect",
                rights_revision=("a" if self.calls == 1 else "b") * 64,
                contract_revision=DART_CONTRACT_REVISION,
                release_state="closed",
            )

    class EmptyDartConnector:
        list_requests = 0
        pages_fetched = 0
        rows_fetched = 0
        requests_made = 0

        def __init__(self, _credentials: object) -> None:
            pass

        def iter_disclosure_rows(
            self,
            *_args: object,
            **_kwargs: object,
        ):  # type: ignore[no-untyped-def]
            return iter(())

        def fetch_company_master(self) -> list[dict[str, object]]:
            return []

    def unexpected_sync(
        _payload: dict[str, object],
        *,
        run: dict[str, object],
    ) -> dict[str, int]:
        raise AssertionError(f"write attempted after SourceRight change: {run}")

    monkeypatch.setenv("DART_API_KEY", "a" * 40)
    monkeypatch.setattr(
        official_ingest,
        "DartOfficialSourceRightClient",
        ChangingRightsClient,
    )
    monkeypatch.setattr(official_ingest, "DartConnector", EmptyDartConnector)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", unexpected_sync)

    with pytest.raises(
        official_ingest.OfficialSourceRightError,
        match="changed between collection and write",
    ):
        official_ingest.run(
            now=datetime(2026, 7, 16, tzinfo=timezone.utc),
            start=date(2026, 7, 15),
            end=date(2026, 7, 16),
            settings_overrides={"dart_enabled": True, "kind_enabled": False},
        )


def test_enabled_dart_without_api_key_is_a_failed_source_not_a_skip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_run: dict[str, object] = {}

    def fake_sync(_payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    monkeypatch.delenv("DART_API_KEY", raising=False)
    _allow_dart_apply(monkeypatch)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": True, "kind_enabled": False},
    )

    assert summary["official_failed"] == 1
    assert summary["official_skipped"] == 0
    assert summary["official_dart_errors"] == 1
    outcomes = captured_run["source_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["dart"]["status"] == "failed"
    assert outcomes["dart"]["failure_kinds"]["configuration"] == 1


def test_ingest_passes_validated_opendart_pool_without_serializing_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_a, key_b, key_c = "a" * 40, "b" * 40, "c" * 40
    observed_ids: list[str] = []

    class EmptyPoolConnector:
        list_requests = 0
        pages_fetched = 0
        rows_fetched = 0
        requests_made = 0

        def __init__(self, credentials: object) -> None:
            values = tuple(credentials)  # type: ignore[arg-type]
            assert [credential.key for credential in values] == [key_a, key_b, key_c]
            observed_ids.extend(credential.credential_id for credential in values)

        def iter_disclosure_rows(self, *_args: object, **_kwargs: object):  # type: ignore[no-untyped-def]
            return iter(())

        def fetch_company_master(self) -> list[dict[str, object]]:
            return []

    monkeypatch.setenv(
        "OPENDART_API_KEYS",
        f"{key_a}\r\n{key_b},{key_c}",
    )
    monkeypatch.delenv("DART_API_KEY", raising=False)
    monkeypatch.setattr(official_ingest, "DartConnector", EmptyPoolConnector)

    summary = official_ingest.run(
        now=datetime(2026, 7, 26, tzinfo=timezone.utc),
        start=date(2026, 7, 26),
        end=date(2026, 7, 26),
        settings_overrides={"kind_enabled": False},
        dry_run=True,
    )

    assert summary["official_failed"] == 0
    assert len(observed_ids) == 3
    rendered = repr(summary)
    assert key_a not in rendered
    assert key_b not in rendered
    assert key_c not in rendered


def test_conflicting_pool_and_legacy_key_fail_closed_without_connector_or_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_a, key_b = "a" * 40, "b" * 40

    class UnexpectedConnector:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("invalid credential configuration must not construct connector")

    monkeypatch.setenv("OPENDART_API_KEYS", key_a)
    monkeypatch.setenv("DART_API_KEY", key_b)
    monkeypatch.setattr(official_ingest, "DartConnector", UnexpectedConnector)

    summary = official_ingest.run(
        now=datetime(2026, 7, 26, tzinfo=timezone.utc),
        start=date(2026, 7, 26),
        end=date(2026, 7, 26),
        settings_overrides={"kind_enabled": False},
        dry_run=True,
    )

    assert summary["official_failed"] == 1
    assert summary["official_dart_errors"] == 1
    assert key_a not in repr(summary)
    assert key_b not in repr(summary)


def test_durable_official_ingest_keeps_a_ten_thousand_request_invocation_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DurableQuota:
        limit = 40_000
        used = 0

        def consume(self, **_kwargs: object) -> object:
            return object()

        def block_020(self, _permit: object) -> None:
            return None

        def disable_901(self, _permit: object) -> None:
            return None

    durable = DurableQuota()
    captured: list[object] = []

    class EmptyConnector:
        list_requests = 0
        pages_fetched = 0
        rows_fetched = 0
        requests_made = 0

        def __init__(
            self,
            _credentials: object,
            *,
            request_budget: object,
        ) -> None:
            captured.append(request_budget)

        def iter_disclosure_rows(
            self,
            *_args: object,
            **_kwargs: object,
        ):  # type: ignore[no-untyped-def]
            return iter(())

        def fetch_company_master(self) -> list[dict[str, object]]:
            return []

    monkeypatch.setenv("OPENDART_API_KEYS", "a" * 40)
    monkeypatch.delenv("DART_API_KEY", raising=False)
    monkeypatch.setattr(
        official_ingest,
        "durable_dart_quota_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        official_ingest,
        "durable_dart_quota_required",
        lambda: False,
    )
    monkeypatch.setattr(
        official_ingest,
        "durable_dart_quota_client",
        lambda **_kwargs: durable,
    )
    monkeypatch.setattr(official_ingest, "DartConnector", EmptyConnector)

    summary = official_ingest.run(
        now=datetime(2026, 7, 26, tzinfo=timezone.utc),
        start=date(2026, 7, 26),
        end=date(2026, 7, 26),
        settings_overrides={"kind_enabled": False},
        dry_run=True,
    )

    assert summary["official_failed"] == 0
    assert len(captured) == 1
    budget = captured[0]
    assert isinstance(budget, official_ingest.DartInvocationQuota)
    assert budget.limit == 10_000
    assert budget.used == 0


def test_dart_quota_exhaustion_is_exposed_to_the_durable_backfill_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_run: dict[str, object] = {}

    class QuotaDartConnector:
        list_requests = 1
        pages_fetched = 0
        rows_fetched = 0
        requests_made = 1

        def __init__(self, _api_key: str, **_kwargs: object) -> None:
            pass

        def iter_disclosure_rows(self, *_args: object, **_kwargs: object):  # type: ignore[no-untyped-def]
            raise official_ingest.DartQuotaExceededError("status 020")
            yield  # pragma: no cover

    def fake_sync(_payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    monkeypatch.setenv("DART_API_KEY", "a" * 40)
    _allow_dart_apply(monkeypatch)
    monkeypatch.setattr(official_ingest, "DartConnector", QuotaDartConnector)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"kind_enabled": False},
    )

    assert summary["official_failed"] == 1
    assert summary["official_dart_quota_exhausted"] == 1
    outcomes = captured_run["source_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["dart"]["failure_kinds"]["quota"] == 1


def test_default_incremental_window_uses_kst_date_before_utc_midnight_rollover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[date, date]] = []

    class EmptyDartConnector:
        list_requests = 0
        pages_fetched = 0
        rows_fetched = 0

        def __init__(self, _api_key: str) -> None:
            pass

        def iter_disclosure_rows(self, start: date, end: date, **_kwargs: object):  # type: ignore[no-untyped-def]
            seen.append((start, end))
            return iter(())

        def fetch_company_master(self) -> list[dict[str, object]]:
            return []

    monkeypatch.setenv("DART_API_KEY", "a" * 40)
    monkeypatch.delenv("OFFICIAL_INGEST_START", raising=False)
    monkeypatch.delenv("OFFICIAL_INGEST_END", raising=False)
    monkeypatch.setattr(official_ingest, "DartConnector", EmptyDartConnector)

    summary = official_ingest.run(
        now=datetime(2026, 7, 15, 15, 5, tzinfo=timezone.utc),  # 2026-07-16 00:05 KST
        settings_overrides={"lookback_days": 2, "kind_enabled": False},
        dry_run=True,
    )

    assert summary["official_failed"] == 0
    assert seen == [(date(2026, 7, 14), date(2026, 7, 16))]


def test_scheduled_run_persists_exact_slot_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_run: dict[str, object] = {}

    def fake_sync(_payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    _set_scheduled_claim_env(
        monkeypatch,
        schedule="0,30 15-21 * * *",
        slot="2026-07-15T15:00:00Z",
        trigger="2026-07-15T15:02:00Z",
        claimed="2026-07-15T15:07:00Z",
        next_slot="2026-07-15T15:30:00Z",
        trigger_lag=120,
        claim_lag=420,
        late=False,
    )
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    official_ingest.run(
        now=datetime(2026, 7, 15, 15, 7, tzinfo=timezone.utc),
        start=date(2026, 7, 16),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": False, "kind_enabled": False},
    )

    assert captured_run["run_kind"] == "scheduled_incremental"
    assert captured_run["event_schedule"] == "0,30 15-21 * * *"
    assert captured_run["scheduled_slot_at"] == "2026-07-15T15:00:00+00:00"
    assert captured_run["trigger_created_at"] == "2026-07-15T15:02:00+00:00"
    assert captured_run["slot_claim_id"] == "official-slot:claim-1"
    assert captured_run["github_run_id"] == "123456789"
    assert captured_run["github_run_attempt"] == 1
    assert captured_run["slot_claimed_at"] == "2026-07-15T15:07:00+00:00"
    assert captured_run["next_cadence_slot_at"] == "2026-07-15T15:30:00+00:00"
    assert captured_run["trigger_lag_seconds"] == 120
    assert captured_run["claim_lag_seconds"] == 420
    assert captured_run["slot_claim_late"] is False
    assert captured_run["company_master_sync"] is False
    assert captured_run["metrics"]["scheduled_slot_at"] == captured_run["scheduled_slot_at"]  # type: ignore[index]


def test_scheduled_run_id_is_stable_for_the_same_durable_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_ids: list[str] = []

    def fake_sync(_payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        captured_ids.append(str(run["run_id"]))
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    _set_scheduled_claim_env(
        monkeypatch,
        schedule="0,30 15-21 * * *",
        slot="2026-07-15T15:00:00Z",
        trigger="2026-07-15T15:02:00Z",
        claimed="2026-07-15T15:07:00Z",
        next_slot="2026-07-15T15:30:00Z",
        trigger_lag=120,
        claim_lag=420,
        late=False,
    )
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    official_ingest.run(
        now=datetime(2026, 7, 15, 15, 7, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 15),
        settings_overrides={"dart_enabled": False, "kind_enabled": False},
    )
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    monkeypatch.setenv("CURATOR_GITHUB_RUN_ATTEMPT", "2")
    official_ingest.run(
        now=datetime(2026, 7, 16, 16, 7, tzinfo=timezone.utc),
        start=date(2026, 7, 1),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": False, "kind_enabled": False},
    )

    assert len(captured_ids) == 2
    assert captured_ids[0] == captured_ids[1]


def test_scheduled_slot_uses_server_claim_not_delayed_trigger_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_scheduled_claim_env(
        monkeypatch,
        schedule="0,15,30,45 0-14 * * *",
        slot="2026-07-16T03:00:00Z",
        trigger="2026-07-16T03:16:00Z",
        claimed="2026-07-16T03:47:00Z",
        next_slot="2026-07-16T03:15:00Z",
        trigger_lag=960,
        claim_lag=2820,
        late=True,
    )

    provenance = official_ingest._run_provenance(
        current=datetime(2026, 7, 16, 3, 47, tzinfo=timezone.utc),
        idempotency_key=None,
        company_master_sync=False,
    )

    assert provenance["scheduled_slot_at"] == "2026-07-16T03:00:00+00:00"
    assert provenance["trigger_created_at"] == "2026-07-16T03:16:00+00:00"
    assert provenance["slot_claim_late"] is True


def test_scheduled_run_without_durable_claim_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("CURATOR_EVENT_SCHEDULE", "0,30 15-21 * * *")
    monkeypatch.delenv("CURATOR_GITHUB_RUN_CREATED_AT", raising=False)

    with pytest.raises(ValueError, match="durable slot claim fields"):
        official_ingest._run_provenance(
            current=datetime(2026, 7, 16, tzinfo=timezone.utc),
            idempotency_key=None,
            company_master_sync=False,
        )


def test_unrelated_scheduled_workflow_without_official_cadence_is_manual(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.delenv("CURATOR_EVENT_SCHEDULE", raising=False)
    monkeypatch.delenv("CURATOR_OFFICIAL_SLOT_CLAIM_ID", raising=False)

    provenance = official_ingest._run_provenance(
        current=datetime(2026, 7, 16, tzinfo=timezone.utc),
        idempotency_key=None,
        company_master_sync=False,
    )

    assert provenance["run_kind"] == "manual"
    assert provenance["event_schedule"] is None


def test_unknown_explicit_official_schedule_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("CURATOR_EVENT_SCHEDULE", "5,35 0-15,23 * * *")

    with pytest.raises(ValueError, match="unknown event schedule"):
        official_ingest._run_provenance(
            current=datetime(2026, 7, 16, tzinfo=timezone.utc),
            idempotency_key=None,
            company_master_sync=False,
        )


def test_backfill_and_company_master_are_not_scheduled_slots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backfill = official_ingest._run_provenance(
        current=datetime(2026, 7, 16, tzinfo=timezone.utc),
        idempotency_key="official-backfill-v1:" + "a" * 32,
        company_master_sync=True,
    )
    assert backfill["run_kind"] == "backfill"
    assert backfill["scheduled_slot_at"] is None

    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("CURATOR_EVENT_SCHEDULE", "40 21 * * 0")
    master = official_ingest._run_provenance(
        current=datetime(2026, 7, 19, 21, 40, tzinfo=timezone.utc),
        idempotency_key=None,
        company_master_sync=True,
    )
    assert master["run_kind"] == "company_master"
    assert master["scheduled_slot_at"] is None
