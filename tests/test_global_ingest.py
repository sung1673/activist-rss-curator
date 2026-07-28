from __future__ import annotations

import json
import hashlib
import re
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import pytest
import yaml

from curator.global_connectors import (
    GlobalConnectorEnvelope,
    GlobalConnectorRequest,
    GlobalDocumentRecord,
    GlobalLifecycleObservation,
    IssuerReference,
    SecDailyIndexConnector,
    SecHybridConnector,
)
from curator.global_ingest import (
    GlobalConnectorCheckpoint,
    GlobalIngestApiError,
    GlobalIngestChunk,
    GlobalIngestConfigurationError,
    GlobalIngestReceipt,
    V2GlobalIngestClient,
    _failure_evidence,
    automatic_completed_window,
    build_connector,
    chunk_connector_envelope,
    content_idempotency_key,
    coverage_unavailable_evidence,
    default_completed_window,
    execute_global_ingest,
    execute_global_ingest_with_replay,
    global_ingest_batch_id,
    global_ingest_execution_mode,
    global_ingest_chunk,
    main,
    parse_companies_house_allowlist,
    replay_only_verification_evidence,
    replay_verification_evidence,
    sec_completed_day_limit,
    select_completed_window,
    validate_window,
    write_evidence,
)
from curator.global_market import CoverageMode
from curator.official_source_rights import OfficialSourceRightEligibility


REVISION = "a" * 40
RIGHTS_REVISION = "b" * 64
NOW = "2026-07-24T07:30:00+00:00"
WORKFLOW = (
    Path(__file__).resolve().parents[1]
    / ".github"
    / "workflows"
    / "ingest-global.yml"
)


def _eligibility() -> OfficialSourceRightEligibility:
    return OfficialSourceRightEligibility(
        source_right_id="official:sec-edgar",
        use="collect",
        rights_revision=RIGHTS_REVISION,
        checked_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        source_type="official_disclosure",
        source_key="sec-edgar",
        redistribution_allowed=True,
        ai_allowed=False,
    )


def _record(
    index: int,
    *,
    first_observed_at: str = NOW,
) -> GlobalDocumentRecord:
    external_id = f"0000000000-26-{index:06d}"
    return GlobalDocumentRecord(
        record_id=f"record:us:sec:{index:06d}",
        external_id=external_id,
        issuer_id="issuer:us:cik:0000000001",
        issuer_reference=IssuerReference(
            namespace="US:CIK",
            identifier_type="CIK",
            value="0000000001",
            legal_name="Example Corporation",
            market="US",
        ),
        country_code="US",
        source_key="sec-edgar",
        source_right_id="official:sec-edgar",
        record_kind="disclosure",
        document_type="SC 13D",
        event_family="large_ownership",
        title=f"SC 13D filing {index}",
        original_language="en",
        filed_at="2026-07-23T20:00:00+00:00",
        first_observed_at=first_observed_at,
        original_url=(
            "https://www.sec.gov/Archives/edgar/data/1/"
            f"{index:018d}/filing.htm"
        ),
        content_hash=hashlib.sha256(
            f"record-{index}".encode("utf-8")
        ).hexdigest(),
        metadata={"title_provenance": "generated_metadata"},
    )


def _envelope(
    *,
    raw_count: int = 0,
    request_count: int = 2,
    retrieved_at: str = NOW,
    records: tuple[GlobalDocumentRecord, ...] = (),
    lifecycle_observations: tuple[GlobalLifecycleObservation, ...] = (),
) -> GlobalConnectorEnvelope:
    return GlobalConnectorEnvelope(
        schema_version=1,
        connector_id="connector:us:sec-edgar",
        country_code="US",
        source_right_id="official:sec-edgar",
        rights_revision=RIGHTS_REVISION,
        retrieved_at=retrieved_at,
        coverage_mode=CoverageMode.MARKET_WIDE,
        records=records,
        next_cursor=None,
        exhausted=True,
        request_count=request_count,
        raw_count=raw_count,
        lifecycle_observations=lifecycle_observations,
    )


def _single_chunk(envelope: GlobalConnectorEnvelope) -> GlobalIngestChunk:
    return global_ingest_chunk(
        envelope=envelope,
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
        index=0,
        count=1,
        code_revision=REVISION,
    )


class _Rights:
    def __init__(self) -> None:
        self.calls = 0

    def check(
        self,
        source_right_id: str,
        *,
        use: str = "collect",
    ) -> OfficialSourceRightEligibility:
        assert source_right_id == "official:sec-edgar"
        assert use == "collect"
        self.calls += 1
        return _eligibility()


class _PagedConnector:
    descriptor = SecDailyIndexConnector.descriptor

    def __init__(self) -> None:
        self.provider_calls = 0

    def fetch(
        self,
        request: GlobalConnectorRequest,
        *,
        eligibility: OfficialSourceRightEligibility,
        eligibility_provider=None,
        now=None,
    ) -> GlobalConnectorEnvelope:
        assert request.window_start == date(2026, 7, 22)
        assert request.window_end_exclusive == date(2026, 7, 24)
        assert eligibility.rights_revision == RIGHTS_REVISION
        assert eligibility_provider is not None
        # A real connector calls the supplied provider immediately before each
        # source page/day.  Simulate two pages to prove the runner wires that
        # fail-closed provider through instead of reusing one startup grant.
        for _ in range(2):
            assert eligibility_provider().rights_revision == RIGHTS_REVISION
            self.provider_calls += 1
        return _envelope()


class _Ingest:
    def __init__(self) -> None:
        self.key = ""

    def submit(
        self,
        *,
        envelope: GlobalConnectorEnvelope,
        chunk: GlobalIngestChunk,
        idempotency_key: str,
        code_revision: str,
    ) -> GlobalIngestReceipt:
        self.key = idempotency_key
        assert code_revision == REVISION
        assert chunk.index == chunk.count == 1
        return GlobalIngestReceipt(
            ingest_id="global-ingest:test",
            connector_id=envelope.connector_id,
            raw_count=envelope.raw_count,
            acknowledged_count=0,
            idempotent=False,
        )


def test_execution_preflights_rechecks_each_page_and_checks_before_post() -> None:
    rights = _Rights()
    connector = _PagedConnector()
    ingest = _Ingest()
    result = execute_global_ingest(
        country_code="US",
        connector=connector,
        issuers=(),
        window_start=date(2026, 7, 22),
        window_end_exclusive=date(2026, 7, 24),
        code_revision=REVISION,
        rights_client=rights,
        ingest_client=ingest,
    )
    assert rights.calls == 4  # initial + two page checks + final pre-POST check
    assert connector.provider_calls == 2
    assert result.api_version == "v2"
    assert result.raw_count == result.acknowledged_count == 0
    assert ingest.key == result.idempotency_key


class _ReplayIngest:
    def __init__(self, *, replay_idempotent: bool = True) -> None:
        self.submissions: list[tuple[str, str]] = []
        self.replay_idempotent = replay_idempotent

    def submit(
        self,
        *,
        envelope: GlobalConnectorEnvelope,
        chunk: GlobalIngestChunk,
        idempotency_key: str,
        code_revision: str,
    ) -> GlobalIngestReceipt:
        del chunk
        assert code_revision == REVISION
        self.submissions.append((idempotency_key, envelope.retrieved_at))
        attempt = len(self.submissions)
        return GlobalIngestReceipt(
            ingest_id="global-ingest:exact-replay",
            connector_id=envelope.connector_id,
            raw_count=envelope.raw_count,
            acknowledged_count=(
                len(envelope.records)
                + len(envelope.lifecycle_observations)
            ),
            idempotent=attempt > 1 and self.replay_idempotent,
        )


def test_exact_replay_fetches_source_once_and_requires_idempotent_api_ack() -> None:
    connector = _PagedConnector()
    ingest = _ReplayIngest()
    initial, replay = execute_global_ingest_with_replay(
        country_code="US",
        connector=connector,
        issuers=(),
        window_start=date(2026, 7, 22),
        window_end_exclusive=date(2026, 7, 24),
        code_revision=REVISION,
        rights_client=_Rights(),
        ingest_client=ingest,
    )

    assert connector.provider_calls == 2
    assert len(ingest.submissions) == 2
    assert ingest.submissions[0] == ingest.submissions[1]
    assert initial.idempotent is False
    assert replay.idempotent is True
    assert replay.idempotent_chunk_count == replay.chunk_count == 1
    assert replay_verification_evidence(initial, replay) == {
        "attempted": True,
        "same_payload": True,
        "idempotent": True,
        "chunk_count": 1,
        "idempotent_chunk_count": 1,
        "idempotency_keys_match": True,
        "ingest_ids_match": True,
        "raw_count": 0,
        "acknowledged_count": 0,
    }


def test_exact_replay_fails_when_second_api_ack_is_not_idempotent() -> None:
    with pytest.raises(
        GlobalIngestApiError,
        match="global_ingest_replay_not_idempotent",
    ):
        execute_global_ingest_with_replay(
            country_code="US",
            connector=_PagedConnector(),
            issuers=(),
            window_start=date(2026, 7, 22),
            window_end_exclusive=date(2026, 7, 24),
            code_revision=REVISION,
            rights_client=_Rights(),
            ingest_client=_ReplayIngest(replay_idempotent=False),
        )


class _ReplayOnlyIngest:
    def __init__(self, *, idempotent: bool = True) -> None:
        self.idempotent = idempotent
        self.replay_only_values: list[bool] = []

    def submit(
        self,
        *,
        envelope: GlobalConnectorEnvelope,
        chunk: GlobalIngestChunk,
        idempotency_key: str,
        code_revision: str,
        replay_only: bool = False,
    ) -> GlobalIngestReceipt:
        del chunk, idempotency_key
        assert code_revision == REVISION
        self.replay_only_values.append(replay_only)
        return GlobalIngestReceipt(
            ingest_id="global-ingest:read-only-replay",
            connector_id=envelope.connector_id,
            raw_count=envelope.raw_count,
            acknowledged_count=(
                len(envelope.records)
                + len(envelope.lifecycle_observations)
            ),
            idempotent=self.idempotent,
        )


def test_replay_only_requires_preexisting_idempotent_receipts() -> None:
    ingest = _ReplayOnlyIngest()
    result = execute_global_ingest(
        country_code="US",
        connector=_PagedConnector(),
        issuers=(),
        window_start=date(2026, 7, 22),
        window_end_exclusive=date(2026, 7, 24),
        code_revision=REVISION,
        rights_client=_Rights(),
        ingest_client=ingest,
        replay_only=True,
    )

    assert ingest.replay_only_values == [True]
    assert result.idempotent is True
    assert replay_only_verification_evidence(result) == {
        "attempted": True,
        "same_payload": True,
        "idempotent": True,
        "read_only": True,
        "chunk_count": 1,
        "idempotent_chunk_count": 1,
        "idempotency_keys_match": True,
        "ingest_ids_match": True,
        "raw_count": 0,
        "acknowledged_count": 0,
    }

    with pytest.raises(
        GlobalIngestApiError,
        match="global_ingest_replay_not_idempotent",
    ):
        execute_global_ingest(
            country_code="US",
            connector=_PagedConnector(),
            issuers=(),
            window_start=date(2026, 7, 22),
            window_end_exclusive=date(2026, 7, 24),
            code_revision=REVISION,
            rights_client=_Rights(),
            ingest_client=_ReplayOnlyIngest(idempotent=False),
            replay_only=True,
        )


def test_content_idempotency_key_is_stable_for_exact_content() -> None:
    first = content_idempotency_key(
        envelope=_envelope(),
        code_revision=REVISION,
    )
    second = content_idempotency_key(
        envelope=_envelope(),
        code_revision=REVISION,
    )
    changed = content_idempotency_key(
        envelope=_envelope(raw_count=1),
        code_revision=REVISION,
    )
    assert first == second
    assert first.startswith("global-ingest-v2:us:")
    assert changed != first
    current_poll = content_idempotency_key(
        envelope=_envelope(),
        code_revision=REVISION,
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
        current_poll=True,
    )
    assert current_poll.startswith("global-ingest-v2-current:us:")
    assert current_poll != first
    completed_day = content_idempotency_key(
        envelope=_envelope(),
        code_revision=REVISION,
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
        completed_day_evidence=True,
    )
    assert completed_day.startswith("global-ingest-v2-day:us:")
    assert completed_day != first
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="conflicting_ingest_receipt_class",
    ):
        content_idempotency_key(
            envelope=_envelope(),
            code_revision=REVISION,
            completed_day_evidence=True,
            current_poll=True,
        )


def test_classified_key_matches_php73_line_separator_encoding() -> None:
    record = replace(
        _record(9),
        title="SEC title\u2028line\u2029separator",
        metadata={
            "title_provenance": "source",
            "separator": "\u2028\u2029",
            "nested_empty": {},
        },
    )
    key = content_idempotency_key(
        envelope=_envelope(raw_count=1, records=(record,)),
        code_revision=REVISION,
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
        current_poll=True,
    )
    assert key == (
        "global-ingest-v2-current:us:"
        "3d4c412a01646230813862d6de7971a092fbddbdc94f6431202ec851019fdcc3"
    )


def test_connector_envelope_rejects_more_accepted_entities_than_raw_rows() -> None:
    lifecycle = GlobalLifecycleObservation(
        observation_id="lifecycle:us:sec:1",
        country_code="US",
        source_key="sec-edgar",
        external_id="0000000000-26-000001",
        parent_external_id=None,
        change_type="updated",
        observed_at=NOW,
    )
    with pytest.raises(ValueError, match="counts are inconsistent"):
        _envelope(
            raw_count=1,
            records=(_record(1),),
            lifecycle_observations=(lifecycle,),
        )


def test_chunk_raw_counts_partition_records_lifecycle_and_filtered_rows() -> None:
    lifecycle = GlobalLifecycleObservation(
        observation_id="lifecycle:us:sec:1",
        country_code="US",
        source_key="sec-edgar",
        external_id="0000000000-26-000001",
        parent_external_id=None,
        change_type="updated",
        observed_at=NOW,
    )
    envelope = _envelope(
        raw_count=5,
        records=(_record(1), _record(2), _record(3)),
        lifecycle_observations=(lifecycle,),
    )
    chunks = chunk_connector_envelope(
        envelope,
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
        limit=2,
    )

    assert [chunk.raw_count for chunk in chunks] == [3, 2]
    assert [
        len(chunk.records) + len(chunk.lifecycle_observations)
        for chunk in chunks
    ] == [3, 1]
    assert sum(chunk.raw_count for chunk in chunks) == envelope.raw_count


def test_idempotency_ignores_observation_time_and_request_telemetry() -> None:
    first_envelope = _envelope(
        raw_count=1,
        request_count=2,
        retrieved_at="2026-07-24T07:30:00+00:00",
        records=(
            _record(
                1,
                first_observed_at="2026-07-24T07:30:00+00:00",
            ),
        ),
    )
    retried_envelope = _envelope(
        raw_count=1,
        request_count=5,
        retrieved_at="2026-07-24T08:00:00+00:00",
        records=(
            _record(
                1,
                first_observed_at="2026-07-24T08:00:00+00:00",
            ),
        ),
    )
    first = content_idempotency_key(
        envelope=first_envelope,
        code_revision=REVISION,
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
    )
    retried = content_idempotency_key(
        envelope=retried_envelope,
        code_revision=REVISION,
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
    )

    assert first == retried
    assert global_ingest_batch_id(
        envelope=first_envelope,
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
        code_revision=REVISION,
    ) == global_ingest_batch_id(
        envelope=retried_envelope,
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
        code_revision=REVISION,
    )


class _LargeConnector:
    descriptor = SecDailyIndexConnector.descriptor

    def fetch(
        self,
        request: GlobalConnectorRequest,
        *,
        eligibility: OfficialSourceRightEligibility,
        eligibility_provider=None,
        now=None,
    ) -> GlobalConnectorEnvelope:
        del request, eligibility, eligibility_provider, now
        records = tuple(_record(index) for index in range(1001))
        return _envelope(
            raw_count=1200,
            request_count=1,
            records=records,
        )


class _ChunkIngest:
    def __init__(self) -> None:
        self.submissions: list[
            tuple[GlobalConnectorEnvelope, str, GlobalIngestChunk]
        ] = []

    def submit(
        self,
        *,
        envelope: GlobalConnectorEnvelope,
        chunk: GlobalIngestChunk,
        idempotency_key: str,
        code_revision: str,
    ) -> GlobalIngestReceipt:
        assert code_revision == REVISION
        self.submissions.append((envelope, idempotency_key, chunk))
        return GlobalIngestReceipt(
            ingest_id=f"global-ingest:chunk:{len(self.submissions)}",
            connector_id=envelope.connector_id,
            raw_count=envelope.raw_count,
            acknowledged_count=(
                len(envelope.records)
                + len(envelope.lifecycle_observations)
            ),
            idempotent=False,
        )


def test_large_official_window_is_submitted_in_stable_acknowledged_chunks() -> None:
    rights = _Rights()
    ingest = _ChunkIngest()
    result = execute_global_ingest(
        country_code="US",
        connector=_LargeConnector(),
        issuers=(),
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
        code_revision=REVISION,
        rights_client=rights,
        ingest_client=ingest,
        completed_day_evidence=True,
    )

    assert [len(item.records) for item, _key, _chunk in ingest.submissions] == [
        500,
        500,
        1,
    ]
    assert [item.raw_count for item, _key, _chunk in ingest.submissions] == [
        500,
        500,
        200,
    ]
    assert [item.request_count for item, _key, _chunk in ingest.submissions] == [
        0,
        0,
        1,
    ]
    assert [item.exhausted for item, _key, _chunk in ingest.submissions] == [
        False,
        False,
        True,
    ]
    assert all(
        re.fullmatch(
            (
                rf"global-ingest-chunk:2026-07-23:2026-07-24:"
                rf"{index}:3:[a-f0-9]{{24}}"
            ),
            str(item.next_cursor),
        )
        is not None
        for index, (item, _key, _chunk) in enumerate(
            ingest.submissions[:2],
            start=1,
        )
    )
    assert ingest.submissions[-1][0].next_cursor is None
    assert len({key for _item, key, _chunk in ingest.submissions}) == 3
    chunks = [chunk for _item, _key, chunk in ingest.submissions]
    assert [chunk.index for chunk in chunks] == [1, 2, 3]
    assert all(chunk.count == 3 for chunk in chunks)
    assert all(chunk.batch_raw_count == 1200 for chunk in chunks)
    assert all(chunk.batch_acknowledged_count == 1001 for chunk in chunks)
    assert all(chunk.batch_request_count == 1 for chunk in chunks)
    assert all(chunk.window_start == "2026-07-23" for chunk in chunks)
    assert all(
        chunk.window_end_exclusive == "2026-07-24"
        for chunk in chunks
    )
    assert len({chunk.batch_id for chunk in chunks}) == 1
    assert result.chunk_count == 3
    assert result.record_count == result.acknowledged_count == 1001
    assert result.ingest_ids == (
        "global-ingest:chunk:1",
        "global-ingest:chunk:2",
        "global-ingest:chunk:3",
    )
    assert result.idempotent_chunk_count == 0
    assert result.idempotent is False
    assert result.idempotency_key.startswith(
        "global-ingest-v2-day:us:batch:"
    )
    assert all(
        key.startswith("global-ingest-v2-day:us:")
        for _item, key, _chunk in ingest.submissions
    )


def test_chunk_cursor_is_stable_across_collection_times() -> None:
    records_one = tuple(_record(index) for index in range(501))
    records_two = tuple(
        _record(
            index,
            first_observed_at="2026-07-24T08:00:00+00:00",
        )
        for index in range(501)
    )
    first = chunk_connector_envelope(
        _envelope(
            raw_count=600,
            retrieved_at="2026-07-24T07:30:00+00:00",
            records=records_one,
        ),
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
    )
    retried = chunk_connector_envelope(
        _envelope(
            raw_count=600,
            retrieved_at="2026-07-24T08:00:00+00:00",
            records=records_two,
        ),
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
    )

    assert first[0].next_cursor == retried[0].next_cursor


def test_batch_identity_is_revision_scoped_for_single_and_multi_chunk() -> None:
    envelope = _envelope(raw_count=1, records=(_record(1),))
    single = global_ingest_chunk(
        envelope=envelope,
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
        index=0,
        count=1,
        code_revision=REVISION,
    )
    multi = tuple(
        global_ingest_chunk(
            envelope=envelope,
            window_start=date(2026, 7, 23),
            window_end_exclusive=date(2026, 7, 24),
            index=index,
            count=3,
            code_revision=REVISION,
        )
        for index in range(3)
    )
    next_revision = "b" * 40
    next_deployment = global_ingest_chunk(
        envelope=envelope,
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
        index=0,
        count=1,
        code_revision=next_revision,
    )
    assert {chunk.batch_id for chunk in multi} == {single.batch_id}
    assert next_deployment.batch_id != single.batch_id


def test_v2_client_posts_exact_envelope_and_validates_counts() -> None:
    envelope = _envelope(raw_count=7)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/api/v2/ops/ingest")
        assert request.headers["authorization"] == "Bearer ops-secret"
        payload = json.loads(request.content)
        assert payload == {
            "idempotency_key": "global-ingest-v2:us:" + "c" * 64,
            "code_revision": REVISION,
            "envelope": {
                **envelope.to_payload(),
                "chunk": _single_chunk(envelope).to_payload(),
            },
        }
        return httpx.Response(
            200,
            json={
                "ok": True,
                "api_version": "v2",
                "data": {
                    "ingest_id": "global-ingest:receipt",
                    "connector_id": envelope.connector_id,
                    "raw_count": 7,
                    "acknowledged_count": 0,
                    "idempotent": True,
                },
            },
        )

    client = V2GlobalIngestClient(
        base_url="https://alignpe.gabia.io/activist/api.php/api/v1",
        token="ops-secret",
        transport=httpx.MockTransport(handler),
    )
    receipt = client.submit(
        envelope=envelope,
        chunk=_single_chunk(envelope),
        idempotency_key="global-ingest-v2:us:" + "c" * 64,
        code_revision=REVISION,
    )
    assert receipt.api_version == "v2"
    assert receipt.raw_count == 7
    assert receipt.acknowledged_count == 0
    assert receipt.idempotent is True


def test_v2_client_marks_replay_as_server_enforced_read_only() -> None:
    envelope = _envelope(raw_count=7)

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload["ingest_mode"] == "replay"
        return httpx.Response(
            200,
            json={
                "ok": True,
                "api_version": "v2",
                "data": {
                    "ingest_id": "global-ingest:receipt",
                    "connector_id": envelope.connector_id,
                    "raw_count": 7,
                    "acknowledged_count": 0,
                    "idempotent": True,
                },
            },
        )

    receipt = V2GlobalIngestClient(
        base_url="https://example.test/api/v2",
        token="ops-secret",
        transport=httpx.MockTransport(handler),
    ).submit(
        envelope=envelope,
        chunk=_single_chunk(envelope),
        idempotency_key="global-ingest-v2:us:" + "c" * 64,
        code_revision=REVISION,
        replay_only=True,
    )
    assert receipt.idempotent is True


def test_v2_client_binds_preview_write_to_state_and_second_credential() -> None:
    envelope = _envelope()

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload["expected_release_state"] == "preview"
        assert request.headers["authorization"] == "Bearer ops-secret"
        assert request.headers["x-bside-preview-token"] == "preview-secret"
        return httpx.Response(
            200,
            json={
                "ok": True,
                "api_version": "v2",
                "data": {
                    "ingest_id": "global-ingest:preview",
                    "connector_id": envelope.connector_id,
                    "raw_count": 0,
                    "acknowledged_count": 0,
                    "idempotent": True,
                },
            },
        )

    receipt = V2GlobalIngestClient(
        base_url="https://example.test/api/v2",
        token="ops-secret",
        expected_release_state="preview",
        preview_token="preview-secret",
        transport=httpx.MockTransport(handler),
    ).submit(
        envelope=envelope,
        chunk=_single_chunk(envelope),
        idempotency_key="global-ingest-v2-current:us:" + "c" * 64,
        code_revision=REVISION,
    )
    assert receipt.idempotent is True
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="missing_preview_ingest_token",
    ):
        V2GlobalIngestClient(
            base_url="https://example.test/api/v2",
            token="ops-secret",
            expected_release_state="preview",
        )


def test_v2_client_reads_strict_durable_connector_checkpoint() -> None:
    connector_id = "connector:us:sec-edgar"
    batch_id = "global-batch:" + "d" * 64

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path.endswith(
            f"/api/v2/ops/connectors/{connector_id}/checkpoint"
        )
        assert request.headers["authorization"] == "Bearer ops-secret"
        return httpx.Response(
            200,
            json={
                "ok": True,
                "api_version": "v2",
                "data": {
                    "connector_id": connector_id,
                    "cursor_json": {
                        "schema_version": 1,
                        "window_end_exclusive": "2026-07-20",
                        "batch_id": batch_id,
                    },
                    "last_success_at": "2026-07-20T01:00:00+00:00",
                    "last_checked_at": "2026-07-20T01:00:00+00:00",
                    "code_revision": REVISION,
                },
            },
        )

    client = V2GlobalIngestClient(
        base_url="https://example.test/api/v2",
        token="ops-secret",
        transport=httpx.MockTransport(handler),
    )
    checkpoint = client.fetch_checkpoint(connector_id)
    assert checkpoint == GlobalConnectorCheckpoint(
        connector_id=connector_id,
        window_end_exclusive=date(2026, 7, 20),
        batch_id=batch_id,
        last_success_at="2026-07-20T01:00:00+00:00",
        last_checked_at="2026-07-20T01:00:00+00:00",
        code_revision=REVISION,
    )


def test_v2_client_reads_sec_intraday_source_cursor() -> None:
    connector_id = "connector:us:sec-edgar"
    batch_id = "global-batch:" + "d" * 64
    source_cursor = "sec-current-v1:" + "e" * 80
    client = V2GlobalIngestClient(
        base_url="https://example.test/api/v2",
        token="ops-secret",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "ok": True,
                    "api_version": "v2",
                    "data": {
                        "connector_id": connector_id,
                        "cursor_json": {
                            "schema_version": 2,
                            "window_end_exclusive": "2026-07-20",
                            "batch_id": batch_id,
                            "source_cursor": source_cursor,
                        },
                        "last_success_at": "2026-07-20T01:00:00+00:00",
                        "last_checked_at": "2026-07-20T01:00:00+00:00",
                        "code_revision": REVISION,
                    },
                },
            )
        ),
    )
    checkpoint = client.fetch_checkpoint(connector_id)
    assert checkpoint.source_cursor == source_cursor
    assert checkpoint.window_end_exclusive == date(2026, 7, 20)


@pytest.mark.parametrize(
    "data",
    [
        {
            "connector_id": "connector:us:sec-edgar",
            "cursor_json": {
                "schema_version": 2,
                "window_end_exclusive": "2026-07-20",
                "batch_id": "global-batch:" + "d" * 64,
            },
            "last_success_at": "2026-07-20T01:00:00+00:00",
            "last_checked_at": "2026-07-20T01:00:00+00:00",
            "code_revision": REVISION,
        },
        {
            "connector_id": "connector:us:sec-edgar",
            "cursor_json": {
                "schema_version": 1,
                "window_end_exclusive": "2026-99-99",
                "batch_id": "global-batch:" + "d" * 64,
            },
            "last_success_at": "2026-07-20T01:00:00+00:00",
            "last_checked_at": "2026-07-20T01:00:00+00:00",
            "code_revision": REVISION,
        },
        {
            "connector_id": "connector:us:sec-edgar",
            "cursor_json": None,
            "last_success_at": "2026-07-20T01:00:00+00:00",
            "last_checked_at": "2026-07-20T01:00:00+00:00",
            "code_revision": REVISION,
        },
    ],
)
def test_v2_client_rejects_abnormal_connector_checkpoint(
    data: dict[str, object],
) -> None:
    client = V2GlobalIngestClient(
        base_url="https://example.test/api/v2",
        token="ops-secret",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "ok": True,
                    "api_version": "v2",
                    "data": data,
                },
            )
        ),
    )
    with pytest.raises(GlobalIngestApiError):
        client.fetch_checkpoint("connector:us:sec-edgar")


@pytest.mark.parametrize("status", [401, 403, 409, 503])
def test_v2_client_fails_closed_on_security_conflict_and_unavailable(
    status: int,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status,
            json={"ok": False, "api_version": "v2", "error": "request_rejected"},
        )

    client = V2GlobalIngestClient(
        base_url="https://example.test/api/v2",
        token="ops-secret",
        transport=httpx.MockTransport(handler),
    )
    envelope = _envelope()
    with pytest.raises(GlobalIngestApiError) as raised:
        client.submit(
            envelope=envelope,
            chunk=_single_chunk(envelope),
            idempotency_key="global-ingest-v2:us:" + "c" * 64,
            code_revision=REVISION,
        )
    assert raised.value.http_status == status


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="<html>not json</html>"),
        httpx.Response(
            200,
            json={
                "ok": True,
                "api_version": "v1",
                "data": {},
            },
        ),
        httpx.Response(
            200,
            json={
                "ok": True,
                "api_version": "v2",
                "data": {
                    "ingest_id": "receipt",
                    "connector_id": "connector:us:sec-edgar",
                    "raw_count": 999,
                    "acknowledged_count": 0,
                    "idempotent": False,
                },
            },
        ),
    ],
)
def test_v2_client_rejects_malformed_version_or_count_response(
    response: httpx.Response,
) -> None:
    client = V2GlobalIngestClient(
        base_url="https://example.test/api/v2",
        token="ops-secret",
        transport=httpx.MockTransport(lambda request: response),
    )
    envelope = _envelope()
    with pytest.raises(GlobalIngestApiError):
        client.submit(
            envelope=envelope,
            chunk=_single_chunk(envelope),
            idempotency_key="global-ingest-v2:us:" + "c" * 64,
            code_revision=REVISION,
        )


def test_companies_house_requires_closed_schema_explicit_allowlist() -> None:
    issuers = parse_companies_house_allowlist(
        json.dumps(
            [
                {
                    "company_number": "01234567",
                    "legal_name": "Example Limited",
                    "market": "LSE",
                    "ticker": "EXM",
                }
            ]
        )
    )
    assert len(issuers) == 1
    assert issuers[0].namespace == "GB:COMPANIES_HOUSE"
    assert issuers[0].identifier_type == "COMPANY_NUMBER"
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="empty_companies_house_allowlist",
    ):
        parse_companies_house_allowlist("[]")
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="invalid_companies_house_allowlist",
    ):
        parse_companies_house_allowlist(
            '[{"company_number":"01234567","legal_name":"Example","api_key":"x"}]'
        )
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="companies_house_allowlist_limit_exceeded",
    ):
        parse_companies_house_allowlist(
            json.dumps(
                [
                    {
                        "company_number": f"{index:08d}",
                        "legal_name": f"Company {index}",
                    }
                    for index in range(51)
                ]
            )
        )


def test_dart_is_explicitly_excluded_from_v2_global_ingest() -> None:
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="unsupported_global_ingest_country",
    ):
        build_connector("KR", environment={})


def test_us_builds_fail_closed_intraday_and_daily_hybrid() -> None:
    connector, issuers = build_connector(
        "US",
        environment={"SEC_EDGAR_USER_AGENT": "BSIDE ops@example.com"},
    )
    assert isinstance(connector, SecHybridConnector)
    assert issuers == ()


def test_jp_is_hard_disabled_and_never_scrapes_even_with_stale_secrets() -> None:
    keyless = global_ingest_execution_mode("JP", environment={})
    assert keyless.mode == "disabled"
    assert keyless.api_active is False
    assert keyless.coverage_mode == "link-only"
    assert keyless.reason == (
        "edinet_production_alpha_disabled_html_scraping_prohibited"
    )
    evidence = coverage_unavailable_evidence(
        execution_mode=keyless,
        code_revision=REVISION,
        started_at=NOW,
    )
    assert evidence["html_scraping"] is False
    assert evidence["source_urls_requested"] == 0
    assert evidence["eligible_for_release"] is False
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="jp_official_api_connector_not_active",
    ):
        build_connector("JP", environment={})
    stale = {
        "EDINET_CONNECTOR_MODE": "active",
        "EDINET_API_KEY": "edinet-key",
    }
    assert global_ingest_execution_mode("JP", environment=stale) == keyless
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="jp_official_api_connector_not_active",
    ):
        build_connector("JP", environment=stale)


def test_gb_is_hard_disabled_and_never_scrapes_even_with_stale_secrets() -> None:
    keyless = global_ingest_execution_mode("GB", environment={})
    assert keyless.mode == "disabled"
    assert keyless.api_active is False
    assert keyless.coverage_mode == "link-only"
    evidence = coverage_unavailable_evidence(
        execution_mode=keyless,
        code_revision=REVISION,
        started_at=NOW,
    )
    assert evidence["html_scraping"] is False
    assert evidence["source_urls_requested"] == 0
    assert evidence["keyless_capabilities"] == [
        "monthly_company_bulk_snapshot",
        "daily_electronic_accounts_bulk",
        "psc_snapshot",
        "basic_company_uri",
        "public_register_links",
    ]
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="gb_official_api_connector_not_active",
    ):
        build_connector(
            "GB",
            environment={"COMPANIES_HOUSE_API_KEY": "unused-key"},
        )
    stale = {
        "COMPANIES_HOUSE_CONNECTOR_MODE": "active",
        "COMPANIES_HOUSE_API_KEY": "companies-house-key",
        "COMPANIES_HOUSE_ISSUERS_JSON": "[]",
    }
    assert global_ingest_execution_mode("GB", environment=stale) == keyless
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="gb_official_api_connector_not_active",
    ):
        build_connector("GB", environment=stale)


@pytest.mark.parametrize(
    ("country", "variable", "value", "code"),
    [
        (
            "JP",
            "EDINET_CONNECTOR_MODE",
            "scrape",
            "disabled",
        ),
        (
            "GB",
            "COMPANIES_HOUSE_CONNECTOR_MODE",
            "scrape",
            "disabled",
        ),
    ],
)
def test_stale_mode_configuration_cannot_activate_optional_markets(
    country: str,
    variable: str,
    value: str,
    code: str,
) -> None:
    mode = global_ingest_execution_mode(country, environment={variable: value})
    assert mode.mode == code
    assert mode.api_active is False
    assert mode.coverage_mode == "link-only"


@pytest.mark.parametrize(
    "country",
    ["JP", "GB"],
)
def test_cli_does_not_accept_optional_markets(
    country: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BSIDE_API_BASE_URL", raising=False)
    monkeypatch.delenv("GOVERNANCE_API_BASE_URL", raising=False)
    monkeypatch.delenv("ACTIVIST_API_URL", raising=False)
    monkeypatch.delenv("BSIDE_OPS_TOKEN", raising=False)
    monkeypatch.delenv("EDINET_API_KEY", raising=False)
    monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
    monkeypatch.setenv("EDINET_CONNECTOR_MODE", "link-only")
    monkeypatch.setenv("COMPANIES_HOUSE_CONNECTOR_MODE", "keyless")
    with pytest.raises(SystemExit):
        main(
            [
                "--country",
                country,
                "--code-revision",
                REVISION,
                "--evidence",
                "keyless-evidence.json",
            ]
        )


def test_default_dates_and_validation_are_half_open() -> None:
    assert default_completed_window(today=date(2026, 7, 24)) == (
        date(2026, 7, 22),
        date(2026, 7, 24),
    )
    validate_window(date(2026, 7, 23), date(2026, 7, 24))
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="invalid_half_open_window",
    ):
        validate_window(date(2026, 7, 24), date(2026, 7, 24))


def test_completed_day_connector_excludes_sec_intraday_atom() -> None:
    environment = {
        "SEC_EDGAR_USER_AGENT": "BSIDE-Test/1.0 support@bside.ai",
    }
    incremental, _ = build_connector(
        "US",
        environment=environment,
    )
    completed_day, _ = build_connector(
        "US",
        environment=environment,
        completed_day_only=True,
    )
    assert isinstance(incremental, SecHybridConnector)
    assert isinstance(completed_day, SecDailyIndexConnector)


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        (
            datetime.fromisoformat("2026-07-25T05:59:59-04:00"),
            date(2026, 7, 24),
        ),
        (
            datetime.fromisoformat("2026-07-25T06:00:00-04:00"),
            date(2026, 7, 25),
        ),
        (
            datetime.fromisoformat("2026-07-25T09:59:59+00:00"),
            date(2026, 7, 24),
        ),
        (
            datetime.fromisoformat("2026-07-25T10:00:00+00:00"),
            date(2026, 7, 25),
        ),
    ],
)
def test_sec_daily_index_uses_conservative_six_am_eastern_cutoff(
    observed: datetime,
    expected: date,
) -> None:
    assert sec_completed_day_limit(now=observed) == expected


def _checkpoint(
    completed_through: date | None,
) -> GlobalConnectorCheckpoint:
    return GlobalConnectorCheckpoint(
        connector_id="connector:us:sec-edgar",
        window_end_exclusive=completed_through,
        batch_id=(
            None
            if completed_through is None
            else "global-batch:" + "d" * 64
        ),
        last_success_at=(
            None
            if completed_through is None
            else "2026-07-20T01:00:00+00:00"
        ),
        last_checked_at="2026-07-20T01:00:00+00:00",
        code_revision=None if completed_through is None else REVISION,
    )


def test_automatic_window_resumes_with_overlap_and_bounded_catchup() -> None:
    today = date(2026, 7, 24)
    assert automatic_completed_window(
        _checkpoint(None),
        today=today,
    ) == (date(2026, 7, 22), today)
    assert automatic_completed_window(
        _checkpoint(date(2026, 7, 20)),
        today=today,
    ) == (date(2026, 7, 19), today)
    assert automatic_completed_window(
        _checkpoint(date(2026, 5, 1)),
        today=today,
    ) == (date(2026, 4, 30), date(2026, 5, 31))
    assert automatic_completed_window(
        _checkpoint(today),
        today=today,
    ) == (date(2026, 7, 23), today)


@pytest.mark.parametrize(
    ("completed_through", "code"),
    [
        (date(2026, 7, 25), "future_connector_checkpoint"),
        (date(2014, 12, 31), "invalid_connector_checkpoint"),
    ],
)
def test_automatic_window_rejects_future_or_abnormal_checkpoint(
    completed_through: date,
    code: str,
) -> None:
    with pytest.raises(GlobalIngestConfigurationError, match=code):
        automatic_completed_window(
            _checkpoint(completed_through),
            today=date(2026, 7, 24),
        )


class _CheckpointLoader:
    def __init__(
        self,
        checkpoint: GlobalConnectorCheckpoint | None = None,
    ) -> None:
        self.checkpoint = checkpoint
        self.calls = 0

    def fetch_checkpoint(
        self,
        connector_id: str,
    ) -> GlobalConnectorCheckpoint:
        self.calls += 1
        if self.checkpoint is None:
            raise AssertionError("checkpoint must not be read")
        assert connector_id == self.checkpoint.connector_id
        return self.checkpoint


def test_explicit_window_never_reads_checkpoint_and_partial_pair_fails() -> None:
    loader = _CheckpointLoader()
    assert select_completed_window(
        from_date="2026-07-01",
        to_date="2026-07-03",
        connector_id="connector:us:sec-edgar",
        checkpoint_client=loader,
        today=date(2026, 7, 24),
    ) == (date(2026, 7, 1), date(2026, 7, 3))
    assert loader.calls == 0
    with pytest.raises(
        GlobalIngestConfigurationError,
        match="partial_explicit_window",
    ):
        select_completed_window(
            from_date="2026-07-01",
            to_date="",
            connector_id="connector:us:sec-edgar",
            checkpoint_client=loader,
            today=date(2026, 7, 24),
        )
    assert loader.calls == 0


def test_automatic_window_reads_exact_connector_checkpoint_once() -> None:
    checkpoint = _checkpoint(date(2026, 7, 20))
    loader = _CheckpointLoader(checkpoint)
    assert select_completed_window(
        from_date="",
        to_date="",
        connector_id=checkpoint.connector_id,
        checkpoint_client=loader,
        today=date(2026, 7, 24),
    ) == (date(2026, 7, 19), date(2026, 7, 24))
    assert loader.calls == 1


def test_failure_evidence_never_contains_credentials_or_response_body(
    tmp_path: Path,
) -> None:
    secret = "super-secret-api-key"
    evidence = _failure_evidence(
        country_code="JP",
        window_start=date(2026, 7, 23),
        window_end_exclusive=date(2026, 7, 24),
        code_revision=REVISION,
        started_at=NOW,
        error=RuntimeError(
            f"https://example.test/?api_key={secret} response={secret}"
        ),
    )
    output = tmp_path / "evidence.json"
    write_evidence(output, evidence)
    serialized = output.read_text(encoding="utf-8")
    assert secret not in serialized
    assert "example.test" not in serialized
    assert json.loads(serialized)["error"] == {
        "class": "RuntimeError",
        "code": "global_ingest_failed",
    }


def test_workflow_is_matrixed_guarded_serial_and_never_uses_telegram() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    job = payload["jobs"]["ingest"]
    for secret_name in (
        "BSIDE_API_BASE_URL",
        "BSIDE_OPS_TOKEN",
        "EDINET_API_KEY",
        "COMPANIES_HOUSE_API_KEY",
    ):
        assert secret_name not in job["env"]
    collect = next(
        step
        for step in job["steps"]
        if step["name"] == "Collect and ingest official source"
    )
    assert "BSIDE_OPS_TOKEN" in collect["env"]
    assert "EDINET_API_KEY" not in workflow
    assert "COMPANIES_HOUSE_API_KEY" not in workflow
    assert "matrix:" in workflow
    assert "- US" in workflow
    assert "- JP" not in workflow
    assert "- GB" not in workflow
    assert "cancel-in-progress: false" in workflow
    assert "GOVERNANCE_PIPELINE_MODE" in workflow
    assert "--require-active-pipeline" in workflow
    assert "EDINET_CONNECTOR_MODE" not in workflow
    assert "COMPANIES_HOUSE_CONNECTOR_MODE" not in workflow
    assert "COMPANIES_HOUSE_ISSUERS_JSON" not in workflow
    assert "if: always()" in workflow
    assert "TELEGRAM_" not in workflow
    assert 'max_pages="200"' in workflow
    assert 'max_pages="100"' not in workflow
    assert '--max-pages "$max_pages"' in workflow
