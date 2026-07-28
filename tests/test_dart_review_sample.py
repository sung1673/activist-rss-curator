from __future__ import annotations

import csv
import hashlib
import io
import json
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import httpx
import pytest

from curator.dart_review_sample import (
    BackfillEvidence,
    CorpusSnapshot,
    DartReviewApiError,
    DartReviewCorpusClient,
    DartReviewSampleError,
    build_parser,
    corpus_sha256,
    main,
    normalize_corpus_item,
    select_stratified_sample,
    validate_backfill_evidence,
    validate_sample_date_range,
    write_review_bundle,
)


FROM_DATE = date(2026, 7, 1)
TO_DATE = date(2026, 7, 11)
SAMPLE_FROM_DATE = date(2026, 6, 1)
SAMPLE_TO_DATE = date(2026, 7, 1)
OPS_TOKEN = "ops-token-" + ("x" * 40)
API_BASE = "https://example.test/activist/api.php/api/v1"
BACKEND_BINDING_ID = "b" * 64
CODE_REVISION = "a" * 40
REVISION_STATUSES = (
    "current",
    "original_superseded",
    "correction_linked",
    "correction_unlinked",
    "withdrawal_linked",
    "withdrawal_unlinked",
)


def corpus_item(
    index: int,
    *,
    revision_status: str | None = None,
    from_date: date = FROM_DATE,
    to_date: date = TO_DATE,
) -> dict[str, object]:
    status = revision_status or REVISION_STATUSES[index % len(REVISION_STATUSES)]
    receipt_date = from_date + timedelta(days=index % (to_date - from_date).days)
    external_id = f"{receipt_date:%Y%m%d}{index:06d}"
    linked = status.endswith("_linked")
    withdrawn = status.startswith("withdrawal_")
    correction = status.startswith("correction_")
    superseded = status == "original_superseded"
    return {
        "document_id": f"dart:{external_id}",
        "event_id": f"event:{index:06d}",
        "company_id": f"{10000000 + (index % 17):08d}",
        "company_name": f" 회사 {index % 17} 원문 ",
        "event_type": ("general_meeting", "tender_offer", "shareholding_5pct")[
            index % 3
        ],
        "revision_status": status,
        "external_id": external_id,
        "title": f" 원문 공시 제목 {index} ",
        "original_language": "ko",
        "original_url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={external_id}",
        "published_at": f"{receipt_date.isoformat()}T00:00:00Z",
        "source_right_id": "official:dart",
        "correction_of_document_id": (
            f"dart:20260630{index:06d}" if linked else None
        ),
        "version_no": 2 if linked else 1,
        "has_later_correction": superseded,
        "has_successor": False,
        "is_correction": correction,
        "is_cancelled": withdrawn,
        "event_verification_status": "withdrawn" if withdrawn else "official",
        "document_verification_status": "withdrawn" if withdrawn else "official",
        "document_publication_status": "published",
        "identity_status": "needs_review",
        "review_status": "pending",
        "importance": "market_sensitive" if index % 5 == 0 else "normal",
    }


def normalized_items(
    count: int,
    *,
    from_date: date = FROM_DATE,
    to_date: date = TO_DATE,
) -> list[dict[str, object]]:
    return [
        normalize_corpus_item(
            corpus_item(index, from_date=from_date, to_date=to_date),
            from_date=from_date,
            to_date=to_date,
        )
        for index in range(count)
    ]


def response_payload(
    *,
    all_items: list[dict[str, object]],
    page_items: list[dict[str, object]],
    next_cursor: str | None,
    population_count: int | None = None,
    digest: str | None = None,
) -> dict[str, object]:
    return {
        "ok": True,
        "api_version": "v1",
        "contract_version": "dart-review-corpus-v1",
        "range": {"from": FROM_DATE.isoformat(), "to": TO_DATE.isoformat()},
        "backend_binding_id": BACKEND_BINDING_ID,
        "population_count": (
            len(all_items) if population_count is None else population_count
        ),
        "corpus_sha256": digest or corpus_sha256(all_items),
        "items": page_items,
        "next_cursor": next_cursor,
    }


def mock_client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> DartReviewCorpusClient:
    return DartReviewCorpusClient(
        base_url=API_BASE,
        token=OPS_TOKEN,
        backend_binding_id=BACKEND_BINDING_ID,
        transport=httpx.MockTransport(handler),
    )


def test_client_pages_complete_corpus_and_verifies_digest() -> None:
    items = normalized_items(137)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.headers["Authorization"] == f"Bearer {OPS_TOKEN}"
        assert request.url.params["from"] == FROM_DATE.isoformat()
        assert request.url.params["to"] == TO_DATE.isoformat()
        if calls == 1:
            assert "cursor" not in request.url.params
            page = items[:100]
            cursor = "page_2"
        else:
            assert request.url.params["cursor"] == "page_2"
            page = items[100:]
            cursor = None
        return httpx.Response(
            200,
            json=response_payload(
                all_items=items,
                page_items=page,
                next_cursor=cursor,
            ),
        )

    snapshot = mock_client(handler).fetch(
        from_date=FROM_DATE,
        to_date=TO_DATE,
    )
    assert calls == 2
    assert snapshot.population_count == 137
    assert snapshot.backend_binding_id == BACKEND_BINDING_ID
    assert snapshot.corpus_sha256 == corpus_sha256(items)
    assert len(snapshot.items) == 137


def test_client_retries_transient_read_timeout_on_a_fresh_get() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.headers["Connection"] == "close"
        if calls == 1:
            raise httpx.ReadTimeout(
                f"transient {OPS_TOKEN}",
                request=request,
            )
        return httpx.Response(
            200,
            json=response_payload(
                all_items=[],
                page_items=[],
                next_cursor=None,
            ),
        )

    snapshot = mock_client(handler).fetch(
        from_date=FROM_DATE,
        to_date=TO_DATE,
    )

    assert calls == 2
    assert snapshot.population_count == 0


def test_client_bounds_transport_retries_and_does_not_retry_http_contracts() -> None:
    transport_calls = 0

    def transport_handler(request: httpx.Request) -> httpx.Response:
        nonlocal transport_calls
        transport_calls += 1
        raise httpx.RemoteProtocolError(
            f"hostile {OPS_TOKEN}",
            request=request,
        )

    with pytest.raises(DartReviewApiError, match="RemoteProtocolError") as error:
        mock_client(transport_handler).fetch(
            from_date=FROM_DATE,
            to_date=TO_DATE,
        )
    assert transport_calls == 3
    assert OPS_TOKEN not in str(error.value)
    assert OPS_TOKEN not in "".join(
        traceback.format_exception(error.type, error.value, error.tb)
    )

    response_calls = 0

    def response_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal response_calls
        response_calls += 1
        return httpx.Response(
            503,
            json=response_payload(
                all_items=[],
                page_items=[],
                next_cursor=None,
            ),
        )

    with pytest.raises(DartReviewApiError, match="HTTP 503"):
        mock_client(response_handler).fetch(
            from_date=FROM_DATE,
            to_date=TO_DATE,
        )
    assert response_calls == 1


def test_stratified_sample_is_input_order_invariant_and_balanced() -> None:
    items = normalized_items(180)
    selected = select_stratified_sample(items, sample_size=100, seed=42)
    reversed_selected = select_stratified_sample(
        list(reversed(items)),
        sample_size=100,
        seed=42,
    )
    assert [row["document_id"] for row in selected] == [
        row["document_id"] for row in reversed_selected
    ]
    assert len({row["document_id"] for row in selected}) == 100
    assert {row["revision_status"] for row in selected} == set(REVISION_STATUSES)
    assert {row["event_type"] for row in selected} == {
        "general_meeting",
        "tender_offer",
        "shareholding_5pct",
    }
    assert len({row["company_id"] for row in selected}) == 17


def test_default_exact_sample_rejects_99_records() -> None:
    with pytest.raises(DartReviewSampleError, match="required=100, actual=99"):
        select_stratified_sample(normalized_items(99))


def test_stratified_sample_rejects_duplicate_document() -> None:
    items = normalized_items(100)
    items[-1] = dict(items[0])
    with pytest.raises(DartReviewSampleError, match="duplicate documents"):
        select_stratified_sample(items)


@pytest.mark.parametrize(
    "mutation",
    ("population_drift", "digest_drift", "duplicate", "cursor_loop"),
)
def test_client_rejects_pagination_drift_duplicates_and_cursor_failure(
    mutation: str,
) -> None:
    items = normalized_items(120)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            page = items[:100]
            cursor = "page_2"
            population = len(items)
            digest = corpus_sha256(items)
        else:
            page = items[99:] if mutation == "duplicate" else items[100:]
            cursor = "page_2" if mutation == "cursor_loop" else None
            population = len(items) + (1 if mutation == "population_drift" else 0)
            digest = (
                "a" * 64 if mutation == "digest_drift" else corpus_sha256(items)
            )
        return httpx.Response(
            200,
            json=response_payload(
                all_items=items,
                page_items=page,
                next_cursor=cursor,
                population_count=population,
                digest=digest,
            ),
        )

    with pytest.raises(DartReviewApiError):
        mock_client(handler).fetch(from_date=FROM_DATE, to_date=TO_DATE)


def test_client_rejects_terminal_digest_mismatch() -> None:
    items = normalized_items(100)

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            json=response_payload(
                all_items=items,
                page_items=items,
                next_cursor=None,
                digest="f" * 64,
            ),
        )

    with pytest.raises(DartReviewApiError, match="digest does not match"):
        mock_client(handler).fetch(from_date=FROM_DATE, to_date=TO_DATE)


@pytest.mark.parametrize("remote_binding_id", ("c" * 64, "한" * 64))
def test_client_rejects_backend_binding_mismatch(
    remote_binding_id: str,
) -> None:
    items = normalized_items(100)

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        payload = response_payload(
            all_items=items,
            page_items=items,
            next_cursor=None,
        )
        payload["backend_binding_id"] = remote_binding_id
        return httpx.Response(200, json=payload)

    with pytest.raises(DartReviewApiError, match="backend binding"):
        mock_client(handler).fetch(from_date=FROM_DATE, to_date=TO_DATE)


def test_hostile_response_and_transport_text_are_never_exposed() -> None:
    hostile = "TOP-SECRET-HOSTILE-RESPONSE"

    def extra_field_handler(request: httpx.Request) -> httpx.Response:
        del request
        payload = response_payload(
            all_items=[],
            page_items=[],
            next_cursor=None,
        )
        payload["message"] = hostile
        return httpx.Response(500, json=payload)

    with pytest.raises(DartReviewApiError) as extra_error:
        mock_client(extra_field_handler).fetch(
            from_date=FROM_DATE,
            to_date=TO_DATE,
        )
    assert hostile not in str(extra_error.value)
    assert OPS_TOKEN not in str(extra_error.value)

    def transport_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(
            f"{hostile} {OPS_TOKEN}",
            request=request,
        )

    with pytest.raises(DartReviewApiError) as transport_error:
        mock_client(transport_handler).fetch(
            from_date=FROM_DATE,
            to_date=TO_DATE,
        )
    assert hostile not in str(transport_error.value)
    assert OPS_TOKEN not in str(transport_error.value)


def test_hostile_item_value_is_not_reflected_in_validation_error() -> None:
    hostile = "TOP-SECRET-ITEM-VALUE"
    item = corpus_item(1)
    item["event_type"] = hostile
    with pytest.raises(DartReviewApiError) as error:
        normalize_corpus_item(
            item,
            from_date=FROM_DATE,
            to_date=TO_DATE,
        )
    assert hostile not in str(error.value)


def test_client_rejects_credential_bearing_url_without_reflecting_it() -> None:
    hostile_url = "https://user:password@example.test/secret?token=abc"
    with pytest.raises(DartReviewApiError) as error:
        DartReviewCorpusClient(base_url=hostile_url, token=OPS_TOKEN)
    assert hostile_url not in str(error.value)
    assert "password" not in str(error.value)


@pytest.mark.parametrize(
    "unsafe_url",
    (
        "https://example.test/api/v1%2f..",
        "https://example.test\\api\\v1",
        "https://example.test/other",
        "https://example.test:444/api/v1",
    ),
)
def test_client_rejects_ambiguous_operational_urls(unsafe_url: str) -> None:
    with pytest.raises(DartReviewApiError, match="base URL"):
        DartReviewCorpusClient(base_url=unsafe_url, token=OPS_TOKEN)


@pytest.mark.parametrize("binding_id", ("", "B" * 64, "not-a-binding"))
def test_client_requires_exact_backend_binding_id(binding_id: str) -> None:
    with pytest.raises(DartReviewApiError, match="backend binding"):
        DartReviewCorpusClient(
            base_url=API_BASE,
            token=OPS_TOKEN,
            backend_binding_id=binding_id,
        )


def backfill_files(
    tmp_path: Path,
    *,
    from_date: date,
    to_date: date,
    population_count: int,
    code_revision: str = CODE_REVISION,
) -> tuple[Path, Path]:
    window_count = (to_date - from_date).days
    contract: dict[str, object] = {
        "code_revision": code_revision,
        "range_start": from_date.isoformat(),
        "range_end_exclusive": to_date.isoformat(),
        "chunk_days": 1,
        "sources": ["dart"],
        "page_count": 100,
        "max_pages": 100,
        "sync_company_master": False,
    }
    fingerprint = hashlib.sha256(
        json.dumps(
            contract,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    base_count, remainder = divmod(population_count, window_count)
    completed: dict[str, object] = {}
    cursor = from_date
    for index in range(window_count):
        next_date = cursor + timedelta(days=1)
        accepted = base_count + (1 if index < remainder else 0)
        key = f"{cursor.isoformat()}:{next_date.isoformat()}"
        idempotency_digest = hashlib.sha256(
            f"{fingerprint}|{key}".encode("utf-8")
        ).hexdigest()[:32]
        completed[key] = {
            "window_start": cursor.isoformat(),
            "window_end_exclusive": next_date.isoformat(),
            "idempotency_key": f"official-backfill-v1:{idempotency_digest}",
            "attempt": 1,
            "code_revision": code_revision,
            "status": "succeeded",
            "summary": {
                "official_failed": 0,
                "official_skipped": 0,
                "official_remote_ack_mismatches": 0,
                "official_remote_run_persisted": 1,
                "official_remote_raw_count": accepted,
                "official_remote_ack_count": accepted,
                "official_remote_failed": 0,
                "official_remote_skipped": 0,
                "official_remote_synced": 1,
                "official_dart_requests": 9,
                "official_dart_fetched": accepted,
                "official_dart_accepted": accepted,
                "official_dart_errors": 0,
                "official_dart_quota_exhausted": 0,
            },
        }
        cursor = next_date
    checkpoint = {
        "schema_version": 1,
        "job": {**contract, "fingerprint": fingerprint},
        "created_at": "2026-07-01T00:00:00+00:00",
        "updated_at": "2026-07-02T00:00:00+00:00",
        "company_master_synced": False,
        "dart_quota_blocked_until": None,
        "completed_windows": completed,
        "failed_windows": {},
    }
    report = {
        "schema_version": 1,
        "status": "succeeded",
        "dry_run": False,
        "code_revision": code_revision,
        "job_fingerprint": fingerprint,
        "range_start": from_date.isoformat(),
        "range_end_exclusive": to_date.isoformat(),
        "windows_total": window_count,
        "windows_remaining": 0,
        "checkpoint_source": "mysql_remote",
        "checkpoint_version": window_count + 1,
    }
    report_path = tmp_path / "official-backfill-report.json"
    checkpoint_path = tmp_path / "backfill_official_checkpoint.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")
    return report_path, checkpoint_path


def test_backfill_evidence_binds_every_window_and_population(tmp_path: Path) -> None:
    start = date(2026, 6, 1)
    end = date(2026, 7, 1)
    report_path, checkpoint_path = backfill_files(
        tmp_path,
        from_date=start,
        to_date=end,
        population_count=120,
        code_revision=CODE_REVISION,
    )
    evidence = validate_backfill_evidence(
        report_path=report_path,
        checkpoint_path=checkpoint_path,
        from_date=start,
        to_date=end,
        population_count=120,
        code_revision=CODE_REVISION,
    )
    assert evidence.completed_window_count == 30
    assert evidence.code_revision == CODE_REVISION
    assert evidence.expected_dart_document_count == 120
    assert len(evidence.job_fingerprint) == 64
    assert len(evidence.checkpoint_sha256) == 64


def test_backfill_evidence_rejects_missing_window_and_population_mismatch(
    tmp_path: Path,
) -> None:
    start = date(2026, 6, 1)
    end = date(2026, 7, 1)
    report_path, checkpoint_path = backfill_files(
        tmp_path,
        from_date=start,
        to_date=end,
        population_count=120,
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["completed_windows"].pop(next(iter(checkpoint["completed_windows"])))
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")
    with pytest.raises(DartReviewSampleError, match="every requested"):
        validate_backfill_evidence(
            report_path=report_path,
            checkpoint_path=checkpoint_path,
            from_date=start,
            to_date=end,
            population_count=120,
            code_revision=CODE_REVISION,
        )

    report_path, checkpoint_path = backfill_files(
        tmp_path,
        from_date=start,
        to_date=end,
        population_count=120,
    )
    with pytest.raises(DartReviewSampleError, match="does not match"):
        validate_backfill_evidence(
            report_path=report_path,
            checkpoint_path=checkpoint_path,
            from_date=start,
            to_date=end,
            population_count=121,
            code_revision=CODE_REVISION,
        )


@pytest.mark.parametrize("mutation", ("missing", "mixed", "report"))
def test_backfill_evidence_rejects_revision_mismatch(
    tmp_path: Path,
    mutation: str,
) -> None:
    report_path, checkpoint_path = backfill_files(
        tmp_path,
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
        population_count=120,
    )
    if mutation == "report":
        report = json.loads(report_path.read_text(encoding="utf-8"))
        report["code_revision"] = "b" * 40
        report_path.write_text(json.dumps(report), encoding="utf-8")
    else:
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        first = next(iter(checkpoint["completed_windows"].values()))
        if mutation == "missing":
            first.pop("code_revision")
        else:
            first["code_revision"] = "b" * 40
        checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(DartReviewSampleError, match="complete applied range|unacknowledged"):
        validate_backfill_evidence(
            report_path=report_path,
            checkpoint_path=checkpoint_path,
            from_date=SAMPLE_FROM_DATE,
            to_date=SAMPLE_TO_DATE,
            population_count=120,
            code_revision=CODE_REVISION,
        )


@pytest.mark.parametrize(
    ("mutation", "value"),
    (
        ("missing_revision", None),
        ("wrong_revision", "b" * 40),
        ("combined_sources", ["dart", "kind"]),
    ),
)
def test_backfill_evidence_rejects_non_dart_release_job(
    tmp_path: Path,
    mutation: str,
    value: object,
) -> None:
    report_path, checkpoint_path = backfill_files(
        tmp_path,
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
        population_count=120,
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    job = checkpoint["job"]
    job.pop("fingerprint")
    if mutation == "missing_revision":
        job.pop("code_revision")
    elif mutation == "wrong_revision":
        job["code_revision"] = value
    else:
        job["sources"] = value
    fingerprint = hashlib.sha256(
        json.dumps(
            job,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    job["fingerprint"] = fingerprint
    report["job_fingerprint"] = fingerprint
    report_path.write_text(json.dumps(report), encoding="utf-8")
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(DartReviewSampleError, match="job does not match"):
        validate_backfill_evidence(
            report_path=report_path,
            checkpoint_path=checkpoint_path,
            from_date=SAMPLE_FROM_DATE,
            to_date=SAMPLE_TO_DATE,
            population_count=120,
            code_revision=CODE_REVISION,
        )


def test_backfill_evidence_rejects_prefix_only_window_idempotency(
    tmp_path: Path,
) -> None:
    report_path, checkpoint_path = backfill_files(
        tmp_path,
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
        population_count=120,
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    first = next(iter(checkpoint["completed_windows"].values()))
    first["idempotency_key"] = "official-backfill-v1:" + "0" * 32
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(DartReviewSampleError, match="unacknowledged window"):
        validate_backfill_evidence(
            report_path=report_path,
            checkpoint_path=checkpoint_path,
            from_date=SAMPLE_FROM_DATE,
            to_date=SAMPLE_TO_DATE,
            population_count=120,
            code_revision=CODE_REVISION,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("official_remote_raw_count", 5),
        ("official_remote_synced", 0),
        ("official_dart_requests", 0),
        ("official_dart_fetched", 3),
        ("official_dart_accepted", 5),
        ("official_dart_errors", 1),
        ("official_dart_quota_exhausted", 1),
    ),
)
def test_backfill_evidence_rejects_incomplete_dart_ack_summary(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    report_path, checkpoint_path = backfill_files(
        tmp_path,
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
        population_count=120,
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    first = next(iter(checkpoint["completed_windows"].values()))
    first["summary"][field] = value
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(DartReviewSampleError, match="unacknowledged window"):
        validate_backfill_evidence(
            report_path=report_path,
            checkpoint_path=checkpoint_path,
            from_date=SAMPLE_FROM_DATE,
            to_date=SAMPLE_TO_DATE,
            population_count=120,
            code_revision=CODE_REVISION,
        )


def test_backfill_evidence_accepts_zero_filing_day_with_actual_request(
    tmp_path: Path,
) -> None:
    report_path, checkpoint_path = backfill_files(
        tmp_path,
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
        population_count=0,
    )
    evidence = validate_backfill_evidence(
        report_path=report_path,
        checkpoint_path=checkpoint_path,
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
        population_count=0,
        code_revision=CODE_REVISION,
    )
    assert evidence.expected_dart_document_count == 0
    assert evidence.completed_window_count == 30


def test_backfill_evidence_rejects_completed_window_outside_job_range(
    tmp_path: Path,
) -> None:
    report_path, checkpoint_path = backfill_files(
        tmp_path,
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
        population_count=120,
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    first_key = next(iter(checkpoint["completed_windows"]))
    first = checkpoint["completed_windows"].pop(first_key)
    outside_start = SAMPLE_FROM_DATE - timedelta(days=1)
    outside_end = outside_start + timedelta(days=1)
    outside_key = f"{outside_start.isoformat()}:{outside_end.isoformat()}"
    first["window_start"] = outside_start.isoformat()
    first["window_end_exclusive"] = outside_end.isoformat()
    digest = hashlib.sha256(
        (
            checkpoint["job"]["fingerprint"]
            + "|"
            + outside_key
        ).encode("utf-8")
    ).hexdigest()[:32]
    first["idempotency_key"] = f"official-backfill-v1:{digest}"
    checkpoint["completed_windows"][outside_key] = first
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(DartReviewSampleError, match="every requested"):
        validate_backfill_evidence(
            report_path=report_path,
            checkpoint_path=checkpoint_path,
            from_date=SAMPLE_FROM_DATE,
            to_date=SAMPLE_TO_DATE,
            population_count=120,
            code_revision=CODE_REVISION,
        )


def test_sample_evidence_requires_exactly_30_days(tmp_path: Path) -> None:
    report_path, checkpoint_path = backfill_files(
        tmp_path,
        from_date=FROM_DATE,
        to_date=TO_DATE,
        population_count=120,
    )

    with pytest.raises(DartReviewSampleError, match="exactly 30"):
        validate_backfill_evidence(
            report_path=report_path,
            checkpoint_path=checkpoint_path,
            from_date=FROM_DATE,
            to_date=TO_DATE,
            population_count=120,
            code_revision=CODE_REVISION,
        )


def test_sample_range_rejects_unfinished_kst_dates() -> None:
    with pytest.raises(DartReviewSampleError, match="completed KST"):
        validate_sample_date_range(
            date(2026, 7, 1),
            date(2026, 7, 31),
            now=datetime(2026, 7, 24, 0, 0, tzinfo=timezone.utc),
        )


def test_writer_preserves_original_text_and_emits_jsonl_csv_manifest(
    tmp_path: Path,
) -> None:
    items = normalized_items(
        120,
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
    )
    snapshot = CorpusSnapshot(
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
        backend_binding_id=BACKEND_BINDING_ID,
        population_count=120,
        corpus_sha256=corpus_sha256(items),
        items=tuple(items),
    )
    evidence = BackfillEvidence(
        code_revision=CODE_REVISION,
        job_fingerprint="1" * 64,
        checkpoint_version=31,
        checkpoint_sha256="2" * 64,
        report_sha256="3" * 64,
        completed_window_count=30,
        expected_dart_document_count=120,
    )
    jsonl_path = tmp_path / "sample.jsonl"
    csv_path = tmp_path / "sample.csv"
    manifest_path = tmp_path / "manifest.json"
    manifest = write_review_bundle(
        snapshot=snapshot,
        evidence=evidence,
        sample_size=100,
        seed=77,
        code_revision=CODE_REVISION,
        jsonl_output=jsonl_path,
        csv_output=csv_path,
        manifest_output=manifest_path,
        generated_at=datetime(2026, 7, 24, 0, 0, tzinfo=timezone.utc),
    )
    rows = [
        json.loads(line)
        for line in jsonl_path.read_text(encoding="utf-8").splitlines()
    ]
    csv_rows = list(
        csv.DictReader(
            io.StringIO(csv_path.read_text(encoding="utf-8-sig"))
        )
    )
    stored_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len(rows) == len(csv_rows) == 100
    assert rows[0]["title"].startswith(" 원문")
    assert rows[0]["company_name"].startswith(" 회사")
    assert csv_rows[0]["review_outcome"] == ""
    assert manifest == stored_manifest
    assert manifest["release_eligible"] is False
    assert manifest["backend_binding_id"] == BACKEND_BINDING_ID
    assert manifest["sample_count"] == 100
    assert manifest["files"]["jsonl"]["sha256"] == hashlib.sha256(
        jsonl_path.read_bytes()
    ).hexdigest()


def test_csv_neutralizes_formula_cells_while_jsonl_preserves_originals(
    tmp_path: Path,
) -> None:
    items = normalized_items(
        120,
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
    )
    for item in items:
        item["title"] = " \t=HYPERLINK(\"https://example.test\",\"open\")"
        item["company_name"] = "-1+1"
    snapshot = CorpusSnapshot(
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
        backend_binding_id=BACKEND_BINDING_ID,
        population_count=len(items),
        corpus_sha256=corpus_sha256(items),
        items=tuple(items),
    )
    evidence = BackfillEvidence(
        code_revision=CODE_REVISION,
        job_fingerprint="1" * 64,
        checkpoint_version=31,
        checkpoint_sha256="2" * 64,
        report_sha256="3" * 64,
        completed_window_count=30,
        expected_dart_document_count=len(items),
    )
    jsonl_path = tmp_path / "formula-sample.jsonl"
    csv_path = tmp_path / "formula-sample.csv"
    manifest_path = tmp_path / "formula-manifest.json"

    write_review_bundle(
        snapshot=snapshot,
        evidence=evidence,
        sample_size=100,
        seed=77,
        code_revision=CODE_REVISION,
        jsonl_output=jsonl_path,
        csv_output=csv_path,
        manifest_output=manifest_path,
    )

    jsonl_rows = [
        json.loads(line)
        for line in jsonl_path.read_text(encoding="utf-8").splitlines()
    ]
    csv_rows = list(
        csv.DictReader(io.StringIO(csv_path.read_text(encoding="utf-8-sig")))
    )
    assert jsonl_rows[0]["title"] == " \t=HYPERLINK(\"https://example.test\",\"open\")"
    assert jsonl_rows[0]["company_name"] == "-1+1"
    assert csv_rows[0]["title"] == "' \t=HYPERLINK(\"https://example.test\",\"open\")"
    assert csv_rows[0]["company_name"] == "'-1+1"


def test_cli_flags_and_success_exit_with_exact_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    items = normalized_items(
        120,
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
    )
    snapshot = CorpusSnapshot(
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
        backend_binding_id=BACKEND_BINDING_ID,
        population_count=120,
        corpus_sha256=corpus_sha256(items),
        items=tuple(items),
    )
    report_path, checkpoint_path = backfill_files(
        tmp_path,
        from_date=SAMPLE_FROM_DATE,
        to_date=SAMPLE_TO_DATE,
        population_count=120,
    )

    def fake_fetch(
        self: DartReviewCorpusClient,
        *,
        from_date: date,
        to_date: date,
        page_size: int = 100,
    ) -> CorpusSnapshot:
        del self, page_size
        assert from_date == SAMPLE_FROM_DATE and to_date == SAMPLE_TO_DATE
        return snapshot

    monkeypatch.setattr(DartReviewCorpusClient, "fetch", fake_fetch)
    jsonl_path = tmp_path / "cli.jsonl"
    csv_path = tmp_path / "cli.csv"
    manifest_path = tmp_path / "cli-manifest.json"
    exit_code = main(
        [
            "--from-date",
            SAMPLE_FROM_DATE.isoformat(),
            "--to-date",
            SAMPLE_TO_DATE.isoformat(),
            "--sample-size",
            "100",
            "--seed",
            "9",
            "--api-base-url",
            API_BASE,
            "--ops-token",
            OPS_TOKEN,
            "--backend-binding-id",
            BACKEND_BINDING_ID,
            "--backfill-report",
            str(report_path),
            "--checkpoint",
            str(checkpoint_path),
            "--jsonl-output",
            str(jsonl_path),
            "--csv-output",
            str(csv_path),
            "--manifest-output",
            str(manifest_path),
            "--code-revision",
            CODE_REVISION,
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert OPS_TOKEN not in captured.out + captured.err
    assert json.loads(captured.out)["sample_count"] == 100
    assert jsonl_path.exists() and csv_path.exists() and manifest_path.exists()


def test_parser_exposes_workflow_integration_flags() -> None:
    options = {action.dest for action in build_parser()._actions}
    assert {
        "from_date",
        "to_date",
        "sample_size",
        "seed",
        "api_base_url",
        "ops_token",
        "backend_binding_id",
        "backfill_report",
        "checkpoint",
        "jsonl_output",
        "csv_output",
        "manifest_output",
        "code_revision",
    } <= options
