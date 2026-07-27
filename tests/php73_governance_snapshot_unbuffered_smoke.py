#!/usr/bin/env python3
"""Exercise guarded DART snapshot writes with unbuffered native PDO MySQL."""

from __future__ import annotations

import argparse
import hashlib
from typing import Any

import php73_release_state_smoke as release


COMPANY_ID = "00888888"
ACTOR_ID = "actor:unbuffered-dart-smoke"
EVENT_TYPE = "shareholder_proposal"
ACTION = "submit"
TARGET = "board seat"
EFFECTIVE_AT = "2026-07-27"
DEADLINE_AT = "2026-08-31"
ORIGINAL_DOCUMENT_ID = "dart:20260727888001"
CORRECTION_DOCUMENT_ID = "dart:20260727888002"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise release.SmokeFailure(message)


def company() -> dict[str, Any]:
    return {
        "company_id": COMPANY_ID,
        "stock_code": "888888",
        "market": "KOSDAQ",
        "legal_name": "CI Unbuffered DART Corp",
        "listing_status": "listed",
        "record_status": "active",
    }


def document(
    document_id: str,
    title: str,
    *,
    correction_of: str | None = None,
    version_no: int = 1,
) -> dict[str, Any]:
    external_id = document_id.split(":", 1)[1]
    url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={external_id}"
    return {
        "document_id": document_id,
        "company_id": COMPANY_ID,
        "source": "dart",
        "source_right_id": "official:dart",
        "source_class": "official_disclosure",
        "external_id": external_id,
        "document_type": EVENT_TYPE,
        "original_language": "ko",
        "title": title,
        "body_text": "",
        "original_url": url,
        "content_hash": hashlib.sha256(
            f"{title}\n{url}\n{external_id}".encode()
        ).hexdigest(),
        "collection_key": "unbuffered-dart-correction-chain",
        "correction_of_document_id": correction_of,
        "version_no": version_no,
        "published_at": "2026-07-27T00:00:00Z",
        "retrieved_at": "2026-07-27T00:05:00Z",
        "verification_status": "official",
        "publication_status": "published",
        "is_correction": correction_of is not None,
    }


def event(target: str, comparison_key: str) -> dict[str, Any]:
    return {
        "event_id": release.event_identity_comparison_key(
            COMPANY_ID,
            EVENT_TYPE,
            ACTION,
            TARGET,
            ACTOR_ID,
            EFFECTIVE_AT,
            DEADLINE_AT,
        ),
        "company_id": COMPANY_ID,
        "event_type": EVENT_TYPE,
        "title": "CI unbuffered DART filing",
        "original_language": "ko",
        "summary": "",
        "occurred_at": EFFECTIVE_AT,
        "deadline_at": DEADLINE_AT,
        "importance": "normal",
        "verification_status": "official",
        "collection_key": "unbuffered-dart-event",
        "document_ids": [ORIGINAL_DOCUMENT_ID],
        "review_required": True,
        "action": ACTION,
        "target": target,
        "identity_action": ACTION,
        "identity_target": target,
        "identity_actor_id": ACTOR_ID,
        "identity_effective_at": EFFECTIVE_AT,
        "identity_deadline_at": DEADLINE_AT,
        "identity_status": "complete",
        "comparison_key": comparison_key,
        "actor": {
            "actor_id": ACTOR_ID,
            "actor_type": "institution",
            "display_name": "CI Unbuffered Filer",
            "company_id": "",
            "review_status": "pending",
            "record_status": "inactive",
        },
        "event_actor": {
            "event_id": release.event_identity_comparison_key(
                COMPANY_ID,
                EVENT_TYPE,
                ACTION,
                TARGET,
                ACTOR_ID,
                EFFECTIVE_AT,
                DEADLINE_AT,
            ),
            "actor_id": ACTOR_ID,
            "actor_role": "filer",
            "review_status": "pending",
        },
    }


def snapshot_counts(container_id: str) -> str:
    event_id = release.event_identity_comparison_key(
        COMPANY_ID,
        EVENT_TYPE,
        ACTION,
        TARGET,
        ACTOR_ID,
        EFFECTIVE_AT,
        DEADLINE_AT,
    )
    return release.mysql_execute(
        container_id,
        "SELECT "
        f"(SELECT COUNT(*) FROM ci_companies WHERE company_id='{COMPANY_ID}'),"
        f"(SELECT COUNT(*) FROM ci_documents WHERE document_id IN "
        f"('{ORIGINAL_DOCUMENT_ID}','{CORRECTION_DOCUMENT_ID}')),"
        f"(SELECT COUNT(*) FROM ci_governance_events WHERE event_id='{event_id}'),"
        f"(SELECT COUNT(*) FROM ci_event_documents WHERE event_id='{event_id}'),"
        f"(SELECT COUNT(*) FROM ci_event_observations WHERE event_id='{event_id}'),"
        f"(SELECT COUNT(*) FROM ci_event_actors WHERE event_id='{event_id}' "
        f"AND actor_id='{ACTOR_ID}')",
    )


def run(base_url: str, container_id: str) -> None:
    release.EXPECTED_BACKEND_BINDING_ID = release.mysql_backend_binding_id(
        container_id
    )
    release.activate_exact_dart_source_right(base_url, container_id)
    comparison_key = release.event_identity_comparison_key(
        COMPANY_ID,
        EVENT_TYPE,
        ACTION,
        TARGET,
        ACTOR_ID,
        EFFECTIVE_AT,
        DEADLINE_AT,
    )
    original_payload = {
        "companies": [company()],
        "documents": [
            document(ORIGINAL_DOCUMENT_ID, "CI unbuffered DART filing")
        ],
        "events": [event(TARGET, comparison_key)],
        "source_rights": [],
        "run": {},
    }
    first = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        original_payload,
        expected_status=200,
    )
    upserted = first.get("upserted", {})
    require(
        first.get("ok") is True
        and upserted.get("companies") == 1
        and upserted.get("documents") == 1
        and upserted.get("events") == 1
        and upserted.get("event_documents") == 1
        and upserted.get("event_observations") == 1
        and upserted.get("event_actors") == 1,
        repr(first),
    )
    require(snapshot_counts(container_id) == "1\t1\t1\t1\t1\t1", "first apply")

    replay = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        original_payload,
        expected_status=200,
    )
    require(replay.get("ok") is True, repr(replay))
    require(snapshot_counts(container_id) == "1\t1\t1\t1\t1\t1", "exact replay")

    correction_payload = {
        "companies": [company()],
        "documents": [
            document(
                CORRECTION_DOCUMENT_ID,
                "CI unbuffered DART filing correction",
                correction_of=ORIGINAL_DOCUMENT_ID,
                version_no=2,
            )
        ],
        "events": [],
        "source_rights": [],
        "run": {},
    }
    correction = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        correction_payload,
        expected_status=200,
    )
    require(
        correction.get("ok") is True
        and correction.get("upserted", {}).get("documents") == 1,
        repr(correction),
    )
    require(snapshot_counts(container_id) == "1\t2\t1\t1\t1\t1", "correction")

    correction_replay = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        correction_payload,
        expected_status=200,
    )
    require(correction_replay.get("ok") is True, repr(correction_replay))
    require(
        snapshot_counts(container_id) == "1\t2\t1\t1\t1\t1",
        "correction replay",
    )

    conflict_target = "conflicting board seat"
    conflict_key = release.event_identity_comparison_key(
        COMPANY_ID,
        EVENT_TYPE,
        ACTION,
        conflict_target,
        ACTOR_ID,
        EFFECTIVE_AT,
        DEADLINE_AT,
    )
    conflict = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot_dart_guarded",
        {
            "companies": [company()],
            "documents": [],
            "events": [event(conflict_target, conflict_key)],
            "source_rights": [],
            "run": {"source_key": "dart"},
        },
        expected_status=409,
    )
    require(
        conflict.get("error") == "event_identity_field_conflict",
        repr(conflict),
    )
    require(
        snapshot_counts(container_id) == "1\t2\t1\t1\t1\t1",
        "validation rollback",
    )
    print(
        "PHP 7.3 unbuffered guarded DART snapshot smoke passed "
        "(apply, replay, correction, actor, rollback)."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--mysql-container-id", required=True)
    args = parser.parse_args()
    run(args.base_url, args.mysql_container_id)


if __name__ == "__main__":
    main()
