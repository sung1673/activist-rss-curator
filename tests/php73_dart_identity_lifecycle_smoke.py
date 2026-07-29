#!/usr/bin/env python3
"""Regress ordinary DART filer mutations without weakening identity conflicts."""

from __future__ import annotations

import argparse
import hashlib
import json
from typing import Any

import php73_release_state_smoke as release


COMPANY_ID = "00888777"
EVENT_TYPE = "large_shareholding"
ACTION = EVENT_TYPE
EFFECTIVE_AT = "2026-06-30T00:00:00Z"
ORDINARY_EVENT_ID = "event:dart-identity-lifecycle-20260630"
ORDINARY_DOCUMENT_ID = "dart:20260630888777"
COMPLETE_DOCUMENT_ID = "dart:20260630888778"
ORIGINAL_ACTOR_ID = "actor:dart-filer-original"
MUTATED_ACTOR_ID = "actor:dart-filer-mutated"
THIRD_ACTOR_ID = "actor:dart-filer-third"
ORIGINAL_ACTOR_NAME = "CI Original Filing Agent"
MUTATED_ACTOR_NAME = "CI Mutated Filing Agent"
THIRD_ACTOR_NAME = "CI Third Filing Agent"
ORIGINAL_TARGET = "existing ownership purpose"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise release.SmokeFailure(message)


def company() -> dict[str, Any]:
    return {
        "company_id": COMPANY_ID,
        "stock_code": "888777",
        "market": "KOSDAQ",
        "legal_name": "CI DART Identity Lifecycle Corp",
        "listing_status": "listed",
        "record_status": "active",
    }


def document(document_id: str, title: str) -> dict[str, Any]:
    external_id = document_id.split(":", 1)[1]
    original_url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={external_id}"
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
        "original_url": original_url,
        "content_hash": hashlib.sha256(
            f"{title}\n{original_url}\n{external_id}".encode("utf-8")
        ).hexdigest(),
        "collection_key": f"dart-receipt:{external_id}",
        "correction_of_document_id": None,
        "version_no": 1,
        "published_at": EFFECTIVE_AT,
        "retrieved_at": "2026-06-30T00:05:00Z",
        "verification_status": "official",
        "publication_status": "published",
        "is_correction": False,
        "is_cancelled": False,
        "remarks": "",
        "has_later_correction": False,
        "is_withdrawn_by_remark": False,
    }


def actor_candidate(
    event_id: str,
    actor_id: str,
    actor_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    return (
        {
            "actor_id": actor_id,
            "actor_type": "institution",
            "display_name": actor_name,
            "company_id": "",
            "review_status": "pending",
            "record_status": "inactive",
        },
        {
            "event_id": event_id,
            "actor_id": actor_id,
            "actor_role": "filer",
            "review_status": "pending",
        },
    )


def ordinary_event(
    actor_id: str,
    actor_name: str,
    *,
    target: str = ORIGINAL_TARGET,
) -> dict[str, Any]:
    actor, event_actor = actor_candidate(
        ORDINARY_EVENT_ID,
        actor_id,
        actor_name,
    )
    return {
        "event_id": ORDINARY_EVENT_ID,
        "company_id": COMPANY_ID,
        "event_type": EVENT_TYPE,
        "title": "CI ordinary DART large-shareholding filing",
        "metadata": {"title_provenance": "source"},
        "original_language": "ko",
        "summary": "",
        "occurred_at": EFFECTIVE_AT,
        "deadline_at": None,
        "importance": "normal",
        "verification_status": "official",
        "review_status": "pending",
        "publication_status": "draft",
        "collection_key": "dart-identity-lifecycle-ordinary",
        "document_ids": [ORDINARY_DOCUMENT_ID],
        "is_correction": False,
        "is_cancelled": False,
        "review_required": True,
        "action": ACTION,
        "target": target,
        "identity_action": ACTION,
        "identity_target": target,
        "actor_id": actor_id,
        "identity_actor_id": actor_id,
        "identity_effective_at": EFFECTIVE_AT,
        "identity_deadline_at": None,
        "identity_status": "needs_review",
        "comparison_key": None,
        "actor": actor,
        "event_actor": event_actor,
    }


def complete_event(
    actor_id: str,
    actor_name: str,
) -> dict[str, Any]:
    target = "5-percent ownership"
    deadline = "2026-07-07T00:00:00Z"
    comparison_key = release.event_identity_comparison_key(
        COMPANY_ID,
        EVENT_TYPE,
        ACTION,
        target,
        actor_id,
        EFFECTIVE_AT,
        deadline,
    )
    actor, event_actor = actor_candidate(
        comparison_key,
        actor_id,
        actor_name,
    )
    return {
        "event_id": comparison_key,
        "company_id": COMPANY_ID,
        "event_type": EVENT_TYPE,
        "title": "CI complete DART identity filing",
        "metadata": {"title_provenance": "source"},
        "original_language": "ko",
        "summary": "",
        "occurred_at": EFFECTIVE_AT,
        "deadline_at": deadline,
        "importance": "normal",
        "verification_status": "official",
        "review_status": "pending",
        "publication_status": "draft",
        "collection_key": "dart-identity-lifecycle-complete",
        "document_ids": [COMPLETE_DOCUMENT_ID],
        "is_correction": False,
        "is_cancelled": False,
        "review_required": True,
        "action": ACTION,
        "target": target,
        "identity_action": ACTION,
        "identity_target": target,
        "actor_id": actor_id,
        "identity_actor_id": actor_id,
        "identity_effective_at": EFFECTIVE_AT,
        "identity_deadline_at": deadline,
        "identity_status": "complete",
        "comparison_key": comparison_key,
        "actor": actor,
        "event_actor": event_actor,
    }


def snapshot(
    event: dict[str, Any],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    return {
        "companies": [company()],
        "documents": [evidence],
        "events": [event],
        "source_rights": [],
        "run": {},
    }


def cleanup(container_id: str) -> None:
    actor_ids = f"'{ORIGINAL_ACTOR_ID}','{MUTATED_ACTOR_ID}','{THIRD_ACTOR_ID}'"
    release.mysql_execute(
        container_id,
        "DELETE FROM ci_global_lifecycle_observations "
        f"WHERE resolved_event_id='{ORDINARY_EVENT_ID}' "
        f"OR resolved_document_id IN ('{ORDINARY_DOCUMENT_ID}',"
        f"'{COMPLETE_DOCUMENT_ID}');"
        "DELETE FROM ci_event_observations "
        f"WHERE event_id='{ORDINARY_EVENT_ID}' "
        f"OR document_id IN ('{ORDINARY_DOCUMENT_ID}',"
        f"'{COMPLETE_DOCUMENT_ID}');"
        "DELETE FROM ci_event_documents "
        f"WHERE event_id='{ORDINARY_EVENT_ID}' "
        f"OR document_id IN ('{ORDINARY_DOCUMENT_ID}',"
        f"'{COMPLETE_DOCUMENT_ID}');"
        "DELETE FROM ci_event_actors "
        f"WHERE event_id='{ORDINARY_EVENT_ID}' "
        f"OR actor_id IN ({actor_ids});"
        "DELETE FROM ci_governance_events "
        f"WHERE event_id='{ORDINARY_EVENT_ID}' "
        f"OR collection_key='dart-identity-lifecycle-complete';"
        "DELETE FROM ci_documents "
        f"WHERE document_id IN ('{ORDINARY_DOCUMENT_ID}',"
        f"'{COMPLETE_DOCUMENT_ID}');"
        f"DELETE FROM ci_actors WHERE actor_id IN ({actor_ids});",
    )


def canonical_signature(container_id: str) -> str:
    return release.mysql_execute(
        container_id,
        "SELECT CONCAT_WS('|',"
        "(SELECT SHA2(CONCAT_WS(CHAR(31),event_type,title,original_language,"
        "COALESCE(summary,'<NULL>'),occurred_at,COALESCE(deadline_at,'<NULL>'),"
        "importance,verification_status,review_status,publication_status,"
        "COALESCE(identity_action,'<NULL>'),COALESCE(identity_target,'<NULL>'),"
        "COALESCE(identity_actor_id,'<NULL>'),"
        "COALESCE(identity_effective_at,'<NULL>'),"
        "COALESCE(identity_deadline_at,'<NULL>'),identity_status,"
        "COALESCE(comparison_key,'<NULL>'),payload_json,created_at,updated_at),256) "
        "FROM ci_governance_events "
        f"WHERE event_id='{ORDINARY_EVENT_ID}'),"
        "(SELECT SHA2(CONCAT_WS(CHAR(31),company_id,source_class,"
        "COALESCE(source_right_id,'<NULL>'),external_id,"
        "COALESCE(document_type,'<NULL>'),original_language,title,"
        "COALESCE(body_text,'<NULL>'),original_url,content_hash,"
        "COALESCE(collection_key,'<NULL>'),version_no,"
        "COALESCE(retrieved_at,'<NULL>'),verification_status,"
        "publication_status,payload_json,created_at,updated_at),256) "
        "FROM ci_documents "
        f"WHERE document_id='{ORDINARY_DOCUMENT_ID}'),"
        "(SELECT SHA2(CONCAT_WS(CHAR(31),document_id,relation_type,"
        "position_no,created_at),256) FROM ci_event_documents "
        f"WHERE event_id='{ORDINARY_EVENT_ID}'),"
        "(SELECT SHA2(CONCAT_WS(CHAR(31),document_id,source_class,source_key,"
        "first_observed_at,observed_at,payload_hash,payload_json,created_at,"
        "updated_at),256) FROM ci_event_observations "
        f"WHERE event_id='{ORDINARY_EVENT_ID}'),"
        "(SELECT SHA2(CONCAT_WS(CHAR(31),actor_id,actor_role,review_status,"
        "created_at,updated_at),256) FROM ci_event_actors "
        f"WHERE event_id='{ORDINARY_EVENT_ID}'),"
        "(SELECT SHA2(CONCAT_WS(CHAR(31),actor_id,actor_type,display_name,"
        "COALESCE(company_id,'<NULL>'),review_status,record_status,created_at,"
        "updated_at),256) FROM ci_actors "
        f"WHERE actor_id='{ORIGINAL_ACTOR_ID}'))",
    )


def lifecycle_signature(container_id: str) -> str:
    return release.mysql_execute(
        container_id,
        "SELECT CONCAT_WS('|',observation_id,connector_id,country_code,"
        "source_key,external_id,COALESCE(parent_external_id,'<NULL>'),"
        "change_type,observed_at,SHA2(payload_json,256),resolution_status,"
        "resolved_document_id,resolved_event_id,created_at,updated_at) "
        "FROM ci_global_lifecycle_observations "
        f"WHERE resolved_event_id='{ORDINARY_EVENT_ID}' "
        f"AND resolved_document_id='{ORDINARY_DOCUMENT_ID}'",
    )


def lifecycle_metadata(container_id: str) -> dict[str, Any]:
    encoded = release.mysql_execute(
        container_id,
        "SELECT payload_json FROM ci_global_lifecycle_observations "
        f"WHERE resolved_event_id='{ORDINARY_EVENT_ID}' "
        f"AND resolved_document_id='{ORDINARY_DOCUMENT_ID}'",
    )
    decoded = json.loads(encoded)
    require(isinstance(decoded, dict), "lifecycle metadata must be an object")
    return decoded


def assert_private_hashed_identity(metadata: dict[str, Any]) -> None:
    require(
        metadata.get("source_semantics") == "event_identity_changed"
        and metadata.get("conflict_field") == "identity_actor_id"
        and metadata.get("source_right_id") == "official:dart",
        f"lifecycle semantics are incomplete: {metadata!r}",
    )
    previous_hash = metadata.get("previous_identity_sha256")
    current_hash = metadata.get("current_identity_sha256")
    require(
        isinstance(previous_hash, str)
        and isinstance(current_hash, str)
        and len(previous_hash) == 64
        and len(current_hash) == 64
        and all(char in "0123456789abcdef" for char in previous_hash)
        and all(char in "0123456789abcdef" for char in current_hash)
        and previous_hash != current_hash,
        f"identity hashes are invalid: {metadata!r}",
    )
    serialized = json.dumps(
        metadata,
        ensure_ascii=False,
        sort_keys=True,
    )
    for forbidden in (
        ORIGINAL_ACTOR_ID,
        MUTATED_ACTOR_ID,
        THIRD_ACTOR_ID,
        ORIGINAL_ACTOR_NAME,
        MUTATED_ACTOR_NAME,
        THIRD_ACTOR_NAME,
    ):
        require(
            forbidden not in serialized,
            "raw actor identity leaked into lifecycle metadata",
        )
    for key in metadata:
        if "actor" in str(key).casefold():
            require(
                key == "conflict_field",
                f"raw actor field was stored in lifecycle metadata: {key!r}",
            )


def assert_public_response_has_no_identity(
    response: dict[str, Any],
) -> None:
    serialized = json.dumps(response, ensure_ascii=False, sort_keys=True)
    for forbidden in (
        ORDINARY_EVENT_ID,
        ORDINARY_DOCUMENT_ID,
        ORIGINAL_ACTOR_ID,
        MUTATED_ACTOR_ID,
        ORIGINAL_ACTOR_NAME,
        MUTATED_ACTOR_NAME,
    ):
        require(
            forbidden not in serialized,
            f"identity leaked into write response: {serialized!r}",
        )


def run(base_url: str, container_id: str) -> None:
    release.EXPECTED_BACKEND_BINDING_ID = release.mysql_backend_binding_id(container_id)
    release.activate_exact_dart_source_right(base_url, container_id)
    cleanup(container_id)

    ordinary_document = document(
        ORDINARY_DOCUMENT_ID,
        "CI ordinary DART large-shareholding filing",
    )
    original_payload = snapshot(
        ordinary_event(ORIGINAL_ACTOR_ID, ORIGINAL_ACTOR_NAME),
        ordinary_document,
    )
    first = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        original_payload,
        expected_status=200,
    )
    require(first.get("ok") is True, repr(first))
    assert_public_response_has_no_identity(first)
    require(
        release.mysql_execute(
            container_id,
            "SELECT CONCAT_WS('|',identity_status,review_status,"
            "publication_status,COALESCE(comparison_key,'<NULL>'),"
            "identity_actor_id) FROM ci_governance_events "
            f"WHERE event_id='{ORDINARY_EVENT_ID}'",
        )
        == f"needs_review|pending|draft|<NULL>|{ORIGINAL_ACTOR_ID}",
        "ordinary DART fixture did not remain fail-closed",
    )
    require(
        release.mysql_execute(
            container_id,
            "SELECT CONCAT_WS('|',"
            "(SELECT COUNT(*) FROM ci_governance_events "
            f"WHERE event_id='{ORDINARY_EVENT_ID}'),"
            "(SELECT COUNT(*) FROM ci_documents "
            f"WHERE document_id='{ORDINARY_DOCUMENT_ID}'),"
            "(SELECT COUNT(*) FROM ci_event_documents "
            f"WHERE event_id='{ORDINARY_EVENT_ID}'),"
            "(SELECT COUNT(*) FROM ci_event_observations "
            f"WHERE event_id='{ORDINARY_EVENT_ID}'),"
            "(SELECT COUNT(*) FROM ci_event_actors "
            f"WHERE event_id='{ORDINARY_EVENT_ID}' "
            f"AND actor_id='{ORIGINAL_ACTOR_ID}'),"
            "(SELECT COUNT(*) FROM ci_global_lifecycle_observations "
            f"WHERE resolved_event_id='{ORDINARY_EVENT_ID}'))",
        )
        == "1|1|1|1|1|0",
        "ordinary DART fixture row counts are incomplete",
    )
    canonical_before = canonical_signature(container_id)
    require(
        len(canonical_before.split("|")) == 6
        and all(len(value) == 64 for value in canonical_before.split("|")),
        f"canonical signature is incomplete: {canonical_before!r}",
    )

    mutated_payload = snapshot(
        ordinary_event(MUTATED_ACTOR_ID, MUTATED_ACTOR_NAME),
        ordinary_document,
    )
    accepted_mutation = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        mutated_payload,
        expected_status=200,
    )
    require(accepted_mutation.get("ok") is True, repr(accepted_mutation))
    assert_public_response_has_no_identity(accepted_mutation)
    require(
        canonical_signature(container_id) == canonical_before,
        "DART filer mutation changed first-seen canonical state",
    )
    require(
        release.mysql_execute(
            container_id,
            "SELECT CONCAT_WS('|',identity_status,review_status,"
            "publication_status,identity_actor_id,"
            "(SELECT COUNT(*) FROM ci_event_actors "
            f"WHERE event_id='{ORDINARY_EVENT_ID}' "
            f"AND actor_id='{ORIGINAL_ACTOR_ID}'),"
            "(SELECT COUNT(*) FROM ci_event_actors "
            f"WHERE event_id='{ORDINARY_EVENT_ID}' "
            f"AND actor_id='{MUTATED_ACTOR_ID}'),"
            "(SELECT COUNT(*) FROM ci_actors "
            f"WHERE actor_id='{MUTATED_ACTOR_ID}')) "
            "FROM ci_governance_events "
            f"WHERE event_id='{ORDINARY_EVENT_ID}'",
        )
        == f"needs_review|pending|draft|{ORIGINAL_ACTOR_ID}|1|0|0",
        "DART filer mutation changed canonical actor or review state",
    )
    lifecycle_rows = release.mysql_execute(
        container_id,
        "SELECT CONCAT_WS('|',COUNT(*),MIN(connector_id),MIN(country_code),"
        "MIN(source_key),MIN(change_type),MIN(resolution_status),"
        "MIN(resolved_document_id),MIN(resolved_event_id)) "
        "FROM ci_global_lifecycle_observations "
        f"WHERE resolved_event_id='{ORDINARY_EVENT_ID}' "
        f"AND resolved_document_id='{ORDINARY_DOCUMENT_ID}'",
    )
    require(
        lifecycle_rows
        == (
            "1|connector:kr:dart|KR|dart|updated|resolved|"
            f"{ORDINARY_DOCUMENT_ID}|{ORDINARY_EVENT_ID}"
        ),
        f"DART identity lifecycle observation is incomplete: {lifecycle_rows!r}",
    )
    assert_private_hashed_identity(lifecycle_metadata(container_id))
    observation_before_replay = lifecycle_signature(container_id)
    require(
        len(observation_before_replay.split("|")) == 14,
        f"lifecycle signature is incomplete: {observation_before_replay!r}",
    )

    exact_replay = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        mutated_payload,
        expected_status=200,
    )
    require(exact_replay.get("ok") is True, repr(exact_replay))
    assert_public_response_has_no_identity(exact_replay)
    require(
        canonical_signature(container_id) == canonical_before
        and lifecycle_signature(container_id) == observation_before_replay,
        "exact DART filer-mutation replay changed canonical or lifecycle state",
    )
    require(
        release.mysql_execute(
            container_id,
            "SELECT COUNT(*) FROM ci_global_lifecycle_observations "
            f"WHERE resolved_event_id='{ORDINARY_EVENT_ID}' "
            f"AND resolved_document_id='{ORDINARY_DOCUMENT_ID}'",
        )
        == "1",
        "exact replay appended a duplicate lifecycle observation",
    )

    non_actor_conflict = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        snapshot(
            ordinary_event(
                ORIGINAL_ACTOR_ID,
                ORIGINAL_ACTOR_NAME,
                target="conflicting target",
            ),
            ordinary_document,
        ),
        expected_status=409,
    )
    require(
        release.error_code(non_actor_conflict) == "event_identity_field_conflict",
        f"non-actor identity conflict did not fail closed: {non_actor_conflict!r}",
    )
    require(
        canonical_signature(container_id) == canonical_before
        and lifecycle_signature(container_id) == observation_before_replay,
        "non-actor conflict changed canonical or lifecycle state",
    )

    release.mysql_execute(
        container_id,
        "UPDATE ci_governance_events "
        "SET review_status='approved',publication_status='published' "
        f"WHERE event_id='{ORDINARY_EVENT_ID}'",
    )
    approved_signature = canonical_signature(container_id)
    approved_conflict = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        snapshot(
            ordinary_event(THIRD_ACTOR_ID, THIRD_ACTOR_NAME),
            ordinary_document,
        ),
        expected_status=409,
    )
    require(
        release.error_code(approved_conflict) == "event_identity_field_conflict",
        f"approved event accepted an actor mutation: {approved_conflict!r}",
    )
    require(
        canonical_signature(container_id) == approved_signature
        and lifecycle_signature(container_id) == observation_before_replay,
        "approved-event conflict changed canonical or lifecycle state",
    )

    complete_document = document(
        COMPLETE_DOCUMENT_ID,
        "CI complete DART identity filing",
    )
    complete_original = complete_event(
        ORIGINAL_ACTOR_ID,
        ORIGINAL_ACTOR_NAME,
    )
    complete_first = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        snapshot(complete_original, complete_document),
        expected_status=200,
    )
    require(complete_first.get("ok") is True, repr(complete_first))
    complete_event_id = str(complete_original["event_id"])
    complete_before = release.mysql_execute(
        container_id,
        "SELECT SHA2(CONCAT_WS(CHAR(31),identity_status,comparison_key,"
        "identity_actor_id,payload_json,created_at,updated_at),256) "
        "FROM ci_governance_events "
        f"WHERE event_id='{complete_event_id}'",
    )
    complete_mutated = complete_event(
        THIRD_ACTOR_ID,
        THIRD_ACTOR_NAME,
    )
    complete_mutated["event_id"] = complete_event_id
    complete_mutated["event_actor"]["event_id"] = complete_event_id
    rejected_complete = release.request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        snapshot(complete_mutated, complete_document),
        expected_status=409,
    )
    require(
        release.error_code(rejected_complete) == "event_identity_field_conflict",
        f"complete event accepted an actor mutation: {rejected_complete!r}",
    )
    require(
        release.mysql_execute(
            container_id,
            "SELECT SHA2(CONCAT_WS(CHAR(31),identity_status,comparison_key,"
            "identity_actor_id,payload_json,created_at,updated_at),256) "
            "FROM ci_governance_events "
            f"WHERE event_id='{complete_event_id}'",
        )
        == complete_before
        and release.mysql_execute(
            container_id,
            "SELECT COUNT(*) FROM ci_global_lifecycle_observations "
            f"WHERE resolved_event_id='{complete_event_id}'",
        )
        == "0",
        "complete-event conflict changed state or wrote lifecycle metadata",
    )

    cleanup(container_id)
    print(
        "PHP 7.3 DART identity lifecycle smoke passed "
        "(actor mutation isolation, privacy, replay, fail-closed negatives)."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--mysql-container-id", required=True)
    args = parser.parse_args()
    run(args.base_url, args.mysql_container_id)


if __name__ == "__main__":
    main()
