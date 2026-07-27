from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
RECOVERY = (ROOT / "docs" / "opendart-credential-recovery.md").read_text(
    encoding="utf-8"
)
QUOTA = (ROOT / "docs" / "dart-global-quota.md").read_text(encoding="utf-8")
GLOBAL_BACKFILL = (ROOT / "docs" / "global-official-backfill.md").read_text(
    encoding="utf-8"
)
OPENAPI = yaml.safe_load(
    (ROOT / "deploy" / "activist" / "openapi.yaml").read_text(encoding="utf-8")
)

PUBLIC_PERSISTENCE_DETAILS = {
    "transaction_commit_failed",
    "transaction_state_invalid",
    "transaction_readback_connection_failed",
    "transaction_readback_binding_failed",
    "transaction_readback_attempt_failed",
    "transaction_readback_day_failed",
    "transaction_readback_credential_failed",
}
INTERNAL_PERSISTENCE_OUTCOMES = {
    "commit_threw",
    "commit_returned_false",
    "transaction_state_after_commit",
    "cursor_close_threw",
    "cursor_close_returned_false",
    "persistence_failure",
}


def test_recovery_runs_apply_only_after_switching_to_dart_canary() -> None:
    off_index = RECOVERY.index("`GOVERNANCE_PIPELINE_MODE=off`")
    closed_index = RECOVERY.index("`release_state=closed`")
    canary_index = RECOVERY.index("`GOVERNANCE_PIPELINE_MODE=dart_canary`")
    apply_step_index = RECOVERY.index("\n7.")
    apply_index = RECOVERY.index("`apply`", apply_step_index)

    assert off_index < closed_index < canary_index < apply_step_index < apply_index
    assert "`DART_OFFICIAL_INGEST_ENABLED=false`" in RECOVERY
    assert RECOVERY.count("`GOVERNANCE_PIPELINE_MODE=dart_canary`") >= 1

    normalized_global = " ".join(GLOBAL_BACKFILL.split())
    sec_global_index = normalized_global.index("SEC `global-backfill.yml`")
    sec_off_index = normalized_global.index(
        "`GOVERNANCE_PIPELINE_MODE=off`",
        sec_global_index,
    )
    dart_apply_index = normalized_global.index("DART `official-backfill.yml` apply")
    dart_canary_index = normalized_global.index(
        "`GOVERNANCE_PIPELINE_MODE=dart_canary`",
        dart_apply_index,
    )
    assert sec_global_index < sec_off_index < dart_apply_index < dart_canary_index


def test_internal_quota_diagnostics_are_documented_but_not_public_api_fields() -> None:
    post = OPENAPI["paths"]["/ops/dart-quota"]["post"]
    durable_failure = post["x-durable-ack-contract"]["persistence-failure"]
    error_schema = OPENAPI["components"]["schemas"][
        "DartQuotaPersistenceFailure"
    ]["properties"]["error"]

    assert durable_failure["code"] == "dart_quota_persistence_failed"
    assert set(durable_failure["detail-enum"]) == PUBLIC_PERSISTENCE_DETAILS
    assert set(error_schema["required"]) == {"code", "detail"}
    assert error_schema["additionalProperties"] is False
    assert (
        set(error_schema["properties"]["detail"]["enum"])
        == PUBLIC_PERSISTENCE_DETAILS
    )

    serialized_openapi = (ROOT / "deploy" / "activist" / "openapi.yaml").read_text(
        encoding="utf-8"
    )
    for outcome in INTERNAL_PERSISTENCE_OUTCOMES:
        assert f"`{outcome}`" in QUOTA
        assert outcome not in serialized_openapi

    assert "`sqlstate_class`" in QUOTA
    assert "`driver_code`" in QUOTA
    normalized_quota = " ".join(QUOTA.split())
    assert normalized_quota.index("HTTP") < normalized_quota.index("OpenAPI schema")
    assert "`detail=transaction_commit_failed`" in normalized_quota
