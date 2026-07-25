from __future__ import annotations

import runpy
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_php73_job_runs_isolated_mysql_http_staging_smoke() -> None:
    workflow = yaml.safe_load(
        (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    )
    job = workflow["jobs"]["php73"]

    # The v2 schema/lifecycle smoke adds a bounded seven-minute integration
    # stage to the existing PHP 7.3 + MySQL checks. Keep the whole job bounded
    # without forcing a timeout shorter than its explicit child stages.
    assert int(job["timeout-minutes"]) <= 20
    mysql = job["services"]["mysql"]
    assert mysql["image"].startswith("mysql:8.0.")
    assert mysql["env"] == {
        "MYSQL_DATABASE": "activist_ci",
        "MYSQL_USER": "activist_ci",
        "MYSQL_PASSWORD": "activist_ci_password",
        "MYSQL_ROOT_PASSWORD": "activist_ci_root_password",
    }
    assert mysql["ports"] == ["3306:3306"]
    assert "mysqladmin ping" in mysql["options"]

    steps = {step["name"]: step for step in job["steps"]}
    mysql_auth = steps["Configure MySQL test user for PHP 7.3"]
    assert "mysql_native_password" in mysql_auth["run"]
    assert "job.services.mysql.id" in mysql_auth["env"]["MYSQL_CONTAINER_ID"]

    install = steps["Install isolated PHP API test configuration"]
    assert "tests/php73_config.php" in install["run"]
    assert "deploy/activist/_private/config.php" in install["run"]

    smoke = steps["Run PHP 7.3 and MySQL Telegram staging smoke"]
    assert int(smoke["timeout-minutes"]) <= 3
    assert "php -S 127.0.0.1:8787" in smoke["run"]
    assert "tests/php73_router.php" in smoke["run"]
    assert "python3 tests/php73_staging_smoke.py" in smoke["run"]

    identity_index = steps["Validate Telegram channel identity index migration"]
    assert "DROP INDEX idx_telegram_channel_message_id" in identity_index["run"]
    assert "005_telegram_channel_identity_index.sql" in identity_index["run"]
    assert "sed 's/activist_/ci_/g'" in identity_index["run"]
    assert "ci_telegram_messages" in identity_index["run"]
    assert "telegram_channel_id,telegram_message_id" in identity_index["run"]
    assert "DROP COLUMN identity_migration_version" in identity_index["run"]
    assert "ci_telegram_channels" in identity_index["run"]
    assert "version_columns" in identity_index["run"]
    assert "accepted an incompatible marker column" in identity_index["run"]
    assert "accepted an incompatible same-name index" in identity_index["run"]
    assert "--verify-post-migration" in identity_index["run"]

    collect = steps["Collect PHP and MySQL diagnostics"]
    preserve = steps["Preserve failed PHP and MySQL diagnostics"]
    assert collect["if"] == "failure()"
    assert "docker logs" in collect["run"]
    assert preserve["if"] == "failure()"
    assert preserve["uses"] == "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"


def test_php73_fixture_contains_no_repository_secret_reference() -> None:
    fixture = (ROOT / "tests" / "php73_config.php").read_text(encoding="utf-8")

    assert "${{ secrets." not in fixture
    assert "activist_ci" in fixture
    assert "php73-ci-only-hmac-key" in fixture
    assert "'release_authorizer' => array(" in fixture
    assert (
        "83a00f2797d3a214080e86809cb2eba45e0163581c1612ee7699055fa109ecb7"
        in fixture
    )
    assert "'telegram_signal_rebuild_lease_seconds' => 1" in fixture


def test_php73_release_fixture_preserves_canonical_dart_source_right_id() -> None:
    smoke = (ROOT / "tests" / "php73_release_state_smoke.py").read_text(encoding="utf-8")
    identity_fixture = smoke[
        smoke.index("def exercise_event_identity_datetime_storage") :
        smoke.index("def exercise_dart_review_corpus")
    ]

    assert 'source_right_id = "official:dart"' in identity_fixture
    assert "DELETE FROM ci_event_observations " in identity_fixture
    assert "identity precision fixture must not leak into later corpus checks" in identity_fixture
    assert "official:dart-identity-precision-smoke" not in smoke


def test_php73_global_fixture_restores_the_latest_source_title_from_mysql() -> None:
    smoke = (ROOT / "tests" / "php73_global_v2_smoke.py").read_text(encoding="utf-8")
    restoration = smoke[
        smoke.index("automated_with_mutation") :
        smoke.index("automated_preserved")
    ]

    assert "SET e.title=d.title" in restoration
    assert "SELECT MAX(latest.version_no)" in restoration
    assert "BINARY e.title=BINARY d.title" in restoration


def test_php73_global_fixture_refreshes_rights_revision_after_grant_mutation() -> None:
    smoke = (ROOT / "tests" / "php73_global_v2_smoke.py").read_text(encoding="utf-8")
    restored_grant = smoke[
        smoke.index("stale_rights_revision = rights_revision") :
        smoke.index("pagination_ids = add_byte_pagination_fixture_events")
    ]

    assert "restored_eligibility" in restored_grant
    assert 'use": "collect"' in restored_grant
    assert "rights_revision = restored_eligibility.get" in restored_grant
    assert "rights_revision != stale_rights_revision" in restored_grant


def test_lifecycle_fixture_keeps_raw_and_ack_counts_consistent() -> None:
    module = runpy.run_path(
        str(ROOT / "tests" / "php73_global_v2_smoke.py"),
        run_name="php73_global_v2_smoke_contract",
    )
    payload = module["empty_chunk_payload"](
        rights_revision="a" * 64,
        idempotency_key="lifecycle-count-contract",
        retrieved_at="2026-07-24T00:00:00Z",
        batch_id="global-batch:" + ("b" * 64),
        index=1,
        count=1,
    )
    module["attach_single_chunk_lifecycle_observations"](
        payload,
        [{"observation_id": "globalobs:" + ("c" * 40)}],
    )

    envelope = payload["envelope"]
    assert envelope["raw_count"] == 1
    assert envelope["chunk"]["batch_raw_count"] == 1
    assert envelope["chunk"]["batch_acknowledged_count"] == 1
