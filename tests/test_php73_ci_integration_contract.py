from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_php73_job_runs_isolated_mysql_http_staging_smoke() -> None:
    workflow = yaml.safe_load(
        (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    )
    job = workflow["jobs"]["php73"]

    assert int(job["timeout-minutes"]) < 15
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
    assert preserve["uses"] == "actions/upload-artifact@v7"


def test_php73_fixture_contains_no_repository_secret_reference() -> None:
    fixture = (ROOT / "tests" / "php73_config.php").read_text(encoding="utf-8")

    assert "${{ secrets." not in fixture
    assert "activist_ci" in fixture
    assert "php73-ci-only-hmac-key" in fixture
    assert "'telegram_signal_rebuild_lease_seconds' => 1" in fixture
