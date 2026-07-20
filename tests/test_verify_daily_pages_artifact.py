from __future__ import annotations

import importlib.util
from datetime import date, datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[1]


def load_verifier() -> ModuleType:
    path = ROOT / ".github" / "scripts" / "verify-daily-pages-artifact.py"
    spec = importlib.util.spec_from_file_location("bside_daily_pages_verifier", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def success_responses(marker_name: str) -> dict[str, dict[str, Any]]:
    return {
        "/actions/artifacts": {
            "total_count": 1,
            "artifacts": [
                {
                    "id": 321,
                    "name": marker_name,
                    "expired": False,
                    "created_at": "2026-07-15T20:50:00Z",
                    "workflow_run": {"id": 654},
                }
            ],
        },
        "/actions/runs/654": {
            "id": 654,
            "workflow_id": 987,
            "path": ".github/workflows/daily.yml",
            "status": "completed",
            "conclusion": "success",
            "event": "schedule",
        },
        "/actions/workflows/987": {
            "id": 987,
            "path": ".github/workflows/daily.yml",
        },
    }


def install_fake_api(monkeypatch, verifier: ModuleType, responses: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:  # type: ignore[no-untyped-def]
    calls: list[dict[str, Any]] = []

    def fake_get_json(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return responses[kwargs["path"]]

    monkeypatch.setattr(verifier, "github_get_json", fake_get_json)
    return calls


def test_success_verifies_artifact_run_and_workflow(monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    verifier = load_verifier()
    marker_name = "governance-pages-ready-2026-07-16"
    calls = install_fake_api(monkeypatch, verifier, success_responses(marker_name))
    monkeypatch.setenv("GH_TOKEN", "super-secret-token")
    monkeypatch.setenv("GITHUB_TOKEN", "ignored-token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "sung1673/activist-rss-curator")
    monkeypatch.setenv("BSIDE_KST_DATE", "2026-07-16")

    assert verifier.main() == 0

    captured = capsys.readouterr()
    assert "pages_marker_verified=1" in captured.out
    assert "artifact_id=321" in captured.out
    assert "run_id=654" in captured.out
    assert "workflow=.github/workflows/daily.yml" in captured.out
    assert "event=schedule" in captured.out
    assert "super-secret-token" not in captured.out + captured.err
    assert [call["path"] for call in calls] == [
        "/actions/artifacts",
        "/actions/runs/654",
        "/actions/workflows/987",
    ]
    assert calls[0]["query"] == {"name": marker_name, "per_page": "100"}
    assert all(call["token"] == "super-secret-token" for call in calls)


def test_github_token_fallback_and_manual_run_are_allowed(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    verifier = load_verifier()
    responses = success_responses("governance-pages-ready-2026-07-16")
    responses["/actions/runs/654"]["event"] = "workflow_dispatch"
    calls = install_fake_api(monkeypatch, verifier, responses)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.setenv("GITHUB_TOKEN", "actions-token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repository")
    monkeypatch.setenv("BSIDE_KST_DATE", "2026-07-16")

    assert verifier.main() == 0
    assert all(call["token"] == "actions-token" for call in calls)


@pytest.mark.parametrize(
    ("artifact_changes", "expected"),
    [
        ({"expired": True}, "no unexpired"),
        ({"created_at": "2026-07-15T20:39:59Z"}, "05:40-07:00"),
        ({"created_at": "2026-07-15T22:00:01Z"}, "05:40-07:00"),
        ({"workflow_run": None}, "no workflow run"),
    ],
)
def test_artifact_must_be_current_unexpired_and_linked(
    monkeypatch,
    artifact_changes: dict[str, Any],
    expected: str,
) -> None:  # type: ignore[no-untyped-def]
    verifier = load_verifier()
    responses = success_responses("governance-pages-ready-2026-07-16")
    responses["/actions/artifacts"]["artifacts"][0].update(artifact_changes)
    install_fake_api(monkeypatch, verifier, responses)

    with pytest.raises(verifier.VerificationError, match=expected):
        verifier.verify_daily_pages_marker(
            repository="owner/repository",
            token="secret",
            kst_date=date(2026, 7, 16),
        )


@pytest.mark.parametrize(
    ("run_changes", "expected"),
    [
        ({"conclusion": "failure"}, "not completed successfully"),
        ({"status": "in_progress", "conclusion": None}, "not completed successfully"),
        ({"event": "push"}, "unsupported trigger"),
        ({"path": ".github/workflows/watchdog.yml"}, "not from daily.yml"),
        ({"workflow_id": None}, "no valid workflow id"),
    ],
)
def test_run_must_be_successful_daily_workflow(
    monkeypatch,
    run_changes: dict[str, Any],
    expected: str,
) -> None:  # type: ignore[no-untyped-def]
    verifier = load_verifier()
    responses = success_responses("governance-pages-ready-2026-07-16")
    responses["/actions/runs/654"].update(run_changes)
    install_fake_api(monkeypatch, verifier, responses)

    with pytest.raises(verifier.VerificationError, match=expected):
        verifier.verify_daily_pages_marker(
            repository="owner/repository",
            token="secret",
            kst_date=date(2026, 7, 16),
        )


def test_workflow_metadata_must_match_daily_yml(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    verifier = load_verifier()
    responses = success_responses("governance-pages-ready-2026-07-16")
    responses["/actions/workflows/987"]["path"] = ".github/workflows/other.yml"
    install_fake_api(monkeypatch, verifier, responses)

    with pytest.raises(verifier.VerificationError, match="workflow 987 is not daily.yml"):
        verifier.verify_daily_pages_marker(
            repository="owner/repository",
            token="secret",
            kst_date=date(2026, 7, 16),
        )


def test_recovered_deployment_marker_is_accepted_at_0630_kst(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    verifier = load_verifier()
    responses = success_responses("governance-pages-ready-2026-07-16")
    responses["/actions/artifacts"]["artifacts"][0]["created_at"] = "2026-07-15T21:30:00Z"
    install_fake_api(monkeypatch, verifier, responses)

    result = verifier.verify_daily_pages_marker(
        repository="owner/repository",
        token="secret",
        kst_date=date(2026, 7, 16),
    )

    assert result["artifact_id"] == 321


def test_invalid_configuration_fails_without_disclosing_token(monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    verifier = load_verifier()
    monkeypatch.setenv("GH_TOKEN", "must-never-appear")
    monkeypatch.setenv("GITHUB_REPOSITORY", "invalid repository value")
    monkeypatch.setenv("BSIDE_KST_DATE", "16-07-2026")

    assert verifier.main() == 1

    captured = capsys.readouterr()
    assert "GITHUB_REPOSITORY must use owner/repository format" in captured.err
    assert "must-never-appear" not in captured.out + captured.err


def test_date_defaults_to_current_kst_day() -> None:
    verifier = load_verifier()
    now = datetime(2026, 7, 15, 16, 30, tzinfo=timezone.utc)
    assert verifier.resolve_kst_date(None, now=now) == date(2026, 7, 16)


def test_timestamp_requires_timezone() -> None:
    verifier = load_verifier()
    assert verifier.parse_github_timestamp("2026-07-15T20:50:00Z") == datetime(
        2026, 7, 15, 20, 50, tzinfo=timezone.utc
    )
    assert verifier.parse_github_timestamp("2026-07-15T20:50:00") is None
