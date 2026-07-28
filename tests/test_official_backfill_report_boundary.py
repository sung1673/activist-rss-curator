from __future__ import annotations

import importlib.util
import json
import sys
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Callable

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / ".github" / "scripts" / "sanitize-official-backfill-report.py"
WORKFLOW = ROOT / ".github" / "workflows" / "official-backfill.yml"
REVISION = "a" * 40


def _load_boundary_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "sanitize_official_backfill_report",
        SCRIPT,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


BOUNDARY = _load_boundary_module()


def _success_report() -> dict[str, object]:
    return {
        "schema_version": 1,
        "status": "succeeded",
        "mode": "apply",
        "dry_run": False,
        "code_revision": REVISION,
        "range_start": "2026-06-28",
        "range_end_exclusive": "2026-06-29",
        "windows_total": 1,
        "windows_attempted": 1,
        "windows_succeeded": 1,
        "windows_failed": 0,
        "windows_remaining": 0,
        "window_results": [
            {
                "window_start": "2026-06-28",
                "window_end_exclusive": "2026-06-29",
                "status": "succeeded",
                "summary": {
                    "official_raw_count": 9,
                    "official_filtered_out_count": 4,
                    "official_accepted_count": 5,
                    "official_acknowledged_count": 5,
                },
            }
        ],
    }


def _failed_report() -> dict[str, object]:
    report = _success_report()
    report.update(
        {
            "status": "failed",
            "windows_succeeded": 0,
            "windows_failed": 1,
            "windows_remaining": 1,
        }
    )
    result = report["window_results"][0]  # type: ignore[index]
    result.update(  # type: ignore[union-attr]
        {
            "status": "failed",
            "error": "RemoteSyncError: safe diagnostic",
        }
    )
    return report


def _expected(command_exit_code: int = 0) -> object:
    return BOUNDARY.ExpectedReport(
        mode="apply",
        from_date=BOUNDARY.date(2026, 6, 28),
        to_date=BOUNDARY.date(2026, 6, 29),
        code_revision=REVISION,
        command_exit_code=command_exit_code,
    )


def test_success_report_is_validated_and_written_atomically(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw.json"
    output = tmp_path / "report.json"
    raw.write_text(json.dumps(_success_report()), encoding="utf-8")

    sanitized = BOUNDARY.sanitize_report(
        raw,
        output,
        expected=_expected(),
        environment={},
    )

    saved = json.loads(output.read_text(encoding="utf-8"))
    assert saved == sanitized
    assert saved["status"] == "succeeded"
    assert saved["command_exit_code"] == 0
    assert saved["artifact_sanitization"]["status"] == "verified"
    assert not tuple(tmp_path.glob(".report.json.*.tmp"))


def test_failure_report_keeps_safe_detail_and_blocks_credentials(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw.json"
    output = tmp_path / "report.json"
    secret = "fixture-only-dart-key-" + ("a" * 40)
    report = _failed_report()
    result = report["window_results"][0]  # type: ignore[index]
    result.update(  # type: ignore[union-attr]
        {
            "error": f"RemoteSyncError token={secret}",
            "request_headers": {"Authorization": f"Bearer {secret}"},
            "response_body": f'{{"api_key":"{secret}"}}',
            "cookies": {"session": secret},
            "safe_failure_code": "remote_sync_rejected",
        }
    )
    raw.write_text(json.dumps(report), encoding="utf-8")

    BOUNDARY.sanitize_report(
        raw,
        output,
        expected=_expected(command_exit_code=17),
        environment={"OPENDART_API_KEYS": secret},
    )

    saved = json.loads(output.read_text(encoding="utf-8"))
    encoded = json.dumps(saved)
    saved_result = saved["window_results"][0]
    assert saved["failed_window"] == "2026-06-28"
    assert saved["failed_windows"] == ["2026-06-28"]
    assert saved_result["safe_failure_code"] == "remote_sync_rejected"
    assert saved_result["error"] == BOUNDARY.REDACTED_VALUE
    assert "request_headers" not in saved_result
    assert "response_body" not in saved_result
    assert "cookies" not in saved_result
    assert secret not in encoded
    assert saved["artifact_sanitization"] == {
        "schema_version": 1,
        "status": "verified",
        "source_size_bytes": raw.stat().st_size,
        "removed_field_count": 3,
        "redacted_value_count": 1,
    }


def _invalid_json(_report: dict[str, object]) -> bytes:
    return b"{not-json"


def _oversized_json(_report: dict[str, object]) -> bytes:
    return b"{" + b" " * BOUNDARY.MAX_REPORT_BYTES + b"}"


def _mutate(
    callback: Callable[[dict[str, object]], None],
) -> Callable[[dict[str, object]], bytes]:
    def apply(report: dict[str, object]) -> bytes:
        callback(report)
        return json.dumps(report).encode("utf-8")

    return apply


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        (_invalid_json, "invalid_json"),
        (_oversized_json, "report_size_out_of_bounds"),
        (
            _mutate(lambda report: report.update(schema_version=2)),
            "invalid_schema_version",
        ),
        (
            _mutate(lambda report: report.update(status="failed")),
            "failure_report_with_successful_command",
        ),
        (
            _mutate(lambda report: report.update(mode="replay")),
            "report_mode_mismatch",
        ),
        (
            _mutate(lambda report: report.update(range_start="2026-06-27")),
            "report_from_date_mismatch",
        ),
        (
            _mutate(lambda report: report.update(code_revision="b" * 40)),
            "report_code_revision_mismatch",
        ),
        (
            _mutate(
                lambda report: report["window_results"][0].update(  # type: ignore[index, union-attr]
                    window_end_exclusive="2026-06-30"
                )
            ),
            "window_result_out_of_range",
        ),
    ],
)
def test_unsafe_or_invalid_input_becomes_safe_generic_fallback(
    tmp_path: Path,
    mutation: Callable[[dict[str, object]], bytes],
    expected_code: str,
) -> None:
    raw = tmp_path / "raw.json"
    output = tmp_path / "report.json"
    raw.write_bytes(mutation(deepcopy(_success_report())))

    exit_code = BOUNDARY.main(
        [
            "--input",
            str(raw),
            "--output",
            str(output),
            "--mode",
            "apply",
            "--from-date",
            "2026-06-28",
            "--to-date",
            "2026-06-29",
            "--code-revision",
            REVISION,
            "--command-exit-code",
            "0",
        ]
    )

    saved = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 2
    assert saved["status"] == "failed"
    assert saved["mode"] == "apply"
    assert saved["range_start"] == "2026-06-28"
    assert saved["range_end_exclusive"] == "2026-06-29"
    assert saved["code_revision"] == REVISION
    assert saved["command_exit_code"] == 0
    assert saved["failure"]["validation_code"] == expected_code
    assert saved["artifact_sanitization"]["status"] == "fallback"
    assert output.stat().st_size <= BOUNDARY.MAX_REPORT_BYTES


def test_verify_existing_replaces_unmarked_report_with_safe_fallback(
    tmp_path: Path,
) -> None:
    output = tmp_path / "report.json"
    output.write_text('{"status":"not_started"}\n', encoding="utf-8")

    exit_code = BOUNDARY.main(
        [
            "--output",
            str(output),
            "--mode",
            "apply",
            "--from-date",
            "2026-06-28",
            "--to-date",
            "2026-06-29",
            "--code-revision",
            REVISION,
            "--command-exit-code",
            "1",
            "--verify-existing",
        ]
    )

    saved = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 2
    assert saved["artifact_sanitization"]["status"] == "fallback"
    assert saved["command_exit_code"] == 1


def test_workflow_never_logs_or_uploads_raw_backfill_output() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    run_start = workflow.index("      - name: Run one-day official backfill windows")
    run_end = workflow.index(
        "      - name: Capture production DART state after replay",
        run_start,
    )
    run_step = workflow[run_start:run_end]
    upload_start = workflow.index("      - name: Preserve backfill report")
    upload_end = workflow.index(
        "      - name: Preserve resumable checkpoint",
        upload_start,
    )
    upload_step = workflow[upload_start:upload_end]

    assert '> "$raw_report" 2> "$raw_stderr"' in run_step
    assert "tee " not in run_step
    assert "sanitize-official-backfill-report.py" in run_step
    assert 'exit "$exit_code"' in run_step
    assert 'rm -f "$raw_report" "$raw_stderr"' in run_step
    assert "official-backfill.stderr.log" not in workflow
    assert ".raw" not in upload_step
    assert ".tmp" not in upload_step
    assert (
        "steps.backfill_artifact_boundary.outputs.evidence_safe == 'true'"
        in upload_step
    )
