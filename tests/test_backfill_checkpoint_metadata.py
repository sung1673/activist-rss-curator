from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = (
    ROOT / ".github" / "scripts" / "validate-backfill-checkpoint-metadata.py"
)
SPEC = importlib.util.spec_from_file_location(
    "validate_backfill_checkpoint_metadata",
    SCRIPT_PATH,
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def valid_payload() -> dict[str, object]:
    return {
        "mode": "apply",
        "source": "dart",
        "from_date": "2026-06-28",
        "to_date": "2026-07-28",
        "checkpoint_present": False,
    }


def validate(payload: object) -> None:
    MODULE.validate_empty_checkpoint_metadata(
        payload,
        mode="apply",
        source="dart",
        from_date="2026-06-28",
        to_date="2026-07-28",
    )


def test_exact_empty_checkpoint_marker_is_accepted() -> None:
    validate(valid_payload())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mode", "dry-run"),
        ("source", "kind"),
        ("from_date", "2026-06-27"),
        ("to_date", "2026-07-29"),
        ("checkpoint_present", True),
    ],
)
def test_mismatched_empty_checkpoint_marker_is_rejected(
    field: str,
    value: object,
) -> None:
    payload = valid_payload()
    payload[field] = value
    with pytest.raises(MODULE.CheckpointMetadataError):
        validate(payload)


@pytest.mark.parametrize("mutation", ["missing", "unexpected"])
def test_checkpoint_metadata_requires_exact_keys(mutation: str) -> None:
    payload = valid_payload()
    if mutation == "missing":
        del payload["source"]
    else:
        payload["untrusted"] = "value"
    with pytest.raises(MODULE.CheckpointMetadataError):
        validate(payload)


@pytest.mark.parametrize("payload", [[], "not-an-object", None])
def test_checkpoint_metadata_requires_an_object(payload: object) -> None:
    with pytest.raises(MODULE.CheckpointMetadataError):
        validate(payload)


def test_checkpoint_metadata_loader_rejects_malformed_json(tmp_path: Path) -> None:
    metadata = tmp_path / "checkpoint-metadata.json"
    metadata.write_text('{"mode":', encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        MODULE.load_checkpoint_metadata(metadata)


def test_checkpoint_metadata_loader_rejects_duplicate_keys(tmp_path: Path) -> None:
    metadata = tmp_path / "checkpoint-metadata.json"
    metadata.write_text(
        '{"mode":"apply","mode":"apply","source":"dart",'
        '"from_date":"2026-06-28","to_date":"2026-07-28",'
        '"checkpoint_present":false}',
        encoding="utf-8",
    )
    with pytest.raises(MODULE.CheckpointMetadataError):
        MODULE.load_checkpoint_metadata(metadata)


def test_checkpoint_metadata_cli_fails_closed_for_missing_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = MODULE.main(
        [
            "--metadata",
            str(tmp_path / "missing.json"),
            "--mode",
            "apply",
            "--source",
            "dart",
            "--from-date",
            "2026-06-28",
            "--to-date",
            "2026-07-28",
        ]
    )
    assert exit_code == 1
    assert "Unsafe empty checkpoint artifact" in capsys.readouterr().out
