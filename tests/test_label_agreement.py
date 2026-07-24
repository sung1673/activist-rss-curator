import json
from pathlib import Path

import pytest

from curator.label_agreement import AgreementError, cohen_kappa, evaluate_pair, load_label_set


def write_labels(path: Path, *, id_field: str, labels: list[str], annotator: str) -> None:
    rows = [
        {
            id_field: f"id-{index}",
            "label": label,
            "label_source": "human",
            "annotator_id": annotator,
        }
        for index, label in enumerate(labels)
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_kappa_is_one_for_identical_labels() -> None:
    labels = {"a": "same_story", "b": "different"}
    assert cohen_kappa(labels, labels) == 1.0


def test_pilot_reports_disagreements_and_threshold(tmp_path: Path) -> None:
    left_path = tmp_path / "left.jsonl"
    right_path = tmp_path / "right.jsonl"
    write_labels(left_path, id_field="pair_id", labels=["same_story"] * 8 + ["different"] * 2, annotator="a")
    write_labels(right_path, id_field="pair_id", labels=["same_story"] * 7 + ["different"] * 3, annotator="b")
    left = load_label_set(left_path, id_field="pair_id", allowed_labels={"same_story", "different"})
    right = load_label_set(right_path, id_field="pair_id", allowed_labels={"same_story", "different"})
    result = evaluate_pair(left, right, expected_count=10, threshold=0.8, task="same_event")
    assert result["disagreement_count"] == 1
    assert result["passed"] is False
    assert result["item_count"] == 10
    assert len(result["item_ids_sha256"]) == 64
    assert len(result["reviewer_a_dataset_sha256"]) == 64


def test_ai_labels_and_same_annotator_fail_closed(tmp_path: Path) -> None:
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    write_labels(first, id_field="event_id", labels=["relevant"], annotator="a")
    row = json.loads(first.read_text(encoding="utf-8"))
    row["label_source"] = "ai"
    first.write_text(json.dumps(row) + "\n", encoding="utf-8")
    with pytest.raises(AgreementError, match="must be human"):
        load_label_set(first, id_field="event_id", allowed_labels={"relevant"})

    write_labels(first, id_field="event_id", labels=["relevant"], annotator="a")
    write_labels(second, id_field="event_id", labels=["relevant"], annotator="a")
    with pytest.raises(AgreementError, match="two different annotators"):
        evaluate_pair(
            load_label_set(first, id_field="event_id", allowed_labels={"relevant"}),
            load_label_set(second, id_field="event_id", allowed_labels={"relevant"}),
            expected_count=1,
            threshold=0.8,
            task="core_event",
        )
