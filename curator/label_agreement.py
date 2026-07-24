"""Validate independent pilot labels and calculate nominal Cohen's kappa."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence


PILOT_LABEL_SOURCE = "human"


class AgreementError(ValueError):
    """Raised when pilot labels are incomplete or are not independent human work."""


@dataclass(frozen=True)
class LabelSet:
    annotator_id: str
    labels: dict[str, str]
    dataset_sha256: str
    item_ids_sha256: str


def load_label_set(path: Path, *, id_field: str, allowed_labels: set[str]) -> LabelSet:
    raw_bytes = path.read_bytes()
    labels: dict[str, str] = {}
    annotators: set[str] = set()
    for line_number, raw_line in enumerate(raw_bytes.decode("utf-8").splitlines(), start=1):
        if not raw_line.strip():
            continue
        location = f"{path}:{line_number}"
        try:
            row = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise AgreementError(f"{location}: invalid JSON") from exc
        if not isinstance(row, dict):
            raise AgreementError(f"{location}: expected an object")
        item_id = str(row.get(id_field) or "").strip()
        label = str(row.get("label") or "").strip()
        source = str(row.get("label_source") or "").strip().casefold()
        annotator = str(row.get("annotator_id") or "").strip()
        if not item_id or item_id in labels:
            raise AgreementError(f"{location}: {id_field} is missing or duplicated")
        if label not in allowed_labels:
            raise AgreementError(f"{location}: invalid label {label!r}")
        if source != PILOT_LABEL_SOURCE:
            raise AgreementError(f"{location}: blind pilot label_source must be human")
        if not annotator:
            raise AgreementError(f"{location}: annotator_id is required")
        labels[item_id] = label
        annotators.add(annotator)
    if not labels:
        raise AgreementError(f"{path}: no labels")
    if len(annotators) != 1:
        raise AgreementError(f"{path}: each blind file must contain exactly one annotator")
    item_ids = sorted(labels)
    item_ids_sha256 = hashlib.sha256(
        json.dumps(item_ids, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return LabelSet(
        annotator_id=next(iter(annotators)),
        labels=labels,
        dataset_sha256=hashlib.sha256(raw_bytes).hexdigest(),
        item_ids_sha256=item_ids_sha256,
    )


def cohen_kappa(left: Mapping[str, str], right: Mapping[str, str]) -> float:
    if set(left) != set(right) or not left:
        raise AgreementError("the two annotators must label the same non-empty item set")
    item_ids = sorted(left)
    count = len(item_ids)
    observed = sum(left[item_id] == right[item_id] for item_id in item_ids) / count
    left_counts = Counter(left.values())
    right_counts = Counter(right.values())
    expected = sum(
        (left_counts[label] / count) * (right_counts[label] / count)
        for label in set(left_counts) | set(right_counts)
    )
    if math.isclose(expected, 1.0):
        return 1.0 if math.isclose(observed, 1.0) else 0.0
    return (observed - expected) / (1.0 - expected)


def evaluate_pair(
    left: LabelSet,
    right: LabelSet,
    *,
    expected_count: int,
    threshold: float,
    task: str,
) -> dict[str, object]:
    if left.annotator_id == right.annotator_id:
        raise AgreementError(f"{task}: pilot requires two different annotators")
    if set(left.labels) != set(right.labels):
        missing_left = sorted(set(right.labels) - set(left.labels))
        missing_right = sorted(set(left.labels) - set(right.labels))
        raise AgreementError(
            f"{task}: label sets differ (missing_left={missing_left[:5]}, missing_right={missing_right[:5]})"
        )
    if len(left.labels) != expected_count:
        raise AgreementError(f"{task}: expected {expected_count} items, got {len(left.labels)}")
    kappa = cohen_kappa(left.labels, right.labels)
    item_ids = sorted(left.labels)
    disagreements = sorted(
        item_id for item_id in left.labels if left.labels[item_id] != right.labels[item_id]
    )
    return {
        "task": task,
        "item_count": len(left.labels),
        "item_ids": item_ids,
        "item_ids_sha256": left.item_ids_sha256,
        "annotators": [left.annotator_id, right.annotator_id],
        "reviewer_a_dataset_sha256": left.dataset_sha256,
        "reviewer_b_dataset_sha256": right.dataset_sha256,
        "cohen_kappa": round(kappa, 6),
        "threshold": threshold,
        "passed": kappa >= threshold,
        "disagreement_count": len(disagreements),
        "disagreement_ids": disagreements,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check blind human pilot label agreement.")
    parser.add_argument("--same-story-a", type=Path, required=True)
    parser.add_argument("--same-story-b", type=Path, required=True)
    parser.add_argument("--relevance-a", type=Path, required=True)
    parser.add_argument("--relevance-b", type=Path, required=True)
    parser.add_argument("--expected-pairs", type=int, default=50)
    parser.add_argument("--expected-events", type=int, default=30)
    parser.add_argument("--threshold", type=float, default=0.8)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.expected_pairs < 1 or args.expected_events < 1:
        parser.error("expected counts must be positive")
    if not 0 <= args.threshold <= 1:
        parser.error("threshold must be between 0 and 1")
    try:
        same_story = evaluate_pair(
            load_label_set(
                args.same_story_a,
                id_field="pair_id",
                allowed_labels={"same_story", "related_but_different", "different"},
            ),
            load_label_set(
                args.same_story_b,
                id_field="pair_id",
                allowed_labels={"same_story", "related_but_different", "different"},
            ),
            expected_count=args.expected_pairs,
            threshold=args.threshold,
            task="same_event",
        )
        relevance = evaluate_pair(
            load_label_set(
                args.relevance_a,
                id_field="event_id",
                allowed_labels={"relevant", "not_relevant"},
            ),
            load_label_set(
                args.relevance_b,
                id_field="event_id",
                allowed_labels={"relevant", "not_relevant"},
            ),
            expected_count=args.expected_events,
            threshold=args.threshold,
            task="core_event",
        )
    except (AgreementError, OSError) as exc:
        parser.error(str(exc))
    passed = bool(same_story["passed"] and relevance["passed"])
    report = {
        "schema_version": 1,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "release_eligible": False,
        "pilot_passed": passed,
        "guide_revision_required": not passed,
        "tasks": [same_story, relevance],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
