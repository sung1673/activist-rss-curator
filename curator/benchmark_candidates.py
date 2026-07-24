from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOKEN_RE = re.compile(r"[0-9A-Za-z가-힣]{2,}")


class CandidateDataError(ValueError):
    pass


def _records(path: Path, keys: Sequence[str]) -> list[dict[str, object]]:
    if not path.is_file():
        raise CandidateDataError(f"candidate input does not exist: {path}")
    text = path.read_text(encoding="utf-8-sig").strip()
    if not text:
        return []
    if text[0] in "[{":
        parsed = json.loads(text)
        if isinstance(parsed, list):
            rows = parsed
        elif isinstance(parsed, dict):
            rows = []
            for key in keys:
                value = parsed.get(key)
                if isinstance(value, list):
                    rows = value
                    break
            if not rows and isinstance(parsed.get("data"), list):
                rows = parsed["data"]
        else:
            raise CandidateDataError(f"candidate input must contain objects: {path}")
    else:
        rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    if not all(isinstance(row, dict) for row in rows):
        raise CandidateDataError(f"candidate input contains a non-object row: {path}")
    return [dict(row) for row in rows]


def _json_mapping(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip().startswith("{"):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return sorted({str(item).strip() for item in value if str(item).strip()})


def _article(row: Mapping[str, object]) -> dict[str, object] | None:
    payload = _json_mapping(row.get("payload_json"))
    article_id = str(
        row.get("article_id")
        or row.get("document_id")
        or row.get("record_id")
        or row.get("id")
        or row.get("external_id")
        or ""
    ).strip()
    title = str(row.get("title") or row.get("document_title") or "").strip()
    published_at = str(
        row.get("published_at")
        or row.get("received_at")
        or row.get("occurred_at")
        or row.get("seen_at")
        or row.get("sort_at")
        or row.get("created_at")
        or ""
    ).strip()
    if not article_id or not title or not published_at:
        return None
    companies = _string_list(row.get("company_candidates") or payload.get("company_candidates"))
    company_id = str(row.get("company_id") or row.get("corp_code") or "").strip()
    if company_id:
        companies = sorted(set([*companies, company_id]))
    topics = _string_list(row.get("topic_keywords") or payload.get("topic_keywords"))
    return {
        "article_id": article_id,
        "title": title,
        "summary": str(row.get("summary") or row.get("body_excerpt") or ""),
        "published_at": published_at,
        "canonical_url": str(
            row.get("canonical_url") or row.get("original_url") or row.get("url") or ""
        ),
        "source": str(row.get("source") or row.get("source_type") or row.get("feed_name") or ""),
        "story_key": str(row.get("story_key") or payload.get("story_key") or ""),
        "company_candidates": companies,
        "topic_keywords": topics,
    }


def _evidence_ids(row: Mapping[str, object], article: Mapping[str, object]) -> set[str]:
    identifiers = {
        str(article.get("article_id") or "").strip(),
        str(row.get("article_id") or "").strip(),
        str(row.get("document_id") or "").strip(),
        str(row.get("external_id") or "").strip(),
        str(row.get("record_id") or "").strip(),
        str(row.get("id") or "").strip(),
    }
    return {identifier for identifier in identifiers if identifier}


def _event_document_ids(row: Mapping[str, object]) -> list[str]:
    payload = _json_mapping(row.get("payload_json"))
    identifiers = set(_string_list(row.get("document_ids") or payload.get("document_ids")))
    observations = row.get("observations") or payload.get("observations")
    if isinstance(observations, list):
        for observation in observations:
            if isinstance(observation, dict):
                document_id = str(observation.get("document_id") or "").strip()
                if document_id:
                    identifiers.add(document_id)
    return sorted(identifiers)


def _public_article(article: Mapping[str, object]) -> dict[str, object]:
    return {
        key: value
        for key, value in article.items()
        if key != "story_key" and (value != "" or key in {"summary", "source"})
    }


def _tokens(article: Mapping[str, object]) -> set[str]:
    return {token.casefold() for token in TOKEN_RE.findall(str(article.get("title") or ""))}


def _time(article: Mapping[str, object]) -> datetime | None:
    raw = str(article.get("published_at") or "").replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _pair_key(left: Mapping[str, object], right: Mapping[str, object]) -> tuple[str, str]:
    return tuple(sorted((str(left["article_id"]), str(right["article_id"]))))  # type: ignore[return-value]


def _pair_id(left: Mapping[str, object], right: Mapping[str, object]) -> str:
    digest = hashlib.sha256("\x1f".join(_pair_key(left, right)).encode("utf-8")).hexdigest()[:24]
    return f"pair:{digest}"


def _candidate_pair(
    left: Mapping[str, object], right: Mapping[str, object], *, stratum: str
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "task": "same_event_candidate",
        "pair_id": _pair_id(left, right),
        "left": _public_article(left),
        "right": _public_article(right),
        "stratum": stratum,
        "label": None,
        "label_source": None,
    }


def _choose(
    candidates: Iterable[tuple[dict[str, object], dict[str, object]]],
    *,
    count: int,
    stratum: str,
    rng: random.Random,
    used: set[tuple[str, str]],
) -> list[dict[str, object]]:
    materialized = list(candidates)
    rng.shuffle(materialized)
    selected: list[dict[str, object]] = []
    for left, right in materialized:
        key = _pair_key(left, right)
        if key in used:
            continue
        used.add(key)
        selected.append(_candidate_pair(left, right, stratum=stratum))
        if len(selected) == count:
            break
    return selected


def build_same_event_candidates(
    rows: Sequence[Mapping[str, object]],
    *,
    predicted_same: int = 300,
    hard_negative: int = 250,
    easy_negative: int = 100,
    seed: int = 20260722,
    allow_partial: bool = False,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    articles = [article for row in rows if (article := _article(row)) is not None]
    by_story: dict[str, list[dict[str, object]]] = defaultdict(list)
    by_company: dict[str, list[dict[str, object]]] = defaultdict(list)
    for article in articles:
        story_key = str(article.get("story_key") or "")
        if story_key:
            by_story[story_key].append(article)
        for company in _string_list(article.get("company_candidates")):
            by_company[str(company)].append(article)

    positives: list[tuple[dict[str, object], dict[str, object]]] = []
    for group in by_story.values():
        ordered = sorted(group, key=lambda item: str(item["article_id"]))
        for index, left in enumerate(ordered):
            for right in ordered[index + 1 : index + 4]:
                positives.append((left, right))

    hard: list[tuple[dict[str, object], dict[str, object]]] = []
    for group in by_company.values():
        ordered = sorted(group, key=lambda item: str(item["article_id"]))
        for index, left in enumerate(ordered):
            for right in ordered[index + 1 : index + 12]:
                if left.get("story_key") and left.get("story_key") == right.get("story_key"):
                    continue
                left_time, right_time = _time(left), _time(right)
                if left_time and right_time and abs((left_time - right_time).total_seconds()) > 7 * 86400:
                    continue
                if _tokens(left) & _tokens(right):
                    hard.append((left, right))

    easy: list[tuple[dict[str, object], dict[str, object]]] = []
    ordered_articles = sorted(articles, key=lambda item: str(item["article_id"]))
    for index, left in enumerate(ordered_articles):
        for right in ordered_articles[index + 17 :: 97]:
            if set(_string_list(left.get("company_candidates"))) & set(
                _string_list(right.get("company_candidates"))
            ):
                continue
            if _tokens(left) & _tokens(right):
                continue
            easy.append((left, right))
            if len(easy) >= max(easy_negative * 5, easy_negative):
                break
        if len(easy) >= max(easy_negative * 5, easy_negative):
            break

    rng = random.Random(seed)
    used: set[tuple[str, str]] = set()
    selected = [
        *_choose(positives, count=predicted_same, stratum="predicted_same", rng=rng, used=used),
        *_choose(hard, count=hard_negative, stratum="hard_negative", rng=rng, used=used),
        *_choose(easy, count=easy_negative, stratum="easy_negative", rng=rng, used=used),
    ]
    actual = {
        stratum: sum(1 for record in selected if record["stratum"] == stratum)
        for stratum in ("predicted_same", "hard_negative", "easy_negative")
    }
    required = {
        "predicted_same": predicted_same,
        "hard_negative": hard_negative,
        "easy_negative": easy_negative,
    }
    if not allow_partial and actual != required:
        raise CandidateDataError(f"insufficient same-event candidates: required={required}, actual={actual}")
    rng.shuffle(selected)
    return selected, {
        "article_input_count": len(rows),
        "valid_article_count": len(articles),
        "required": required,
        "selected": actual,
    }


def _blank_relevance_candidate(
    *,
    event_id: str,
    article: Mapping[str, object],
    stratum: str,
    linked_document_ids: Sequence[str],
) -> dict[str, object]:
    digest = hashlib.sha256(
        f"{event_id}\x1f{article['article_id']}\x1f{stratum}".encode("utf-8")
    ).hexdigest()[:24]
    return {
        "schema_version": 1,
        "task": "relevance",
        "sample_id": f"relevance:{digest}",
        "event_id": event_id,
        "article": _public_article(article),
        "stratum": stratum,
        "linked_document_ids": sorted(set(linked_document_ids)),
        "label": None,
        "label_source": None,
        "annotator_id": None,
        "labeled_at": None,
    }


def _round_robin(
    groups: Sequence[Sequence[dict[str, object]]], *, count: int
) -> list[dict[str, object]]:
    remaining_groups = [list(group) for group in groups]
    selected: list[dict[str, object]] = []
    while len(selected) < count and any(remaining_groups):
        next_groups: list[list[dict[str, object]]] = []
        for group in remaining_groups:
            if group and len(selected) < count:
                selected.append(group.pop(0))
            if group:
                next_groups.append(group)
        remaining_groups = next_groups
    return selected


def build_relevance_candidates(
    event_rows: Sequence[Mapping[str, object]],
    evidence_rows: Sequence[Mapping[str, object]],
    *,
    hard_negative_rows: Sequence[Mapping[str, object]] | None = None,
    official_events: int = 300,
    hard_negatives: int = 120,
    seed: int = 20260722,
    allow_partial: bool = False,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Build blind rows that become valid relevance benchmark JSONL after labeling.

    Official-event rows are admitted only when an exported document/article ID is
    explicitly present in the event's document list. Hard-negative candidates are
    same-company articles with no event-document link; their final negative status
    is established only by human labels, never by this sampler.
    """

    if official_events < 1 or hard_negatives < 1:
        raise CandidateDataError("relevance candidate counts must be positive")

    evidence: list[tuple[dict[str, object], set[str]]] = []
    evidence_by_id: dict[str, list[tuple[dict[str, object], set[str]]]] = defaultdict(list)
    for row in evidence_rows:
        article = _article(row)
        if article is None:
            continue
        identifiers = _evidence_ids(row, article)
        evidence.append((article, identifiers))
        for identifier in identifiers:
            evidence_by_id[identifier].append((article, identifiers))

    event_strata: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    event_context: dict[str, tuple[str, set[str]]] = {}
    all_linked_ids: set[str] = set()
    seen_event_ids: set[str] = set()
    for row in event_rows:
        event_id = str(row.get("event_id") or "").strip()
        company_id = str(row.get("company_id") or row.get("corp_code") or "").strip()
        event_type = str(row.get("event_type") or "").strip()
        title = str(row.get("title") or "").strip()
        occurred_at = str(row.get("occurred_at") or "").strip()
        document_ids = _event_document_ids(row)
        if not event_id or not company_id or not event_type or not title or not occurred_at or not document_ids:
            continue
        if event_id in seen_event_ids:
            continue
        matches: list[tuple[dict[str, object], set[str]]] = []
        for document_id in document_ids:
            matches.extend(evidence_by_id.get(document_id, []))
        unique_matches = {
            str(article["article_id"]): (article, identifiers) for article, identifiers in matches
        }
        if not unique_matches:
            continue
        article, identifiers = unique_matches[sorted(unique_matches)[0]]
        linked_document_ids = sorted(set(document_ids) & identifiers)
        if not linked_document_ids:
            continue
        year = occurred_at[:4] if occurred_at[:4].isdigit() else "unknown"
        importance = str(row.get("importance") or "medium")
        candidate = _blank_relevance_candidate(
            event_id=event_id,
            article=article,
            stratum="official_event",
            linked_document_ids=linked_document_ids,
        )
        event_strata[(event_type, importance, year)].append(candidate)
        seen_event_ids.add(event_id)
        event_context[event_id] = (company_id, _tokens({"title": title}))
        all_linked_ids.update(document_ids)

    ordered_groups = [
        sorted(group, key=lambda item: (str(item["event_id"]), str(item["sample_id"])))
        for _, group in sorted(event_strata.items())
    ]
    selected_official = _round_robin(ordered_groups, count=official_events)
    if not allow_partial and len(selected_official) < official_events:
        raise CandidateDataError(
            "insufficient official events with linked evidence: "
            f"required={official_events}, actual={len(selected_official)}"
        )

    selected_event_ids = {str(candidate["event_id"]) for candidate in selected_official}
    contexts_by_company: dict[str, list[tuple[str, set[str]]]] = defaultdict(list)
    for event_id in sorted(selected_event_ids):
        company_id, title_tokens = event_context[event_id]
        contexts_by_company[company_id].append((event_id, title_tokens))

    negative_source_rows = hard_negative_rows if hard_negative_rows is not None else evidence_rows
    negative_pool: list[tuple[int, str, dict[str, object], str]] = []
    seen_negative_articles: set[str] = set()
    for row in negative_source_rows:
        article = _article(row)
        if article is None:
            continue
        article_id = str(article["article_id"])
        if article_id in seen_negative_articles or _evidence_ids(row, article) & all_linked_ids:
            continue
        possible_events: list[tuple[int, str]] = []
        article_tokens = _tokens(article)
        for company_id in _string_list(article.get("company_candidates")):
            for event_id, event_tokens in contexts_by_company.get(str(company_id), []):
                possible_events.append((len(article_tokens & event_tokens), event_id))
        if not possible_events:
            continue
        overlap, event_id = max(possible_events, key=lambda item: (item[0], item[1]))
        seen_negative_articles.add(article_id)
        negative_pool.append((overlap, article_id, article, event_id))

    rng = random.Random(seed)
    rng.shuffle(negative_pool)
    negative_pool.sort(key=lambda item: item[0], reverse=True)
    selected_negatives = [
        _blank_relevance_candidate(
            event_id=event_id,
            article=article,
            stratum="non_governance_hard_negative",
            linked_document_ids=[],
        )
        for _overlap, _article_id, article, event_id in negative_pool[:hard_negatives]
    ]
    if not allow_partial and len(selected_negatives) < hard_negatives:
        raise CandidateDataError(
            "insufficient non-governance hard-negative candidates: "
            f"required={hard_negatives}, actual={len(selected_negatives)}"
        )

    selected = [*selected_official, *selected_negatives]
    rng.shuffle(selected)
    return selected, {
        "event_input_count": len(event_rows),
        "evidence_input_count": len(evidence_rows),
        "valid_evidence_count": len(evidence),
        "linked_official_event_count": sum(len(group) for group in event_strata.values()),
        "stratum_count": len(event_strata),
        "required": {
            "official_event": official_events,
            "non_governance_hard_negative": hard_negatives,
        },
        "selected": {
            "official_event": len(selected_official),
            "non_governance_hard_negative": len(selected_negatives),
        },
    }


def build_core_event_candidates(
    rows: Sequence[Mapping[str, object]], *, count: int = 300, allow_partial: bool = False
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Legacy blind-candidate helper retained for callers migrating to relevance rows.

    New release evidence must use :func:`build_relevance_candidates`, which
    requires an explicit document/article link and hard-negative cohort.
    """

    strata: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        event_id = str(row.get("event_id") or "").strip()
        company_id = str(row.get("company_id") or "").strip()
        event_type = str(row.get("event_type") or "").strip()
        title = str(row.get("title") or "").strip()
        occurred_at = str(row.get("occurred_at") or "").strip()
        if not event_id or not company_id or not event_type or not title or not occurred_at:
            continue
        year = occurred_at[:4] if occurred_at[:4].isdigit() else "unknown"
        importance = str(row.get("importance") or "medium")
        strata[(event_type, importance, year)].append(
            {
                "schema_version": 1,
                "task": "core_event_candidate",
                "sample_id": f"event-sample:{event_id}",
                "event_id": event_id,
                "company_id": company_id,
                "event_type": event_type,
                "title": title,
                "occurred_at": occurred_at,
                "importance": importance,
                "document_ids": _event_document_ids(row),
                "label": None,
                "label_source": None,
            }
        )
    selected = _round_robin(
        [sorted(group, key=lambda item: str(item["event_id"])) for _, group in sorted(strata.items())],
        count=count,
    )
    if not allow_partial and len(selected) < count:
        raise CandidateDataError(f"insufficient core events: required={count}, actual={len(selected)}")
    return selected, {
        "event_input_count": len(rows),
        "valid_event_count": sum(len(group) for group in strata.values()),
        "stratum_count": len(strata),
        "required": count,
        "selected": len(selected),
        "deprecated_for_release": True,
    }


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
        newline="\n",
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate blind human-review candidates without release labels.")
    parser.add_argument("--articles", type=Path, required=True)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument(
        "--documents",
        type=Path,
        help="official document export; IDs must match each event's document_ids",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20260722)
    parser.add_argument("--official-events", type=int, default=300)
    parser.add_argument("--relevance-hard-negatives", type=int, default=120)
    parser.add_argument("--allow-partial", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    articles = _records(args.articles, ("articles",))
    events = _records(args.events, ("events", "governance_events"))
    documents = _records(args.documents, ("documents",)) if args.documents else []
    pairs, pair_summary = build_same_event_candidates(
        articles, seed=args.seed, allow_partial=args.allow_partial
    )
    relevance, relevance_summary = build_relevance_candidates(
        events,
        [*articles, *documents],
        hard_negative_rows=articles,
        official_events=args.official_events,
        hard_negatives=args.relevance_hard_negatives,
        seed=args.seed,
        allow_partial=args.allow_partial,
    )
    output_dir = args.output_dir.resolve()
    pair_path = output_dir / "same_event_candidates.jsonl"
    event_path = output_dir / "relevance_candidates.jsonl"
    _write_jsonl(pair_path, pairs)
    _write_jsonl(event_path, relevance)
    manifest = {
        "schema_version": 1,
        "release_eligible": False,
        "reason": "candidates require independent human labels and adjudication",
        "seed": args.seed,
        "same_event": {**pair_summary, "sha256": _sha256(pair_path)},
        "relevance": {**relevance_summary, "sha256": _sha256(event_path)},
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "candidate_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
