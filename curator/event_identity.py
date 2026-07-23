from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Iterable, Mapping


IDENTITY_FIELDS: tuple[str, ...] = (
    "company_id",
    "event_type",
    "action",
    "target",
    "actor_id",
    "effective_at",
    "deadline_at",
)


class EventIdentityStatus(StrEnum):
    COMPLETE = "complete"
    NEEDS_REVIEW = "needs_review"


class EventIdentityMatch(StrEnum):
    SAME = "same"
    DIFFERENT = "different"
    NEEDS_REVIEW = "needs_review"


@dataclass(frozen=True)
class EventIdentity:
    """A normalized, strict identity for a single governance event.

    Missing values are never represented by a shared sentinel in the key.  A
    comparison key exists only after every identity dimension is known and
    valid, preventing two incomplete observations from being merged merely
    because they are missing the same facts.
    """

    company_id: str
    event_type: str
    action: str
    target: str
    actor_id: str
    effective_at: str
    deadline_at: str
    status: EventIdentityStatus
    comparison_key: str | None
    review_reasons: tuple[str, ...] = ()

    def values(self) -> dict[str, str]:
        return {field: str(getattr(self, field)) for field in IDENTITY_FIELDS}

    def to_payload(self) -> dict[str, object]:
        return {
            "identity_action": self.action,
            "identity_target": self.target,
            "identity_actor_id": self.actor_id,
            "identity_effective_at": self.effective_at or None,
            "identity_deadline_at": self.deadline_at or None,
            "identity_status": self.status.value,
            "comparison_key": self.comparison_key,
            "identity_review_reasons": list(self.review_reasons),
        }


@dataclass(frozen=True)
class EventIdentityDecision:
    outcome: EventIdentityMatch
    conflicting_fields: tuple[str, ...] = ()
    review_reasons: tuple[str, ...] = ()

    @property
    def same_event(self) -> bool:
        return self.outcome is EventIdentityMatch.SAME


def _enum_text(value: object) -> str:
    enum_value = getattr(value, "value", None)
    return str(enum_value if enum_value is not None else value or "")


def normalize_identity_text(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", text).strip().casefold()


def normalize_company_id(value: object) -> str:
    text = str(value or "").strip()
    return text if re.fullmatch(r"\d{8}", text) else ""


def normalize_event_type(value: object) -> str:
    text = normalize_identity_text(_enum_text(value)).replace("-", "_")
    return text if re.fullmatch(r"[a-z][a-z0-9_]{0,63}", text) else ""


def normalize_identity_datetime(value: object) -> str:
    """Normalize an explicit calendar date or ISO timestamp without guessing.

    Date-only values remain calendar dates. Offset-aware timestamps are
    normalized to UTC at whole-second precision. Naive timestamps are rejected
    because their timezone cannot be inferred safely for canonical matching.
    """

    text = str(value or "").strip()
    if not text:
        return ""
    compact = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", text)
    if compact:
        text = "-".join(compact.groups())
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            return datetime.fromisoformat(text).date().isoformat()
        except ValueError:
            return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        return ""
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return ""
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def actor_id_from_name(name: object) -> str:
    normalized = normalize_identity_text(name)
    if not normalized:
        return ""
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]
    return f"actor:name:{digest}"


def _comparison_key(values: Iterable[str]) -> str:
    seed = "\x1f".join(("governance-event-identity-v1", *values))
    return f"eventcmp:v1:{hashlib.sha256(seed.encode('utf-8')).hexdigest()}"


def build_event_identity(
    *,
    company_id: object,
    event_type: object,
    action: object,
    target: object,
    actor_id: object,
    effective_at: object,
    deadline_at: object,
) -> EventIdentity:
    raw_values = {
        "company_id": str(company_id or "").strip(),
        "event_type": _enum_text(event_type).strip(),
        "action": str(action or "").strip(),
        "target": str(target or "").strip(),
        "actor_id": str(actor_id or "").strip(),
        "effective_at": str(effective_at or "").strip(),
        "deadline_at": str(deadline_at or "").strip(),
    }
    values = {
        "company_id": normalize_company_id(company_id),
        "event_type": normalize_event_type(event_type),
        "action": normalize_identity_text(action),
        "target": normalize_identity_text(target),
        "actor_id": normalize_identity_text(actor_id),
        "effective_at": normalize_identity_datetime(effective_at),
        "deadline_at": normalize_identity_datetime(deadline_at),
    }
    reasons: list[str] = []
    for field in IDENTITY_FIELDS:
        if not raw_values[field]:
            reasons.append(f"missing_{field}")
        elif not values[field]:
            reasons.append(f"invalid_{field}")
    status = EventIdentityStatus.COMPLETE if not reasons else EventIdentityStatus.NEEDS_REVIEW
    key = _comparison_key(values[field] for field in IDENTITY_FIELDS) if not reasons else None
    return EventIdentity(
        company_id=values["company_id"],
        event_type=values["event_type"],
        action=values["action"],
        target=values["target"],
        actor_id=values["actor_id"],
        effective_at=values["effective_at"],
        deadline_at=values["deadline_at"],
        status=status,
        comparison_key=key,
        review_reasons=tuple(reasons),
    )


def compare_event_identities(left: EventIdentity, right: EventIdentity) -> EventIdentityDecision:
    left_values = left.values()
    right_values = right.values()
    conflicts = tuple(
        field
        for field in IDENTITY_FIELDS
        if left_values[field] and right_values[field] and left_values[field] != right_values[field]
    )
    if conflicts:
        return EventIdentityDecision(EventIdentityMatch.DIFFERENT, conflicting_fields=conflicts)
    if left.status is not EventIdentityStatus.COMPLETE or right.status is not EventIdentityStatus.COMPLETE:
        reasons = tuple(dict.fromkeys((*left.review_reasons, *right.review_reasons)))
        return EventIdentityDecision(EventIdentityMatch.NEEDS_REVIEW, review_reasons=reasons)
    if left.comparison_key and left.comparison_key == right.comparison_key:
        return EventIdentityDecision(EventIdentityMatch.SAME)
    return EventIdentityDecision(EventIdentityMatch.DIFFERENT, conflicting_fields=("comparison_key",))


def _mapping_layers(row: Mapping[str, object]) -> list[Mapping[str, object]]:
    layers = [row]
    for field in ("identity", "metadata", "details", "detail"):
        nested = row.get(field)
        if isinstance(nested, Mapping):
            layers.append(nested)
    return layers


def _first_value(row: Mapping[str, object], fields: Iterable[str]) -> object:
    for layer in _mapping_layers(row):
        for field in fields:
            value = layer.get(field)
            if value is not None and str(value).strip():
                return value
    return ""


def event_identity_from_mapping(
    row: Mapping[str, object],
    *,
    company_id: object,
    event_type: object,
    default_action: object = "",
    default_actor_name: object = "",
) -> EventIdentity:
    """Build identity only from explicit source facts and conservative defaults.

    Receipt timestamps and report titles are deliberately not treated as the
    event's effective date, deadline, or target. Doing so would manufacture
    identity facts and could merge a correction into the wrong event.
    """

    actor_id = _first_value(row, ("identity_actor_id", "actor_id", "proponent_actor_id"))
    if not actor_id:
        actor_id = actor_id_from_name(default_actor_name)
    return build_event_identity(
        company_id=company_id,
        event_type=event_type,
        action=_first_value(row, ("identity_action", "event_action", "action")) or default_action,
        target=_first_value(row, ("identity_target", "event_target", "target")),
        actor_id=actor_id,
        effective_at=_first_value(
            row,
            ("identity_effective_at", "effective_at", "effective_date", "event_date", "meeting_date"),
        ),
        deadline_at=_first_value(
            row,
            ("identity_deadline_at", "deadline_at", "deadline", "due_at", "due_date"),
        ),
    )
