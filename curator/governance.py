from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, timezone
from enum import StrEnum
from typing import Any, Iterable, cast

from .event_identity import (
    EventIdentity,
    EventIdentityStatus,
    build_event_identity,
    compare_event_identities,
)


class SourceCategory(StrEnum):
    OFFICIAL_DISCLOSURE = "official_disclosure"
    OFFICIAL_REGISTER = "official_register"
    OFFICIAL_ISSUER = "official_issuer"
    COMPANY_STATEMENT = "company_statement"
    ACTIVIST_STATEMENT = "activist_statement"
    MEDIA_REPORT = "media_report"
    AUTHORIZED_TELEGRAM = "authorized_telegram"
    EDITORIAL_ANALYSIS = "editorial_analysis"


class EventStatus(StrEnum):
    SIGNAL = "signal"
    NEEDS_REVIEW = "needs_review"
    CONFIRMED = "confirmed"
    PUBLISHED = "published"
    CORRECTED = "corrected"
    CLOSED = "closed"


class Importance(StrEnum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class GovernanceEventType(StrEnum):
    FIVE_PERCENT_HOLDING = "five_percent_holding"
    SHAREHOLDER_PROPOSAL = "shareholder_proposal"
    GENERAL_MEETING = "general_meeting"
    BOARD = "board"
    EXECUTIVE_COMPENSATION = "executive_compensation"
    DIVIDEND = "dividend"
    TREASURY_SHARES = "treasury_shares"
    MERGER = "merger"
    SPLIT = "split"
    DUPLICATE_LISTING = "duplicate_listing"
    RIGHTS_ISSUE = "rights_issue"
    CONVERTIBLE_BOND = "convertible_bond"
    BOND_WITH_WARRANT = "bond_with_warrant"
    EXCHANGEABLE_BOND = "exchangeable_bond"
    TENDER_OFFER = "tender_offer"
    DELISTING = "delisting"
    TRADING_SUSPENSION = "trading_suspension"
    VALUE_UP = "value_up"
    OTHER = "other"


class CampaignStage(StrEnum):
    INITIAL_SIGNAL = "initial_signal"
    PRIVATE_ENGAGEMENT = "private_engagement"
    PUBLIC_LETTER = "public_letter"
    PUBLIC_CAMPAIGN = "public_campaign"
    SHAREHOLDER_PROPOSAL = "shareholder_proposal"
    PROXY_VOTE = "proxy_vote"
    RESOLUTION = "resolution"
    IMPLEMENTATION_TRACKING = "implementation_tracking"
    CLOSED = "closed"


CAMPAIGN_STAGE_LABELS: dict[CampaignStage, dict[str, str]] = {
    CampaignStage.INITIAL_SIGNAL: {"ko": "초기 신호", "en": "Initial signal"},
    CampaignStage.PRIVATE_ENGAGEMENT: {"ko": "비공개 관여", "en": "Private engagement"},
    CampaignStage.PUBLIC_LETTER: {"ko": "공개서한·질의", "en": "Public letter / inquiry"},
    CampaignStage.PUBLIC_CAMPAIGN: {"ko": "공개 캠페인", "en": "Public campaign"},
    CampaignStage.SHAREHOLDER_PROPOSAL: {"ko": "주주제안", "en": "Shareholder proposal"},
    CampaignStage.PROXY_VOTE: {"ko": "위임·표결", "en": "Proxy / vote"},
    CampaignStage.RESOLUTION: {"ko": "합의/철회/가결/부결", "en": "Resolution"},
    CampaignStage.IMPLEMENTATION_TRACKING: {"ko": "이행 추적", "en": "Implementation tracking"},
    CampaignStage.CLOSED: {"ko": "종료", "en": "Closed"},
}


class ClaimKind(StrEnum):
    ACTOR_CLAIM = "actor_claim"
    COMPANY_RESPONSE = "company_response"
    OFFICIAL_FACT = "official_fact"
    MEDIA_REPORT = "media_report"
    EDITORIAL_INTERPRETATION = "editorial_interpretation"


def stable_id(prefix: str, *parts: object, length: int = 24) -> str:
    seed = "\x1f".join(str(part or "").strip() for part in parts)
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:length]
    return f"{prefix}:{digest}"


def _utc_datetime(value: str | date | datetime | None) -> datetime | None:
    """Parse a SourceRight boundary using the API's UTC ``DATETIME`` contract.

    PHP's ``v1_mysql_datetime_utc`` treats date-only and offset-free values as
    UTC before storing them in MySQL.  Apply the same explicit rule here; an
    offset-aware value is normalized to UTC.  MySQL ``DATETIME`` and the PHP
    formatter use whole-second precision, so fractional seconds are discarded.
    Invalid values return ``None`` so callers can deny the right rather than
    silently broadening its validity.
    """
    if value is None or value == "":
        return None
    try:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, date):
            parsed = datetime(value.year, value.month, value.day)
        else:
            text = str(value).strip()
            if not text:
                return None
            if text.endswith(("Z", "z")):
                text = f"{text[:-1]}+00:00"
            parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).replace(microsecond=0)
    except (TypeError, ValueError, OverflowError):
        return None


def _enum_values(value: Any) -> Any:
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, dict):
        return {key: _enum_values(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_enum_values(item) for item in value]
    if is_dataclass(value):
        return _enum_values(asdict(cast(Any, value)))
    return value


class GovernanceRecord:
    def to_dict(self) -> dict[str, Any]:
        return _enum_values(self)


@dataclass(frozen=True)
class Company(GovernanceRecord):
    company_id: str
    legal_name: str
    stock_code: str = ""
    market: str = ""
    aliases: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not re.fullmatch(r"\d{8}", self.company_id):
            raise ValueError("company_id must be an 8-digit DART corp_code")
        if not self.legal_name.strip():
            raise ValueError("legal_name is required")


@dataclass(frozen=True)
class Actor(GovernanceRecord):
    actor_id: str
    name: str
    actor_type: str
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class SourceRight(GovernanceRecord):
    """Evidence-backed permission with a half-open UTC validity interval.

    The active interval is ``valid_from <= at < expires_at``.  A date-only
    boundary denotes midnight UTC, so a grant intended to include an entire
    UTC calendar date must use the following date as ``expires_at`` (the Python
    alias of the API's ``valid_until`` field).
    """

    source_right_id: str
    source_category: SourceCategory
    source_identity: str
    scope: str
    evidence_ref: str
    valid_from: str
    expires_at: str | None = None
    revoked_at: str | None = None
    allow_ai: bool = False
    allow_redistribution: bool = False
    status: str = "active"

    def is_active(self, at: date | datetime | None = None, *, purpose: str = "collect") -> bool:
        current = _utc_datetime(at or datetime.now(timezone.utc))
        valid_from = _utc_datetime(self.valid_from)
        expires_at = _utc_datetime(self.expires_at)
        revoked_at = _utc_datetime(self.revoked_at)
        if self.status.strip().casefold() != "active":
            return False
        if current is None or valid_from is None:
            return False
        if self.expires_at is not None and expires_at is None:
            return False
        if self.revoked_at is not None and revoked_at is None:
            return False
        if not self.scope.strip() or not self.evidence_ref.strip() or current < valid_from:
            return False
        if expires_at is not None and current >= expires_at:
            return False
        if revoked_at is not None and current >= revoked_at:
            return False
        if purpose == "ai" and not self.allow_ai:
            return False
        if purpose in {"public", "redistribute"} and not self.allow_redistribution:
            return False
        return True


@dataclass(frozen=True)
class Document(GovernanceRecord):
    document_id: str
    stable_source_id: str
    source_category: SourceCategory
    company_id: str
    title: str
    original_language: str
    received_at: str
    original_url: str
    content_hash: str = ""
    correction_of: str | None = None
    correction_sequence: int = 0
    is_cancelled: bool = False
    source_right_id: str | None = None

    def __post_init__(self) -> None:
        if not self.title:
            raise ValueError("document title must be preserved and non-empty")
        if not re.fullmatch(r"[a-z]{2,3}(?:-[A-Z]{2})?", self.original_language):
            raise ValueError("original_language must be an IETF-like language tag")


@dataclass(frozen=True)
class GovernanceEvent(GovernanceRecord):
    event_id: str
    company_id: str
    event_type: GovernanceEventType
    occurred_at: str
    status: EventStatus
    importance: Importance = Importance.MEDIUM
    deadline_at: str | None = None
    actor_id: str | None = None
    action: str = ""
    target: str = ""
    document_ids: tuple[str, ...] = ()
    review_required: bool = False
    identity_action: str = ""
    identity_target: str = ""
    identity_actor_id: str | None = None
    identity_effective_at: str | None = None
    identity_deadline_at: str | None = None
    identity_status: EventIdentityStatus = EventIdentityStatus.NEEDS_REVIEW
    comparison_key: str | None = None
    identity_review_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        identity = build_event_identity(
            company_id=self.company_id,
            event_type=self.event_type,
            action=self.identity_action or self.action,
            target=self.identity_target or self.target,
            actor_id=self.identity_actor_id or self.actor_id,
            effective_at=self.identity_effective_at or self.occurred_at,
            deadline_at=self.identity_deadline_at or self.deadline_at,
        )
        object.__setattr__(self, "identity_action", identity.action)
        object.__setattr__(self, "identity_target", identity.target)
        object.__setattr__(self, "identity_actor_id", identity.actor_id or None)
        object.__setattr__(self, "identity_effective_at", identity.effective_at or None)
        object.__setattr__(self, "identity_deadline_at", identity.deadline_at or None)
        object.__setattr__(self, "identity_status", identity.status)
        object.__setattr__(self, "comparison_key", identity.comparison_key)
        object.__setattr__(self, "identity_review_reasons", identity.review_reasons)

    def identity(self) -> EventIdentity:
        return build_event_identity(
            company_id=self.company_id,
            event_type=self.event_type,
            action=self.identity_action,
            target=self.identity_target,
            actor_id=self.identity_actor_id,
            effective_at=self.identity_effective_at,
            deadline_at=self.identity_deadline_at,
        )


@dataclass(frozen=True)
class Campaign(GovernanceRecord):
    campaign_id: str
    company_id: str
    proponent_actor_id: str
    demands: tuple[str, ...]
    stage: CampaignStage
    status: str = "active"
    outcome: str = ""


@dataclass(frozen=True)
class ClaimEvidence(GovernanceRecord):
    claim_evidence_id: str
    event_id: str
    kind: ClaimKind
    text: str
    document_id: str
    actor_id: str | None = None
    editorial: bool = False


@dataclass(frozen=True)
class ProposalVote(GovernanceRecord):
    proposal_vote_id: str
    event_id: str
    meeting_date: str
    agenda: str
    proposer_actor_id: str | None = None
    recommendation: str = ""
    result: str = "pending"
    votes_for: float | None = None
    votes_against: float | None = None


@dataclass(frozen=True)
class CommitmentOutcome(GovernanceRecord):
    commitment_outcome_id: str
    company_id: str
    commitment: str
    target_value: str
    due_at: str | None
    actual_action: str = ""
    status: str = "planned"
    document_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class TimelineEntry(GovernanceRecord):
    timeline_entry_id: str
    event_id: str
    occurred_at: str
    entry_type: str
    title: str
    document_id: str | None = None


@dataclass(frozen=True)
class EditorialRevision(GovernanceRecord):
    editorial_revision_id: str
    entity_type: str
    entity_id: str
    changed_at: str
    changed_by: str
    reason: str
    before_hash: str = ""
    after_hash: str = ""


@dataclass(frozen=True)
class DeliveryOutbox(GovernanceRecord):
    outbox_id: str
    destination: str
    payload: dict[str, Any]
    status: str = "pending"
    attempts: int = 0
    next_attempt_at: str | None = None
    external_message_id: str | None = None
    last_error: str = ""


def event_fingerprint(
    company_id: str,
    event_type: GovernanceEventType | str,
    *,
    actor_id: str | None,
    action: str,
    target: str,
    deadline_at: str | None,
) -> str:
    """Return a strict event identity; a shared theme is intentionally insufficient."""
    return stable_id(
        "event",
        company_id,
        str(event_type),
        actor_id or "",
        action.casefold(),
        target.casefold(),
        (deadline_at or "")[:10],
    )


def same_specific_event(left: GovernanceEvent, right: GovernanceEvent) -> bool:
    return compare_event_identities(left.identity(), right.identity()).same_event


def requires_editorial_review(event: GovernanceEvent) -> bool:
    market_sensitive = {
        GovernanceEventType.TENDER_OFFER,
        GovernanceEventType.MERGER,
        GovernanceEventType.SPLIT,
        GovernanceEventType.DELISTING,
        GovernanceEventType.TRADING_SUSPENSION,
        GovernanceEventType.RIGHTS_ISSUE,
        GovernanceEventType.CONVERTIBLE_BOND,
        GovernanceEventType.BOND_WITH_WARRANT,
        GovernanceEventType.EXCHANGEABLE_BOND,
    }
    return event.review_required or event.importance in {Importance.CRITICAL, Importance.HIGH} or event.event_type in market_sensitive


def publication_status(
    source_categories: Iterable[SourceCategory | str],
    *,
    event: GovernanceEvent | None = None,
    editor_approved: bool = False,
) -> EventStatus:
    categories = {SourceCategory(str(category)) for category in source_categories}
    if categories and categories <= {SourceCategory.AUTHORIZED_TELEGRAM}:
        return EventStatus.SIGNAL
    has_primary_or_independent_evidence = bool(
        categories
        & {
            SourceCategory.OFFICIAL_DISCLOSURE,
            SourceCategory.COMPANY_STATEMENT,
            SourceCategory.ACTIVIST_STATEMENT,
            SourceCategory.MEDIA_REPORT,
        }
    )
    if not has_primary_or_independent_evidence:
        return EventStatus.NEEDS_REVIEW
    if event is not None and requires_editorial_review(event) and not editor_approved:
        return EventStatus.NEEDS_REVIEW
    return EventStatus.CONFIRMED


def campaign_stage_options() -> list[dict[str, str]]:
    return [
        {"value": stage.value, "label_ko": CAMPAIGN_STAGE_LABELS[stage]["ko"], "label_en": CAMPAIGN_STAGE_LABELS[stage]["en"]}
        for stage in CampaignStage
    ]
