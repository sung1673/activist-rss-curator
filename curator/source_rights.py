from __future__ import annotations

import re
from datetime import date, datetime
from typing import Iterable

from .governance import SourceCategory, SourceRight


TELEGRAM_SOURCE_KINDS = {"telegram", "telegram_reference", "telegram_signal", "authorized_telegram"}


def _bool_setting(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "on"}
    return bool(value)


def normalize_source_identity(value: object) -> str:
    text = str(value or "").strip().casefold()
    text = re.sub(r"^https?://(?:www\.)?t\.me/", "", text)
    text = text.lstrip("@").split("/", 1)[0]
    return text


def record_source_identity(record: dict[str, object]) -> str:
    explicit = normalize_source_identity(record.get("source_identity") or record.get("source_right_id"))
    if explicit:
        return explicit.removeprefix("telegram:")
    return normalize_source_identity(
        record.get("handle")
        or record.get("channel_handle")
        or record.get("telegram_source_handle")
        or record.get("username")
        or record.get("telegram_handle")
        or record.get("source")
    )


def record_source_kind(record: dict[str, object]) -> str:
    return str(
        record.get("source_kind")
        or record.get("source_category")
        or record.get("feed_category")
        or ("telegram" if record.get("handle") or record.get("channel_handle") else "")
    ).strip().casefold()


def requires_registered_right(record: dict[str, object]) -> bool:
    # Once lineage has been attached, losing or changing the display-oriented
    # source kind must not turn a licensed record into an unlicensed one.
    # This also makes revocation effective for historical derived records.
    if str(record.get("source_right_id") or "").strip():
        return True
    kind = record_source_kind(record)
    return kind in TELEGRAM_SOURCE_KINDS or "telegram" in kind


def configured_source_rights(config: dict[str, object]) -> list[SourceRight]:
    settings = config.get("source_rights", {})
    if not isinstance(settings, dict):
        return []
    raw_records = settings.get("records", [])
    rights: list[SourceRight] = []
    if not isinstance(raw_records, list):
        return rights
    for raw in raw_records:
        if not isinstance(raw, dict):
            continue
        try:
            rights.append(
                SourceRight(
                    source_right_id=str(raw.get("source_right_id") or raw.get("id") or ""),
                    source_category=SourceCategory(str(raw.get("source_category") or SourceCategory.AUTHORIZED_TELEGRAM.value)),
                    source_identity=normalize_source_identity(raw.get("source_identity") or raw.get("handle")),
                    scope=str(raw.get("scope") or ""),
                    evidence_ref=str(raw.get("evidence_ref") or ""),
                    valid_from=str(raw.get("valid_from") or ""),
                    expires_at=str(raw.get("expires_at")) if raw.get("expires_at") else None,
                    revoked_at=str(raw.get("revoked_at")) if raw.get("revoked_at") else None,
                    allow_ai=_bool_setting(raw.get("allow_ai", False)),
                    allow_redistribution=_bool_setting(raw.get("allow_redistribution", False)),
                    status=str(raw.get("status") or "active"),
                )
            )
        except (TypeError, ValueError):
            # Invalid entries remain unavailable; ingestion must fail closed.
            continue
    return rights


def source_right_index(config: dict[str, object]) -> dict[str, SourceRight]:
    index: dict[str, SourceRight] = {}
    for right in configured_source_rights(config):
        if right.source_right_id:
            index[right.source_right_id.casefold()] = right
        if right.source_identity:
            index[normalize_source_identity(right.source_identity)] = right
            index[f"telegram:{normalize_source_identity(right.source_identity)}"] = right
    return index


def find_source_right(record: dict[str, object], config: dict[str, object]) -> SourceRight | None:
    index = source_right_index(config)
    explicit_id = str(record.get("source_right_id") or "").strip().casefold()
    if explicit_id:
        # Explicit lineage is immutable.  If its referenced right was removed,
        # expired, or replaced, never fall back to a newer right for the same
        # display handle; an administrator must migrate the lineage explicitly.
        return index.get(explicit_id)
    identity = record_source_identity(record)
    return index.get(identity) or index.get(f"telegram:{identity}")


def source_is_authorized(
    record: dict[str, object],
    config: dict[str, object],
    at: date | datetime | None = None,
    *,
    purpose: str = "collect",
) -> bool:
    settings = config.get("source_rights", {})
    enforce = not isinstance(settings, dict) or bool(settings.get("enforce", True))
    if not enforce or not requires_registered_right(record):
        return True
    right = find_source_right(record, config)
    return bool(right and right.is_active(at, purpose=purpose))


def source_right_reason(
    record: dict[str, object],
    config: dict[str, object],
    at: date | datetime | None = None,
    *,
    purpose: str = "collect",
) -> str:
    if not requires_registered_right(record):
        return "not_required"
    right = find_source_right(record, config)
    if right is None:
        return "source_right_missing"
    if not right.is_active(at, purpose=purpose):
        return "source_right_inactive_or_scope_denied"
    return "authorized"


def partition_authorized_records(
    records: Iterable[dict[str, object]],
    config: dict[str, object],
    at: date | datetime | None = None,
    *,
    purpose: str = "collect",
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    allowed: list[dict[str, object]] = []
    blocked: list[dict[str, object]] = []
    for record in records:
        if source_is_authorized(record, config, at, purpose=purpose):
            allowed.append(record)
        else:
            denied = dict(record)
            denied["source_right_status"] = source_right_reason(record, config, at, purpose=purpose)
            blocked.append(denied)
    return allowed, blocked


def apply_channel_source_rights(
    state: dict[str, object],
    config: dict[str, object],
    at: date | datetime | None = None,
) -> int:
    blocked = 0
    channels = state.get("telegram_source_channels", [])
    if not isinstance(channels, list):
        return blocked
    for channel in channels:
        if not isinstance(channel, dict):
            continue
        probe = {
            "source_kind": "telegram",
            "handle": channel.get("handle") or channel.get("username"),
            "source_right_id": channel.get("source_right_id"),
        }
        authorized = source_is_authorized(probe, config, at, purpose="collect")
        channel["source_right_status"] = source_right_reason(probe, config, at, purpose="collect")
        channel["source_right_blocked"] = not authorized
        right = find_source_right(probe, config)
        if right is not None:
            channel["source_right_id"] = right.source_right_id
        if not authorized:
            blocked += 1
    return blocked


def active_source_right_records(
    config: dict[str, object],
    at: date | datetime | None = None,
) -> list[dict[str, object]]:
    return [right.to_dict() for right in configured_source_rights(config) if right.is_active(at)]
