from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


KST = ZoneInfo("Asia/Seoul")
DAYTIME_CRON_EXPRESSIONS = frozenset(
    {
        "0,15,30,45 22-23 * * *",
        "0,15,30,45 0-14 * * *",
    }
)
OVERNIGHT_CRON_EXPRESSION = "0,30 15-21 * * *"
INCREMENTAL_CRON_EXPRESSIONS = DAYTIME_CRON_EXPRESSIONS | {OVERNIGHT_CRON_EXPRESSION}
COMPANY_MASTER_CRON_EXPRESSION = "40 21 * * 0"
OFFICIAL_RUN_KINDS = frozenset(
    {"scheduled_incremental", "manual", "backfill", "company_master"}
)


def expected_incremental_slots(day: date) -> tuple[datetime, ...]:
    """Return the immutable official-ingest cadence for one completed KST day."""

    slots: list[datetime] = []
    for hour in range(0, 7):
        for minute in (0, 30):
            slots.append(datetime.combine(day, time(hour, minute), tzinfo=KST))
    for hour in range(7, 24):
        for minute in (0, 15, 30, 45):
            slots.append(datetime.combine(day, time(hour, minute), tzinfo=KST))
    return tuple(slot.astimezone(timezone.utc) for slot in slots)


def _slots_for_cron(day: date, event_schedule: str) -> tuple[datetime, ...]:
    minutes: tuple[int, ...]
    if event_schedule == OVERNIGHT_CRON_EXPRESSION:
        hours = range(0, 7)
        minutes = (0, 30)
    elif event_schedule == "0,15,30,45 22-23 * * *":
        hours = range(7, 9)
        minutes = (0, 15, 30, 45)
    elif event_schedule == "0,15,30,45 0-14 * * *":
        hours = range(9, 24)
        minutes = (0, 15, 30, 45)
    else:
        raise ValueError("event_schedule is not an incremental official-ingest cron")
    return tuple(
        datetime.combine(day, time(hour, minute), tzinfo=KST).astimezone(timezone.utc)
        for hour in hours
        for minute in minutes
    )


def slot_matches_incremental_schedule(slot: datetime, event_schedule: str) -> bool:
    """Return whether a UTC/KST slot belongs to the cron that claims it."""

    normalized = slot.replace(tzinfo=timezone.utc) if slot.tzinfo is None else slot
    normalized = normalized.astimezone(timezone.utc)
    kst_day = normalized.astimezone(KST).date()
    try:
        return normalized in _slots_for_cron(kst_day, event_schedule.strip())
    except ValueError:
        return False


def next_incremental_slot(slot: datetime) -> datetime:
    """Return the next slot in the complete immutable 82-slot KST cadence."""

    normalized = slot.replace(tzinfo=timezone.utc) if slot.tzinfo is None else slot
    normalized = normalized.astimezone(timezone.utc).replace(microsecond=0)
    kst_day = normalized.astimezone(KST).date()
    candidates = expected_incremental_slots(kst_day) + expected_incremental_slots(
        kst_day + timedelta(days=1)
    )
    if normalized not in candidates:
        raise ValueError("slot is outside the official incremental cadence")
    return min(candidate for candidate in candidates if candidate > normalized)


def slot_iso(value: datetime) -> str:
    normalized = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    return normalized.astimezone(timezone.utc).replace(microsecond=0).isoformat()
