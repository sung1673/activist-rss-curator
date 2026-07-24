from datetime import date, datetime, timezone

import pytest

from curator.official_schedule import (
    expected_incremental_slots,
    next_incremental_slot,
    slot_matches_incremental_schedule,
    slot_iso,
)


def test_expected_daily_ledger_has_82_unique_kst_slots() -> None:
    slots = expected_incremental_slots(date(2026, 7, 16))

    assert len(slots) == 82
    assert len(set(slots)) == 82
    assert slot_iso(slots[0]) == "2026-07-15T15:00:00+00:00"
    assert slot_iso(slots[13]) == "2026-07-15T21:30:00+00:00"
    assert slot_iso(slots[14]) == "2026-07-15T22:00:00+00:00"
    assert slot_iso(slots[-1]) == "2026-07-16T14:45:00+00:00"


@pytest.mark.parametrize(
    ("slot", "expected"),
    (
        (
            datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc),
            "2026-07-15T15:30:00+00:00",
        ),
        (
            datetime(2026, 7, 15, 21, 30, tzinfo=timezone.utc),
            "2026-07-15T22:00:00+00:00",
        ),
        (
            datetime(2026, 7, 15, 22, 0, tzinfo=timezone.utc),
            "2026-07-15T22:15:00+00:00",
        ),
        (
            datetime(2026, 7, 16, 14, 45, tzinfo=timezone.utc),
            "2026-07-16T15:00:00+00:00",
        ),
    ),
)
def test_next_boundary_uses_the_complete_82_slot_cadence(
    slot: datetime, expected: str
) -> None:
    assert slot_iso(next_incremental_slot(slot)) == expected


def test_unknown_cron_is_rejected() -> None:
    with pytest.raises(ValueError, match="outside"):
        next_incremental_slot(datetime(2026, 7, 16, 14, 44, tzinfo=timezone.utc))


def test_slot_must_belong_to_the_cron_family_that_claims_it() -> None:
    overnight = datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc)
    daytime = datetime(2026, 7, 16, 3, 15, tzinfo=timezone.utc)

    assert slot_matches_incremental_schedule(overnight, "0,30 15-21 * * *")
    assert not slot_matches_incremental_schedule(overnight, "0,15,30,45 0-14 * * *")
    assert slot_matches_incremental_schedule(daytime, "0,15,30,45 0-14 * * *")
