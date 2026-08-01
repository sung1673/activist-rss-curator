from __future__ import annotations

import hashlib
import json
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from curator.expedited_legacy_compat import (
    EXPEDITED_WINDOW_END,
    EXPEDITED_WINDOW_START,
    MANIFEST_NAME,
    RELEASE_CHANNEL,
    WAIVER_EXCEPTION_ID,
    WAIVER_EXPIRES_AT,
    prepare_expedited_legacy_compatibility,
    verify_expedited_legacy_compatibility,
)
from curator.legacy_feed_compat import (
    REQUIRED_WINDOW_DAYS,
    LegacyArtifactIdentity,
    LegacyFeedCompatibilityError,
)


BEFORE_DEADLINE = datetime(2026, 7, 28, 20, 30, tzinfo=UTC)


def _html(report_date: date, *, marker: str | None = None) -> bytes:
    value = marker if marker is not None else report_date.isoformat()
    return f"<!doctype html><html><body>{value}</body></html>".encode()


def _archive(
    path: Path,
    *,
    start: date = EXPEDITED_WINDOW_START,
    end: date = EXPEDITED_WINDOW_END,
    missing: set[date] | None = None,
    duplicate_content: bool = False,
    embedded_telegram: bool = False,
) -> Path:
    missing = missing or set()
    with zipfile.ZipFile(path, "w") as opened:
        opened.writestr("feed.xml", "<rss><channel/></rss>")
        opened.writestr("CNAME", "news.bside.ai\n")
        current = start
        while current <= end:
            if current not in missing:
                marker = "cloned" if duplicate_content else None
                payload = _html(current, marker=marker)
                if embedded_telegram and current == end:
                    payload = (
                        b"<!doctype html><html><body>"
                        b"<script data-story-telegram-mentions>"
                        b'[{"message_url":"https://t.me/private/42"}]'
                        b"</script></body></html>"
                    )
                opened.writestr(f"feed/{current.isoformat()}.html", payload)
            current += timedelta(days=1)
    return path


def _identity(path: Path) -> LegacyArtifactIdentity:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return LegacyArtifactIdentity(
        run_id="12345",
        artifact_id="67890",
        artifact_name="legacy-pages-archive-seed",
        code_revision="a" * 40,
        artifact_digest=f"sha256:{digest}",
    )


def _waiver(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "exception_id": WAIVER_EXCEPTION_ID,
        "release_channel": RELEASE_CHANNEL,
        "approved": True,
        "reviewer_type": "human",
        "reviewer_id": "production-owner",
        "approved_at": "2026-07-28T20:25:00Z",
        "reason": "Approve the exact immutable 89-day rollback artifact for Early Access.",
        "ai_generated_ground_truth": False,
        "is_synthetic": False,
    }
    value.update(overrides)
    return value


def test_exact_89_day_human_waiver_is_accepted_before_deadline(tmp_path: Path) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    identity = _identity(archive)

    manifest = prepare_expedited_legacy_compatibility(
        archive,
        tmp_path / "site",
        identity=identity,
        observed_at=BEFORE_DEADLINE,
        waiver=_waiver(),
    )

    assert REQUIRED_WINDOW_DAYS == 90
    assert manifest["mode"] == "89_day_human_waiver"
    assert manifest["window_days"] == 89
    assert manifest["window_start"] == "2026-05-01"
    assert manifest["window_end"] == "2026-07-28"
    assert manifest["dated_report_count"] == 89
    assert manifest["complete_legacy_feed_window"] is True
    assert manifest["waiver"]["reviewer_type"] == "human"
    assert manifest["waiver"]["is_synthetic"] is False
    assert (
        verify_expedited_legacy_compatibility(
            tmp_path / "site",
            expected_identity=identity,
            observed_at=BEFORE_DEADLINE,
        )
        == manifest
    )


def test_dirty_89_day_archive_is_redacted_before_compatibility_is_sealed(
    tmp_path: Path,
) -> None:
    archive = _archive(tmp_path / "legacy.zip", embedded_telegram=True)
    identity = _identity(archive)
    site = tmp_path / "site"

    prepare_expedited_legacy_compatibility(
        archive,
        site,
        identity=identity,
        observed_at=BEFORE_DEADLINE,
        waiver=_waiver(),
    )

    report = (site / "feed" / "2026-07-28.html").read_bytes()
    assert b"telegram" not in report.lower()
    assert b"https://t.me/" not in report
    assert verify_expedited_legacy_compatibility(
        site,
        expected_identity=identity,
        observed_at=BEFORE_DEADLINE,
    )


def test_89_day_waiver_is_rejected_at_deadline(tmp_path: Path) -> None:
    archive = _archive(tmp_path / "legacy.zip")

    with pytest.raises(LegacyFeedCompatibilityError, match="expired"):
        prepare_expedited_legacy_compatibility(
            archive,
            tmp_path / "site",
            identity=_identity(archive),
            observed_at=WAIVER_EXPIRES_AT,
            waiver=_waiver(),
        )


def test_existing_89_day_site_cannot_be_verified_after_deadline(tmp_path: Path) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    identity = _identity(archive)
    prepare_expedited_legacy_compatibility(
        archive,
        tmp_path / "site",
        identity=identity,
        observed_at=BEFORE_DEADLINE,
        waiver=_waiver(),
    )

    with pytest.raises(LegacyFeedCompatibilityError, match="expired"):
        verify_expedited_legacy_compatibility(
            tmp_path / "site",
            expected_identity=identity,
            observed_at=WAIVER_EXPIRES_AT,
        )


def test_standard_90_day_artifact_is_preferred_and_needs_no_waiver(tmp_path: Path) -> None:
    archive = _archive(
        tmp_path / "legacy.zip",
        end=EXPEDITED_WINDOW_END + timedelta(days=1),
    )
    identity = _identity(archive)
    observed_at = WAIVER_EXPIRES_AT + timedelta(minutes=1)

    manifest = prepare_expedited_legacy_compatibility(
        archive,
        tmp_path / "site",
        identity=identity,
        observed_at=observed_at,
    )

    assert manifest["mode"] == "standard_90_day"
    assert manifest["window_days"] == 90
    assert manifest["window_start"] == "2026-05-01"
    assert manifest["window_end"] == "2026-07-29"
    assert manifest["waiver"]["status"] == "not_required"
    assert verify_expedited_legacy_compatibility(
        tmp_path / "site",
        expected_identity=identity,
        observed_at=observed_at,
    ) == manifest


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"reviewer_type": "ai"}, "human reviewer"),
        ({"ai_generated_ground_truth": True}, "AI cannot approve"),
        ({"is_synthetic": True}, "synthetic"),
        ({"approved": False}, "explicitly approved"),
        ({"exception_id": "another-exception"}, "exception_id"),
    ],
)
def test_waiver_must_be_exact_human_non_synthetic_approval(
    tmp_path: Path,
    overrides: dict[str, object],
    message: str,
) -> None:
    archive = _archive(tmp_path / "legacy.zip")

    with pytest.raises(LegacyFeedCompatibilityError, match=message):
        prepare_expedited_legacy_compatibility(
            archive,
            tmp_path / "site",
            identity=_identity(archive),
            observed_at=BEFORE_DEADLINE,
            waiver=_waiver(**overrides),
        )


def test_missing_day_and_duplicated_page_content_are_rejected(tmp_path: Path) -> None:
    missing_archive = _archive(
        tmp_path / "missing.zip",
        missing={date(2026, 6, 1)},
    )
    with pytest.raises(LegacyFeedCompatibilityError, match="exactly the real 89-day window"):
        prepare_expedited_legacy_compatibility(
            missing_archive,
            tmp_path / "missing-site",
            identity=_identity(missing_archive),
            observed_at=BEFORE_DEADLINE,
            waiver=_waiver(),
        )

    cloned_archive = _archive(tmp_path / "cloned.zip", duplicate_content=True)
    with pytest.raises(LegacyFeedCompatibilityError, match="duplicated dated report content"):
        prepare_expedited_legacy_compatibility(
            cloned_archive,
            tmp_path / "cloned-site",
            identity=_identity(cloned_archive),
            observed_at=BEFORE_DEADLINE,
            waiver=_waiver(),
        )


def test_manifest_or_report_tampering_is_rejected(tmp_path: Path) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    identity = _identity(archive)
    site = tmp_path / "site"
    prepare_expedited_legacy_compatibility(
        archive,
        site,
        identity=identity,
        observed_at=BEFORE_DEADLINE,
        waiver=_waiver(),
    )
    report = site / "feed" / "2026-07-28.html"
    report.write_bytes(_html(date(2026, 7, 28), marker="tampered"))

    with pytest.raises(LegacyFeedCompatibilityError, match="manifest does not match"):
        verify_expedited_legacy_compatibility(
            site,
            expected_identity=identity,
            observed_at=BEFORE_DEADLINE,
        )

    report.write_bytes(_html(date(2026, 7, 28)))
    manifest_path = site / MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["dated_report_count"] = 88
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(LegacyFeedCompatibilityError, match="manifest does not match"):
        verify_expedited_legacy_compatibility(
            site,
            expected_identity=identity,
            observed_at=BEFORE_DEADLINE,
        )


def test_verify_rejects_reintroduced_telegram_exposure(tmp_path: Path) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    identity = _identity(archive)
    site = tmp_path / "site"
    prepare_expedited_legacy_compatibility(
        archive,
        site,
        identity=identity,
        observed_at=BEFORE_DEADLINE,
        waiver=_waiver(),
    )
    (site / "feed" / "2026-07-28.html").write_bytes(
        b"<!doctype html><html><body>"
        b"<script data-story-telegram-mentions>"
        b'[{"message_url":"https://t.me/private/42"}]'
        b"</script></body></html>"
    )

    with pytest.raises(LegacyFeedCompatibilityError, match="Telegram"):
        verify_expedited_legacy_compatibility(
            site,
            expected_identity=identity,
            observed_at=BEFORE_DEADLINE,
        )
