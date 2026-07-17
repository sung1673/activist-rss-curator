from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .rss_writer import write_feed, write_index


def _report_clusters(report: dict[str, object]) -> list[dict[str, object]]:
    """Return published clusters for the legacy RSS adapter.

    ``rss_clusters`` contains the complete published runtime window and is the
    preferred source.  ``clusters`` keeps this writer compatible with reports
    produced before that field was introduced.
    """

    values = report.get("rss_clusters")
    if not isinstance(values, list):
        values = report.get("clusters")
    if not isinstance(values, list):
        return []
    return [value for value in values if isinstance(value, dict)]


def write_legacy_pages_adapter(
    report: dict[str, object],
    root: Path,
) -> list[Path]:
    """Write the 90-day root-page and RSS compatibility artifacts.

    The daily report process already owns a hydrated MySQL snapshot.  Writing
    these files from that in-memory snapshot avoids restoring generated state
    files to git while preserving the established ``/`` and ``/feed.xml``
    URLs in the GitHub Pages artifact.
    """

    config = report.get("config")
    generated_at = report.get("end_at")
    if not isinstance(config, dict):
        raise ValueError("daily report is missing its configuration")
    if not isinstance(generated_at, datetime):
        raise ValueError("daily report is missing its generation time")

    public_dir = root / "public"
    feed_path = public_dir / "feed.xml"
    index_path = public_dir / "index.html"
    clusters = _report_clusters(report)

    write_feed(feed_path, clusters, config, generated_at)
    write_index(
        index_path,
        {"published_clusters": clusters},
        config,
        generated_at,
    )
    return [index_path, feed_path]
