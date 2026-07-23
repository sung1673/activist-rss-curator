from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def send_digest_preview(root: Path | None = None) -> dict[str, int]:
    """Keep the historical command as a permanent web-only policy no-op."""

    return {"digest_preview_sent": 0, "digest_preview_failed": 0}


def main() -> None:
    summary = send_digest_preview()
    print(
        "Digest preview disabled by web-only policy: "
        + ", ".join(f"{key}={value}" for key, value in summary.items())
    )


if __name__ == "__main__":
    main()
