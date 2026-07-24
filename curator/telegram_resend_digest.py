from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def resend_last_digest(root: Path | None = None) -> dict[str, int]:
    """Keep the historical resend command as a permanent web-only policy no-op."""

    return {
        "telegram_digest_resend_sent": 0,
        "telegram_digest_resend_failed": 0,
    }


def main() -> None:
    summary = resend_last_digest()
    print(
        "Telegram digest resend disabled by web-only policy: "
        + ", ".join(f"{key}={value}" for key, value in summary.items())
    )


if __name__ == "__main__":
    main()
