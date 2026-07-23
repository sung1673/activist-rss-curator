from __future__ import annotations

import argparse
import json
from datetime import datetime


DEFAULT_REMOTE_DELIVERY_LIMIT = 5


def process_remote_delivery_outbox(
    config: dict[str, object],
    now: datetime,
    *,
    limit: int = DEFAULT_REMOTE_DELIVERY_LIMIT,
    delivery_id: str = "",
) -> dict[str, object]:
    """Refuse every remote claim/delivery operation under the web-only policy."""

    return {
        "mode": "disabled",
        "distribution_mode": "web_only",
        "telegram_outbox_claimed": 0,
        "telegram_sent": 0,
        "telegram_failed": 0,
        "telegram_dead_letter": 0,
        "telegram_already_delivered": 0,
        "rights_blocked_count": 0,
        "outcome_unknown_count": 0,
        "requested_status": "disabled",
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Report the permanent web-only delivery policy without accessing an outbound queue."
    )
    parser.add_argument(
        "--root",
        default=".",
        help="Accepted for command-line compatibility; no project files are read.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_REMOTE_DELIVERY_LIMIT,
        help="Accepted for command-line compatibility; no rows are claimed.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    build_arg_parser().parse_args(argv)
    print(
        json.dumps(
            {
                "mode": "disabled",
                "distribution_mode": "web_only",
                "telegram_outbox_skipped": 1,
                "reason": "telegram_outbound_disabled",
                "telegram_sent": 0,
                "telegram_failed": 0,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
