"""Resolve governance rollout variables with fail-closed legacy adapters.

The repository is migrating from three independent booleans to one Pages
owner and one pipeline mode.  This module keeps the transition deterministic:
an old variable may assert an owner/mode, but it may never contradict the new
source of truth.  Telegram delivery is intentionally not representable as an
enabled state.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping, Sequence


PAGES_OWNERS = {"legacy", "governance"}
PIPELINE_MODES = {"off", "dart_canary", "shadow", "live"}
KIND_CONNECTOR_MODES = {"off", "active"}


class OperationModeError(ValueError):
    """Raised when rollout variables are invalid or contradictory."""


def _text(values: Mapping[str, str], key: str) -> str:
    return str(values.get(key, "") or "").strip().casefold()


def _legacy_bool(values: Mapping[str, str], key: str) -> bool:
    value = _text(values, key)
    if value in {"", "false", "0", "no"}:
        return False
    if value in {"true", "1", "yes"}:
        return True
    raise OperationModeError(f"{key} must be true or false when set")


def _strict_bool(values: Mapping[str, str], key: str, *, default: bool = False) -> bool:
    value = _text(values, key)
    if not value:
        return default
    if value == "false":
        return False
    if value == "true":
        return True
    raise OperationModeError(f"{key} must be true or false when set")


@dataclass(frozen=True)
class OperationMode:
    pages_owner: str
    governance_pipeline_mode: str
    legacy_pages_enabled: bool
    governance_pages_enabled: bool
    scheduled_governance_enabled: bool
    dart_canary_allowed: bool
    kind_connector_mode: str
    kind_connector_enabled: bool
    global_alpha_observation_enabled: bool
    telegram_delivery_enabled: bool = False
    distribution_mode: str = "web_only"


def resolve_operation_mode(values: Mapping[str, str]) -> OperationMode:
    old_legacy_pages = _legacy_bool(values, "ENABLE_PAGES")
    old_governance_pages = _legacy_bool(values, "ENABLE_GOVERNANCE_PAGES")
    if old_legacy_pages and old_governance_pages:
        raise OperationModeError("legacy and governance Pages cannot both be enabled")

    configured_owner = _text(values, "PAGES_OWNER")
    if configured_owner and configured_owner not in PAGES_OWNERS:
        raise OperationModeError("PAGES_OWNER must be legacy or governance")
    if configured_owner == "legacy" and old_governance_pages:
        raise OperationModeError("PAGES_OWNER=legacy conflicts with ENABLE_GOVERNANCE_PAGES=true")
    if configured_owner == "governance" and old_legacy_pages:
        raise OperationModeError("PAGES_OWNER=governance conflicts with ENABLE_PAGES=true")
    owner = configured_owner
    if not owner:
        owner = "legacy" if old_legacy_pages else ("governance" if old_governance_pages else "none")

    old_shadow = _legacy_bool(values, "ENABLE_GOVERNANCE_SHADOW")
    configured_pipeline = _text(values, "GOVERNANCE_PIPELINE_MODE")
    if configured_pipeline and configured_pipeline not in PIPELINE_MODES:
        raise OperationModeError(
            "GOVERNANCE_PIPELINE_MODE must be off, dart_canary, shadow, or live"
        )
    if old_shadow and configured_pipeline in {"off", "dart_canary"}:
        raise OperationModeError(
            "ENABLE_GOVERNANCE_SHADOW=true conflicts with a non-scheduled pipeline mode"
        )
    pipeline = configured_pipeline or ("shadow" if old_shadow else "off")

    if _legacy_bool(values, "ENABLE_TELEGRAM_DELIVERY"):
        raise OperationModeError("Telegram outbound delivery is permanently disabled")
    if _legacy_bool(values, "ENABLE_GOVERNANCE_DELIVERY"):
        raise OperationModeError("Governance outbound delivery is permanently disabled")

    kind_connector_mode = _text(values, "KIND_CONNECTOR_MODE") or "off"
    if kind_connector_mode not in KIND_CONNECTOR_MODES:
        raise OperationModeError("KIND_CONNECTOR_MODE must be off or active")
    global_alpha_observation_enabled = _strict_bool(
        values,
        "GLOBAL_ALPHA_OBSERVATION_ENABLED",
    )

    return OperationMode(
        pages_owner=owner,
        governance_pipeline_mode=pipeline,
        legacy_pages_enabled=owner == "legacy",
        governance_pages_enabled=owner == "governance",
        scheduled_governance_enabled=pipeline in {"shadow", "live"},
        dart_canary_allowed=pipeline in {"dart_canary", "shadow", "live"},
        kind_connector_mode=kind_connector_mode,
        kind_connector_enabled=kind_connector_mode == "active",
        global_alpha_observation_enabled=global_alpha_observation_enabled,
    )


def _github_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def write_github_outputs(path: Path, mode: OperationMode) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        for key, value in asdict(mode).items():
            handle.write(f"{key}={_github_value(value)}\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Resolve BSIDE governance rollout variables.")
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)
    try:
        mode = resolve_operation_mode(os.environ)
    except OperationModeError as exc:
        parser.error(str(exc))
    if args.github_output:
        write_github_outputs(args.github_output, mode)
    print(json.dumps(asdict(mode), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
