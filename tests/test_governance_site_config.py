from __future__ import annotations

import json
from pathlib import Path

import pytest

from curator.governance_site_config import (
    GovernanceSiteConfigError,
    verify_governance_site_config,
)


REVISION = "a" * 40
API_BASE = "https://alignpe.gabia.io/activist/api.php/api/v1"
WEB_BASE = "https://news.bside.ai"


def write_site(site: Path, payload: dict[str, object]) -> None:
    encoded = json.dumps(payload, separators=(",", ":"))
    content = f"window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze({encoded});\n"
    (site / "governance").mkdir(parents=True)
    (site / "config.js").write_text(content, encoding="utf-8", newline="\n")
    (site / "governance" / "config.js").write_text(content, encoding="utf-8", newline="\n")


def expected_payload() -> dict[str, object]:
    return {
        "apiBase": API_BASE,
        "webBase": WEB_BASE,
        "buildSha": REVISION,
        "releaseChannel": "production_alpha_early_access",
    }


def test_exact_root_and_nested_release_config_is_accepted(tmp_path: Path) -> None:
    write_site(tmp_path, expected_payload())
    result = verify_governance_site_config(
        tmp_path,
        expected_api_base="https://alignpe.gabia.io/activist/api.php",
        expected_web_base=WEB_BASE + "/",
        expected_build_sha=REVISION,
    )
    assert result == expected_payload()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("apiBase", "/api/v1", "embedded apiBase"),
        ("webBase", "https://stale.example", "embedded webBase"),
        ("buildSha", "b" * 40, "embedded buildSha"),
        ("releaseChannel", "production_alpha", "embedded releaseChannel"),
    ),
)
def test_stale_embedded_release_identity_is_rejected(
    tmp_path: Path, field: str, value: str, message: str
) -> None:
    payload = expected_payload()
    payload[field] = value
    write_site(tmp_path, payload)
    with pytest.raises(GovernanceSiteConfigError, match=message):
        verify_governance_site_config(
            tmp_path,
            expected_api_base=API_BASE,
            expected_web_base=WEB_BASE,
            expected_build_sha=REVISION,
        )


def test_root_and_nested_config_mismatch_is_rejected(tmp_path: Path) -> None:
    write_site(tmp_path, expected_payload())
    nested_payload = expected_payload()
    nested_payload["webBase"] = "https://stale.example"
    encoded = json.dumps(nested_payload, separators=(",", ":"))
    (tmp_path / "governance" / "config.js").write_text(
        f"window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze({encoded});\n",
        encoding="utf-8",
        newline="\n",
    )
    with pytest.raises(GovernanceSiteConfigError, match="exactly|byte-identical"):
        verify_governance_site_config(
            tmp_path,
            expected_api_base=API_BASE,
            expected_web_base=WEB_BASE,
            expected_build_sha=REVISION,
        )


def test_extra_config_keys_are_rejected(tmp_path: Path) -> None:
    payload = expected_payload()
    payload["previewToken"] = "must-never-be-public"
    write_site(tmp_path, payload)
    with pytest.raises(GovernanceSiteConfigError, match="exactly"):
        verify_governance_site_config(
            tmp_path,
            expected_api_base=API_BASE,
            expected_web_base=WEB_BASE,
            expected_build_sha=REVISION,
        )
