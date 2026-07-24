from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / ".github" / "scripts" / "validate-api-base-urls.py"


def load_validator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("validate_api_base_urls", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_equivalent_safe_routes_match_after_normalization() -> None:
    validator = load_validator()

    canonical = validator.validate_api_routing(
        privileged=" https://API.Example.test:443/activist/api.php/api/v1/ ",
        public="https://api.example.test/activist/api.php/api/v1",
    )

    assert canonical == "https://api.example.test/activist/api.php/api/v1"


def test_route_mismatch_never_reports_the_input_routes() -> None:
    validator = load_validator()
    secret_fragment = "private-routing-name"

    with pytest.raises(validator.ApiRoutingError) as captured:
        validator.validate_api_routing(
            privileged=f"https://{secret_fragment}.example/api/v1",
            public="https://public.example/api/v1",
        )

    rendered = str(captured.value)
    assert "mismatch" in rendered
    assert secret_fragment not in rendered
    assert "public.example" not in rendered


@pytest.mark.parametrize(
    "unsafe",
    [
        "",
        "http://api.example.test/api/v1",
        "https://user:password@api.example.test/api/v1",
        "https://api.example.test/api/v1?token=secret",
        "https://api.example.test/api/v1#secret",
        "https://api.example.test/not-v1",
        "https://api.example.test\\evil.example/api/v1",
        "https://api.example.test/a/../api/v1",
        "https://api.example.test/%0d%0aInjected/api/v1",
        "https://2130706433/api/v1",
        "https://127.0.0.1/api/v1",
        "https://[::1]/api/v1",
        "https://api.example.test:8443/api/v1",
    ],
)
def test_unsafe_routes_are_rejected_without_echoing_the_value(unsafe: str) -> None:
    validator = load_validator()

    with pytest.raises(validator.ApiRoutingError) as captured:
        validator.canonical_api_base(unsafe, name="BSIDE_API_BASE_URL")

    rendered = str(captured.value)
    assert "BSIDE_API_BASE_URL" in rendered
    if unsafe:
        assert unsafe not in rendered


def test_main_fails_closed_on_missing_or_mismatched_environment(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    validator = load_validator()
    monkeypatch.setenv("BSIDE_API_BASE_URL", "https://one.example/api/v1")
    monkeypatch.setenv("GOVERNANCE_API_BASE_URL", "https://two.example/api/v1")

    assert validator.main([]) == 1
    output = capsys.readouterr().out
    assert output.startswith("::error::operational API base URL mismatch")
    assert "one.example" not in output
    assert "two.example" not in output


def test_main_writes_only_canonical_values_to_github_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    validator = load_validator()
    github_env = tmp_path / "github-env"
    monkeypatch.setenv(
        "BSIDE_API_BASE_URL",
        "https://API.Example.test:443/activist/api.php/api/v1/",
    )
    monkeypatch.setenv(
        "GOVERNANCE_API_BASE_URL",
        "https://api.example.test/activist/api.php/api/v1",
    )

    assert validator.main(["--github-env", str(github_env)]) == 0

    expected = "https://api.example.test/activist/api.php/api/v1"
    assert github_env.read_text(encoding="utf-8").splitlines() == [
        f"BSIDE_API_BASE_URL={expected}",
        f"GOVERNANCE_API_BASE_URL={expected}",
    ]
    assert capsys.readouterr().out == "Operational API routing verified.\n"
