from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "diagnose_docker_desktop_ingress.ps1"


def _powershell() -> str | None:
    if os.name == "nt":
        return shutil.which("powershell.exe") or shutil.which("pwsh")
    return shutil.which("pwsh")


def _fixture(*, running: bool = True, include_mapping: bool = True) -> dict:
    bindings = []
    if include_mapping:
        bindings = [
            {
                "container_port": 22,
                "protocol": "tcp",
                "host_ip": "0.0.0.0",
                "host_port": 2000,
            },
            {
                "container_port": 80,
                "protocol": "tcp",
                "host_ip": "0.0.0.0",
                "host_port": 2100,
            },
        ]
    return {
        "schema_version": "bside.docker_desktop_ingress.v1",
        "generated_at_utc": "2026-01-01T00:00:00Z",
        "is_administrator": True,
        "inputs": {
            "container_name": "example_container",
            "ssh_host_port": 2000,
            "ssh_container_port": 22,
            "http_host_port": 2100,
            "http_container_port": 80,
            "lan_address": "192.0.2.10",
            "public_address": "198.51.100.20",
            "expected_client_ip": "203.0.113.30",
        },
        "docker": {
            "available": True,
            "container_exists": True,
            "container_running": running,
            "bindings": bindings,
            "container_listeners": [
                "LISTEN 0 128 0.0.0.0:22 0.0.0.0:* users:sshd",
                "LISTEN 0 128 0.0.0.0:80 0.0.0.0:* users:nginx",
            ],
        },
        "host_listeners": [
            {
                "local_address": "::",
                "local_port": 2000,
                "owning_process_id": 100,
                "process_name": "com.docker.backend",
                "process_path": "C:\\Program Files\\Docker\\backend.exe",
            },
            {
                "local_address": "::1",
                "local_port": 2000,
                "owning_process_id": 101,
                "process_name": "wslrelay",
                "process_path": "C:\\Program Files\\WSL\\wslrelay.exe",
            },
            {
                "local_address": "::",
                "local_port": 2100,
                "owning_process_id": 100,
                "process_name": "com.docker.backend",
                "process_path": "C:\\Program Files\\Docker\\backend.exe",
            },
        ],
        "firewall": {
            "available": True,
            "profiles": [],
            "rules": [
                {
                    "display_name": "Example scoped allow",
                    "enabled": True,
                    "direction": "Inbound",
                    "action": "Allow",
                    "profile": "Any",
                    "local_port": "2000",
                    "remote_address": ["203.0.113.30"],
                }
            ],
        },
        "network": {},
        "wfp_drops": {
            "available": True,
            "matching_drop_count": 0,
            "expected_client_drop_count": 0,
        },
        "probes": [
            {
                "vantage": vantage,
                "protocol": protocol,
                "protocol_ok": True,
                "result": "ssh_banner" if protocol == "ssh" else "http_status",
            }
            for vantage in ("loopback", "lan")
            for protocol in ("ssh", "http")
        ],
        "external_tcp_check": {
            "enabled": True,
            "results": [{"port": 2000, "tcp_open": True}],
        },
    }


def _execute_fixture(tmp_path: Path, payload: dict) -> tuple[subprocess.CompletedProcess[str], dict, str]:
    powershell = _powershell()
    assert powershell is not None
    fixture = tmp_path / "fixture.json"
    fixture.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    output = tmp_path / "output"
    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT),
            "-FixturePath",
            str(fixture),
            "-OutputDirectory",
            str(output),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    reports = list(output.glob("*.json"))
    assert len(reports) == 1, completed.stderr or completed.stdout
    report_text = reports[0].read_text(encoding="utf-8")
    markdown_text = next(output.glob("*.md")).read_text(encoding="utf-8")
    return completed, json.loads(report_text), report_text + markdown_text


def test_script_has_safe_read_only_contract() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    for required in (
        "[switch]$UseExternalPortCheck",
        "-FixturePath",
        "-PlanOnly",
        "$ExternalEvidencePath",
        "$ExternalProbeOnly",
        "$NativeCommandTimeoutMs",
        "Stop-LaunchedProcessTree",
        "closed_without_banner",
        "reset_before_banner",
        "ssh_banner",
        "bside.docker_desktop_ingress.v1",
        "Get-NetFirewallProfile -PolicyStore ActiveStore",
        "Get-WinEvent",
        "{{json .NetworkSettings.Ports}}",
    ):
        assert required in source

    lowered = source.lower()
    for forbidden in (
        "invoke-expression",
        "start-transcript",
        "new-netfirewallrule",
        "set-netfirewallrule",
        "remove-netfirewallrule",
        "enable-netfirewallrule",
        "disable-netfirewallrule",
        "docker compose config",
        ".config.env",
        "get-childitem env:",
        "docker restart",
        "docker stop",
        "docker rm",
    ):
        assert forbidden not in lowered


def test_documentation_does_not_embed_real_environment_addresses() -> None:
    docs = (ROOT / "docs" / "docker-desktop-ingress-diagnostics.md").read_text(
        encoding="utf-8"
    )
    assert "211.177." not in docs
    assert "211.36." not in docs
    assert "192.168.171." not in docs
    assert "192.0.2.10" in docs
    assert "198.51.100.20" in docs
    assert "203.0.113.30" in docs


@pytest.mark.skipif(_powershell() is None, reason="PowerShell is unavailable")
def test_powershell_parser_accepts_script() -> None:
    powershell = _powershell()
    assert powershell is not None
    command = (
        "$tokens=$null; $errors=$null; "
        f"[System.Management.Automation.Language.Parser]::ParseFile('{SCRIPT}', "
        "[ref]$tokens, [ref]$errors) | Out-Null; "
        "if ($errors.Count -gt 0) { $errors | ForEach-Object { Write-Error $_ }; exit 1 }"
    )
    completed = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-Command", command],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


@pytest.mark.skipif(_powershell() is None, reason="PowerShell is unavailable")
def test_firewall_collection_supports_split_interface_filters(tmp_path: Path) -> None:
    """Windows exposes interface alias and interface type via different filters."""
    powershell = _powershell()
    assert powershell is not None
    harness = tmp_path / "firewall-filter-harness.ps1"
    escaped_script = str(SCRIPT).replace("'", "''")
    harness.write_text(
        f"""
. '{escaped_script}' -LibraryMode
function Get-NetFirewallProfile {{
    [CmdletBinding()] param([string]$PolicyStore)
    [pscustomobject]@{{ Name='Private'; Enabled=$true; DefaultInboundAction='Block'; AllowInboundRules='True'; AllowLocalFirewallRules='True'; LogAllowed=$false; LogBlocked=$false }}
}}
function Get-NetFirewallPortFilter {{
    [CmdletBinding()] param([string]$PolicyStore)
    [pscustomobject]@{{ Protocol='TCP'; LocalPort='2000' }}
}}
function Get-NetFirewallRule {{
    [CmdletBinding()] param([Parameter(ValueFromPipeline=$true)]$InputObject)
    process {{ [pscustomobject]@{{ DisplayName='Fixture allow'; Enabled=$true; Direction='Inbound'; Action='Allow'; Profile='Private'; EnforcementStatus='Full'; PolicyStoreSourceType='Local'; EdgeTraversalPolicy='Block' }} }}
}}
function Get-NetFirewallAddressFilter {{
    [CmdletBinding()] param([Parameter(ValueFromPipeline=$true)]$InputObject)
    process {{ [pscustomobject]@{{ RemoteAddress=@('Any'); LocalAddress=@('Any') }} }}
}}
function Get-NetFirewallApplicationFilter {{
    [CmdletBinding()] param([Parameter(ValueFromPipeline=$true)]$InputObject)
    process {{ [pscustomobject]@{{ Program='Any' }} }}
}}
function Get-NetFirewallInterfaceFilter {{
    [CmdletBinding()] param([Parameter(ValueFromPipeline=$true)]$InputObject)
    process {{ [pscustomobject]@{{ InterfaceAlias=@('Any') }} }}
}}
function Get-NetFirewallInterfaceTypeFilter {{
    [CmdletBinding()] param([Parameter(ValueFromPipeline=$true)]$InputObject)
    process {{ [pscustomobject]@{{ InterfaceType='Any' }} }}
}}
function Get-NetFirewallServiceFilter {{
    [CmdletBinding()] param([Parameter(ValueFromPipeline=$true)]$InputObject)
    process {{ [pscustomobject]@{{ Service='Any' }} }}
}}
$result = Get-FirewallSnapshot -Ports @(2000)
$result | ConvertTo-Json -Depth 8 -Compress
""",
        encoding="utf-8-sig",
    )
    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(harness),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    snapshot = json.loads(completed.stdout.strip())
    assert snapshot["errors"] == []
    assert snapshot["rules"][0]["interface_alias"] == ["Any"]
    assert snapshot["rules"][0]["interface_type"] == "Any"


@pytest.mark.skipif(_powershell() is None, reason="PowerShell is unavailable")
def test_ipv6_loopback_probe_uses_target_address_family() -> None:
    powershell = _powershell()
    assert powershell is not None
    listener = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    try:
        listener.bind(("::1", 0))
    except OSError as exc:
        listener.close()
        pytest.skip(f"IPv6 loopback is unavailable: {exc}")
    listener.listen(1)
    listener.settimeout(10)
    port = listener.getsockname()[1]

    def serve_banner() -> None:
        connection, _ = listener.accept()
        with connection:
            connection.sendall(b"SSH-2.0-BSIDE-fixture\r\n")

    server = threading.Thread(target=serve_banner, daemon=True)
    server.start()
    escaped_script = str(SCRIPT).replace("'", "''")
    command = (
        f". '{escaped_script}' -LibraryMode; "
        f"Invoke-ProtocolProbe -Address '::1' -Port {port} -Protocol ssh "
        "-Vantage loopback -TimeoutMs 3000 | ConvertTo-Json -Compress"
    )
    try:
        completed = subprocess.run(
            [powershell, "-NoProfile", "-NonInteractive", "-Command", command],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            check=False,
        )
    finally:
        listener.close()
        server.join(timeout=2)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result = json.loads(completed.stdout.strip())
    assert result["result"] == "ssh_banner"
    assert result["protocol_ok"] is True


@pytest.mark.skipif(_powershell() is None, reason="PowerShell is unavailable")
def test_plan_only_performs_no_report_write(tmp_path: Path) -> None:
    powershell = _powershell()
    assert powershell is not None
    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT),
            "-PlanOnly",
            "-OutputDirectory",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "docker.mapping.ssh" in completed.stdout
    assert "No Docker, WSL, firewall, network, or file operation was performed" in completed.stdout
    assert list(tmp_path.iterdir()) == []


@pytest.mark.skipif(_powershell() is None, reason="PowerShell is unavailable")
@pytest.mark.parametrize(
    ("running", "include_mapping", "expected_code", "expected_id"),
    [
        (False, True, 1, "docker.container"),
        (True, False, 1, "docker.mapping.ssh"),
        (True, True, 2, "external.ssh-protocol"),
    ],
)
def test_fixture_mode_classifies_without_system_collection(
    tmp_path: Path,
    running: bool,
    include_mapping: bool,
    expected_code: int,
    expected_id: str,
) -> None:
    powershell = _powershell()
    assert powershell is not None
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        json.dumps(
            _fixture(running=running, include_mapping=include_mapping),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    output = tmp_path / "output"
    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT),
            "-FixturePath",
            str(fixture),
            "-OutputDirectory",
            str(output),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    assert completed.returncode == expected_code, completed.stderr or completed.stdout
    reports = list(output.glob("*.json"))
    assert len(reports) == 1
    report = json.loads(reports[0].read_text(encoding="utf-8"))
    finding = next(item for item in report["findings"] if item["id"] == expected_id)
    if expected_code == 1:
        assert finding["status"] == "fail"
    else:
        assert finding["status"] == "inconclusive"
    assert "password" not in reports[0].read_text(encoding="utf-8").lower()


@pytest.mark.skipif(_powershell() is None, reason="PowerShell is unavailable")
def test_fixture_is_allowlisted_and_redacted(tmp_path: Path) -> None:
    canary = "BSIDE-SECRET-CANARY-7f18e1"
    payload = _fixture()
    payload["unknown_secret"] = {"token": canary}
    payload["docker"]["errors"] = [
        f"Authorization: Bearer {canary}",
        f'{{"token":"{canary}"}}',
        f"https://example.invalid/?X-Amz-Signature={canary}",
    ]
    completed, _, artifacts = _execute_fixture(tmp_path, payload)
    combined = artifacts + completed.stdout + completed.stderr
    assert canary not in combined
    assert "unknown_secret" not in artifacts


@pytest.mark.skipif(_powershell() is None, reason="PowerShell is unavailable")
def test_scoped_firewall_rule_without_client_is_not_a_match(tmp_path: Path) -> None:
    payload = _fixture()
    payload["inputs"]["expected_client_ip"] = ""
    completed, report, _ = _execute_fixture(tmp_path, payload)
    finding = next(item for item in report["findings"] if item["id"] == "firewall.ssh")
    assert completed.returncode == 2
    assert finding["status"] == "inconclusive"


@pytest.mark.skipif(_powershell() is None, reason="PowerShell is unavailable")
def test_any_port_fully_applicable_block_is_detected(tmp_path: Path) -> None:
    payload = _fixture()
    payload["firewall"]["rules"] = [
        {
            "display_name": "Fixture block",
            "enabled": True,
            "direction": "Inbound",
            "action": "Block",
            "profile": "Any",
            "enforcement_status": "Full",
            "protocol": "TCP",
            "local_port": "Any",
            "remote_address": ["Any"],
            "local_address": ["Any"],
            "program": "Any",
            "interface_alias": ["Any"],
            "interface_type": "Any",
            "service_name": "Any",
        }
    ]
    completed, report, _ = _execute_fixture(tmp_path, payload)
    finding = next(item for item in report["findings"] if item["id"] == "firewall.ssh")
    assert completed.returncode == 1
    assert finding["status"] == "fail"


@pytest.mark.skipif(_powershell() is None, reason="PowerShell is unavailable")
def test_controlled_external_close_is_compared_with_http(tmp_path: Path) -> None:
    payload = _fixture()
    payload["external_protocol_evidence"] = {
        "available": True,
        "generated_at_utc": "2026-01-01T00:00:00Z",
        "probes": [
            {
                "protocol": "ssh",
                "port": 2000,
                "result": "closed_without_banner",
                "connected": True,
                "protocol_ok": False,
            },
            {
                "protocol": "http",
                "port": 2100,
                "result": "http_status",
                "connected": True,
                "protocol_ok": True,
            },
        ],
    }
    completed, report, _ = _execute_fixture(tmp_path, payload)
    ssh = next(item for item in report["findings"] if item["id"] == "external.ssh-protocol")
    http = next(item for item in report["findings"] if item["id"] == "external.http-control")
    assert completed.returncode == 1
    assert ssh["status"] == "fail"
    assert "port/protocol-specific" in ssh["summary"]
    assert http["status"] == "pass"


@pytest.mark.skipif(_powershell() is None, reason="PowerShell is unavailable")
def test_third_party_closed_is_inconclusive_for_scoped_rules(tmp_path: Path) -> None:
    payload = _fixture()
    payload["external_tcp_check"]["results"] = [{"port": 2000, "tcp_open": False}]
    completed, report, _ = _execute_fixture(tmp_path, payload)
    finding = next(item for item in report["findings"] if item["id"] == "external.tcp")
    assert completed.returncode == 2
    assert finding["status"] == "inconclusive"


@pytest.mark.skipif(os.name != "nt" or _powershell() is None, reason="Windows PowerShell timeout test")
def test_native_runner_times_out_and_kills_launched_tree() -> None:
    powershell = _powershell()
    assert powershell is not None
    command = (
        f". '{SCRIPT}' -LibraryMode; "
        "$exe=(Get-Process -Id $PID).Path; "
        "$r=Invoke-NativeCommand -FilePath $exe "
        "-Arguments @('-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 5') "
        "-CommandId 'fixture.timeout' -TimeoutMs 500; "
        "$r | ConvertTo-Json -Compress"
    )
    started = time.monotonic()
    completed = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-Command", command],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
        check=False,
    )
    elapsed = time.monotonic() - started
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result = json.loads(completed.stdout.strip())
    assert result["timed_out"] is True
    assert result["tree_kill_attempted"] is True
    assert elapsed < 4
