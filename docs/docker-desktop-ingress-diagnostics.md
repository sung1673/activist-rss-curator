# Docker Desktop ingress diagnostics

`scripts/diagnose_docker_desktop_ingress.ps1` identifies where a Windows
Docker Desktop published-port path stops working. It compares an SSH port with
an nginx control port without authenticating to SSH or changing host state.

The script checks these layers independently:

1. Docker context, daemon, container state, and selected published ports.
2. Container listeners and a limited set of effective `sshd` settings.
3. Windows listeners and their owning processes.
4. ActiveStore Windows Firewall scopes and recent WFP allow/drop events.
5. WSL, default routes, network profiles, and matching `portproxy` entries.
6. SSH banners and HTTP status lines through loopback, LAN, and NAT hairpin.
7. Optionally, public TCP reachability from `portchecker.io`.

It does not collect Docker environment variables, Compose configuration,
mounts, raw container logs, process command lines, Wi-Fi credentials, or
passwords. Except for writing reports under the selected output directory, it
does not change firewall, Docker, WSL, service, container, route, or network
state. A timed-out native command is terminated together with the process tree
that this diagnostic launched so a hung Docker/WSL client is not left behind.

## Run for the BSIDE container

Open an elevated Windows PowerShell in the repository directory:

```powershell
Set-ExecutionPolicy -Scope Process Bypass

.\scripts\diagnose_docker_desktop_ingress.ps1 `
  -ContainerName activist_channel `
  -SshHostPort 2000 `
  -SshContainerPort 22 `
  -HttpHostPort 2100 `
  -HttpContainerPort 80 `
  -LanAddress 192.0.2.10 `
  -PublicAddress 198.51.100.20 `
  -ExpectedClientIp 203.0.113.30 `
  -UseExternalPortCheck
```

Replace the documentation-only addresses with the actual Docker host LAN
address, public VIP, and external test client IP. The optional external check
sends only the public address and the two port numbers to `portchecker.io`.
If an elevated shell can run Docker but the diagnostic service account cannot
find it, pass the exact CLI path with `-DockerExecutablePath`; do not add a
directory-wide search to the script.

By default, reports are written atomically as UTF-8 JSON and Markdown beneath:

```text
%TEMP%\bside-docker-ingress-diagnostics
```

Use `-OutputDirectory` to select another private local directory. Do not
publish the JSON report without reviewing its IP addresses and firewall rule
names.

## Exit codes

| Code | Meaning |
|---:|---|
| `0` | All requested evidence passed, including a separately supplied external protocol result. |
| `1` | A confirmed blocker exists, such as a stopped container, missing mapping, missing listener, or failed local protocol path. |
| `2` | No definitive blocker was found, but evidence is incomplete. This is expected when an independent external SSH banner has not been observed. |
| `3` | Reserved for invalid invocation. PowerShell parameter validation can stop before the script body. |
| `4` | The diagnostic or report generation failed safely. |

The current script intentionally returns `2` even when an external service
confirms that TCP 2000 is open. A TCP handshake does not prove that the SSH
banner, key exchange, or authentication path works.

## Controlled external protocol evidence

The third-party TCP checker connects from its own source address. If the
Windows rule permits only `ExpectedClientIp`, a closed third-party result is
therefore `inconclusive`, not proof that VIP/DNAT is broken.

For a decisive SSH-versus-nginx comparison, copy the same script to the
expected external client network and run:

```powershell
.\diagnose_docker_desktop_ingress.ps1 `
  -ExternalProbeOnly `
  -PublicAddress 198.51.100.20 `
  -SshHostPort 2000 `
  -HttpHostPort 2100 `
  -OutputDirectory .\external-evidence
```

Copy the generated `external-ingress-*.json` privately to the Docker Desktop,
then rerun the host diagnostic with:

```powershell
.\scripts\diagnose_docker_desktop_ingress.ps1 `
  -ContainerName activist_channel `
  -LanAddress 192.0.2.10 `
  -PublicAddress 198.51.100.20 `
  -ExpectedClientIp 203.0.113.30 `
  -ExternalEvidencePath .\external-ingress-result.json
```

The imported evidence must target the same public address and ports and be no
more than 30 minutes old by default. It contains only protocol outcomes and a
bounded banner/status excerpt; it never authenticates to SSH.

## Interpret common combinations

| SSH result | nginx result | Likely layer |
|---|---|---|
| Loopback SSH banner missing | Loopback HTTP works | SSH process, SSH-specific Docker forwarding, or local endpoint security |
| Loopback works; LAN SSH fails | LAN HTTP works | Host interface firewall, EDR/WFP, or port-specific policy |
| Loopback and LAN work; public TCP closed | Public HTTP works | Router VIP/DNAT or upstream TCP 2000 policy |
| Public TCP open; SSH closes before banner | Public HTTP works | Endpoint security, Docker backend forwarding, or SSH pre-auth drop |
| WFP 5152/5157 matches the client and server port | Any | Windows Filtering Platform rule or security product |
| WFP 5156 matches the client and server port | Any | The observed flow reached and was allowed by Windows WFP |
| No WFP event | Any | Inconclusive; WFP connection auditing may be disabled |

`::` owned by `com.docker.backend` and `::1` owned by `wslrelay` are not, by
themselves, a port conflict. The protocol probe is the decisive local check.
Likewise, `EdgeTraversalPolicy=Block` is not proof that ordinary router DNAT is
blocked, and `EnforcementStatus=NotApplicable` alone is not proof that a rule
is inactive.

## Plan and fixture modes

Review planned checks without touching Docker or the network:

```powershell
.\scripts\diagnose_docker_desktop_ingress.ps1 -PlanOnly
```

Tests can pass a versioned JSON snapshot with `-FixturePath`. Fixture mode
performs no Docker, WSL, firewall, event-log, or network probe; it only applies
the same findings and report renderer to supplied data.

## Independent SSH confirmation

Run the following from a separate trusted network, not from the Docker Desktop
host itself:

```bash
ssh-keyscan -T 10 -p 2000 PUBLIC_ADDRESS
```

The public-address probe performed on the Docker Desktop itself is labelled
`hairpin` and is never treated as proof of external ingress.
