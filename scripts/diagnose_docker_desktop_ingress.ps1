<#
.SYNOPSIS
Collects read-only Docker Desktop ingress evidence and identifies the first
confirmed failing layer.

.DESCRIPTION
Use -PlanOnly to list checks without touching Docker, Windows networking, or
the filesystem. Use -FixturePath to evaluate a previously captured, sanitized
JSON snapshot without issuing any host commands.
#>
[CmdletBinding()]
param(
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$')]
    [string]$ContainerName = "activist_channel",

    [string]$DockerExecutablePath,

    [ValidateRange(1, 65535)]
    [int]$SshHostPort = 2000,

    [ValidateRange(1, 65535)]
    [int]$SshContainerPort = 22,

    [ValidateRange(1, 65535)]
    [int]$HttpHostPort = 2100,

    [ValidateRange(1, 65535)]
    [int]$HttpContainerPort = 80,

    [System.Net.IPAddress]$LanAddress,

    [System.Net.IPAddress]$PublicAddress,

    [System.Net.IPAddress]$ExpectedClientIp,

    [ValidateRange(500, 30000)]
    [int]$ProbeTimeoutMs = 4000,

    [ValidateRange(1, 120)]
    [int]$WfpLookbackMinutes = 15,

    [ValidateRange(1000, 60000)]
    [int]$NativeCommandTimeoutMs = 15000,

    [switch]$UseExternalPortCheck,

    [string]$ExternalEvidencePath,

    [ValidateRange(1, 1440)]
    [int]$ExternalEvidenceMaxAgeMinutes = 30,

    [switch]$ExternalProbeOnly,

    [string]$OutputDirectory = (Join-Path ([System.IO.Path]::GetTempPath()) "bside-docker-ingress-diagnostics"),

    [string]$FixturePath,

    [switch]$PlanOnly,

    [switch]$LibraryMode
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:ReportSchema = "bside.docker_desktop_ingress.v1"
$script:ExternalEvidenceSchema = "bside.docker_desktop_external_probe.v1"
$script:MaximumNativeOutputCharacters = 65536
$script:NativeCommandTimeoutMs = $NativeCommandTimeoutMs

function Get-ObjectField {
    param(
        [AllowNull()]$Object,
        [Parameter(Mandatory = $true)][string]$Name,
        [AllowNull()]$Default = $null
    )

    if ($null -eq $Object) {
        return $Default
    }
    if ($Object -is [System.Collections.IDictionary]) {
        if ($Object.Contains($Name)) {
            return $Object[$Name]
        }
        return $Default
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -ne $property) {
        return $property.Value
    }
    return $Default
}

function Protect-DiagnosticText {
    param(
        [AllowNull()][object]$Value,
        [int]$MaximumLength = $script:MaximumNativeOutputCharacters
    )

    if ($null -eq $Value) {
        return ""
    }

    $text = [string]$Value
    $text = [regex]::Replace($text, "`e\][^`a]*(?:`a|`e\\)", "")
    $text = [regex]::Replace($text, "`e\[[0-?]*[ -/]*[@-~]", "")
    $text = [regex]::Replace($text, '(?im)\b(authorization|proxy-authorization)\s*[:=]\s*[^\r\n]+', '$1=<redacted>')
    $text = [regex]::Replace(
        $text,
        '(?im)\b(password|passwd|secret|token|api[_-]?key|access[_-]?token)\s*["'']?\s*[:=]\s*["'']?(?:bearer\s+|basic\s+)?[^"''\s,;}\]]+',
        '$1=<redacted>'
    )
    $text = [regex]::Replace($text, '(?i)([?&](?:x-amz-signature|sig|token|api[_-]?key)=)[^&#\s]+', '$1<redacted>')
    $text = [regex]::Replace($text, '(?i)(https?://)[^/@\s:]+:[^/@\s]+@', '$1<redacted>@')
    $text = [regex]::Replace(
        $text,
        '(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----',
        '<private-key-redacted>'
    )
    if ($env:USERPROFILE) {
        $text = $text.Replace($env:USERPROFILE, "<USERPROFILE>")
    }
    $text = [regex]::Replace($text, '[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '')
    if ($text.Length -gt $MaximumLength) {
        return $text.Substring(0, $MaximumLength) + "`n<truncated>"
    }
    return $text.Trim()
}

function Resolve-NativeTool {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string[]]$FallbackPaths = @()
    )

    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }
    foreach ($path in $FallbackPaths) {
        if (Test-Path -LiteralPath $path) {
            return $path
        }
    }
    return $null
}

function ConvertTo-WindowsCommandLineArgument {
    param([AllowEmptyString()][string]$Value)

    if ($Value.Length -eq 0) {
        return '""'
    }
    if ($Value -notmatch '[\s"]') {
        return $Value
    }

    # Quote according to the CommandLineToArgvW/MS C runtime rules. The
    # process is started directly; no cmd.exe or expression evaluation occurs.
    $builder = New-Object System.Text.StringBuilder
    [void]$builder.Append([char]34)
    $backslashes = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq [char]92) {
            $backslashes++
            continue
        }
        if ($character -eq [char]34) {
            if ($backslashes -gt 0) {
                [void]$builder.Append([char]92, $backslashes * 2)
            }
            [void]$builder.Append([char]92)
            [void]$builder.Append([char]34)
            $backslashes = 0
            continue
        }
        if ($backslashes -gt 0) {
            [void]$builder.Append([char]92, $backslashes)
            $backslashes = 0
        }
        [void]$builder.Append($character)
    }
    if ($backslashes -gt 0) {
        [void]$builder.Append([char]92, $backslashes * 2)
    }
    [void]$builder.Append([char]34)
    return $builder.ToString()
}

function Stop-LaunchedProcessTree {
    param([Parameter(Mandatory = $true)][System.Diagnostics.Process]$Process)

    try {
        if ($Process.HasExited) {
            return
        }
        $taskkillPath = Join-Path $env:SystemRoot "System32\taskkill.exe"
        if (Test-Path -LiteralPath $taskkillPath) {
            $stopInfo = New-Object System.Diagnostics.ProcessStartInfo
            $stopInfo.FileName = $taskkillPath
            $stopInfo.Arguments = "/PID $($Process.Id) /T /F"
            $stopInfo.UseShellExecute = $false
            $stopInfo.CreateNoWindow = $true
            $stopInfo.RedirectStandardOutput = $true
            $stopInfo.RedirectStandardError = $true
            $stopper = New-Object System.Diagnostics.Process
            try {
                $stopper.StartInfo = $stopInfo
                if ($stopper.Start()) {
                    $stdoutTask = $stopper.StandardOutput.ReadToEndAsync()
                    $stderrTask = $stopper.StandardError.ReadToEndAsync()
                    if (-not $stopper.WaitForExit(3000)) {
                        try { $stopper.Kill() } catch { }
                    }
                    try { [void]$stdoutTask.Result } catch { }
                    try { [void]$stderrTask.Result } catch { }
                }
            } finally {
                $stopper.Dispose()
            }
        }
        if (-not $Process.HasExited) {
            try { $Process.Kill() } catch { }
        }
    } catch {
        try { if (-not $Process.HasExited) { $Process.Kill() } } catch { }
    }
}

function Invoke-NativeCommand {
    param(
        [AllowNull()][string]$FilePath,
        [string[]]$Arguments = @(),
        [string]$CommandId = "native",
        [int]$TimeoutMs = $script:NativeCommandTimeoutMs
    )

    if (-not $FilePath) {
        return [pscustomobject][ordered]@{
            command_id = $CommandId
            available = $false
            exit_code = $null
            duration_ms = 0
            timed_out = $false
            tree_kill_attempted = $false
            stdout = ""
            stderr = ""
            output = ""
        }
    }

    $started = [System.Diagnostics.Stopwatch]::StartNew()
    $process = $null
    try {
        $startInfo = New-Object System.Diagnostics.ProcessStartInfo
        $startInfo.FileName = $FilePath
        $startInfo.Arguments = (@($Arguments | ForEach-Object {
            ConvertTo-WindowsCommandLineArgument ([string]$_)
        }) -join " ")
        $startInfo.UseShellExecute = $false
        $startInfo.CreateNoWindow = $true
        $startInfo.RedirectStandardOutput = $true
        $startInfo.RedirectStandardError = $true

        $process = New-Object System.Diagnostics.Process
        $process.StartInfo = $startInfo
        if (-not $process.Start()) {
            throw "The native process did not start"
        }
        $standardOutputTask = $process.StandardOutput.ReadToEndAsync()
        $standardErrorTask = $process.StandardError.ReadToEndAsync()
        $timedOut = -not $process.WaitForExit($TimeoutMs)
        if ($timedOut) {
            Stop-LaunchedProcessTree -Process $process
            [void]$process.WaitForExit(2000)
        } else {
            # The parameterless wait flushes asynchronous redirected streams.
            $process.WaitForExit()
        }

        $standardOutput = ""
        $standardError = ""
        try { $standardOutput = [string]$standardOutputTask.Result } catch { }
        try { $standardError = [string]$standardErrorTask.Result } catch { }
        $combinedOutput = (@($standardOutput, $standardError) | Where-Object { $_ }) -join "`n"
        $safeStandardOutput = Protect-DiagnosticText $standardOutput
        $safeStandardError = Protect-DiagnosticText $standardError
        $started.Stop()
        return [pscustomobject][ordered]@{
            command_id = $CommandId
            available = $true
            exit_code = if ($timedOut) { $null } else { $process.ExitCode }
            duration_ms = [int]$started.ElapsedMilliseconds
            timed_out = $timedOut
            tree_kill_attempted = $timedOut
            stdout = $safeStandardOutput
            stderr = $safeStandardError
            output = if ($timedOut -and -not $combinedOutput) {
                "Native command timed out after $TimeoutMs ms"
            } else {
                Protect-DiagnosticText $combinedOutput
            }
        }
    } catch {
        $started.Stop()
        return [pscustomobject][ordered]@{
            command_id = $CommandId
            available = $true
            exit_code = -1
            duration_ms = [int]$started.ElapsedMilliseconds
            timed_out = $false
            tree_kill_attempted = $false
            stdout = ""
            stderr = Protect-DiagnosticText $_.Exception.Message
            output = Protect-DiagnosticText $_.Exception.Message
        }
    } finally {
        if ($process) {
            $process.Dispose()
        }
    }
}

function Test-IsAdministrator {
    try {
        $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
        $principal = New-Object Security.Principal.WindowsPrincipal($identity)
        return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    } catch {
        return $false
    }
}

function Get-DockerSnapshot {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [int]$ContainerSshPort,
        [int]$ContainerHttpPort,
        [AllowNull()][string]$ExplicitDockerPath
    )

    $dockerPath = $null
    if ($ExplicitDockerPath) {
        if (Test-Path -LiteralPath $ExplicitDockerPath -PathType Leaf) {
            $dockerPath = (Resolve-Path -LiteralPath $ExplicitDockerPath).Path
        }
    } else {
        $dockerPath = Resolve-NativeTool -Name "docker" -FallbackPaths @(
            "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
        )
    }
    $snapshot = [ordered]@{
        available = [bool]$dockerPath
        context = ""
        version = ""
        container_exists = $false
        container_status = "unknown"
        container_running = $false
        health = "unknown"
        restart_count = $null
        network_mode = ""
        bindings = @()
        container_listeners = @()
        listener_query_succeeded = $false
        listener_format = "unknown"
        sshd_effective = @()
        errors = @()
    }

    if (-not $dockerPath) {
        $snapshot.errors = @("docker.exe was not found")
        return [pscustomobject]$snapshot
    }

    $context = Invoke-NativeCommand -FilePath $dockerPath -Arguments @("context", "show") -CommandId "docker.context"
    if ($context.exit_code -eq 0) {
        $snapshot.context = $context.stdout
    } else {
        $snapshot.errors += "Docker context query failed: $($context.output)"
    }

    $version = Invoke-NativeCommand -FilePath $dockerPath -Arguments @(
        "version",
        "--format",
        "{{.Client.Version}}|{{.Server.Version}}|{{.Server.Os}}"
    ) -CommandId "docker.version"
    if ($version.exit_code -eq 0) {
        $snapshot.version = $version.stdout
    } else {
        $snapshot.errors += "Docker daemon query failed: $($version.output)"
    }

    $state = Invoke-NativeCommand -FilePath $dockerPath -Arguments @(
        "inspect",
        "--type", "container",
        "--format", "{{.State.Status}}|{{.State.Running}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}|{{.RestartCount}}|{{.HostConfig.NetworkMode}}",
        $Name
    ) -CommandId "docker.container.state"
    if ($state.exit_code -ne 0) {
        $snapshot.errors += "Container state query failed: $($state.output)"
        return [pscustomobject]$snapshot
    }

    $stateParts = @($state.stdout -split '\|', 5)
    if ($stateParts.Count -ge 5) {
        $snapshot.container_exists = $true
        $snapshot.container_status = $stateParts[0].Trim()
        $snapshot.container_running = $stateParts[1].Trim() -eq "true"
        $snapshot.health = $stateParts[2].Trim()
        $restartValue = 0
        if ([int]::TryParse($stateParts[3].Trim(), [ref]$restartValue)) {
            $snapshot.restart_count = $restartValue
        }
        $snapshot.network_mode = $stateParts[4].Trim()
    }

    $ports = Invoke-NativeCommand -FilePath $dockerPath -Arguments @(
        "inspect",
        "--type", "container",
        "--format", "{{json .NetworkSettings.Ports}}",
        $Name
    ) -CommandId "docker.container.ports"
    if ($ports.exit_code -eq 0 -and $ports.stdout) {
        try {
            $portObject = $ports.stdout | ConvertFrom-Json
            $bindings = New-Object System.Collections.Generic.List[object]
            foreach ($property in $portObject.PSObject.Properties) {
                $containerParts = @($property.Name -split '/', 2)
                foreach ($binding in @($property.Value)) {
                    if ($null -eq $binding) {
                        continue
                    }
                    $bindings.Add([pscustomobject][ordered]@{
                        container_port = [int]$containerParts[0]
                        protocol = if ($containerParts.Count -gt 1) { $containerParts[1] } else { "tcp" }
                        host_ip = [string]$binding.HostIp
                        host_port = [int]$binding.HostPort
                    })
                }
            }
            $snapshot.bindings = $bindings.ToArray()
        } catch {
            $snapshot.errors += "Container port JSON could not be parsed"
        }
    } else {
        $snapshot.errors += "Container port query failed: $($ports.output)"
    }

    if ($snapshot.container_running) {
        $listeners = Invoke-NativeCommand -FilePath $dockerPath -Arguments @(
            "exec", $Name, "sh", "-lc",
            "if command -v ss >/dev/null 2>&1; then echo __BSIDE_LISTENER_FORMAT__=ss; ss -H -lntp; elif command -v netstat >/dev/null 2>&1; then echo __BSIDE_LISTENER_FORMAT__=netstat; netstat -lntp; else echo __BSIDE_LISTENER_FORMAT__=proc; cat /proc/net/tcp /proc/net/tcp6; fi"
        ) -CommandId "docker.container.listeners"
        if ($listeners.exit_code -eq 0) {
            $snapshot.listener_query_succeeded = $true
            $allListenerLines = @($listeners.stdout -split "`r?`n")
            $formatLine = @($allListenerLines | Where-Object { $_ -like "__BSIDE_LISTENER_FORMAT__=*" } | Select-Object -First 1)
            if ($formatLine.Count -eq 1) {
                $snapshot.listener_format = [string]$formatLine[0].Substring("__BSIDE_LISTENER_FORMAT__=".Length)
            }
            $listenerLines = @($allListenerLines | Where-Object {
                $_ -match (":$ContainerSshPort(?:\s|$)") -or $_ -match (":$ContainerHttpPort(?:\s|$)")
            })
            $snapshot.container_listeners = @($listenerLines | ForEach-Object { Protect-DiagnosticText $_ 1000 })
        } else {
            $snapshot.errors += "Container listener query failed: $($listeners.output)"
        }

        $sshd = Invoke-NativeCommand -FilePath $dockerPath -Arguments @(
            "exec", $Name, "sh", "-lc",
            "if command -v sshd >/dev/null 2>&1; then sshd -T 2>/dev/null; elif [ -x /usr/sbin/sshd ]; then /usr/sbin/sshd -T 2>/dev/null; fi"
        ) -CommandId "docker.container.sshd"
        if ($sshd.exit_code -eq 0) {
            $allowedKeys = @(
                "port", "listenaddress", "passwordauthentication", "pubkeyauthentication",
                "maxstartups", "persourcemaxstartups", "persourcenetblocksize", "loglevel"
            )
            $effectiveLines = @($sshd.stdout -split "`r?`n" | Where-Object {
                $line = $_.Trim()
                if (-not $line) { return $false }
                $key = @($line -split '\s+', 2)[0].ToLowerInvariant()
                return $allowedKeys -contains $key
            })
            $snapshot.sshd_effective = @($effectiveLines | ForEach-Object { Protect-DiagnosticText $_ 1000 })
        }
    }

    return [pscustomobject]$snapshot
}

function Get-HostListenerSnapshot {
    param([int[]]$Ports)

    $snapshot = [ordered]@{
        available = $false
        query_succeeded = $true
        records = @()
        errors = @()
    }
    $result = New-Object System.Collections.Generic.List[object]
    if (-not (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue)) {
        $snapshot.errors = @("Get-NetTCPConnection is unavailable")
        $snapshot.query_succeeded = $false
        return [pscustomobject]$snapshot
    }
    $snapshot.available = $true

    foreach ($port in $Ports) {
        try {
            $listeners = @(Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction Stop)
            foreach ($listener in $listeners) {
                $processName = "unknown"
                $processPath = ""
                try {
                    $process = Get-Process -Id $listener.OwningProcess -ErrorAction Stop
                    $processName = $process.ProcessName
                    $processPath = Protect-DiagnosticText $process.Path 2048
                } catch {
                    $processName = "unavailable"
                }
                $result.Add([pscustomobject][ordered]@{
                    local_address = [string]$listener.LocalAddress
                    local_port = [int]$listener.LocalPort
                    owning_process_id = [int]$listener.OwningProcess
                    process_name = [string]$processName
                    process_path = [string]$processPath
                })
            }
        } catch {
            $snapshot.query_succeeded = $false
            $snapshot.errors += "Listener query failed for port $port"
        }
    }
    $snapshot.records = $result.ToArray()
    return [pscustomobject]$snapshot
}

function Test-PortSpecificationIncludes {
    param(
        [AllowNull()][object]$Specification,
        [int]$Port
    )

    $text = [string]$Specification
    if (-not $text) {
        return $false
    }
    foreach ($tokenObject in @($text -split ',')) {
        $token = ([string]$tokenObject).Trim()
        if ($token -in @("Any", "*")) {
            return $true
        }
        $exact = 0
        if ([int]::TryParse($token, [ref]$exact) -and $exact -eq $Port) {
            return $true
        }
        if ($token -match '^([0-9]{1,5})-([0-9]{1,5})$') {
            $lower = [int]$Matches[1]
            $upper = [int]$Matches[2]
            if ($Port -ge $lower -and $Port -le $upper) {
                return $true
            }
        }
    }
    return $false
}

function Get-FirewallSnapshot {
    param([int[]]$Ports)

    $snapshot = [ordered]@{
        available = $false
        profiles = @()
        rules = @()
        errors = @()
    }
    if (-not (Get-Command Get-NetFirewallProfile -ErrorAction SilentlyContinue)) {
        $snapshot.errors = @("NetSecurity PowerShell module is unavailable")
        return [pscustomobject]$snapshot
    }

    $snapshot.available = $true
    try {
        $profiles = @(Get-NetFirewallProfile -PolicyStore ActiveStore -ErrorAction Stop)
        $snapshot.profiles = @($profiles | ForEach-Object {
            [pscustomobject][ordered]@{
                name = [string]$_.Name
                enabled = [bool]$_.Enabled
                default_inbound_action = [string]$_.DefaultInboundAction
                allow_inbound_rules = [string]$_.AllowInboundRules
                allow_local_firewall_rules = [string]$_.AllowLocalFirewallRules
                log_allowed = [bool]$_.LogAllowed
                log_blocked = [bool]$_.LogBlocked
            }
        })
    } catch {
        $snapshot.errors += "Active firewall profiles could not be read: $(Protect-DiagnosticText $_.Exception.Message 1000)"
    }

    try {
        $filters = @(Get-NetFirewallPortFilter -PolicyStore ActiveStore -ErrorAction Stop | Where-Object {
            $candidate = $_
            if ([string]$candidate.Protocol -notin @("TCP", "6", "Any", "256")) {
                return $false
            }
            foreach ($requestedPort in $Ports) {
                if (Test-PortSpecificationIncludes -Specification $candidate.LocalPort -Port $requestedPort) {
                    return $true
                }
            }
            return $false
        })
        $rules = New-Object System.Collections.Generic.List[object]
        foreach ($filter in $filters) {
            foreach ($rule in @($filter | Get-NetFirewallRule -ErrorAction Stop)) {
                $addressFilter = @($rule | Get-NetFirewallAddressFilter -ErrorAction SilentlyContinue | Select-Object -First 1)
                $applicationFilter = @($rule | Get-NetFirewallApplicationFilter -ErrorAction SilentlyContinue | Select-Object -First 1)
                $interfaceFilter = @($rule | Get-NetFirewallInterfaceFilter -ErrorAction SilentlyContinue | Select-Object -First 1)
                $interfaceTypeFilter = @()
                if (Get-Command Get-NetFirewallInterfaceTypeFilter -ErrorAction SilentlyContinue) {
                    $interfaceTypeFilter = @($rule | Get-NetFirewallInterfaceTypeFilter -ErrorAction SilentlyContinue | Select-Object -First 1)
                }
                $serviceFilter = @($rule | Get-NetFirewallServiceFilter -ErrorAction SilentlyContinue | Select-Object -First 1)
                $remoteAddress = if ($addressFilter.Count -gt 0) { @(Get-ObjectField $addressFilter[0] "RemoteAddress" @("Unknown")) } else { @("Unknown") }
                $localAddress = if ($addressFilter.Count -gt 0) { @(Get-ObjectField $addressFilter[0] "LocalAddress" @("Unknown")) } else { @("Unknown") }
                $program = if ($applicationFilter.Count -gt 0) { [string](Get-ObjectField $applicationFilter[0] "Program" "Unknown") } else { "Unknown" }
                $interfaceAlias = if ($interfaceFilter.Count -gt 0) { @(Get-ObjectField $interfaceFilter[0] "InterfaceAlias" @("Unknown")) } else { @("Unknown") }
                $interfaceType = if ($interfaceTypeFilter.Count -gt 0) { [string](Get-ObjectField $interfaceTypeFilter[0] "InterfaceType" "Unknown") } else { "Unknown" }
                $serviceName = if ($serviceFilter.Count -gt 0) { [string](Get-ObjectField $serviceFilter[0] "Service" "Unknown") } else { "Unknown" }
                $rules.Add([pscustomobject][ordered]@{
                    display_name = Protect-DiagnosticText $rule.DisplayName 500
                    enabled = [bool]$rule.Enabled
                    direction = [string]$rule.Direction
                    action = [string]$rule.Action
                    profile = [string]$rule.Profile
                    enforcement_status = [string]$rule.EnforcementStatus
                    policy_store_source_type = [string]$rule.PolicyStoreSourceType
                    edge_traversal_policy = [string]$rule.EdgeTraversalPolicy
                    protocol = [string]$filter.Protocol
                    local_port = [string]$filter.LocalPort
                    remote_address = @($remoteAddress | ForEach-Object { Protect-DiagnosticText $_ 500 })
                    local_address = @($localAddress | ForEach-Object { Protect-DiagnosticText $_ 500 })
                    program = Protect-DiagnosticText $program 2048
                    interface_alias = @($interfaceAlias | ForEach-Object { Protect-DiagnosticText $_ 500 })
                    interface_type = Protect-DiagnosticText $interfaceType 100
                    service_name = Protect-DiagnosticText $serviceName 500
                })
            }
        }
        $snapshot.rules = $rules.ToArray()
    } catch {
        $snapshot.errors += "Firewall port rules could not be read: $(Protect-DiagnosticText $_.Exception.Message 1000)"
    }

    return [pscustomobject]$snapshot
}

function Get-WfpDropSnapshot {
    param(
        [int[]]$Ports,
        [int]$LookbackMinutes,
        [System.Net.IPAddress]$ClientIp
    )

    $snapshot = [ordered]@{
        available = $false
        audited_event_count = 0
        matching_drop_count = 0
        expected_client_drop_count = 0
        matching_allow_count = 0
        expected_client_allow_count = 0
        events = @()
        error = ""
    }
    if (-not (Get-Command Get-WinEvent -ErrorAction SilentlyContinue)) {
        $snapshot.error = "Get-WinEvent is unavailable"
        return [pscustomobject]$snapshot
    }

    try {
        $events = @(Get-WinEvent -FilterHashtable @{
            LogName = "Security"
            Id = @(5152, 5156, 5157)
            StartTime = (Get-Date).AddMinutes(-1 * $LookbackMinutes)
        } -MaxEvents 1000 -ErrorAction Stop)
        $snapshot.available = $true
        $snapshot.audited_event_count = $events.Count
        $safeEvents = New-Object System.Collections.Generic.List[object]
        foreach ($event in $events) {
            $xml = [xml]$event.ToXml()
            $fields = @{}
            foreach ($data in @($xml.Event.EventData.Data)) {
                $fields[[string]$data.Name] = [string]$data.'#text'
            }
            $sourcePort = 0
            $destinationPort = 0
            [void][int]::TryParse([string]$fields["SourcePort"], [ref]$sourcePort)
            [void][int]::TryParse([string]$fields["DestPort"], [ref]$destinationPort)
            $flowDirection = ""
            $serverPort = 0
            $remoteAddress = ""
            if ($Ports -contains $destinationPort) {
                $flowDirection = "inbound_to_published_port"
                $serverPort = $destinationPort
                $remoteAddress = [string]$fields["SourceAddress"]
            } elseif ($Ports -contains $sourcePort) {
                $flowDirection = "outbound_from_published_port"
                $serverPort = $sourcePort
                $remoteAddress = [string]$fields["DestAddress"]
            } else {
                continue
            }
            $sourceMatches = $false
            if ($ClientIp -and $remoteAddress) {
                $sourceMatches = $remoteAddress -eq $ClientIp.ToString()
            }
            $safeEvents.Add([pscustomobject][ordered]@{
                event_id = [int]$event.Id
                decision = if ([int]$event.Id -eq 5156) { "allow" } else { "drop" }
                time_created_utc = $event.TimeCreated.ToUniversalTime().ToString("o")
                server_port = $serverPort
                flow_direction = $flowDirection
                wfp_direction = Protect-DiagnosticText $fields["Direction"] 100
                application = Protect-DiagnosticText $fields["Application"] 2048
                remote_matches_expected_client = $sourceMatches
            })
        }
        $snapshot.events = $safeEvents.ToArray()
        $snapshot.matching_drop_count = @($safeEvents | Where-Object { $_.decision -eq "drop" }).Count
        $snapshot.expected_client_drop_count = @($safeEvents | Where-Object { $_.decision -eq "drop" -and $_.remote_matches_expected_client }).Count
        $snapshot.matching_allow_count = @($safeEvents | Where-Object { $_.decision -eq "allow" }).Count
        $snapshot.expected_client_allow_count = @($safeEvents | Where-Object { $_.decision -eq "allow" -and $_.remote_matches_expected_client }).Count
    } catch {
        $snapshot.error = Protect-DiagnosticText $_.Exception.Message 1000
    }
    return [pscustomobject]$snapshot
}

function Get-NetworkSnapshot {
    param([int[]]$Ports)

    $snapshot = [ordered]@{
        addresses = @()
        profiles = @()
        default_routes = @()
        docker_services = @()
        portproxy = @()
        wsl_status = ""
        wsl_version = ""
        errors = @()
    }

    try {
        $snapshot.addresses = @(Get-NetIPAddress -AddressFamily IPv4 -ErrorAction Stop | Where-Object {
            $_.AddressState -eq "Preferred" -and $_.IPAddress -notlike "127.*" -and $_.IPAddress -notlike "169.254.*"
        } | ForEach-Object {
            [pscustomobject][ordered]@{
                interface_alias = Protect-DiagnosticText $_.InterfaceAlias 500
                interface_index = [int]$_.InterfaceIndex
                ip_address = [string]$_.IPAddress
                prefix_length = [int]$_.PrefixLength
            }
        })
    } catch {
        $snapshot.errors += "IPv4 address query failed: $(Protect-DiagnosticText $_.Exception.Message 500)"
    }

    try {
        $snapshot.profiles = @(Get-NetConnectionProfile -ErrorAction Stop | ForEach-Object {
            [pscustomobject][ordered]@{
                interface_alias = Protect-DiagnosticText $_.InterfaceAlias 500
                interface_index = [int]$_.InterfaceIndex
                network_category = [string]$_.NetworkCategory
                ipv4_connectivity = [string]$_.IPv4Connectivity
                ipv6_connectivity = [string]$_.IPv6Connectivity
            }
        })
    } catch {
        $snapshot.errors += "Network profile query failed: $(Protect-DiagnosticText $_.Exception.Message 500)"
    }

    try {
        $snapshot.default_routes = @(Get-NetRoute -AddressFamily IPv4 -DestinationPrefix "0.0.0.0/0" -ErrorAction Stop | ForEach-Object {
            [pscustomobject][ordered]@{
                interface_index = [int]$_.InterfaceIndex
                next_hop = [string]$_.NextHop
                route_metric = [int]$_.RouteMetric
                interface_metric = [int]$_.InterfaceMetric
                state = [string]$_.State
            }
        })
    } catch {
        $snapshot.errors += "Default route query failed: $(Protect-DiagnosticText $_.Exception.Message 500)"
    }

    foreach ($serviceName in @("com.docker.service", "wslservice", "LxssManager")) {
        try {
            $service = Get-Service -Name $serviceName -ErrorAction Stop
            $snapshot.docker_services += [pscustomobject][ordered]@{
                name = $serviceName
                status = [string]$service.Status
                start_type = [string]$service.StartType
            }
        } catch {
            # Service names vary across supported Windows versions.
        }
    }

    $netshPath = Resolve-NativeTool -Name "netsh"
    $portproxy = Invoke-NativeCommand -FilePath $netshPath -Arguments @("interface", "portproxy", "show", "all") -CommandId "windows.portproxy"
    if ($portproxy.exit_code -eq 0 -and $portproxy.stdout) {
        $matchingLines = @($portproxy.stdout -split "`r?`n" | Where-Object {
            $line = $_
            @($Ports | Where-Object { $line -match ("(^|\D)" + $_ + "(\D|$)") }).Count -gt 0
        })
        $snapshot.portproxy = @($matchingLines | ForEach-Object { Protect-DiagnosticText $_ 1000 })
    }

    $wslPath = Resolve-NativeTool -Name "wsl"
    $wslStatus = Invoke-NativeCommand -FilePath $wslPath -Arguments @("--status") -CommandId "wsl.status"
    if ($wslStatus.exit_code -eq 0) {
        $snapshot.wsl_status = $wslStatus.stdout
    }
    $wslVersion = Invoke-NativeCommand -FilePath $wslPath -Arguments @("--version") -CommandId "wsl.version"
    if ($wslVersion.exit_code -eq 0) {
        $snapshot.wsl_version = $wslVersion.stdout
    }

    return [pscustomobject]$snapshot
}

function Invoke-ProtocolProbe {
    param(
        [Parameter(Mandatory = $true)][string]$Address,
        [Parameter(Mandatory = $true)][int]$Port,
        [ValidateSet("ssh", "http")][string]$Protocol,
        [ValidateSet("loopback", "lan", "hairpin", "external")][string]$Vantage,
        [int]$TimeoutMs
    )

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $client = $null
    try {
        [System.Net.IPAddress]$parsedAddress = $null
        if ([System.Net.IPAddress]::TryParse($Address, [ref]$parsedAddress)) {
            # TcpClient() can select an IPv4 socket on Windows PowerShell 5.1.
            # Construct it with the target family so ::1 is probed with IPv6.
            $client = [System.Net.Sockets.TcpClient]::new($parsedAddress.AddressFamily)
            $connectTask = $client.ConnectAsync($parsedAddress, $Port)
        } else {
            $client = New-Object System.Net.Sockets.TcpClient
            $connectTask = $client.ConnectAsync($Address, $Port)
        }
        try {
            $connectCompleted = $connectTask.Wait($TimeoutMs)
        } catch {
            $baseException = $_.Exception.GetBaseException()
            $connectResult = "connect_error"
            if ($baseException -is [System.Net.Sockets.SocketException]) {
                if ($baseException.SocketErrorCode -eq [System.Net.Sockets.SocketError]::ConnectionRefused) {
                    $connectResult = "tcp_refused"
                } elseif ($baseException.SocketErrorCode -in @(
                    [System.Net.Sockets.SocketError]::HostUnreachable,
                    [System.Net.Sockets.SocketError]::NetworkUnreachable
                )) {
                    $connectResult = "tcp_unreachable"
                }
            }
            $stopwatch.Stop()
            return [pscustomobject][ordered]@{
                address = $Address; port = $Port; protocol = $Protocol; vantage = $Vantage
                result = $connectResult; connected = $false; protocol_ok = $false
                duration_ms = [int]$stopwatch.ElapsedMilliseconds
                evidence = Protect-DiagnosticText $baseException.Message 500
            }
        }
        if (-not $connectCompleted) {
            $stopwatch.Stop()
            return [pscustomobject][ordered]@{
                address = $Address; port = $Port; protocol = $Protocol; vantage = $Vantage
                result = "tcp_timeout"; connected = $false; protocol_ok = $false
                duration_ms = [int]$stopwatch.ElapsedMilliseconds
                evidence = "TCP connection timed out"
            }
        }

        $stream = $client.GetStream()
        $stream.WriteTimeout = $TimeoutMs
        if ($Protocol -eq "http") {
            $request = [System.Text.Encoding]::ASCII.GetBytes("HEAD / HTTP/1.0`r`nHost: $Address`r`nConnection: close`r`n`r`n")
            $stream.Write($request, 0, $request.Length)
            $stream.Flush()
        }

        $memory = New-Object System.IO.MemoryStream
        $readBuffer = New-Object byte[] 128
        $terminal = "deadline"
        $terminalEvidence = ""
        while ($memory.Length -lt 512 -and $stopwatch.ElapsedMilliseconds -lt $TimeoutMs) {
            $remaining = [Math]::Max(100, $TimeoutMs - [int]$stopwatch.ElapsedMilliseconds)
            $stream.ReadTimeout = $remaining
            try {
                $read = $stream.Read($readBuffer, 0, [Math]::Min($readBuffer.Length, 512 - [int]$memory.Length))
            } catch [System.IO.IOException] {
                $baseException = $_.Exception.GetBaseException()
                if ($baseException -is [System.Net.Sockets.SocketException] -and
                    $baseException.SocketErrorCode -in @(
                        [System.Net.Sockets.SocketError]::ConnectionReset,
                        [System.Net.Sockets.SocketError]::ConnectionAborted
                    )) {
                    $terminal = "reset"
                    $terminalEvidence = $baseException.SocketErrorCode.ToString()
                } else {
                    $terminal = "timeout"
                    $terminalEvidence = $baseException.Message
                }
                break
            }
            if ($read -le 0) {
                $terminal = "closed"
                break
            }
            $memory.Write($readBuffer, 0, $read)
            $candidate = [System.Text.Encoding]::ASCII.GetString($memory.ToArray())
            if ($candidate -match "`r?`n") {
                $terminal = "line"
                break
            }
        }

        $stopwatch.Stop()
        $response = Protect-DiagnosticText ([System.Text.Encoding]::ASCII.GetString($memory.ToArray())) 512
        $memory.Dispose()
        $pattern = if ($Protocol -eq "ssh") {
            '(?m)^SSH-\d+\.\d+-[^\r\n]{1,200}'
        } else {
            '(?m)^HTTP/\d+(?:\.\d+)?\s+\d{3}[^\r\n]*'
        }
        $match = [regex]::Match($response, $pattern)
        if ($match.Success) {
            return [pscustomobject][ordered]@{
                address = $Address; port = $Port; protocol = $Protocol; vantage = $Vantage
                result = if ($Protocol -eq "ssh") { "ssh_banner" } else { "http_status" }
                connected = $true; protocol_ok = $true
                duration_ms = [int]$stopwatch.ElapsedMilliseconds
                evidence = Protect-DiagnosticText $match.Value 250
            }
        }
        if ($terminal -eq "reset") {
            $result = "reset_before_banner"
            $evidence = "TCP connected and was reset before a valid protocol response: $terminalEvidence"
        } elseif ($terminal -eq "closed" -and -not $response) {
            $result = "closed_without_banner"
            $evidence = "TCP connected and then closed before a protocol banner"
        } elseif ($terminal -in @("timeout", "deadline")) {
            $result = "connected_read_timeout"
            $evidence = "TCP connected but no valid protocol response arrived before the deadline"
        } else {
            $result = "unexpected_protocol"
            $evidence = $response
        }
        return [pscustomobject][ordered]@{
            address = $Address; port = $Port; protocol = $Protocol; vantage = $Vantage
            result = $result; connected = $true; protocol_ok = $false
            duration_ms = [int]$stopwatch.ElapsedMilliseconds
            evidence = Protect-DiagnosticText $evidence 512
        }
    } catch {
        $stopwatch.Stop()
        $wasConnected = $false
        if ($null -ne $client) {
            $wasConnected = $client.Connected
        }
        return [pscustomobject][ordered]@{
            address = $Address; port = $Port; protocol = $Protocol; vantage = $Vantage
            result = "probe_error"; connected = $wasConnected; protocol_ok = $false
            duration_ms = [int]$stopwatch.ElapsedMilliseconds
            evidence = Protect-DiagnosticText $_.Exception.GetBaseException().Message 500
        }
    } finally {
        if ($null -ne $client) {
            $client.Dispose()
        }
    }
}

function Read-ExternalProtocolEvidence {
    param(
        [AllowNull()][string]$Path,
        [int]$ExpectedSshPort,
        [int]$ExpectedHttpPort,
        [System.Net.IPAddress]$ExpectedAddress,
        [int]$MaximumAgeMinutes
    )

    $empty = [pscustomobject][ordered]@{
        available = $false
        generated_at_utc = ""
        target_address = ""
        probes = @()
        error = "Independent external protocol evidence was not supplied"
    }
    if (-not $Path) {
        return $empty
    }

    try {
        $resolved = Resolve-Path -LiteralPath $Path -ErrorAction Stop
        $file = Get-Item -LiteralPath $resolved -ErrorAction Stop
        if ($file.Length -gt 65536) {
            throw "External evidence exceeds the 64 KiB safety limit"
        }
        $document = Get-Content -LiteralPath $resolved -Raw -Encoding UTF8 | ConvertFrom-Json
        if ([string](Get-ObjectField $document "schema_version" "") -ne $script:ExternalEvidenceSchema) {
            throw "External evidence schema_version is invalid"
        }
        $targetAddress = [string](Get-ObjectField $document "target_address" "")
        if ($ExpectedAddress -and $targetAddress -ne $ExpectedAddress.ToString()) {
            throw "External evidence target_address does not match PublicAddress"
        }
        $generatedText = [string](Get-ObjectField $document "generated_at_utc" "")
        $generatedAt = [DateTimeOffset]::MinValue
        if (-not [DateTimeOffset]::TryParse($generatedText, [ref]$generatedAt)) {
            throw "External evidence generated_at_utc is invalid"
        }
        $age = [DateTimeOffset]::UtcNow - $generatedAt.ToUniversalTime()
        if ($age.TotalMinutes -gt $MaximumAgeMinutes -or $age.TotalMinutes -lt -5) {
            throw "External evidence is stale or has an invalid future timestamp"
        }
        $probes = @()
        foreach ($probe in @(Get-ObjectField $document "probes" @())) {
            $protocol = [string](Get-ObjectField $probe "protocol" "")
            $port = [int](Get-ObjectField $probe "port" -1)
            if (($protocol -eq "ssh" -and $port -ne $ExpectedSshPort) -or
                ($protocol -eq "http" -and $port -ne $ExpectedHttpPort) -or
                $protocol -notin @("ssh", "http")) {
                continue
            }
            $probes += [pscustomobject][ordered]@{
                protocol = $protocol
                port = $port
                result = Protect-DiagnosticText (Get-ObjectField $probe "result" "unknown") 100
                connected = [bool](Get-ObjectField $probe "connected" $false)
                protocol_ok = [bool](Get-ObjectField $probe "protocol_ok" $false)
                duration_ms = [int](Get-ObjectField $probe "duration_ms" 0)
                evidence = Protect-DiagnosticText (Get-ObjectField $probe "evidence" "") 512
            }
        }
        if (@($probes | Where-Object { $_.protocol -eq "ssh" }).Count -ne 1 -or
            @($probes | Where-Object { $_.protocol -eq "http" }).Count -ne 1) {
            throw "External evidence must contain one SSH and one HTTP control probe"
        }
        return [pscustomobject][ordered]@{
            available = $true
            generated_at_utc = Protect-DiagnosticText $generatedText 100
            target_address = Protect-DiagnosticText $targetAddress 100
            probes = @($probes)
            error = ""
        }
    } catch {
        return [pscustomobject][ordered]@{
            available = $false
            generated_at_utc = ""
            target_address = ""
            probes = @()
            error = Protect-DiagnosticText $_.Exception.Message 1000
        }
    }
}

function Invoke-ExternalTcpCheck {
    param(
        [System.Net.IPAddress]$Address,
        [int[]]$Ports,
        [bool]$Enabled,
        [int]$TimeoutMs
    )

    $snapshot = [ordered]@{
        enabled = $Enabled
        provider = "portchecker.io"
        scope = "independent_external_tcp_only"
        results = @()
        error = ""
    }
    if (-not $Enabled) {
        $snapshot.error = "External check was not requested"
        return [pscustomobject]$snapshot
    }
    if (-not $Address) {
        $snapshot.error = "PublicAddress is required for an external check"
        return [pscustomobject]$snapshot
    }

    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        $body = @{
            host = $Address.ToString()
            ports = @($Ports)
        } | ConvertTo-Json -Compress
        $timeoutSeconds = [Math]::Max(2, [Math]::Ceiling($TimeoutMs / 1000.0) + 5)
        $response = Invoke-RestMethod -Uri "https://portchecker.io/api/query" -Method Post -ContentType "application/json" -Body $body -TimeoutSec $timeoutSeconds
        if ($response.error) {
            $snapshot.error = Protect-DiagnosticText $response.msg 500
        } else {
            $snapshot.results = @($response.check | ForEach-Object {
                [pscustomobject][ordered]@{
                    port = [int]$_.port
                    tcp_open = [bool]$_.status
                }
            })
        }
    } catch {
        $snapshot.error = Protect-DiagnosticText $_.Exception.Message 1000
    }
    return [pscustomobject]$snapshot
}

function Test-RemoteAddressMatch {
    param(
        [AllowNull()]$RemoteAddresses,
        [System.Net.IPAddress]$ClientIp
    )

    $sawScopedAddress = $false
    foreach ($entryObject in @($RemoteAddresses)) {
        $entry = [string]$entryObject
        if ($entry -in @("Any", "*", "0.0.0.0/0", "::/0")) {
            return "match"
        }
        if (-not $entry -or $entry -eq "Unknown") {
            continue
        }
        $sawScopedAddress = $true
        if (-not $ClientIp) {
            continue
        }
        if ($entry -eq $ClientIp.ToString()) {
            return "match"
        }
        if ($entry -match '^([^/]+)/([0-9]{1,3})$') {
            try {
                $network = [System.Net.IPAddress]::Parse($Matches[1])
                $prefix = [int]$Matches[2]
                $networkBytes = $network.GetAddressBytes()
                $clientBytes = $ClientIp.GetAddressBytes()
                if ($networkBytes.Length -ne $clientBytes.Length -or $prefix -gt ($networkBytes.Length * 8)) {
                    continue
                }
                $fullBytes = [Math]::Floor($prefix / 8)
                $remainder = $prefix % 8
                $matches = $true
                for ($index = 0; $index -lt $fullBytes; $index++) {
                    if ($networkBytes[$index] -ne $clientBytes[$index]) { $matches = $false; break }
                }
                if ($matches -and $remainder -gt 0) {
                    $mask = (0xFF -shl (8 - $remainder)) -band 0xFF
                    if (($networkBytes[$fullBytes] -band $mask) -ne ($clientBytes[$fullBytes] -band $mask)) {
                        $matches = $false
                    }
                }
                if ($matches) { return "match" }
            } catch {
                continue
            }
        }
    }
    if (-not $ClientIp -and $sawScopedAddress) {
        return "unknown"
    }
    if (-not $sawScopedAddress) {
        return "unknown"
    }
    return "no_match"
}

function Get-FirewallRuleApplicability {
    param(
        [Parameter(Mandatory = $true)]$Rule,
        [int]$Port,
        [System.Net.IPAddress]$ClientIp,
        [string[]]$ActiveProfiles,
        [string[]]$ActiveInterfaceAliases,
        [string[]]$ListenerProcessPaths
    )

    if (-not (Test-PortSpecificationIncludes -Specification (Get-ObjectField $Rule "local_port" "") -Port $Port)) {
        return "no_match"
    }
    if (-not [bool](Get-ObjectField $Rule "enabled" $false) -or
        [string](Get-ObjectField $Rule "direction" "") -ne "Inbound") {
        return "no_match"
    }

    $states = New-Object System.Collections.Generic.List[string]
    $states.Add((Test-RemoteAddressMatch (Get-ObjectField $Rule "remote_address" @()) $ClientIp))

    $program = [string](Get-ObjectField $Rule "program" "Unknown")
    if ($program -in @("Any", "*", "")) {
        $states.Add("match")
    } elseif ($program -eq "Unknown" -or $ListenerProcessPaths.Count -eq 0) {
        $states.Add("unknown")
    } elseif (@($ListenerProcessPaths | Where-Object { $_ -and $_ -ieq $program }).Count -gt 0) {
        $states.Add("match")
    } else {
        $states.Add("no_match")
    }

    $profile = [string](Get-ObjectField $Rule "profile" "Unknown")
    if ($profile -in @("Any", "*", "")) {
        $states.Add("match")
    } elseif ($profile -eq "Unknown" -or $ActiveProfiles.Count -eq 0) {
        $states.Add("unknown")
    } else {
        $profileTokens = @($profile -split '[, ]+' | Where-Object { $_ })
        if (@($profileTokens | Where-Object { $ActiveProfiles -contains $_ }).Count -gt 0) {
            $states.Add("match")
        } else {
            $states.Add("no_match")
        }
    }

    $interfaceAliases = @((Get-ObjectField $Rule "interface_alias" @()) | ForEach-Object { [string]$_ })
    if (@($interfaceAliases | Where-Object { $_ -in @("Any", "*", "") }).Count -gt 0) {
        $states.Add("match")
    } elseif (@($interfaceAliases | Where-Object { $_ -eq "Unknown" }).Count -gt 0 -or $ActiveInterfaceAliases.Count -eq 0) {
        $states.Add("unknown")
    } elseif (@($interfaceAliases | Where-Object { $ActiveInterfaceAliases -contains $_ }).Count -gt 0) {
        $states.Add("match")
    } else {
        $states.Add("no_match")
    }

    $serviceName = [string](Get-ObjectField $Rule "service_name" "Unknown")
    if ($serviceName -in @("Any", "*", "")) {
        $states.Add("match")
    } elseif ($serviceName -eq "Unknown") {
        $states.Add("unknown")
    } else {
        # A service-scoped rule cannot be attributed to the Docker backend
        # listener without additional service ownership evidence.
        $states.Add("unknown")
    }

    $localAddresses = @((Get-ObjectField $Rule "local_address" @()) | ForEach-Object { [string]$_ })
    if (@($localAddresses | Where-Object { $_ -in @("Any", "*", "0.0.0.0/0", "::/0", "") }).Count -gt 0) {
        $states.Add("match")
    } else {
        $states.Add("unknown")
    }

    # EnforcementStatus=NotApplicable alone is not evidence that a rule is
    # disabled. Only Full is treated as conclusive; all other values remain
    # unknown rather than becoming a false pass or fail.
    if ([string](Get-ObjectField $Rule "enforcement_status" "Unknown") -eq "Full") {
        $states.Add("match")
    } else {
        $states.Add("unknown")
    }

    if ($states.Contains("no_match")) {
        return "no_match"
    }
    if ($states.Contains("unknown")) {
        return "unknown"
    }
    return "match"
}

function ConvertTo-SafeDiagnosticSnapshot {
    param([Parameter(Mandatory = $true)]$Snapshot)

    $inputs = Get-ObjectField $Snapshot "inputs" @{}
    $docker = Get-ObjectField $Snapshot "docker" @{}
    $firewall = Get-ObjectField $Snapshot "firewall" @{}
    $network = Get-ObjectField $Snapshot "network" @{}
    $wfp = Get-ObjectField $Snapshot "wfp_drops" @{}
    $hostCollection = Get-ObjectField $Snapshot "host_listener_collection" @{}
    $externalTcp = Get-ObjectField $Snapshot "external_tcp_check" @{}
    $externalProtocol = Get-ObjectField $Snapshot "external_protocol_evidence" @{}

    return [pscustomobject][ordered]@{
        schema_version = $script:ReportSchema
        generated_at_utc = Protect-DiagnosticText (Get-ObjectField $Snapshot "generated_at_utc" "") 100
        is_administrator = [bool](Get-ObjectField $Snapshot "is_administrator" $false)
        inputs = [pscustomobject][ordered]@{
            container_name = Protect-DiagnosticText (Get-ObjectField $inputs "container_name" "") 256
            ssh_host_port = [int](Get-ObjectField $inputs "ssh_host_port" 2000)
            ssh_container_port = [int](Get-ObjectField $inputs "ssh_container_port" 22)
            http_host_port = [int](Get-ObjectField $inputs "http_host_port" 2100)
            http_container_port = [int](Get-ObjectField $inputs "http_container_port" 80)
            lan_address = Protect-DiagnosticText (Get-ObjectField $inputs "lan_address" "") 100
            public_address = Protect-DiagnosticText (Get-ObjectField $inputs "public_address" "") 100
            expected_client_ip = Protect-DiagnosticText (Get-ObjectField $inputs "expected_client_ip" "") 100
            external_port_check_requested = [bool](Get-ObjectField $inputs "external_port_check_requested" $false)
        }
        docker = [pscustomobject][ordered]@{
            available = [bool](Get-ObjectField $docker "available" $false)
            context = Protect-DiagnosticText (Get-ObjectField $docker "context" "") 500
            version = Protect-DiagnosticText (Get-ObjectField $docker "version" "") 500
            container_exists = [bool](Get-ObjectField $docker "container_exists" $false)
            container_status = Protect-DiagnosticText (Get-ObjectField $docker "container_status" "unknown") 100
            container_running = [bool](Get-ObjectField $docker "container_running" $false)
            health = Protect-DiagnosticText (Get-ObjectField $docker "health" "unknown") 100
            restart_count = Get-ObjectField $docker "restart_count" $null
            network_mode = Protect-DiagnosticText (Get-ObjectField $docker "network_mode" "") 100
            listener_query_succeeded = [bool](Get-ObjectField $docker "listener_query_succeeded" $true)
            listener_format = Protect-DiagnosticText (Get-ObjectField $docker "listener_format" "fixture") 50
            bindings = @((Get-ObjectField $docker "bindings" @()) | Select-Object -First 64 | ForEach-Object {
                [pscustomobject][ordered]@{
                    container_port = [int](Get-ObjectField $_ "container_port" -1)
                    protocol = Protect-DiagnosticText (Get-ObjectField $_ "protocol" "") 20
                    host_ip = Protect-DiagnosticText (Get-ObjectField $_ "host_ip" "") 100
                    host_port = [int](Get-ObjectField $_ "host_port" -1)
                }
            })
            container_listeners = @((Get-ObjectField $docker "container_listeners" @()) | Select-Object -First 64 | ForEach-Object { Protect-DiagnosticText $_ 1000 })
            sshd_effective = @((Get-ObjectField $docker "sshd_effective" @()) | Select-Object -First 32 | ForEach-Object { Protect-DiagnosticText $_ 500 })
            errors = @((Get-ObjectField $docker "errors" @()) | Select-Object -First 32 | ForEach-Object { Protect-DiagnosticText $_ 1000 })
        }
        host_listeners = @((Get-ObjectField $Snapshot "host_listeners" @()) | Select-Object -First 64 | ForEach-Object {
            [pscustomobject][ordered]@{
                local_address = Protect-DiagnosticText (Get-ObjectField $_ "local_address" "") 100
                local_port = [int](Get-ObjectField $_ "local_port" -1)
                owning_process_id = [int](Get-ObjectField $_ "owning_process_id" 0)
                process_name = Protect-DiagnosticText (Get-ObjectField $_ "process_name" "") 200
                process_path = Protect-DiagnosticText (Get-ObjectField $_ "process_path" "") 2048
            }
        })
        host_listener_collection = [pscustomobject][ordered]@{
            available = [bool](Get-ObjectField $hostCollection "available" $true)
            query_succeeded = [bool](Get-ObjectField $hostCollection "query_succeeded" $true)
            errors = @((Get-ObjectField $hostCollection "errors" @()) | Select-Object -First 16 | ForEach-Object { Protect-DiagnosticText $_ 500 })
        }
        firewall = [pscustomobject][ordered]@{
            available = [bool](Get-ObjectField $firewall "available" $false)
            profiles = @((Get-ObjectField $firewall "profiles" @()) | Select-Object -First 8 | ForEach-Object {
                [pscustomobject][ordered]@{
                    name = Protect-DiagnosticText (Get-ObjectField $_ "name" "") 50
                    enabled = [bool](Get-ObjectField $_ "enabled" $false)
                    default_inbound_action = Protect-DiagnosticText (Get-ObjectField $_ "default_inbound_action" "") 50
                    allow_inbound_rules = Protect-DiagnosticText (Get-ObjectField $_ "allow_inbound_rules" "") 50
                    allow_local_firewall_rules = Protect-DiagnosticText (Get-ObjectField $_ "allow_local_firewall_rules" "") 50
                    log_allowed = [bool](Get-ObjectField $_ "log_allowed" $false)
                    log_blocked = [bool](Get-ObjectField $_ "log_blocked" $false)
                }
            })
            rules = @((Get-ObjectField $firewall "rules" @()) | Select-Object -First 256 | ForEach-Object {
                [pscustomobject][ordered]@{
                    display_name = Protect-DiagnosticText (Get-ObjectField $_ "display_name" "") 500
                    enabled = [bool](Get-ObjectField $_ "enabled" $false)
                    direction = Protect-DiagnosticText (Get-ObjectField $_ "direction" "") 50
                    action = Protect-DiagnosticText (Get-ObjectField $_ "action" "") 50
                    profile = Protect-DiagnosticText (Get-ObjectField $_ "profile" "Unknown") 100
                    enforcement_status = Protect-DiagnosticText (Get-ObjectField $_ "enforcement_status" "Unknown") 100
                    policy_store_source_type = Protect-DiagnosticText (Get-ObjectField $_ "policy_store_source_type" "") 100
                    edge_traversal_policy = Protect-DiagnosticText (Get-ObjectField $_ "edge_traversal_policy" "") 100
                    protocol = Protect-DiagnosticText (Get-ObjectField $_ "protocol" "") 50
                    local_port = Protect-DiagnosticText (Get-ObjectField $_ "local_port" "") 100
                    remote_address = @((Get-ObjectField $_ "remote_address" @()) | Select-Object -First 32 | ForEach-Object { Protect-DiagnosticText $_ 200 })
                    local_address = @((Get-ObjectField $_ "local_address" @()) | Select-Object -First 32 | ForEach-Object { Protect-DiagnosticText $_ 200 })
                    program = Protect-DiagnosticText (Get-ObjectField $_ "program" "Unknown") 2048
                    interface_alias = @((Get-ObjectField $_ "interface_alias" @()) | Select-Object -First 32 | ForEach-Object { Protect-DiagnosticText $_ 500 })
                    interface_type = Protect-DiagnosticText (Get-ObjectField $_ "interface_type" "Unknown") 100
                    service_name = Protect-DiagnosticText (Get-ObjectField $_ "service_name" "Unknown") 500
                }
            })
            errors = @((Get-ObjectField $firewall "errors" @()) | Select-Object -First 32 | ForEach-Object { Protect-DiagnosticText $_ 1000 })
        }
        network = [pscustomobject][ordered]@{
            addresses = @((Get-ObjectField $network "addresses" @()) | Select-Object -First 32 | ForEach-Object {
                [pscustomobject][ordered]@{
                    interface_alias = Protect-DiagnosticText (Get-ObjectField $_ "interface_alias" "") 500
                    interface_index = [int](Get-ObjectField $_ "interface_index" 0)
                    ip_address = Protect-DiagnosticText (Get-ObjectField $_ "ip_address" "") 100
                    prefix_length = [int](Get-ObjectField $_ "prefix_length" 0)
                }
            })
            profiles = @((Get-ObjectField $network "profiles" @()) | Select-Object -First 32 | ForEach-Object {
                [pscustomobject][ordered]@{
                    interface_alias = Protect-DiagnosticText (Get-ObjectField $_ "interface_alias" "") 500
                    interface_index = [int](Get-ObjectField $_ "interface_index" 0)
                    network_category = Protect-DiagnosticText (Get-ObjectField $_ "network_category" "") 100
                    ipv4_connectivity = Protect-DiagnosticText (Get-ObjectField $_ "ipv4_connectivity" "") 100
                    ipv6_connectivity = Protect-DiagnosticText (Get-ObjectField $_ "ipv6_connectivity" "") 100
                }
            })
            default_routes = @((Get-ObjectField $network "default_routes" @()) | Select-Object -First 32 | ForEach-Object {
                [pscustomobject][ordered]@{
                    interface_index = [int](Get-ObjectField $_ "interface_index" 0)
                    next_hop = Protect-DiagnosticText (Get-ObjectField $_ "next_hop" "") 100
                    route_metric = [int](Get-ObjectField $_ "route_metric" 0)
                    interface_metric = [int](Get-ObjectField $_ "interface_metric" 0)
                    state = Protect-DiagnosticText (Get-ObjectField $_ "state" "") 100
                }
            })
            docker_services = @((Get-ObjectField $network "docker_services" @()) | Select-Object -First 16 | ForEach-Object {
                [pscustomobject][ordered]@{
                    name = Protect-DiagnosticText (Get-ObjectField $_ "name" "") 200
                    status = Protect-DiagnosticText (Get-ObjectField $_ "status" "") 100
                    start_type = Protect-DiagnosticText (Get-ObjectField $_ "start_type" "") 100
                }
            })
            portproxy = @((Get-ObjectField $network "portproxy" @()) | Select-Object -First 32 | ForEach-Object { Protect-DiagnosticText $_ 1000 })
            wsl_status = Protect-DiagnosticText (Get-ObjectField $network "wsl_status" "") 4000
            wsl_version = Protect-DiagnosticText (Get-ObjectField $network "wsl_version" "") 2000
            errors = @((Get-ObjectField $network "errors" @()) | Select-Object -First 32 | ForEach-Object { Protect-DiagnosticText $_ 1000 })
        }
        wfp_drops = [pscustomobject][ordered]@{
            available = [bool](Get-ObjectField $wfp "available" $false)
            audited_event_count = [int](Get-ObjectField $wfp "audited_event_count" 0)
            matching_drop_count = [int](Get-ObjectField $wfp "matching_drop_count" 0)
            expected_client_drop_count = [int](Get-ObjectField $wfp "expected_client_drop_count" 0)
            matching_allow_count = [int](Get-ObjectField $wfp "matching_allow_count" 0)
            expected_client_allow_count = [int](Get-ObjectField $wfp "expected_client_allow_count" 0)
            events = @((Get-ObjectField $wfp "events" @()) | Select-Object -First 256 | ForEach-Object {
                [pscustomobject][ordered]@{
                    event_id = [int](Get-ObjectField $_ "event_id" 0)
                    decision = Protect-DiagnosticText (Get-ObjectField $_ "decision" "") 20
                    time_created_utc = Protect-DiagnosticText (Get-ObjectField $_ "time_created_utc" "") 100
                    server_port = [int](Get-ObjectField $_ "server_port" 0)
                    flow_direction = Protect-DiagnosticText (Get-ObjectField $_ "flow_direction" "") 100
                    wfp_direction = Protect-DiagnosticText (Get-ObjectField $_ "wfp_direction" "") 100
                    application = Protect-DiagnosticText (Get-ObjectField $_ "application" "") 2048
                    remote_matches_expected_client = [bool](Get-ObjectField $_ "remote_matches_expected_client" $false)
                }
            })
            error = Protect-DiagnosticText (Get-ObjectField $wfp "error" "") 1000
        }
        probes = @((Get-ObjectField $Snapshot "probes" @()) | Select-Object -First 32 | ForEach-Object {
            [pscustomobject][ordered]@{
                address = Protect-DiagnosticText (Get-ObjectField $_ "address" "") 100
                port = [int](Get-ObjectField $_ "port" 0)
                protocol = Protect-DiagnosticText (Get-ObjectField $_ "protocol" "") 20
                vantage = Protect-DiagnosticText (Get-ObjectField $_ "vantage" "") 20
                result = Protect-DiagnosticText (Get-ObjectField $_ "result" "") 100
                connected = [bool](Get-ObjectField $_ "connected" $false)
                protocol_ok = [bool](Get-ObjectField $_ "protocol_ok" $false)
                duration_ms = [int](Get-ObjectField $_ "duration_ms" 0)
                evidence = Protect-DiagnosticText (Get-ObjectField $_ "evidence" "") 512
            }
        })
        external_tcp_check = [pscustomobject][ordered]@{
            enabled = [bool](Get-ObjectField $externalTcp "enabled" $false)
            provider = Protect-DiagnosticText (Get-ObjectField $externalTcp "provider" "") 100
            scope = Protect-DiagnosticText (Get-ObjectField $externalTcp "scope" "") 100
            results = @((Get-ObjectField $externalTcp "results" @()) | Select-Object -First 16 | ForEach-Object {
                [pscustomobject][ordered]@{
                    port = [int](Get-ObjectField $_ "port" 0)
                    tcp_open = [bool](Get-ObjectField $_ "tcp_open" $false)
                }
            })
            error = Protect-DiagnosticText (Get-ObjectField $externalTcp "error" "") 1000
        }
        external_protocol_evidence = [pscustomobject][ordered]@{
            available = [bool](Get-ObjectField $externalProtocol "available" $false)
            generated_at_utc = Protect-DiagnosticText (Get-ObjectField $externalProtocol "generated_at_utc" "") 100
            target_address = Protect-DiagnosticText (Get-ObjectField $externalProtocol "target_address" "") 100
            probes = @((Get-ObjectField $externalProtocol "probes" @()) | Select-Object -First 4 | ForEach-Object {
                [pscustomobject][ordered]@{
                    protocol = Protect-DiagnosticText (Get-ObjectField $_ "protocol" "") 20
                    port = [int](Get-ObjectField $_ "port" 0)
                    result = Protect-DiagnosticText (Get-ObjectField $_ "result" "") 100
                    connected = [bool](Get-ObjectField $_ "connected" $false)
                    protocol_ok = [bool](Get-ObjectField $_ "protocol_ok" $false)
                    duration_ms = [int](Get-ObjectField $_ "duration_ms" 0)
                    evidence = Protect-DiagnosticText (Get-ObjectField $_ "evidence" "") 512
                }
            })
            error = Protect-DiagnosticText (Get-ObjectField $externalProtocol "error" "") 1000
        }
    }
}

function New-Finding {
    param(
        [string]$Id,
        [string]$Layer,
        [ValidateSet("pass", "fail", "inconclusive", "info")][string]$Status,
        [string]$Summary,
        [string]$NextAction = ""
    )

    return [pscustomobject][ordered]@{
        id = $Id
        layer = $Layer
        status = $Status
        summary = Protect-DiagnosticText $Summary 2000
        next_action = Protect-DiagnosticText $NextAction 3000
    }
}

function Resolve-DiagnosticFindings {
    param([Parameter(Mandatory = $true)]$Snapshot)

    $findings = New-Object System.Collections.Generic.List[object]
    $inputs = Get-ObjectField $Snapshot "inputs" @{}
    $docker = Get-ObjectField $Snapshot "docker" @{}
    $listeners = @(Get-ObjectField $Snapshot "host_listeners" @())
    $listenerCollection = Get-ObjectField $Snapshot "host_listener_collection" ([pscustomobject]@{ available = $true; query_succeeded = $true })
    $firewall = Get-ObjectField $Snapshot "firewall" @{}
    $network = Get-ObjectField $Snapshot "network" @{}
    $probes = @(Get-ObjectField $Snapshot "probes" @())
    $external = Get-ObjectField $Snapshot "external_tcp_check" @{}
    $externalProtocol = Get-ObjectField $Snapshot "external_protocol_evidence" @{}
    $wfp = Get-ObjectField $Snapshot "wfp_drops" @{}

    $sshHost = [int](Get-ObjectField $inputs "ssh_host_port" 2000)
    $sshContainer = [int](Get-ObjectField $inputs "ssh_container_port" 22)
    $httpHost = [int](Get-ObjectField $inputs "http_host_port" 2100)
    $httpContainer = [int](Get-ObjectField $inputs "http_container_port" 80)
    $externalProtocolProbes = @(Get-ObjectField $externalProtocol "probes" @())
    $externalSshProbe = @($externalProtocolProbes | Where-Object {
        [string](Get-ObjectField $_ "protocol" "") -eq "ssh" -and
        [int](Get-ObjectField $_ "port" -1) -eq $sshHost
    } | Select-Object -First 1)
    $externalHttpProbe = @($externalProtocolProbes | Where-Object {
        [string](Get-ObjectField $_ "protocol" "") -eq "http" -and
        [int](Get-ObjectField $_ "port" -1) -eq $httpHost
    } | Select-Object -First 1)
    $externalSshPassed = $externalSshProbe.Count -eq 1 -and [bool](Get-ObjectField $externalSshProbe[0] "protocol_ok" $false)

    if (-not [bool](Get-ObjectField $Snapshot "is_administrator" $false)) {
        $findings.Add((New-Finding "host.admin" "windows" "inconclusive" "Not running as administrator; firewall, WFP, and process evidence may be incomplete." "Run the script from an elevated PowerShell window."))
    } else {
        $findings.Add((New-Finding "host.admin" "windows" "pass" "The diagnostic is running with administrator privileges."))
    }

    if (-not [bool](Get-ObjectField $docker "available" $false)) {
        $findings.Add((New-Finding "docker.available" "docker" "fail" "docker.exe is unavailable." "Install Docker Desktop or add docker.exe to PATH."))
        return $findings.ToArray()
    }
    $findings.Add((New-Finding "docker.available" "docker" "pass" "Docker CLI is available."))

    if (-not [bool](Get-ObjectField $docker "container_exists" $false)) {
        $findings.Add((New-Finding "docker.container" "docker" "fail" "The requested container does not exist or the Docker daemon could not inspect it." "Confirm the container name and Docker context."))
        return $findings.ToArray()
    }
    if (-not [bool](Get-ObjectField $docker "container_running" $false)) {
        $findings.Add((New-Finding "docker.container" "docker" "fail" "The requested container is not running." "Start the container and rerun the diagnostic."))
    } else {
        $findings.Add((New-Finding "docker.container" "docker" "pass" "The container is running."))
    }

    $bindings = @(Get-ObjectField $docker "bindings" @())
    foreach ($mapping in @(
        [pscustomobject]@{ Id = "docker.mapping.ssh"; Name = "SSH"; ContainerPort = $sshContainer; HostPort = $sshHost },
        [pscustomobject]@{ Id = "docker.mapping.http"; Name = "HTTP control"; ContainerPort = $httpContainer; HostPort = $httpHost }
    )) {
        $matches = @($bindings | Where-Object {
            [int](Get-ObjectField $_ "container_port" -1) -eq $mapping.ContainerPort -and
            [int](Get-ObjectField $_ "host_port" -1) -eq $mapping.HostPort -and
            [string](Get-ObjectField $_ "protocol" "tcp") -eq "tcp"
        })
        if ($matches.Count -eq 0) {
            $findings.Add((New-Finding $mapping.Id "docker" "fail" "$($mapping.Name) mapping $($mapping.HostPort):$($mapping.ContainerPort)/tcp is missing." "Recreate the container with the expected published port."))
            continue
        }
        $wildcard = @($matches | Where-Object { [string](Get-ObjectField $_ "host_ip" "") -in @("0.0.0.0", "::", "") })
        $intendedAddresses = @(
            [string](Get-ObjectField $inputs "lan_address" "")
            @((Get-ObjectField $network "addresses" @()) | ForEach-Object { [string](Get-ObjectField $_ "ip_address" "") })
        ) | Where-Object { $_ } | Select-Object -Unique
        $intendedSpecific = @($matches | Where-Object {
            $intendedAddresses -contains [string](Get-ObjectField $_ "host_ip" "")
        })
        $loopbackOnly = @($matches | Where-Object {
            [string](Get-ObjectField $_ "host_ip" "") -in @("127.0.0.1", "::1")
        }).Count -eq $matches.Count
        if ($wildcard.Count -gt 0) {
            $findings.Add((New-Finding $mapping.Id "docker" "pass" "$($mapping.Name) mapping is published on an externally scoped host address."))
        } elseif ($intendedSpecific.Count -gt 0) {
            $findings.Add((New-Finding $mapping.Id "docker" "pass" "$($mapping.Name) mapping is published on the intended active LAN address."))
        } elseif ($loopbackOnly) {
            $findings.Add((New-Finding $mapping.Id "docker" "fail" "$($mapping.Name) is published only to loopback." "Publish to the intended LAN address or 0.0.0.0/:: after applying firewall restrictions."))
        } else {
            $findings.Add((New-Finding $mapping.Id "docker" "inconclusive" "$($mapping.Name) is published to a specific address that is not confirmed as the intended active LAN address." "Verify the selected LAN address and active route before changing the mapping."))
        }
    }

    $containerListenerLines = @(Get-ObjectField $docker "container_listeners" @())
    $containerListenerQuerySucceeded = [bool](Get-ObjectField $docker "listener_query_succeeded" $true)
    $containerListenerFormat = [string](Get-ObjectField $docker "listener_format" "fixture")
    foreach ($service in @(
        [pscustomobject]@{ Id = "container.listener.ssh"; Name = "sshd"; Port = $sshContainer },
        [pscustomobject]@{ Id = "container.listener.http"; Name = "HTTP service"; Port = $httpContainer }
    )) {
        $match = @($containerListenerLines | Where-Object { [string]$_ -match (":" + $service.Port + "(?:\s|$)") })
        if ($match.Count -eq 0) {
            if (-not $containerListenerQuerySucceeded -or $containerListenerFormat -eq "proc") {
                $findings.Add((New-Finding $service.Id "container" "inconclusive" "$($service.Name) listener collection failed or used an unparsed /proc fallback for container port $($service.Port)." "Install ss/netstat in the diagnostic image or verify the listener inside the container."))
            } else {
                $findings.Add((New-Finding $service.Id "container" "fail" "$($service.Name) is not listening on container port $($service.Port)." "Check the service process inside the container."))
            }
        } else {
            $loopbackMatches = @($match | Where-Object {
                [string]$_ -match "(?:127\.0\.0\.1|\[::1\]|::1):$($service.Port)(?:\s|$)"
            })
            if ($loopbackMatches.Count -eq $match.Count) {
                $findings.Add((New-Finding $service.Id "container" "fail" "$($service.Name) listens only on container loopback, which Docker port forwarding cannot reach." "Bind the service to the container interface or wildcard address."))
            } else {
                $findings.Add((New-Finding $service.Id "container" "pass" "$($service.Name) is listening on a non-loopback container address."))
            }
        }
    }

    foreach ($service in @(
        [pscustomobject]@{ Id = "host.listener.ssh"; Name = "SSH"; Port = $sshHost },
        [pscustomobject]@{ Id = "host.listener.http"; Name = "HTTP control"; Port = $httpHost }
    )) {
        $portListeners = @($listeners | Where-Object { [int](Get-ObjectField $_ "local_port" -1) -eq $service.Port })
        if ($portListeners.Count -eq 0) {
            if (-not [bool](Get-ObjectField $listenerCollection "available" $true) -or
                -not [bool](Get-ObjectField $listenerCollection "query_succeeded" $true)) {
                $findings.Add((New-Finding $service.Id "windows" "inconclusive" "The Windows listener query was unavailable or failed for $($service.Name) host port $($service.Port)." "Rerun from an elevated PowerShell window before treating the listener as absent."))
            } else {
                $findings.Add((New-Finding $service.Id "windows" "fail" "No Windows listener exists for $($service.Name) host port $($service.Port)." "Check Docker Desktop port publishing and host port conflicts."))
            }
            continue
        }
        $externalScope = @($portListeners | Where-Object {
            [string](Get-ObjectField $_ "local_address" "") -notin @("127.0.0.1", "::1")
        })
        if ($externalScope.Count -eq 0) {
            $findings.Add((New-Finding $service.Id "windows" "fail" "$($service.Name) host port is listening only on loopback." "Publish the port to 0.0.0.0/:: or the intended LAN address."))
        } else {
            $owners = @($externalScope | ForEach-Object { [string](Get-ObjectField $_ "process_name" "unknown") } | Select-Object -Unique)
            $findings.Add((New-Finding $service.Id "windows" "pass" "$($service.Name) host port has an externally scoped listener owned by $($owners -join ', ')."))
        }
    }

    $rules = @(Get-ObjectField $firewall "rules" @())
    $clientIpString = [string](Get-ObjectField $inputs "expected_client_ip" "")
    $clientIp = $null
    if ($clientIpString) {
        [void][System.Net.IPAddress]::TryParse($clientIpString, [ref]$clientIp)
    }
    $sshRules = @($rules | Where-Object {
        Test-PortSpecificationIncludes -Specification (Get-ObjectField $_ "local_port" "") -Port $sshHost
    })
    $activeProfiles = @((Get-ObjectField $network "profiles" @()) | ForEach-Object {
        $category = [string](Get-ObjectField $_ "network_category" "")
        if ($category -eq "DomainAuthenticated") { "Domain" } elseif ($category) { $category }
    } | Select-Object -Unique)
    $activeInterfaceAliases = @((Get-ObjectField $network "profiles" @()) | ForEach-Object {
        [string](Get-ObjectField $_ "interface_alias" "")
    } | Where-Object { $_ } | Select-Object -Unique)
    $listenerProcessPaths = @($listeners | Where-Object {
        [int](Get-ObjectField $_ "local_port" -1) -eq $sshHost -and
        [string](Get-ObjectField $_ "local_address" "") -notin @("127.0.0.1", "::1")
    } | ForEach-Object { [string](Get-ObjectField $_ "process_path" "") } | Where-Object { $_ } | Select-Object -Unique)
    $evaluatedRules = @($sshRules | ForEach-Object {
        [pscustomobject]@{
            rule = $_
            applicability = Get-FirewallRuleApplicability -Rule $_ -Port $sshHost -ClientIp $clientIp -ActiveProfiles $activeProfiles -ActiveInterfaceAliases $activeInterfaceAliases -ListenerProcessPaths $listenerProcessPaths
        }
    })
    $matchingBlocks = @($evaluatedRules | Where-Object {
        $_.applicability -eq "match" -and [string](Get-ObjectField $_.rule "action" "") -eq "Block"
    })
    $matchingAllows = @($evaluatedRules | Where-Object {
        $_.applicability -eq "match" -and [string](Get-ObjectField $_.rule "action" "") -eq "Allow"
    })
    $ambiguousRules = @($evaluatedRules | Where-Object { $_.applicability -eq "unknown" })
    if ($matchingBlocks.Count -gt 0) {
        $findings.Add((New-Finding "firewall.ssh" "windows-firewall" "fail" "A fully applicable ActiveStore inbound block rule matches SSH host port $sshHost and the tested client." "Review the matching rule before changing Docker or upstream NAT."))
    } elseif ($matchingAllows.Count -gt 0) {
        $scopeNote = if ($clientIp) { " and the expected client address" } else { "" }
        $findings.Add((New-Finding "firewall.ssh" "windows-firewall" "pass" "A fully applicable ActiveStore inbound allow rule matches SSH host port $sshHost$scopeNote."))
    } elseif ($externalSshPassed) {
        $findings.Add((New-Finding "firewall.ssh" "windows-firewall" "info" "No fully attributable rule was found, but controlled external SSH protocol evidence proves that this client path was allowed at capture time."))
    } elseif ($ambiguousRules.Count -gt 0) {
        $findings.Add((New-Finding "firewall.ssh" "windows-firewall" "inconclusive" "$($ambiguousRules.Count) ActiveStore rule candidate(s) matched the port but could not be fully attributed across client, profile, program, interface, service, local-address, and enforcement scopes." "Use the controlled external probe and WFP evidence; do not infer from EdgeTraversalPolicy or EnforcementStatus alone."))
    } else {
        $findings.Add((New-Finding "firewall.ssh" "windows-firewall" "inconclusive" "No explicit matching ActiveStore allow rule was confirmed for SSH host port $sshHost. Application-scoped or upstream policy may still allow it." "Review Docker backend application rules and the active firewall profile."))
    }

    $wfpCount = [int](Get-ObjectField $wfp "matching_drop_count" 0)
    $expectedWfpCount = [int](Get-ObjectField $wfp "expected_client_drop_count" 0)
    $wfpAllowCount = [int](Get-ObjectField $wfp "matching_allow_count" 0)
    $expectedWfpAllowCount = [int](Get-ObjectField $wfp "expected_client_allow_count" 0)
    if ($expectedWfpCount -gt 0) {
        $findings.Add((New-Finding "wfp.drop" "windows-firewall" "fail" "Windows Filtering Platform recorded $expectedWfpCount drop event(s) for the expected client on a tested port." "Identify the matching WFP filter or security product before changing Docker."))
    } elseif ($wfpCount -gt 0) {
        $findings.Add((New-Finding "wfp.drop" "windows-firewall" "info" "Windows Filtering Platform recorded $wfpCount drop event(s) for tested ports, but they were not tied to the expected client."))
    } else {
        $findings.Add((New-Finding "wfp.drop" "windows-firewall" "info" "No matching WFP drop was observed. Audit policy may be disabled, so this is supplemental evidence rather than proof of allowance." "Reproduce an external connection while this diagnostic is run if deeper attribution is required."))
    }
    if ($expectedWfpAllowCount -gt 0) {
        $findings.Add((New-Finding "wfp.allow" "windows-firewall" "pass" "Windows Filtering Platform recorded $expectedWfpAllowCount allow event(s) for the expected client on a tested inbound or outbound server-port flow."))
    } elseif ($wfpAllowCount -gt 0) {
        $findings.Add((New-Finding "wfp.allow" "windows-firewall" "info" "WFP recorded $wfpAllowCount allow event(s) on tested server ports, but none was tied to the expected client."))
    } else {
        $findings.Add((New-Finding "wfp.allow" "windows-firewall" "info" "No matching WFP allow event was available. Connection auditing may be disabled; the script does not alter audit policy."))
    }

    foreach ($vantage in @("loopback", "lan")) {
        foreach ($protocol in @("ssh", "http")) {
            $matchingProbes = @($probes | Where-Object {
                [string](Get-ObjectField $_ "vantage" "") -eq $vantage -and
                [string](Get-ObjectField $_ "protocol" "") -eq $protocol
            })
            $id = "probe.$vantage.$protocol"
            if ($matchingProbes.Count -eq 0) {
                $findings.Add((New-Finding $id "protocol" "inconclusive" "No $vantage $protocol protocol probe was available."))
                continue
            }
            $successful = @($matchingProbes | Where-Object { [bool](Get-ObjectField $_ "protocol_ok" $false) })
            if ($successful.Count -gt 0) {
                $findings.Add((New-Finding $id "protocol" "pass" "At least one $vantage $protocol probe returned the expected protocol response."))
            } else {
                $results = @($matchingProbes | ForEach-Object { [string](Get-ObjectField $_ "result" "unknown") } | Select-Object -Unique)
                $summary = "The $vantage $protocol probe did not return the expected protocol response: $($results -join ', ')."
                $nextAction = "Compare SSH and HTTP control results to isolate a port-specific forwarding or service problem."
                if ($protocol -eq "ssh" -and @($results | Where-Object { $_ -in @("closed_without_banner", "reset_before_banner") }).Count -gt 0) {
                    $limits = @((Get-ObjectField $docker "sshd_effective" @()) | Where-Object { $_ -match '^(maxstartups|persourcemaxstartups|persourcenetblocksize)\s' })
                    $limitText = if ($limits.Count -gt 0) { " Effective sshd limits: $($limits -join '; ')." } else { "" }
                    $summary = "TCP reached the $vantage SSH endpoint, then closed or reset before a valid banner.$limitText"
                    $nextAction = "Correlate sshd pre-auth saturation/MaxStartups, endpoint-security logs, and Docker backend forwarding at the same timestamp."
                } elseif (@($results | Where-Object { $_ -in @("tcp_refused", "tcp_unreachable", "tcp_timeout") }).Count -gt 0) {
                    $nextAction = "Investigate the listener, route, firewall, or Docker published-port path before sshd authentication."
                } elseif ($results -contains "unexpected_protocol") {
                    $nextAction = "Verify that the published port and VIP target the intended SSH service rather than another protocol."
                }
                $findings.Add((New-Finding $id "protocol" "fail" $summary $nextAction))
            }
        }
    }

    $hairpinSsh = @($probes | Where-Object {
        [string](Get-ObjectField $_ "vantage" "") -eq "hairpin" -and
        [string](Get-ObjectField $_ "protocol" "") -eq "ssh"
    })
    if ($hairpinSsh.Count -gt 0) {
        $hairpinResult = @($hairpinSsh | ForEach-Object { [string](Get-ObjectField $_ "result" "unknown") } | Select-Object -Unique)
        $findings.Add((New-Finding "probe.hairpin.ssh" "protocol" "info" "Same-host public-address SSH result: $($hairpinResult -join ', '). This is a NAT hairpin test, not independent internet evidence."))
    }

    $externalEnabled = [bool](Get-ObjectField $external "enabled" $false)
    $externalResults = @(Get-ObjectField $external "results" @())
    if (-not $externalEnabled) {
        $findings.Add((New-Finding "external.tcp" "external" "info" "Third-party external TCP checking was not requested. Controlled protocol evidence is preferred for source-scoped firewall rules."))
    } elseif ($externalResults.Count -eq 0) {
        $findings.Add((New-Finding "external.tcp" "external" "inconclusive" "The independent external TCP check returned no usable result." "Review network access to the external checker or test from a separate trusted host."))
    } else {
        $externalSsh = @($externalResults | Where-Object { [int](Get-ObjectField $_ "port" -1) -eq $sshHost })
        if ($externalSsh.Count -gt 0 -and [bool](Get-ObjectField $externalSsh[0] "tcp_open" $false)) {
            $findings.Add((New-Finding "external.tcp" "external" "pass" "An independent service confirmed that public TCP port $sshHost accepts connections. This does not prove an SSH banner or authentication."))
        } else {
            $findings.Add((New-Finding "external.tcp" "external" "inconclusive" "The third-party checker did not find public TCP port $sshHost open. Its source address may be outside the scoped firewall allowance, so this is not proof of an upstream failure." "Use -ExternalProbeOnly from the expected client network and import that result with -ExternalEvidencePath."))
        }
    }

    if (-not [bool](Get-ObjectField $externalProtocol "available" $false) -or $externalSshProbe.Count -ne 1 -or $externalHttpProbe.Count -ne 1) {
        $findings.Add((New-Finding "external.ssh-protocol" "external" "inconclusive" "Controlled external SSH and HTTP protocol evidence was not supplied. This desktop cannot independently prove its own public path." "Run this script with -ExternalProbeOnly on the expected external client, then import the JSON with -ExternalEvidencePath."))
    } else {
        $sshResult = [string](Get-ObjectField $externalSshProbe[0] "result" "unknown")
        $httpResult = [string](Get-ObjectField $externalHttpProbe[0] "result" "unknown")
        $httpPassed = [bool](Get-ObjectField $externalHttpProbe[0] "protocol_ok" $false)
        if ($externalSshPassed) {
            $findings.Add((New-Finding "external.ssh-protocol" "external" "pass" "A controlled external client received a valid SSH banner on public port $sshHost."))
        } elseif ($httpPassed -and [bool](Get-ObjectField $externalSshProbe[0] "connected" $false)) {
            $findings.Add((New-Finding "external.ssh-protocol" "external" "fail" "The controlled external client reached TCP $sshHost but SSH failed before a valid banner ($sshResult), while HTTP control succeeded ($httpResult). The fault is port/protocol-specific after upstream reachability." "Correlate the same timestamp with WFP events, endpoint-security logs, Docker backend activity, and sshd pre-auth limits."))
        } elseif ($httpPassed) {
            $findings.Add((New-Finding "external.ssh-protocol" "external" "fail" "HTTP control succeeded externally but SSH did not reach a valid protocol response ($sshResult)." "Inspect the port-2000 VIP/rule scope, WFP, and Docker backend forwarding."))
        } else {
            $findings.Add((New-Finding "external.ssh-protocol" "external" "fail" "Neither controlled external probe produced the expected protocol response (SSH=$sshResult, HTTP=$httpResult). The failure is confirmed but cannot yet be isolated to a port-specific layer." "Check the external client's source IP, upstream VIP/DNAT, and host reachability together."))
        }
        if ($httpPassed) {
            $findings.Add((New-Finding "external.http-control" "external" "pass" "The controlled external HTTP probe returned a valid HTTP status on public port $httpHost."))
        } else {
            $findings.Add((New-Finding "external.http-control" "external" "fail" "The controlled external HTTP probe did not return a valid HTTP status ($httpResult)."))
        }
    }

    return $findings.ToArray()
}

function Write-AtomicUtf8File {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Content
    )

    $temporary = $Path + ".tmp-" + [Guid]::NewGuid().ToString("N")
    $encoding = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($temporary, $Content, $encoding)
    [System.IO.File]::Move($temporary, $Path)
}

function Convert-ReportToMarkdown {
    param([Parameter(Mandatory = $true)]$Report)

    $builder = New-Object System.Text.StringBuilder
    [void]$builder.AppendLine("# BSIDE Docker Desktop ingress diagnostic")
    [void]$builder.AppendLine("")
    [void]$builder.AppendLine("- Generated (UTC): $($Report.generated_at_utc)")
    [void]$builder.AppendLine("- Overall status: **$($Report.summary.status)**")
    [void]$builder.AppendLine("- Exit code: $($Report.summary.exit_code)")
    [void]$builder.AppendLine("- Container: $($Report.snapshot.inputs.container_name)")
    [void]$builder.AppendLine("- SSH mapping: $($Report.snapshot.inputs.ssh_host_port):$($Report.snapshot.inputs.ssh_container_port)/tcp")
    [void]$builder.AppendLine("- HTTP control mapping: $($Report.snapshot.inputs.http_host_port):$($Report.snapshot.inputs.http_container_port)/tcp")
    [void]$builder.AppendLine("")
    [void]$builder.AppendLine("The same-host public-address probe is NAT hairpin evidence only. External TCP checking does not prove the SSH protocol banner or authentication.")
    [void]$builder.AppendLine("")
    [void]$builder.AppendLine("| ID | Layer | Status | Summary | Next action |")
    [void]$builder.AppendLine("|---|---|---|---|---|")
    foreach ($finding in @($Report.findings)) {
        $summary = ([string]$finding.summary).Replace("|", "\|").Replace("`r", " ").Replace("`n", " ")
        $nextAction = ([string]$finding.next_action).Replace("|", "\|").Replace("`r", " ").Replace("`n", " ")
        [void]$builder.AppendLine("| $($finding.id) | $($finding.layer) | $($finding.status) | $summary | $nextAction |")
    }
    [void]$builder.AppendLine("")
    [void]$builder.AppendLine("## Privacy and safety")
    [void]$builder.AppendLine("")
    [void]$builder.AppendLine("The script is read-only except for these report files. It does not collect Docker environment variables, Compose configuration, mounts, raw container logs, process command lines, Wi-Fi credentials, or authentication secrets.")
    return $builder.ToString()
}

function Get-DiagnosticExitCode {
    param([object[]]$Findings)

    if (@($Findings | Where-Object { $_.status -eq "fail" }).Count -gt 0) {
        return 1
    }
    if (@($Findings | Where-Object { $_.status -eq "inconclusive" }).Count -gt 0) {
        return 2
    }
    return 0
}

function Get-DiagnosticPlan {
    return @(
        "host.admin",
        "docker.available",
        "docker.container",
        "docker.mapping.ssh",
        "docker.mapping.http",
        "container.listener.ssh",
        "container.listener.http",
        "host.listener.ssh",
        "host.listener.http",
        "firewall.ssh",
        "wfp.drop",
        "wfp.allow",
        "probe.loopback.ssh",
        "probe.loopback.http",
        "probe.lan.ssh",
        "probe.lan.http",
        "probe.hairpin.ssh",
        "external.tcp",
        "external.ssh-protocol",
        "external.http-control"
    )
}

if ($LibraryMode) {
    return
}

if ($PlanOnly) {
    Write-Host "Read-only checks that would run:"
    Get-DiagnosticPlan | ForEach-Object { Write-Host ("- " + $_) }
    Write-Host "No Docker, WSL, firewall, network, or file operation was performed."
    exit 0
}

if ($ExternalProbeOnly) {
    try {
        if (-not $PublicAddress) {
            [Console]::Error.WriteLine("-PublicAddress is required with -ExternalProbeOnly")
            exit 3
        }
        $externalProbes = @(
            Invoke-ProtocolProbe -Address $PublicAddress.ToString() -Port $SshHostPort -Protocol "ssh" -Vantage "external" -TimeoutMs $ProbeTimeoutMs
            Invoke-ProtocolProbe -Address $PublicAddress.ToString() -Port $HttpHostPort -Protocol "http" -Vantage "external" -TimeoutMs $ProbeTimeoutMs
        )
        $externalDocument = [pscustomobject][ordered]@{
            schema_version = $script:ExternalEvidenceSchema
            generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
            target_address = $PublicAddress.ToString()
            probes = @($externalProbes | ForEach-Object {
                [pscustomobject][ordered]@{
                    protocol = $_.protocol
                    port = $_.port
                    result = $_.result
                    connected = $_.connected
                    protocol_ok = $_.protocol_ok
                    duration_ms = $_.duration_ms
                    evidence = Protect-DiagnosticText $_.evidence 512
                }
            })
        }
        if (-not (Test-Path -LiteralPath $OutputDirectory)) {
            [void](New-Item -ItemType Directory -Path $OutputDirectory -Force)
        }
        $externalStamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ")
        $externalPath = Join-Path $OutputDirectory ("external-ingress-" + $externalStamp + ".json")
        Write-AtomicUtf8File -Path $externalPath -Content ($externalDocument | ConvertTo-Json -Depth 6)
        $externalSshOk = @($externalProbes | Where-Object { $_.protocol -eq "ssh" -and $_.protocol_ok }).Count -eq 1
        $externalHttpOk = @($externalProbes | Where-Object { $_.protocol -eq "http" -and $_.protocol_ok }).Count -eq 1
        Write-Host ("External evidence: {0}" -f $externalPath)
        if ($externalSshOk -and $externalHttpOk) { exit 0 }
        if ($externalSshOk -or $externalHttpOk) { exit 1 }
        exit 2
    } catch {
        [Console]::Error.WriteLine("External probe failed safely: " + (Protect-DiagnosticText $_.Exception.Message 1000))
        exit 4
    }
}

try {
    if ($FixturePath) {
        $resolvedFixture = Resolve-Path -LiteralPath $FixturePath -ErrorAction Stop
        $fixtureFile = Get-Item -LiteralPath $resolvedFixture -ErrorAction Stop
        if ($fixtureFile.Length -gt 1048576) {
            throw "Fixture exceeds the 1 MiB safety limit"
        }
        $snapshot = Get-Content -LiteralPath $resolvedFixture -Raw -Encoding UTF8 | ConvertFrom-Json
        if ([string](Get-ObjectField $snapshot "schema_version" "") -ne $script:ReportSchema) {
            throw "Fixture schema_version must be $script:ReportSchema"
        }
    } else {
        $ports = @($SshHostPort, $HttpHostPort)
        $docker = Get-DockerSnapshot -Name $ContainerName -ContainerSshPort $SshContainerPort -ContainerHttpPort $HttpContainerPort -ExplicitDockerPath $DockerExecutablePath
        $hostListenerSnapshot = Get-HostListenerSnapshot -Ports $ports
        $hostListeners = @($hostListenerSnapshot.records)
        $firewall = Get-FirewallSnapshot -Ports $ports
        $network = Get-NetworkSnapshot -Ports $ports
        $wfpDrops = Get-WfpDropSnapshot -Ports $ports -LookbackMinutes $WfpLookbackMinutes -ClientIp $ExpectedClientIp

        $probeTargets = New-Object System.Collections.Generic.List[object]
        $probeTargets.Add([pscustomobject]@{ Address = "127.0.0.1"; Vantage = "loopback" })
        $probeTargets.Add([pscustomobject]@{ Address = "::1"; Vantage = "loopback" })
        if ($LanAddress) {
            $probeTargets.Add([pscustomobject]@{ Address = $LanAddress.ToString(); Vantage = "lan" })
        } else {
            foreach ($address in @($network.addresses | Select-Object -First 4)) {
                $probeTargets.Add([pscustomobject]@{ Address = [string]$address.ip_address; Vantage = "lan" })
            }
        }
        if ($PublicAddress) {
            $probeTargets.Add([pscustomobject]@{ Address = $PublicAddress.ToString(); Vantage = "hairpin" })
        }

        $probes = New-Object System.Collections.Generic.List[object]
        foreach ($target in @($probeTargets | Sort-Object Address, Vantage -Unique)) {
            $probes.Add((Invoke-ProtocolProbe -Address $target.Address -Port $SshHostPort -Protocol "ssh" -Vantage $target.Vantage -TimeoutMs $ProbeTimeoutMs))
            $probes.Add((Invoke-ProtocolProbe -Address $target.Address -Port $HttpHostPort -Protocol "http" -Vantage $target.Vantage -TimeoutMs $ProbeTimeoutMs))
        }

        $externalTcp = Invoke-ExternalTcpCheck -Address $PublicAddress -Ports $ports -Enabled ([bool]$UseExternalPortCheck) -TimeoutMs $ProbeTimeoutMs
        $externalProtocolEvidence = Read-ExternalProtocolEvidence -Path $ExternalEvidencePath -ExpectedSshPort $SshHostPort -ExpectedHttpPort $HttpHostPort -ExpectedAddress $PublicAddress -MaximumAgeMinutes $ExternalEvidenceMaxAgeMinutes
        $snapshot = [pscustomobject][ordered]@{
            schema_version = $script:ReportSchema
            generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
            is_administrator = Test-IsAdministrator
            inputs = [pscustomobject][ordered]@{
                container_name = $ContainerName
                ssh_host_port = $SshHostPort
                ssh_container_port = $SshContainerPort
                http_host_port = $HttpHostPort
                http_container_port = $HttpContainerPort
                lan_address = if ($LanAddress) { $LanAddress.ToString() } else { "" }
                public_address = if ($PublicAddress) { $PublicAddress.ToString() } else { "" }
                expected_client_ip = if ($ExpectedClientIp) { $ExpectedClientIp.ToString() } else { "" }
                external_port_check_requested = [bool]$UseExternalPortCheck
            }
            docker = $docker
            host_listeners = @($hostListeners)
            host_listener_collection = $hostListenerSnapshot
            firewall = $firewall
            network = $network
            wfp_drops = $wfpDrops
            probes = $probes.ToArray()
            external_tcp_check = $externalTcp
            external_protocol_evidence = $externalProtocolEvidence
        }
    }

    $snapshot = ConvertTo-SafeDiagnosticSnapshot -Snapshot $snapshot
    $findings = @(Resolve-DiagnosticFindings -Snapshot $snapshot)
    $exitCode = Get-DiagnosticExitCode -Findings $findings
    $status = if ($exitCode -eq 0) { "pass" } elseif ($exitCode -eq 1) { "blocked" } else { "inconclusive" }
    $report = [pscustomobject][ordered]@{
        schema_version = $script:ReportSchema
        generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        summary = [pscustomobject][ordered]@{
            status = $status
            exit_code = $exitCode
            pass_count = @($findings | Where-Object { $_.status -eq "pass" }).Count
            fail_count = @($findings | Where-Object { $_.status -eq "fail" }).Count
            inconclusive_count = @($findings | Where-Object { $_.status -eq "inconclusive" }).Count
            info_count = @($findings | Where-Object { $_.status -eq "info" }).Count
        }
        findings = @($findings)
        snapshot = $snapshot
    }

    if (-not (Test-Path -LiteralPath $OutputDirectory)) {
        [void](New-Item -ItemType Directory -Path $OutputDirectory -Force)
    }
    $runStamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ")
    $jsonPath = Join-Path $OutputDirectory ("docker-ingress-" + $runStamp + ".json")
    $markdownPath = Join-Path $OutputDirectory ("docker-ingress-" + $runStamp + ".md")
    Write-AtomicUtf8File -Path $jsonPath -Content ($report | ConvertTo-Json -Depth 14)
    Write-AtomicUtf8File -Path $markdownPath -Content (Convert-ReportToMarkdown -Report $report)

    Write-Host ""
    Write-Host ("Diagnostic status: {0}" -f $status)
    Write-Host ("Pass={0} Fail={1} Inconclusive={2} Info={3}" -f $report.summary.pass_count, $report.summary.fail_count, $report.summary.inconclusive_count, $report.summary.info_count)
    Write-Host ("JSON: {0}" -f $jsonPath)
    Write-Host ("Markdown: {0}" -f $markdownPath)
    Write-Host "Exit 2 means the local stack has no conclusive blocker but independent SSH protocol evidence is still missing."
    exit $exitCode
} catch {
    [Console]::Error.WriteLine("Diagnostic failed safely: " + (Protect-DiagnosticText $_.Exception.Message 2000))
    exit 4
}
