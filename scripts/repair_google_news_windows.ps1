param(
  [ValidateSet("stats", "dry-run", "apply")]
  [string]$Mode = "stats",

  [int]$Limit = 10,

  [double]$SleepSeconds = 20,

  [double]$SleepMaxSeconds = 0,

  [int]$Repeat = 1,

  [int]$PauseMinutes = 30,

  [switch]$IncludeRejected,

  [bool]$MarkDuplicates = $true,

  [switch]$UpdatePublishedAt,

  [string]$Python = ".\.venv\Scripts\python.exe"
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $ProjectRoot
$env:PYTHONUTF8 = "1"

if (-not (Test-Path $Python)) {
  throw "Python executable was not found: $Python"
}

$argsList = @(
  "scripts\repair_google_news_windows.py",
  "--mode", $Mode,
  "--limit", "$Limit",
  "--sleep-seconds", "$SleepSeconds",
  "--repeat", "$Repeat",
  "--pause-minutes", "$PauseMinutes"
)

if ($SleepMaxSeconds -gt 0) {
  $argsList += @("--sleep-max-seconds", "$SleepMaxSeconds")
}
if ($IncludeRejected) {
  $argsList += "--include-rejected"
}
if (-not $MarkDuplicates) {
  $argsList += "--no-mark-duplicates"
}
if ($UpdatePublishedAt) {
  $argsList += "--update-published-at"
}

& $Python @argsList
exit $LASTEXITCODE
