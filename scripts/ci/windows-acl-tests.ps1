# Builds the Windows permission test binaries and runs them either in this
# (elevated) session or as SYSTEM through a one-shot scheduled task, which is
# the account the Task Scheduler service runs the stack as.
#
#   -Identity Elevated   run here; needs an elevated session (assert-elevated.ps1)
#   -Identity System     register a one-shot task as SYSTEM, run it, collect the
#                        log and exit code, delete the task
#
# NCC_TEST_EXPECT_ELEVATED / NCC_TEST_EXPECT_SYSTEM are set for the run, so a
# test that needs the token fails instead of skipping. On top of that, every
# test in $Required must have PASSed (not skipped) in the output.
param(
  [Parameter(Mandatory)][ValidateSet('Elevated', 'System')][string]$Identity,
  [int]$TimeoutSeconds = 900
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$work = Join-Path $(if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { $env:TEMP }) ("ncc-acl-$Identity-" + [guid]::NewGuid().ToString('N').Substring(0, 8))
New-Item -ItemType Directory -Force -Path $work | Out-Null

$suites = @(
  @{
    Name = 'winacl'; Pkg = './internal/winacl'; Dir = (Join-Path $repo 'internal\winacl'); Run = '.'
    Required = @('TestTokenMatchesExpectation', 'TestElevatedRestrictTakesOwnership',
      'TestElevatedRestrictTreeTakesOwnershipOfChildren', 'TestElevatedRestrictKeepsTrustedOwner',
      'TestRestrictFile', 'TestRestrictTreePrivate', 'TestRestrictTreeNoBroadWrite', 'TestReplaceOwner')
  },
  @{
    Name = 'orchestrator'; Pkg = './cmd/ncc-orchestrator'; Dir = (Join-Path $repo 'cmd\ncc-orchestrator')
    Run = '^(TestSecureInstallACLs|TestSecretsPermsCheckOnWindows|TestCloseInstallRootRefusesUnsafe|TestACLOutcomeMessages|TestRefreshInstallACLsAfterReplace|TestVolumeRootInstallIsLeftAlone|TestDoctorFixElevated|TestDoctorFixInstallerLayoutAsSystem)$'
    Required = @('TestSecureInstallACLs', 'TestSecretsPermsCheckOnWindows', 'TestCloseInstallRootRefusesUnsafe',
      'TestDoctorFixElevated')
  }
)
if ($Identity -eq 'System') {
  $suites[0].Required += 'TestSystemRestrictLeavesSystemAndAdmins'
  $suites[1].Required += 'TestDoctorFixInstallerLayoutAsSystem'
}

function Invoke-AsSystem([string]$exe, [string]$dir, [string]$run, [string]$log, [string]$taskName) {
  $exitFile = "$log.exit"
  $cmdFile = Join-Path $work "$taskName.cmd"
  # The task command line is limited to 261 characters, so it only calls this file.
  @(
    '@echo off',
    'set NCC_TEST_EXPECT_ELEVATED=1',
    'set NCC_TEST_EXPECT_SYSTEM=1',
    "cd /d `"$dir`"",
    "`"$exe`" -test.v -test.count=1 -test.timeout=10m `"-test.run=$run`" > `"$log`" 2>&1",
    "echo %errorlevel% > `"$exitFile.tmp`"",
    "move /y `"$exitFile.tmp`" `"$exitFile`" > nul"
  ) | Set-Content -Path $cmdFile -Encoding ascii

  & schtasks /Create /TN $taskName /RU SYSTEM /SC ONCE /ST 00:00 /TR "`"$cmdFile`"" /F
  if ($LASTEXITCODE -ne 0) { throw "schtasks /Create failed ($LASTEXITCODE)" }
  try {
    & schtasks /Run /TN $taskName
    if ($LASTEXITCODE -ne 0) { throw "schtasks /Run failed ($LASTEXITCODE)" }
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while (-not (Test-Path $exitFile)) {
      if ((Get-Date) -gt $deadline) {
        & schtasks /End /TN $taskName 2>$null
        throw "SYSTEM task $taskName did not finish within $TimeoutSeconds s"
      }
      Start-Sleep -Seconds 2
    }
    return [int](Get-Content $exitFile -Raw).Trim()
  }
  finally {
    & schtasks /Delete /TN $taskName /F 2>$null | Out-Null
  }
}

$failed = $false
foreach ($s in $suites) {
  $exe = Join-Path $work "$($s.Name).acl.test.exe"
  Write-Host "::group::build $($s.Pkg) test binary"
  Push-Location $repo
  try { & go test -c -o $exe $s.Pkg; if ($LASTEXITCODE -ne 0) { throw "go test -c $($s.Pkg) failed" } }
  finally { Pop-Location }
  Write-Host '::endgroup::'

  $log = Join-Path $work "$($s.Name).log"
  Write-Host "::group::$($s.Name) permission tests as $Identity"
  if ($Identity -eq 'System') {
    $code = Invoke-AsSystem $exe $s.Dir $s.Run $log "ncc-acl-test-$($s.Name)-$PID"
  }
  else {
    $env:NCC_TEST_EXPECT_ELEVATED = '1'
    Push-Location $s.Dir
    $ErrorActionPreference = 'Continue'   # test stderr must not abort the script
    try {
      & $exe '-test.v' '-test.count=1' '-test.timeout=10m' "-test.run=$($s.Run)" *> $log
      $code = $LASTEXITCODE
    }
    finally {
      $ErrorActionPreference = 'Stop'
      Pop-Location
      Remove-Item Env:NCC_TEST_EXPECT_ELEVATED -ErrorAction SilentlyContinue
    }
  }
  $out = Get-Content $log -Raw
  Write-Host $out
  Write-Host "exit code: $code"
  Write-Host '::endgroup::'

  if ($code -ne 0) { Write-Host "::error::$($s.Name) tests failed as $Identity (exit $code)"; $failed = $true }
  foreach ($name in $s.Required) {
    if ($out -notmatch "(?m)^--- PASS: $name \(") {
      Write-Host "::error::$name did not PASS as $Identity (skipped, missing or failed)"
      $failed = $true
    }
  }
}
if ($failed) { exit 1 }
Write-Host "All permission tests passed as $Identity."
