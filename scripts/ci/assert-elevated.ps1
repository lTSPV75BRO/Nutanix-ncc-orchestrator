# Fails loudly unless this session is an elevated administrator: a member of
# Administrators with a high-integrity token, and `net session` succeeds.
# CI runs it before the Windows permission tests so a runner image that stops
# being elevated cannot quietly turn the elevated tests into skips.
$ErrorActionPreference = 'Stop'

$principal = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
$isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
$groups = (& whoami /groups) -join "`n"
$high = $groups -match 'S-1-16-12288'   # High Mandatory Level (elevated)
& cmd.exe /c 'net session >nul 2>&1'
$netSession = $LASTEXITCODE

Write-Host "user:                 $([Security.Principal.WindowsIdentity]::GetCurrent().Name)"
Write-Host "administrator role:   $isAdmin"
Write-Host "high integrity:       $high"
Write-Host "net session exit:     $netSession"

$problems = @()
if (-not $isAdmin) { $problems += 'not an enabled member of Administrators' }
if (-not $high) { $problems += 'token is not high integrity (S-1-16-12288), i.e. not elevated' }
if ($netSession -ne 0) { $problems += "'net session' failed with exit code $netSession" }
if ($problems.Count -gt 0) {
  Write-Host "::error title=Runner is not elevated::$($problems -join '; ')"
  exit 1
}
Write-Host 'Runner session is elevated.'
