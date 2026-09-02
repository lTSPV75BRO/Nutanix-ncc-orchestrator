# Run elevated during Sysprep specialize or manually as Administrator.
$ErrorActionPreference = "Stop"

$NccVersion = if ($env:NCC_VERSION) { $env:NCC_VERSION } else { "2.2.0" }
$ReleaseBaseUrl = if ($env:NCC_RELEASE_BASE_URL) {
    $env:NCC_RELEASE_BASE_URL.TrimEnd("/")
} else {
    "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases/download"
}
$InstallDir = if ($env:NCC_INSTALL_DIR) { $env:NCC_INSTALL_DIR } else { "C:\ProgramData\NCC\v2" }
$ConfigPath = if ($env:NCC_CONFIG) { $env:NCC_CONFIG } else { "$InstallDir\config.yaml" }
$ApiPort = if ($env:NCC_API_PORT) { $env:NCC_API_PORT } else { "8081" }
$UiPort = if ($env:NCC_UI_PORT) { $env:NCC_UI_PORT } else { "8080" }
$Clusters = if ($env:NCC_CLUSTERS) { $env:NCC_CLUSTERS } else { "" }
$Pcs = if ($env:NCC_PCS) { $env:NCC_PCS } else { "" }
$Username = if ($env:NCC_USERNAME) { $env:NCC_USERNAME } else { "admin" }

$arch = if ([Environment]::Is64BitOperatingSystem) { "amd64" } else { throw "64-bit Windows is required" }
$archive = "ncc-v2-stack-windows-$arch.zip"
$baseUrl = "$ReleaseBaseUrl/$NccVersion"
$tempDir = Join-Path $env:TEMP ("ncc-" + [Guid]::NewGuid().ToString("N"))
$archivePath = Join-Path $tempDir $archive
$checksumPath = Join-Path $tempDir "checksums.txt"

New-Item -ItemType Directory -Path $tempDir -Force | Out-Null
try {
    Invoke-WebRequest -Uri "$baseUrl/$archive" -OutFile $archivePath
    Invoke-WebRequest -Uri "$baseUrl/checksums.txt" -OutFile $checksumPath

    $expected = (Get-Content $checksumPath |
        Where-Object { $_ -match "\s$([regex]::Escape($archive))$" } |
        Select-Object -First 1).Split(" ")[0]
    $actual = (Get-FileHash $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
    if (!$expected -or $expected.ToLowerInvariant() -ne $actual) {
        throw "Release checksum verification failed for $archive"
    }

    if (Test-Path $InstallDir) { Remove-Item $InstallDir -Recurse -Force }
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    Expand-Archive -Path $archivePath -DestinationPath $InstallDir -Force

    $configDir = Split-Path $ConfigPath -Parent
    New-Item -ItemType Directory -Path $configDir -Force | Out-Null
    New-Item -ItemType Directory -Path "$InstallDir\outputfiles" -Force | Out-Null
    New-Item -ItemType Directory -Path "$InstallDir\logs" -Force | Out-Null
    @"
schema-version: 1
runner:
  targets:
    mode: clusters
    clusters: [$Clusters]
    pcs: [$Pcs]
  credentials:
    username: "$Username"
    password: "secret://NCC_PASSWORD"
  execution:
    timeout: "15m"
    request-timeout: "20s"
    max-parallel: 4
  retry:
    max-attempts: 6
    base-delay: "400ms"
    max-delay: "8s"
    circuit-breaker: 3
storage:
  output-dir: "$InstallDir\outputfiles"
  logs-dir: "$InstallDir\logs"
  run-history-dir: "$InstallDir\outputfiles\runs"
api:
  cache:
    pc-alerts-cache-ttl: "5m"
deployment:
  platform: host
  self-heal: true
secrets-provider: env
"@ | Set-Content -Path $ConfigPath -Encoding UTF8

    New-NetFirewallRule -DisplayName "NCC Orchestrator UI ($UiPort)" `
        -Direction Inbound -Action Allow -Protocol TCP -LocalPort $UiPort `
        -Profile Domain,Private | Out-Null
    New-NetFirewallRule -DisplayName "NCC Orchestrator API ($ApiPort)" `
        -Direction Inbound -Action Allow -Protocol TCP -LocalPort $ApiPort `
        -Profile Domain,Private | Out-Null

    $bin = "$InstallDir\bin\ncc-orchestrator.exe"
    $args = "v2-start --install-dir `"$InstallDir`" --config-path `"$ConfigPath`" " +
        "--output-dir `"$InstallDir\outputfiles`" --log-dir `"$InstallDir\logs`" " +
        "--orchestrator-bin `"$bin`" --api-listen :$ApiPort --ui-listen :$UiPort " +
        "--detach --self-heal"
    $action = New-ScheduledTaskAction -Execute $bin -Argument $args
    $trigger = New-ScheduledTaskTrigger -AtStartup
    $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
    Register-ScheduledTask -TaskName "NCC Orchestrator v2" -Action $action `
        -Trigger $trigger -Principal $principal -Force | Out-Null
    Start-ScheduledTask -TaskName "NCC Orchestrator v2"

    $doctorArgs = "doctor --install-dir `"$InstallDir`" --fix --only-checks log-sizes --json"
    $doctorAction = New-ScheduledTaskAction -Execute $bin -Argument $doctorArgs
    $doctorTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(5) `
        -RepetitionInterval (New-TimeSpan -Minutes 15) `
        -RepetitionDuration (New-TimeSpan -Days 3650)
    Register-ScheduledTask -TaskName "NCC Log Maintenance" -Action $doctorAction `
        -Trigger $doctorTrigger -Principal $principal -Force | Out-Null

    Write-Host "NCC v$NccVersion installed at $InstallDir."
    Write-Host "Edit $ConfigPath and replace CHANGE_ME before running checks."
} finally {
    Remove-Item $tempDir -Recurse -Force -ErrorAction SilentlyContinue
}
