<#
.SYNOPSIS
  Self-signed Authenticode signing for the Windows NCC Orchestrator
  binaries, runnable natively on Windows (PowerShell 5.1+ / 7+).

.DESCRIPTION
  Produces a REAL Authenticode signature using a self-signed
  code-signing certificate created with New-SelfSignedCertificate, then
  signs the *-windows-*.exe binaries with Set-AuthenticodeSignature and
  exports the public certificate (.cer) so fleet administrators can
  import it into Trusted Publishers.

  IMPORTANT: a self-signed certificate does NOT make Windows SmartScreen
  trust the binaries for the general public. SmartScreen only trusts
  certificates chaining to a CA in the Microsoft Trusted Root program
  (OV/EV code-signing certs). A self-signed cert is trusted only on
  machines that have imported it (typical for managed enterprise
  fleets via GPO/Intune). For a public, no-warning experience, buy a
  CA-issued code-signing certificate and sign with that instead.
  See docs/SECURITY_AND_TRUST.md.

.PARAMETER Dist
  Directory holding the *-windows-*.exe files. Default: dist

.PARAMETER Subject
  Certificate subject. Default: the project publisher CN.

.PARAMETER OutCert
  Path for the exported public .cer. Default: <Dist>\ncc-codesign-public.cer

.PARAMETER TimestampServer
  RFC3161 timestamp URL. Default: DigiCert. Pass '' to skip timestamping.

.PARAMETER KeepCert
  Keep the generated cert in CurrentUser\My (default: removed after signing).

.EXAMPLE
  .\scripts\sign-windows.ps1 -Dist dist
#>
[CmdletBinding()]
param(
  [string]$Dist = "dist",
  [string]$Subject = "CN=NCC Orchestrator (open-source project), O=NCC Orchestrator project",
  [string]$OutCert = "",
  [string]$TimestampServer = "http://timestamp.digicert.com",
  [switch]$KeepCert
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $Dist)) { throw "dist dir not found: $Dist" }
if ([string]::IsNullOrEmpty($OutCert)) { $OutCert = Join-Path $Dist "ncc-codesign-public.cer" }

$patterns = @("ncc-orchestrator-windows-*.exe", "ncc-api-server-windows-*.exe", "ncc-ui-server-windows-*.exe")
$exes = foreach ($p in $patterns) { Get-ChildItem -Path $Dist -Filter $p -File -ErrorAction SilentlyContinue }
if (-not $exes -or $exes.Count -eq 0) { throw "no *-windows-*.exe files under $Dist" }

Write-Host "==> generating self-signed code-signing certificate"
Write-Host "    subject: $Subject"
$cert = New-SelfSignedCertificate `
  -Type CodeSigningCert `
  -Subject $Subject `
  -KeyAlgorithm RSA -KeyLength 3072 `
  -KeyUsage DigitalSignature `
  -CertStoreLocation "Cert:\CurrentUser\My" `
  -NotAfter (Get-Date).AddYears(3)

try {
  # Export the public certificate so fleet admins can import it into
  # LocalMachine\TrustedPublisher (and Root) to trust the binaries.
  Export-Certificate -Cert $cert -FilePath $OutCert | Out-Null
  Write-Host "==> wrote public certificate for fleet import: $OutCert"

  foreach ($f in $exes) {
    Write-Host "==> signing $($f.FullName)"
    $sigParams = @{
      FilePath      = $f.FullName
      Certificate   = $cert
      HashAlgorithm = "SHA256"
    }
    if (-not [string]::IsNullOrEmpty($TimestampServer)) {
      $sigParams["TimestampServer"] = $TimestampServer
    }
    $result = Set-AuthenticodeSignature @sigParams
    Write-Host "    status: $($result.Status)  signer: $($result.SignerCertificate.Subject)"
    if ($result.Status -ne "Valid" -and $result.Status -ne "UnknownError") {
      # UnknownError is expected for self-signed (untrusted chain) but the
      # signature is still applied; treat hard failures only as errors.
      Write-Warning "signature status for $($f.Name): $($result.Status)"
    }
  }
}
finally {
  if (-not $KeepCert) {
    Remove-Item -Path "Cert:\CurrentUser\My\$($cert.Thumbprint)" -ErrorAction SilentlyContinue
  }
}

Write-Host ""
Write-Host "Done. Signed $($exes.Count) binary(ies)."
Write-Host "For machines to trust these binaries, import the public cert:"
Write-Host "    $OutCert"
Write-Host "into Trusted Publishers (and Trusted Root) -- see docs/SECURITY_AND_TRUST.md."
