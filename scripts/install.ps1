# ============================================================
# Apiary install script for Windows (PowerShell)
#
# Downloads a pre-built Apiary binary from GitHub Releases and
# installs it into a user-local directory.
#
# Usage (PowerShell):
#   irm https://raw.githubusercontent.com/ApiaryData/apiary/main/scripts/install.ps1 | iex
#
#   # Or install a specific version / directory:
#   $env:APIARY_VERSION = "0.1.0"
#   $env:INSTALL_DIR    = "$env:USERPROFILE\.apiary\bin"
#   .\install.ps1
# ============================================================

$ErrorActionPreference = "Stop"

$Repo       = "ApiaryData/apiary"
$Version    = if ($env:APIARY_VERSION) { $env:APIARY_VERSION } else { "latest" }
$InstallDir = if ($env:INSTALL_DIR)    { $env:INSTALL_DIR }    else { "$env:USERPROFILE\.apiary\bin" }

function Info($msg)  { Write-Host "==> $msg" -ForegroundColor Cyan }
function Fatal($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }

# ---- resolve version ----------------------------------------
if ($Version -eq "latest") {
    Info "Querying latest release …"
    try {
        $release = Invoke-RestMethod "https://api.github.com/repos/$Repo/releases/latest"
        $Version = $release.tag_name -replace '^v', ''
    } catch {
        Fatal "Could not determine latest release: $_"
    }
} else {
    $Version = $Version -replace '^v', ''
}

# ---- detect architecture ------------------------------------
$arch = if ([Environment]::Is64BitOperatingSystem) { "x86_64" } else { Fatal "32-bit Windows is not supported" }

$artifact = "apiary-${arch}-windows"
$url = "https://github.com/$Repo/releases/download/v$Version/$artifact.zip"

Info "Detected platform: windows $arch"
Info "Installing Apiary v$Version -> $InstallDir\apiary.exe"

# ---- download -----------------------------------------------
$tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) "apiary-install-$(Get-Random)"
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null

$zipPath = Join-Path $tmpDir "$artifact.zip"

Info "Downloading $url …"
try {
    Invoke-WebRequest -Uri $url -OutFile $zipPath -UseBasicParsing
} catch {
    Fatal "Download failed. Check the version (v$Version) and platform ($artifact).`n$_"
}

# ---- extract ------------------------------------------------
Info "Extracting …"
Expand-Archive -Path $zipPath -DestinationPath $tmpDir -Force

$binary = Get-ChildItem -Path $tmpDir -Filter "apiary.exe" -Recurse | Select-Object -First 1
if (-not $binary) { Fatal "Expected binary 'apiary.exe' not found in archive" }

# ---- install ------------------------------------------------
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
Copy-Item -Path $binary.FullName -Destination "$InstallDir\apiary.exe" -Force

# Clean up temp files
Remove-Item -Recurse -Force $tmpDir -ErrorAction SilentlyContinue

# ---- update PATH --------------------------------------------
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($userPath -notlike "*$InstallDir*") {
    Info "Adding $InstallDir to your user PATH …"
    [Environment]::SetEnvironmentVariable("Path", "$userPath;$InstallDir", "User")
    $env:Path = "$env:Path;$InstallDir"
}

Info "Apiary v$Version installed to $InstallDir\apiary.exe"

# Verify
try { & "$InstallDir\apiary.exe" --version 2>$null } catch {}
