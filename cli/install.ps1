$ErrorActionPreference = "Stop"

$InstallDir = if ($env:GAME_INSTALL_DIR) { $env:GAME_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps" }
$DownloadBase = if ($env:GAME_DOWNLOAD_BASE) { $env:GAME_DOWNLOAD_BASE } else { "https://world.ascii.dev/download" }

if ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne "X64") {
  Write-Error "Unsupported architecture: $([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)"
  exit 1
}

$Asset = "world-windows-x64.exe"
$Url = "$DownloadBase/$Asset"
$Target = Join-Path $InstallDir "world.exe"
$Tmp = New-TemporaryFile

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
Invoke-WebRequest -Uri $Url -OutFile $Tmp
Move-Item -Force -Path $Tmp -Destination $Target

Write-Host "Installed world to $Target"
Write-Host "Run: world"
