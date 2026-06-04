$ErrorActionPreference = "Stop"

$Repo = if ($env:GAME_CLI_REPO) { $env:GAME_CLI_REPO } else { "ariana-dot-dev/ascii-world" }
$InstallDir = if ($env:GAME_INSTALL_DIR) { $env:GAME_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps" }

if ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne "X64") {
  Write-Error "Unsupported architecture: $([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)"
  exit 1
}

$Asset = "world-windows-x64.exe"
$Url = "https://github.com/$Repo/releases/latest/download/$Asset"
$Target = Join-Path $InstallDir "world.exe"
$Tmp = New-TemporaryFile

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
Invoke-WebRequest -Uri $Url -OutFile $Tmp
Move-Item -Force -Path $Tmp -Destination $Target

Write-Host "Installed world to $Target"
Write-Host "Run: world"
