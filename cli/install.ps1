$Repo = if ($env:GAME_CLI_REPO) { $env:GAME_CLI_REPO } else { "REPLACE_WITH_GITHUB_REPO" }
$InstallDir = if ($env:GAME_INSTALL_DIR) { $env:GAME_INSTALL_DIR } else { Join-Path $HOME ".ascii\bin" }

if ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne "X64") {
  Write-Error "Unsupported architecture: $([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture)"
  exit 1
}

$Asset = "game-windows-x64.exe"
$Url = "https://github.com/$Repo/releases/latest/download/$Asset"

New-Item -ItemType Directory -Force $InstallDir | Out-Null
$Target = Join-Path $InstallDir "game.exe"
$Tmp = New-TemporaryFile
Invoke-WebRequest -Uri $Url -OutFile $Tmp
Move-Item -Force $Tmp $Target

Write-Host "Installed game to $Target"

