$DataDir = if ($env:LOCALAPPDATA) {
  $env:LOCALAPPDATA
} elseif ($env:APPDATA) {
  $env:APPDATA
} else {
  Join-Path $HOME "AppData\Local"
}

$Target = Join-Path $DataDir "ascii-game"

if (-not (Test-Path -LiteralPath $Target)) {
  Write-Host "No local data found at $Target"
  exit 0
}

Remove-Item -LiteralPath $Target -Recurse -Force
Write-Host "Removed $Target"
