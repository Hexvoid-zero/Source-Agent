# Build Source Agent into a standalone Windows app (native window, no browser).
# Usage:  powershell -ExecutionPolicy Bypass -File build.ps1
Set-Location $PSScriptRoot

Write-Host "1/2  Preparing Python build env…"
if (-not (Test-Path .venv-build)) { python -m venv .venv-build }
.\.venv-build\Scripts\python.exe -m pip install -q -r backend\requirements.txt pyinstaller

Write-Host "2/2  Packaging exe (onedir = fast startup)…"
Remove-Item -Recurse -Force build, dist, SourceAgent.spec -ErrorAction SilentlyContinue
.\.venv-build\Scripts\python.exe -m PyInstaller --noconfirm --onedir --windowed --name SourceAgent `
  --add-data "static;static" --collect-submodules uvicorn `
  --paths backend backend\launcher.py

Write-Host "Done -> dist\SourceAgent\SourceAgent.exe"
