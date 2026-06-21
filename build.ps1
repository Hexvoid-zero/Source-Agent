# Build Source Agent into a standalone Windows app (native pywebview window).
# Usage:  powershell -ExecutionPolicy Bypass -File build.ps1
Set-Location $PSScriptRoot

Write-Host "1/2  Preparing Python build env…"
if (-not (Test-Path .venv-build)) { python -m venv .venv-build }
.\.venv-build\Scripts\python.exe -m pip install -q -r backend\requirements.txt pyinstaller

Write-Host "2/2  Packaging exe (onedir = fast startup)…"
Remove-Item -Recurse -Force build, dist, SourceAgent.spec -ErrorAction SilentlyContinue
# Exclude tkinter: pyautogui pulls it in (via pymsgbox/mouseinfo), but we never use it
# (UI is pywebview/Edge). On Python 3.14 PyInstaller fails to collect Tcl/Tk data, so the
# tkinter runtime hook crashes at launch ("_tcl_data not found"). pyautogui's screenshot/
# mouse/keyboard work fine without it (screenshots come from Pillow's ImageGrab).
.\.venv-build\Scripts\python.exe -m PyInstaller --noconfirm --onedir --windowed --optimize 2 --name SourceAgent `
  --icon SourceAgent.ico `
  --exclude-module tkinter --exclude-module _tkinter --exclude-module pymsgbox --exclude-module mouseinfo --exclude-module PIL.ImageTk `
  --add-data "static;static" --collect-submodules uvicorn `
  --collect-all webview --collect-all clr_loader --collect-all pythonnet --hidden-import clr `
  --hidden-import overlay `
  --paths backend backend\launcher.py

Write-Host "Done -> dist\SourceAgent\SourceAgent.exe"
