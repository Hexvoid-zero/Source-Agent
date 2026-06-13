"""Source Agent launcher — opens as a standalone desktop app window.

uvicorn runs in the main thread (rock-solid); a background thread waits for the
port to accept connections, then opens the UI in a chromeless app window
(Edge/Chrome `--app` mode: no tabs, no address bar, its own taskbar entry).
Reliable in a packaged exe; no .NET/WebView2 Python bindings needed.
"""
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


def find_browser() -> str | None:
    candidates = [
        os.path.expandvars(r"%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe"),
        os.path.expandvars(r"%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"),
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    for name in ("msedge", "chrome", "chromium"):
        w = shutil.which(name)
        if w:
            return w
    return None


def main():
    data_dir = Path(os.getenv("LOCALAPPDATA") or Path.home()) / "SourceAgent"
    data_dir.mkdir(parents=True, exist_ok=True)
    logfile = data_dir / "source-agent.log"

    def log(msg):
        try:
            with logfile.open("a", encoding="utf-8") as f:
                f.write(f"{time.strftime('%H:%M:%S')} {msg}\n")
        except Exception:
            pass

    # --windowed builds have sys.stdout/stderr = None; uvicorn logging needs them.
    if sys.stdout is None or sys.stderr is None:
        _log = open(logfile, "a", buffering=1, encoding="utf-8")
        sys.stdout = sys.stdout or _log
        sys.stderr = sys.stderr or _log

    log("launcher start")
    if getattr(sys, "frozen", False):
        os.environ.setdefault("SOURCE_AGENT_STATIC", str(Path(sys._MEIPASS) / "static"))

    port = int(os.getenv("SOURCE_AGENT_PORT", "8775"))
    url = f"http://127.0.0.1:{port}"

    import uvicorn

    from server import app

    def open_window():
        for _ in range(300):  # wait until the port accepts connections
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    break
            except OSError:
                time.sleep(0.15)
        log(f"server ready on {url}")
        browser = find_browser()
        log(f"browser={browser}")
        if browser:
            profile = tempfile.mkdtemp(prefix="SourceAgent_")
            try:
                log("launching app window")
                proc = subprocess.Popen([
                    browser, f"--app={url}", f"--user-data-dir={profile}",
                    "--no-first-run", "--no-default-browser-check", "--window-size=1280,860",
                ])
                proc.wait()  # blocks until the app window is closed
                log("app window closed")
                os._exit(0)
            except Exception as e:
                log(f"app-mode launch failed: {e}")
        log("falling back to default browser")
        import webbrowser
        webbrowser.open(url)

    threading.Thread(target=open_window, daemon=True).start()

    uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")).run()
    os._exit(0)


if __name__ == "__main__":
    main()
