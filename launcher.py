"""Source Agent launcher — opens as a standalone desktop app window.

Runs the FastAPI server in a background thread and shows the UI in a native
desktop window (pywebview / WebView2) — no browser needed. Falls back to the
system browser if a webview backend isn't available.

Startup strategy: show a splash window INSTANTLY (local HTML string, zero
network), then start the server in the background and navigate to the real
URL once ready. This eliminates the perceived "blank screen" wait.
"""
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

# ---------- Splash HTML (inlined to avoid file-path issues) ----------
_SPLASH_HTML = """\
<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>Source Agent</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#1E1B4B;display:flex;align-items:center;justify-content:center;
height:100vh;font-family:-apple-system,"Segoe UI",Roboto,sans-serif;color:#F8FAFC;overflow:hidden}
.wrap{text-align:center}
.logo{font-size:64px;color:#3B82F6;text-shadow:0 0 20px rgba(59,130,246,0.6);margin-bottom:20px;animation:pulse 2s ease-in-out infinite}
h1{font-size:22px;font-weight:700;letter-spacing:.4px;margin-bottom:10px;color:#F8FAFC}
h1 span{color:#3B82F6}
p{font-size:13px;color:#64748B;margin-bottom:32px}
.bar{width:200px;height:3px;background:#252150;border-radius:3px;margin:0 auto;overflow:hidden;position:relative}
.bar::after{content:'';position:absolute;left:-40%;top:0;width:40%;height:100%;
background:linear-gradient(90deg,transparent,#3B82F6,#06B6D4,transparent);border-radius:3px;
animation:slide 1.2s ease-in-out infinite}
@keyframes slide{0%{left:-40%}100%{left:100%}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.6}}
</style></head><body>
<div class="wrap">
  <div class="logo">☤</div>
  <h1>Source<span>Agent</span></h1>
  <p>Loading personal assistant…</p>
  <div class="bar"></div>
</div></body></html>
"""


def main():
    data_dir = Path(os.getenv("SOURCE_AGENT_DATA") or (Path(os.getenv("LOCALAPPDATA") or Path.home()) / "SourceAgent"))
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

    try:
        if os.getenv("SOURCE_AGENT_HEADLESS") == "1":
            raise RuntimeError("Headless mode requested via SOURCE_AGENT_HEADLESS")

        import webview

        window = webview.create_window(
            "Source Agent", html=_SPLASH_HTML,
            width=1280, height=860, min_size=(960, 620),
            text_select=True
        )

        def _on_gui_ready():
            def _boot():
                try:
                    log("boot thread start")
                    import httpx
                    import uvicorn
                    log("importing server")
                    from server import app
                    log("server imported")

                    config = uvicorn.Config(
                        app,
                        host="127.0.0.1",
                        port=port,
                        log_level="warning",
                        log_config={
                            "version": 1,
                            "disable_existing_loggers": False,
                            "formatters": {
                                "default": {"format": "%(levelname)s: %(message)s"}
                            },
                            "handlers": {
                                "default": {
                                    "class": "logging.StreamHandler",
                                    "formatter": "default"
                                }
                            },
                            "loggers": {
                                "uvicorn": {
                                    "handlers": ["default"],
                                    "level": "WARNING"
                                }
                            }
                        }
                    )
                    server = uvicorn.Server(config)
                    log("starting uvicorn")
                    threading.Thread(target=server.run, daemon=True).start()

                    log("waiting for health endpoint")
                    for _ in range(300):
                        try:
                            if httpx.get(url + "/api/ping", timeout=1.0).status_code == 200:
                                break
                        except Exception:
                            time.sleep(0.05)

                    log(f"Source Agent ready — {url}")
                    window.load_url(url)
                except Exception as e:
                    import traceback
                    log(f"BOOT EXCEPTION: {e}")
                    log(traceback.format_exc())

            threading.Thread(target=_boot, daemon=True).start()

        webview.start(func=_on_gui_ready, gui="edgechromium")

    except Exception as e:
        log(f"Native window unavailable ({e}); falling back to default browser")
        import httpx
        import uvicorn
        from server import app

        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        server = uvicorn.Server(config)
        threading.Thread(target=server.run, daemon=True).start()

        for _ in range(300):
            try:
                if httpx.get(url + "/api/ping", timeout=1.0).status_code == 200:
                    break
            except Exception:
                time.sleep(0.05)

        if os.getenv("SOURCE_AGENT_HEADLESS") != "1":
            import webbrowser
            webbrowser.open(url)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass

    os._exit(0)


if __name__ == "__main__":
    main()
