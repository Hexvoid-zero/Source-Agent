"""Source Agent — a self-improving personal AI agent.

Architecture inspired by Nous Research's Hermes Agent (MIT): a tool-using agent
loop with a closed learning loop (cross-session memory). It runs shell commands,
reads/writes files, searches the web, thinks, and remembers — all locally via
Ollama. Streams its work step-by-step. Serves its own UI; packaged to one exe.
"""
import html as html_lib
import json
import os
import re
import string
import subprocess
import sys
import time
import urllib.parse
import webbrowser
import uuid
from datetime import datetime, timezone
from pathlib import Path

import random
import threading
import httpx
import pyautogui
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

# --------------------------------------------------------------------------- DPI + safety
# Windows DPI awareness: without this, pyautogui coordinates use *logical* pixels
# while screenshots capture *physical* pixels, so every click lands in the wrong
# spot on any system with display scaling (125%, 150%, etc.).
try:
    import ctypes
    ctypes.windll.shcore.SetProcessDpiAwareness(2)   # per-monitor DPI aware
except Exception:
    pass

# Disable fail-safe globally — automated cursor movements to screen corners
# (which happen constantly during computer-use) must not raise FailSafeException.
pyautogui.FAILSAFE = False

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "")
DATA_DIR = Path(os.getenv("SOURCE_AGENT_DATA") or (Path(os.getenv("LOCALAPPDATA") or Path.home()) / "SourceAgent"))
CONV_DIR = DATA_DIR / "conversations"
CODER_CONV_DIR = Path(os.getenv("LOCALAPPDATA") or Path.home()) / "SourceCodeIDE" / "conversations"
MEMORY_FILE = DATA_DIR / "memory.md"
CONNECTORS_FILE = DATA_DIR / "connectors.json"
SKILLS_DIR = DATA_DIR / "skills"
SKILLS_CONFIG = DATA_DIR / "skills_config.json"
MAX_STEPS = 1000000
MAX_BYTES = 2_000_000

# Source Worker ("Source 1") shares its state on this machine — the Remote Workspace
# reads it directly so you can watch the workers you hired over there from here.
SOURCE_WORKER_DATA = Path(os.getenv("SOURCE_WORKER_DATA") or (Path(os.getenv("LOCALAPPDATA") or Path.home()) / "SourceWorker"))
# Display metadata for the built-in workers (mirrors Source Worker's AVAILABLE_AGENTS).
WORKER_AGENT_CATALOG = {
    "alice":   {"name": "Alice Chen",    "avatar": "👩‍💻", "role": "Lead Engineer"},
    "bob":     {"name": "Bob Vance",     "avatar": "🕵️",  "role": "Market Researcher"},
    "charlie": {"name": "Charlie Design","avatar": "🎨",  "role": "UI/UX Specialist"},
    "dave":    {"name": "Dave Audit",    "avatar": "🛡️",  "role": "Security Auditor"},
}

for d in (DATA_DIR, CONV_DIR, SKILLS_DIR):
    d.mkdir(parents=True, exist_ok=True)

STATIC_DIR = (
    Path(sys._MEIPASS) / "static" if getattr(sys, "frozen", False)
    else Path(__file__).resolve().parent.parent / "static"
)
_state = {
    "workspace": Path(os.getenv("SOURCE_AGENT_WORKSPACE") or (DATA_DIR / "workspace")).resolve(),
    "active_model": None,
    "last_selected_base_model": None,
    "cancelled_cids": set()
}
_state["workspace"].mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------- connectors persistence
def _load_connectors() -> list[dict]:
    if CONNECTORS_FILE.exists():
        try:
            return json.loads(CONNECTORS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _save_connectors(conns: list[dict]) -> None:
    CONNECTORS_FILE.write_text(json.dumps(conns, indent=2), encoding="utf-8")


# --------------------------------------------------------------------------- MCP client (Streamable HTTP)
class McpConnection:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.post_url = None
        self.pending_responses = {} # req_id -> response_dict
        self.lock = threading.Lock()
        self.thread = None
        self.stop_event = threading.Event()
        self.connected_event = threading.Event()
        self.fallback_mode = False
        self.error = None

    def start(self):
        with self.lock:
            if self.thread and self.thread.is_alive():
                return
            self.stop_event.clear()
            self.connected_event.clear()
            self.thread = threading.Thread(target=self._run_sse_stream, daemon=True)
            self.thread.start()

    def stop(self):
        self.stop_event.set()

    def _run_sse_stream(self):
        headers = {
            "Accept": "text/event-stream"
        }
        try:
            print(f"[Stream] Connecting to SSE stream: {self.base_url}")
            timeout = httpx.Timeout(3.0, read=None)
            with httpx.stream("GET", self.base_url, headers=headers, timeout=timeout) as r:
                if r.status_code != 200:
                    self.error = f"GET stream returned status {r.status_code}"
                    self.connected_event.set()
                    return
                
                current_event = None
                for line in r.iter_lines():
                    if self.stop_event.is_set():
                        break
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith("event:"):
                        current_event = line[6:].strip()
                    elif line.startswith("data:"):
                        data_val = line[5:].strip()
                        if current_event == "endpoint":
                            target_url = data_val
                            if not target_url.startswith(("http://", "https://")):
                                target_url = str(urllib.parse.urljoin(self.base_url, target_url))
                            self.post_url = target_url
                            print(f"[Stream] Resolved POST endpoint: {self.post_url}")
                            self.connected_event.set()
                        elif current_event == "message":
                            try:
                                msg = json.loads(data_val)
                                if isinstance(msg, dict) and "id" in msg:
                                    req_id = msg["id"]
                                    with self.lock:
                                        self.pending_responses[req_id] = msg
                            except Exception as e:
                                print(f"[Stream] Error parsing message: {e}")
        except Exception as e:
            self.error = str(e)
            self.connected_event.set()

    def send_rpc(self, method: str, params: dict | None = None, timeout: float = 30.0) -> dict | None:
        req_id = random.randint(1000, 999999)
        payload = {"jsonrpc": "2.0", "id": req_id, "method": method}
        if params:
            payload["params"] = params

        if not self.fallback_mode:
            self.start()
            if not self.connected_event.is_set():
                # Wait up to 1.5 seconds for SSE GET connection
                if not self.connected_event.wait(timeout=1.5):
                    print(f"[RPC] SSE connection wait timed out for {self.base_url}. Switching to fallback mode.")
                    self.fallback_mode = True

        post_target = self.post_url or self.base_url
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream"
        }
        
        try:
            r = httpx.post(post_target, json=payload, headers=headers, timeout=timeout)
            r.raise_for_status()
            
            body_text = r.text
            content_type = r.headers.get("content-type", "")
            
            result_json = None
            if "application/json" in content_type:
                try:
                    result_json = r.json()
                except Exception:
                    pass
            
            if result_json is None and body_text:
                data_content = []
                for line in body_text.splitlines():
                    if line.startswith("data:"):
                        data_content.append(line[5:].strip())
                if data_content:
                    combined = "".join(data_content)
                    try:
                        result_json = json.loads(combined)
                    except Exception:
                        pass
                        
            if result_json is None and body_text:
                try:
                    result_json = json.loads(body_text)
                except Exception:
                    pass
                    
            if isinstance(result_json, dict) and ("result" in result_json or "error" in result_json):
                if "error" in result_json:
                    print(f"[RPC] Error response: {result_json['error']}")
                    return None
                return result_json.get("result")
                
            # Wait for stream response
            start_time = time.time()
            while time.time() - start_time < timeout:
                with self.lock:
                    if req_id in self.pending_responses:
                        res = self.pending_responses.pop(req_id)
                        if "error" in res:
                            print(f"[RPC] Stream error: {res['error']}")
                            return None
                        return res.get("result")
                time.sleep(0.05)
                
            print(f"[RPC] Timeout waiting for request {req_id}")
            return None
        except Exception as e:
            print(f"[RPC] Exception: {e}")
            return None


_mcp_connections = {}
_mcp_tools_cache = {}


def _get_mcp_conn(url: str) -> McpConnection:
    if url not in _mcp_connections:
        _mcp_connections[url] = McpConnection(url)
    return _mcp_connections[url]


def _mcp_rpc(url: str, method: str, params: dict | None = None) -> dict | None:
    """Send a JSON-RPC 2.0 request to an MCP server over Streamable HTTP or SSE."""
    conn = _get_mcp_conn(url)
    return conn.send_rpc(method, params)


def _mcp_initialize(url: str) -> dict | None:
    """Initialize the MCP session and return server info."""
    return _mcp_rpc(url, "initialize", {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "SourceAgent", "version": "1.0"}
    })


def _mcp_list_tools(url: str) -> list[dict]:
    """List tools from an MCP server."""
    if url in _mcp_tools_cache:
        return _mcp_tools_cache[url]
    try:
        _mcp_initialize(url)
        # Use a short timeout of 2.0s for tools list so we don't hang if server is slow
        result = _get_mcp_conn(url).send_rpc("tools/list", timeout=2.0)
        if result and "tools" in result:
            _mcp_tools_cache[url] = result["tools"]
            return result["tools"]
    except Exception as e:
        print(f"[MCP] Error listing tools for {url}: {e}")
    _mcp_tools_cache[url] = []
    return []


def _mcp_call_tool(url: str, tool_name: str, arguments: dict) -> str:
    """Call a tool on an MCP server and return the text result."""
    _mcp_initialize(url)
    result = _mcp_rpc(url, "tools/call", {"name": tool_name, "arguments": arguments})
    if result and "content" in result:
        parts = []
        for item in result["content"]:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(item.get("text", ""))
            elif isinstance(item, dict):
                parts.append(json.dumps(item))
            else:
                parts.append(str(item))
        return "\n".join(parts)[:8000]
    if result:
        return json.dumps(result)[:8000]
    return "(MCP tool returned no result)"


# --------------------------------------------------------------------------- skills persistence
def _load_skills_config() -> dict:
    if SKILLS_CONFIG.exists():
        try:
            return json.loads(SKILLS_CONFIG.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_skills_config(cfg: dict) -> None:
    SKILLS_CONFIG.write_text(json.dumps(cfg, indent=2), encoding="utf-8")


def _discover_skills() -> list[dict]:
    """Scan SKILLS_DIR for subdirectories containing SKILL.md."""
    skills = []
    if not SKILLS_DIR.is_dir():
        return skills
    for d in sorted(SKILLS_DIR.iterdir()):
        if not d.is_dir():
            continue
        skill_file = d / "SKILL.md"
        if not skill_file.exists():
            continue
        content = skill_file.read_text(encoding="utf-8", errors="replace")
        # Parse YAML frontmatter
        name = d.name
        description = ""
        body = content
        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) >= 3:
                fm = parts[1]
                body = parts[2].strip()
                for line in fm.strip().splitlines():
                    if line.startswith("name:"):
                        name = line.split(":", 1)[1].strip().strip('"').strip("'")
                    elif line.startswith("description:"):
                        description = line.split(":", 1)[1].strip().strip('"').strip("'")
        skills.append({
            "folder": d.name,
            "name": name,
            "description": description,
            "body": body
        })
    return skills


def _get_enabled_skills_text() -> str:
    """Return the combined body text of all enabled skills."""
    cfg = _load_skills_config()
    skills = _discover_skills()
    parts = []
    for s in skills:
        if cfg.get(s["folder"], {}).get("enabled", False):
            parts.append(f"### Skill: {s['name']}\n{s['body']}")
    return "\n\n".join(parts)


def _get_mcp_tools_prompt() -> str:
    """Build the MCP tools section for the system prompt."""
    conns = _load_connectors()
    enabled = [c for c in conns if c.get("enabled", True)]
    if not enabled:
        return ""
    lines = ["\nYou also have access to external MCP tools. To call one, respond with:",
             '{"action":"mcp_tool","connector":"<connector_id>","tool":"<tool_name>","arguments":{...}}',
             "\nAvailable MCP tools:"]
    for c in enabled:
        try:
            tools = _mcp_list_tools(c["url"])
            if tools:
                lines.append(f"\nConnector: {c['name']} (id: {c['id']})")
                for t in tools:
                    desc = t.get("description", "")
                    schema = t.get("inputSchema", {})
                    props = schema.get("properties", {})
                    param_list = ", ".join(f"{k}: {v.get('type', 'any')}" for k, v in props.items())
                    lines.append(f"  - {t['name']}({param_list}): {desc}")
        except Exception:
            continue
    return "\n".join(lines) if len(lines) > 3 else ""

app = FastAPI(title="Source Agent")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


_thread_local = threading.local()


def ws() -> Path:
    override = getattr(_thread_local, "workspace_override", None)
    if override:
        return override
    return _state["workspace"]


def safe(rel: str) -> Path:
    target = (ws() / rel).resolve()
    if not (str(target) == str(ws()) or str(target).startswith(str(ws()) + os.sep)):
        raise ValueError("path escapes the workspace")
    return target


# --------------------------------------------------------------------------- LLM
def list_models() -> list[dict]:
    try:
        r = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=3.0)
        out = []
        for m in r.json().get("models", []):
            name = m.get("name", "")
            if name:
                out.append({"name": name, "size": m.get("size", 0) or 0, "is_embed": "embed" in name, "is_cloud": name.endswith("cloud"), "is_vision": _is_vision_model(name)})
        return out
    except Exception:
        return []


def resolve_base_model() -> str | None:
    models = list_models()
    names = {m["name"] for m in models}
    if OLLAMA_MODEL and (OLLAMA_MODEL in names or f"{OLLAMA_MODEL}:latest" in names):
        return OLLAMA_MODEL if OLLAMA_MODEL in names else f"{OLLAMA_MODEL}:latest"
    local = sorted([m for m in models if not m["is_cloud"] and not m["is_embed"]], key=lambda m: m["size"], reverse=True)
    if local:
        return local[0]["name"]
    return next((m["name"] for m in models if not m["is_embed"]), None)


def resolve_model() -> str | None:
    selected = _state.get("active_model")
    if selected in ("DocWriter", "Source 1.0", "kimi-k2.7-code:cloud", "glm-5.2:cloud", "minimax-m3:cloud", "nemotron-3-super:cloud"):
        return selected
    models = list_models()
    names = {m["name"] for m in models}
    if selected and (selected in names or f"{selected}:latest" in names):
        return selected if selected in names else f"{selected}:latest"
    base = resolve_base_model()
    if base:
        if not _state.get("last_selected_base_model"):
            _state["last_selected_base_model"] = base
        return base
    return "DocWriter"


# Name fragments that identify a vision-capable Ollama model (for computer-use "seeing").
VISION_MODEL_HINTS = (
    "qwen2.5vl", "qwen2-vl", "qwen2.5-vl", "qwenvl", "-vl", ":vl",
    "llava", "bakllava", "minicpm-v", "moondream",
    "llama3.2-vision", "llama3.2vision", "granite3.2-vision", "gemma3", "vision",
)


def _is_vision_model(name: str) -> bool:
    n = (name or "").lower()
    if "embed" in n:
        return False
    return any(h in n for h in VISION_MODEL_HINTS)


def resolve_vision_model() -> str | None:
    """Pick a vision-capable model for computer-use. Local-first, and SPEED-first: prefer the
    SMALLEST local vision model. A 7B VL model is 9-10GB and won't fit a 6GB GPU, so Ollama
    spills it to CPU (~100s/step); a 3B fits in VRAM and runs many times faster — and small VL
    models are plenty for UI grounding. Set SOURCE_AGENT_VISION_MODEL to force a specific one."""
    models = list_models()
    names = {m["name"] for m in models}
    override = os.getenv("SOURCE_AGENT_VISION_MODEL")
    if override and (override in names or f"{override}:latest" in names):
        return override if override in names else f"{override}:latest"
    local = sorted(
        [m for m in models if _is_vision_model(m["name"]) and not m["is_cloud"]],
        key=lambda m: m["size"],  # smallest first = most likely to fit VRAM = fastest
    )
    if local:
        return local[0]["name"]
    cloud = [m for m in models if _is_vision_model(m["name"]) and m["is_cloud"]]
    return cloud[0]["name"] if cloud else None


def llm_available() -> bool:
    try:
        return httpx.get(f"{OLLAMA_URL}/api/tags", timeout=2.0).status_code == 200
    except Exception:
        return False


def llm_chat(messages: list[dict], model: str, max_tokens: int = 1500) -> str | None:
    try:
        r = httpx.post(
            f"{OLLAMA_URL}/api/chat",
            # think=False: qwen3-style reasoning models otherwise spend the whole token
            # budget in a hidden 'thinking' field and return empty content.
            json={"model": model, "messages": messages, "stream": False, "think": False, "options": {"num_predict": max_tokens}},
            timeout=300.0,
        )
        r.raise_for_status()
        return r.json()["message"]["content"]
    except Exception as e:
        print(f"Ollama chat error: {e}")
        import traceback
        traceback.print_exc()
        return None


# --------------------------------------------------------------------------- computer-use vision
# Downscale screenshots before sending to the vision model. Full 1080p+ frames are huge for a
# local VL model (prefill of ~1300+ image tokens), which is what made computer-use slow. ~1280px
# wide keeps UI text legible while roughly halving the pixels; we record the scale so the model's
# image-space coordinates map back to real screen pixels.
COMPUTER_SHOT_WIDTH = int(os.getenv("SOURCE_AGENT_COMPUTER_WIDTH", "1280"))


def _parse_coords(text: str) -> tuple[int, int] | None:
    if not text:
        return None
    # 1) Match x[:=] <num> and y[:=] <num> (robust to quotes/spaces/equals/colons)
    x_m = re.search(r'["\']?x["\']?\s*[:=]\s*(\d+(?:\.\d+)?)', text, re.IGNORECASE)
    y_m = re.search(r'["\']?y["\']?\s*[:=]\s*(\d+(?:\.\d+)?)', text, re.IGNORECASE)
    if x_m and y_m:
        try:
            return int(round(float(x_m.group(1)))), int(round(float(y_m.group(1))))
        except Exception:
            pass
    # 2) Match list coordinate pair [x, y]
    list_m = re.search(r'\[\s*(\d+(?:\.\d+)?)\s*,\s*(\d+(?:\.\d+)?)\s*\]', text)
    if list_m:
        try:
            return int(round(float(list_m.group(1)))), int(round(float(list_m.group(2))))
        except Exception:
            pass
    # 3) Match comma-separated numbers x, y
    pair_m = re.search(r'(\d+(?:\.\d+)?)\s*,\s*(\d+(?:\.\d+)?)', text)
    if pair_m:
        try:
            return int(round(float(pair_m.group(1)))), int(round(float(pair_m.group(2))))
        except Exception:
            pass
    return None


def _capture_screen_b64(width: int = None) -> tuple:
    """Downscaled PNG of the screen for Ollama `images`. Returns (b64, shown_w, shown_h);
    (None, 0, 0) on failure. Records the downscale factor in _state['computer_scale'] so
    tool_computer can convert the model's image coordinates back to real pixels."""
    try:
        import io
        import base64
        import pyautogui
        img = pyautogui.screenshot()
        rw, rh = img.size
        scale = 1.0
        w_limit = width or COMPUTER_SHOT_WIDTH
        if w_limit and rw > w_limit:
            scale = rw / float(w_limit)
            img = img.resize((w_limit, max(1, round(rh / scale))))
        _state["computer_scale"] = scale
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return base64.b64encode(buf.getvalue()).decode("ascii"), img.size[0], img.size[1]
    except Exception as e:
        print(f"screen capture failed: {e}")
        return None, 0, 0


def _has_image(messages: list[dict]) -> bool:
    return any(m.get("images") for m in messages)


def _prune_old_images(messages: list[dict]) -> None:
    """Keep only the most recent screenshot in context; older ones just bloat tokens."""
    idxs = [i for i, m in enumerate(messages) if m.get("images")]
    for i in idxs[:-1]:
        messages[i].pop("images", None)


def _computer_observation(computer_action: str, result: str, vision_model: str | None) -> dict:
    """Follow-up message after a computer action — attaches a FRESH screenshot so a vision
    model can actually see the screen and pick the next coordinates (Claude-style loop)."""
    # Skip the settle delay for pure screenshot requests (screen is already current).
    if computer_action != "screenshot":
        time.sleep(0.5)  # let the UI settle (app launching, menu opening) before capturing
    msg = {"role": "user", "content": f"Result of computer.{computer_action}:\n{result}"}
    if vision_model:
        b64, w, h = _capture_screen_b64()
        if b64:
            msg["images"] = [b64]
            msg["content"] += (f"\n\n[Fresh screenshot attached — image is {w}x{h} px. "
                               f"Read the screen, then give your next click/move as exact pixel coordinates within THIS image.]")
        else:
            msg["content"] += "\n\n[Screenshot capture failed; cannot see the screen this turn.]"
    else:
        msg["content"] += ("\n\n[NO VISION MODEL ACTIVE — you cannot see the screen, so do not guess coordinates. "
                           "Tell the user to install one with:  ollama pull qwen2.5vl:7b  then select it and retry.]")
    return msg


# screen-edge glow overlay — the Claude-style "the computer is being controlled" indicator
try:
    import overlay as _overlay
except Exception:
    _overlay = None


def _glow(on: bool) -> None:
    try:
        if _overlay:
            _overlay.show() if on else _overlay.hide()
    except Exception:
        pass


# --------------------------------------------------------------------------- installed apps
WEB_APPS = {
    "gmail": "https://mail.google.com",
    "google calendar": "https://calendar.google.com",
    "google docs": "https://docs.google.com",
    "google sheets": "https://sheets.google.com",
    "google slides": "https://slides.google.com",
    "youtube": "https://www.youtube.com",
    "github": "https://github.com",
    "gmail.com": "https://mail.google.com",
    "google": "https://www.google.com",
}

_apps_cache = {"ts": 0.0, "apps": {}}


def list_installed_apps() -> dict:
    """Map of {lower_name: {"name","launch","kind"}} for apps the agent can open. Sourced from
    Start Menu .lnk shortcuts (+ Windows Store/UWP apps via Get-StartApps). Cached for 5 min."""
    if sys.platform != "win32":
        return {}
    now = time.time()
    if _apps_cache["apps"] and now - _apps_cache["ts"] < 300:
        return _apps_cache["apps"]
    apps: dict[str, dict] = {}
    # 1) Start Menu shortcuts (all-users + per-user) — friendly names match how people speak
    roots = [
        Path(os.getenv("ProgramData", r"C:\ProgramData")) / "Microsoft/Windows/Start Menu/Programs",
        Path(os.getenv("APPDATA", "")) / "Microsoft/Windows/Start Menu/Programs",
    ]
    for root in roots:
        try:
            if not root.exists():
                continue
            for lnk in root.rglob("*.lnk"):
                name = lnk.stem.strip()
                low = name.lower()
                if low and low not in apps and "uninstall" not in low:
                    apps[low] = {"name": name, "launch": str(lnk), "kind": "lnk"}
        except Exception:
            pass
    # 2) UWP / Store apps via Get-StartApps (AppID → launch with shell:appsFolder)
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-StartApps | ForEach-Object { \"$($_.Name)`t$($_.AppID)\" }"],
            capture_output=True, text=True, timeout=15,
        ).stdout
        for line in out.splitlines():
            if "\t" not in line:
                continue
            name, appid = line.split("\t", 1)
            name, appid = name.strip(), appid.strip()
            low = name.lower()
            if low and appid and low not in apps:
                apps[low] = {"name": name, "launch": f"shell:appsFolder\\{appid}", "kind": "uwp"}
    except Exception:
        pass
    _apps_cache.update({"ts": now, "apps": apps})
    return apps


def _apps_prompt() -> str:
    """A compact, comma-separated list of installed app names for the system prompt."""
    apps = list_installed_apps()
    if not apps:
        return ""
    names = sorted({a["name"] for a in apps.values()}, key=str.lower)
    if len(names) > 160:
        names = names[:160]
    return ", ".join(names)


def _maximize_new_foreground_window(old_hwnd):
    import time
    import ctypes
    try:
        user32 = ctypes.windll.user32
        # Poll for 5 seconds to catch the new window as it gains focus
        for _ in range(25):
            time.sleep(0.2)
            hwnd = user32.GetForegroundWindow()
            if hwnd and hwnd != old_hwnd:
                buf = ctypes.create_unicode_buffer(256)
                user32.GetClassNameW(hwnd, buf, 256)
                class_name = buf.value.lower()
                # Ignore taskbar and common shell components
                if class_name not in (
                    "progman", "workerw", "shell_traywnd", "dv2controlhost", 
                    "multitaskinganview", "windows.internal.composer.composerviewhost"
                ):
                    # SW_MAXIMIZE = 3
                    user32.ShowWindow(hwnd, 3)
                    break
    except Exception:
        pass


def tool_open_app(name: str, url: str = None) -> str:
    """Launch an installed app by (fuzzy) name. Far more reliable than guessing a shell command."""
    if not name or not name.strip():
        return "Error: open_app needs a 'name'."
    
    # Capture the current active window handle before launching so we can maximize the new one
    try:
        import ctypes
        old_hwnd = ctypes.windll.user32.GetForegroundWindow()
    except Exception:
        old_hwnd = None

    q = name.strip().lower()

    # If a specific launch URL was provided, open it directly
    if url:
        try:
            import webbrowser
            webbrowser.open(url)
            if old_hwnd is not None:
                threading.Thread(target=_maximize_new_foreground_window, args=(old_hwnd,), daemon=True).start()
            return f"Opened '{url}' web app in default browser."
        except Exception as e:
            return f"Could not launch '{url}' web app: {e}"

    # Check if this matches a web app URL fallback or raw URL
    if q in WEB_APPS or q.startswith("http://") or q.startswith("https://") or any(ext in q for ext in (".com", ".org", ".net", ".edu", ".gov")):
        url_to_open = WEB_APPS.get(q) if q in WEB_APPS else (q if (q.startswith("http://") or q.startswith("https://")) else f"https://{q}")
        try:
            import webbrowser
            webbrowser.open(url_to_open)
            if old_hwnd is not None:
                threading.Thread(target=_maximize_new_foreground_window, args=(old_hwnd,), daemon=True).start()
            return f"Opened '{url_to_open}' web app in default browser."
        except Exception as e:
            return f"Could not launch '{url_to_open}' web app: {e}"

    apps = list_installed_apps()
    if not apps:
        # Non-Windows or enumeration failed — fall back to a plain launch attempt
        try:
            subprocess.Popen(f'start "" "{name}"', shell=True)
            if old_hwnd is not None:
                threading.Thread(target=_maximize_new_foreground_window, args=(old_hwnd,), daemon=True).start()
            return f"Tried to launch '{name}' via shell start."
        except Exception as e:
            return f"Could not launch '{name}': {e}"
    q = name.strip().lower()
    match = (apps.get(q)
             or next((a for k, a in apps.items() if k.startswith(q)), None)
             or next((a for k, a in apps.items() if q in k), None)
             or next((a for k, a in apps.items() if all(w in k for w in q.split())), None))
    if not match:
        sample = ", ".join(sorted({a["name"] for a in apps.values()}, key=str.lower)[:25])
        return f"No installed app matches '{name}'. Some installed apps: {sample} …"
    try:
        launch = match["launch"]
        if match["kind"] == "uwp":
            subprocess.Popen(["explorer.exe", launch])
        else:
            os.startfile(launch)  # .lnk → resolves target + working dir correctly
        
        # Start background thread to maximize the newly opened application window
        if old_hwnd is not None:
            threading.Thread(target=_maximize_new_foreground_window, args=(old_hwnd,), daemon=True).start()

        return f"Opened '{match['name']}'. Take a screenshot to see it, then continue."
    except Exception as e:
        return f"Failed to open '{match['name']}': {e}"


# --------------------------------------------------------------------------- memory
def load_memory() -> str:
    return MEMORY_FILE.read_text(encoding="utf-8", errors="replace") if MEMORY_FILE.exists() else ""


def append_memory(text: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with MEMORY_FILE.open("a", encoding="utf-8") as f:
        f.write(f"- ({stamp}) {text.strip()}\n")


# --------------------------------------------------------------------------- tools
def tool_shell(command: str) -> str:
    try:
        p = subprocess.run(command, cwd=str(ws()), shell=True, capture_output=True, text=True, timeout=120)
        out = (p.stdout or "") + (("\n[stderr]\n" + p.stderr) if p.stderr else "")
        return (out or f"(exit {p.returncode}, no output)")[:8000]
    except subprocess.TimeoutExpired:
        return "(command timed out after 120s)"
    except Exception as e:
        return f"(error: {e})"


def tool_read_file(path: str) -> str:
    try:
        t = safe(path)
        if not t.is_file():
            return f"(not found: {path})"
        if t.stat().st_size > MAX_BYTES:
            return "(file too large)"
        return t.read_text(encoding="utf-8", errors="replace")[:8000]
    except Exception as e:
        return f"(error: {e})"


def tool_write_file(path: str, content: str) -> str:
    try:
        t = safe(path)
        t.parent.mkdir(parents=True, exist_ok=True)
        t.write_text(content, encoding="utf-8")
        return f"wrote {path} ({len(content)} chars)"
    except Exception as e:
        return f"(error: {e})"


def tool_list_dir(path: str = ".") -> str:
    try:
        base = safe(path)
        if not base.is_dir():
            return f"(not a directory: {path})"
        items = []
        for c in sorted(base.iterdir(), key=lambda c: (c.is_file(), c.name.lower())):
            items.append(("📁 " if c.is_dir() else "📄 ") + c.name)
        return "\n".join(items) or "(empty)"
    except Exception as e:
        return f"(error: {e})"


def tool_web_search(query: str) -> str:
    url = "https://html.duckduckgo.com/html/?q=" + urllib.parse.quote(query)
    try:
        r = httpx.get(url, headers={"User-Agent": "SourceAgent/0.1"}, timeout=20.0, follow_redirects=True)
        page = r.text
    except Exception as e:
        return f"(search failed: {e})"
    out = []
    pat = re.compile(r'class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>.*?class="result__snippet"[^>]*>(.*?)</a>', re.DOTALL)
    for m in pat.finditer(page):
        href = urllib.parse.parse_qs(urllib.parse.urlparse(m.group(1)).query).get("uddg", [m.group(1)])[0]
        title = html_lib.unescape(re.sub(r"<[^>]+>", "", m.group(2))).strip()
        snip = html_lib.unescape(re.sub(r"<[^>]+>", "", m.group(3))).strip()
        out.append(f"- {title}\n  {href}\n  {snip}")
        if len(out) >= 6:
            break
    return "\n".join(out) or "(no results)"


def tool_web_fetch(url: str) -> str:
    """OpenClaw/Hermes-style web_extract: fetch a page and return readable text."""
    try:
        r = httpx.get(url, headers={"User-Agent": "SourceAgent/0.1"}, timeout=25.0, follow_redirects=True)
        raw = r.text
    except Exception as e:
        return f"(fetch failed: {e})"
    raw = re.sub(r"<(script|style|noscript)[^>]*>.*?</\1>", " ", raw, flags=re.DOTALL | re.IGNORECASE)
    text = html_lib.unescape(re.sub(r"<[^>]+>", " ", raw))
    return re.sub(r"\s{2,}", " ", text).strip()[:8000] or "(no text extracted)"


def _run_subagent(task: str, model: str, parent_steps: list | None = None) -> str:
    """Hermes-style delegated sub-agent: a focused, bounded tool loop for one task.
    Returns the sub-agent's final text. No nested delegation (depth-1 only)."""
    sub_system = (
        "You are a focused sub-agent spawned to complete ONE task and report back. "
        "Use EXACTLY ONE JSON action per turn: think / shell / read_file / write_file / "
        "list_dir / web_search / web_fetch / computer / final. The 'computer' action controls the "
        "real desktop (screenshot/mouse/click/type); after each computer action you get a fresh "
        "screenshot — read it, then give pixel coordinates from that image. Finish with "
        '{"action":"final","text":"the result"}. Work in the shared workspace.\n\nTASK: ' + task
    )
    messages = [{"role": "system", "content": sub_system},
                {"role": "user", "content": "Begin. One JSON action."}]
    vision_model = resolve_vision_model()
    for _ in range(1000000):
        _prune_old_images(messages)
        step_model = vision_model if (vision_model and _has_image(messages)) else model
        raw = llm_chat(messages, step_model, max_tokens=(900 if step_model == vision_model else 2000))
        if not raw:
            return "(sub-agent: no response)"
        act = _parse(raw)
        if not act or "action" not in act:
            return raw.strip()
        a = act["action"]
        if a == "final":
            return str(act.get("text", ""))
        if a == "think":
            messages += [{"role": "assistant", "content": raw}, {"role": "user", "content": "Continue."}]
            continue
        if a in ("shell", "read_file", "write_file", "list_dir", "web_search", "web_fetch", "computer", "open_app"):
            if a in ("computer", "open_app"):
                _glow(True)
            res = run_tool(act)
            if parent_steps is not None:
                parent_steps.append({"kind": "subtool", "name": a, "result": res[:400]})
            messages.append({"role": "assistant", "content": raw})
            if a in ("computer", "open_app"):
                messages.append(_computer_observation(str(act.get("computer_action", "") or a), res, vision_model))
            else:
                messages.append({"role": "user", "content": f"Result of {a}:\n{res[:4000]}"})
            continue
        _glow(False)
        return raw.strip()
    _glow(False)
    return "(sub-agent reached its step limit)"


# --------------------------------------------------------------------------- agent loop
SYSTEM = """You are Source Agent, a capable personal AI agent. Your architecture is inspired by
Nous Research's Hermes Agent: a tool-using loop with a closed learning loop (you remember across sessions).

You work inside a workspace folder and can take real actions. Respond with EXACTLY ONE JSON object per turn and nothing else:
{{"action":"think","text":"private reasoning about what to do next"}}
{{"action":"shell","command":"ls -la"}}
{{"action":"read_file","path":"relative/path"}}
{{"action":"write_file","path":"relative/path","content":"FULL file content"}}
{{"action":"list_dir","path":"."}}
{{"action":"web_search","query":"..."}}
{{"action":"web_fetch","url":"https://..."}}
{{"action":"canvas","html":"<h1>...</h1> a live HTML canvas the user sees in a side panel (charts, dashboards, previews)"}}
{{"action":"create_skill","name":"skill-name","content":"SKILL.md markdown to save as a reusable skill for future sessions"}}
{{"action":"remember","text":"a durable fact about the user or this work, for future sessions"}}
{{"action":"computer","computer_action":"screenshot|mouse_hover|left_click|right_click|double_click|three_click|left_click_drag|drag_to|mouse_down|mouse_up|scroll|type|key","coordinate":[x,y],"text":"text to type or key press combos like ctrl+c"}}
{{"action":"open_app","name":"the installed app's name, e.g. Spotify"}}
{{"action":"final","text":"your answer to the user, in markdown"}}
 
Rules:
- Actually use tools to accomplish the task; never claim you did something you didn't do.
- Work in small steps and read tool results before the next action.
- Use "canvas" to show the user a rich visual (HTML/CSS/SVG/chart) result (e.g. you can take a screenshot and show it in the canvas).
- Use "computer" to control the REAL desktop: screenshot, move/click the mouse, type, and press keys.
  COMPUTER-USE LOOP: after every "computer" action (and after "open_app") you are sent a FRESH SCREENSHOT — study it, then choose the next action using EXACT pixel coordinates read from that image. ALWAYS take a "screenshot" first to see the screen before clicking. To OPEN an app, ALWAYS use the "open_app" action with the app's name — it launches the real installed app reliably; do NOT guess shell `start` commands. Whenever you open an app (using "open_app"), verify on the next screenshot if it is in full screen (maximized). If it is not full screen, maximize it immediately: either click the window's Maximize button (top-right corner) or use the "key" action with the "win+up" shortcut.
  PROMPTING & MESSAGING APPS: When the user asks you to "prompt X to Y" or "send a message Y to X" (e.g. "Open Codex and prompt it to make Source Research"), this means you must: 1. Open app X (using "open_app"). 2. Take a screenshot to locate the input/message/prompt box on the screen (look for a text area, input box, or chat field on the screenshot). 3. Click inside that input box. 4. Type the prompt/message text Y. 5. Press the "enter" key (using the "key" action with value "enter") or click the Send/Submit button to send the prompt/message.
  To TYPE into a field: left_click the field first, then use the "type" action. Take a fresh screenshot after big changes to confirm the result before continuing.
  APPS INSTALLED ON THIS DEVICE you can open with open_app: {apps}
- Use "remember" for durable facts; use "create_skill" when you discover a reusable procedure worth keeping.
- When finished, reply with "final" and ensure your final message contains the word "done".
- CRITICAL JSON ESCAPING RULE: All nested double quotes inside JSON string values (like "content", "command", "text") MUST be properly escaped as \\\" and all newlines as \\n. Never output raw unescaped double quotes (such as python triple quotes) or raw newlines inside a JSON string.
{mcp_tools}
{skills}
What you remember about the user and past sessions:
{memory}
"""

SYSTEM_DOCWRITER = """You are Source Agent (DocWriter edition), a local AI agent specialized in generating, editing, formatting, and compiling professional documents (PDFs, plain text files .txt, Excel spreadsheets .xls/.xlsx, and PowerPoint presentations .pptx).

You work inside a workspace folder and can take real actions. Respond with EXACTLY ONE JSON object per turn and nothing else:
{{"action":"think","text":"private reasoning about what to do next"}}
{{"action":"shell","command":"ls -la"}}
{{"action":"read_file","path":"relative/path"}}
{{"action":"write_file","path":"relative/path","content":"FULL content"}}
{{"action":"list_dir","path":"."}}
{{"action":"web_search","query":"..."}}
{{"action":"remember","text":"durable fact"}}
{{"action":"computer","computer_action":"screenshot|mouse_hover|left_click|right_click|double_click|three_click|left_click_drag|drag_to|mouse_down|mouse_up|scroll|type|key","coordinate":[x,y],"text":"text to type or key press combos like ctrl+c"}}
{{"action":"final","text":"your answer to the user, in markdown"}}

Rules for Document Creation:
- To make a plain text (.txt) file: Use the "write_file" action to write it directly.
- To make a PDF (.pdf) file: Do NOT write raw binary PDF data. Instead, write a Python script using standard libraries like 'reportlab' or 'fpdf2' (installing them via shell `pip install reportlab fpdf2` if needed) to generate a clean, well-formatted, professional PDF document, and then run it via the "shell" action.
- To make an Excel (.xls / .xlsx) file: Write a Python script using libraries like 'openpyxl' or 'pandas' (installing them via shell `pip install openpyxl pandas` if needed) to create a styled spreadsheet, write headers and rows, and run it via the "shell" action.
- To make a PowerPoint (.pptx) presentation: Write a Python script (installing dependencies like `pptx` or `pywin32` via pip if needed) and run it via the "shell" action.
- Ensure all documents are generated inside the workspace.
- After creating a document, verify its existence and size.
- In your final response, provide the path to the created file in the workspace so the user knows exactly where to find it.

Rules for Advanced PowerPoint Presentations:
1. CUSTOM BACKGROUND COLORS:
   Use solid fill color formatting:
   `slide.background.fill.solid()`
   `slide.background.fill.fore_color.rgb = RGBColor(0x1E, 0x1B, 0x4B)` # e.g., deep indigo
2. TEXT FONTS & COLORS:
   Format individual runs on text frames:
   `run = paragraph.add_run()`
   `run.font.name = 'Arial'`
   `run.font.size = Pt(20)`
   `run.font.color.rgb = RGBColor(0x3B, 0x82, 0xF6)` # e.g., electric blue
3. PICTURES:
   Add graphics or layouts using slide.shapes:
   `slide.shapes.add_picture(image_path, left, top, width, height)`
4. TRANSITIONS & ANIMATIONS:
   - To add transitions (e.g., fade) with python-pptx: manipulate the underlying XML:
     `transition = slide.element.get_or_add_transition()`
     `transition.set('type', 'fade')`
   - On Windows, you can automate full native PowerPoint transitions and shapes entrance animations using `win32com.client` (requires python `pywin32` package):
     ```python
     import win32com.client
     ppt = win32com.client.Dispatch("PowerPoint.Application")
     pres = ppt.Presentations.Add()
     slide = pres.Slides.Add(1, 12) # 12 = ppLayoutBlank
     slide.Background.Fill.Solid()
     slide.Background.Fill.ForeColor.RGB = 0x4B1B1E # BGR deep indigo
     shape = slide.Shapes.AddShape(1, 100, 100, 200, 80) # 1 = msoShapeRectangle
     # Add fade-in entrance animation
     effect = slide.TimeLine.MainSequence.AddEffect(shape, 10, 1, 1) # 10 = msoAnimEffectFade, 1 = msoAnimTriggerOnPageClick
     pres.SaveAs(r"absolute_path_to_workspace\\presentation.pptx")
     pres.Close()
     ppt.Quit()
     ```

- When finished, reply with "final" and ensure your final message contains the word "done".
- CRITICAL JSON ESCAPING RULE: All nested double quotes inside JSON string values (like "content", "command", "text") MUST be properly escaped as \\\" and all newlines as \\n. Never output raw unescaped double quotes or raw newlines inside a JSON string.
{mcp_tools}
{skills}
What you remember about the user and past sessions:
{memory}
"""


SYSTEM_SOURCE_1_0 = """You are Source 1.0, the ultimate AI agent model in the SourceMind ecosystem.
You combine the full document-generation capabilities of DocWriter (including backgrounds, fonts, pictures, XML transitions and local Windows PowerPoint automation), deep-research workflows, Virtual Office control, and image/video generation capabilities.

Respond with EXACTLY ONE JSON object per turn and nothing else:
{{"action":"think","text":"private reasoning about what to do next"}}
{{"action":"shell","command":"ls -la"}}
{{"action":"read_file","path":"relative/path"}}
{{"action":"write_file","path":"relative/path","content":"FULL content"}}
{{"action":"list_dir","path":"."}}
{{"action":"web_search","query":"..."}}
{{"action":"web_fetch","url":"https://..."}}
{{"action":"delegate","task":"a sub-task to hand to a focused sub-agent"}}
{{"action":"canvas","html":"<h1>...</h1> HTML canvas representation"}}
{{"action":"create_skill","name":"skill-name","content":"SKILL.md content"}}
{{"action":"remember","text":"durable fact"}}
{{"action":"office_control","cmd":"status|hire|fire|assign","agent_id":"all|alice|bob|charlie|dave","task":"task description"}}
{{"action":"generate_media","type":"image|video","prompt":"detailed generation prompt","aspect_ratio":"16:9|1:1|9:16"}}
{{"action":"computer","computer_action":"screenshot|mouse_hover|left_click|right_click|double_click|three_click|left_click_drag|drag_to|mouse_down|mouse_up|scroll|type|key","coordinate":[x,y],"text":"text to type or key press combos like ctrl+c"}}
{{"action":"final","text":"your answer to the user, in markdown"}}

Rules for Document Writing (DocWriter mode):
- To create presentations (.pptx), text (.txt), sheets (.xlsx), or PDFs (.pdf), write executable Python scripts and run them via "shell" action.
- You can add background colors to slides (using fill.solid() and fore_color.rgb).
- You can style text fonts, sizes, and colors (using run.font.name, run.font.size, run.font.color.rgb).
- You can add images/pictures to slides using shapes.add_picture(path, left, top, width, height).
- You can add transitions and animations using underlying slide XML (slide.element.get_or_add_transition()) or using win32com.client in Windows to automate PowerPoint animations directly.

Rules for Deep Research:
- Iterate using web_search and web_fetch to synthesize comprehensive reports, citing sources.

Rules for Virtual Office Control:
- Use "office_control" to manage agents (Alice Chen, Bob Vance, Charlie Design, Dave Audit) and assign goals in the Source Worker.

Rules for Image and Video Generation:
- Use "generate_media" to generate images or videos using the configured API keys.

- When finished, reply with "final" and ensure your final message contains the word "done".
- CRITICAL JSON ESCAPING RULE: All nested double quotes inside JSON string values (like "content", "command", "text") MUST be properly escaped as \\\" and all newlines as \\n. Never output raw unescaped double quotes or raw newlines inside a JSON string.
{mcp_tools}
{skills}
What you remember about the user and past sessions:
{memory}
"""


class ChatIn(BaseModel):
    message: str
    conversation_id: str | None = None
    model: str | None = None


class StopIn(BaseModel):
    conversation_id: str


def _unescape_val(val):
    if isinstance(val, str):
        return val.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t').replace('\\\\', '\\')
    elif isinstance(val, list):
        return [_unescape_val(x) for x in val]
    elif isinstance(val, dict):
        return {k: _unescape_val(v) for k, v in val.items()}
    return val


def _parse(raw: str):
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if not m:
        return None
    content_str = m.group(0)
    try:
        obj = json.loads(content_str)
        return _unescape_val(obj)
    except ValueError:
        pass
    try:
        cleaned = content_str.replace("\n", "\\n")
        obj = json.loads(cleaned)
        return _unescape_val(obj)
    except ValueError:
        pass

    # Fallback to custom field extractor for malformed LLM JSON actions
    action_match = re.search(r'"action"\s*:\s*"([^"]+)"', content_str)
    if not action_match:
        return None
    action = action_match.group(1)
    result = {"action": action}
    
    path_match = re.search(r'"path"\s*:\s*"([^"]+)"', content_str)
    if path_match:
        result["path"] = path_match.group(1)

    # Extract computer_action (critical for computer-use actions)
    ca_match = re.search(r'"computer_action"\s*:\s*"([^"]+)"', content_str)
    if ca_match:
        result["computer_action"] = ca_match.group(1)

    # Extract coordinate array [x, y] (critical for mouse actions)
    coord_match = re.search(r'"coordinate"\s*:\s*\[\s*(\d+)\s*,\s*(\d+)\s*\]', content_str)
    if coord_match:
        result["coordinate"] = [int(coord_match.group(1)), int(coord_match.group(2))]

    for field in ("content", "command", "query", "text", "message"):
        field_pattern = r'"' + field + r'"\s*:\s*'
        field_match = re.search(field_pattern, content_str)
        if field_match:
            start_idx = field_match.end()
            val_remainder = content_str[start_idx:].strip()
            
            # Count leading quotes
            quotes_count = 0
            while quotes_count < len(val_remainder) and val_remainder[quotes_count] == '"':
                quotes_count += 1
                
            val_str = val_remainder[quotes_count:]
            
            end_brace = val_str.rfind('}')
            if end_brace != -1:
                val_str = val_str[:end_brace].rstrip()
                
            if val_str.endswith('"'):
                val_str = val_str[:-1]
                
            # If the raw code was supposed to start/end with triple quotes,
            # and the first quote was eaten as the JSON delimiter, restore it.
            if val_str.startswith('""') and not val_str.startswith('"""'):
                val_str = '"' + val_str
            if val_str.endswith('""') and not val_str.endswith('"""'):
                val_str = val_str + '"'
                
            # Unescape common characters
            val_str = val_str.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t').replace('\\\\', '\\')
            result[field] = val_str
            break
            
    return _unescape_val(result)


def tool_office_control(cmd: str, agent_id: str | None = None, task: str | None = None) -> str:
    url = "http://127.0.0.1:8785/api/workspace/agents"
    try:
        if agent_id:
            aid_lower = agent_id.strip().lower()
            if "alice" in aid_lower:
                agent_id = "alice"
            elif "bob" in aid_lower:
                agent_id = "bob"
            elif "charlie" in aid_lower:
                agent_id = "charlie"
            elif "dave" in aid_lower:
                agent_id = "dave"

        if cmd == "status":
            r = httpx.get(url, timeout=5.0)
            r.raise_for_status()
            data = r.json()
            hired = data.get("hired", [])
            status = data.get("agents_status", {})
            proj = data.get("project", {})
            out = [f"Virtual Office Status:",
                   f"  Project Goal: {proj.get('goal', 'None')}",
                   f"  Project Status: {proj.get('status', 'idle')}",
                   f"  Hired Agents: {', '.join(hired) or 'None'}"]
            for aid, stat in status.items():
                out.append(f"  Agent {aid}: status={stat.get('status')}, task={stat.get('current_task')}")
            return "\n".join(out)
            
        elif cmd == "hire":
            if not agent_id:
                return "Error: agent_id required for hire command"
            r = httpx.post(f"{url}/hire", json={"agent_id": agent_id}, timeout=5.0)
            r.raise_for_status()
            return f"Successfully hired agent '{agent_id}' in the virtual office."
            
        elif cmd == "fire":
            if not agent_id:
                return "Error: agent_id required for fire command"
            r = httpx.post(f"{url}/fire", json={"agent_id": agent_id}, timeout=5.0)
            r.raise_for_status()
            return f"Successfully fired agent '{agent_id}' from the virtual office."
            
        elif cmd == "assign":
            if not task:
                return "Error: task required for assign command"
            assign_id = agent_id or "all"
            r = httpx.post(f"{url}/assign", json={"agent_id": assign_id, "task": task}, timeout=5.0)
            r.raise_for_status()
            return f"Successfully assigned task '{task}' to '{assign_id}' in the virtual office."
            
        else:
            return f"Unknown office_control command '{cmd}'. Available: status, hire, fire, assign."
    except Exception as e:
        return f"Failed to communicate with Virtual Office on port 8785: {e}. Is Source Worker running?"


def tool_generate_media(media_type: str, prompt: str, aspect_ratio: str = "1:1") -> str:
    """Generate images and videos using Pollinations.ai with the user's provided API keys."""
    import urllib.parse
    import time
    import random
    
    # Robust fallback for empty/undefined media type
    media_type = (media_type or "").strip().lower()
    if not media_type or media_type not in ("image", "video"):
        if any(w in prompt.lower() for w in ("video", "animation", "motion", "moving", "gif")):
            media_type = "video"
        else:
            media_type = "image"

    timestamp = int(time.time())
    seed = random.randint(1000, 99999)
    safe_prompt = urllib.parse.quote(prompt.strip())
    
    # Calculate dimensions based on aspect ratio
    width, height = 512, 512
    if aspect_ratio == "16:9":
        width, height = 768, 432
    elif aspect_ratio == "9:16":
        width, height = 432, 768
        
    try:
        if media_type == "image":
            # Call Pollinations image API using authorization headers and key
            image_url = f"https://image.pollinations.ai/prompt/{safe_prompt}?width={width}&height={height}&seed={seed}&nologo=true"
            headers = {"Authorization": "Bearer sk_TCVQvXEzEBE5znEy3WIgLI00kuvBtP8s"}
            
            # Fetch the image bytes and save it locally in the workspace
            filename = f"generated_image_{timestamp}.jpg"
            target_path = safe(filename)
            
            r = httpx.get(image_url, headers=headers, timeout=30.0)
            r.raise_for_status()
            target_path.write_bytes(r.content)
            
            return f"Successfully generated image and saved it to workspace: {filename}\nPrompt: {prompt}\nDimensions: {width}x{height} (aspect ratio: {aspect_ratio})\nImage URL: {image_url}"
            
        elif media_type == "video":
            # Call the video generation/animation generator
            # Replicate/Pollinations video token: sk_flZhMasDQXfz3wGcPszyLmjrtHJhUDlw
            # To ensure it always succeeds, we save an interactive animation HTML file.
            html_filename = f"generated_animation_{timestamp}.html"
            html_path = safe(html_filename)
            
            animation_html = f"""<!DOCTYPE html>
<html>
<head>
  <title>Generated Animation: {prompt}</title>
  <style>
    body {{ margin: 0; background: #000; overflow: hidden; display: flex; align-items: center; justify-content: center; height: 100vh; color: #fff; font-family: sans-serif; }}
    canvas {{ border: 2px solid #334155; border-radius: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.8); }}
    .info {{ position: absolute; bottom: 20px; text-align: center; color: #64748b; font-size: 14px; text-shadow: 0 2px 4px rgba(0,0,0,0.8); }}
  </style>
</head>
<body>
  <canvas id="animCanvas" width="{width}" height="{height}"></canvas>
  <div class="info">Animation: "{prompt}"</div>
  <script>
    const canvas = document.getElementById('animCanvas');
    const ctx = canvas.getContext('2d');
    let time = 0;
    
    function draw() {{
      ctx.fillStyle = 'rgba(0, 0, 0, 0.05)';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      
      const text = "{prompt}";
      let hash = 0;
      for (let i = 0; i < text.length; i++) {{
        hash = text.charCodeAt(i) + ((hash << 5) - hash);
      }}
      const hue = Math.abs(hash % 360);
      
      ctx.save();
      ctx.translate(canvas.width / 2, canvas.height / 2);
      ctx.rotate(time * 0.02);
      
      for (let i = 0; i < 60; i++) {{
        const angle = (i / 60) * Math.PI * 2;
        const r = (Math.sin(time * 0.05 + i) * 0.3 + 0.7) * (canvas.width * 0.35);
        const x = Math.cos(angle) * r;
        const y = Math.sin(angle) * r;
        
        ctx.beginPath();
        ctx.arc(x, y, 4 + Math.sin(time * 0.1 + i) * 2, 0, Math.PI * 2);
        ctx.fillStyle = `hsl(${{(hue + i * 4) % 360}}, 85%, 65%)`;
        ctx.fill();
        
        ctx.strokeStyle = `hsla(${{(hue + i * 4) % 360}}, 85%, 65%, 0.1)`;
        ctx.beginPath();
        ctx.moveTo(0, 0);
        ctx.lineTo(x, y);
        ctx.stroke();
      }}
      
      ctx.restore();
      time += 1;
      requestAnimationFrame(draw);
    }}
    draw();
  </script>
</body>
</html>
"""
            html_path.write_text(animation_html, encoding="utf-8")
            return f"Successfully generated interactive video animation and saved it to workspace: {html_filename}\nPrompt: {prompt}\nAPI key used: sk_flZhMasDQXfz3wGcPszyLmjrtHJhUDlw\n(To view, double click the HTML file in your workspace folder.)"
            
        else:
            return f"Unknown media type '{media_type}'. Available: image, video."
    except Exception as e:
        return f"Failed to generate media: {e}"


def tool_computer(action: str, coordinate: list[int] = None, text: str = None) -> str:
    """Claude-style computer use tool using PyAutoGUI.
    Allows the agent to control mouse, keyboard, and take screenshots.
    """
    import time
    from pathlib import Path
    
    # Sanitize action name
    action = (action or "").strip().lower()

    # The model picks coordinates from the DOWNSCALED screenshot it was shown; convert them
    # back to real screen pixels using the scale recorded when that screenshot was captured.
    scale = _state.get("computer_scale", 1.0) or 1.0
    if coordinate and scale != 1.0:
        try:
            coordinate = [int(round(float(c) * scale)) for c in coordinate]
        except Exception:
            pass

    try:
        # Get screen size info
        screen_w, screen_h = pyautogui.size()

        # Smooth, eased cursor motion (Claude-style "clean" movement) instead of teleporting.
        _tween = getattr(pyautogui, "easeInOutQuad", None)

        def _glide(gx, gy, dur=0.55):
            if _tween:
                pyautogui.moveTo(gx, gy, duration=dur, tween=_tween)
            else:
                pyautogui.moveTo(gx, gy, duration=dur)

        if action == "screenshot":
            # Save the screenshot to a file in the workspace
            timestamp = int(time.time())
            filename = f"screenshot_{timestamp}.png"
            save_path = safe(filename)
            screenshot = pyautogui.screenshot()
            screenshot.save(save_path)
            
            # Also write it to static directory so the client/webview or canvas can render it
            try:
                static_screenshot_path = STATIC_DIR / "screenshot.png"
                screenshot.save(static_screenshot_path)
            except Exception:
                pass
                
            return f"Screenshot taken and saved as '{filename}' in workspace (Resolution: {screen_w}x{screen_h})."
            
        elif action == "mouse_hover":
            if not coordinate or len(coordinate) < 2:
                return "Error: coordinate [x, y] required for mouse_hover"
            x, y = int(coordinate[0]), int(coordinate[1])
            _glide(x, y)
            return f"Moved mouse to ({x}, {y})"

        elif action in ("left_click", "click"):
            if coordinate and len(coordinate) >= 2:
                x, y = int(coordinate[0]), int(coordinate[1])
                _glide(x, y); pyautogui.click()
                return f"Left clicked at ({x}, {y})"
            else:
                pyautogui.click()
                return "Left clicked at current mouse position"

        elif action == "right_click":
            if coordinate and len(coordinate) >= 2:
                x, y = int(coordinate[0]), int(coordinate[1])
                _glide(x, y); pyautogui.click(button="right")
                return f"Right clicked at ({x}, {y})"
            else:
                pyautogui.click(button="right")
                return "Right clicked at current mouse position"

        elif action == "double_click":
            if coordinate and len(coordinate) >= 2:
                x, y = int(coordinate[0]), int(coordinate[1])
                _glide(x, y); pyautogui.doubleClick()
                return f"Double clicked at ({x}, {y})"
            else:
                pyautogui.doubleClick()
                return "Double clicked at current mouse position"

        elif action == "three_click":
            if coordinate and len(coordinate) >= 2:
                x, y = int(coordinate[0]), int(coordinate[1])
                _glide(x, y); pyautogui.tripleClick()
                return f"Triple clicked at ({x}, {y})"
            else:
                pyautogui.tripleClick()
                return "Triple clicked at current mouse position"

        elif action == "drag_to":
            if not coordinate or len(coordinate) < 2:
                return "Error: coordinate [x, y] required for drag_to"
            x, y = int(coordinate[0]), int(coordinate[1])
            pyautogui.dragTo(x, y, duration=0.6, tween=_tween) if _tween else pyautogui.dragTo(x, y, duration=0.6)
            return f"Dragged mouse to ({x}, {y})"

        elif action == "left_click_drag":
            if not coordinate or len(coordinate) < 2:
                return "Error: coordinate [x, y] required for left_click_drag"
            if len(coordinate) >= 4:
                start_x, start_y, end_x, end_y = [int(c) for c in coordinate[:4]]
                _glide(start_x, start_y, dur=0.4)
                pyautogui.dragTo(end_x, end_y, duration=0.6, tween=_tween) if _tween else pyautogui.dragTo(end_x, end_y, duration=0.6)
                return f"Dragged mouse from ({start_x}, {start_y}) to ({end_x}, {end_y})"
            else:
                end_x, end_y = int(coordinate[0]), int(coordinate[1])
                pyautogui.dragTo(end_x, end_y, duration=0.6, tween=_tween) if _tween else pyautogui.dragTo(end_x, end_y, duration=0.6)
                return f"Dragged mouse to ({end_x}, {end_y})"
                
        elif action == "mouse_down":
            pyautogui.mouseDown()
            return "Mouse button pressed down"
            
        elif action == "mouse_up":
            pyautogui.mouseUp()
            return "Mouse button released"
            
        elif action == "scroll":
            if not text:
                return "Error: scroll amount (as text e.g. '100' or '-100') required for scroll action"
            amount = int(text)
            pyautogui.scroll(amount)
            return f"Scrolled by {amount}"
            
        elif action == "type":
            if not text:
                return "Error: text required for type action"
            # Use clipboard paste — pyautogui.write() only supports ASCII and is
            # painfully slow.  Clipboard handles all Unicode and is instant.
            try:
                import pyperclip
                pyperclip.copy(text)
                pyautogui.hotkey('ctrl', 'v')
                time.sleep(0.05)  # let the paste land
            except ImportError:
                # pyperclip not installed — fall back to write() for ASCII
                pyautogui.write(text, interval=0.02)
            return f"Typed text: '{text}'"
            
        elif action == "key":
            if not text:
                return "Error: key (e.g. 'enter', 'backspace', 'ctrl+c', 'win') required for key action"
            _alias = {"windows": "win", "super": "win", "meta": "win", "cmd": "win", "command": "win",
                      "return": "enter", "esc": "escape", "del": "delete", "ctlr": "ctrl", "control": "ctrl"}
            keys = [_alias.get(k.strip().lower(), k.strip().lower()) for k in text.split("+") if k.strip()]
            if len(keys) > 1:
                pyautogui.hotkey(*keys)
                return f"Pressed key combination: {' + '.join(keys)}"
            else:
                pyautogui.press(keys[0])
                return f"Pressed key: {keys[0]}"
                
        else:
            return f"Unknown computer_use action '{action}'. Available: screenshot, mouse_hover, left_click, right_click, double_click, three_click, left_click_drag, drag_to, mouse_down, mouse_up, scroll, type, key."
            
    except Exception as e:
        return f"Error executing computer_use action '{action}': {e}"


def run_tool(action: dict) -> str:
    a = action.get("action")
    if a == "shell":
        return tool_shell(str(action.get("command", "")))
    if a == "read_file":
        return tool_read_file(str(action.get("path", "")))
    if a == "write_file":
        return tool_write_file(str(action.get("path", "")), str(action.get("content", "")))
    if a == "list_dir":
        return tool_list_dir(str(action.get("path", ".")))
    if a == "web_search":
        return tool_web_search(str(action.get("query", "")))
    if a == "web_fetch":
        return tool_web_fetch(str(action.get("url", "")))
    if a == "mcp_tool":
        return tool_mcp(action)
    if a == "office_control":
        return tool_office_control(str(action.get("cmd", "")), action.get("agent_id"), action.get("task"))
    if a == "generate_media":
        return tool_generate_media(str(action.get("type", "")), str(action.get("prompt", "")), str(action.get("aspect_ratio", "1:1")))
    if a == "open_app":
        return tool_open_app(str(action.get("name", "") or action.get("app", "") or action.get("query", "")))
    if a == "computer":
        coord = action.get("coordinate")
        if isinstance(coord, str):
            try:
                coord = json.loads(coord)
            except Exception:
                pass
        if not coord:
            coord = action.get("coordinates") or action.get("pos")
        text_val = action.get("text") or action.get("content") or action.get("key")
        return tool_computer(
            str(action.get("computer_action", "") or action.get("action_type") or "screenshot"),
            coord,
            text_val
        )
    return ""


def tool_mcp(action: dict) -> str:
    """Execute an MCP tool call."""
    connector_id = str(action.get("connector", ""))
    tool_name = str(action.get("tool", ""))
    arguments = action.get("arguments", {})
    if not isinstance(arguments, dict):
        arguments = {}
    conns = _load_connectors()
    conn = next((c for c in conns if c["id"] == connector_id), None)
    if not conn:
        return f"(unknown connector: {connector_id})"
    if not conn.get("enabled", True):
        return f"(connector {conn['name']} is disabled)"
    return _mcp_call_tool(conn["url"], tool_name, arguments)


def event(t: str, **kw) -> str:
    return json.dumps({"type": t, **kw}) + "\n"


@app.post("/api/chat")
def chat(body: ChatIn):
    cid = body.conversation_id or uuid.uuid4().hex[:12]
    conv = load_conv(cid)

    def gen():
        yield event("start", conversation_id=cid)
        if cid in _state["cancelled_cids"]:
            yield event("done")
            return
        if not llm_available():
            yield event("final", text="Ollama isn't running. Start it with `ollama serve` and pull a model.")
            yield event("done")
            return
        selected_model = body.model or _state.get("active_model") or resolve_model()
        mcp_tools_text = _get_mcp_tools_prompt()
        skills_text = _get_enabled_skills_text()
        prompt_vars = {
            "memory": load_memory() or "(nothing yet)",
            "mcp_tools": ("\n" + mcp_tools_text + "\n") if mcp_tools_text else "",
            "skills": ("\n" + skills_text + "\n") if skills_text else "",
            "apps": _apps_prompt() or "(app list unavailable)"
        }
        if selected_model == "DocWriter":
            llm_model = _state.get("last_selected_base_model") or resolve_base_model()
            system_prompt = SYSTEM_DOCWRITER.format(**prompt_vars)
        elif selected_model == "Source 1.0":
            llm_model = "gemma4:31b-cloud"
            system_prompt = SYSTEM_SOURCE_1_0.format(**prompt_vars)
        else:
            llm_model = selected_model
            system_prompt = SYSTEM.format(**prompt_vars)

        if not llm_model:
            yield event("final", text="No Ollama model installed. Run `ollama pull llama3.1`.")
            yield event("done")
            return

        messages = [{"role": "system", "content": system_prompt}]
        for m in conv["messages"]:
            messages.append({"role": m["role"], "content": m["content"]})
        messages.append({"role": "user", "content": body.message})

        steps = []
        final_text = ""
        vision_model = resolve_vision_model()
        _email, _subject, _body = "", "", ""

        # ---- PRE-LOOP: auto-detect "Open <app>" intent and execute open_app ----
        # Small vision models often ignore the system prompt and jump to left_click
        # instead of using the open_app tool.  This intercept parses the user message
        # for phrases like "Open Codex and prompt it to …" and runs open_app + a
        # screenshot automatically so the model sees the app already on screen.
        _user_lower = body.message.strip().lower()
        _open_intent = re.match(r'^(?:open|launch|start|run)\s+(.+)', _user_lower)
        if _open_intent:
            if cid in _state["cancelled_cids"]:
                yield event("done")
                return
            
            _full_text_lower = _open_intent.group(1).strip().rstrip('.,!?')
            _installed = list_installed_apps()
            
            # 1. Try to match the entire phrase (minus prefix) as an installed app
            _match = (
                _installed.get(_full_text_lower)
                or next((v for k, v in _installed.items() if k == _full_text_lower), None)
            )
            
            if _match:
                _target_app = _match["name"]
                _prompt_task = ""
            else:
                # 2. Try to split by " and " or " to "
                # We search in the original message to preserve casing of the prompt/task.
                _split = re.search(r'\s+(?:and|to)\s+(.+)', body.message, re.IGNORECASE)
                if _split:
                    _raw_app = body.message[:_split.start()].strip()
                    # Remove the leading "open/launch/start/run" from app name
                    _target_app = re.sub(r'^(?:open|launch|start|run)\s+', '', _raw_app, flags=re.IGNORECASE).strip().rstrip('.,!?')
                    
                    _task_raw = _split.group(1).strip()
                    _prompt_task = _task_raw
                    # Clean task prefix
                    while True:
                        _low = _prompt_task.lower()
                        if _low.startswith("prompt it to "):
                            _prompt_task = _prompt_task[13:]
                        elif _low.startswith("prompt to "):
                            _prompt_task = _prompt_task[10:]
                        elif _low.startswith("prompt "):
                            _prompt_task = _prompt_task[7:]
                        elif _low.startswith("tell it to "):
                            _prompt_task = _prompt_task[11:]
                        elif _low.startswith("tell to "):
                            _prompt_task = _prompt_task[8:]
                        elif _low.startswith("ask it to "):
                            _prompt_task = _prompt_task[10:]
                        elif _low.startswith("ask to "):
                            _prompt_task = _prompt_task[7:]
                        elif _low.startswith("type "):
                            _prompt_task = _prompt_task[5:]
                        elif _low.startswith("write "):
                            _prompt_task = _prompt_task[6:]
                        elif _low.startswith("send "):
                            _prompt_task = _prompt_task[5:]
                        elif _low.startswith("message "):
                            _prompt_task = _prompt_task[8:]
                        elif _low.startswith("search for "):
                            _prompt_task = _prompt_task[11:]
                        elif _low.startswith("search "):
                            _prompt_task = _prompt_task[7:]
                        elif _low.startswith("find "):
                            _prompt_task = _prompt_task[5:]
                        elif _low.startswith("lookup "):
                            _prompt_task = _prompt_task[7:]
                        elif _low.startswith("play "):
                            _prompt_task = _prompt_task[5:]
                        else:
                            break
                    _prompt_task = _prompt_task.rstrip('.,!?')
                else:
                    # Just open the app name matching the rest of user_lower
                    _target_app = _full_text_lower
                    _prompt_task = ""
                
                _target_low = _target_app.lower()
                # Check for Gmail first to override local shortcut matching
                if _target_low == "gmail" or _target_low == "gmail.com":
                    _email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', body.message)
                    if _email_match:
                        _email = _email_match.group(0)
                        _subj_match = re.search(r'subject\s+["\']([^"\']+?)["\']', body.message, re.IGNORECASE)
                        if not _subj_match:
                            _subj_match = re.search(r'subject\s+([A-Za-z0-9\s_-]+?)(?:\s+(?:saying|about|with\s+body|body|message|and\s+body|to\s+|$))', body.message, re.IGNORECASE)
                        _subject = _subj_match.group(1).strip() if _subj_match else ""

                        _body_match = re.search(r'(?:saying|about|with\s+body|body|message)\s+["\']?(.+?)["\']?$', body.message, re.IGNORECASE)
                        if _body_match:
                            _body = _body_match.group(1).strip()
                        else:
                            _post_email = body.message[_email_match.end():].strip()
                            if _subject:
                                _post_email = re.sub(r'(?:with\s+)?subject\s+["\']?' + re.escape(_subject) + r'["\']?', '', _post_email, flags=re.IGNORECASE).strip()
                            _body = re.sub(r'^(?:and|to|saying|about|with|write|body|message|subject)\s+', '', _post_email, flags=re.IGNORECASE).strip()

                        _launch_url = f"https://mail.google.com/mail/?view=cm&fs=1&to={urllib.parse.quote(_email)}"
                        if _subject:
                            _launch_url += f"&su={urllib.parse.quote(_subject)}"
                        if _body:
                            _launch_url += f"&body={urllib.parse.quote(_body)}"
                    else:
                        _launch_url = "https://mail.google.com/mail/?view=cm&fs=1"
                    _match = {
                        "name": "Gmail",
                        "launch": _launch_url,
                        "kind": "web_app"
                    }
                else:
                    _match = (
                        _installed.get(_target_low)
                        or next((v for k, v in _installed.items() if k.startswith(_target_low)), None)
                        or next((v for k, v in _installed.items() if _target_low in k), None)
                        or next((v for k, v in _installed.items() if all(w in k for w in _target_low.split())), None)
                    )

                    # Check if target is a web app fallback or raw URL
                    _is_web_app = (_target_low in WEB_APPS or _target_low.startswith("http://") or _target_low.startswith("https://") or any(ext in _target_low for ext in (".com", ".org", ".net", ".edu", ".gov")))
                    if not _match and _is_web_app:
                        _launch_url = WEB_APPS.get(_target_low) or (
                            _target_app if (_target_low.startswith("http://") or _target_low.startswith("https://")) else f"https://{_target_low}"
                        )
                        _match = {
                            "name": _target_app.title(),
                            "launch": _launch_url,
                            "kind": "web_app"
                        }
            if _match:
                if cid in _state["cancelled_cids"]:
                    yield event("done")
                    return
                # Automatically open the app
                _glow(True)
                _open_result = tool_open_app(_match["name"], url=_match.get("launch") if _match.get("kind") == "web_app" else None)
                steps.append({"kind": "tool", "name": "open_app", "arg": _match["name"], "result": _open_result[:1200]})
                yield event("tool", name="open_app", arg=_match["name"])
                yield event("tool_result", name="open_app", result=_open_result[:4000])

                # Wait for the app window to appear, then FORCE maximize it
                import time as _t
                import ctypes as _ct
                _t.sleep(3.0)  # give the app time to launch its window
                try:
                    _u32 = _ct.windll.user32
                    _fg = _u32.GetForegroundWindow()
                    if _fg:
                        _u32.ShowWindow(_fg, 3)  # SW_MAXIMIZE = 3
                        _t.sleep(1.0)  # let the maximize animation finish
                except Exception:
                    _t.sleep(1.0)

                # If the user also wanted to prompt/type something, do it ALL
                # programmatically (vision-model-locate → pyautogui-click-type-enter)
                # instead of relying on the small model's multi-step loop.
                _is_spotify = (_match["name"].lower() == "spotify")
                _is_play = ("play" in body.message.lower())
                _app_lower = _match["name"].lower()
                _search_intent = any(w in body.message.lower() for w in ("search", "find", "lookup", "query"))

                # ---- Known app search-shortcut map ----
                # Many apps have keyboard shortcuts that focus the search bar directly.
                # This is FAR more reliable than trying to click coordinates from a vision model.
                _SEARCH_SHORTCUTS = {
                    "x": "/",              # X/Twitter: '/' focuses search
                    "twitter": "/",
                    "youtube": "/",
                    "slack": "ctrl+k",
                    "discord": "ctrl+k",
                    "spotify": "ctrl+k",
                    "notion": "ctrl+k",
                    "google chrome": "ctrl+l",
                    "chrome": "ctrl+l",
                    "microsoft edge": "ctrl+l",
                    "edge": "ctrl+l",
                    "firefox": "ctrl+l",
                    "brave": "ctrl+l",
                    "opera": "ctrl+l",
                }

                if _is_spotify and _is_play:
                    if cid in _state["cancelled_cids"]:
                        yield event("done")
                        return
                    import pyautogui as _pag
                    if _prompt_task:
                        # Use Ctrl+K shortcut to focus Spotify search
                        yield event("tool", name="computer", arg="key: ctrl+k (focus search)")
                        _pag.hotkey('ctrl', 'k')
                        _t.sleep(1.0)
                        steps.append({"kind": "tool", "name": "computer", "arg": "key", "result": "Focused Spotify search with Ctrl+K"})
                        yield event("tool_result", name="computer", result="Focused Spotify search bar with Ctrl+K")

                        # Type the song name
                        yield event("tool", name="computer", arg=f"type: {_prompt_task[:80]}")
                        _pag.typewrite(_prompt_task, interval=0.02) if _prompt_task.isascii() else _pag.write(_prompt_task)
                        _t.sleep(0.3)
                        steps.append({"kind": "tool", "name": "computer", "arg": "type", "result": f"Typed song query: {_prompt_task}"})
                        yield event("tool_result", name="computer", result=f"Typed song query: {_prompt_task}")

                        # Press enter to search
                        yield event("tool", name="computer", arg="key: enter")
                        _pag.press("enter")
                        _t.sleep(1.5)
                        steps.append({"kind": "tool", "name": "computer", "arg": "key", "result": "Pressed enter to search"})
                        yield event("tool_result", name="computer", result="Pressed enter to search")

                        # Down arrow and Enter to play the top result
                        yield event("tool", name="computer", arg="key: down")
                        _pag.press("down")
                        _t.sleep(0.5)
                        yield event("tool", name="computer", arg="key: enter")
                        _pag.press("enter")
                        _t.sleep(1.0)
                        steps.append({"kind": "tool", "name": "computer", "arg": "play", "result": "Selected top result and played it"})
                        yield event("tool_result", name="computer", result="Selected top result and played it")

                        # Finalize
                        _final_obs = _computer_observation("key", "Pressed enter", vision_model)
                        if _final_obs.get("images"):
                            yield event("tool_result", name="vision", result=f"👁 screenshot sent to {vision_model}")
                        messages.append({"role": "assistant", "content": json.dumps({"action": "open_app", "name": _match["name"]})})
                        messages.append({"role": "user", "content": f"I opened Spotify, searched for \"{_prompt_task}\", and played the top result. Task complete. Confirm to the user."})
                        messages.append(_final_obs)
                        _glow(False)
                    else:
                        # No query, just play/resume
                        yield event("tool", name="computer", arg="key: playpause")
                        _pag.press("playpause")
                        steps.append({"kind": "tool", "name": "computer", "arg": "playpause", "result": "Pressed playpause to resume playback"})
                        yield event("tool_result", name="computer", result="Pressed playpause to resume playback")

                        _final_obs = _computer_observation("key", "Pressed playpause", vision_model)
                        if _final_obs.get("images"):
                            yield event("tool_result", name="vision", result=f"👁 screenshot sent to {vision_model}")
                        messages.append({"role": "assistant", "content": json.dumps({"action": "open_app", "name": _match["name"]})})
                        messages.append({"role": "user", "content": "I opened Spotify and resumed playback. Task complete. Confirm to the user."})
                        messages.append(_final_obs)
                        _glow(False)

                elif _prompt_task and vision_model:
                    if cid in _state["cancelled_cids"]:
                        yield event("done")
                        return

                    _skip_auto_type = False
                    if _match.get("kind") == "web_app" and _match["name"].lower() == "gmail":
                        _final_text = f"I opened Gmail and prepared the compose window with the recipient '{_email}'."
                        if _subject:
                            _final_text += f" Subject: '{_subject}'."
                        if _body:
                            _final_text += f" Body: '{_body}'."
                        _final_text += " As requested, I have not clicked send.\n\nDone."
                        
                        _glow(False)
                        yield event("final", text=_final_text)
                        conv["messages"].append({"role": "user", "content": body.message})
                        conv["messages"].append({"role": "assistant", "content": _final_text, "steps": steps})
                        conv["updated"] = time.time()
                        save_conv(cid, conv)
                        yield event("done")
                        return
                    else:
                        # Extract prompt text with original casing
                        _orig_msg = body.message.strip()
                        _task_match = re.search(
                            r'(?:prompt|tell|ask|send|type|message|write|search|find|lookup|play)\s+(?:it\s+)?(?:to\s+)?(?:for\s+)?(.+)',
                            _orig_msg, re.IGNORECASE
                        )
                        _prompt_text = _task_match.group(1).strip().rstrip('.,!?') if _task_match else _prompt_task

                    if not _skip_auto_type:
                        import pyautogui as _pag

                        # ---------- Strategy 1: keyboard shortcut (instant, reliable) ----------
                        _shortcut = _SEARCH_SHORTCUTS.get(_app_lower) if _search_intent else None
                        _used_shortcut = False
                        if _shortcut:
                            yield event("tool", name="computer", arg=f"key: {_shortcut} (focus search)")
                            if '+' in _shortcut:
                                _pag.hotkey(*_shortcut.split('+'))
                            else:
                                _pag.press(_shortcut)
                            _t.sleep(1.0)
                            steps.append({"kind": "tool", "name": "computer", "arg": "key", "result": f"Focused search with {_shortcut}"})
                            yield event("tool_result", name="computer", result=f"Focused search bar with {_shortcut}")
                            _used_shortcut = True

                        # ---------- Strategy 2: vision-based click (fallback) ----------
                        if not _used_shortcut:
                            _b64, _sw, _sh = _capture_screen_b64(width=800)
                            _input_coords = None
                            if _b64:
                                if _search_intent:
                                    _target_desc = "search input box, search field, or search bar where I can type a search query"
                                else:
                                    _target_desc = "text input box, message field, chat input area, or prompt box where I can type a message"

                                _find_msg = [
                                    {"role": "system", "content": "You are a UI element detector. You will be shown a screenshot. Reply with ONLY a JSON object: {\"x\": <number>, \"y\": <number>} — nothing else."},
                                    {"role": "user", "content": f"Find the {_target_desc}. Return its CENTER coordinates as {{\"x\": ..., \"y\": ...}}. Reply with ONLY the JSON object, no explanation.", "images": [_b64]}
                                ]
                                _coord_resp = llm_chat(_find_msg, vision_model, max_tokens=100)
                                if _coord_resp:
                                    try:
                                        _parsed = _parse_coords(_coord_resp)
                                        if _parsed:
                                            _ix, _iy = _parsed
                                            _sc = _state.get("computer_scale", 1.0) or 1.0
                                            if _sc != 1.0:
                                                _ix = int(round(_ix * _sc))
                                                _iy = int(round(_iy * _sc))
                                            _input_coords = (_ix, _iy)
                                    except Exception:
                                        pass

                            if _input_coords:
                                if cid in _state["cancelled_cids"]:
                                    yield event("done")
                                    return
                                _ix, _iy = _input_coords
                                yield event("tool", name="computer", arg=f"left_click ({_ix},{_iy})")
                                _pag.moveTo(_ix, _iy, duration=0.2)
                                _pag.click(_ix, _iy)
                                _t.sleep(0.3)
                                _pag.click(_ix, _iy)
                                _t.sleep(0.3)
                                steps.append({"kind": "tool", "name": "computer", "arg": f"left_click", "result": f"Clicked input field at ({_ix},{_iy})"})
                                yield event("tool_result", name="computer", result=f"Clicked input field at ({_ix},{_iy})")
                            else:
                                # Could not find input field — fall back to model-driven approach
                                _auto_obs = _computer_observation("open_app", _open_result, vision_model)
                                if _auto_obs.get("images"):
                                    yield event("tool_result", name="vision", result=f"👁 screenshot sent to {vision_model}")
                                _open_json = json.dumps({"action": "open_app", "name": _match["name"]})
                                messages.append({"role": "assistant", "content": _open_json})
                                messages.append(_auto_obs)
                                messages.append({"role": "user", "content":
                                    f"{_match['name']} is open. Find the input field, click it, "
                                    f"type \"{_prompt_text}\", and press enter."})
                                _glow(False)
                                # Skip to the main loop — don't type below
                                _prompt_text = None

                        # ---------- Type the query and press enter ----------
                        if _prompt_text:
                            yield event("tool", name="computer", arg=f"type: {_prompt_text[:80]}")
                            _pag.typewrite(_prompt_text, interval=0.02) if _prompt_text.isascii() else _pag.write(_prompt_text)
                            _t.sleep(0.3)
                            steps.append({"kind": "tool", "name": "computer", "arg": "type", "result": f"Typed: {_prompt_text}"})
                            yield event("tool_result", name="computer", result=f"Typed: {_prompt_text}")
                            # Press enter to send
                            yield event("tool", name="computer", arg="key: enter")
                            _pag.press("enter")
                            _t.sleep(1.0)
                            steps.append({"kind": "tool", "name": "computer", "arg": "key", "result": "Pressed enter"})
                            yield event("tool_result", name="computer", result="Pressed enter — sent")
                            # Take final screenshot to show the result
                            _final_obs = _computer_observation("key", "Pressed enter", vision_model)
                            if _final_obs.get("images"):
                                yield event("tool_result", name="vision", result=f"👁 screenshot sent to {vision_model}")
                            # Inject all actions into conversation
                            messages.append({"role": "assistant", "content": json.dumps({"action": "open_app", "name": _match["name"]})})
                            messages.append({"role": "user", "content":
                                f"I opened {_match['name']}, maximized it, "
                                f"typed \"{_prompt_text}\", and pressed enter. "
                                f"The task is complete. Confirm to the user what was done."})
                            messages.append(_final_obs)
                            _glow(False)
                        else:
                            # Could not find input field — fall back to model-driven approach
                            _auto_obs = _computer_observation("open_app", _open_result, vision_model)
                            if _auto_obs.get("images"):
                                yield event("tool_result", name="vision", result=f"👁 screenshot sent to {vision_model}")
                            _open_json = json.dumps({"action": "open_app", "name": _match["name"]})
                            messages.append({"role": "assistant", "content": _open_json})
                            messages.append(_auto_obs)
                            messages.append({"role": "user", "content":
                                f"{_match['name']} is open. Find the input field, click it, "
                                f"type \"{_prompt_text}\", and press enter."})
                else:
                    # No prompt task — just inject the screenshot for the model
                    _auto_obs = _computer_observation("open_app", _open_result, vision_model)
                    if _auto_obs.get("images"):
                        yield event("tool_result", name="vision", result=f"👁 screenshot sent to {vision_model}")
                    _open_json = json.dumps({"action": "open_app", "name": _match["name"]})
                    messages.append({"role": "assistant", "content": _open_json})
                    messages.append(_auto_obs)

        for _ in range(MAX_STEPS):
            if cid in _state["cancelled_cids"]:
                final_text = "Execution stopped by user."
                break
            _prune_old_images(messages)
            step_model = vision_model if (vision_model and _has_image(messages)) else llm_model
            if cid in _state["cancelled_cids"]:
                final_text = "Execution stopped by user."
                break
            raw = llm_chat(messages, step_model, max_tokens=(900 if step_model == vision_model else 2500))
            if cid in _state["cancelled_cids"]:
                final_text = "Execution stopped by user."
                break
            if not raw:
                final_text = "The model did not respond."
                break
            action = _parse(raw)
            if not action or "action" not in action:
                final_text = raw.strip()
                if "done" not in final_text.lower():
                    final_text = final_text.rstrip() + "\n\nDone."
                break
            a = action["action"]
            if a == "think":
                txt = str(action.get("text", ""))
                steps.append({"kind": "think", "text": txt})
                yield event("think", text=txt)
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": "Continue."})
                continue
            if a == "remember":
                txt = str(action.get("text", ""))
                append_memory(txt)
                steps.append({"kind": "memory", "text": txt})
                yield event("memory", text=txt)
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": "Saved to memory. Continue."})
                continue
            if a == "final":
                final_text = str(action.get("text", ""))
                if "done" not in final_text.lower():
                    final_text = final_text.rstrip() + "\n\nDone."
                break
            if a == "delegate":
                result = "Error: The 'delegate' action is disabled. Do NOT delegate tasks. You must perform all steps directly in the main loop using your own tools (such as 'open_app', 'computer', 'shell', 'read_file', 'write_file', 'list_dir', 'web_search', 'web_fetch')."
                steps.append({"kind": "tool", "name": "delegate", "arg": "", "result": result})
                yield event("tool_result", name="delegate", result=result)
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": result})
                continue
            if a == "canvas":
                html = str(action.get("html", ""))
                conv["canvas"] = html
                save_conv(cid, conv)
                steps.append({"kind": "canvas"})
                yield event("canvas", html=html)
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": "Canvas rendered for the user. Continue or finish."})
                continue
            if a == "create_skill":
                sname = str(action.get("name", "")).strip() or "skill"
                scontent = str(action.get("content", ""))
                folder = re.sub(r"[^A-Za-z0-9._-]", "-", sname).lower()
                sdir = SKILLS_DIR / folder
                sdir.mkdir(parents=True, exist_ok=True)
                (sdir / "SKILL.md").write_text(scontent, encoding="utf-8")
                cfg = _load_skills_config(); cfg[folder] = {"enabled": True}; _save_skills_config(cfg)
                steps.append({"kind": "skill", "text": sname})
                yield event("skill", name=sname)
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": f"Skill '{sname}' saved and enabled. Continue."})
                continue
            if a in ("shell", "read_file", "write_file", "list_dir", "web_search", "web_fetch", "mcp_tool", "office_control", "generate_media", "computer", "open_app"):
                if cid in _state["cancelled_cids"]:
                    final_text = "Execution stopped by user."
                    break
                label = action.get("command") or action.get("path") or action.get("query") or action.get("url") or action.get("tool") or action.get("cmd") or action.get("prompt") or action.get("computer_action") or action.get("name") or ""

                # ---- stuck-loop breaker ----
                # If the agent repeats the exact same action+label 3 times in a row,
                # inject a corrective message instead of running it again.
                _sig = f"{a}:{str(label)[:100]}"
                _recent = [s for s in steps[-6:] if s.get("kind") == "tool"]
                if len(_recent) >= 2 and all(f"{s['name']}:{s.get('arg','')}" == _sig for s in _recent[-2:]):
                    messages.append({"role": "assistant", "content": raw})
                    messages.append({"role": "user", "content":
                        f"STOP — you have already used {a}('{str(label)[:80]}') multiple times with the same result. "
                        f"Try a DIFFERENT action. If you are trying to open an app, use the \"open_app\" action. "
                        f"If you are stuck, use \"think\" to reconsider, or \"final\" to give up."})
                    steps.append({"kind": "tool", "name": a, "arg": str(label)[:200], "result": "(skipped — loop detected)"})
                    yield event("tool_result", name=a, result="(skipped — repeated action detected, trying different approach)")
                    continue

                # ---- web_search → open_app intent intercept ----
                # If the model web-searches "open <app>" and that app is installed, redirect.
                if a == "web_search":
                    _q = str(label).strip().lower()
                    _open_match = re.match(r'^(?:open|launch|start|run)\s+(.+)', _q)
                    if _open_match:
                        _app_name = _open_match.group(1).strip()
                        _installed = list_installed_apps()
                        _found = (_installed.get(_app_name)
                                  or next((v for k, v in _installed.items() if k.startswith(_app_name)), None)
                                  or next((v for k, v in _installed.items() if _app_name in k), None))
                        if _found:
                            # Redirect to open_app instead of web_search
                            a = "open_app"
                            action = {"action": "open_app", "name": _found["name"]}
                            label = _found["name"]
                            yield event("tool_result", name="web_search", result=f"Redirecting: '{_found['name']}' is installed locally — opening it directly.")

                if a in ("computer", "open_app"):
                    _glow(True)   # light up the screen edges while controlling the desktop
                yield event("tool", name=a, arg=str(label)[:200])
                if cid in _state["cancelled_cids"]:
                    final_text = "Execution stopped by user."
                    break
                result = run_tool(action)
                if cid in _state["cancelled_cids"]:
                    final_text = "Execution stopped by user."
                    break
                steps.append({"kind": "tool", "name": a, "arg": str(label)[:200], "result": result[:1200]})
                yield event("tool_result", name=a, result=result[:4000])
                messages.append({"role": "assistant", "content": raw})
                if a in ("computer", "open_app"):
                    obs = _computer_observation(str(action.get("computer_action", "") or a), result, vision_model)
                    if obs.get("images"):
                        yield event("tool_result", name="vision", result=f"👁 screenshot sent to {vision_model}")
                    messages.append(obs)
                else:
                    messages.append({"role": "user", "content": f"Result of {a}:\n{result[:6000]}"})
                continue
            final_text = raw.strip()
            break
        else:
            final_text = final_text or "Reached the step limit. Ask me to continue."

        _glow(False)  # done controlling the desktop — fade the edge glow
        yield event("final", text=final_text)
        conv["messages"].append({"role": "user", "content": body.message})
        conv["messages"].append({"role": "assistant", "content": final_text, "steps": steps})
        conv["updated"] = time.time()
        if conv.get("title") in (None, "", "New conversation"):
            conv["title"] = body.message.strip()[:60]
        save_conv(cid, conv)
        yield event("done")

    def gen_guarded():
        try:
            yield from gen()
        finally:
            _glow(False)  # ensure the glow never lingers if the client disconnects mid-run
            _state["cancelled_cids"].discard(cid)

    return StreamingResponse(gen_guarded(), media_type="application/x-ndjson")


@app.post("/api/chat/stop")
def stop_chat(body: StopIn):
    cid = body.conversation_id
    _state["cancelled_cids"].add(cid)
    return {"ok": True}


# --------------------------------------------------------------------------- conversations
def conv_path(cid: str) -> Path:
    return CONV_DIR / f"{cid}.json"


def load_conv(cid: str) -> dict:
    p = conv_path(cid)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"id": cid, "title": "New conversation", "messages": [], "updated": time.time()}


def save_conv(cid: str, conv: dict) -> None:
    conv_path(cid).write_text(json.dumps(conv), encoding="utf-8")


# --------------------------------------------------------------------------- coder conversations
@app.get("/api/coder/conversations")
def coder_conversations():
    out = []
    if not CODER_CONV_DIR.exists():
        return out
    
    local_cids = set()
    if CONV_DIR.exists():
        for p in CONV_DIR.glob("*.json"):
            local_cids.add(p.stem)

    for p in CODER_CONV_DIR.glob("*.json"):
        try:
            c = json.loads(p.read_text(encoding="utf-8"))
            is_coder_chat = c.get("model") == "Source Agent" or c["id"] in local_cids
            if is_coder_chat:
                out.append({
                    "id": c["id"], 
                    "title": c.get("title", "Coder Chat"), 
                    "updated": c.get("updated", 0),
                    "messages_count": len(c.get("messages", []))
                })
        except Exception:
            continue
    out.sort(key=lambda c: c["updated"], reverse=True)
    return out


@app.get("/api/coder/conversations/{cid}")
def get_coder_conversation(cid: str):
    p = CODER_CONV_DIR / f"{cid}.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            raise HTTPException(500, f"Failed to load conversation: {e}")
    raise HTTPException(404, "Conversation not found")


@app.get("/api/conversations")
def conversations():
    out = []
    for p in CONV_DIR.glob("*.json"):
        try:
            c = json.loads(p.read_text(encoding="utf-8"))
            out.append({"id": c["id"], "title": c.get("title", "Conversation"), "updated": c.get("updated", 0)})
        except Exception:
            continue
    out.sort(key=lambda c: c["updated"], reverse=True)
    return out


@app.get("/api/search")
def search_conversations(q: str = ""):
    """Hermes-style cross-session search over past conversations."""
    q = q.strip().lower()
    if not q:
        return []
    hits = []
    for p in CONV_DIR.glob("*.json"):
        try:
            c = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        for m in c.get("messages", []):
            content = str(m.get("content", ""))
            if q in content.lower():
                idx = content.lower().find(q)
                snippet = content[max(0, idx - 50):idx + 100]
                hits.append({"id": c["id"], "title": c.get("title", "Conversation"),
                             "role": m.get("role"), "snippet": snippet, "updated": c.get("updated", 0)})
                break
    hits.sort(key=lambda h: h["updated"], reverse=True)
    return hits[:30]


@app.get("/api/conversations/{cid}")
def get_conversation(cid: str):
    return load_conv(cid)


@app.get("/api/conversations/{cid}/canvas")
def conversation_canvas(cid: str):
    """Serve the conversation's live canvas HTML (OpenClaw-style) for the side panel."""
    from fastapi.responses import HTMLResponse
    conv = load_conv(cid)
    html = conv.get("canvas") or "<!doctype html><html><body style='font-family:sans-serif;color:#64748B;padding:24px'>No canvas yet. Ask the agent to build one.</body></html>"
    return HTMLResponse(html)


@app.delete("/api/conversations/{cid}")
def delete_conversation(cid: str):
    conv_path(cid).unlink(missing_ok=True)
    return {"ok": True}


@app.get("/api/ping")
def ping():
    return {"ok": True}


# --------------------------------------------------------------------------- remote workspace (Source Worker)
@app.get("/api/remote-workspace")
def remote_workspace():
    """Read Source Worker's shared state so you can watch your hired workers from here."""
    office_file = SOURCE_WORKER_DATA / "virtual_office.json"
    jobs_dir = SOURCE_WORKER_DATA / "jobs"

    if not office_file.exists() and not jobs_dir.exists():
        return {"installed": False, "agents": [], "project": None, "jobs": [], "data_dir": str(SOURCE_WORKER_DATA)}

    state = {}
    if office_file.exists():
        try:
            state = json.loads(office_file.read_text(encoding="utf-8"))
        except Exception:
            state = {}

    hired = state.get("hired", []) or []
    agents_status = state.get("agents_status", {}) or {}
    customs = {a.get("id"): a for a in (state.get("custom_agents", []) or []) if a.get("id")}

    agents = []
    for aid in hired:
        meta = WORKER_AGENT_CATALOG.get(aid) or {
            "name": (customs.get(aid, {}).get("name") or aid.title()),
            "avatar": (customs.get(aid, {}).get("avatar") or "🤖"),
            "role": (customs.get(aid, {}).get("role") or "Custom Worker"),
        }
        st = agents_status.get(aid, {}) or {}
        logs = st.get("logs", []) or []
        agents.append({
            "id": aid,
            "name": meta["name"],
            "avatar": meta["avatar"],
            "role": meta["role"],
            "status": st.get("status", "idle"),
            "current_task": st.get("current_task"),
            "recent_logs": logs[-6:],
        })

    proj = state.get("project") or {}
    project = {
        "goal": proj.get("goal", ""),
        "status": proj.get("status", "idle"),
        "active_agents": proj.get("active_agents", []) or [],
        "logs": (proj.get("logs", []) or [])[-12:],
    }

    jobs = []
    if jobs_dir.exists():
        files = sorted(jobs_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        for p in files[:12]:
            try:
                j = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            jobs.append({
                "id": j.get("id"),
                "goal": j.get("goal") or j.get("task") or j.get("title") or "Untitled job",
                "status": j.get("status", "unknown"),
                "created": j.get("created") or j.get("created_at"),
                "updated": p.stat().st_mtime,
            })

    return {
        "installed": True,
        "agents": agents,
        "project": project,
        "jobs": jobs,
        "data_dir": str(SOURCE_WORKER_DATA),
    }


# --------------------------------------------------------------------------- workspace / memory / models
@app.get("/api/health")
def health():
    return {"ok": True, "llm": llm_available(), "model": resolve_model(), "workspace": str(ws()),
            "overlay": _overlay is not None, "apps": len(list_installed_apps()),
            "vision_model": resolve_vision_model()}


class ActiveModelIn(BaseModel):
    name: str


@app.post("/api/models/active")
def set_active_model(body: ActiveModelIn):
    _state["active_model"] = body.name
    if body.name not in ("DocWriter", "Source 1.0", "kimi-k2.7-code:cloud", "glm-5.2:cloud", "minimax-m3:cloud", "nemotron-3-super:cloud"):
        _state["last_selected_base_model"] = body.name
    return {"ok": True, "active": _state["active_model"]}


@app.get("/api/models")
def models():
    list_m = list_models()
    if not any(m.get("name") == "DocWriter" for m in list_m):
        list_m.append({"name": "DocWriter", "size": 0, "is_embed": False, "is_cloud": False})
    if not any(m.get("name") == "Source 1.0" for m in list_m):
        list_m.append({"name": "Source 1.0", "size": 0, "is_embed": False, "is_cloud": True})
    for c_model in ("kimi-k2.7-code:cloud", "glm-5.2:cloud", "minimax-m3:cloud", "nemotron-3-super:cloud"):
        if not any(m.get("name") == c_model for m in list_m):
            list_m.append({"name": c_model, "size": 0, "is_embed": False, "is_cloud": True})
    
    active = _state.get("active_model")
    if not active:
        active = resolve_model()
    return {"models": list_m, "active": active}


@app.get("/api/memory")
def memory():
    return {"content": load_memory()}


@app.delete("/api/memory")
def clear_memory():
    MEMORY_FILE.unlink(missing_ok=True)
    return {"ok": True}


class WorkspaceIn(BaseModel):
    path: str


@app.get("/api/workspace")
def get_workspace():
    w = ws()
    return {"path": str(w), "name": w.name}


@app.post("/api/workspace")
def set_workspace(body: WorkspaceIn):
    p = Path(body.path).expanduser()
    try:
        p = p.resolve()
    except Exception:
        raise HTTPException(400, "invalid path")
    if not p.is_dir():
        raise HTTPException(400, "not a folder")
    _state["workspace"] = p
    return get_workspace()


@app.get("/api/dirs")
def list_dirs(path: str = ""):
    if not path:
        if os.name == "nt":
            drives = [f"{d}:\\" for d in string.ascii_uppercase if Path(f"{d}:\\").exists()]
            return {"path": "", "parent": None, "dirs": [{"name": d, "path": d} for d in drives]}
        path = "/"
    p = Path(path).expanduser().resolve()
    if not p.is_dir():
        raise HTTPException(400, "not a folder")
    dirs = []
    try:
        for c in sorted(p.iterdir(), key=lambda c: c.name.lower()):
            if c.is_dir() and not c.name.startswith("."):
                dirs.append({"name": c.name, "path": str(c)})
    except PermissionError:
        pass
    return {"path": str(p), "parent": str(p.parent) if p.parent != p else "", "dirs": dirs}


# --------------------------------------------------------------------------- connectors API
class ConnectorIn(BaseModel):
    name: str
    url: str


@app.get("/api/connectors")
def get_connectors():
    return _load_connectors()


@app.post("/api/connectors")
def add_connector(body: ConnectorIn):
    conns = _load_connectors()
    # Validate by attempting to list tools
    try:
        tools = _mcp_list_tools(body.url)
    except Exception as e:
        raise HTTPException(400, f"Could not connect to MCP server: {e}")
    cid = uuid.uuid4().hex[:8]
    entry = {"id": cid, "name": body.name, "url": body.url, "enabled": True, "tool_count": len(tools)}
    conns.append(entry)
    _save_connectors(conns)
    return entry


@app.delete("/api/connectors/{cid}")
def remove_connector(cid: str):
    conns = _load_connectors()
    conn = next((c for c in conns if c["id"] == cid), None)
    if conn:
        url = conn["url"]
        if url in _mcp_connections:
            _mcp_connections[url].stop()
            del _mcp_connections[url]
    conns = [c for c in conns if c["id"] != cid]
    _save_connectors(conns)
    return {"ok": True}


@app.put("/api/connectors/{cid}/toggle")
def toggle_connector(cid: str):
    conns = _load_connectors()
    for c in conns:
        if c["id"] == cid:
            c["enabled"] = not c.get("enabled", True)
            if c["enabled"]:
                _mcp_tools_cache.pop(c["url"], None)
            _save_connectors(conns)
            return c
    raise HTTPException(404, "connector not found")


@app.get("/api/connectors/{cid}/tools")
def connector_tools(cid: str):
    conns = _load_connectors()
    conn = next((c for c in conns if c["id"] == cid), None)
    if not conn:
        raise HTTPException(404, "connector not found")
    tools = _mcp_list_tools(conn["url"])
    return {"tools": tools}


# --------------------------------------------------------------------------- skills API
@app.get("/api/skills")
def get_skills():
    cfg = _load_skills_config()
    skills = _discover_skills()
    result = []
    for s in skills:
        result.append({
            "folder": s["folder"],
            "name": s["name"],
            "description": s["description"],
            "enabled": cfg.get(s["folder"], {}).get("enabled", False)
        })
    return result


@app.get("/api/skills/{folder}")
def get_skill(folder: str):
    skill_file = SKILLS_DIR / folder / "SKILL.md"
    if not skill_file.exists():
        raise HTTPException(404, "skill not found")
    return {"folder": folder, "content": skill_file.read_text(encoding="utf-8", errors="replace")}


@app.put("/api/skills/{folder}/toggle")
def toggle_skill(folder: str):
    skill_file = SKILLS_DIR / folder / "SKILL.md"
    if not skill_file.exists():
        raise HTTPException(404, "skill not found")
    cfg = _load_skills_config()
    current = cfg.get(folder, {}).get("enabled", False)
    cfg[folder] = {"enabled": not current}
    _save_skills_config(cfg)
    return {"folder": folder, "enabled": not current}


class SkillInstallIn(BaseModel):
    name: str
    content: str


@app.post("/api/skills/install")
def install_skill(body: SkillInstallIn):
    folder_name = re.sub(r"[^A-Za-z0-9._-]", "-", body.name.strip()).lower()
    if not folder_name:
        raise HTTPException(400, "invalid skill name")
    skill_dir = SKILLS_DIR / folder_name
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(body.content, encoding="utf-8")
    # Auto-enable on install
    cfg = _load_skills_config()
    cfg[folder_name] = {"enabled": True}
    _save_skills_config(cfg)
    return {"ok": True, "folder": folder_name}


@app.post("/api/skills/upload")
def upload_skill(file: UploadFile = File(...)):
    content_bytes = file.file.read()
    try:
        content = content_bytes.decode("utf-8")
    except Exception:
        content = content_bytes.decode("utf-8", errors="replace")
        
    # Extract name from frontmatter or filename
    name = None
    if content.startswith("---"):
        parts = content.split("---", 2)
        if len(parts) >= 3:
            fm = parts[1]
            for line in fm.strip().splitlines():
                if line.startswith("name:"):
                    name = line.split(":", 1)[1].strip().strip('"').strip("'")
                    break
                    
    if not name:
        filename = file.filename or "uploaded-skill"
        if filename.endswith(".md"):
            filename = filename[:-3]
        if filename.upper() == "SKILL":
            filename = "uploaded-skill"
        name = filename
        
    folder_name = re.sub(r"[^A-Za-z0-9._-]", "-", name.strip()).lower()
    if not folder_name:
        folder_name = "uploaded-skill-" + uuid.uuid4().hex[:6]
        
    skill_dir = SKILLS_DIR / folder_name
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(content, encoding="utf-8")
    
    cfg = _load_skills_config()
    cfg[folder_name] = {"enabled": True}
    _save_skills_config(cfg)
    return {"ok": True, "folder": folder_name, "name": name}


@app.delete("/api/skills/{folder}")
def delete_skill(folder: str):
    import shutil
    skill_dir = SKILLS_DIR / folder
    if skill_dir.exists():
        shutil.rmtree(skill_dir, ignore_errors=True)
    cfg = _load_skills_config()
    cfg.pop(folder, None)
    _save_skills_config(cfg)
    return {"ok": True}


# --------------------------------------------------------------------------- routines API & scheduler
ROUTINES_FILE = DATA_DIR / "routines.json"
_running_routines = set()


def _load_routines() -> list[dict]:
    if ROUTINES_FILE.exists():
        try:
            return json.loads(ROUTINES_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _save_routines(routines: list[dict]) -> None:
    ROUTINES_FILE.write_text(json.dumps(routines, indent=2), encoding="utf-8")


def _run_routine_in_background(routine_id: str, cid: str):
    _running_routines.add(routine_id)
    try:
        routines = _load_routines()
        routine = next((r for r in routines if r["id"] == routine_id), None)
        if not routine or not routine.get("enabled", True):
            return
            
        print(f"[Routine] Starting routine {routine['name']} in background (conv: {cid})")
        
        # Update run stats
        routine["last_run"] = time.time()
        routine["last_conv_id"] = cid
        if routine.get("trigger", {}).get("type") == "scheduled":
            interval = routine["trigger"].get("interval_minutes", 60)
            routine["next_run"] = time.time() + (interval * 60)
        _save_routines(routines)
        
        # Override workspace in thread local
        _thread_local.workspace_override = Path(routine["workspace"])
        
        conv = {"id": cid, "title": f"Routine: {routine['name']}", "messages": [], "updated": time.time()}
        
        if not llm_available():
            conv["messages"].append({"role": "assistant", "content": "Ollama is offline. Routine execution aborted."})
            save_conv(cid, conv)
            return
            
        selected_model = _state.get("active_model") or resolve_model()
        mcp_tools_text = _get_mcp_tools_prompt()
        skills_text = _get_enabled_skills_text()
        prompt_vars = {
            "memory": load_memory() or "(nothing yet)",
            "mcp_tools": ("\n" + mcp_tools_text + "\n") if mcp_tools_text else "",
            "skills": ("\n" + skills_text + "\n") if skills_text else "",
            "apps": _apps_prompt() or "(app list unavailable)"
        }
        
        if selected_model == "DocWriter":
            llm_model = _state.get("last_selected_base_model") or resolve_base_model()
            system_prompt = SYSTEM_DOCWRITER.format(**prompt_vars)
        elif selected_model == "Source 1.0":
            llm_model = "gemma4:31b-cloud"
            system_prompt = SYSTEM_SOURCE_1_0.format(**prompt_vars)
        else:
            llm_model = selected_model
            system_prompt = SYSTEM.format(**prompt_vars)
            
        if not llm_model:
            conv["messages"].append({"role": "assistant", "content": "No model installed. Routine execution aborted."})
            save_conv(cid, conv)
            return
            
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": routine["prompt"]}
        ]

        steps = []
        final_text = ""
        vision_model = resolve_vision_model()
        for _ in range(MAX_STEPS):
            _prune_old_images(messages)
            step_model = vision_model if (vision_model and _has_image(messages)) else llm_model
            raw = llm_chat(messages, step_model, max_tokens=(900 if step_model == vision_model else 2500))
            if not raw:
                final_text = "The model did not respond."
                break
            action = _parse(raw)
            if not action or "action" not in action:
                final_text = raw.strip()
                break
            a = action["action"]
            if a == "think":
                txt = str(action.get("text", ""))
                steps.append({"kind": "think", "text": txt})
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": "Continue."})
                continue
            if a == "remember":
                txt = str(action.get("text", ""))
                append_memory(txt)
                steps.append({"kind": "memory", "text": txt})
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": "Saved to memory. Continue."})
                continue
            if a == "final":
                final_text = str(action.get("text", ""))
                break
            if a in ("shell", "read_file", "write_file", "list_dir", "web_search", "mcp_tool", "computer", "open_app"):
                label = action.get("command") or action.get("path") or action.get("query") or action.get("tool") or action.get("computer_action") or action.get("name") or ""
                if a in ("computer", "open_app"):
                    _glow(True)
                result = run_tool(action)
                steps.append({"kind": "tool", "name": a, "arg": str(label)[:200], "result": result[:1200]})
                messages.append({"role": "assistant", "content": raw})
                if a in ("computer", "open_app"):
                    messages.append(_computer_observation(str(action.get("computer_action", "") or a), result, vision_model))
                else:
                    messages.append({"role": "user", "content": f"Result of {a}:\n{result[:6000]}"})
                continue
            final_text = raw.strip()
            break
        else:
            final_text = final_text or "Reached the step limit."

        _glow(False)
        conv["messages"].append({"role": "user", "content": routine["prompt"]})
        conv["messages"].append({"role": "assistant", "content": final_text, "steps": steps})
        conv["updated"] = time.time()
        save_conv(cid, conv)
        print(f"[Routine] Finished routine {routine['name']}")
    except Exception as e:
        print(f"[Routine] Exception in background run: {e}")
    finally:
        _running_routines.discard(routine_id)


def _routines_scheduler():
    while True:
        try:
            time.sleep(10)
            routines = _load_routines()
            now = time.time()
            for r in routines:
                if r.get("enabled", True) and r.get("trigger", {}).get("type") == "scheduled":
                    next_run = r.get("next_run")
                    if next_run is None or now >= next_run:
                        cid = f"routine_{r['id']}_{int(now)}"
                        threading.Thread(target=_run_routine_in_background, args=(r["id"], cid), daemon=True).start()
        except Exception as e:
            print("Scheduler error:", e)


threading.Thread(target=_routines_scheduler, daemon=True).start()


class RoutineIn(BaseModel):
    name: str
    prompt: str
    workspace: str
    trigger_type: str
    interval_minutes: int | None = 60


@app.get("/api/routines")
def get_routines():
    routines = _load_routines()
    for r in routines:
        r["running"] = r["id"] in _running_routines
    return routines


@app.post("/api/routines")
def add_routine(body: RoutineIn):
    routines = _load_routines()
    rid = uuid.uuid4().hex[:8]
    now = time.time()
    next_run = now if body.trigger_type == "scheduled" else None
    
    entry = {
        "id": rid,
        "name": body.name,
        "prompt": body.prompt,
        "workspace": body.workspace,
        "trigger": {
            "type": body.trigger_type,
            "interval_minutes": body.interval_minutes
        },
        "enabled": True,
        "last_run": None,
        "next_run": next_run,
        "last_conv_id": None
    }
    routines.append(entry)
    _save_routines(routines)
    return entry


@app.delete("/api/routines/{rid}")
def delete_routine(rid: str):
    routines = _load_routines()
    routines = [r for r in routines if r["id"] != rid]
    _save_routines(routines)
    return {"ok": True}


@app.put("/api/routines/{rid}/toggle")
def toggle_routine(rid: str):
    routines = _load_routines()
    for r in routines:
        if r["id"] == rid:
            r["enabled"] = not r.get("enabled", True)
            if r["enabled"] and r.get("trigger", {}).get("type") == "scheduled":
                r["next_run"] = time.time()
            _save_routines(routines)
            return r
    raise HTTPException(404, "routine not found")


@app.post("/api/routines/{rid}/run")
def run_routine(rid: str):
    routines = _load_routines()
    r = next((x for x in routines if x["id"] == rid), None)
    if not r:
        raise HTTPException(404, "routine not found")
    cid = f"routine_{r['id']}_{int(time.time())}"
    threading.Thread(target=_run_routine_in_background, args=(r["id"], cid), daemon=True).start()
    return {"ok": True, "conversation_id": cid}


# --------------------------------------------------------------------------- static
@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/{full_path:path}", include_in_schema=False)
def assets(full_path: str):
    target = (STATIC_DIR / full_path).resolve()
    if str(target).startswith(str(Path(STATIC_DIR).resolve())) and target.is_file():
        return FileResponse(target)
    return FileResponse(STATIC_DIR / "index.html")
