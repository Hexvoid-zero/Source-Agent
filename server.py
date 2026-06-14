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
import uuid
from datetime import datetime, timezone
from pathlib import Path

import random
import threading
import httpx
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "")
DATA_DIR = Path(os.getenv("SOURCE_AGENT_DATA") or (Path(os.getenv("LOCALAPPDATA") or Path.home()) / "SourceAgent"))
CONV_DIR = DATA_DIR / "conversations"
MEMORY_FILE = DATA_DIR / "memory.md"
CONNECTORS_FILE = DATA_DIR / "connectors.json"
SKILLS_DIR = DATA_DIR / "skills"
SKILLS_CONFIG = DATA_DIR / "skills_config.json"
MAX_STEPS = 12
MAX_BYTES = 2_000_000

for d in (DATA_DIR, CONV_DIR, SKILLS_DIR):
    d.mkdir(parents=True, exist_ok=True)

STATIC_DIR = (
    Path(sys._MEIPASS) / "static" if getattr(sys, "frozen", False)
    else Path(__file__).resolve().parent.parent / "static"
)
_state = {
    "workspace": Path(os.getenv("SOURCE_AGENT_WORKSPACE") or (DATA_DIR / "workspace")).resolve(),
    "active_model": None,
    "last_selected_base_model": None
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
    _mcp_initialize(url)
    result = _mcp_rpc(url, "tools/list")
    if result and "tools" in result:
        return result["tools"]
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
                out.append({"name": name, "size": m.get("size", 0) or 0, "is_embed": "embed" in name, "is_cloud": name.endswith("cloud")})
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
    if selected == "DocWriter":
        return "DocWriter"
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


def llm_available() -> bool:
    try:
        return httpx.get(f"{OLLAMA_URL}/api/tags", timeout=2.0).status_code == 200
    except Exception:
        return False


def llm_chat(messages: list[dict], model: str, max_tokens: int = 1500) -> str | None:
    try:
        r = httpx.post(
            f"{OLLAMA_URL}/api/chat",
            json={"model": model, "messages": messages, "stream": False, "options": {"num_predict": max_tokens}},
            timeout=300.0,
        )
        r.raise_for_status()
        return r.json()["message"]["content"]
    except Exception as e:
        print(f"Ollama chat error: {e}")
        import traceback
        traceback.print_exc()
        return None


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


# --------------------------------------------------------------------------- agent loop
SYSTEM = """You are Source Agent, a capable personal AI agent. Your architecture is inspired by
Nous Research's Hermes Agent: a tool-using loop with a closed learning loop (you remember across sessions).

You work inside a workspace folder and can take real actions. Respond with EXACTLY ONE JSON object per
turn and nothing else:
{{"action":"think","text":"private reasoning about what to do next"}}
{{"action":"shell","command":"ls -la"}}
{{"action":"read_file","path":"relative/path"}}
{{"action":"write_file","path":"relative/path","content":"FULL file content"}}
{{"action":"list_dir","path":"."}}
{{"action":"web_search","query":"..."}}
{{"action":"remember","text":"a durable fact about the user or this work, for future sessions"}}
{{"action":"final","text":"your answer to the user, in markdown"}}

Rules:
- Actually use tools to accomplish the task; never claim you did something you didn't do.
- Work in small steps and read tool results before the next action.
- Use "remember" when you learn something durable about the user or project.
- When finished, reply with "final".
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
{{"action":"final","text":"your answer to the user, in markdown"}}

Rules for Document Creation:
- To make a plain text (.txt) file: Use the "write_file" action to write it directly.
- To make a PDF (.pdf) file: Do NOT write raw binary PDF data. Instead, write a Python script using standard libraries like 'reportlab' or 'fpdf2' (installing them via shell `pip install reportlab fpdf2` if needed) to generate a clean, well-formatted, professional PDF document, and then run it via the "shell" action.
- To make an Excel (.xls / .xlsx) file: Write a Python script using libraries like 'openpyxl' or 'pandas' (installing them via shell `pip install openpyxl pandas` if needed) to create a styled spreadsheet, write headers and rows, and run it via the "shell" action.
- To make a PowerPoint (.pptx) presentation: Write a Python script using the 'python-pptx' library (installing it via shell `pip install python-pptx` if needed) to generate slides with clean layouts, typography, and content, and then run it via the "shell" action.
- Ensure all documents are generated inside the workspace.
- After creating a document, verify its existence and size.
- In your final response, provide the path to the created file in the workspace so the user knows exactly where to find it.
- CRITICAL JSON ESCAPING RULE: All nested double quotes inside JSON string values (like "content", "command", "text") MUST be properly escaped as \\\" and all newlines as \\n. Never output raw unescaped double quotes (such as python triple quotes) or raw newlines inside a JSON string.
{mcp_tools}
{skills}
What you remember about the user and past sessions:
{memory}
"""


class ChatIn(BaseModel):
    message: str
    conversation_id: str | None = None
    model: str | None = None


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
    if a == "mcp_tool":
        return tool_mcp(action)
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
            "skills": ("\n" + skills_text + "\n") if skills_text else ""
        }
        if selected_model == "DocWriter":
            llm_model = _state.get("last_selected_base_model") or resolve_base_model()
            system_prompt = SYSTEM_DOCWRITER.format(**prompt_vars)
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
        for _ in range(MAX_STEPS):
            raw = llm_chat(messages, llm_model, max_tokens=2500)
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
                break
            if a in ("shell", "read_file", "write_file", "list_dir", "web_search", "mcp_tool"):
                label = action.get("command") or action.get("path") or action.get("query") or action.get("tool") or ""
                yield event("tool", name=a, arg=str(label)[:200])
                result = run_tool(action)
                steps.append({"kind": "tool", "name": a, "arg": str(label)[:200], "result": result[:1200]})
                yield event("tool_result", name=a, result=result[:4000])
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": f"Result of {a}:\n{result[:6000]}"})
                continue
            final_text = raw.strip()
            break
        else:
            final_text = final_text or "Reached the step limit. Ask me to continue."

        yield event("final", text=final_text)
        conv["messages"].append({"role": "user", "content": body.message})
        conv["messages"].append({"role": "assistant", "content": final_text, "steps": steps})
        conv["updated"] = time.time()
        if conv.get("title") in (None, "", "New conversation"):
            conv["title"] = body.message.strip()[:60]
        save_conv(cid, conv)
        yield event("done")

    return StreamingResponse(gen(), media_type="application/x-ndjson")


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


@app.get("/api/conversations/{cid}")
def get_conversation(cid: str):
    return load_conv(cid)


@app.delete("/api/conversations/{cid}")
def delete_conversation(cid: str):
    conv_path(cid).unlink(missing_ok=True)
    return {"ok": True}


@app.get("/api/ping")
def ping():
    return {"ok": True}


# --------------------------------------------------------------------------- workspace / memory / models
@app.get("/api/health")
def health():
    return {"ok": True, "llm": llm_available(), "model": resolve_model(), "workspace": str(ws())}


class ActiveModelIn(BaseModel):
    name: str


@app.post("/api/models/active")
def set_active_model(body: ActiveModelIn):
    _state["active_model"] = body.name
    if body.name != "DocWriter":
        _state["last_selected_base_model"] = body.name
    return {"ok": True, "active": _state["active_model"]}


@app.get("/api/models")
def models():
    list_m = list_models()
    if not any(m.get("name") == "DocWriter" for m in list_m):
        list_m.append({"name": "DocWriter", "size": 0, "is_embed": False, "is_cloud": False})
    
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
            "skills": ("\n" + skills_text + "\n") if skills_text else ""
        }
        
        if selected_model == "DocWriter":
            llm_model = _state.get("last_selected_base_model") or resolve_base_model()
            system_prompt = SYSTEM_DOCWRITER.format(**prompt_vars)
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
        for _ in range(MAX_STEPS):
            raw = llm_chat(messages, llm_model, max_tokens=2500)
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
            if a in ("shell", "read_file", "write_file", "list_dir", "web_search", "mcp_tool"):
                label = action.get("command") or action.get("path") or action.get("query") or action.get("tool") or ""
                result = run_tool(action)
                steps.append({"kind": "tool", "name": a, "arg": str(label)[:200], "result": result[:1200]})
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": f"Result of {a}:\n{result[:6000]}"})
                continue
            final_text = raw.strip()
            break
        else:
            final_text = final_text or "Reached the step limit."
            
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
