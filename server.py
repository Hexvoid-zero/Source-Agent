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

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "")
DATA_DIR = Path(os.getenv("SOURCE_AGENT_DATA") or (Path(os.getenv("LOCALAPPDATA") or Path.home()) / "SourceAgent"))
CONV_DIR = DATA_DIR / "conversations"
MEMORY_FILE = DATA_DIR / "memory.md"
MAX_STEPS = 12
MAX_BYTES = 2_000_000

for d in (DATA_DIR, CONV_DIR):
    d.mkdir(parents=True, exist_ok=True)

STATIC_DIR = (
    Path(sys._MEIPASS) / "static" if getattr(sys, "frozen", False)
    else Path(__file__).resolve().parent.parent / "static"
)
_state = {"workspace": Path(os.getenv("SOURCE_AGENT_WORKSPACE") or (DATA_DIR / "workspace")).resolve()}
_state["workspace"].mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Source Agent")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def ws() -> Path:
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


def resolve_model() -> str | None:
    models = list_models()
    names = {m["name"] for m in models}
    if OLLAMA_MODEL and (OLLAMA_MODEL in names or f"{OLLAMA_MODEL}:latest" in names):
        return OLLAMA_MODEL if OLLAMA_MODEL in names else f"{OLLAMA_MODEL}:latest"
    local = sorted([m for m in models if not m["is_cloud"] and not m["is_embed"]], key=lambda m: m["size"], reverse=True)
    if local:
        return local[0]["name"]
    return next((m["name"] for m in models if not m["is_embed"]), None)


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
    except Exception:
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

What you remember about the user and past sessions:
{memory}
"""


class ChatIn(BaseModel):
    message: str
    conversation_id: str | None = None


def _parse(raw: str):
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except ValueError:
        # tolerate unescaped newlines inside content strings
        try:
            return json.loads(m.group(0).replace("\n", "\\n"))
        except ValueError:
            return None


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
    return ""


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
        model = resolve_model()
        if not model:
            yield event("final", text="No Ollama model installed. Run `ollama pull llama3.1`.")
            yield event("done")
            return

        messages = [{"role": "system", "content": SYSTEM.format(memory=load_memory() or "(nothing yet)")}]
        for m in conv["messages"]:
            messages.append({"role": m["role"], "content": m["content"]})
        messages.append({"role": "user", "content": body.message})

        steps = []
        final_text = ""
        for _ in range(MAX_STEPS):
            raw = llm_chat(messages, model, max_tokens=2500)
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
            if a in ("shell", "read_file", "write_file", "list_dir", "web_search"):
                label = action.get("command") or action.get("path") or action.get("query") or ""
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


# --------------------------------------------------------------------------- workspace / memory / models
@app.get("/api/health")
def health():
    return {"ok": True, "llm": llm_available(), "model": resolve_model(), "workspace": str(ws())}


@app.get("/api/models")
def models():
    return {"models": list_models(), "active": resolve_model()}


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
