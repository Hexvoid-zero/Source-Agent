# Source Agent ☤

A self-improving personal **AI agent**, part of the SourceMind suite. Architecture inspired by
[Nous Research's Hermes Agent](https://github.com/NousResearch/hermes-agent) (MIT) — a tool-using
loop with a closed **learning loop**: it remembers what matters across sessions.

It runs entirely locally via [Ollama](https://ollama.com), in SourceMind's colors, and ships as a
standalone native-window `.exe` (no browser).

## What it can do

The agent works in a **workspace folder** and takes real actions through tools:

| Tool | What it does |
|---|---|
| `shell` | run any command in the workspace (build, git, scripts…) |
| `read_file` / `write_file` | inspect and edit files |
| `list_dir` | browse the workspace |
| `web_search` | search the web (DuckDuckGo) |
| `remember` | save durable facts about you / the work — recalled in every future session |
| `think` | reason before acting |

Every turn streams live: you watch it think, call tools, see results, and answer. Memory persists in
`%LOCALAPPDATA%\SourceAgent\memory.md`; conversations in `%LOCALAPPDATA%\SourceAgent\conversations`.

## Run from source

```bash
cd backend
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt
.venv/Scripts/python launcher.py        # native window on http://127.0.0.1:8775
```

Needs Ollama running (`ollama serve`) with a chat model (`ollama pull llama3.1`). Without it, the UI
loads but the agent stays idle.

## Build the standalone .exe

```powershell
powershell -ExecutionPolicy Bypass -File build.ps1
# -> dist\SourceAgent\SourceAgent.exe
```

Double-click `SourceAgent.exe`: it starts the server, waits until ready, and opens a native window.

## Safety

`shell` and `write_file` act on your real filesystem within the workspace. It's a local tool — point
it at folders you own. Set the workspace from the sidebar (default: `%LOCALAPPDATA%\SourceAgent\workspace`).
