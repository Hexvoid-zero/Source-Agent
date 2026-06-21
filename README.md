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

## Recent Updates & Enhancements

- **UI Stop/Cancel Button**: You can abort active model execution at any time. The yellow send button (`↑`) turns into a stop button (`■`) during execution; clicking it instantly terminates the backend agent loop, cancels pending tool runs, and prints an `"Execution stopped by user."` notice.
- **Robust App-Launch Intercept**: Broadened the automatic launch parser. Commands like `"Open <app> and <action>"` are programmatically intercepted to open the application, maximize its window, use the vision model to detect text inputs, and use PyAutoGUI to type and press Enter.
- **Shortcut-First App Search**: Integrated keyboard shortcuts (`/` for X/Twitter and YouTube, `Ctrl+K` for Slack/Discord/Spotify/Notion, `Ctrl+L` for Chrome/Edge/Firefox/Brave/Opera) to focus search/address inputs directly. This bypasses the slower and less reliable vision coordinate model clicks and avoids selecting the entire page or typing in the wrong areas.
- **Dynamic Search Intent Detection**: Automatically detects if the task involves searching (e.g. `"search for Yan Diomande"` in X) and dynamically instructs the vision model to target the app's search bar/input box rather than a chat input field.
- **Resilient Coordinates Parser**: Enhanced coordinate extraction in `server.py` to parse coordinates regardless of JSON formatting quirks (like single quotes, double quotes, or missing quotes around JSON keys).
- **Spotify Playback Automation**: Added native automation for Spotify. Searches for songs, waits for results, and uses keyboard navigation (`Down Arrow` + `Enter`) to play the top result. Resumes/toggles playback using the media play/pause key when no song is specified.
- **UI Responsiveness & Freeze Fixes**: Refactored the edge-glow window overlay in `overlay.py` with a Win32 message loop (`PeekMessageW`) to prevent "Not Responding" screen freezes or blank screen hangs under Windows.
- **Disabled Delegation**: Blocked the model from using the `delegate` action, forcing it to use direct workspace/system tools.


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
