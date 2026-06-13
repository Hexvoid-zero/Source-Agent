"use strict";
const $ = (id) => document.getElementById(id);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function rfetch(url, opts, tries) {
  tries = tries || 4;
  for (let i = 0; i < tries; i++) {
    try { return await fetch(url, opts); }
    catch (e) { if (i < tries - 1) { await sleep(500 * (i + 1)); continue; } throw e; }
  }
}
async function api(path, opts) {
  const res = await rfetch("/api" + path, Object.assign({ headers: { "Content-Type": "application/json" } }, opts));
  if (!res.ok) throw new Error((await res.text()) || res.statusText);
  return res.json();
}

const S = { cid: null, busy: false };
const TOOL_ICON = { shell: "›_", read_file: "📄", write_file: "✎", list_dir: "🗂", web_search: "🌐" };

function toast(m, ms) { const t = $("toast"); t.textContent = m; t.hidden = false; clearTimeout(toast._t); toast._t = setTimeout(() => (t.hidden = true), ms || 2400); }

function fmt(text) {
  return (text || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/```([\s\S]*?)```/g, (m, c) => `<pre>${c}</pre>`)
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/^### (.*)$/gm, "<h3>$1</h3>").replace(/^## (.*)$/gm, "<h2>$1</h2>").replace(/^# (.*)$/gm, "<h1>$1</h1>")
    .replace(/^\s*[-*] (.*)$/gm, "<li>$1</li>")
    .replace(/(<li>[\s\S]*?<\/li>)/g, "<ul>$1</ul>")
    .replace(/\n{2,}/g, "</p><p>").replace(/\n/g, "<br>");
}

// --------------------------------------------------------------------------- boot
async function boot() {
  try {
    const h = await api("/health");
    const s = $("status");
    s.className = "status " + (h.llm ? "on" : "off");
    s.innerHTML = `<span class="dot"></span> ${h.llm ? (h.model || "ready") : "Ollama offline"}`;
  } catch (e) { $("status").innerHTML = `<span class="dot"></span> backend offline`; }
  await Promise.all([loadModels(), loadConversations(), loadMemory(), loadWorkspace()]);
}
async function loadModels() {
  try {
    const m = await api("/models");
    const sel = $("modelSelect"); sel.innerHTML = "";
    if (!m.models.length) { sel.innerHTML = "<option>no models</option>"; return; }
    for (const mod of m.models.filter((x) => !x.is_embed)) {
      const o = document.createElement("option");
      o.value = mod.name; o.textContent = mod.name + (mod.is_cloud ? " ☁" : "");
      if (mod.name === m.active) o.selected = true;
      sel.appendChild(o);
    }
  } catch (e) {}
}
async function loadConversations() {
  const list = await api("/conversations").catch(() => []);
  const el = $("convList"); el.innerHTML = "";
  for (const c of list) {
    const row = document.createElement("div");
    row.className = "conv" + (c.id === S.cid ? " active" : "");
    row.innerHTML = `<span class="ct">${c.title || "Conversation"}</span><span class="cx">✕</span>`;
    row.querySelector(".ct").onclick = () => openConversation(c.id);
    row.querySelector(".cx").onclick = async (e) => { e.stopPropagation(); await api(`/conversations/${c.id}`, { method: "DELETE" }); if (S.cid === c.id) newChat(); loadConversations(); };
    el.appendChild(row);
  }
}
async function loadMemory() {
  const m = await api("/memory").catch(() => ({ content: "" }));
  const lines = (m.content || "").split("\n").map((l) => l.replace(/^- /, "").trim()).filter(Boolean);
  const el = $("memList");
  el.innerHTML = lines.length ? "" : '<div class="mem-empty">Nothing yet. The agent saves durable facts here as you work.</div>';
  for (const l of lines.slice(-30).reverse()) {
    const d = document.createElement("div"); d.className = "mem-item"; d.textContent = l; el.appendChild(d);
  }
}
async function loadWorkspace() {
  const w = await api("/workspace").catch(() => ({ path: "—", name: "—" }));
  $("wsPath").textContent = w.path; $("wsPath").title = w.path;
  $("wsFoot").textContent = "workspace: " + w.path;
}

// --------------------------------------------------------------------------- conversations
function newChat() { S.cid = null; $("transcript").innerHTML = ""; showEmpty(true); loadConversations(); }
function showEmpty(on) {
  if (on && !$("empty")) {
    $("transcript").innerHTML = `<div class="empty" id="empty"><div class="empty-mark">☤</div><h1>Source Agent</h1>
      <p>A personal AI agent that runs shell commands, edits files, searches the web, and remembers across sessions — locally via Ollama.</p></div>`;
  } else if (!on && $("empty")) { $("empty").remove(); }
}
async function openConversation(cid) {
  S.cid = cid;
  const conv = await api(`/conversations/${cid}`);
  const t = $("transcript"); t.innerHTML = "";
  for (const m of conv.messages) {
    if (m.role === "user") addUser(m.content);
    else renderSavedTurn(m);
  }
  loadConversations();
  t.scrollTop = t.scrollHeight;
}
function renderSavedTurn(m) {
  const turn = document.createElement("div"); turn.className = "turn";
  const a = document.createElement("div"); a.className = "assistant";
  for (const s of m.steps || []) {
    if (s.kind === "think") a.appendChild(thinkEl(s.text));
    else if (s.kind === "memory") a.appendChild(memEl(s.text));
    else if (s.kind === "tool") { const t = toolEl(s.name, s.arg); setToolResult(t, s.result); a.appendChild(t); }
  }
  const fin = document.createElement("div"); fin.innerHTML = "<p>" + fmt(m.content) + "</p>"; a.appendChild(fin);
  turn.appendChild(a); $("transcript").appendChild(turn);
}

// --------------------------------------------------------------------------- step elements
function thinkEl(text) { const d = document.createElement("div"); d.className = "step step-think"; d.textContent = text; return d; }
function memEl(text) { const d = document.createElement("div"); d.className = "step step-mem"; d.innerHTML = `🧠 <b>Remembered:</b> ${fmt(text)}`; return d; }
function toolEl(name, arg) {
  const d = document.createElement("div"); d.className = "step tool";
  d.innerHTML = `<div class="tool-head"><span class="tool-ico">${TOOL_ICON[name] || "▶"}</span>` +
    `<span class="tool-name">${name}</span><span class="tool-arg">${(arg || "").replace(/</g, "&lt;")}</span>` +
    `<span class="tool-spin"></span></div>`;
  const head = d.querySelector(".tool-head");
  head.onclick = () => { const r = d.querySelector(".tool-result"); if (r) r.hidden = !r.hidden; };
  return d;
}
function setToolResult(toolNode, result) {
  const spin = toolNode.querySelector(".tool-spin"); if (spin) spin.remove();
  let r = toolNode.querySelector(".tool-result");
  if (!r) { r = document.createElement("div"); r.className = "tool-result"; toolNode.appendChild(r); }
  r.textContent = result || "(no output)";
}
function addUser(text) {
  showEmpty(false);
  const turn = document.createElement("div"); turn.className = "turn";
  const u = document.createElement("div"); u.className = "msg-user"; u.textContent = text;
  turn.appendChild(u); $("transcript").appendChild(turn);
}

// --------------------------------------------------------------------------- send (streaming agent loop)
async function send(text) {
  text = (text || $("input").value).trim();
  if (!text || S.busy) return;
  $("input").value = ""; autosize();
  S.busy = true; $("send").disabled = true;
  addUser(text);

  const turn = document.createElement("div"); turn.className = "turn";
  const a = document.createElement("div"); a.className = "assistant";
  const working = document.createElement("div"); working.className = "working";
  working.innerHTML = `<span class="tool-spin"></span> thinking…`;
  a.appendChild(working); turn.appendChild(a);
  const t = $("transcript"); t.appendChild(turn); t.scrollTop = t.scrollHeight;

  let curTool = null;
  const onEvent = (ev) => {
    if (ev.type === "start") { S.cid = ev.conversation_id; }
    else if (ev.type === "think") { working.remove(); a.appendChild(thinkEl(ev.text)); a.appendChild(working); }
    else if (ev.type === "memory") { working.remove(); a.appendChild(memEl(ev.text)); a.appendChild(working); }
    else if (ev.type === "tool") { working.remove(); curTool = toolEl(ev.name, ev.arg); a.appendChild(curTool); a.appendChild(working); }
    else if (ev.type === "tool_result") { if (curTool) setToolResult(curTool, ev.result); }
    else if (ev.type === "final") { working.remove(); const f = document.createElement("div"); f.innerHTML = "<p>" + fmt(ev.text) + "</p>"; a.appendChild(f); }
    else if (ev.type === "done") {}
    t.scrollTop = t.scrollHeight;
  };

  try {
    const res = await rfetch("/api/chat", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ message: text, conversation_id: S.cid }) }, 2);
    if (!res.ok || !res.body) throw new Error(await res.text());
    const reader = res.body.getReader(); const dec = new TextDecoder(); let buf = "";
    for (;;) {
      const { done, value } = await reader.read(); if (done) break;
      buf += dec.decode(value, { stream: true });
      let i;
      while ((i = buf.indexOf("\n")) >= 0) { const line = buf.slice(0, i); buf = buf.slice(i + 1); if (line.trim()) try { onEvent(JSON.parse(line)); } catch (e) {} }
    }
  } catch (e) {
    working.remove(); const er = document.createElement("div"); er.innerHTML = `<p style="color:var(--red)">Error: ${e.message}</p>`; a.appendChild(er);
  } finally {
    if (working.parentNode) working.remove();
    S.busy = false; $("send").disabled = false;
    loadConversations(); loadMemory(); loadWorkspace();
  }
}

// --------------------------------------------------------------------------- workspace modal
let modalCwd = "";
async function openWsModal() { $("wsModal").hidden = false; const w = await api("/workspace"); loadDirs(w.path); }
async function loadDirs(path) {
  const r = await api("/dirs?path=" + encodeURIComponent(path || ""));
  modalCwd = r.path; $("modalPath").textContent = r.path || "This PC";
  const list = $("modalList"); list.innerHTML = "";
  for (const d of r.dirs) {
    const row = document.createElement("div"); row.className = "row";
    row.innerHTML = `<span>📁</span><span>${d.name}</span>`; row.onclick = () => loadDirs(d.path); list.appendChild(row);
  }
  $("modalUp").onclick = () => loadDirs(r.parent || "");
}
async function chooseWs() {
  if (!modalCwd) return;
  await api("/workspace", { method: "POST", body: JSON.stringify({ path: modalCwd }) });
  $("wsModal").hidden = true; loadWorkspace(); toast("Workspace set");
}

// --------------------------------------------------------------------------- wire
function autosize() { const i = $("input"); i.style.height = "auto"; i.style.height = Math.min(i.scrollHeight, 180) + "px"; }
document.addEventListener("DOMContentLoaded", () => {
  $("send").onclick = () => send();
  $("input").addEventListener("input", autosize);
  $("input").addEventListener("keydown", (e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } });
  $("newChat").onclick = newChat;
  $("clearMem").onclick = async () => { if (confirm("Clear all agent memory?")) { await api("/memory", { method: "DELETE" }); loadMemory(); } };
  $("changeWs").onclick = openWsModal;
  $("modalCancel").onclick = () => ($("wsModal").hidden = true);
  $("modalOpen").onclick = chooseWs;
  const sug = $("suggest"); if (sug) sug.querySelectorAll("button").forEach((b) => (b.onclick = () => send(b.textContent)));
  boot();
});
