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

const S = { cid: null, busy: false, abortController: null };
const TOOL_ICON = { shell: "›_", read_file: "📄", write_file: "✎", list_dir: "🗂", web_search: "🌐", mcp_tool: "🔌" };

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
  await Promise.all([loadModels(), loadConversations(), loadMemory(), loadWorkspace(), loadConnectors(), loadSkills(), loadRoutines()]);
}
async function loadModels() {
  try {
    const m = await api("/models");
    const sel = $("modelSelect"); sel.innerHTML = "";
    if (!m.models.length) { sel.innerHTML = "<option>no models</option>"; return; }
    for (const mod of m.models.filter((x) => !x.is_embed)) {
      const o = document.createElement("option");
      o.value = mod.name;
      o.textContent = mod.name + (mod.is_cloud ? " ☁" : "") + (mod.is_vision ? " 👁" : "");
      if (mod.is_vision) o.title = "Vision model — sees the screen, so computer-use runs entirely on this one model (no screenshots handed to a separate model).";
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

// --------------------------------------------------------------------------- connectors
async function loadConnectors() {
  const conns = await api("/connectors").catch(() => []);
  const el = $("connList2"); el.innerHTML = "";
  if (!conns.length) {
    el.innerHTML = '<div class="conn-empty">No connectors. Click ＋ to add an MCP server.</div>';
    return;
  }
  for (const c of conns) {
    const row = document.createElement("div");
    row.className = "conn-item" + (c.enabled ? "" : " disabled");
    row.innerHTML = `<div class="conn-info"><span class="conn-dot ${c.enabled ? 'on' : 'off'}"></span><span class="conn-name">${c.name}</span><span class="conn-tools">${c.tool_count || '?'} tools</span></div><div class="conn-actions"><button class="conn-toggle" title="${c.enabled ? 'Disable' : 'Enable'}">${c.enabled ? '●' : '○'}</button><button class="conn-rm" title="Remove">✕</button></div>`;
    row.querySelector(".conn-toggle").onclick = async () => { await api(`/connectors/${c.id}/toggle`, { method: "PUT" }); loadConnectors(); };
    row.querySelector(".conn-rm").onclick = async () => { if (confirm(`Remove connector "${c.name}"?`)) { await api(`/connectors/${c.id}`, { method: "DELETE" }); loadConnectors(); toast("Connector removed"); } };
    el.appendChild(row);
  }
}
async function addConnector() {
  const name = $("connName").value.trim();
  const url = $("connUrl").value.trim();
  if (!name || !url) { toast("Name and URL are required"); return; }
  try {
    await api("/connectors", { method: "POST", body: JSON.stringify({ name, url }) });
    $("connModal").hidden = true;
    $("connName").value = ""; $("connUrl").value = "";
    loadConnectors();
    toast("Connector added successfully");
  } catch (e) {
    toast("Failed: " + e.message, 4000);
  }
}

// --------------------------------------------------------------------------- skills
async function loadSkills() {
  const skills = await api("/skills").catch(() => []);
  const el = $("skillList"); el.innerHTML = "";
  if (!skills.length) {
    el.innerHTML = '<div class="skill-empty">No skills installed. Click ＋ to add one.</div>';
    return;
  }
  for (const s of skills) {
    const row = document.createElement("div");
    row.className = "skill-item" + (s.enabled ? "" : " disabled");
    row.innerHTML = `<div class="skill-info"><span class="skill-icon">⚡</span><span class="skill-name">${s.name}</span></div><div class="skill-desc">${s.description || ''}</div><div class="skill-actions"><button class="skill-toggle" title="${s.enabled ? 'Disable' : 'Enable'}">${s.enabled ? '●' : '○'}</button><button class="skill-rm" title="Remove">✕</button></div>`;
    row.querySelector(".skill-toggle").onclick = async () => { await api(`/skills/${s.folder}/toggle`, { method: "PUT" }); loadSkills(); };
    row.querySelector(".skill-rm").onclick = async () => { if (confirm(`Remove skill "${s.name}"?`)) { await api(`/skills/${s.folder}`, { method: "DELETE" }); loadSkills(); toast("Skill removed"); } };
    el.appendChild(row);
  }
}
async function apiUpload(path, file) {
  const formData = new FormData();
  formData.append("file", file);
  const res = await rfetch("/api" + path, {
    method: "POST",
    body: formData
  });
  if (!res.ok) throw new Error((await res.text()) || res.statusText);
  return res.json();
}

let skillInputMode = "paste";
let selectedSkillFile = null;

async function installSkill() {
  if (skillInputMode === "paste") {
    const name = $("skillName").value.trim();
    const content = $("skillContent").value.trim();
    if (!name || !content) { toast("Name and content are required"); return; }
    try {
      await api("/skills/install", { method: "POST", body: JSON.stringify({ name, content }) });
      closeSkillModal();
      loadSkills();
      toast("Skill installed and enabled");
    } catch (e) {
      toast("Failed: " + e.message, 4000);
    }
  } else {
    if (!selectedSkillFile) { toast("Please select or drop a SKILL.md file"); return; }
    try {
      await apiUpload("/skills/upload", selectedSkillFile);
      closeSkillModal();
      loadSkills();
      toast("Skill uploaded and enabled");
    } catch (e) {
      toast("Failed: " + e.message, 4000);
    }
  }
}

function closeSkillModal() {
  $("skillModal").hidden = true;
  $("skillName").value = "";
  $("skillContent").value = "";
  const fileInput = $("skillFileInput");
  if (fileInput) fileInput.value = "";
  selectedSkillFile = null;
  const fileInfo = $("skillFileInfo");
  if (fileInfo) fileInfo.hidden = true;
  // reset to paste tab
  const btnPaste = $("btnSkillPaste");
  if (btnPaste) {
    btnPaste.classList.add("active");
    const btnUpload = $("btnSkillUpload");
    if (btnUpload) btnUpload.classList.remove("active");
    $("skillPasteSection").hidden = false;
    $("skillUploadSection").hidden = true;
    skillInputMode = "paste";
  }
}

// --------------------------------------------------------------------------- routines
async function loadRoutines() {
  const routines = await api("/routines").catch(() => []);
  const el = $("routineList"); el.innerHTML = "";
  if (!routines.length) {
    el.innerHTML = '<div class="routine-empty">No routines created. Click ＋ to add one.</div>';
    return;
  }
  for (const r of routines) {
    const row = document.createElement("div");
    row.className = "routine-item" + (r.enabled ? "" : " disabled");
    
    let triggerDesc = "Manual Only";
    if (r.trigger.type === "scheduled") {
      triggerDesc = `Every ${r.trigger.interval_minutes}m`;
    }
    
    let statusText = "Never run";
    if (r.running) {
      statusText = "Running...";
    } else if (r.last_run) {
      const date = new Date(r.last_run * 1000);
      statusText = "Last: " + date.toLocaleTimeString();
    }
    
    row.innerHTML = `<div class="routine-info">` +
      `<span class="routine-icon">${r.running ? '⏳' : '⚙️'}</span>` +
      `<span class="routine-name" id="name-${r.id}">${r.name}</span>` +
      `</div>` +
      `<div class="routine-meta">` +
      `<span>${triggerDesc}</span>` +
      `<span>${statusText}</span>` +
      `</div>` +
      `<div class="routine-actions">` +
      `<button class="routine-run" title="Run now">▶</button>` +
      `<button class="routine-toggle" title="${r.enabled ? 'Disable' : 'Enable'}">${r.enabled ? '●' : '○'}</button>` +
      `<button class="routine-rm" title="Delete">✕</button>` +
      `</div>`;
      
    if (r.last_conv_id) {
      const nameEl = row.querySelector(`#name-${r.id}`);
      nameEl.onclick = () => openConversation(r.last_conv_id);
    }
    
    row.querySelector(".routine-run").onclick = async () => {
      try {
        await api(`/routines/${r.id}/run`, { method: "POST" });
        toast("Routine started");
        loadRoutines();
      } catch (e) {
        toast("Run failed: " + e.message);
      }
    };
    
    row.querySelector(".routine-toggle").onclick = async () => {
      await api(`/routines/${r.id}/toggle`, { method: "PUT" });
      loadRoutines();
    };
    
    row.querySelector(".routine-rm").onclick = async () => {
      if (confirm(`Delete routine "${r.name}"?`)) {
        await api(`/routines/${r.id}`, { method: "DELETE" });
        loadRoutines();
        toast("Routine deleted");
      }
    };
    
    el.appendChild(row);
  }
}

async function createRoutine() {
  const name = $("routineName").value.trim();
  const prompt = $("routinePrompt").value.trim();
  const workspace = $("routineWorkspace").value.trim();
  const trigger_type = $("routineTriggerType").value;
  const interval_minutes = parseInt($("routineInterval").value) || 60;
  
  if (!name || !prompt || !workspace) {
    toast("Name, prompt, and workspace folder are required");
    return;
  }
  
  try {
    await api("/routines", {
      method: "POST",
      body: JSON.stringify({
        name,
        prompt,
        workspace,
        trigger_type,
        interval_minutes
      })
    });
    closeRoutineModal();
    loadRoutines();
    toast("Routine created successfully");
  } catch (e) {
    toast("Failed: " + e.message, 4000);
  }
}

function closeRoutineModal() {
  $("routineModal").hidden = true;
  $("routineName").value = "";
  $("routinePrompt").value = "";
  $("routineWorkspace").value = "";
  $("routineTriggerType").value = "manual";
  $("routineInterval").value = "60";
  $("routineIntervalLabel").hidden = true;
}

// --------------------------------------------------------------------------- coder conversations
function showCoderPage() {
  $("chatPage").hidden = true;
  $("coderPage").hidden = false;
  $("btnCoderChats").classList.add("active");
  document.querySelectorAll(".conv").forEach((c) => c.classList.remove("active"));
  loadCoderConversations();
}

function showChatPage() {
  $("chatPage").hidden = false;
  $("coderPage").hidden = true;
  $("btnCoderChats").classList.remove("active");
}

async function loadCoderConversations() {
  const list = await api("/coder/conversations").catch(() => []);
  const el = $("coderList"); el.innerHTML = "";
  
  if (!list.length) {
    el.innerHTML = '<div class="conn-empty" style="padding: 12px; text-align: center;">No coder conversations found.</div>';
    return;
  }
  
  for (const c of list) {
    const card = document.createElement("div");
    card.className = "coder-item";
    
    const msgs = c.messages_count || 0;
    const dateStr = c.updated ? new Date(c.updated * 1000).toLocaleDateString() : "Unknown";
    
    card.innerHTML = `
      <div class="coder-item-title" title="${c.title || 'Coder Chat'}">${c.title || 'Coder Chat'}</div>
      <div class="coder-item-meta">
        <span>${msgs} message${msgs !== 1 ? 's' : ''}</span>
        <span>${dateStr}</span>
      </div>
    `;
    
    card.onclick = () => openCoderConversation(c.id, card);
    el.appendChild(card);
  }
}

async function openCoderConversation(cid, cardElement) {
  document.querySelectorAll(".coder-item").forEach((item) => item.classList.remove("active"));
  if (cardElement) cardElement.classList.add("active");
  
  try {
    const conv = await api(`/coder/conversations/${cid}`);
    $("coderEmpty").hidden = true;
    $("coderTranscriptWrap").hidden = false;
    
    $("coderChatTitle").textContent = conv.title || "Coder Chat";
    
    const date = conv.updated ? new Date(conv.updated * 1000).toLocaleString() : "Unknown";
    $("coderChatMeta").textContent = `Last updated: ${date} | ID: ${cid}`;
    
    const t = $("coderTranscript");
    t.innerHTML = "";
    for (const m of conv.messages) {
      renderCoderTurn(m, t);
    }
    t.scrollTop = 0;
  } catch (e) {
    toast("Error loading coder conversation: " + e.message);
  }
}

function renderCoderTurn(m, container) {
  const turn = document.createElement("div");
  turn.className = "turn";
  
  if (m.role === "user") {
    const u = document.createElement("div");
    u.className = "msg-user";
    u.textContent = m.content;
    turn.appendChild(u);
  } else {
    const a = document.createElement("div");
    a.className = "assistant";
    
    for (const s of m.steps || []) {
      if (s.kind === "think" || s.action === "thinking") {
        a.appendChild(thinkEl(s.text || s.path || "thinking…"));
      } else if (s.kind === "memory") {
        a.appendChild(memEl(s.text));
      } else if (s.kind === "tool" || s.action === "shell" || s.action === "read" || s.action === "write" || s.action === "read_file" || s.action === "write_file" || s.action === "list_dir" || s.action === "web_search" || s.action === "mcp_tool") {
        const name = s.name || s.action || "tool";
        const arg = s.arg || s.path || "";
        const t = toolEl(name, arg);
        setToolResult(t, s.result || (s.ok ? "Operation succeeded" : "Operation failed"));
        a.appendChild(t);
      }
    }
    
    if (m.content) {
      const fin = document.createElement("div");
      fin.innerHTML = "<p>" + fmt(m.content) + "</p>";
      a.appendChild(fin);
    }
    
    turn.appendChild(a);
  }
  
  container.appendChild(turn);
}


// --------------------------------------------------------------------------- conversations
function newChat() { showChatPage(); S.cid = null; $("transcript").innerHTML = ""; showEmpty(true); loadConversations(); }
function showEmpty(on) {
  if (on && !$("empty")) {
    $("transcript").innerHTML = `<div class="empty" id="empty"><div class="empty-mark">☤</div><h1>Source Agent</h1>
      <p>A personal AI agent that runs shell commands, edits files, searches the web, and remembers across sessions — locally via Ollama.</p></div>`;
  } else if (!on && $("empty")) { $("empty").remove(); }
}
async function openConversation(cid) {
  showChatPage();
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
  if (!S.cid) {
    S.cid = Array.from({length: 12}, () => Math.floor(Math.random()*16).toString(16)).join('');
  }
  const controller = new AbortController();
  S.abortController = controller;
  S.busy = true;
  updateSendButtons();
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
    const res = await rfetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: text, conversation_id: S.cid, model: $("modelSelect").value }),
      signal: controller.signal
    }, 2);
    if (!res.ok || !res.body) throw new Error(await res.text());
    const reader = res.body.getReader(); const dec = new TextDecoder(); let buf = "";
    for (;;) {
      const { done, value } = await reader.read(); if (done) break;
      buf += dec.decode(value, { stream: true });
      let i;
      while ((i = buf.indexOf("\n")) >= 0) { const line = buf.slice(0, i); buf = buf.slice(i + 1); if (line.trim()) try { onEvent(JSON.parse(line)); } catch (e) {} }
    }
  } catch (e) {
    working.remove();
    const er = document.createElement("div");
    if (e.name === "AbortError" || (e.message && e.message.includes("aborted"))) {
      er.innerHTML = `<p style="color:var(--accent)">Execution stopped by user.</p>`;
    } else {
      er.innerHTML = `<p style="color:var(--red)">Error: ${e.message}</p>`;
    }
    a.appendChild(er);
  } finally {
    if (working.parentNode) working.remove();
    S.busy = false;
    S.abortController = null;
    updateSendButtons();
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

// --------------------------------------------------------------------------- stop chat
function updateSendButtons() {
  const sendBtn = $("send");
  const miniSendBtn = $("miniSend");
  if (S.busy) {
    if (sendBtn) { sendBtn.textContent = "■"; sendBtn.title = "Stop"; }
    if (miniSendBtn) { miniSendBtn.textContent = "■"; miniSendBtn.title = "Stop"; }
  } else {
    if (sendBtn) { sendBtn.textContent = "↑"; sendBtn.title = "Send"; }
    if (miniSendBtn) { miniSendBtn.textContent = "↑"; miniSendBtn.title = "Send"; }
  }
}

async function stopChat() {
  if (S.abortController) {
    S.abortController.abort();
  }
  if (S.cid) {
    try {
      fetch("/api/chat/stop", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ conversation_id: S.cid })
      });
    } catch (e) {
      console.error("Failed to stop chat:", e);
    }
  }
}
window.stopChat = stopChat;

// --------------------------------------------------------------------------- wire
function autosize() { const i = $("input"); i.style.height = "auto"; i.style.height = Math.min(i.scrollHeight, 180) + "px"; }
document.addEventListener("DOMContentLoaded", () => {
  $("send").onclick = () => { if (S.busy) stopChat(); else send(); };
  $("input").addEventListener("input", autosize);
  $("input").addEventListener("keydown", (e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } });
  $("newChat").onclick = newChat;
  $("clearMem").onclick = async () => { if (confirm("Clear all agent memory?")) { await api("/memory", { method: "DELETE" }); loadMemory(); } };
  $("changeWs").onclick = openWsModal;
  $("modalCancel").onclick = () => ($("wsModal").hidden = true);
  $("modalOpen").onclick = chooseWs;
  const sug = $("suggest"); if (sug) sug.querySelectorAll("button").forEach((b) => (b.onclick = () => send(b.textContent)));
  $("modelSelect").onchange = async () => {
    const val = $("modelSelect").value;
    try {
      await api("/models/active", { method: "POST", body: JSON.stringify({ name: val }) });
      toast("Active model set to " + val);
    } catch (e) {
      toast("Failed to set active model");
    }
  };
  // Connectors
  $("addConn").onclick = () => ($("connModal").hidden = false);
  $("connCancel").onclick = () => ($("connModal").hidden = true);
  $("connSave").onclick = addConnector;
  // Skills
  $("addSkill").onclick = () => ($("skillModal").hidden = false);
  $("skillCancel").onclick = closeSkillModal;
  $("skillSave").onclick = installSkill;

  // Tabs
  const btnPaste = $("btnSkillPaste");
  const btnUpload = $("btnSkillUpload");
  const pasteSec = $("skillPasteSection");
  const uploadSec = $("skillUploadSection");
  if (btnPaste && btnUpload) {
    btnPaste.onclick = () => {
      skillInputMode = "paste";
      btnPaste.classList.add("active");
      btnUpload.classList.remove("active");
      pasteSec.hidden = false;
      uploadSec.hidden = true;
    };
    btnUpload.onclick = () => {
      skillInputMode = "upload";
      btnUpload.classList.add("active");
      btnPaste.classList.remove("active");
      pasteSec.hidden = true;
      uploadSec.hidden = false;
    };
  }

  // Dropzone
  const fileInput = $("skillFileInput");
  const dropzone = $("skillDropzone");
  const fileInfo = $("skillFileInfo");
  if (dropzone && fileInput) {
    dropzone.onclick = () => fileInput.click();
    fileInput.onchange = (e) => {
      if (e.target.files.length > 0) {
        handleSelectedSkillFile(e.target.files[0]);
      }
    };

    dropzone.addEventListener("dragover", (e) => {
      e.preventDefault();
      dropzone.classList.add("dragover");
    });
    dropzone.addEventListener("dragleave", () => {
      dropzone.classList.remove("dragover");
    });
    dropzone.addEventListener("drop", (e) => {
      e.preventDefault();
      dropzone.classList.remove("dragover");
      if (e.dataTransfer.files.length > 0) {
        handleSelectedSkillFile(e.dataTransfer.files[0]);
      }
    });
  }

  function handleSelectedSkillFile(file) {
    if (!file.name.endsWith(".md")) {
      toast("Please select a .md file");
      return;
    }
    selectedSkillFile = file;
    fileInfo.textContent = `Selected: ${file.name} (${(file.size / 1024).toFixed(1)} KB)`;
    fileInfo.hidden = false;
  }

  // Routines
  $("addRoutine").onclick = () => {
    $("routineModal").hidden = false;
    const activeWs = $("wsPath").textContent;
    if (activeWs && activeWs !== "—") {
      $("routineWorkspace").value = activeWs;
    }
  };
  $("routineCancel").onclick = closeRoutineModal;
  $("routineSave").onclick = createRoutine;
  
  $("routineTriggerType").onchange = (e) => {
    $("routineIntervalLabel").hidden = (e.target.value !== "scheduled");
  };
  
  setInterval(loadRoutines, 5000);

  // Coder section click
  $("btnCoderChats").onclick = showCoderPage;

  boot();
});
