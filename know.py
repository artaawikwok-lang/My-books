"""

  jangan di salin kode ini dan ketahui lah konsekuensi nya !!
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

app = FastAPI(title="NEXUS SERVER", version="2.0.0")

# ==================== CONFIG ====================
WORKSPACE = Path(tempfile.gettempdir()) / "nexus_ws"
WORKSPACE.mkdir(parents=True, exist_ok=True)

EXEC_TIMEOUT = 25
MAX_OUTPUT = 200_000
MAX_CODE_SIZE = 200_000
MAX_CONCURRENT = 3


# ==================== RUNTIMES ====================
def _which(name: str) -> Optional[str]:
    return shutil.which(name)


RUNTIMES: dict[str, dict] = {
    "python": {"label": "Python", "ext": ".py", "exe": _which("python3") or _which("python"), "args": []},
    "node":   {"label": "Node.js", "ext": ".js", "exe": _which("node"), "args": []},
    "go":     {"label": "Go", "ext": ".go", "exe": _which("go"), "args": ["run"]},
}


def available_runtimes() -> list[dict]:
    return [
        {"key": k, "label": v["label"], "ext": v["ext"], "available": bool(v["exe"])}
        for k, v in RUNTIMES.items()
    ]


# ==================== STATE ====================
_runs: dict[str, dict] = {}
_active_count = 0


class RunPayload(BaseModel):
    code: str
    language: str = "python"


class StopPayload(BaseModel):
    id: str


# ==================== EXECUTOR ====================
async def _execute(run_id: str, code: str, language: str) -> None:
    global _active_count

    def _finish(exit_code: int) -> None:
        global _active_count
        _runs[run_id]["done"] = True
        _runs[run_id]["exit_code"] = exit_code
        _active_count = max(0, _active_count - 1)

    runtime = language.lower()
    if runtime not in RUNTIMES:
        _runs[run_id]["events"].append({"type": "stderr", "data": f"Runtime '{language}' tidak didukung.\n"})
        _runs[run_id]["events"].append({"type": "exit", "code": 2})
        _finish(2)
        return

    info = RUNTIMES[runtime]
    if not info["exe"]:
        _runs[run_id]["events"].append({"type": "stderr", "data": f"Runtime {info['label']} tidak tersedia.\n"})
        _runs[run_id]["events"].append({"type": "exit", "code": 127})
        _finish(127)
        return

    run_dir = WORKSPACE / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    script = run_dir / f"main{info['ext']}"
    script.write_text(code, encoding="utf-8")

    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(run_dir),
        "PYTHONUNBUFFERED": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "NODE_NO_WARNINGS": "1",
        "TMPDIR": str(run_dir),
    }

    argv = [info["exe"], *info["args"], str(script)]
    _runs[run_id]["events"].append({"type": "system", "data": f"$ {' '.join(argv)}\n"})
    _runs[run_id]["started_at"] = time.time()

    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
            cwd=str(run_dir),
            env=env,
        )
    except FileNotFoundError as e:
        _runs[run_id]["events"].append({"type": "stderr", "data": f"runtime error: {e}\n"})
        _runs[run_id]["events"].append({"type": "exit", "code": 127})
        _finish(127)
        return

    _runs[run_id]["proc"] = proc
    total = 0

    async def pump(stream, kind):
        nonlocal total
        while True:
            chunk = await stream.readline()
            if not chunk:
                return
            text = chunk.decode("utf-8", errors="replace")
            total += len(text)
            if total <= MAX_OUTPUT:
                _runs[run_id]["events"].append({"type": kind, "data": text})

    try:
        await asyncio.wait_for(
            asyncio.gather(
                pump(proc.stdout, "stdout"),
                pump(proc.stderr, "stderr"),
                proc.wait(),
                return_exceptions=True,
            ),
            timeout=EXEC_TIMEOUT,
        )
    except asyncio.TimeoutError:
        _runs[run_id]["events"].append({"type": "stderr", "data": f"\n[timeout {EXEC_TIMEOUT}s] proses dibunuh\n"})
        try:
            proc.send_signal(signal.SIGTERM)
            await asyncio.wait_for(proc.wait(), timeout=3)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    code_exit = proc.returncode if proc.returncode is not None else -1
    dur = round(time.time() - _runs[run_id]["started_at"], 3)
    _runs[run_id]["events"].append({"type": "exit", "code": code_exit, "duration": dur})
    _finish(code_exit)

    try:
        shutil.rmtree(run_dir, ignore_errors=True)
    except Exception:
        pass


# ==================== API ====================
@app.get("/api/status")
async def status():
    return {
        "ok": True,
        "app": "NEXUS SERVER",
        "mode": "vercel",
        "runtimes": available_runtimes(),
        "limits": {
            "timeout": EXEC_TIMEOUT,
            "max_output": MAX_OUTPUT,
            "max_code": MAX_CODE_SIZE,
            "max_concurrent": MAX_CONCURRENT,
        },
        "active": _active_count,
    }


@app.get("/api/runtimes")
async def runtimes():
    return available_runtimes()


@app.post("/api/run")
async def api_run(payload: RunPayload):
    global _active_count

    if _active_count >= MAX_CONCURRENT:
        raise HTTPException(429, "server sibuk, coba lagi sebentar")
    if len(payload.code) > MAX_CODE_SIZE:
        raise HTTPException(413, f"kode terlalu besar (max {MAX_CODE_SIZE} byte)")
    if not payload.code.strip():
        raise HTTPException(400, "kode kosong")

    run_id = uuid.uuid4().hex
    _runs[run_id] = {
        "id": run_id, "events": [], "done": False,
        "started_at": time.time(), "language": payload.language, "proc": None,
    }
    _active_count += 1
    asyncio.create_task(_execute(run_id, payload.code, payload.language))
    return {"ok": True, "id": run_id}


@app.post("/api/stop")
async def api_stop(payload: StopPayload):
    run = _runs.get(payload.id)
    if not run:
        raise HTTPException(404, "run id tidak ditemukan")
    proc = run.get("proc")
    if proc and proc.returncode is None:
        try:
            proc.send_signal(signal.SIGTERM)
            run["events"].append({"type": "stderr", "data": "\n[dihentikan]\n"})
            return {"ok": True, "stopped": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}
    return {"ok": False, "stopped": False}


@app.get("/api/output")
async def api_output(id: str, since: int = 0):
    run = _runs.get(id)
    if not run:
        raise HTTPException(404, "run id tidak ditemukan")
    return {
        "id": id,
        "events": run["events"][since:],
        "next": len(run["events"]),
        "done": run["done"],
        "exit_code": run.get("exit_code"),
    }


@app.get("/api/stream/{run_id}")
async def api_stream(run_id: str, request: Request):
    if run_id not in _runs:
        raise HTTPException(404, "run id tidak ditemukan")

    async def gen():
        since = 0
        idle = 0
        while True:
            if await request.is_disconnected():
                break
            run = _runs.get(run_id)
            if not run:
                yield f"data: {json.dumps({'type':'end'})}\n\n"
                break
            events = run["events"][since:]
            for ev in events:
                yield f"data: {json.dumps(ev)}\n\n"
            since = len(run["events"])
            if run["done"] and not events:
                yield f"data: {json.dumps({'type':'end'})}\n\n"
                break
            if not events:
                idle += 1
                if idle > 60:
                    yield f"data: {json.dumps({'type':'end'})}\n\n"
                    break
                await asyncio.sleep(0.5)
            else:
                idle = 0

    return StreamingResponse(gen(), media_type="text/event-stream")


# ==================== FRONTEND ====================
FRONTEND_HTML = r"""<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>NEXUS SERVER</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/monaco-editor@0.45.0/min/vs/loader.js"></script>
<style>
:root{--bg:#0a0b0f;--e:#10121a;--e2:#151824;--e3:#1b1f2e;--b:#232838;--bs:#1a1e2c;--t:#e6e9f2;--td:#8b93a7;--tm:#5a6275;--ac:#6366f1;--ac2:#8b5cf6;--acs:rgba(99,102,241,.12);--g:#22c55e;--r:#ef4444;--y:#eab308;--rad:12px;--sh:0 8px 32px rgba(0,0,0,.4)}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--bg);color:var(--t);font-family:'Inter',system-ui,sans-serif;font-size:14px;line-height:1.5;-webkit-font-smoothing:antialiased;overflow:hidden}
::-webkit-scrollbar{width:8px;height:8px}
::-webkit-scrollbar-thumb{background:var(--b);border-radius:8px}
.mono{font-family:'JetBrains Mono',monospace}
.app{display:grid;grid-template-columns:220px 1fr;height:100vh}
.sb{background:var(--e);border-right:1px solid var(--bs);display:flex;flex-direction:column;padding:16px 12px;gap:6px}
.brand{display:flex;align-items:center;gap:10px;padding:8px 8px 18px}
.bm{width:34px;height:34px;border-radius:9px;background:linear-gradient(135deg,var(--ac),var(--ac2));display:grid;place-items:center;font-family:'JetBrains Mono',monospace;font-weight:700;color:#fff;font-size:13px}
.bt{display:flex;flex-direction:column;line-height:1.15}
.bt b{font-weight:700;font-size:14px;letter-spacing:.3px}
.bt span{font-size:10px;color:var(--tm);font-weight:500;letter-spacing:.5px;text-transform:uppercase}
.sfoot{margin-top:auto;padding:10px 12px;display:flex;align-items:center;gap:8px;font-size:11px;color:var(--tm);border-top:1px solid var(--bs)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--tm);flex-shrink:0}
.dot.on{background:var(--g);box-shadow:0 0 8px var(--g)}
.dot.off{background:var(--r);box-shadow:0 0 8px var(--r)}
.main{display:flex;flex-direction:column;overflow:hidden}
.top{height:54px;border-bottom:1px solid var(--bs);background:rgba(16,18,26,.6);backdrop-filter:blur(12px);display:flex;align-items:center;justify-content:space-between;padding:0 20px;gap:12px;flex-shrink:0}
.crumbs{font-size:13px;color:var(--td)}
.crumbs b{color:var(--t);font-weight:600}
.right{display:flex;gap:8px;align-items:center}
.content{flex:1;overflow-y:auto;padding:20px;display:flex;flex-direction:column;gap:14px;min-height:0}
.btn{display:inline-flex;align-items:center;gap:6px;padding:8px 14px;border-radius:8px;font-size:13px;font-weight:500;border:1px solid var(--b);background:var(--e2);color:var(--t);cursor:pointer;transition:all .15s;font-family:inherit;white-space:nowrap}
.btn:hover{background:var(--e3)}
.btn-primary{background:linear-gradient(135deg,var(--ac),var(--ac2));border-color:transparent;color:#fff}
.btn-primary:hover{filter:brightness(1.1)}
.btn-danger{color:var(--r);border-color:rgba(239,68,68,.25)}
.btn-danger:hover{background:rgba(239,68,68,.1)}
.btn-sm{padding:5px 10px;font-size:12px}
.btn:disabled{opacity:.5;cursor:not-allowed}
.card{background:var(--e);border:1px solid var(--bs);border-radius:var(--rad);padding:14px;display:flex;flex-direction:column;gap:6px}
.ct{font-size:11px;color:var(--td);text-transform:uppercase;letter-spacing:.6px;font-weight:600}
.cv{font-size:16px;font-weight:700}
.cv.small{font-size:12px;font-weight:500;word-break:break-all}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}
.panel{background:var(--e);border:1px solid var(--bs);border-radius:var(--rad);display:flex;flex-direction:column;overflow:hidden;flex:1;min-height:0}
.ph{padding:10px 14px;border-bottom:1px solid var(--bs);display:flex;align-items:center;justify-content:space-between;background:var(--e2);gap:10px}
.sel{padding:7px 10px;background:var(--e2);border:1px solid var(--b);border-radius:7px;color:var(--t);font-size:12.5px;font-family:inherit;cursor:pointer}
.sel:focus{outline:none;border-color:var(--ac)}
#monaco{flex:1;min-height:220px;background:#0d0f15}
.term{background:#050609;border:1px solid var(--bs);border-radius:var(--rad);height:220px;display:flex;flex-direction:column;overflow:hidden;flex-shrink:0}
.th{padding:8px 12px;background:var(--e2);border-bottom:1px solid var(--bs);display:flex;align-items:center;justify-content:space-between;font-size:11px;font-weight:600;color:var(--td);letter-spacing:.6px;text-transform:uppercase;gap:8px}
.tb{flex:1;overflow-y:auto;padding:12px 14px;font-size:12.5px;line-height:1.6;color:#c9d1e0;white-space:pre-wrap;word-break:break-word}
.tb .stdout{color:#c9d1e0}
.tb .stderr{color:#ff8b8b}
.tb .system{color:var(--ac)}
.tb .exit{color:var(--g)}
.tb .info{color:var(--y)}
.toasts{position:fixed;bottom:20px;right:20px;display:flex;flex-direction:column;gap:8px;z-index:1000}
.toast{background:var(--e);border:1px solid var(--b);border-radius:10px;padding:11px 15px;font-size:13px;min-width:220px;animation:si .2s ease;box-shadow:var(--sh)}
.toast.ok{border-left:3px solid var(--g)}
.toast.err{border-left:3px solid var(--r)}
.toast.info{border-left:3px solid var(--ac)}
@keyframes si{from{transform:translateX(20px);opacity:0}to{transform:translateX(0);opacity:1}}
.badge{display:inline-flex;align-items:center;gap:5px;padding:3px 8px;border-radius:999px;font-size:11px;font-weight:500;background:var(--e2);color:var(--td)}
.badge .dot{width:6px;height:6px}
@media(max-width:800px){.app{grid-template-columns:1fr}.sb{display:none}.content{padding:12px}}
</style>
</head>
<body>
<div class="app">
  <aside class="sb">
    <div class="brand"><div class="bm">&gt;_</div><div class="bt"><b>NEXUS</b><span>SERVER</span></div></div>
    <div class="sfoot"><span class="dot" id="dot"></span><span id="stxt">checking...</span></div>
  </aside>

  <main class="main">
    <header class="top">
      <div class="crumbs">/ <b>Runner</b> <span style="color:var(--tm);font-size:11px;margin-left:6px">Vercel Edition</span></div>
      <div class="right">
        <select class="sel" id="lang"></select>
        <button class="btn btn-primary" id="runBtn">Run</button>
        <button class="btn btn-danger" id="stopBtn" disabled>Stop</button>
      </div>
    </header>

    <section class="content">
      <div class="cards">
        <div class="card"><div class="ct">Runtime</div><div class="cv small" id="rtInfo">-</div></div>
        <div class="card"><div class="ct">Timeout</div><div class="cv" id="tInfo">-</div></div>
        <div class="card"><div class="ct">Max Output</div><div class="cv" id="oInfo">-</div></div>
        <div class="card"><div class="ct">Status</div><div class="cv small" id="sInfo">idle</div></div>
      </div>

      <div class="panel" style="min-height:300px">
        <div class="ph">
          <span style="font-size:12px;color:var(--td);font-weight:600">CODE EDITOR</span>
          <span class="badge"><span class="dot" id="wsDot"></span><span id="wsTxt">idle</span></span>
        </div>
        <div id="monaco"></div>
      </div>

      <div class="term">
        <div class="th">
          <span>Terminal</span>
          <div>
            <button class="btn btn-sm" id="clearBtn">Clear</button>
            <button class="btn btn-sm" id="copyBtn">Copy</button>
          </div>
        </div>
        <div class="tb mono" id="term"></div>
      </div>
    </section>
  </main>
</div>

<div class="toasts" id="toasts"></div>

<script>
const $ = (id) => document.getElementById(id);
const STATE = { currentRunId: null, eventSource: null, pollTimer: null, editor: null, runtimes: [] };

function toast(msg, type="info") {
  const el = document.createElement("div");
  el.className = "toast " + type;
  el.textContent = msg;
  $("toasts").appendChild(el);
  setTimeout(() => el.remove(), 3500);
}

function term(kind, text) {
  const el = $("term");
  const span = document.createElement("span");
  span.className = kind;
  span.textContent = text;
  el.appendChild(span);
  el.scrollTop = el.scrollHeight;
}
function clearTerm() { $("term").innerHTML = ""; }

async function api(path, opts={}) {
  const r = await fetch(path, { headers: {"Content-Type": "application/json"}, ...opts });
  if (!r.ok) {
    const j = await r.json().catch(() => ({}));
    throw new Error(j.detail || r.statusText);
  }
  return r.json();
}

async function checkStatus() {
  try {
    const s = await api("/api/status");
    $("dot").className = "dot on";
    $("stxt").textContent = "online";
    STATE.runtimes = s.runtimes;
    const avail = s.runtimes.filter(r => r.available).map(r => r.label).join(", ") || "tidak ada";
    $("rtInfo").textContent = avail;
    $("tInfo").textContent = s.limits.timeout + "s";
    $("oInfo").textContent = Math.round(s.limits.max_output / 1024) + " KB";
    const sel = $("lang");
    const cur = sel.value;
    sel.innerHTML = "";
    s.runtimes.forEach(r => {
      const o = document.createElement("option");
      o.value = r.key;
      o.textContent = r.label + (r.available ? "" : " (tidak tersedia)");
      o.disabled = !r.available;
      sel.appendChild(o);
    });
    if (cur && s.runtimes.some(r => r.key === cur && r.available)) sel.value = cur;
  } catch(e) {
    $("dot").className = "dot off";
    $("stxt").textContent = "offline";
  }
}

const DEFAULT_CODE = {
  python: `import sys, time
print("Hello dari NEXUS SERVER di Vercel!")
print(f"Python: {sys.version.split()[0]}")
for i in range(3):
    print(f"tick {i}")
    time.sleep(0.3)
print("selesai.")`,
  node: `console.log("Hello dari NEXUS SERVER di Vercel!");
console.log("Node:", process.version);
let i = 0;
const t = setInterval(() => {
  console.log("tick " + i);
  if (++i >= 3) { clearInterval(t); console.log("selesai."); }
}, 300);`,
  go: `package main
import ("fmt"; "time")
func main() {
    fmt.Println("Hello dari NEXUS SERVER di Vercel!")
    for i := 0; i < 3; i++ {
        fmt.Println("tick", i)
        time.Sleep(300 * time.Millisecond)
    }
    fmt.Println("selesai.")
}`,
};

function initEditor() {
  require.config({ paths: { vs: "https://cdn.jsdelivr.net/npm/monaco-editor@0.45.0/min/vs" } });
  require(["vs/editor/editor.main"], () => {
    monaco.editor.defineTheme("nexus-dark", {
      base: "vs-dark", inherit: true, rules: [],
      colors: {
        "editor.background": "#0d0f15",
        "editor.lineHighlightBackground": "#151824",
        "editorLineNumber.foreground": "#3a4256",
      }
    });
    STATE.editor = monaco.editor.create($("monaco"), {
      value: DEFAULT_CODE.python,
      language: "python",
      theme: "nexus-dark",
      automaticLayout: true,
      fontSize: 13,
      fontFamily: "'JetBrains Mono', monospace",
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      tabSize: 4,
    });
  });
}

$("lang").onchange = () => {
  const lang = $("lang").value;
  if (!STATE.editor) return;
  const cur = STATE.editor.getValue();
  const isDefault = Object.values(DEFAULT_CODE).includes(cur);
  if (isDefault && DEFAULT_CODE[lang]) {
    STATE.editor.setValue(DEFAULT_CODE[lang]);
    const map = { python:"python", node:"javascript", go:"go" };
    monaco.editor.setModelLanguage(STATE.editor.getModel(), map[lang] || "plaintext");
  }
};

function setRunning(on) {
  $("runBtn").disabled = on;
  $("stopBtn").disabled = !on;
  $("sInfo").textContent = on ? "running" : "idle";
  $("wsDot").style.background = on ? "var(--g)" : "var(--tm)";
  $("wsDot").style.boxShadow = on ? "0 0 8px var(--g)" : "none";
  $("wsTxt").textContent = on ? "running" : "idle";
}

async function runCode() {
  if (!STATE.editor) { toast("Editor belum siap", "err"); return; }
  clearTerm();
  term("system", "$ menjalankan...\n");
  setRunning(true);
  try {
    const res = await api("/api/run", {
      method: "POST",
      body: JSON.stringify({ code: STATE.editor.getValue(), language: $("lang").value }),
    });
    STATE.currentRunId = res.id;
    startStream(res.id);
  } catch(e) {
    term("stderr", e.message + "\n");
    toast(e.message, "err");
    setRunning(false);
  }
}

function startStream(id) {
  try {
    const es = new EventSource(`/api/stream/${id}`);
    STATE.eventSource = es;
    es.onmessage = (msg) => { try { handleEvent(JSON.parse(msg.data)); } catch(e){} };
    es.onerror = () => {
      es.close();
      STATE.eventSource = null;
      if (STATE.currentRunId === id) startPolling(id);
    };
  } catch(e) { startPolling(id); }
}

function startPolling(id) {
  if (STATE.pollTimer) clearInterval(STATE.pollTimer);
  let since = 0;
  STATE.pollTimer = setInterval(async () => {
    try {
      const data = await api(`/api/output?id=${id}&since=${since}`);
      since = data.next;
      for (const ev of data.events) handleEvent(ev);
      if (data.done) {
        clearInterval(STATE.pollTimer);
        STATE.pollTimer = null;
        setRunning(false);
        STATE.currentRunId = null;
      }
    } catch(e) {
      clearInterval(STATE.pollTimer);
      STATE.pollTimer = null;
      setRunning(false);
    }
  }, 400);
}

function handleEvent(ev) {
  const t = ev.type;
  if (t === "stdout") term("stdout", ev.data);
  else if (t === "stderr") term("stderr", ev.data);
  else if (t === "system") term("system", ev.data);
  else if (t === "exit") {
    term("exit", `\n[exit ${ev.code}] durasi ${ev.duration ?? "?"}s\n`);
    if (STATE.eventSource) { STATE.eventSource.close(); STATE.eventSource = null; }
    if (STATE.pollTimer) { clearInterval(STATE.pollTimer); STATE.pollTimer = null; }
    setRunning(false);
    STATE.currentRunId = null;
  } else if (t === "end") {
    if (STATE.eventSource) { STATE.eventSource.close(); STATE.eventSource = null; }
    setRunning(false);
    STATE.currentRunId = null;
  } else if (t === "info") term("info", ev.data);
}

async function stopRun() {
  if (!STATE.currentRunId) return;
  try {
    await api("/api/stop", { method: "POST", body: JSON.stringify({ id: STATE.currentRunId }) });
    toast("Dihentikan", "ok");
  } catch(e) { toast(e.message, "err"); }
}

$("runBtn").onclick = runCode;
$("stopBtn").onclick = stopRun;
$("clearBtn").onclick = clearTerm;
$("copyBtn").onclick = () => {
  navigator.clipboard.writeText($("term").innerText);
  toast("Output dicopy", "ok");
};

document.addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
    e.preventDefault();
    runCode();
  }
});

checkStatus();
setInterval(checkStatus, 30000);
initEditor();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(FRONTEND_HTML)
```