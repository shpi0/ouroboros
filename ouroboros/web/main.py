"""
Ouroboros Web Interface — FastAPI backend.

Endpoints:
  GET  /              → chat.html (SPA)
  GET  /stats         → stats.html
  GET  /settings      → settings.html
  WS   /ws/chat       → WebSocket chat relay
  GET  /api/stats     → JSON stats from state.json + events.jsonl
  GET  /api/settings  → JSON current settings
  POST /api/settings  → Update total_usd budget
  POST /api/restart   → Request agent restart

Run via runner.py (started from supervisor/colab_launcher.py at boot).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import time
import uuid
from collections import Counter
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# -- FastAPI / Starlette
try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
    from fastapi.staticfiles import StaticFiles
except ImportError:
    raise ImportError("fastapi and uvicorn required: pip install fastapi uvicorn")

# ---------------------------------------------------------------------------
# Paths (resolved from env or defaults)
# ---------------------------------------------------------------------------
STATE_ROOT = pathlib.Path(os.environ.get("OUROBOROS_STATE_ROOT", "/home/ouroboros/state"))
STATE_FILE = STATE_ROOT / "state.json"
EVENTS_LOG = STATE_ROOT / "logs" / "events.jsonl"
WEB_INBOX = STATE_ROOT / "web_inbox"
WEB_OUTBOX = STATE_ROOT / "web_outbox"
STATIC_DIR = pathlib.Path(__file__).parent / "static"

WEB_INBOX.mkdir(parents=True, exist_ok=True)
WEB_OUTBOX.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Ouroboros Web UI", docs_url=None, redoc_url=None)


def _read_state() -> Dict[str, Any]:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_events_tail(n: int = 500) -> List[Dict[str, Any]]:
    """Read last N lines from events.jsonl."""
    if not EVENTS_LOG.exists():
        return []
    try:
        lines = EVENTS_LOG.read_text(encoding="utf-8").splitlines()
        events = []
        for line in lines[-n:]:
            raw = line.strip()
            if not raw:
                continue
            try:
                events.append(json.loads(raw))
            except Exception:
                pass
        return events
    except Exception:
        return []


def _write_state(patch: Dict[str, Any]) -> bool:
    """Patch state.json fields."""
    try:
        st = _read_state()
        st.update(patch)
        STATE_FILE.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception as e:
        log.error("Failed to write state: %s", e)
        return False


# ---------------------------------------------------------------------------
# Static pages
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def page_chat():
    p = STATIC_DIR / "index.html"
    return HTMLResponse(p.read_text(encoding="utf-8") if p.exists() else "<h1>Not found</h1>")


@app.get("/stats", response_class=HTMLResponse)
async def page_stats():
    p = STATIC_DIR / "stats.html"
    return HTMLResponse(p.read_text(encoding="utf-8") if p.exists() else "<h1>Not found</h1>")


@app.get("/settings", response_class=HTMLResponse)
async def page_settings():
    p = STATIC_DIR / "settings.html"
    return HTMLResponse(p.read_text(encoding="utf-8") if p.exists() else "<h1>Not found</h1>")


# ---------------------------------------------------------------------------
# API: stats
# ---------------------------------------------------------------------------

@app.get("/api/stats")
async def api_stats():
    st = _read_state()
    events = _read_events_tail(1000)

    # Tool call counter
    tool_counts: Counter = Counter()
    timeline: List[Dict[str, Any]] = []

    for evt in events:
        if not isinstance(evt, dict):
            continue
        t = evt.get("type") or ""
        if t == "tool_call" or t == "tool_result":
            tool = evt.get("tool") or evt.get("name") or ""
            if tool:
                tool_counts[tool] += 1
        if t == "llm_round" and evt.get("cost_usd"):
            timeline.append({
                "ts": evt.get("ts") or evt.get("timestamp"),
                "cost_usd": float(evt.get("cost_usd") or 0),
            })

    top_tools = [{"tool": k, "count": v}
                 for k, v in tool_counts.most_common(15)]

    total_usd = float(st.get("total_budget_usd") or st.get("total_usd") or 50.0)
    spent_usd = float(st.get("spent_usd") or 0.0)
    session_snap = float(st.get("session_total_snapshot") or 0.0)
    session_spent = float(st.get("session_spent_snapshot") or 0.0)
    session_spent_usd = max(0.0, spent_usd - session_snap + session_spent - session_snap)
    # simpler: just report current session delta
    session_spent_usd = max(0.0, spent_usd - session_snap)

    return JSONResponse({
        "spent_usd": spent_usd,
        "session_spent_usd": session_spent_usd,
        "total_usd": total_usd,
        "remaining_usd": max(0.0, total_usd - spent_usd),
        "spent_calls": int(st.get("spent_calls") or 0),
        "spent_tokens_prompt": int(st.get("spent_tokens_prompt") or 0),
        "spent_tokens_completion": int(st.get("spent_tokens_completion") or 0),
        "spent_tokens_cached": int(st.get("spent_tokens_cached") or 0),
        "version": st.get("version") or "?",
        "branch": st.get("current_branch") or "?",
        "top_tools": top_tools,
        "timeline": timeline[-120:],  # last 120 LLM rounds
    })


# ---------------------------------------------------------------------------
# API: settings
# ---------------------------------------------------------------------------

@app.get("/api/settings")
async def api_settings_get():
    st = _read_state()
    return JSONResponse({
        "version": st.get("version") or "?",
        "branch": st.get("current_branch") or "?",
        "session_id": st.get("session_id") or "?",
        "spent_usd": float(st.get("spent_usd") or 0),
        "total_usd": float(st.get("total_budget_usd") or st.get("total_usd") or 50.0),
    })


@app.post("/api/settings")
async def api_settings_post(body: Dict[str, Any]):
    if "total_usd" in body:
        val = float(body["total_usd"])
        if val <= 0:
            return JSONResponse({"ok": False, "error": "total_usd must be > 0"}, status_code=400)
        ok = _write_state({"total_budget_usd": val})
        if not ok:
            return JSONResponse({"ok": False, "error": "failed to write state"}, status_code=500)
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# API: restart
# ---------------------------------------------------------------------------

@app.post("/api/restart")
async def api_restart():
    """Inject a web restart request via web_inbox. The agent loop picks it up."""
    msg_id = uuid.uuid4().hex[:12]
    ws_id = "api"
    inbox_file = WEB_INBOX / f"{int(time.time()*1000)}_{msg_id}.json"
    try:
        inbox_file.write_text(json.dumps({
            "id": msg_id,
            "ws_id": ws_id,
            "text": "/restart Requested via web UI",
        }, ensure_ascii=False), encoding="utf-8")
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# WebSocket chat relay
# ---------------------------------------------------------------------------

# Active WebSocket connections: ws_id → WebSocket
_connections: Dict[str, WebSocket] = {}
_outbox_watcher_started = False


async def _outbox_watcher():
    """Poll WEB_OUTBOX for response files and deliver to the right WebSocket."""
    while True:
        try:
            for f in sorted(WEB_OUTBOX.glob("*.json")):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                    f.unlink(missing_ok=True)
                    ws_id = data.get("ws_id") or ""
                    if ws_id and ws_id in _connections:
                        ws = _connections[ws_id]
                        await ws.send_json(data)
                    elif not ws_id:
                        # Broadcast to all
                        for ws in list(_connections.values()):
                            try:
                                await ws.send_json(data)
                            except Exception:
                                pass
                except Exception as e:
                    log.debug("outbox read error: %s", e)
        except Exception as e:
            log.debug("outbox watcher error: %s", e)
        await asyncio.sleep(0.3)


@app.websocket("/ws/chat")
async def ws_chat(websocket: WebSocket):
    global _outbox_watcher_started

    await websocket.accept()
    ws_id = uuid.uuid4().hex[:12]
    _connections[ws_id] = websocket
    log.info("WebSocket connected: %s", ws_id)

    # Start outbox watcher if not running yet
    if not _outbox_watcher_started:
        _outbox_watcher_started = True
        asyncio.create_task(_outbox_watcher())

    # Send welcome message
    await websocket.send_json({
        "type": "system",
        "text": "Connected to Ouroboros",
        "ws_id": ws_id,
    })

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                msg = {"text": raw}

            text = (msg.get("text") or "").strip()
            if not text:
                continue

            # Echo user message back
            await websocket.send_json({
                "type": "user",
                "text": text,
            })

            # Write to inbox — supervisor will pick it up and route to agent
            msg_id = uuid.uuid4().hex[:12]
            inbox_file = WEB_INBOX / f"{int(time.time()*1000)}_{msg_id}.json"
            inbox_file.write_text(json.dumps({
                "id": msg_id,
                "ws_id": ws_id,
                "text": text,
            }, ensure_ascii=False), encoding="utf-8")

    except WebSocketDisconnect:
        log.info("WebSocket disconnected: %s", ws_id)
    finally:
        _connections.pop(ws_id, None)
