"""
Ouroboros Web Interface — FastAPI backend.

Serves static frontend + REST API + WebSocket chat.
Listens on localhost:8080. nginx handles mTLS in front.

Endpoints:
  GET  /              → chat page
  GET  /stats         → stats page
  GET  /settings      → settings page
  GET  /api/stats     → JSON metrics
  GET  /api/settings  → JSON current settings
  POST /api/settings  → update budget
  POST /api/restart   → request agent restart
  WS   /ws/chat       → WebSocket chat channel
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
STATE_ROOT = pathlib.Path(os.environ.get("OUROBOROS_STATE_ROOT", "/home/ouroboros/state"))
STATE_FILE = STATE_ROOT / "state.json"
EVENTS_LOG = STATE_ROOT / "logs" / "events.jsonl"
CHAT_LOG = STATE_ROOT / "logs" / "chat.jsonl"
WEB_INBOX = STATE_ROOT / "web_inbox"
WEB_OUTBOX = STATE_ROOT / "web_outbox"

STATIC_DIR = pathlib.Path(__file__).parent / "static"
TOTAL_BUDGET = float(os.environ.get("TOTAL_BUDGET", "50"))

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Ouroboros Web", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_state() -> Dict[str, Any]:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_last_lines(path: pathlib.Path, n: int = 500) -> List[str]:
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        lines = [ln for ln in text.splitlines() if ln.strip()]
        return lines[-n:]
    except Exception:
        return []


def _parse_events(lines: List[str]) -> List[Dict[str, Any]]:
    events = []
    for ln in lines:
        try:
            events.append(json.loads(ln))
        except Exception:
            pass
    return events


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
async def page_chat():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/stats", include_in_schema=False)
async def page_stats():
    return FileResponse(STATIC_DIR / "stats.html")


@app.get("/settings", include_in_schema=False)
async def page_settings():
    return FileResponse(STATIC_DIR / "settings.html")


# ---------------------------------------------------------------------------
# API: stats
# ---------------------------------------------------------------------------

@app.get("/api/stats")
async def api_stats():
    st = _read_state()
    total = float(st.get("total_usd") or TOTAL_BUDGET)
    spent = float(st.get("spent_usd") or 0.0)

    lines = _read_last_lines(EVENTS_LOG, 500)
    events = _parse_events(lines)

    # Timeline: llm_round events with cost
    timeline = []
    tool_counts: Dict[str, int] = {}
    for evt in events:
        if evt.get("type") == "llm_round":
            cost = float(evt.get("cost_usd") or 0.0)
            if cost > 0:
                timeline.append({
                    "ts": evt.get("ts", ""),
                    "cost_usd": cost,
                    "tool": evt.get("tool_name") or evt.get("tool") or "",
                })
        # Count tool calls
        tool = evt.get("tool_name") or evt.get("tool") or ""
        if tool:
            tool_counts[tool] = tool_counts.get(tool, 0) + 1

    # Top 10 tools
    top_tools = sorted(tool_counts.items(), key=lambda x: -x[1])[:10]

    # Session snapshot
    session_snapshot = float(st.get("session_spent_snapshot") or 0.0)
    session_total_snapshot = float(st.get("session_total_snapshot") or 0.0)
    session_spent = max(0.0, spent - session_snapshot) if session_snapshot else 0.0

    return JSONResponse({
        "spent_usd": spent,
        "remaining_usd": max(0.0, total - spent),
        "total_usd": total,
        "spent_calls": int(st.get("spent_calls") or 0),
        "spent_tokens_prompt": int(st.get("spent_tokens_prompt") or 0),
        "spent_tokens_completion": int(st.get("spent_tokens_completion") or 0),
        "spent_tokens_cached": int(st.get("spent_tokens_cached") or 0),
        "session_spent_usd": round(session_spent, 4),
        "version": st.get("version", "?"),
        "branch": st.get("current_branch", "?"),
        "timeline": timeline[-100:],
        "top_tools": [{"tool": t, "count": c} for t, c in top_tools],
    })


# ---------------------------------------------------------------------------
# API: settings
# ---------------------------------------------------------------------------

@app.get("/api/settings")
async def api_settings_get():
    st = _read_state()
    return JSONResponse({
        "total_usd": float(st.get("total_usd") or TOTAL_BUDGET),
        "spent_usd": float(st.get("spent_usd") or 0.0),
        "version": st.get("version", "?"),
        "branch": st.get("current_branch", "?"),
        "session_id": st.get("session_id", ""),
    })


@app.post("/api/settings")
async def api_settings_post(body: Dict[str, Any]):
    st = _read_state()
    if "total_usd" in body:
        val = float(body["total_usd"])
        if val <= 0:
            return JSONResponse({"ok": False, "error": "Budget must be > 0"}, status_code=400)
        st["total_usd"] = val
        # Also update TOTAL_BUDGET_LIMIT if present
        try:
            text = json.dumps(st, ensure_ascii=False, indent=2)
            STATE_FILE.write_text(text, encoding="utf-8")
            return JSONResponse({"ok": True})
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
    return JSONResponse({"ok": False, "error": "Nothing to update"}, status_code=400)


# ---------------------------------------------------------------------------
# API: restart
# ---------------------------------------------------------------------------

@app.post("/api/restart")
async def api_restart():
    try:
        req_file = STATE_ROOT / "web_restart_request.json"
        req_file.write_text(json.dumps({
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "source": "web",
        }), encoding="utf-8")
        return JSONResponse({"ok": True, "message": "Restart request written. Supervisor will process it shortly."})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# WebSocket: chat
# ---------------------------------------------------------------------------

# Active connections: ws_id → WebSocket
_connections: Dict[str, WebSocket] = {}

# Ensure inbox/outbox dirs exist
WEB_INBOX.mkdir(parents=True, exist_ok=True)
WEB_OUTBOX.mkdir(parents=True, exist_ok=True)


async def _send_chat_history(ws: WebSocket) -> None:
    """Send last 20 chat messages to newly connected client."""
    lines = _read_last_lines(CHAT_LOG, 100)
    messages = []
    for ln in lines:
        try:
            msg = json.loads(ln)
            role = msg.get("role") or ("user" if msg.get("from") == "owner" else "assistant")
            text = msg.get("text") or msg.get("content") or ""
            if text:
                messages.append({
                    "type": "history",
                    "role": role,
                    "text": text,
                    "ts": msg.get("ts", ""),
                })
        except Exception:
            pass
    # Send last 20
    for m in messages[-20:]:
        await ws.send_json(m)


async def _poll_outbox(ws: WebSocket, ws_id: str) -> None:
    """Poll web_outbox for responses addressed to this ws_id."""
    while ws_id in _connections:
        try:
            for f in sorted(WEB_OUTBOX.glob(f"{ws_id}_*.json")):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                    await ws.send_json(data)
                    f.unlink(missing_ok=True)
                except Exception:
                    pass
        except Exception:
            pass
        await asyncio.sleep(0.5)


@app.websocket("/ws/chat")
async def ws_chat(ws: WebSocket):
    await ws.accept()
    ws_id = uuid.uuid4().hex[:12]
    _connections[ws_id] = ws

    try:
        await _send_chat_history(ws)
        await ws.send_json({"type": "connected", "ws_id": ws_id})

        # Start outbox poller
        poll_task = asyncio.create_task(_poll_outbox(ws, ws_id))

        async for raw in ws.iter_text():
            try:
                msg = json.loads(raw)
            except Exception:
                msg = {"text": raw}

            text = str(msg.get("text") or "").strip()
            if not text:
                continue

            # Write to inbox for supervisor to pick up
            msg_id = uuid.uuid4().hex[:12]
            inbox_file = WEB_INBOX / f"{msg_id}.json"
            inbox_file.write_text(json.dumps({
                "id": msg_id,
                "ws_id": ws_id,
                "text": text,
                "ts": datetime.now(timezone.utc).isoformat(),
            }), encoding="utf-8")

            # Echo back as user message
            await ws.send_json({
                "type": "message",
                "role": "user",
                "text": text,
                "ts": datetime.now(timezone.utc).isoformat(),
            })

    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.warning("WS error for %s: %s", ws_id, e)
    finally:
        _connections.pop(ws_id, None)
        poll_task.cancel()


# ---------------------------------------------------------------------------
# Entry point (for direct run)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="127.0.0.1", port=8080)
