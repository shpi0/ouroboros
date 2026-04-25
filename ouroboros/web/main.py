"""Ouroboros Web Interface — FastAPI backend."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
    from fastapi.responses import FileResponse, JSONResponse
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel
    _HAS_FASTAPI = True
except ImportError:
    _HAS_FASTAPI = False

from ouroboros.web.db import (
    init_db, list_chats, create_chat, delete_chat, rename_chat,
    add_message, get_messages, get_messages_since,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
STATE_ROOT = Path(os.environ.get("DRIVE_ROOT", "/home/ouroboros/state"))
STATE_FILE = STATE_ROOT / "state" / "state.json"
EVENTS_FILE = STATE_ROOT / "logs" / "events.jsonl"
WEB_INBOX  = STATE_ROOT / "web_inbox.jsonl"
WEB_OUTBOX = STATE_ROOT / "web_outbox.jsonl"
STATIC_DIR = Path(__file__).parent / "static"

# Numeric chat_id used in the task queue for "main" web chat (shared with Telegram owner)
WEB_MAIN_CHAT_ID = -999999

# ---------------------------------------------------------------------------
# WebSocket client registry: chat_id → set of connected sockets
# ---------------------------------------------------------------------------
_ws_clients: Dict[str, Set[WebSocket]] = {}


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
if _HAS_FASTAPI:
    app = FastAPI(title="Ouroboros", docs_url=None, redoc_url=None)

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.on_event("startup")
    async def _startup():
        init_db()
        asyncio.create_task(_poll_outbox_forever())

    # ── Static pages ──────────────────────────────────────────────────────
    @app.get("/")
    async def _index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/stats")
    async def _stats_page():
        return FileResponse(STATIC_DIR / "stats.html")

    @app.get("/settings")
    async def _settings_page():
        return FileResponse(STATIC_DIR / "settings.html")

    @app.get("/health")
    async def _health():
        return {"ok": True}

    # ── Chat REST API ──────────────────────────────────────────────────────
    @app.get("/api/chats")
    async def api_list_chats():
        return list_chats()

    class CreateChatBody(BaseModel):
        title: str = "Новый чат"

    @app.post("/api/chats")
    async def api_create_chat(body: CreateChatBody):
        return create_chat(body.title)

    @app.delete("/api/chats/{chat_id}")
    async def api_delete_chat(chat_id: str):
        if not delete_chat(chat_id):
            raise HTTPException(400, "Cannot delete main chat")
        return {"ok": True}

    class RenameChatBody(BaseModel):
        title: str

    @app.patch("/api/chats/{chat_id}")
    async def api_rename_chat(chat_id: str, body: RenameChatBody):
        rename_chat(chat_id, body.title)
        return {"ok": True}

    @app.get("/api/chats/{chat_id}/messages")
    async def api_get_messages(chat_id: str, limit: int = 200, offset: int = 0):
        return get_messages(chat_id, limit=limit, offset=offset)

    # ── WebSocket ──────────────────────────────────────────────────────────
    @app.websocket("/ws/chat/{chat_id}")
    async def ws_chat(websocket: WebSocket, chat_id: str):
        await websocket.accept()
        _ws_clients.setdefault(chat_id, set()).add(websocket)
        try:
            while True:
                raw = await websocket.receive_text()
                try:
                    payload = json.loads(raw)
                except Exception:
                    continue
                text = str(payload.get("text", "")).strip()
                if not text:
                    continue
                # Save user message
                msg = add_message(chat_id, "user", text)
                # Broadcast to all clients on this chat (optimistic echo)
                await _broadcast(chat_id, {"type": "message", **msg})
                # Route to agent (non-blocking)
                asyncio.get_event_loop().run_in_executor(
                    None, _enqueue_for_agent, chat_id, text
                )
        except WebSocketDisconnect:
            pass
        finally:
            _ws_clients.get(chat_id, set()).discard(websocket)

    async def _broadcast(chat_id: str, data: dict):
        dead = []
        for ws in list(_ws_clients.get(chat_id, set())):
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            _ws_clients.get(chat_id, set()).discard(ws)

    async def _poll_outbox_forever():
        """Read web_outbox.jsonl, push new messages to WebSocket clients and DB."""
        processed_ids: Set[str] = set()
        last_size: int = 0
        while True:
            await asyncio.sleep(0.5)
            try:
                if not WEB_OUTBOX.exists():
                    continue
                size = WEB_OUTBOX.stat().st_size
                if size == last_size:
                    continue
                last_size = size
                for line in WEB_OUTBOX.read_text(encoding="utf-8").splitlines():
                    try:
                        msg = json.loads(line)
                    except Exception:
                        continue
                    mid = msg.get("id")
                    if not mid or mid in processed_ids:
                        continue
                    processed_ids.add(mid)
                    cid   = str(msg.get("web_chat_id") or msg.get("chat_id") or "main")
                    role  = str(msg.get("role", "assistant"))
                    text  = str(msg.get("text", ""))
                    is_p  = bool(msg.get("is_progress", False))
                    ts    = float(msg.get("ts", time.time()))
                    # Persist to DB
                    add_message(cid, role, text, is_progress=is_p, msg_id=mid, ts=ts)
                    # Push to connected clients
                    await _broadcast(cid, {
                        "type": "message",
                        "id": mid, "chat_id": cid, "role": role,
                        "text": text, "ts": ts, "is_progress": is_p,
                    })
                # Keep set bounded
                if len(processed_ids) > 20000:
                    processed_ids = set(list(processed_ids)[-10000:])
            except Exception as exc:
                log.debug("Outbox poll error: %s", exc)

    def _enqueue_for_agent(chat_id: str, text: str) -> None:
        """Write user message to web_inbox.jsonl for the main agent."""
        try:
            WEB_INBOX.parent.mkdir(parents=True, exist_ok=True)
            # For "main" chat — use owner's numeric chat_id so agent treats it
            # identically to Telegram messages. For isolated chats — use a
            # synthetic negative ID derived from chat_id string.
            if chat_id == "main":
                numeric_id = WEB_MAIN_CHAT_ID
            else:
                # deterministic negative int per chat
                numeric_id = -(abs(hash(chat_id)) % 900000 + 100000)
            entry = {
                "id": uuid.uuid4().hex[:12],
                "type": "task",
                "chat_id": numeric_id,
                "web_chat_id": chat_id,
                "text": text,
                "_source": "web",
                "ts": time.time(),
            }
            with open(WEB_INBOX, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as exc:
            log.warning("Failed to write web_inbox: %s", exc)

    # ── Stats API ──────────────────────────────────────────────────────────
    @app.get("/api/stats")
    async def api_stats():
        state = _read_json(STATE_FILE) or {}
        events = _read_jsonl_tail(EVENTS_FILE, 3000)
        tool_counts: Dict[str, int] = {}
        costs_by_hour: Dict[str, float] = {}
        for ev in events:
            if ev.get("type") == "tool_call":
                name = str(ev.get("tool") or ev.get("name") or "unknown")
                tool_counts[name] = tool_counts.get(name, 0) + 1
            if ev.get("type") == "llm_usage":
                ts = str(ev.get("ts", ""))[:13]
                cost = float(ev.get("cost") or 0)
                costs_by_hour[ts] = costs_by_hour.get(ts, 0.0) + cost
        top_tools = sorted(tool_counts.items(), key=lambda x: x[1], reverse=True)[:15]
        timeline = sorted(costs_by_hour.items())[-48:]
        spent = float(state.get("spent_usd", 0))
        total_budget = float(state.get("budget_limit") or
                             float(os.environ.get("TOTAL_BUDGET", "120") or 120))
        session_snap = float(state.get("session_total_snapshot") or spent)
        return {
            "state": {
                "spent_usd": spent,
                "session_spent": max(0.0, spent - session_snap),
                "remaining_usd": max(0.0, total_budget - spent),
                "total_budget": total_budget,
                "spent_calls": state.get("spent_calls", 0),
                "spent_tokens_prompt": state.get("spent_tokens_prompt", 0),
                "spent_tokens_completion": state.get("spent_tokens_completion", 0),
                "spent_tokens_cached": state.get("spent_tokens_cached", 0),
                "version": state.get("version", "?"),
                "branch": state.get("current_branch", "?"),
                "session_id": state.get("session_id", "?"),
            },
            "top_tools": [{"name": k, "count": v} for k, v in top_tools],
            "cost_timeline": [{"hour": k, "cost": round(v, 6)} for k, v in timeline],
        }

    # ── Settings API ───────────────────────────────────────────────────────
    @app.get("/api/settings")
    async def api_settings_get():
        state = _read_json(STATE_FILE) or {}
        spent = float(state.get("spent_usd", 0))
        total = float(state.get("budget_limit") or
                      float(os.environ.get("TOTAL_BUDGET", "120") or 120))
        return {
            "spent_usd": spent,
            "total_usd": total,
            "remaining_usd": max(0.0, total - spent),
            "version": state.get("version", "?"),
            "branch": state.get("current_branch", "?"),
            "session_id": state.get("session_id", "?"),
        }

    class SettingsUpdate(BaseModel):
        total_usd: float

    @app.post("/api/settings")
    async def api_settings_post(body: SettingsUpdate):
        if not (0 < body.total_usd <= 10000):
            raise HTTPException(400, "Budget must be between 1 and 10000")
        state = _read_json(STATE_FILE) or {}
        state["budget_limit"] = body.total_usd
        _write_json(STATE_FILE, state)
        return {"ok": True, "total_usd": body.total_usd}

    # ── Models API ─────────────────────────────────────────────────────────
    _MODEL_PRESETS = [
        {"id": "anthropic/claude-opus-4-5",       "name": "Claude Opus 4.5",    "tier": "powerful"},
        {"id": "anthropic/claude-sonnet-4-5",      "name": "Claude Sonnet 4.5",  "tier": "balanced"},
        {"id": "anthropic/claude-haiku-3-5",       "name": "Claude Haiku 3.5",   "tier": "fast"},
        {"id": "openai/gpt-4o",                    "name": "GPT-4o",             "tier": "balanced"},
        {"id": "openai/gpt-4o-mini",               "name": "GPT-4o Mini",        "tier": "fast"},
        {"id": "google/gemini-2.5-pro-preview",    "name": "Gemini 2.5 Pro",     "tier": "powerful"},
        {"id": "google/gemini-2.0-flash-001",      "name": "Gemini 2.0 Flash",   "tier": "fast"},
        {"id": "meta-llama/llama-4-maverick",      "name": "Llama 4 Maverick",   "tier": "balanced"},
        {"id": "qwen/qwen3-235b-a22b",             "name": "Qwen3 235B",         "tier": "powerful"},
    ]

    @app.get("/api/models")
    async def api_models_get():
        state = _read_json(STATE_FILE) or {}
        return {
            "current": {
                "main":  os.environ.get("OUROBOROS_MODEL", ""),
                "light": os.environ.get("OUROBOROS_MODEL_LIGHT", ""),
                "code":  os.environ.get("OUROBOROS_MODEL_CODE", ""),
            },
            "saved": {
                "main":  state.get("model_main"),
                "light": state.get("model_light"),
                "code":  state.get("model_code"),
            },
            "presets": _MODEL_PRESETS,
        }

    class ModelUpdate(BaseModel):
        role: str
        model_id: str

    @app.post("/api/models")
    async def api_models_post(body: ModelUpdate):
        if not body.model_id or "/" not in body.model_id:
            raise HTTPException(400, "model_id must contain '/'")
        if body.role not in ("main", "light", "code"):
            raise HTTPException(400, "role must be main, light, or code")
        state = _read_json(STATE_FILE) or {}
        state[f"model_{body.role}"] = body.model_id
        _write_json(STATE_FILE, state)
        env_map = {
            "main":  "OUROBOROS_MODEL",
            "light": "OUROBOROS_MODEL_LIGHT",
            "code":  "OUROBOROS_MODEL_CODE",
        }
        env_key = env_map[body.role]
        env_path = Path("/home/ouroboros/.env")
        try:
            lines = env_path.read_text().splitlines() if env_path.exists() else []
            new_line = f"{env_key}={body.model_id}"
            updated = False
            for i, line in enumerate(lines):
                if line.startswith(f"{env_key}="):
                    lines[i] = new_line
                    updated = True
            if not updated:
                lines.append(new_line)
            env_path.write_text("\n".join(lines) + "\n")
        except Exception as exc:
            log.warning("Failed to update .env: %s", exc)
        return {"ok": True, "note": "Restart required to apply model change"}

    # ── Ollama status ──────────────────────────────────────────────────────
    @app.get("/api/ollama/status")
    async def api_ollama_status():
        import urllib.request
        def _get(path: str):
            with urllib.request.urlopen(f"http://127.0.0.1:11434{path}", timeout=3) as r:
                return json.loads(r.read())
        loop = asyncio.get_event_loop()
        try:
            v = await loop.run_in_executor(None, _get, "/api/version")
        except Exception as exc:
            return {"available": False, "error": str(exc), "models": [], "loaded": []}
        try:
            tags = await loop.run_in_executor(None, _get, "/api/tags")
            models = [m["name"] for m in (tags.get("models") or [])]
        except Exception:
            models = []
        try:
            ps = await loop.run_in_executor(None, _get, "/api/ps")
            loaded = [m["name"] for m in (ps.get("models") or [])]
        except Exception:
            loaded = []
        return {"available": True, "version": v.get("version"), "models": models, "loaded": loaded}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _read_json(path: Path) -> Optional[Dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(path: Path, data: Dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _read_jsonl_tail(path: Path, n: int) -> List[Dict]:
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        result = []
        for line in lines[-n:]:
            try:
                result.append(json.loads(line))
            except Exception:
                pass
        return result
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Public helper for agent to write to web_outbox
# ---------------------------------------------------------------------------
def write_web_outbox(
    web_chat_id: str,
    text: str,
    role: str = "assistant",
    is_progress: bool = False,
    msg_id: Optional[str] = None,
) -> None:
    """Called by the main agent process to push a message to web clients."""
    try:
        entry = {
            "id": msg_id or uuid.uuid4().hex[:16],
            "web_chat_id": web_chat_id,
            "role": role,
            "text": text,
            "ts": time.time(),
            "is_progress": is_progress,
        }
        with open(WEB_OUTBOX, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as exc:
        log.warning("write_web_outbox failed: %s", exc)
