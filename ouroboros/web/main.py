"""Ouroboros Web Interface — FastAPI backend."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
    from fastapi.responses import FileResponse
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel
except ImportError:
    FastAPI = None

log = logging.getLogger(__name__)

WEB_CHAT_ID = -999999

_WEB_OUTBOX: Optional[asyncio.Queue] = None
_ws_clients: List[WebSocket] = []

DRIVE_ROOT = Path(os.environ.get("OUROBOROS_STATE_DIR", "/home/ouroboros/state"))
STATE_FILE = DRIVE_ROOT / "state" / "state.json"
EVENTS_FILE = DRIVE_ROOT / "logs" / "events.jsonl"
STATIC_DIR = Path(__file__).parent / "static"

if FastAPI:
    app = FastAPI(title="Ouroboros", docs_url=None, redoc_url=None)
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
else:
    app = None


def get_outbox() -> asyncio.Queue:
    global _WEB_OUTBOX
    if _WEB_OUTBOX is None:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        _WEB_OUTBOX = asyncio.Queue(maxsize=500)
    return _WEB_OUTBOX


def push_to_web(text: str, is_progress: bool = False) -> None:
    """Called from sync context to push agent response to web clients."""
    msg = {"ts": time.time(), "text": text, "is_progress": is_progress}
    try:
        get_outbox().put_nowait(msg)
    except Exception:
        pass


if app:
    @app.get("/")
    async def chat_page():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/stats")
    async def stats_page():
        return FileResponse(STATIC_DIR / "stats.html")

    @app.get("/settings")
    async def settings_page():
        return FileResponse(STATIC_DIR / "settings.html")

    @app.websocket("/ws/chat")
    async def websocket_chat(websocket: WebSocket):
        await websocket.accept()
        _ws_clients.append(websocket)
        outbox = get_outbox()

        async def forward_outbox():
            while True:
                try:
                    msg = await asyncio.wait_for(outbox.get(), timeout=0.5)
                    dead = []
                    for ws in list(_ws_clients):
                        try:
                            await ws.send_json({"role": "assistant", **msg})
                        except Exception:
                            dead.append(ws)
                    for ws in dead:
                        if ws in _ws_clients:
                            _ws_clients.remove(ws)
                except asyncio.TimeoutError:
                    pass
                except Exception:
                    await asyncio.sleep(0.1)

        fwd = asyncio.create_task(forward_outbox())
        try:
            while True:
                raw = await websocket.receive_text()
                payload = json.loads(raw)
                text = str(payload.get("text", "")).strip()
                if not text:
                    continue
                for ws in list(_ws_clients):
                    try:
                        await ws.send_json({"role": "user", "text": text, "ts": time.time()})
                    except Exception:
                        pass
                loop = asyncio.get_event_loop()
                loop.run_in_executor(None, _route_to_agent, text)
        except WebSocketDisconnect:
            pass
        finally:
            fwd.cancel()
            if websocket in _ws_clients:
                _ws_clients.remove(websocket)

    def _route_to_agent(text: str) -> None:
        try:
            import uuid
            from supervisor.queue import enqueue_task
            task = {
                "id": uuid.uuid4().hex[:8],
                "type": "task",
                "chat_id": WEB_CHAT_ID,
                "text": text,
                "_source": "web",
            }
            enqueue_task(task)
        except Exception as e:
            log.warning("Failed to route web message to agent: %s", e)
            try:
                push_to_web(f"\u26a0\ufe0f Agent error: {e}")
            except Exception:
                pass

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
        total_budget = float(state.get("budget_limit") or 50)
        session_snap = float(state.get("session_total_snapshot", spent))
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

    @app.get("/api/state")
    async def api_state():
        state = _read_json(STATE_FILE) or {}
        return {
            "spent_usd": state.get("spent_usd", 0),
            "budget_limit": float(state.get("budget_limit") or 50),
            "version": state.get("version", "?"),
            "branch": state.get("current_branch", "?"),
            "session_id": state.get("session_id", "?"),
        }

    @app.get("/api/settings")
    async def api_settings_get():
        state = _read_json(STATE_FILE) or {}
        return {
            "spent_usd": float(state.get("spent_usd", 0)),
            "total_usd": float(state.get("budget_limit") or 50),
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

    class BudgetUpdate(BaseModel):
        budget_limit: float

    @app.post("/api/settings/budget")
    async def update_budget(body: BudgetUpdate):
        if not (0 < body.budget_limit <= 1000):
            raise HTTPException(400, "Budget must be between 1 and 1000")
        state = _read_json(STATE_FILE) or {}
        state["budget_limit"] = body.budget_limit
        _write_json(STATE_FILE, state)
        return {"ok": True, "budget_limit": body.budget_limit}

    @app.post("/api/restart")
    async def trigger_restart():
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, _route_to_agent, "/restart")
            return {"ok": True, "message": "Restart triggered"}
        except Exception as e:
            raise HTTPException(500, str(e))

    @app.get("/api/ollama/status")
    async def api_ollama_status():
        import urllib.request
        import urllib.error

        def _ollama_get(path: str):
            with urllib.request.urlopen(f"http://127.0.0.1:11434{path}", timeout=3) as resp:
                return json.loads(resp.read().decode())

        loop = asyncio.get_event_loop()
        try:
            version_data = await loop.run_in_executor(None, _ollama_get, "/api/version")
            version = version_data.get("version", "?")
        except Exception as e:
            return {"available": False, "version": None, "models": [], "loaded": [], "error": str(e)}

        try:
            tags_data = await loop.run_in_executor(None, _ollama_get, "/api/tags")
            models = [m["name"] for m in (tags_data.get("models") or [])]
        except Exception:
            models = []

        try:
            ps_data = await loop.run_in_executor(None, _ollama_get, "/api/ps")
            loaded = [m["name"] for m in (ps_data.get("models") or [])]
        except Exception:
            loaded = []

        return {"available": True, "version": version, "models": models, "loaded": loaded, "error": None}

    _MODEL_PRESETS = [
        {"id": "anthropic/claude-opus-4-5", "name": "Claude Opus 4.5", "tier": "powerful"},
        {"id": "anthropic/claude-sonnet-4-5", "name": "Claude Sonnet 4.5", "tier": "balanced"},
        {"id": "anthropic/claude-haiku-3-5", "name": "Claude Haiku 3.5", "tier": "fast"},
        {"id": "openai/gpt-4o", "name": "GPT-4o", "tier": "balanced"},
        {"id": "openai/gpt-4o-mini", "name": "GPT-4o Mini", "tier": "fast"},
        {"id": "google/gemini-2.5-pro-preview", "name": "Gemini 2.5 Pro", "tier": "powerful"},
        {"id": "google/gemini-2.0-flash-001", "name": "Gemini 2.0 Flash", "tier": "fast"},
        {"id": "google/gemini-flash-1.5", "name": "Gemini Flash 1.5", "tier": "fast"},
        {"id": "meta-llama/llama-4-maverick", "name": "Llama 4 Maverick", "tier": "balanced"},
        {"id": "qwen/qwen3-235b-a22b", "name": "Qwen3 235B", "tier": "powerful"},
    ]

    @app.get("/api/models")
    async def api_models_get():
        state = _read_json(STATE_FILE) or {}
        return {
            "current": {
                "main": os.environ.get("OUROBOROS_MODEL", ""),
                "light": os.environ.get("OUROBOROS_MODEL_LIGHT", ""),
                "code": os.environ.get("OUROBOROS_MODEL_CODE", ""),
            },
            "saved": {
                "main": state.get("model_main"),
                "light": state.get("model_light"),
                "code": state.get("model_code"),
            },
            "presets": _MODEL_PRESETS,
        }

    class ModelUpdate(BaseModel):
        role: str
        model_id: str

    @app.post("/api/models")
    async def api_models_post(body: ModelUpdate):
        if not body.model_id or "/" not in body.model_id:
            raise HTTPException(400, "model_id must be non-empty and contain '/'")
        if body.role not in ("main", "light", "code"):
            raise HTTPException(400, "role must be main, light, or code")

        state = _read_json(STATE_FILE) or {}
        state[f"model_{body.role}"] = body.model_id
        _write_json(STATE_FILE, state)

        env_key_map = {
            "main": "OUROBOROS_MODEL",
            "light": "OUROBOROS_MODEL_LIGHT",
            "code": "OUROBOROS_MODEL_CODE",
        }
        env_key = env_key_map[body.role]
        env_path = Path("/home/ouroboros/.env")
        try:
            if env_path.exists():
                lines = env_path.read_text().splitlines()
                new_line = f"{env_key}={body.model_id}"
                replaced = False
                for i, line in enumerate(lines):
                    if line.startswith(f"{env_key}=") or line.startswith(f"export {env_key}="):
                        lines[i] = new_line
                        replaced = True
                        break
                if not replaced:
                    lines.append(new_line)
                env_path.write_text("\n".join(lines) + "\n")
            else:
                env_path.write_text(f"{env_key}={body.model_id}\n")
        except Exception as exc:
            log.warning("Failed to update .env: %s", exc)

        return {"ok": True, "note": "Restart required to apply"}


def _read_json(path: Path) -> Optional[Dict]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _write_json(path: Path, data: Dict) -> None:
    import tempfile
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
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
