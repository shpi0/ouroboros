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
    add_message, get_messages,
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

WEB_MAIN_CHAT_ID = -999999

# ---------------------------------------------------------------------------
# WebSocket client registry
# ---------------------------------------------------------------------------
_ws_clients: Dict[str, Set[WebSocket]] = {}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _read_json(path: Path) -> Optional[Dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def _read_jsonl_tail(path: Path, n: int) -> List[Dict]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()[-n:]
        result = []
        for line in lines:
            try:
                result.append(json.loads(line))
            except Exception:
                pass
        return result
    except Exception:
        return []

def _read_proc_cpu() -> float:
    """Return CPU usage % (1-second sample)."""
    def _read_stat():
        line = open("/proc/stat").readline()
        vals = list(map(int, line.split()[1:8]))
        idle = vals[3]
        total = sum(vals)
        return idle, total
    try:
        i1, t1 = _read_stat()
        time.sleep(0.2)
        i2, t2 = _read_stat()
        di, dt = i2 - i1, t2 - t1
        return round((1 - di / dt) * 100, 1) if dt else 0.0
    except Exception:
        return 0.0

def _read_proc_mem() -> Dict:
    try:
        data = {}
        for line in open("/proc/meminfo"):
            k, v = line.split(":", 1)
            data[k.strip()] = int(v.strip().split()[0])
        total = data.get("MemTotal", 0)
        avail = data.get("MemAvailable", 0)
        used  = total - avail
        return {
            "total_mb": round(total / 1024, 1),
            "used_mb":  round(used  / 1024, 1),
            "free_mb":  round(avail / 1024, 1),
            "pct":      round(used / total * 100, 1) if total else 0,
        }
    except Exception:
        return {"total_mb": 0, "used_mb": 0, "free_mb": 0, "pct": 0}

def _read_disk() -> Dict:
    import shutil
    try:
        u = shutil.disk_usage("/")
        return {
            "total_gb": round(u.total / 1e9, 1),
            "used_gb":  round(u.used  / 1e9, 1),
            "free_gb":  round(u.free  / 1e9, 1),
            "pct":      round(u.used / u.total * 100, 1),
        }
    except Exception:
        return {"total_gb": 0, "used_gb": 0, "free_gb": 0, "pct": 0}

def _read_uptime() -> str:
    try:
        secs = float(open("/proc/uptime").read().split()[0])
        h = int(secs // 3600); m = int((secs % 3600) // 60)
        return f"{h}ч {m}м"
    except Exception:
        return "?"

def _read_load() -> List[float]:
    try:
        vals = open("/proc/loadavg").read().split()[:3]
        return [float(v) for v in vals]
    except Exception:
        return [0, 0, 0]

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

    # ── Static ────────────────────────────────────────────────────────────
    @app.get("/")
    async def _index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/health")
    async def _health():
        return {"ok": True}

    # ── Chats REST ────────────────────────────────────────────────────────
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

    # ── WebSocket ─────────────────────────────────────────────────────────
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
                msg = add_message(chat_id, "user", text)
                await _broadcast(chat_id, {"type": "message", **msg})
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
                    cid  = str(msg.get("web_chat_id") or msg.get("chat_id") or "main")
                    role = str(msg.get("role", "assistant"))
                    text = str(msg.get("text", ""))
                    is_p = bool(msg.get("is_progress", False))
                    ts   = float(msg.get("ts", time.time()))
                    add_message(cid, role, text, is_progress=is_p, msg_id=mid, ts=ts)
                    await _broadcast(cid, {
                        "type": "message",
                        "id": mid, "chat_id": cid, "role": role,
                        "text": text, "ts": ts, "is_progress": is_p,
                    })
                if len(processed_ids) > 20000:
                    processed_ids = set(list(processed_ids)[-10000:])
            except Exception as exc:
                log.debug("Outbox poll error: %s", exc)

    def _enqueue_for_agent(chat_id: str, text: str) -> None:
        try:
            WEB_INBOX.parent.mkdir(parents=True, exist_ok=True)
            numeric_id = WEB_MAIN_CHAT_ID if chat_id == "main" else -(abs(hash(chat_id)) % 900000 + 100000)
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

    # ── Stats API ─────────────────────────────────────────────────────────
    @app.get("/api/stats")
    async def api_stats():
        state = _read_json(STATE_FILE) or {}
        events = _read_jsonl_tail(EVENTS_FILE, 3000)

        tool_counts: Dict[str, int] = {}
        # costs by provider
        costs_openrouter: float = 0.0
        costs_openai: float = 0.0
        costs_ollama: float = 0.0
        tokens_by_provider: Dict[str, Dict] = {
            "openrouter": {"prompt": 0, "completion": 0, "cached": 0, "calls": 0},
            "openai":     {"prompt": 0, "completion": 0, "cached": 0, "calls": 0},
            "ollama":     {"prompt": 0, "completion": 0, "cached": 0, "calls": 0},
        }
        costs_by_hour: Dict[str, float] = {}

        for ev in events:
            if ev.get("type") == "tool_call":
                name = str(ev.get("tool") or ev.get("name") or "unknown")
                tool_counts[name] = tool_counts.get(name, 0) + 1
            if ev.get("type") == "llm_usage":
                ts_str = str(ev.get("ts", ""))[:13]
                cost = float(ev.get("cost") or 0)
                costs_by_hour[ts_str] = costs_by_hour.get(ts_str, 0.0) + cost
                # provider split
                model = str(ev.get("model") or "")
                provider = "ollama" if "ollama" in model or "127.0.0.1" in model else \
                           "openai" if model.startswith("openai/") or "whisper" in model else \
                           "openrouter"
                if provider == "openrouter": costs_openrouter += cost
                elif provider == "openai":   costs_openai += cost
                else:                        costs_ollama += cost
                bucket = tokens_by_provider.get(provider, tokens_by_provider["openrouter"])
                bucket["prompt"]     += int(ev.get("prompt_tokens") or 0)
                bucket["completion"] += int(ev.get("completion_tokens") or 0)
                bucket["cached"]     += int(ev.get("cached_tokens") or 0)
                bucket["calls"]      += 1

        top_tools = sorted(tool_counts.items(), key=lambda x: x[1], reverse=True)[:15]
        timeline  = sorted(costs_by_hour.items())[-48:]

        spent = float(state.get("spent_usd", 0))
        total_budget = float(state.get("budget_limit") or
                             float(os.environ.get("TOTAL_BUDGET", "120") or 120))
        session_snap = float(state.get("session_total_snapshot") or spent)

        return {
            "state": {
                "spent_usd":              spent,
                "session_spent":          max(0.0, spent - session_snap),
                "remaining_usd":          max(0.0, total_budget - spent),
                "total_budget":           total_budget,
                "spent_calls":            state.get("spent_calls", 0),
                "spent_tokens_prompt":    state.get("spent_tokens_prompt", 0),
                "spent_tokens_completion":state.get("spent_tokens_completion", 0),
                "spent_tokens_cached":    state.get("spent_tokens_cached", 0),
                "version":                state.get("version", "?"),
                "branch":                 state.get("current_branch", "?"),
                "session_id":             (state.get("session_id") or "?")[:8],
            },
            "by_provider": {
                "openrouter": {"cost": round(costs_openrouter, 4), **tokens_by_provider["openrouter"]},
                "openai":     {"cost": round(costs_openai, 4),     **tokens_by_provider["openai"]},
                "ollama":     {"cost": 0,                          **tokens_by_provider["ollama"]},
            },
            "top_tools":    [{"name": k, "count": v} for k, v in top_tools],
            "cost_timeline":[{"hour": k, "cost": round(v, 6)} for k, v in timeline],
        }

    # ── System stats API ──────────────────────────────────────────────────
    @app.get("/api/system")
    async def api_system():
        loop = asyncio.get_event_loop()
        cpu_pct = await loop.run_in_executor(None, _read_proc_cpu)
        mem  = _read_proc_mem()
        disk = _read_disk()
        load = _read_load()
        uptime = _read_uptime()

        # Network (bytes since boot)
        net = {"rx_mb": 0, "tx_mb": 0}
        try:
            for line in open("/proc/net/dev"):
                if ":" in line:
                    parts = line.split()
                    iface = parts[0].rstrip(":")
                    if iface in ("lo",):
                        continue
                    net["rx_mb"] = round(net["rx_mb"] + int(parts[1]) / 1e6, 1)
                    net["tx_mb"] = round(net["tx_mb"] + int(parts[9]) / 1e6, 1)
        except Exception:
            pass

        return {
            "cpu_pct": cpu_pct,
            "load":    load,
            "uptime":  uptime,
            "mem":     mem,
            "disk":    disk,
            "net":     net,
        }

    # ── Settings API ──────────────────────────────────────────────────────
    @app.get("/api/settings")
    async def api_settings_get():
        state = _read_json(STATE_FILE) or {}
        spent = float(state.get("spent_usd", 0))
        total = float(state.get("budget_limit") or
                      float(os.environ.get("TOTAL_BUDGET", "120") or 120))
        env_path = Path("/home/ouroboros/.env")
        env_vars: Dict[str, str] = {}
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, _, v = line.partition("=")
                    env_vars[k.strip()] = v.strip()
        return {
            "total_usd":      total,
            "spent_usd":      spent,
            "remaining_usd":  max(0.0, total - spent),
            "openai_budget":  env_vars.get("OPENAI_BUDGET", ""),
            "claude_budget":  env_vars.get("CLAUDE_CODE_BUDGET", ""),
            "version":        state.get("version", "?"),
            "branch":         state.get("current_branch", "?"),
        }

    class SettingsUpdate(BaseModel):
        total_usd:     Optional[float] = None
        openai_budget: Optional[float] = None
        claude_budget: Optional[float] = None

    @app.post("/api/settings")
    async def api_settings_post(body: SettingsUpdate):
        state = _read_json(STATE_FILE) or {}
        env_path = Path("/home/ouroboros/.env")
        lines = env_path.read_text().splitlines() if env_path.exists() else []

        def _set_env(key: str, val: str):
            nonlocal lines
            new_line = f"{key}={val}"
            for i, line in enumerate(lines):
                if line.startswith(f"{key}="):
                    lines[i] = new_line
                    return
            lines.append(new_line)

        if body.total_usd is not None:
            if not (0 < body.total_usd <= 10000):
                raise HTTPException(400, "Budget must be 1–10000")
            state["budget_limit"] = body.total_usd
            _set_env("TOTAL_BUDGET", str(body.total_usd))
        if body.openai_budget is not None:
            _set_env("OPENAI_BUDGET", str(body.openai_budget))
        if body.claude_budget is not None:
            _set_env("CLAUDE_CODE_BUDGET", str(body.claude_budget))

        _write_json(STATE_FILE, state)
        try:
            env_path.write_text("\n".join(lines) + "\n")
        except Exception as exc:
            log.warning("Failed to update .env: %s", exc)
        return {"ok": True}

    # ── Models API ────────────────────────────────────────────────────────
    _MODEL_PRESETS = [
        {"id": "anthropic/claude-opus-4-5",      "name": "Claude Opus 4.5",   "tier": "powerful"},
        {"id": "anthropic/claude-sonnet-4-5",     "name": "Claude Sonnet 4.5", "tier": "balanced"},
        {"id": "anthropic/claude-haiku-3-5",      "name": "Claude Haiku 3.5",  "tier": "fast"},
        {"id": "openai/gpt-4o",                   "name": "GPT-4o",            "tier": "balanced"},
        {"id": "openai/gpt-4o-mini",              "name": "GPT-4o Mini",       "tier": "fast"},
        {"id": "google/gemini-2.5-pro-preview",   "name": "Gemini 2.5 Pro",    "tier": "powerful"},
        {"id": "google/gemini-2.0-flash-001",     "name": "Gemini 2.0 Flash",  "tier": "fast"},
        {"id": "meta-llama/llama-4-maverick",     "name": "Llama 4 Maverick",  "tier": "balanced"},
        {"id": "qwen/qwen3-235b-a22b",            "name": "Qwen3 235B",        "tier": "powerful"},
        {"id": "ollama/qwen2.5:35b",              "name": "Ollama qwen2.5:35b","tier": "local"},
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
        if body.role not in ("main", "light", "code"):
            raise HTTPException(400, "role must be main, light, or code")
        state = _read_json(STATE_FILE) or {}
        state[f"model_{body.role}"] = body.model_id
        _write_json(STATE_FILE, state)
        env_map = {"main": "OUROBOROS_MODEL", "light": "OUROBOROS_MODEL_LIGHT", "code": "OUROBOROS_MODEL_CODE"}
        env_key  = env_map[body.role]
        env_path = Path("/home/ouroboros/.env")
        try:
            lines = env_path.read_text().splitlines() if env_path.exists() else []
            new_line = f"{env_key}={body.model_id}"
            updated = False
            for i, line in enumerate(lines):
                if line.startswith(f"{env_key}="):
                    lines[i] = new_line; updated = True
            if not updated:
                lines.append(new_line)
            env_path.write_text("\n".join(lines) + "\n")
        except Exception as exc:
            log.warning("Failed to update .env: %s", exc)
        return {"ok": True, "note": "Restart required to apply"}

    # ── Ollama status ─────────────────────────────────────────────────────
    @app.get("/api/ollama/status")
    async def api_ollama_status():
        import urllib.request
        def _get(path: str):
            with urllib.request.urlopen(f"http://127.0.0.1:11434{path}", timeout=3) as r:
                return json.loads(r.read())
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, _get, "/api/version")
        except Exception as exc:
            return {"available": False, "error": str(exc), "models": [], "loaded": []}
        try:
            tags = await loop.run_in_executor(None, _get, "/api/tags")
            models = [m["name"] for m in tags.get("models", [])]
        except Exception:
            models = []
        try:
            ps = await loop.run_in_executor(None, _get, "/api/ps")
            loaded = []
            for m in ps.get("models", []):
                loaded.append({
                    "name":    m.get("name"),
                    "size_gb": round(m.get("size", 0) / 1e9, 1),
                    "vram_gb": round(m.get("size_vram", 0) / 1e9, 1),
                    "expires": m.get("expires_at", ""),
                })
        except Exception:
            loaded = []
        return {"available": True, "models": models, "loaded": loaded}

    # ── Utils ─────────────────────────────────────────────────────────────
    # (helpers defined at module level above)
