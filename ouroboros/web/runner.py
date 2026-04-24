"""
Ouroboros Web Interface — runner.

Starts the FastAPI server in a daemon thread and polls web_inbox
for incoming messages, routing them through handle_chat_direct.
Responses from the agent are picked up from events and written to web_outbox.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import threading
import time
import uuid
from typing import Any, Callable, Dict, Optional

log = logging.getLogger(__name__)

STATE_ROOT = pathlib.Path(os.environ.get("OUROBOROS_STATE_ROOT", "/home/ouroboros/state"))
WEB_INBOX = STATE_ROOT / "web_inbox"
WEB_OUTBOX = STATE_ROOT / "web_outbox"
WEB_HOST = "127.0.0.1"
WEB_PORT = 8080

_started = False
_lock = threading.Lock()

# Callback set by supervisor after workers init
_handle_chat_fn: Optional[Callable] = None
_send_progress_fn: Optional[Callable] = None


def set_chat_handler(fn: Callable) -> None:
    """Register function to handle incoming web chat messages."""
    global _handle_chat_fn
    _handle_chat_fn = fn


def set_progress_handler(fn: Callable) -> None:
    """Register function to receive progress messages from agent."""
    global _send_progress_fn
    _send_progress_fn = fn


# ---------------------------------------------------------------------------
# Web sender — called by supervisor event loop to push responses to outbox
# ---------------------------------------------------------------------------

def send_web_response(ws_id: str, text: str, msg_type: str = "bot") -> None:
    """Write a response to web_outbox so the WebSocket relay picks it up."""
    try:
        WEB_OUTBOX.mkdir(parents=True, exist_ok=True)
        out_file = WEB_OUTBOX / f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}.json"
        out_file.write_text(json.dumps({
            "type": msg_type,
            "ws_id": ws_id,
            "text": text,
        }, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        log.error("send_web_response error: %s", e)


def send_web_progress(ws_id: str, text: str) -> None:
    send_web_response(ws_id, text, msg_type="progress")


# ---------------------------------------------------------------------------
# Inbox poller — runs in daemon thread
# ---------------------------------------------------------------------------

def _inbox_poller_loop() -> None:
    """Poll web_inbox for new messages and route to agent."""
    WEB_INBOX.mkdir(parents=True, exist_ok=True)
    log.info("[web_inbox] poller started")
    while True:
        try:
            for f in sorted(WEB_INBOX.glob("*.json")):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                    f.unlink(missing_ok=True)
                except Exception as e:
                    log.warning("inbox read error %s: %s", f.name, e)
                    try:
                        f.unlink(missing_ok=True)
                    except Exception:
                        pass
                    continue

                text = (data.get("text") or "").strip()
                ws_id = data.get("ws_id") or ""
                if not text:
                    continue

                log.info("[web_inbox] message from ws=%s: %s...", ws_id, text[:60])

                if _handle_chat_fn is None:
                    log.warning("[web_inbox] no chat handler registered, dropping message")
                    send_web_response(ws_id, "⚠️ Agent not ready yet", "error")
                    continue

                # Route through agent (handle_chat_direct is synchronous, runs in same thread)
                # Use a separate thread to avoid blocking the poller
                def _dispatch(t=text, w=ws_id):
                    try:
                        # WEB_CHAT_ID is a special sentinel: supervisor recognizes it
                        # and routes responses back through web_outbox instead of Telegram
                        _handle_chat_fn(chat_id=_WEB_CHAT_ID, text=t, _web_ws_id=w)
                    except Exception as exc:
                        log.error("[web_inbox] dispatch error: %s", exc, exc_info=True)
                        send_web_response(w, f"⚠️ Error: {exc}", "error")

                threading.Thread(target=_dispatch, daemon=True).start()

        except Exception as e:
            log.error("[web_inbox] poller error: %s", e)
        time.sleep(0.4)


# Sentinel chat_id that supervisor recognizes as "web" origin
_WEB_CHAT_ID = -99999


# ---------------------------------------------------------------------------
# FastAPI server starter
# ---------------------------------------------------------------------------

def _start_server() -> None:
    try:
        import uvicorn
        from ouroboros.web.main import app
        uvicorn.run(
            app,
            host=WEB_HOST,
            port=WEB_PORT,
            log_level="warning",
            access_log=False,
        )
    except Exception as e:
        log.error("[web] server crashed: %s", e, exc_info=True)


def start(handle_chat_fn: Callable) -> None:
    """Start web server + inbox poller. Call once from supervisor at boot."""
    global _started
    with _lock:
        if _started:
            return
        _started = True

    set_chat_handler(handle_chat_fn)

    # Install fastapi + uvicorn if missing
    try:
        import fastapi  # noqa: F401
        import uvicorn  # noqa: F401
    except ImportError:
        import subprocess
        import sys
        log.info("[web] installing fastapi + uvicorn…")
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "fastapi", "uvicorn[standard]"],
            check=True,
        )

    WEB_INBOX.mkdir(parents=True, exist_ok=True)
    WEB_OUTBOX.mkdir(parents=True, exist_ok=True)

    # Start FastAPI server
    t_server = threading.Thread(target=_start_server, daemon=True, name="web-server")
    t_server.start()

    # Start inbox poller
    t_poller = threading.Thread(target=_inbox_poller_loop, daemon=True, name="web-inbox-poller")
    t_poller.start()

    log.info("[web] started: http://%s:%d", WEB_HOST, WEB_PORT)
