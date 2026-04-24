"""
Web interface runner. Called from supervisor at startup.
Starts FastAPI server on localhost:8080 in a background thread.
"""

import logging
import threading

log = logging.getLogger(__name__)

_started = False
_lock = threading.Lock()


def start_web_server(host: str = "127.0.0.1", port: int = 8080) -> None:
    """Start FastAPI in a daemon thread. Safe to call multiple times (idempotent)."""
    global _started
    with _lock:
        if _started:
            return
        _started = True

    def _run():
        try:
            import uvicorn
            from ouroboros.web.main import app
            log.info("Starting web server on %s:%s", host, port)
            uvicorn.run(app, host=host, port=port, log_level="warning", access_log=False)
        except Exception as e:
            log.error("Web server error: %s", e)

    t = threading.Thread(target=_run, daemon=True, name="ouroboros-web")
    t.start()
    log.info("Web server thread started")


def start_inbox_poller(queue_put_fn) -> None:
    """
    Poll web_inbox directory for new messages from WebSocket clients.
    When found — inject into agent queue the same way Telegram messages are.

    queue_put_fn: callable that accepts (text: str, metadata: dict)
    """
    import json
    import pathlib
    import os
    import time

    inbox = pathlib.Path(os.environ.get("OUROBOROS_STATE_ROOT", "/home/ouroboros/state")) / "web_inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    outbox = pathlib.Path(os.environ.get("OUROBOROS_STATE_ROOT", "/home/ouroboros/state")) / "web_outbox"
    outbox.mkdir(parents=True, exist_ok=True)

    def _poll():
        while True:
            try:
                for f in sorted(inbox.glob("*.json")):
                    try:
                        data = json.loads(f.read_text(encoding="utf-8"))
                        f.unlink(missing_ok=True)
                        text = data.get("text", "").strip()
                        if text:
                            queue_put_fn(text, {
                                "source": "web",
                                "ws_id": data.get("ws_id"),
                                "msg_id": data.get("id"),
                            })
                    except Exception as e:
                        log.warning("web inbox error: %s", e)
                        try:
                            f.unlink(missing_ok=True)
                        except Exception:
                            pass
            except Exception as e:
                log.warning("web inbox poll error: %s", e)
            time.sleep(0.5)

    t = threading.Thread(target=_poll, daemon=True, name="ouroboros-web-inbox")
    t.start()


def send_to_websocket(ws_id: str, message: dict) -> None:
    """Write a response message to web_outbox for a specific ws_id."""
    import json
    import pathlib
    import os
    import uuid

    outbox = pathlib.Path(os.environ.get("OUROBOROS_STATE_ROOT", "/home/ouroboros/state")) / "web_outbox"
    outbox.mkdir(parents=True, exist_ok=True)
    fname = outbox / f"{ws_id}_{uuid.uuid4().hex[:8]}.json"
    fname.write_text(json.dumps(message, ensure_ascii=False), encoding="utf-8")
