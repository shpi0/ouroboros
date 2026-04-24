"""Ouroboros Web Runner — starts FastAPI server in background thread."""
from __future__ import annotations

import logging
import threading

log = logging.getLogger(__name__)

_started = False
_server_thread: threading.Thread = None


def start(host: str = "127.0.0.1", port: int = 8080) -> None:
    """Start the FastAPI web server in a daemon thread + patch send_with_budget."""
    global _started, _server_thread
    if _started:
        return
    _started = True

    def _run():
        try:
            import uvicorn
            from ouroboros.web.main import app
            if app is None:
                log.warning("FastAPI not available, web server not started")
                return
            uvicorn.run(app, host=host, port=port, log_level="warning", access_log=False)
        except Exception as e:
            log.error("Web server crashed: %s", e)

    _server_thread = threading.Thread(target=_run, name="web-server", daemon=True)
    _server_thread.start()
    log.info("Web server starting on http://%s:%d", host, port)
    _patch_send_with_budget()


def _patch_send_with_budget() -> None:
    """Monkey-patch send_with_budget so WEB_CHAT_ID messages go to web outbox."""
    try:
        import supervisor.telegram as tg_module
        from ouroboros.web.main import WEB_CHAT_ID, push_to_web

        _orig = getattr(tg_module, "send_with_budget", None)
        if _orig is None:
            return

        def _patched(chat_id, text, log_text=None, fmt="", is_progress=False):
            if int(chat_id) == WEB_CHAT_ID:
                push_to_web(str(text or ""), is_progress=bool(is_progress))
                return
            _orig(chat_id, text, log_text=log_text, fmt=fmt, is_progress=is_progress)

        tg_module.send_with_budget = _patched
        log.info("send_with_budget patched for WEB_CHAT_ID=%d", WEB_CHAT_ID)
    except Exception as e:
        log.warning("Failed to patch send_with_budget: %s", e)
