#!/usr/bin/env python3
"""Standalone script to fill the buffer channel. Runs as a subprocess, independent of the agent."""
import asyncio
import datetime
import json
import logging
import os
import sys

# Add repo to path
sys.path.insert(0, "/home/ouroboros/app/repo")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

def main():
    total_posts = int(sys.argv[1]) if len(sys.argv) > 1 else 70
    hours_back = int(sys.argv[2]) if len(sys.argv) > 2 else 168
    target_chat_id = int(sys.argv[3]) if len(sys.argv) > 3 else -1003519809178
    progress_file = sys.argv[4] if len(sys.argv) > 4 else "/tmp/init_buffer_progress.json"

    from ouroboros.tools.channel_buffer import (
        DONOR_CHANNELS, _write_init_progress, _async_clear_buffer,
        _async_forward_posts, _warmup_ollama,
    )

    async def run():
        started_at = datetime.datetime.utcnow().isoformat()
        _write_init_progress(0, total_posts, "", started_at, "running")
        try:
            await _async_clear_buffer(target_chat_id)
            log.info("Buffer cleared, warming up Ollama...")
            await _warmup_ollama()
            log.info("Ollama warmed, starting forwarding...")
            result = await _async_forward_posts(
                donors=DONOR_CHANNELS,
                target_chat_id=target_chat_id,
                limit_per_donor=max(30, total_posts // len(DONOR_CHANNELS) + 5),
                total_limit=total_posts,
                only_relevant=True,
                hours_back=hours_back,
                progress_file=progress_file,
            )
            sent = result.get("forwarded_count", 0)
            last_post = result["forwarded"][-1]["text_preview"] if result.get("forwarded") else ""
            _write_init_progress(sent, total_posts, last_post, started_at, "done")
            log.info(f"init_buffer done: {sent}/{total_posts} posts sent")
        except Exception as e:
            log.error(f"init_buffer error: {e!r}", exc_info=True)
            _write_init_progress(0, total_posts, str(e)[:200], started_at, "error")

    asyncio.run(run())

if __name__ == "__main__":
    main()
