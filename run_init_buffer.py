#!/usr/bin/env python3
"""One-shot runner: clear buffer and fill with 70 posts."""
import sys, asyncio, logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
sys.path.insert(0, '/home/ouroboros/app/repo')

from ouroboros.tools.channel_buffer import (
    _async_clear_buffer, _async_forward_posts,
    DONOR_CHANNELS, BUFFER_CHANNEL_ID
)

async def main():
    print("=== Clearing buffer... ===")
    r = await _async_clear_buffer(BUFFER_CHANNEL_ID)
    print(f"Cleared: {r}")

    print("=== Forwarding 70 posts (up to 7 days back)... ===")
    r = await _async_forward_posts(
        donors=DONOR_CHANNELS,
        target_chat_id=BUFFER_CHANNEL_ID,
        limit_per_donor=25,
        total_limit=70,
        only_relevant=True,
        hours_back=168,
    )
    print(f"=== Done: {r} ===")

asyncio.run(main())
