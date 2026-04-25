"""Count messages in the buffer channel."""
import sys, asyncio, json

sys.path.insert(0, '/home/ouroboros/app/repo')


async def count():
    from pyrogram import Client
    from pyrogram.enums import MessageMediaType

    with open('/home/ouroboros/state/secrets/pyrogram.json') as f:
        secrets = json.load(f)

    async with Client(
        name="check_buf_session",
        api_id=secrets["api_id"],
        api_hash=secrets["api_hash"],
        session_string=secrets["session_string"],
        no_updates=True,
    ) as app:
        count = 0
        async for msg in app.get_chat_history(-1003519809178, limit=200):
            count += 1
        print(f"Buffer: {count} messages")


asyncio.run(count())
