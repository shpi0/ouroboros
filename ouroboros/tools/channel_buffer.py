"""
Channel buffer tools: send_to_channel, fetch_channel_posts.

Used for @UranWar content pipeline:
- fetch_channel_posts: scrape public Telegram channel web preview
- send_to_channel: post content to buffer channel via Telegram Bot API
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List

from ouroboros.tools.registry import ToolContext, ToolEntry
from ouroboros.utils import utc_now_iso

log = logging.getLogger(__name__)

# Common Russian profanity patterns to clean
_PROFANITY_RE = re.compile(
    r'\b(бля|блять|блядь|хуй|хуя|хуе|хуи|пизд\w*|еба\w*|ёба\w*|ёб\w*|'
    r'еб\w*|пидор\w*|сука|ёбан\w*|ёбн\w*|хуев\w*|нахуй|нахер|'
    r'залуп\w*|мудак\w*|мудил\w*|ёпт|епт|ёпта|епта|пиздец|пиздит\w*|'
    r'пиздёж|заеб\w*|заёб\w*|охуе\w*|охуй\w*|охуел|похуй\w*)\b',
    re.IGNORECASE | re.UNICODE,
)


def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = _PROFANITY_RE.sub("***", text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _fetch_channel_posts(ctx: ToolContext, username: str, limit: int = 30) -> str:
    """Fetch recent posts from a public Telegram channel web preview."""
    import requests

    username = username.lstrip("@")
    url = f"https://t.me/s/{username}"

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "ru-RU,ru;q=0.9",
        }
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        return json.dumps({"error": f"Failed to fetch {url}: {repr(e)}"}, ensure_ascii=False)

    media_pattern = re.compile(
        r'class="[^"]*tgme_widget_message_(?:photo|video|document|sticker)[^"]*"',
    )

    posts = []
    blocks = re.split(r'<div[^>]+class="[^"]*tgme_widget_message_wrap[^"]*"', html)

    for block in blocks[1:]:
        id_m = re.search(r'data-post="([^/]+/(\d+))"', block)
        if not id_m:
            continue
        post_path = id_m.group(1)
        post_num = id_m.group(2)

        text_m = re.search(
            r'class="[^"]*tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
            block, re.DOTALL,
        )
        raw_text = ""
        if text_m:
            raw_text = text_m.group(1)
            raw_text = re.sub(r'<br\s*/?>', '\n', raw_text)
            raw_text = re.sub(r'<[^>]+>', '', raw_text)
            raw_text = (raw_text
                        .replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
                        .replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' '))

        date_m = re.search(r'<time[^>]+datetime="([^"]+)"', block)
        date_str = date_m.group(1) if date_m else ""

        has_media = bool(media_pattern.search(block))

        cleaned = _clean_text(raw_text)

        if len(cleaned) < 30 and not has_media:
            continue

        posts.append({
            "id": post_num,
            "text": cleaned[:1500],
            "date": date_str,
            "has_media": has_media,
            "url": f"https://t.me/{post_path}",
            "source": username,
        })

        if len(posts) >= limit:
            break

    posts.reverse()

    return json.dumps({
        "channel": username,
        "posts_found": len(posts),
        "posts": posts[:limit],
    }, ensure_ascii=False, indent=2)


def _send_to_channel(ctx: ToolContext, chat_id: int, text: str, source_url: str = "") -> str:
    """Send a text message to a Telegram channel (e.g., buffer channel)."""
    if not text or not text.strip():
        return "⚠️ Empty text — nothing to send."

    msg = text.strip()
    if source_url:
        msg = f"{msg}\n\n🔗 {source_url}"

    ctx.pending_events.append({
        "type": "send_to_channel",
        "chat_id": int(chat_id),
        "text": msg,
        "ts": utc_now_iso(),
    })

    preview = msg[:80].replace('\n', ' ')
    return f"OK: queued message to channel {chat_id}: {preview}..."


def get_tools() -> List[ToolEntry]:
    return [
        ToolEntry("fetch_channel_posts", {
            "name": "fetch_channel_posts",
            "description": (
                "Fetch recent posts from a public Telegram channel by scraping t.me/s/{username}. "
                "Returns JSON with posts: [{id, text, date, has_media, url, source}]. "
                "Text is cleaned of profanity. Posts shorter than 30 chars are filtered out."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "username": {
                        "type": "string",
                        "description": "Telegram channel username (with or without @)",
                    },
                    "limit": {
                        "type": "integer",
                        "default": 30,
                        "description": "Max number of posts to return (default: 30)",
                    },
                },
                "required": ["username"],
            },
        }, _fetch_channel_posts),
        ToolEntry("send_to_channel", {
            "name": "send_to_channel",
            "description": (
                "Send a text message to a Telegram channel or chat by chat_id. "
                "Use for posting content to the buffer channel (-1003519809178). "
                "Optionally include source_url which will be appended to the message."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "chat_id": {
                        "type": "integer",
                        "description": "Telegram chat_id (e.g., -1003519809178 for buffer channel)",
                    },
                    "text": {
                        "type": "string",
                        "description": "Message text to send",
                    },
                    "source_url": {
                        "type": "string",
                        "description": "Optional source URL to append to the message",
                    },
                },
                "required": ["chat_id", "text"],
            },
        }, _send_to_channel),
    ]
