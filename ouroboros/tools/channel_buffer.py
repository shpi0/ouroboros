"""
Channel buffer tools: forward_posts_to_buffer, fetch_channel_posts.

Content pipeline for @UranWar:
- forward_posts_to_buffer: use Pyrogram to forward real posts (with media) from donors
- fetch_channel_posts: scrape public Telegram channel web preview (fallback / read-only)

Pyrogram credentials are stored in Drive at secrets/pyrogram.json:
  {"api_id": ..., "api_hash": "...", "session_string": "..."}

Buffer channel: -1003519809178
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

from ouroboros.tools.registry import ToolContext, ToolEntry

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

BUFFER_CHANNEL_ID = -1003519809178
CONFIG_PATH_DRIVE = "config/uran_buffer.json"
SECRETS_PATH_DRIVE = "secrets/pyrogram.json"

DONOR_CHANNELS = [
    "warhistoryalconafter",
    "Vspomni_o_Voine_neraz",
    "zloy_zhurnalist",
    "roscosmos_gk",
]

# Topics to select
TOPIC_KEYWORDS = [
    "боевые", "обстрел", "наступлен", "оборон", "фронт", "операци",
    "космос", "роскосмос", "уран", "батальон",
    "день победы", "9 мая", "23 февраля", "праздник", "годовщин",
    "достижен", "рекорд", "открыт", "запуск", "орбит",
    "гуманитар", "помощь", "сбор", "добровол",
    "история", "великая отечественная", "вов",
]

# Profanity patterns
_PROFANITY_RE = re.compile(
    r'\b(бля|блять|блядь|хуй|хуя|хуе|хуи|пизд\w*|еба\w*|ёба\w*|ёб\w*|'
    r'еб\w*|пидор\w*|сука|ёбан\w*|ёбн\w*|хуев\w*|нахуй|нахер|'
    r'залуп\w*|мудак\w*|мудил\w*|ёпт|епт|ёпта|епта|пиздец|пиздит\w*|'
    r'пиздёж|заеб\w*|заёб\w*|охуе\w*|охуй\w*|охуел|похуй\w*)\b',
    re.IGNORECASE | re.UNICODE,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_drive_root() -> str:
    return os.environ.get("DRIVE_ROOT", "/home/ouroboros/state")


def _load_pyrogram_secrets() -> Dict[str, Any]:
    path = os.path.join(_get_drive_root(), SECRETS_PATH_DRIVE)
    with open(path) as f:
        return json.load(f)


def _load_config() -> Dict[str, Any]:
    path = os.path.join(_get_drive_root(), CONFIG_PATH_DRIVE)
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _is_relevant(text: str) -> bool:
    """Check if post text matches topic keywords."""
    if not text:
        return False
    low = text.lower()
    return any(kw in low for kw in TOPIC_KEYWORDS)


def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = _PROFANITY_RE.sub("***", text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _has_profanity(text: str) -> bool:
    return bool(_PROFANITY_RE.search(text or ""))


# ── Pyrogram async core ────────────────────────────────────────────────────────

async def _async_forward_posts(
    donors: List[str],
    target_chat_id: int,
    limit_per_donor: int,
    total_limit: int,
    only_relevant: bool,
    hours_back: int,
) -> Dict[str, Any]:
    """
    Connect via Pyrogram, iterate donor channels, forward suitable posts to target.
    Returns summary dict.
    """
    from pyrogram import Client
    from pyrogram.errors import FloodWait
    from datetime import datetime, timezone, timedelta

    secrets = _load_pyrogram_secrets()
    api_id = secrets["api_id"]
    api_hash = secrets["api_hash"]
    session_string = secrets["session_string"]

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    forwarded = []
    errors = []

    async with Client(
        name="ouroboros_session",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        no_updates=True,
    ) as app:
        for donor in donors:
            donor_count = 0
            try:
                async for msg in app.get_chat_history(donor, limit=100):
                    if total_limit and len(forwarded) >= total_limit:
                        break

                    # Time filter
                    if msg.date and msg.date < cutoff:
                        break  # older than cutoff, channel history is reverse-chron

                    # Skip service messages
                    if not (msg.text or msg.caption or msg.photo or msg.video or msg.document):
                        continue

                    text = msg.text or msg.caption or ""

                    # Relevance filter
                    if only_relevant and not _is_relevant(text):
                        continue

                    # Forward the message
                    try:
                        needs_caption_edit = _has_profanity(text)

                        if needs_caption_edit:
                            # Can't edit during forward, forward then edit caption
                            fwd = await app.forward_messages(
                                chat_id=target_chat_id,
                                from_chat_id=donor,
                                message_ids=msg.id,
                            )
                            # Try to edit caption/text if the forwarded msg has one
                            clean = _clean_text(text)
                            try:
                                if fwd and fwd[0]:
                                    if fwd[0].caption:
                                        await app.edit_message_caption(target_chat_id, fwd[0].id, clean)
                                    elif fwd[0].text:
                                        await app.edit_message_text(target_chat_id, fwd[0].id, clean)
                            except Exception:
                                pass  # Edit failed — forward still happened, skip silently
                        else:
                            await app.forward_messages(
                                chat_id=target_chat_id,
                                from_chat_id=donor,
                                message_ids=msg.id,
                            )

                        forwarded.append({
                            "donor": donor,
                            "msg_id": msg.id,
                            "date": msg.date.isoformat() if msg.date else "",
                            "text_preview": text[:80],
                        })
                        donor_count += 1

                        if donor_count >= limit_per_donor:
                            break

                        # Polite delay to avoid FloodWait
                        await asyncio.sleep(1.5)

                    except FloodWait as e:
                        log.warning(f"FloodWait {e.value}s from {donor}")
                        await asyncio.sleep(e.value + 2)
                    except Exception as e:
                        errors.append({"donor": donor, "msg_id": msg.id, "error": repr(e)})

            except Exception as e:
                errors.append({"donor": donor, "error": repr(e)})

    return {
        "forwarded_count": len(forwarded),
        "forwarded": forwarded,
        "errors": errors,
    }


# ── Tool implementations ───────────────────────────────────────────────────────

def _forward_posts_to_buffer(
    ctx: ToolContext,
    donors: Optional[List[str]] = None,
    limit_per_donor: int = 5,
    total_limit: int = 15,
    only_relevant: bool = True,
    hours_back: int = 26,
    target_chat_id: int = BUFFER_CHANNEL_ID,
) -> str:
    """
    Forward posts from donor Telegram channels to the buffer channel using Pyrogram.
    Uses the real user account (session_string) to forward messages with all media intact.
    """
    if donors is None:
        donors = DONOR_CHANNELS

    try:
        result = asyncio.run(
            _async_forward_posts(
                donors=donors,
                target_chat_id=target_chat_id,
                limit_per_donor=limit_per_donor,
                total_limit=total_limit,
                only_relevant=only_relevant,
                hours_back=hours_back,
            )
        )
        return json.dumps(result, ensure_ascii=False, indent=2)
    except Exception as e:
        log.exception("forward_posts_to_buffer failed")
        return json.dumps({"error": repr(e)}, ensure_ascii=False)


def _fetch_channel_posts(ctx: ToolContext, username: str, limit: int = 30) -> str:
    """Fetch recent posts from a public Telegram channel web preview (read-only, no auth needed)."""
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


# ── Registry ───────────────────────────────────────────────────────────────────

def get_tools() -> List[ToolEntry]:
    return [
        ToolEntry("forward_posts_to_buffer", {
            "name": "forward_posts_to_buffer",
            "description": (
                "Forward real Telegram posts (with media, video, photos) from donor channels "
                "to the buffer channel (-1003519809178) using Pyrogram user account. "
                "Filters by topic relevance. Cleans profanity in captions. "
                "Use for daily content pipeline or initial 70-post fill. "
                "donors: list of channel usernames (default: all 4 donors). "
                "limit_per_donor: max posts per channel (default 5). "
                "total_limit: hard cap on total forwarded (default 15). "
                "only_relevant: filter by topic keywords (default true). "
                "hours_back: how far back to look in hours (default 26 = ~yesterday). "
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "donors": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of donor channel usernames. Default: all 4 donors.",
                    },
                    "limit_per_donor": {
                        "type": "integer",
                        "description": "Max posts to forward per donor channel (default: 5)",
                    },
                    "total_limit": {
                        "type": "integer",
                        "description": "Max total posts to forward in one call (default: 15)",
                    },
                    "only_relevant": {
                        "type": "boolean",
                        "description": "Filter posts by topic keywords (default: true)",
                    },
                    "hours_back": {
                        "type": "integer",
                        "description": "How many hours back to look for posts (default: 26)",
                    },
                    "target_chat_id": {
                        "type": "integer",
                        "description": "Target channel chat_id (default: -1003519809178 buffer)",
                    },
                },
                "required": [],
            },
        }, _forward_posts_to_buffer),

        ToolEntry("fetch_channel_posts", {
            "name": "fetch_channel_posts",
            "description": (
                "Fetch recent posts from a public Telegram channel by scraping t.me/s/{username}. "
                "Read-only, no auth needed. Returns JSON with posts list. "
                "Use this for inspection/preview. For actual forwarding use forward_posts_to_buffer."
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
                        "description": "Max number of posts to return (default: 30)",
                    },
                },
                "required": ["username"],
            },
        }, _fetch_channel_posts),
    ]
