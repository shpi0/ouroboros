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
BUFFER_INVITE_LINK = "https://t.me/+SK3CvpXtUc0wM2Ri"
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

# Strict keywords for donors that cover broad topics (e.g. warhistoryalconafter)
# Only posts matching these will be selected from strict-filtered donors
STRICT_KEYWORDS = [
    # Direct Russia/SVO connection
    "россия", "российск", "рф", "русск",
    "сво", "спецоперац",
    "украин",  # only as battlefield context
    "донбасс", "луганск", "донецк", "запорожье", "херсон", "харьков",
    "белгород", "брянск", "курск",
    # Russian military/weapons
    "армия россий", "вс рф", "минобороны", "генштаб",
    "ланцет", "герань", "кинжал", "калибр", "искандер",
    "дрон", "бпла", "fpv",
    # Battalion/Roscosmos specific
    "уран", "батальон", "роскосмос", "космос",
    # Russian achievements/history
    "победа", "9 мая", "великая отечественная", "вов",
    "достижен", "рекорд",
]

# Donors that require strict filtering (only STRICT_KEYWORDS, not TOPIC_KEYWORDS)
STRICT_FILTER_DONORS = {"warhistoryalconafter"}

# LLM classification cache: msg_id -> bool
_llm_classify_cache: Dict[int, bool] = {}

_LLM_SYSTEM_PROMPT = (
    "Ты — модератор военно-патриотического Telegram-канала «Батальон УРАН» (подразделение Роскосмоса, СВО).\n"
    "Оцени, подходит ли данный пост для публикации.\n\n"
    "Подходит:\n"
    "- Боевые операции российской армии, уничтожение техники ВСУ, работа дронов/Ланцетов/Гераней\n"
    "- Достижения российского ВПК и армии (новое вооружение, передача техники)\n"
    "- Рекрутинг в батальон УРАН\n"
    "- Деятельность Роскосмоса (гуманитарная помощь, мероприятия, космос)\n"
    "- Помощь ветеранам СВО, поддержка военных\n"
    "- Победа, история ВОВ, российские праздники в контексте СВО\n\n"
    "НЕ подходит:\n"
    "- Зарубежная политика и дипломатия без прямой связи с СВО\n"
    "- Новости о западном вооружении без контекста применения против России\n"
    "- Внутренняя политика Украины\n"
    "- Чужие войны (Ближний Восток, Африка, Азия) без связи с Россией\n"
    "- Юмор, развлечения, никак не связанные с тематикой\n"
    "- Негативные новости о событиях на территории России: обстрелы городов РФ, пожары, экологические катастрофы, жертвы мирного населения РФ\n"
    "- Посты, основная цель которых — реклама или раскрутка чужого канала (без полезного контента)\n"
    "- Призывы донатить/поддержать сторонние проекты\n\n"
    "Ответь строго JSON: {\"ok\": true} или {\"ok\": false}"
)

_LLM_CLEAN_SYSTEM = (
    "Ты — редактор военно-патриотического Telegram-канала «Батальон УРАН».\n"
    "Твоя задача — очистить текст поста от лишнего и привести к публикабельному виду.\n\n"
    "Правила:\n"
    "1. УДАЛИ из текста:\n"
    "   - Все строки-футеры типа: \"@channel_name\", \"t.me/channel\", \"Подписаться на канал\","
    " \"Мы теперь в МАХ\", \"наш МАХ\", \"⚔️ Вспомнить о войне\","
    " \"😡 Злой журналист http://t.me/...\", и любые подобные рекламные/навигационные строки в конце поста\n"
    "   - Самопрезентации типа \"Подписаться | наш ВКонтакте | наш МАХ\"\n\n"
    "2. ПЕРЕФРАЗИРУЙ (если есть):\n"
    "   - Токсичные/жаргонные выражения: \"прожарка укропа\" → \"кадры уничтожения техники ВСУ\","
    " \"укроп в хату\" → \"работа наших бойцов\", \"укропов нет\" → \"ВСУ уничтожены\" и т.п.\n"
    "   - Сохраняй боевой дух, но без оскорблений\n\n"
    "3. ЗАМЕНИ матерные слова на ***\n\n"
    "4. НЕ МЕНЯЙ остальной текст — не переписывай, не дополняй, не сокращай смысловую часть\n\n"
    "5. Если текст состоит только из футера/ссылок — верни пустую строку \"\"\n\n"
    "Ответь строго JSON: {\"text\": \"очищенный текст\"}"
)

def _is_advertisement(text: str) -> bool:
    """Pre-filter: returns True only if the post's PRIMARY purpose is channel promotion.
    A post that has real content + channel link at the end is NOT an ad."""
    if not text:
        return False

    low = text.lower().strip()

    # Hard ad phrases — if post starts with these, it's an ad regardless of length
    start_ad_phrases = [
        "рекомендую подписаться",
        "рекомендую канал",
        "подпишитесь на канал",
        "друзья, рекомендую",
        "советую подписаться",
        "подписывайтесь на канал",
    ]
    for phrase in start_ad_phrases:
        if low.startswith(phrase):
            return True

    # If post is short AND contains ad language — likely pure ad
    clean_len = len(low.replace(" ", "").replace("\n", ""))
    if clean_len < 150:
        short_ad_phrases = [
            "подпишитесь на",
            "подписывайтесь на",
            "рекомендую",
            "переходите в канал",
            "поддержите канал",
            "поддержать наш канал",
        ]
        if any(phrase in low for phrase in short_ad_phrases):
            return True

    return False


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


def _is_strictly_relevant(text: str) -> bool:
    """Strict relevance check for broad-topic donors — must have direct Russia/SVO connection."""
    if not text:
        return False
    low = text.lower()
    return any(kw in low for kw in STRICT_KEYWORDS)


def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = _PROFANITY_RE.sub("***", text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _has_profanity(text: str) -> bool:
    return bool(_PROFANITY_RE.search(text or ""))


def _strip_footers(text: str) -> str:
    """Remove channel-promo footer lines from post text."""
    if not text:
        return ""
    lines = text.split('\n')
    clean: List[str] = []
    for line in lines:
        s = line.strip()
        if re.match(r'^@\w+$', s):
            continue
        if re.match(r'^https?://t\.me/\S+$', s):
            continue
        if 'Подписаться на канал' in s:
            continue
        if re.search(r'Мы теперь в.{0,10}М[АA]Х', s, re.IGNORECASE):
            continue
        if re.search(r'наш\s+М[АA]Х', s, re.IGNORECASE):
            continue
        if 'наш ВКонтакте' in s:
            continue
        if 'Подписаться |' in s or '| наш' in s:
            continue
        clean.append(line)
    while clean and not clean[-1].strip():
        clean.pop()
    return '\n'.join(clean)


async def _llm_classify_post(text: str, msg_id: Optional[int] = None) -> bool:
    """Return True if the post fits @UranWar, using OpenRouter LLM. Falls back to strict keyword filter on error."""
    if msg_id is not None and msg_id in _llm_classify_cache:
        return _llm_classify_cache[msg_id]

    if not text or not text.strip():
        result = False
        if msg_id is not None:
            _llm_classify_cache[msg_id] = result
        return result

    api_key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("OR_KEY", "")
    if not api_key:
        log.warning("LLM classify: no API key, falling back to strict keyword filter")
        result = _is_strictly_relevant(text)
        if msg_id is not None:
            _llm_classify_cache[msg_id] = result
        return result

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=api_key, base_url="https://openrouter.ai/api/v1")
        response = await asyncio.wait_for(
            client.chat.completions.create(
                model="google/gemini-2.0-flash-001",
                messages=[
                    {"role": "system", "content": _LLM_SYSTEM_PROMPT},
                    {"role": "user", "content": text[:2000]},
                ],
                max_tokens=10,
                temperature=0,
            ),
            timeout=15,
        )
        raw = response.choices[0].message.content or ""
        m = re.search(r'"ok"\s*:\s*(true|false)', raw, re.IGNORECASE)
        result = m.group(1).lower() == "true" if m else _is_strictly_relevant(text)
    except Exception as e:
        log.warning(f"LLM classify failed: {e!r}, falling back to strict keyword filter")
        result = _is_strictly_relevant(text)

    if msg_id is not None:
        _llm_classify_cache[msg_id] = result
    return result


async def _clean_post_text(text: str) -> str:
    """Clean post text via LLM: strip footers, neutralize slurs, censor profanity.
    Falls back to regex-only if LLM is unavailable."""
    if not text:
        return ""

    api_key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("OR_KEY", "")
    if not api_key:
        return _clean_text(_strip_footers(text))

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=api_key, base_url="https://openrouter.ai/api/v1")
        response = await asyncio.wait_for(
            client.chat.completions.create(
                model="google/gemini-2.0-flash-001",
                messages=[
                    {"role": "system", "content": _LLM_CLEAN_SYSTEM},
                    {"role": "user", "content": text[:3000]},
                ],
                max_tokens=2000,
                temperature=0,
            ),
            timeout=20,
        )
        raw = (response.choices[0].message.content or "").strip()
        json_match = re.search(r'\{[\s\S]*\}', raw)
        if json_match:
            data = json.loads(json_match.group(0))
            return str(data.get("text", ""))
    except Exception as e:
        log.warning(f"LLM clean_post_text failed: {e!r}, using regex fallback")

    return _clean_text(_strip_footers(text))


async def _send_as_own_message(app, target_chat_id: int, msgs: list, text: str) -> list:
    """Send post as own message (not a forward), preserving media. Returns list of sent messages."""
    from pyrogram.types import InputMediaPhoto, InputMediaVideo

    cleaned = await _clean_post_text(text)

    if len(msgs) == 1:
        msg = msgs[0]
        if msg.photo:
            sent = await app.send_photo(target_chat_id, photo=msg.photo.file_id, caption=cleaned or None)
        elif msg.video:
            sent = await app.send_video(target_chat_id, video=msg.video.file_id, caption=cleaned or None)
        elif msg.animation:
            sent = await app.send_animation(target_chat_id, animation=msg.animation.file_id, caption=cleaned or None)
        elif msg.document:
            sent = await app.send_document(target_chat_id, document=msg.document.file_id, caption=cleaned or None)
        else:
            sent = await app.send_message(target_chat_id, cleaned) if cleaned else None
        return [sent] if sent else []

    # Media group (album)
    media = []
    for i, msg in enumerate(msgs):
        cap = cleaned if i == 0 else None
        if msg.photo:
            media.append(InputMediaPhoto(msg.photo.file_id, caption=cap))
        elif msg.video:
            media.append(InputMediaVideo(msg.video.file_id, caption=cap))

    if not media:
        sent = await app.send_message(target_chat_id, cleaned) if cleaned else None
        return [sent] if sent else []

    sent_list = await app.send_media_group(target_chat_id, media=media)
    return sent_list if isinstance(sent_list, list) else [sent_list]


async def _warm_up_buffer_peer(app, target_chat_id: int, invite_link: str = BUFFER_INVITE_LINK) -> int:
    """Resolve buffer channel peer via invite link (needed when peer cache is empty after session restore)."""
    try:
        chat = await app.get_chat(invite_link)
        log.info(f"Buffer peer resolved via invite: id={chat.id} title={chat.title}")
        return chat.id
    except Exception as e:
        log.warning(f"Could not resolve via invite link: {e!r}, using raw id")
        return target_chat_id


# ── Pyrogram async core ────────────────────────────────────────────────────────

async def _async_forward_posts(
    donors: List[str],
    target_chat_id: int,
    limit_per_donor: int,
    total_limit: int,
    only_relevant: bool,
    hours_back: int,
) -> Dict[str, Any]:
    from pyrogram import Client
    from pyrogram.errors import FloodWait
    from datetime import datetime, timedelta
    from collections import OrderedDict

    secrets = _load_pyrogram_secrets()
    api_id = secrets["api_id"]
    api_hash = secrets["api_hash"]
    session_string = secrets["session_string"]

    cutoff = datetime.utcnow() - timedelta(hours=hours_back)

    forwarded = []
    errors = []

    _military_donors = {"warhistoryalconafter", "Vspomni_o_Voine_neraz", "zloy_zhurnalist"}

    async with Client(
        name="ouroboros_session",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        no_updates=True,
    ) as app:
        # Warm up peer cache via invite link
        target_chat_id = await _warm_up_buffer_peer(app, target_chat_id)

        # Phase 1: collect all filtered candidates from all donors
        # Each candidate: (donor, gid, msgs, text)
        military_candidates: List[tuple] = []
        roscosmos_candidates: List[tuple] = []

        for donor in donors:
            try:
                raw_messages = []
                async for msg in app.get_chat_history(donor, limit=200):
                    if msg.date and msg.date < cutoff:
                        break  # history is newest-first; once past cutoff, stop
                    if not (msg.text or msg.caption or msg.photo or msg.video or msg.document or msg.animation):
                        continue
                    raw_messages.append(msg)

                groups: OrderedDict = OrderedDict()
                for msg in reversed(raw_messages):  # oldest first
                    gid = msg.media_group_id if msg.media_group_id else f"single_{msg.id}"
                    if gid not in groups:
                        groups[gid] = []
                    groups[gid].append(msg)

                donor_count = 0
                for gid, msgs in groups.items():
                    if donor_count >= limit_per_donor:
                        break

                    rep_msg = msgs[0]
                    text = rep_msg.text or rep_msg.caption or ""
                    for m in msgs[1:]:
                        if m.text or m.caption:
                            text = m.text or m.caption
                            break

                    # Ad pre-filter: skip immediately without calling LLM
                    if _is_advertisement(text):
                        continue

                    # LLM relevance filter
                    if only_relevant:
                        if not await _llm_classify_post(text, msg_id=rep_msg.id):
                            continue

                    if donor in _military_donors:
                        military_candidates.append((donor, gid, msgs, text))
                    else:
                        roscosmos_candidates.append((donor, gid, msgs, text))
                    donor_count += 1

            except Exception as e:
                errors.append({"donor": donor, "error": repr(e)})

        # Phase 2: interleave — every 4 military posts, insert 1 roscosmos post
        candidates: List[tuple] = []
        ros_idx = 0
        for i, item in enumerate(military_candidates):
            candidates.append(item)
            if (i + 1) % 4 == 0 and ros_idx < len(roscosmos_candidates):
                candidates.append(roscosmos_candidates[ros_idx])
                ros_idx += 1
        candidates.extend(roscosmos_candidates[ros_idx:])

        # Phase 3: send as own messages in interleaved order
        for donor, gid, msgs, text in candidates:
            if total_limit and len(forwarded) >= total_limit:
                break

            message_ids = [m.id for m in msgs]
            rep_msg = msgs[0]

            try:
                sent_msgs = await _send_as_own_message(app, target_chat_id, msgs, text)

                forwarded.append({
                    "donor": donor,
                    "msg_ids": message_ids,
                    "media_group_id": gid if not gid.startswith("single_") else None,
                    "date": rep_msg.date.isoformat() if rep_msg.date else "",
                    "text_preview": text[:80],
                    "count_in_group": len(msgs),
                })

                await asyncio.sleep(1.5)

            except FloodWait as e:
                log.warning(f"FloodWait {e.value}s from {donor}")
                await asyncio.sleep(e.value + 2)
            except Exception as e:
                errors.append({"donor": donor, "msg_ids": message_ids, "error": repr(e)})

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


async def _async_clear_buffer(target_chat_id: int) -> Dict[str, Any]:
    from pyrogram import Client
    from pyrogram.errors import FloodWait

    secrets = _load_pyrogram_secrets()
    deleted = 0
    errors = []

    async with Client(
        name="ouroboros_session",
        api_id=secrets["api_id"],
        api_hash=secrets["api_hash"],
        session_string=secrets["session_string"],
        no_updates=True,
    ) as app:
        target_chat_id = await _warm_up_buffer_peer(app, target_chat_id)
        msg_ids = []
        async for msg in app.get_chat_history(target_chat_id):
            msg_ids.append(msg.id)

        # Delete in batches of 100
        for i in range(0, len(msg_ids), 100):
            batch = msg_ids[i:i+100]
            try:
                await app.delete_messages(target_chat_id, batch)
                deleted += len(batch)
                await asyncio.sleep(0.5)
            except FloodWait as e:
                await asyncio.sleep(e.value + 2)
            except Exception as e:
                errors.append(repr(e))

    return {"deleted_count": deleted, "errors": errors}


def _clear_buffer_channel(ctx: ToolContext, target_chat_id: int = BUFFER_CHANNEL_ID) -> str:
    """Delete all messages in the buffer channel."""
    try:
        result = asyncio.run(_async_clear_buffer(target_chat_id))
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        log.exception("clear_buffer_channel failed")
        return json.dumps({"error": repr(e)}, ensure_ascii=False)


def _init_buffer(ctx: ToolContext, total_posts: int = 70, hours_back: int = 168, target_chat_id: int = BUFFER_CHANNEL_ID) -> str:
    """Clear buffer channel and re-fill with relevant posts from donors."""
    try:
        # Step 1: clear
        clear_result = json.loads(_clear_buffer_channel(ctx, target_chat_id=target_chat_id))
        # Step 2: fill
        fill_result = json.loads(_forward_posts_to_buffer(
            ctx,
            donors=DONOR_CHANNELS,
            limit_per_donor=25,
            total_limit=total_posts,
            only_relevant=True,
            hours_back=hours_back,
            target_chat_id=target_chat_id,
        ))
        return json.dumps({
            "cleared": clear_result,
            "filled": fill_result,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        log.exception("init_buffer failed")
        return json.dumps({"error": repr(e)}, ensure_ascii=False)


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

        ToolEntry("clear_buffer_channel", {
            "name": "clear_buffer_channel",
            "description": "Delete all messages in the buffer channel to reset it",
            "parameters": {
                "type": "object",
                "properties": {
                    "target_chat_id": {
                        "type": "integer",
                        "description": "Target channel chat_id (default: -1003519809178 buffer)",
                    },
                },
                "required": [],
            },
        }, _clear_buffer_channel),

        ToolEntry("init_buffer", {
            "name": "init_buffer",
            "description": "Clear buffer and re-fill with ~70 relevant posts from donor channels (use for initialization or full reset)",
            "parameters": {
                "type": "object",
                "properties": {
                    "total_posts": {
                        "type": "integer",
                        "description": "Total posts to forward after clearing (default: 70)",
                    },
                    "hours_back": {
                        "type": "integer",
                        "description": "How many hours back to look for posts (default: 168 = 7 days)",
                    },
                    "target_chat_id": {
                        "type": "integer",
                        "description": "Target channel chat_id (default: -1003519809178 buffer)",
                    },
                },
                "required": [],
            },
        }, _init_buffer),
    ]
