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
import datetime
import json
import logging
import os
import re
import sqlite3
import sys
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

# Per-donor fetch config: how many posts and how many hours back to look
# warhistoryalconafter (Kirill Fedorov) = primary donor: take all posts for last week
# Others: take last 30-50 posts within last 2-3 days
DONOR_FETCH_CONFIG: Dict[str, Dict[str, int]] = {
    "warhistoryalconafter":  {"limit": 200, "hours_back": 168},  # primary: full week
    "Vspomni_o_Voine_neraz": {"limit": 40,  "hours_back": 72},
    "zloy_zhurnalist":       {"limit": 40,  "hours_back": 72},
    "roscosmos_gk":          {"limit": 30,  "hours_back": 72},
}
DONOR_DEFAULT_CONFIG = {"limit": 40, "hours_back": 72}

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
STRICT_FILTER_DONORS = {"warhistoryalconafter", "zloy_zhurnalist"}

TELEGRAM_CAPTION_LIMIT = 1024

# Footer appended to every post sent to the buffer channel
URAN_FOOTER = (
    '\n\n<b>Батальон "УРАН"</b>\n'
    '<a href="https://max.ru/uranwar"><b>MAX</b></a>'
    ' <b>|</b> <a href="https://t.me/uranwar"><b>ТГ</b></a>'
    ' <b>|</b> <a href="https://vk.com/uranwar"><b>ВК</b></a>'
    ' <b>|</b> <a href="https://ok.ru/uranwar"><b>ОК</b></a>'
)

# Content plan: target distribution of categories in the buffer
# Based on analysis of @UranWar posts over last 4 months (500 posts):
# BATTLE: 69%, URAN: 7%, ROSCOSMOS: 4%, HISTORY: 3%, OTHER/misc: 16%
# We set BATTLE higher to account for unlabeled combat content in OTHER
CONTENT_PLAN: Dict[str, float] = {
    "BATTLE":       0.75,
    "URAN":         0.07,
    "ROSCOSMOS":    0.10,   # slightly boost Roscosmos to ensure regular presence
    "HISTORY":      0.05,
    "HUMANITARIAN": 0.03,
}

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3.5:35b")

_BUFFER_RUNNER_PATH = "/tmp/ouroboros_buffer_runner.py"
_BUFFER_PROGRESS_FILE = "/tmp/init_buffer_progress.json"


async def _warmup_ollama() -> None:
    """Load model into VRAM and keep it alive for 1h. Call before batch processing."""
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            payload = {"model": OLLAMA_MODEL, "keep_alive": "1h", "prompt": ""}
            async with session.post(
                f"{OLLAMA_BASE_URL}/api/generate",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status == 200:
                    log.info("[ollama] Model warmed up and kept alive for 1h")
                else:
                    log.warning("[ollama] Warmup failed: status %d", resp.status)
    except Exception as e:
        log.warning("[ollama] Warmup error (non-fatal): %s", e)


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
    "- СТРОГО ЗАПРЕЩЕНО: любые новости об атаках на территорию России — обстрелы городов РФ дронами/ракетами ВСУ, пожары/взрывы на российских предприятиях от атак, жертвы среди мирного населения РФ, атаки БПЛА на российские регионы. Даже если пострадавших нет — такие новости НЕ публикуем.\n"
    "- Посты, основная цель которых — реклама или раскрутка чужого канала (без полезного контента)\n"
    "- Призывы донатить/поддержать сторонние проекты\n"
    "- Личные рекомендации книг, фильмов, других каналов или продуктов (даже по военной тематике) — если это не боевые новости\n\n"
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

    # Commercial ads: paid events, courses, webinars, registration links
    commercial_ad_phrases = [
        "регистрация по ссылке",
        "регистрация на семинар",
        "регистрация на курс",
        "регистрация на вебинар",
        "регистрация на мероприятие",
        "закрываем регистрацию",
        "открываем регистрацию",
        "присоединяйтесь к семинару",
        "пройдет семинар",
        "платный курс",
        "платный семинар",
        "записывайтесь на курс",
        "записывайтесь на вебинар",
        "купить билет",
        "купите билет",
        "стоимость участия",
        "цена участия",
    ]
    if any(phrase in low for phrase in commercial_ad_phrases):
        return True

    # Fundraising posts with bank card numbers (e.g. "2202206296822799\nКирилл Фёдоров, сбер")
    # Pattern: 16-digit card number (possibly with spaces) in the post
    if re.search(r'\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b', text):
        return True

    # External commercial links (non-telegram): artamonov.online, site.ru etc.
    # If post contains a non-tg URL AND has registration/event keywords — it's a commercial ad
    has_external_url = bool(re.search(r'https?://(?!t\.me)[^\s]+|(?<!\S)[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?(?=\s|$)', low))
    event_keywords = ["семинар", "вебинар", "курс", "тренинг", "мероприятие", "лекция", "воркшоп"]
    if has_external_url and any(kw in low for kw in event_keywords):
        return True

    # Soft ads: personal recommendations of books, products, content
    soft_rec_phrases = [
        "крайне рекомендую",
        "очень рекомендую",
        "рекомендую книгу",
        "рекомендую прочит",
        "рекомендую посмотр",
        "рекомендую этот канал",
        "рекомендую всем",
        "советую прочит",
        "советую посмотр",
        "советую всем",
    ]
    # Check if it's a soft-ad post: recommendation WITHOUT combat content
    combat_keywords = [
        "бпла", "дрон", "ланцет", "герань", "уничтожен", "наступлен",
        "оборон", "фронт", "ВСУ", "всу", "боевые", "операци", "выдвижени",
        "атак", "обстрел", "роскосмос", "батальон уран",
    ]
    has_soft_rec = any(phrase in low for phrase in soft_rec_phrases)
    has_combat = any(kw in low for kw in combat_keywords)
    if has_soft_rec and not has_combat:
        return True

    # Fundraising progress reports: "100к из 600", "50 000р.", "удвоил", "собрано X"
    # These are donation collection posts — we don't publish them
    fundraising_phrases = [
        "из 600", "из 500", "из 400", "из 300", "из 200", "из 100",  # "Xк из Yк"
        "удвоил", "утроил",
        "собрано ", "собрали ",
        "помогите собрать",
        "осталось собрать",
        "цель сбора",
        "сумма сбора",
    ]
    rub_pattern = re.search(r'\d[\d\s]*[рр]\b|[\d]+к\s+из\s+[\d]+|[\d\s]+руб', low)
    has_fundraising = any(phrase in low for phrase in fundraising_phrases)
    if (has_fundraising or rub_pattern) and not has_combat:
        # Only flag if post is short (pure fundraising report) and has no combat content
        if len(low.replace(" ", "").replace("\n", "")) < 300 and not has_combat:
            return True

    # External military recruitment posts — we don't publish these.
    # Only our own (Battalion URAN / Roscosmos) recruitment is allowed — handled manually by Vladimir.
    recruitment_phrases = [
        "служба по контракту",
        "контрактная служба",
        "запись по контракту",
        "набор по контракту",
        "вступить в ряды",
        "записаться добровольц",
        "записаться в отряд",
        "консультация по контракту",
        "пункт отбора",
        "военный контракт",
        "подбор условий контракта",
    ]
    uran_markers = ["уран", "батальон уран", "roscosmos", "роскосмос"]
    is_recruitment = any(phrase in low for phrase in recruitment_phrases)
    is_our_recruitment = any(m in low for m in uran_markers)
    if is_recruitment and not is_our_recruitment:
        return True

    return False


def _is_negative_russia_news(text: str) -> bool:
    """Pre-filter: block news about attacks on Russian territory, civilian casualties in RF, fires from drone strikes."""
    if not text:
        return False
    low = text.lower()

    # Атаки ВСУ/БПЛА на территорию России
    attack_patterns = [
        r'атак[а-я]*\s+(бпла|дрон|ракет)',
        r'(бпла|дрон|ракет)[а-я]*\s+вс[уу]',
        r'удар[а-я]*\s+по\s+[а-я]+ской\s+области',
        r'попадани[а-я]*\s+(украинского|вражеского)\s+(дрон|бпла|ракет)',
        r'обломк[а-я]*\s+(дрон|бпла|ракет)[а-я]*',
    ]

    # Российские регионы — контекст атаки
    russia_regions = [
        'самарской области', 'нижегородской области', 'белгородской области',
        'курской области', 'брянской области', 'воронежской области',
        'ростовской области', 'краснодарского края', 'московской области',
        'саратовской области', 'тульской области', 'орловской области',
        'липецкой области', 'тамбовской области', 'рязанской области',
        'в новокуйбышевске', 'в туапсе', 'в белгороде', 'в брянске',
        'в курске', 'в воронеже',
    ]

    # Последствия атак
    consequence_patterns = [
        r'(погиб|ранен|пострадав)[а-я]*.*\s+(при\s+атак|от\s+удар|при\s+обстрел)',
        r'(пожар|взрыв|разрушен)[а-я]*.*\s+(от\s+(дрон|бпла|атак|удар))',
        r'(атак[а-я]*|обстрел[а-я]*)\s+.{0,30}(промпредприят|предприят|завод|нефтеперераб)',
    ]

    # Уровень загрязнения / экокатастрофы в РФ
    eco_patterns = [
        r'уровень\s+(бензол|ксилол|сажи|загрязнен)',
        r'превысил норму в .{0,20}раз',
    ]

    import re as _re

    has_attack = any(_re.search(p, low) for p in attack_patterns)
    has_region = any(r in low for r in russia_regions)
    has_consequence = any(_re.search(p, low) for p in consequence_patterns)
    has_eco = any(_re.search(p, low) for p in eco_patterns)

    # Блокируем если: (атака + российский регион) ИЛИ последствие атаки ИЛИ экокатастрофа
    return (has_attack and has_region) or has_consequence or has_eco


# Profanity patterns
_PROFANITY_RE = re.compile(
    r'\b(бля|блять|блядь|хуй|хуя|хуе|хуи|пизд\w*|еба\w*|ёба\w*|ёб\w*|'
    r'еб\w*|пидор\w*|сука|ёбан\w*|ёбн\w*|хуев\w*|нахуй|нахер|'
    r'залуп\w*|мудак\w*|мудил\w*|ёпт|епт|ёпта|епта|пиздец|пиздит\w*|'
    r'пиздёж|заеб\w*|заёб\w*|охуе\w*|охуй\w*|охуел|похуй\w*)\b',
    re.IGNORECASE | re.UNICODE,
)


# ── Persistent post database ───────────────────────────────────────────────────

DB_PATH = "/home/ouroboros/state/buffer_posts.db"

class PostDB:
    """Thread-safe SQLite store for tracking processed posts."""

    def __init__(self, path: str = DB_PATH):
        import threading
        self._path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()

    def _create_tables(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS posts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                donor       TEXT    NOT NULL,
                msg_id      INTEGER NOT NULL,
                album_id    TEXT,
                text_snippet TEXT,
                has_media   INTEGER DEFAULT 0,
                category    TEXT,
                confidence  INTEGER,
                method      TEXT,
                decision    TEXT,
                error_msg   TEXT,
                created_at  TEXT    NOT NULL,
                UNIQUE(donor, msg_id)
            );
            CREATE INDEX IF NOT EXISTS idx_donor_msg  ON posts(donor, msg_id);
            CREATE INDEX IF NOT EXISTS idx_decision   ON posts(decision);
            CREATE INDEX IF NOT EXISTS idx_created    ON posts(created_at);
        """)
        self._conn.commit()

    def is_seen(self, donor: str, msg_id: int) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "SELECT 1 FROM posts WHERE donor=? AND msg_id=? LIMIT 1", (donor, msg_id)
            )
            return cur.fetchone() is not None

    def mark_seen(self, donor: str, msg_id: int, *, album_id: str = None,
                  text_snippet: str = None, has_media: bool = False,
                  category: str = None, confidence: int = None,
                  method: str = None, decision: str = None, error_msg: str = None):
        import datetime as _dt
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO posts
                   (donor, msg_id, album_id, text_snippet, has_media,
                    category, confidence, method, decision, error_msg, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (donor, msg_id, album_id,
                 (text_snippet or "")[:200], int(has_media),
                 category, confidence, method, decision, error_msg,
                 _dt.datetime.utcnow().isoformat())
            )
            self._conn.commit()

    def update_decision(self, donor: str, msg_id: int, decision: str,
                        category: str = None, confidence: int = None,
                        method: str = None, error_msg: str = None):
        with self._lock:
            self._conn.execute(
                """UPDATE posts SET decision=?, category=?, confidence=?, method=?, error_msg=?
                   WHERE donor=? AND msg_id=?""",
                (decision, category, confidence, method, error_msg, donor, msg_id)
            )
            self._conn.commit()

    def get_last_seen_id(self, donor: str) -> Optional[int]:
        """Return highest msg_id seen for this donor (for incremental fetching)."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT MAX(msg_id) FROM posts WHERE donor=?", (donor,)
            )
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else None

    def get_stats(self) -> dict:
        with self._lock:
            rows = self._conn.execute(
                "SELECT decision, method, COUNT(*) FROM posts GROUP BY decision, method"
            ).fetchall()
            total = self._conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
            return {"total": total, "breakdown": [{"decision": r[0], "method": r[1], "count": r[2]} for r in rows]}

    def close(self):
        self._conn.close()


# Global DB instance (created lazily)
_post_db: Optional["PostDB"] = None

def _get_post_db() -> "PostDB":
    global _post_db
    if _post_db is None:
        _post_db = PostDB()
    return _post_db


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
        # Bare @username line
        if re.match(r'^@\w+$', s):
            continue
        # Any line that IS or CONTAINS a t.me link (e.g. "😡 Злой журналист http://t.me/...")
        if re.search(r'https?://t\.me/\S+', s):
            continue
        # Lines with bare @username anywhere (e.g. "подписаться @channel_name")
        if re.search(r'\s@\w{3,}$', s):
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


async def _llm_call(system: str, user: str, max_tokens: int = 20) -> str:
    """Call LLM: tries Ollama first, falls back to OpenRouter on connection failure."""
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "think": False,
        "options": {"num_predict": max_tokens, "temperature": 0},
    }
    try:
        try:
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
        except ImportError:
            import httpx
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
        log.debug("LLM backend: ollama")
        return data["message"]["content"]
    except Exception as ollama_err:
        log.debug(f"Ollama unavailable ({ollama_err!r}), falling back to OpenRouter")

    api_key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("OR_KEY", "")
    from openai import AsyncOpenAI
    client = AsyncOpenAI(api_key=api_key, base_url="https://openrouter.ai/api/v1")
    response = await asyncio.wait_for(
        client.chat.completions.create(
            model="google/gemini-2.0-flash-001",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_tokens=max_tokens,
            temperature=0,
        ),
        timeout=30,
    )
    log.debug("LLM backend: openrouter")
    return response.choices[0].message.content or ""


async def _llm_classify_post(text: str, msg_id: Optional[int] = None) -> bool:
    """Return True if the post fits @UranWar, using OpenRouter LLM. Falls back to strict keyword filter on error."""
    if msg_id is not None and msg_id in _llm_classify_cache:
        return _llm_classify_cache[msg_id]

    if not text or not text.strip():
        result = False
        if msg_id is not None:
            _llm_classify_cache[msg_id] = result
        return result

    # Pre-filter: block negative Russia territory news without LLM call
    if _is_negative_russia_news(text):
        if msg_id is not None:
            _llm_classify_cache[msg_id] = False
        return False

    try:
        raw = await _llm_call(_LLM_SYSTEM_PROMPT, text[:2000], max_tokens=20)
        m = re.search(r'"ok"\s*:\s*(true|false)', raw, re.IGNORECASE)
        result = m.group(1).lower() == "true" if m else _is_strictly_relevant(text)
    except Exception as e:
        log.warning(f"LLM classify failed: {e!r}, falling back to strict keyword filter")
        result = _is_strictly_relevant(text)

    if msg_id is not None:
        _llm_classify_cache[msg_id] = result
    return result


async def _llm_get_post_category(text: str) -> str:
    """Get content category for a post using LLM. Returns one of: BATTLE, ROSCOSMOS, HISTORY, URAN, HUMANITARIAN, OTHER."""
    if not text or len(text.strip()) < 20:
        return "OTHER"

    system = (
        "Classify this Russian military Telegram post into ONE category.\n"
        "Categories:\n"
        "- BATTLE: combat ops, weapon destruction, drones, front lines, ВСУ losses\n"
        "- ROSCOSMOS: Roscosmos news, space launches, humanitarian aid from Roscosmos\n"
        "- HISTORY: WWII facts, historical anniversaries, Russia's past victories\n"
        "- URAN: Battalion URAN recruitment or reports\n"
        "- HUMANITARIAN: aid convoys, volunteer help, support for civilians\n"
        "- OTHER: anything else\n"
        'Reply ONLY with JSON: {"cat": "CATEGORY"}'
    )
    raw = await _llm_call(system, text[:400], max_tokens=20)
    try:
        m = re.search(r'"cat"\s*:\s*"(\w+)"', raw)
        if m:
            cat = m.group(1).upper()
            valid = {"BATTLE", "ROSCOSMOS", "HISTORY", "URAN", "HUMANITARIAN", "OTHER"}
            return cat if cat in valid else "OTHER"
    except Exception:
        pass
    return "OTHER"


async def _llm_classify_and_categorize(text: str, msg_id: Optional[int] = None) -> tuple[bool, str]:
    """Combined classify + categorize in ONE LLM call. Returns (is_relevant, category)."""
    if not text or not text.strip():
        return False, "OTHER"

    # Pre-filters (no LLM needed)
    if _is_negative_russia_news(text):
        return False, "OTHER"
    if _is_advertisement(text):
        return False, "OTHER"

    # Cache check
    cache_key = msg_id if msg_id is not None else hash(text[:200])
    if hasattr(_llm_classify_cache, '__contains__') and cache_key in _llm_classify_cache:
        # Can't return category from old bool cache — skip cache
        pass

    system = (
        "You are a content classifier for a Russian military Telegram channel @UranWar (Battalion URAN, Roscosmos).\n"
        "Given a post, return JSON with two fields:\n"
        "- ok: true if the post fits @UranWar (combat ops, military equipment, SVO updates, Roscosmos, battalion news, military history); false if it's ads, civilian tragedy in Russia, foreign politics unrelated to SVO, fundraising\n"
        "- cat: content category. ONE of: BATTLE, ROSCOSMOS, HISTORY, URAN, HUMANITARIAN, OTHER\n"
        "  BATTLE = combat ops, weapon destruction, drones, front lines, ВСУ losses\n"
        "  ROSCOSMOS = Roscosmos news, space, humanitarian aid from Roscosmos\n"
        "  HISTORY = WWII facts, historical anniversaries\n"
        "  URAN = Battalion URAN news\n"
        "  HUMANITARIAN = aid convoys, volunteer help\n"
        "  OTHER = anything else relevant\n"
        'Reply ONLY with JSON: {"ok": true, "cat": "BATTLE"}'
    )
    try:
        raw = await _llm_call(system, text[:1500], max_tokens=30)
        m = re.search(r'\{[^}]+\}', raw, re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            ok = bool(data.get("ok", False))
            cat = str(data.get("cat", "OTHER")).upper()
            valid_cats = {"BATTLE", "ROSCOSMOS", "HISTORY", "URAN", "HUMANITARIAN", "OTHER"}
            if cat not in valid_cats:
                cat = "OTHER"
            return ok, cat
    except Exception as e:
        log.warning(f"LLM classify_and_categorize failed: {e!r}, falling back to keyword filter")
        return _is_strictly_relevant(text), "BATTLE"

    return _is_strictly_relevant(text), "BATTLE"


async def _clean_post_text(text: str) -> str:
    """Clean post text via LLM: strip footers, neutralize slurs, censor profanity.
    Falls back to regex-only if LLM is unavailable."""
    if not text:
        return ""

    try:
        raw = (await _llm_call(_LLM_CLEAN_SYSTEM, text[:3000], max_tokens=2000)).strip()
        json_match = re.search(r'\{[\s\S]*\}', raw)
        if json_match:
            data = json.loads(json_match.group(0))
            return str(data.get("text", ""))
    except Exception as e:
        log.warning(f"LLM clean_post_text failed: {e!r}, using regex fallback")

    return _clean_text(_strip_footers(text))


def _add_uran_footer(text: str) -> str:
    """Append Батальон УРАН branded footer to post text (HTML formatted)."""
    return (text.rstrip() if text else "") + URAN_FOOTER


async def _send_as_own_message(app, target_chat_id: int, msgs: list, text: str) -> list:
    """Send post as own message (not a forward), preserving media. Returns list of sent messages.
    Tries HTML parse mode first, falls back to plaintext on parse errors from Telegram API.
    """
    from pyrogram import enums
    from pyrogram.types import InputMediaPhoto, InputMediaVideo

    cleaned = await _clean_post_text(text)
    cleaned = str(cleaned) if cleaned is not None else ""
    max_main_len = TELEGRAM_CAPTION_LIMIT - len(URAN_FOOTER)
    if len(cleaned) > max_main_len:
        cleaned = cleaned[:max_main_len].rstrip()
    cleaned = _add_uran_footer(cleaned)
    caption_text = cleaned if cleaned else None

    async def _try_send(parse_mode):
        """Attempt send with given parse_mode. Returns sent messages list."""
        if len(msgs) == 1:
            msg = msgs[0]
            if msg.photo:
                sent = await app.send_photo(target_chat_id, photo=msg.photo.file_id, caption=caption_text, parse_mode=parse_mode)
            elif msg.video:
                sent = await app.send_video(target_chat_id, video=msg.video.file_id, caption=caption_text, parse_mode=parse_mode)
            elif msg.animation:
                sent = await app.send_animation(target_chat_id, animation=msg.animation.file_id, caption=caption_text, parse_mode=parse_mode)
            elif msg.document:
                sent = await app.send_document(target_chat_id, document=msg.document.file_id, caption=caption_text, parse_mode=parse_mode)
            else:
                sent = await app.send_message(target_chat_id, cleaned, parse_mode=parse_mode) if cleaned else None
            return [sent] if sent else []
        # Media group (album)
        media = []
        for i, m in enumerate(msgs):
            cap = caption_text if i == 0 else None
            if m.photo:
                media.append(InputMediaPhoto(m.photo.file_id, caption=cap, parse_mode=parse_mode))
            elif m.video:
                media.append(InputMediaVideo(m.video.file_id, caption=cap, parse_mode=parse_mode))
        if not media:
            sent = await app.send_message(target_chat_id, cleaned, parse_mode=parse_mode) if cleaned else None
            return [sent] if sent else []
        sent_list = await app.send_media_group(target_chat_id, media=media)
        return sent_list if isinstance(sent_list, list) else [sent_list]

    try:
        return await _try_send(enums.ParseMode.HTML)
    except Exception as e:
        err_str = str(e).lower()
        if "parse mode" in err_str or "bad request" in err_str or "can't parse" in err_str:
            log.warning(f"[buffer] HTML parse_mode rejected by Telegram, retrying as DISABLED: {e!r}")
            return await _try_send(enums.ParseMode.DISABLED)
        raise


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

def _write_init_progress(sent: int, total: int, last_post: str, started_at: str, status: str) -> None:
    try:
        data = {
            "sent": sent,
            "total": total,
            "last_post": (last_post or "")[:120],
            "started_at": started_at,
            "status": status,
        }
        with open("/tmp/init_buffer_progress.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as exc:
        log.warning(f"Failed to write init progress: {exc!r}")


async def _async_forward_posts(
    donors: List[str],
    target_chat_id: int,
    total_limit: int,
    only_relevant: bool,
    hours_back: int,
    batch_size: int = 5,
    progress_file: Optional[str] = None,
) -> Dict[str, Any]:
    from pyrogram import Client, enums
    from pyrogram.errors import FloodWait
    from pyrogram.types import InputMediaPhoto, InputMediaVideo
    from datetime import datetime, timedelta
    from collections import OrderedDict

    secrets = _load_pyrogram_secrets()
    db = _get_post_db()

    started_at = datetime.utcnow().isoformat()
    sent_total = 0
    errors_total = 0
    content_plan_counts: Dict[str, int] = {cat: 0 for cat in CONTENT_PLAN}
    allocations: Dict[str, int] = {
        cat: round(total_limit * frac) for cat, frac in CONTENT_PLAN.items()
    }

    def _write_progress(status: str, last_text: str = ""):
        try:
            with open(progress_file or "/tmp/init_buffer_progress.json", "w", encoding="utf-8") as f:
                json.dump({
                    "status": status,
                    "sent": sent_total,
                    "total": total_limit,
                    "last_post": (last_text or "")[:120],
                    "errors": errors_total,
                    "db_stats": db.get_stats(),
                }, f, ensure_ascii=False)
        except Exception:
            pass

    async with Client(
        name="ouroboros_session",
        api_id=secrets["api_id"],
        api_hash=secrets["api_hash"],
        session_string=secrets["session_string"],
        no_updates=True,
    ) as app:
        target_chat_id = await _warm_up_buffer_peer(app, target_chat_id)
        _write_progress("running", "Connected, starting...")

        for donor in donors:
            if sent_total >= total_limit:
                break

            cfg = DONOR_FETCH_CONFIG.get(donor, DONOR_DEFAULT_CONFIG)
            cutoff = datetime.utcnow() - timedelta(hours=hours_back)

            # Fetch all raw messages from donor (newest first), group albums
            raw_messages = []
            try:
                async for msg in app.get_chat_history(donor, limit=cfg["limit"]):
                    if msg.date and msg.date < cutoff:
                        break
                    if not (msg.text or msg.caption or msg.photo or msg.video or msg.document or msg.animation):
                        continue
                    raw_messages.append(msg)
            except Exception as e:
                log.warning(f"[buffer] fetch error from {donor}: {e!r}")
                errors_total += 1
                continue

            # Group into albums (oldest first)
            groups: OrderedDict = OrderedDict()
            for msg in reversed(raw_messages):
                gid = msg.media_group_id if msg.media_group_id else f"single_{msg.id}"
                if gid not in groups:
                    groups[gid] = []
                groups[gid].append(msg)

            # Build list of (gid, msgs, representative_msg_id, text)
            all_groups = []
            for gid, msgs in groups.items():
                rep = msgs[0]
                text = rep.text or rep.caption or ""
                for m in msgs[1:]:
                    if m.text or m.caption:
                        text = m.text or m.caption
                        break
                all_groups.append((gid, msgs, rep.id, text))

            # Sort: media first
            all_groups.sort(key=lambda x: 0 if any(m.photo or m.video or m.document or m.animation for m in x[1]) else 1)

            # Process in chunks of batch_size
            for chunk_start in range(0, len(all_groups), batch_size):
                if sent_total >= total_limit:
                    break

                chunk = all_groups[chunk_start:chunk_start + batch_size]

                # Classify chunk in parallel
                async def _classify_one(gid, msgs, rep_id, text):
                    # Skip already seen
                    if db.is_seen(donor, rep_id):
                        return None

                    # Ad pre-filter (no LLM)
                    if _is_advertisement(text):
                        db.mark_seen(donor, rep_id, album_id=gid, text_snippet=text,
                                     has_media=bool(msgs[0].photo or msgs[0].video),
                                     decision="filtered", method="regex")
                        log.debug(f"[buffer] SKIP {donor}/{rep_id} filtered:ad regex")
                        return None

                    # Strict donor check (no LLM)
                    if donor in STRICT_FILTER_DONORS and not _is_strictly_relevant(text):
                        db.mark_seen(donor, rep_id, album_id=gid, text_snippet=text,
                                     has_media=bool(msgs[0].photo or msgs[0].video),
                                     decision="filtered", method="regex")
                        log.debug(f"[buffer] SKIP {donor}/{rep_id} filtered:strict_donor regex")
                        return None

                    # LLM classify + categorize
                    try:
                        ok, cat = await _llm_classify_and_categorize(text, msg_id=rep_id)
                        method = "llm"
                    except Exception as e:
                        log.warning(f"[buffer] LLM failed for {donor}/{rep_id}: {e!r}")
                        ok = _is_strictly_relevant(text)
                        cat = "BATTLE"
                        method = "fallback"

                    return (gid, msgs, rep_id, text, ok, cat, method)

                classify_tasks = [_classify_one(gid, msgs, rep_id, text) for gid, msgs, rep_id, text in chunk]
                results = await asyncio.gather(*classify_tasks, return_exceptions=True)

                # Send accepted posts
                for r in results:
                    if sent_total >= total_limit:
                        break
                    if r is None or isinstance(r, Exception):
                        continue

                    gid, msgs, rep_id, text, ok, cat, method = r
                    has_media = bool(msgs[0].photo or msgs[0].video or msgs[0].document or msgs[0].animation)

                    if not ok:
                        db.mark_seen(donor, rep_id, album_id=gid, text_snippet=text,
                                     has_media=has_media, category=cat,
                                     decision="skipped", method=method)
                        log.info(f"[buffer] SKIP {donor}/{rep_id} cat={cat} method={method}")
                        continue

                    # Content plan quota (soft)
                    effective_cat = cat
                    if content_plan_counts.get(cat, 0) >= allocations.get(cat, 999):
                        if cat != "BATTLE":
                            effective_cat = "BATTLE"

                    # Send
                    try:
                        await _send_as_own_message(app, target_chat_id, msgs, text)
                        sent_total += 1
                        content_plan_counts[effective_cat] = content_plan_counts.get(effective_cat, 0) + 1
                        db.mark_seen(donor, rep_id, album_id=gid, text_snippet=text,
                                     has_media=has_media, category=effective_cat,
                                     decision="sent", method=method)
                        log.info(f"[buffer] SENT {donor}/{rep_id} cat={effective_cat} method={method} ({sent_total}/{total_limit})")
                        _write_progress("running", text)
                        await asyncio.sleep(1.5)
                    except FloodWait as e:
                        log.warning(f"[buffer] FloodWait {e.value}s")
                        await asyncio.sleep(e.value + 2)
                        errors_total += 1
                        db.mark_seen(donor, rep_id, album_id=gid, text_snippet=text,
                                     has_media=has_media, decision="error",
                                     error_msg=f"FloodWait {e.value}s", method=method)
                    except Exception as e:
                        log.error(f"[buffer] SEND ERROR {donor}/{rep_id}: {e!r}")
                        errors_total += 1
                        db.mark_seen(donor, rep_id, album_id=gid, text_snippet=text,
                                     has_media=has_media, decision="error",
                                     error_msg=repr(e)[:200], method=method)

    _write_progress("done")
    return {"forwarded_count": sent_total, "errors": errors_total}


# ── Tool implementations ───────────────────────────────────────────────────────

def _forward_posts_to_buffer(
    ctx: ToolContext,
    donors: Optional[List[str]] = None,
    total_limit: int = 15,
    only_relevant: bool = True,
    hours_back: int = 26,
    batch_size: int = 5,
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
                total_limit=total_limit,
                only_relevant=only_relevant,
                hours_back=hours_back,
                batch_size=batch_size,
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
    import threading, queue
    result_q: queue.Queue = queue.Queue()

    def _thread_main():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(_async_clear_buffer(target_chat_id))
            result_q.put(("ok", result))
        except Exception as e:
            result_q.put(("err", repr(e)))
        finally:
            loop.close()

    t = threading.Thread(target=_thread_main, daemon=True, name="clear_buffer_thread")
    t.start()
    t.join(timeout=60)
    if t.is_alive():
        return json.dumps({"error": "timeout after 60s"}, ensure_ascii=False)
    status, val = result_q.get()
    if status == "err":
        return json.dumps({"error": val}, ensure_ascii=False)
    return json.dumps(val, ensure_ascii=False)


def _launch_buffer_subprocess(total_posts: int = 70, hours_back: int = 168, batch_size: int = 5) -> int:
    """Launch buffer fill as an independent subprocess. Returns PID."""
    import subprocess, textwrap

    runner_code = textwrap.dedent(f"""
import asyncio, sys, os
sys.path.insert(0, '/home/ouroboros/app/repo')
os.environ['PYTHONPATH'] = '/home/ouroboros/app/repo'

async def main():
    from ouroboros.tools.channel_buffer import _async_forward_posts, DONOR_CHANNELS, BUFFER_CHANNEL_ID
    result = await _async_forward_posts(
        donors=DONOR_CHANNELS,
        target_chat_id=BUFFER_CHANNEL_ID,
        total_limit={total_posts},
        only_relevant=True,
        hours_back={hours_back},
        batch_size={batch_size},
        progress_file='{_BUFFER_PROGRESS_FILE}',
    )
    import json
    with open('{_BUFFER_PROGRESS_FILE}', 'w', encoding='utf-8') as f:
        json.dump({{'status': 'done', 'sent': result['forwarded_count'], 'errors': result['errors']}}, f)
    print(f"Buffer fill complete: {{result['forwarded_count']}} posts sent, {{result['errors']}} errors")

asyncio.run(main())
""")

    with open(_BUFFER_RUNNER_PATH, 'w', encoding='utf-8') as f:
        f.write(runner_code)

    proc = subprocess.Popen(
        ['/home/ouroboros/app/venv/bin/python', _BUFFER_RUNNER_PATH],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return proc.pid


def _init_buffer(ctx: ToolContext, total_posts: int = 70, hours_back: int = 168, batch_size: int = 5) -> str:
    """
    Initialize the buffer channel with posts from donor channels.
    Launches as an independent subprocess — returns immediately, fill happens in background.
    Track progress via check_buffer_progress tool.
    """
    import os

    with open(_BUFFER_PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            'status': 'starting',
            'sent': 0,
            'total': total_posts,
            'current_text': '',
            'started_at': '',
            'pid': 0,
        }, f)

    pid = _launch_buffer_subprocess(total_posts=total_posts, hours_back=hours_back, batch_size=batch_size)

    with open(_BUFFER_PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            'status': 'running',
            'sent': 0,
            'total': total_posts,
            'current_text': 'Starting...',
            'started_at': __import__('datetime').datetime.utcnow().isoformat(),
            'pid': pid,
        }, f)

    return json.dumps({
        'status': 'launched',
        'pid': pid,
        'total_posts': total_posts,
        'hours_back': hours_back,
        'progress_file': _BUFFER_PROGRESS_FILE,
        'message': f'Buffer fill launched as subprocess PID {pid}. Use check_buffer_progress to track.'
    }, ensure_ascii=False)


def _check_buffer_progress(ctx: ToolContext) -> str:
    """Check current status of buffer fill operation."""
    import os

    if not os.path.exists(_BUFFER_PROGRESS_FILE):
        return json.dumps({'status': 'not_started', 'message': 'No buffer fill in progress'})

    try:
        with open(_BUFFER_PROGRESS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        return json.dumps({'status': 'error', 'message': f'Cannot read progress: {e}'})

    pid = data.get('pid', 0)
    if pid:
        try:
            os.kill(pid, 0)
            data['process_alive'] = True
        except (ProcessLookupError, PermissionError):
            data['process_alive'] = False
            if data.get('status') == 'running':
                data['status'] = 'died'

    return json.dumps(data, ensure_ascii=False, indent=2)


# ── Daily scheduler ───────────────────────────────────────────────────────────

def _start_daily_scheduler():
    """Background thread: forward 12-15 posts to buffer at 06:00 and 18:00 MSK."""
    import threading

    def _scheduler_loop():
        MOSCOW_OFFSET = datetime.timezone(datetime.timedelta(hours=3))
        RUN_HOURS = {6, 18}  # 06:00 and 18:00 MSK
        last_run_hour: Optional[int] = None

        log.info("Daily buffer scheduler started (06:00 and 18:00 MSK)")
        while True:
            try:
                now_msk = datetime.datetime.now(MOSCOW_OFFSET)
                current_hour = now_msk.hour
                current_minute = now_msk.minute

                # Run at target hours, minutes 0-4, and only once per hour
                if current_hour in RUN_HOURS and current_minute < 5 and last_run_hour != current_hour:
                    log.info(f"Daily scheduler: starting buffer top-up at {now_msk.strftime('%H:%M')} MSK")
                    last_run_hour = current_hour
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(_async_forward_posts(
                            donors=DONOR_CHANNELS,
                            target_chat_id=BUFFER_CHANNEL_ID,
                            total_limit=15,
                            only_relevant=True,
                            hours_back=26,
                            batch_size=5,
                        ))
                        log.info(f"Daily scheduler completed: {result}")
                    except Exception as e:
                        log.error(f"Daily scheduler error: {e!r}")
                    finally:
                        loop.close()

                # Sleep 60 seconds between checks
                import time
                time.sleep(60)
            except Exception as e:
                log.error(f"Daily scheduler loop error: {e!r}")
                import time
                time.sleep(60)

    t = threading.Thread(target=_scheduler_loop, daemon=True, name="daily_buffer_scheduler")
    t.start()
    log.info("Daily buffer scheduler thread launched")


# ── Registry ───────────────────────────────────────────────────────────────────

def get_tools() -> List[ToolEntry]:
    if not getattr(_start_daily_scheduler, "_started", False):
        _start_daily_scheduler._started = True
        _start_daily_scheduler()
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
            "description": (
                "Fill the buffer channel with ~70 relevant posts from donor channels. "
                "Launches as an independent subprocess — returns immediately. "
                "Track progress with check_buffer_progress."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "total_posts": {
                        "type": "integer",
                        "description": "Total posts to forward (default: 70)",
                    },
                    "hours_back": {
                        "type": "integer",
                        "description": "How many hours back to look for posts (default: 168 = 7 days)",
                    },
                    "batch_size": {
                        "type": "integer",
                        "description": "Posts per fetch chunk (default: 5). Use 5 for debugging, 20 for production.",
                    },
                },
                "required": [],
            },
        }, _init_buffer),

        ToolEntry("check_buffer_progress", {
            "name": "check_buffer_progress",
            "description": "Check the current progress of buffer initialization started by init_buffer. Returns sent count, total, last post, and status (running/done/error).",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        }, _check_buffer_progress),
    ]
