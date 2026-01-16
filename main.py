
from __future__ import annotations

import asyncio
import hashlib
import hmac
import html
import json
import logging
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter, IS_MEMBER, IS_NOT_MEMBER
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    ErrorEvent,
)
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# =============================
# .ENV LOADER (без зависимостей)
# =============================
def _load_env_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip("'").strip('"')
            if not k:
                continue
            # не перетираем реально выставленные env
            os.environ.setdefault(k, v)
    except OSError:
        # молчим: .env не обязан существовать
        return


# Порядок: ENV_FILE -> .env -> etc/filmsbot.env
_env_hint = os.getenv("ENV_FILE", "").strip()
if _env_hint:
    _load_env_file(Path(_env_hint))
else:
    _load_env_file(Path(".env"))
    _load_env_file(Path("etc") / "filmsbot.env")


# =============================
# ENV / CONFIG
# =============================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is empty")

TARGET_CHAT_ID = int(os.getenv("TARGET_CHAT_ID", "0"))
if TARGET_CHAT_ID == 0:
    raise RuntimeError("TARGET_CHAT_ID is empty/0")

DATA_PATH = os.getenv("DATA_PATH", "./data.json").strip()

# Пароль админа: pbkdf2_sha256$iterations$salt_hex$hash_hex
ADMIN_PASS = os.getenv("ADMIN_PASS", "").strip()

ADMIN_ALLOWLIST_RAW = os.getenv("ADMIN_ALLOWLIST", "").strip()
ADMIN_ALLOWLIST: Set[int] = set()
if ADMIN_ALLOWLIST_RAW:
    for part in re.split(r"[,\s]+", ADMIN_ALLOWLIST_RAW):
        part = part.strip()
        if not part:
            continue
        try:
            ADMIN_ALLOWLIST.add(int(part))
        except ValueError:
            continue

# Куда слать аудит/уведомления (можно группой/каналом/ЛС)
AUDIT_CHAT_ID = int(os.getenv("AUDIT_CHAT_ID", "0"))

# Куда слать уведомления о выходе (по умолчанию туда же, куда аудит)
ADMIN_NOTIFY_CHAT_ID = int(os.getenv("ADMIN_NOTIFY_CHAT_ID", str(AUDIT_CHAT_ID or 0)))

# Куда слать ежедневные бэкапы (по умолчанию туда же, куда аудит)
BACKUP_CHAT_ID = int(os.getenv("BACKUP_CHAT_ID", str(AUDIT_CHAT_ID or 0)))

# Если человек ушел и не вернулся за N дней, его банк+роль удаляются полностью (через purge recently_left)
PURGE_AFTER_DAYS = int(os.getenv("PURGE_AFTER_DAYS", "5"))
PURGE_CHECK_EVERY_MIN = int(os.getenv("PURGE_CHECK_EVERY_MIN", "60"))

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "10"))

BACKUP_DIR = os.getenv("BACKUP_DIR", "./backups").strip()
BACKUP_AT = os.getenv("BACKUP_AT", "04:05").strip()  # HH:MM
TZ_NAME = os.getenv("TZ_NAME", "Europe/Moscow").strip()

# Если хочешь еще и в аудит присылать копии бэкапов
SEND_BACKUP_FILE_TO_AUDIT = os.getenv("SEND_BACKUP_FILE_TO_AUDIT", "0").strip() == "1"

ADMIN_SESSION_TTL_HOURS = int(os.getenv("ADMIN_SESSION_TTL_HOURS", "12"))

if TZ_NAME.lower() == "moscow":
    TZ_NAME = "Europe/Moscow"

# =============================
# LOGGER
# =============================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper().strip()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | films-bot | %(message)s",
)
log = logging.getLogger("films-bot")

# =============================
# STORAGE
# =============================
DATA_LOCK = asyncio.Lock()


def utc_now() -> datetime:
    # datetime.utcnow() deprecated -> timezone-aware
    return datetime.now(timezone.utc)


def normalize_name(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def normalize_role(role: str) -> str:
    return normalize_name(role).lower()


def normalize_delta_name(name: str) -> str:
    return re.sub(r"\s+", "", (name or "").strip()).lower()


def esc(s: str) -> str:
    return html.escape(s or "", quote=False)


def user_key(user_id: int) -> str:
    return str(user_id)


def user_link(user_id: int, title: str) -> str:
    """tg://user?id= — это *упоминание* (может пинговать). Используем только там, где это ок."""
    t = normalize_name(title) or "Профиль"
    return f'<a href="tg://user?id={user_id}">{esc(t)}</a>'


def role_html(rec: Dict[str, Any]) -> str:
    """Роль для вывода. Никаких tg:// упоминаний. Только текст + (опционально) внешняя ссылка."""
    role = normalize_name(rec.get("role") or "") or "Без роли"
    url = (rec.get("role_url") or "").strip()
    if not url:
        uname = (rec.get("username") or "").strip()
        if uname:
            url = f"https://t.me/{uname}"

    if url and re.match(r"^https?://", url, flags=re.IGNORECASE):
        return f'<a href="{esc(url)}">{esc(role)}</a>'

    return esc(role)


def public_label(user_id: int, rec: Dict[str, Any]) -> str:
    # В публичных списках НЕ упоминаем людей (иначе Telegram тегает всех подряд).
    return role_html(rec)


def admin_label(user_id: int, rec: Dict[str, Any]) -> str:
    username = (rec.get("username") or "").strip()
    name = normalize_name(rec.get("name") or "Без имени")
    role = normalize_name(rec.get("role") or "")
    base = role if role else (f"@{username}" if username else name)
    if not username:
        base = f"{base} · id:{user_id}"
    return str(base)


def ensure_store_shape(store: Dict[str, Any]) -> Dict[str, Any]:
    store.setdefault("version", 2)
    store.setdefault("users", {})
    store.setdefault("deltas", {})
    store.setdefault("recently_left", {})
    store.setdefault("daily_riddle", None)  # или dict
    return store


def _read_store_file() -> Dict[str, Any]:
    path = Path(DATA_PATH)
    if not path.exists():
        return ensure_store_shape({})
    raw = path.read_text(encoding="utf-8")
    if not raw.strip():
        return ensure_store_shape({})
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        return ensure_store_shape({})
    return ensure_store_shape(parsed)


def _write_store_file(store: Dict[str, Any]) -> None:
    path = Path(DATA_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


async def load_store() -> Dict[str, Any]:
    async with DATA_LOCK:
        try:
            return _read_store_file()
        except (OSError, json.JSONDecodeError, ValueError) as e:
            log.exception("Failed to load data.json: %s", e)
            return ensure_store_shape({})


async def save_store(store: Dict[str, Any]) -> None:
    async with DATA_LOCK:
        try:
            _write_store_file(store)
        except OSError as e:
            log.exception("Failed to save data.json: %s", e)


# =============================
# PASSWORD (PBKDF2)
# =============================
def verify_admin_password(plain: str, stored: str) -> bool:
    if not stored or not plain:
        return False

    if not stored.startswith("pbkdf2_sha256$"):
        return hmac.compare_digest(plain, stored)

    parts = stored.split("$")
    if len(parts) != 4:
        return False

    _, iters_s, salt_hex, hash_hex = parts
    try:
        iterations = int(iters_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
    except (ValueError, TypeError):
        return False

    dk = hashlib.pbkdf2_hmac("sha256", plain.encode("utf-8"), salt, iterations, dklen=len(expected))
    return hmac.compare_digest(dk, expected)


# =============================
# ADMIN SESSIONS
# =============================
admin_sessions: Dict[int, datetime] = {}  # user_id -> expires_utc


def is_admin_allowlisted(user_id: int) -> bool:
    # Если allowlist пустой, считаем, что "админы" определяются паролем (как раньше).
    return (not ADMIN_ALLOWLIST) or (user_id in ADMIN_ALLOWLIST)


def is_admin_session(user_id: int) -> bool:
    exp = admin_sessions.get(user_id)
    if not exp:
        return False
    if utc_now() >= exp:
        admin_sessions.pop(user_id, None)
        return False
    return True


def require_admin(user_id: int) -> bool:
    if not is_admin_allowlisted(user_id):
        return False
    return is_admin_session(user_id)


# =============================
# KEYBOARDS
# =============================
ADMIN_BUTTON_TEXTS: Set[str] = {
    "Назначить роль",
    "Сменить роль",
    "Выдать плёнки",
    "Отнять плёнки",
    "Создать сокращение",
    "Удалить сокращение",
    "Загадки(дейлики)",
    "Баланс по ролям",
    "Ping",
    "Logout",
}

def admin_reply_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Назначить роль"), KeyboardButton(text="Сменить роль")],
        [KeyboardButton(text="Выдать плёнки"), KeyboardButton(text="Отнять плёнки")],
        [KeyboardButton(text="Создать сокращение"), KeyboardButton(text="Удалить сокращение")],
        [KeyboardButton(text="Загадки(дейлики)"), KeyboardButton(text="Баланс по ролям")],
        [KeyboardButton(text="Ping"), KeyboardButton(text="Logout")],
    ]
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=True,
        input_field_placeholder="Админ-команды",
    )


def inline_nav(page: int, total_pages: int, prefix: str, extra: str = "") -> List[List[InlineKeyboardButton]]:
    btns: List[InlineKeyboardButton] = []
    if page > 0:
        btns.append(InlineKeyboardButton(text="⬅️", callback_data=f"{prefix}:page:{page - 1}{extra}"))
    btns.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        btns.append(InlineKeyboardButton(text="➡️", callback_data=f"{prefix}:page:{page + 1}{extra}"))
    return [btns] if btns else []


# =============================
# FSM
# =============================
class AdminStates(StatesGroup):
    set_role_pick = State()
    set_role_enter = State()

    change_role_pick = State()
    change_role_enter = State()

    give_pick = State()
    give_amount = State()

    take_pick = State()
    take_amount = State()

    delta_value = State()
    delta_name = State()

    delta_delete_pick = State()

    riddle_text = State()
    riddle_answer = State()
    riddle_reward = State()
    riddle_winners = State()


# =============================
# ROUTER
# =============================
router = Router()


# =============================
# AUDIT
# =============================
def link_from_user(u) -> str:
    if not u:
        return "unknown"
    title = normalize_name(u.full_name) or (f"@{u.username}" if u.username else str(u.id))
    return user_link(u.id, title)


async def send_audit(bot: Bot, text: str, file_path: Optional[str] = None) -> None:
    if AUDIT_CHAT_ID == 0:
        return
    try:
        await bot.send_message(
            chat_id=AUDIT_CHAT_ID,
            text=text,
            disable_web_page_preview=True,
        )
        if file_path:
            await bot.send_document(chat_id=AUDIT_CHAT_ID, document=FSInputFile(file_path))
    except Exception as e:
        log.warning("Audit send failed: %s", e)


async def notify_admin(bot: Bot, text: str) -> None:
    if ADMIN_NOTIFY_CHAT_ID == 0:
        return
    try:
        await bot.send_message(chat_id=ADMIN_NOTIFY_CHAT_ID, text=text, disable_web_page_preview=True)
    except Exception as e:
        log.warning("notify_admin failed: %s", e)


# =============================
# GROUP HELPERS
# =============================
def is_target_group(msg: Message) -> bool:
    return (
        msg.chat is not None
        and msg.chat.id == TARGET_CHAT_ID
        and msg.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP)
    )


def extract_text_any(msg: Message) -> str:
    return (msg.text or msg.caption or "").strip()



def parse_role_input(msg: Message) -> Tuple[str, Optional[str]]:
    """Парсим роль из сообщения админа.
    Если админ прислал 'текст со ссылкой' (entity=text_link на весь текст) — сохраняем URL отдельно.
    Это даёт кликабельную роль без tg:// упоминаний (и без массовых тегов в списках).
    """
    raw = (msg.text or "").strip()
    role_text = normalize_name(raw)
    role_url: Optional[str] = None

    if raw and msg.entities:
        for ent in msg.entities:
            # text_link entity: скрытая ссылка, которую в Telegram добавляют через "Вставить ссылку"
            if getattr(ent, "type", None) == "text_link" and int(getattr(ent, "offset", 0)) == 0 and int(getattr(ent, "length", 0)) == len(raw):
                role_url = (getattr(ent, "url", None) or "").strip()
                break

    if role_url and not re.match(r"^https?://", role_url, flags=re.IGNORECASE):
        role_url = None

    return role_text, role_url


# =============================
# USERS: UPSERT / LEAVE / RESTORE / PURGE
# =============================
def upsert_user_from_tg(store: Dict[str, Any], user) -> bool:
    """
    Возвращает True если юзер записан/обновлён.
    Ботов намертво игнорируем, чтобы они вообще нигде не существовали в валюте.
    """
    if not user or getattr(user, "is_bot", False):
        return False

    uid = int(user.id)
    k = user_key(uid)
    users = store["users"]
    rec = users.get(k) or {}
    rec["username"] = user.username
    rec["name"] = user.full_name
    rec.setdefault("role", None)
    rec.setdefault("role_url", None)
    rec.setdefault("films", 0)
    rec["updated_at"] = utc_now().isoformat()
    rec.setdefault("joined_at", utc_now().isoformat())
    users[k] = rec
    return True


def restore_if_recently_left(store: Dict[str, Any], user) -> bool:
    if not user or getattr(user, "is_bot", False):
        return False

    k = user_key(user.id)
    left = store["recently_left"].get(k)
    if not left:
        return False

    until_raw = left.get("restore_until")
    if not isinstance(until_raw, str):
        store["recently_left"].pop(k, None)
        return False

    try:
        until = datetime.fromisoformat(until_raw)
    except ValueError:
        store["recently_left"].pop(k, None)
        return False

    if utc_now() > until:
        store["recently_left"].pop(k, None)
        return False

    store["recently_left"].pop(k, None)
    store["users"][k] = {
        "username": user.username,
        "name": user.full_name,
        "role": left.get("role"),
        "role_url": left.get("role_url"),
        "films": int(left.get("films", 0)),
        "joined_at": utc_now().isoformat(),
        "updated_at": utc_now().isoformat(),
    }
    return True


def move_to_recently_left(store: Dict[str, Any], user_id: int) -> None:
    k = user_key(user_id)
    rec = store["users"].pop(k, None)
    if not rec:
        return

    until = utc_now() + timedelta(days=PURGE_AFTER_DAYS)
    store["recently_left"][k] = {
        "username": rec.get("username"),
        "name": rec.get("name"),
        "role": rec.get("role"),
        "role_url": rec.get("role_url"),
        "films": int(rec.get("films", 0)),
        "left_at": utc_now().isoformat(),
        "restore_until": until.isoformat(),
    }


def purge_expired_recently_left(store: Dict[str, Any]) -> int:
    now = utc_now()
    rl = store.get("recently_left", {})
    if not isinstance(rl, dict) or not rl:
        return 0

    to_delete: List[str] = []
    for k, v in rl.items():
        until_raw = (v or {}).get("restore_until")
        if not isinstance(until_raw, str):
            to_delete.append(k)
            continue
        try:
            until = datetime.fromisoformat(until_raw)
        except ValueError:
            to_delete.append(k)
            continue
        if now > until:
            to_delete.append(k)

    for k in to_delete:
        rl.pop(k, None)

    return len(to_delete)


async def purge_loop(bot: Bot) -> None:
    while True:
        try:
            store = await load_store()
            n = purge_expired_recently_left(store)
            if n:
                await save_store(store)
                log.info("Purged %s expired users from recently_left", n)
                await send_audit(bot, f"🧹 <b>purge</b>: удалено записей из recently_left: {n}")
        except Exception as e:
            log.exception("purge_loop error: %s", e)

        await asyncio.sleep(PURGE_CHECK_EVERY_MIN * 60)


@router.chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
async def on_join(event: ChatMemberUpdated):
    if event.chat.id != TARGET_CHAT_ID:
        return
    u = event.new_chat_member.user
    if not u or u.is_bot:
        return

    store = await load_store()
    restored = restore_if_recently_left(store, u)
    if not restored:
        upsert_user_from_tg(store, u)
    await save_store(store)


@router.chat_member(ChatMemberUpdatedFilter(IS_MEMBER >> IS_NOT_MEMBER))
async def on_leave(event: ChatMemberUpdated, bot: Bot):
    if event.chat.id != TARGET_CHAT_ID:
        return
    u = event.old_chat_member.user
    if not u or u.is_bot:
        return

    store = await load_store()
    # достанем роль/username до переноса
    rec = (store.get("users", {}) or {}).get(user_key(u.id), {}) if isinstance(store.get("users"), dict) else {}
    role = str(rec.get("role") or "Без роли")
    username = (u.username or "").strip()
    uname_part = f"@{username}" if username else f"id:{u.id}"

    move_to_recently_left(store, u.id)
    await save_store(store)

    # 1) Уведомление админу в ЛС/чат
    await notify_admin(bot, f"#Выход — {role} ({uname_part})")


# =============================
# USER COMMANDS (!)
# =============================
@router.message(F.text.regexp(r"^!(пленки|плёнки|мои\s+пленки|мои\s+плёнки)\b", flags=re.IGNORECASE))
async def my_films(msg: Message):
    if not is_target_group(msg):
        return
    u = msg.from_user
    if not u or u.is_bot:
        return

    store = await load_store()
    upsert_user_from_tg(store, u)
    await save_store(store)

    rec = store["users"].get(user_key(u.id), {})
    bal = int(rec.get("films", 0))
    if bal <= 0:
        await msg.reply("У вас еще нет плёнок.", disable_web_page_preview=True)
    else:
        await msg.reply(f"У вас: {bal}🎞️", disable_web_page_preview=True)


def _strip_their_films_query(text: str) -> str:
    t = (text or "").strip()
    t = re.sub(r"^!твои\s+(пленки|плёнки)\b", "", t, flags=re.IGNORECASE).strip()
    return t


def find_user_by_query(store: Dict[str, Any], query: str) -> Optional[Tuple[int, Dict[str, Any]]]:
    q = normalize_name(query)
    if not q:
        return None

    users = store.get("users", {})
    if not isinstance(users, dict):
        return None

    if q.startswith("@"):
        uname = q[1:]
        for k, rec in users.items():
            if (rec.get("username") or "").lower() == uname.lower():
                return int(k), rec
        return None

    nr = normalize_role(q)
    for k, rec in users.items():
        r = rec.get("role")
        if r and normalize_role(str(r)) == nr:
            return int(k), rec

    nq = normalize_name(q).lower()
    exact: List[Tuple[int, Dict[str, Any]]] = []
    contains: List[Tuple[int, Dict[str, Any]]] = []
    for k, rec in users.items():
        name = normalize_name(rec.get("name") or "").lower()
        if not name:
            continue
        if name == nq:
            exact.append((int(k), rec))
        elif nq in name:
            contains.append((int(k), rec))

    if exact:
        return exact[0]
    if contains:
        return contains[0]
    return None


@router.message(F.text.regexp(r"^!твои\s+(пленки|плёнки)\b", flags=re.IGNORECASE))
async def their_films(msg: Message):
    if not is_target_group(msg):
        return

    # 2) Если пытаются применить к боту через reply
    if msg.reply_to_message and msg.reply_to_message.from_user and msg.reply_to_message.from_user.is_bot:
        await msg.reply("Совсем ебанулся? Он же бот", disable_web_page_preview=True)
        return

    store = await load_store()
    target_id: Optional[int] = None
    target_rec: Optional[Dict[str, Any]] = None

    if msg.reply_to_message and msg.reply_to_message.from_user:
        tu = msg.reply_to_message.from_user
        if tu.is_bot:
            await msg.reply("Совсем ебанулся? Он же бот", disable_web_page_preview=True)
            return
        target_id = tu.id
        upsert_user_from_tg(store, tu)
        target_rec = store["users"].get(user_key(tu.id))

    if target_id is None:
        tail = _strip_their_films_query(msg.text or "")
        if tail:
            found = find_user_by_query(store, tail)
            if found:
                target_id, target_rec = found

    if target_id is None or target_rec is None:
        await msg.reply("Нужно ответить на сообщение человека или указать роль/@username/имя после команды.", disable_web_page_preview=True)
        return

    await save_store(store)

    bal = int(target_rec.get("films", 0))
    who = public_label(target_id, target_rec)
    await msg.reply(f"{who}: {bal}🎞️", disable_web_page_preview=True)


def build_leaderboard_items(store: Dict[str, Any]) -> List[Tuple[int, int, Dict[str, Any]]]:
    users = store.get("users", {})
    if not isinstance(users, dict):
        return []

    items: List[Tuple[int, int, Dict[str, Any]]] = []
    for k, rec in users.items():
        try:
            uid = int(k)
        except ValueError:
            continue
        films = int(rec.get("films", 0))
        items.append((films, uid, rec))
    items.sort(key=lambda x: (x[0], normalize_name(x[2].get("name", "")).lower()), reverse=True)
    return items


def render_leaderboard_page(
    items: List[Tuple[int, int, Dict[str, Any]]],
    page: int,
    per_page: int,
    limit: Optional[int],
) -> Tuple[str, int, int]:
    if limit is not None:
        items = items[:limit]
    total = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))

    start_idx = page * per_page
    chunk = items[start_idx:start_idx + per_page]

    lines: List[str] = []
    rank_start = start_idx + 1
    for i, (films, uid, rec) in enumerate(chunk, start=rank_start):
        who = public_label(uid, rec)  # роль как кликабельный текст
        lines.append(f"{i}. {who} — {films}🎞️")

    title = "Список участников по плёнкам (↓):"
    if limit is not None:
        title = f"Топ {limit} участников (↓):"
    text = title + "\n" + ("\n".join(lines) if lines else "Пока пусто.")
    return text, page, total_pages


@router.message(F.text.regexp(r"^!список(\s+\d+)?\s*$", flags=re.IGNORECASE))
async def leaderboard(msg: Message):
    if not is_target_group(msg):
        return

    store = await load_store()
    items = build_leaderboard_items(store)

    limit: Optional[int] = None
    parts = (msg.text or "").split()
    if len(parts) >= 2:
        try:
            limit = max(1, min(200, int(parts[1])))
        except ValueError:
            limit = None

    page = 0
    text, page, total_pages = render_leaderboard_page(items, page, PAGE_SIZE, limit)
    extra = f":limit:{limit}" if limit is not None else ""
    kb_rows = inline_nav(page, total_pages, "lb", extra=extra)
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None
    await msg.reply(text, reply_markup=kb, disable_web_page_preview=True)


@router.callback_query(F.data.startswith("lb:page:"))
async def leaderboard_page(cb: CallbackQuery):
    if not cb.message:
        return
    if cb.message.chat.id != TARGET_CHAT_ID:
        await cb.answer()
        return

    store = await load_store()
    items = build_leaderboard_items(store)

    parts = cb.data.split(":")
    try:
        page = int(parts[2])
    except ValueError:
        await cb.answer()
        return

    limit: Optional[int] = None
    if len(parts) >= 5 and parts[3] == "limit":
        try:
            limit = int(parts[4])
        except ValueError:
            limit = None

    text, page, total_pages = render_leaderboard_page(items, page, PAGE_SIZE, limit)
    extra = f":limit:{limit}" if limit is not None else ""
    kb_rows = inline_nav(page, total_pages, "lb", extra=extra)
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None

    try:
        await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        pass
    await cb.answer()


# =============================
# /PING
# =============================
@router.message(Command("ping"))
async def ping(msg: Message):
    await msg.reply("✅ жив", disable_web_page_preview=True)


@router.message(Command("myid"))
async def myid(msg: Message):
    if msg.chat.type != ChatType.PRIVATE or not msg.from_user:
        return
    await msg.reply(f"Ваш id: <code>{msg.from_user.id}</code>", disable_web_page_preview=True)


# =============================
# ADMIN GUARDS (PRIVATE)
# =============================
def _admin_denied_text(user_id: int) -> str:
    if not is_admin_allowlisted(user_id):
        return "иди нахуй отсюда, плебей"
    if not is_admin_session(user_id):
        return "Сначала залогинься"
    return "Доступ закрыт"


async def ensure_admin_msg(msg: Message) -> bool:
    """Проверка админки для ЛС. Если нельзя — отвечаем понятным текстом."""
    if msg.chat.type != ChatType.PRIVATE or not msg.from_user:
        return False
    if require_admin(msg.from_user.id):
        return True

    deny = _admin_denied_text(msg.from_user.id)
    await msg.reply(deny, reply_markup=ReplyKeyboardRemove(), disable_web_page_preview=True)
    return False


async def ensure_admin_cb(cb: CallbackQuery) -> bool:
    """Проверка админки для callback в ЛС. Если нельзя — alert."""
    if not cb.from_user or not cb.message or cb.message.chat.type != ChatType.PRIVATE:
        try:
            await cb.answer()
        except Exception:
            pass
        return False

    if require_admin(cb.from_user.id):
        return True

    deny = _admin_denied_text(cb.from_user.id)
    try:
        await cb.answer(deny, show_alert=True)
    except Exception:
        pass
    return False


def admin_only_guard(msg: Message) -> bool:
    return msg.chat.type == ChatType.PRIVATE and msg.from_user is not None and require_admin(msg.from_user.id)


def admin_only_cb_guard(cb: CallbackQuery) -> bool:
    return cb.message is not None and cb.message.chat.type == ChatType.PRIVATE and cb.from_user is not None and require_admin(cb.from_user.id)



# =============================
# /START /LOGIN /LOGOUT (PRIVATE)
# =============================
@router.message(Command("start"))
async def start_cmd(msg: Message):
    if msg.chat.type != ChatType.PRIVATE:
        return
    u = msg.from_user
    if not u:
        return

    if require_admin(u.id):
        await msg.reply("Админ-панель:", reply_markup=admin_reply_kb(), disable_web_page_preview=True)
        return

    text = "Привет. Я считаю плёнки.\nАдминка: /login пароль (в ЛС)."
    await msg.reply(text, reply_markup=ReplyKeyboardRemove(), disable_web_page_preview=True)


@router.message(Command("login"))
async def login(msg: Message, command: CommandObject, bot: Bot):
    if msg.chat.type != ChatType.PRIVATE:
        return
    u = msg.from_user
    if not u:
        return

    # Если allowlist задан, чужие идут лесом
    if ADMIN_ALLOWLIST and u.id not in ADMIN_ALLOWLIST:
        await msg.reply("иди нахуй отсюда, плебей", disable_web_page_preview=True)
        return

    pwd = (command.args or "").strip()
    if not verify_admin_password(pwd, ADMIN_PASS):
        await msg.reply("Пароль мимо.", disable_web_page_preview=True)
        return

    admin_sessions[u.id] = utc_now() + timedelta(hours=ADMIN_SESSION_TTL_HOURS)
    await msg.reply("Ок, ты админ. Держи панель.", reply_markup=admin_reply_kb(), disable_web_page_preview=True)
    await send_audit(bot, f"🔐 <b>Login</b>: {link_from_user(u)}")


@router.message(Command("logout"))
async def logout(msg: Message, bot: Bot):
    if msg.chat.type != ChatType.PRIVATE:
        return
    u = msg.from_user
    if not u:
        return
    admin_sessions.pop(u.id, None)
    await msg.reply("Вышел.", reply_markup=ReplyKeyboardRemove(), disable_web_page_preview=True)
    await send_audit(bot, f"🔓 <b>Logout</b>: {link_from_user(u)}")


@router.message(F.text == "Logout")
async def logout_btn(msg: Message, bot: Bot):
    await logout(msg, bot)


# =============================
# ADMIN: PING BTN
# =============================
@router.message(F.text == "Ping")
async def admin_ping_btn(msg: Message):
    if not await ensure_admin_msg(msg):
        return
    await msg.reply("✅ жив", disable_web_page_preview=True)


# =============================
# ADMIN: BALANCES BY ROLE
# =============================
@router.message(F.text == "Баланс по ролям")
async def balances_by_role(msg: Message):
    if not await ensure_admin_msg(msg):
        return

    store = await load_store()
    users = store.get("users", {})
    if not isinstance(users, dict):
        await msg.reply("Пока нет ролей.", disable_web_page_preview=True)
        return

    items: List[Tuple[int, int, Dict[str, Any]]] = []
    for k, rec in users.items():
        role = rec.get("role")
        if not role:
            continue
        try:
            uid = int(k)
        except ValueError:
            continue
        films = int(rec.get("films", 0))
        items.append((films, uid, rec))

    items.sort(key=lambda x: (x[0], normalize_role(str(x[2].get("role", "")))), reverse=True)

    if not items:
        await msg.reply("Пока нет ролей.", disable_web_page_preview=True)
        return

    lines = [f"{public_label(uid, rec)} — {films}🎞️" for films, uid, rec in items]
    await msg.reply("Баланс по ролям:\n" + "\n".join(lines), disable_web_page_preview=True)


# =============================
# ADMIN: ROLE HELPERS
# =============================
def collect_roles(store: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    users = store.get("users", {})
    if not isinstance(users, dict):
        return out
    for rec in users.values():
        r = rec.get("role")
        if r:
            out[normalize_role(str(r))] = str(r)
    return out


def role_exists(store: Dict[str, Any], role: str) -> bool:
    return normalize_role(role) in collect_roles(store)


def delta_exists(store: Dict[str, Any], name: str) -> bool:
    nn = normalize_delta_name(name)
    deltas = store.get("deltas", {})
    if not isinstance(deltas, dict):
        return False
    for k in deltas.keys():
        if normalize_delta_name(str(k)) == nn:
            return True
    return False


def paginate(items: List[Any], page: int, per_page: int) -> Tuple[List[Any], int, int]:
    total = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    start_idx = page * per_page
    return items[start_idx:start_idx + per_page], page, total_pages


# =============================
# ADMIN: SET ROLE
# =============================
def kb_pick_users_no_role(store: Dict[str, Any], page: int) -> InlineKeyboardMarkup:
    users = store.get("users", {})
    pool: List[Tuple[str, Dict[str, Any]]] = []
    if isinstance(users, dict):
        for k, rec in users.items():
            if rec.get("role"):
                continue
            pool.append((k, rec))

    pool.sort(key=lambda x: normalize_name(x[1].get("name", "")).lower())
    chunk, page, total_pages = paginate(pool, page, PAGE_SIZE)

    rows: List[List[InlineKeyboardButton]] = []
    for uid_str, rec in chunk:
        label = admin_label(int(uid_str), rec)
        rows.append([InlineKeyboardButton(text=label, callback_data=f"setr:pick:{uid_str}")])

    rows += inline_nav(page, total_pages, "setr")
    rows.append([InlineKeyboardButton(text="↩ Назад", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(F.text == "Назначить роль")
async def set_role_start(msg: Message, state: FSMContext):
    if not await ensure_admin_msg(msg):
        return
    store = await load_store()
    await state.clear()
    await state.set_state(AdminStates.set_role_pick)
    await msg.reply("Выбери участника без роли:", reply_markup=kb_pick_users_no_role(store, page=0), disable_web_page_preview=True)


@router.callback_query(F.data.startswith("setr:page:"))
async def set_role_page(cb: CallbackQuery, state: FSMContext):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != AdminStates.set_role_pick.state:
        await cb.answer()
        return
    store = await load_store()
    page = int(cb.data.split(":")[2])
    await cb.message.edit_reply_markup(reply_markup=kb_pick_users_no_role(store, page))
    await cb.answer()


@router.callback_query(F.data.startswith("setr:pick:"))
async def set_role_pick(cb: CallbackQuery, state: FSMContext):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != AdminStates.set_role_pick.state:
        await cb.answer()
        return

    user_id_str = cb.data.split(":")[2]
    store = await load_store()
    rec = (store.get("users", {}) or {}).get(user_id_str)
    if not rec:
        await cb.answer("Юзер пропал.")
        return

    await state.update_data(target_user_id=user_id_str)
    await state.set_state(AdminStates.set_role_enter)

    who = admin_label(int(user_id_str), rec)
    await cb.message.edit_text(f"Ок. Напиши роль текстом (до 64 символов) для {esc(who)}:", disable_web_page_preview=True)
    await cb.answer("Ожидаю роль", show_alert=False)


@router.message(AdminStates.set_role_enter)
async def set_role_enter(msg: Message, state: FSMContext, bot: Bot):
    if not await ensure_admin_msg(msg):
        return

    role, role_url = parse_role_input(msg)
    if not role or len(role) > 64:
        await msg.reply("Роль должна быть 1..64 символа.", disable_web_page_preview=True)
        return

    store = await load_store()
    if role_exists(store, role):
        await msg.reply("Такая роль уже есть (регистр не спасёт).", disable_web_page_preview=True)
        return

    st = await state.get_data()
    uid = st.get("target_user_id")
    users = store.get("users", {})
    if not isinstance(users, dict) or not uid or uid not in users:
        await msg.reply("Цель потерялась.", disable_web_page_preview=True)
        await state.clear()
        return

    users[uid]["role"] = role
    users[uid]["role_url"] = role_url
    users[uid]["updated_at"] = utc_now().isoformat()
    await save_store(store)

    rec = users[uid]
    who_admin = admin_label(int(uid), rec)
    await msg.reply(f"Готово: {esc(role)} назначена для {esc(who_admin)}.", disable_web_page_preview=True)
    await send_audit(bot, f"🧩 <b>set_role</b>: {link_from_user(msg.from_user)} -> {user_link(int(uid), role)}")
    await state.clear()


# =============================
# ADMIN: CHANGE ROLE
# =============================
def kb_pick_users_with_role(store: Dict[str, Any], page: int) -> InlineKeyboardMarkup:
    users = store.get("users", {})
    pool: List[Tuple[str, Dict[str, Any]]] = []
    if isinstance(users, dict):
        for k, rec in users.items():
            if not rec.get("role"):
                continue
            pool.append((k, rec))

    pool.sort(key=lambda x: normalize_role(str(x[1].get("role", ""))))
    chunk, page, total_pages = paginate(pool, page, PAGE_SIZE)

    rows: List[List[InlineKeyboardButton]] = []
    for uid_str, rec in chunk:
        role = str(rec.get("role"))
        rows.append([InlineKeyboardButton(text=role, callback_data=f"chr:pick:{uid_str}")])

    rows += inline_nav(page, total_pages, "chr")
    rows.append([InlineKeyboardButton(text="↩ Назад", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(F.text == "Сменить роль")
async def change_role_start(msg: Message, state: FSMContext):
    if not await ensure_admin_msg(msg):
        return
    store = await load_store()
    await state.clear()
    await state.set_state(AdminStates.change_role_pick)
    await msg.reply("Выбери роль (кнопкой):", reply_markup=kb_pick_users_with_role(store, page=0), disable_web_page_preview=True)


@router.callback_query(F.data.startswith("chr:page:"))
async def change_role_page(cb: CallbackQuery, state: FSMContext):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != AdminStates.change_role_pick.state:
        await cb.answer()
        return
    store = await load_store()
    page = int(cb.data.split(":")[2])
    await cb.message.edit_reply_markup(reply_markup=kb_pick_users_with_role(store, page))
    await cb.answer()


@router.callback_query(F.data.startswith("chr:pick:"))
async def change_role_pick(cb: CallbackQuery, state: FSMContext):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != AdminStates.change_role_pick.state:
        await cb.answer()
        return

    uid = cb.data.split(":")[2]
    store = await load_store()
    users = store.get("users", {})
    rec = (users or {}).get(uid) if isinstance(users, dict) else None
    if not rec or not rec.get("role"):
        await cb.answer("Роль пропала.")
        return

    await state.update_data(target_user_id=uid, old_role=rec.get("role"))
    await state.set_state(AdminStates.change_role_enter)

    await cb.message.edit_text(f"Напиши новую роль (до 64 символов). Старая: {esc(str(rec.get('role')))}", disable_web_page_preview=True)
    await cb.answer("Ожидаю новую роль", show_alert=False)


@router.message(AdminStates.change_role_enter)
async def change_role_enter(msg: Message, state: FSMContext, bot: Bot):
    if not await ensure_admin_msg(msg):
        return

    role, role_url = parse_role_input(msg)
    if not role or len(role) > 64:
        await msg.reply("Роль должна быть 1..64 символа.", disable_web_page_preview=True)
        return

    st = await state.get_data()
    uid = st.get("target_user_id")
    old_role = st.get("old_role")

    store = await load_store()
    users = store.get("users", {})
    if not isinstance(users, dict) or not uid or uid not in users:
        await msg.reply("Цель потерялась.", disable_web_page_preview=True)
        await state.clear()
        return

    if role_exists(store, role) and normalize_role(role) != normalize_role(str(old_role or "")):
        await msg.reply("Такая роль уже существует (регистр не спасёт).", disable_web_page_preview=True)
        return

    users[uid]["role"] = role
    users[uid]["role_url"] = role_url
    users[uid]["updated_at"] = utc_now().isoformat()
    await save_store(store)

    rec = users[uid]
    who_admin = admin_label(int(uid), rec)
    await msg.reply(f"Готово: роль у {esc(who_admin)} теперь {esc(role)}. Баланс сохранён.", disable_web_page_preview=True)
    await send_audit(
        bot,
        f"🧩 <b>change_role</b>: {link_from_user(msg.from_user)} -> {user_link(int(uid), str(old_role or 'Без роли'))} → {user_link(int(uid), role)}",
    )
    await state.clear()


# =============================
# ADMIN: DELTAS (CREATE / DELETE)
# =============================
@router.message(F.text == "Создать сокращение")
async def delta_create_start(msg: Message, state: FSMContext):
    if not await ensure_admin_msg(msg):
        return
    await state.clear()
    await state.set_state(AdminStates.delta_value)
    await msg.reply("Введи целое число для дельты (например 10):", disable_web_page_preview=True)


@router.message(AdminStates.delta_value)
async def delta_create_value(msg: Message, state: FSMContext):
    if not await ensure_admin_msg(msg):
        return
    try:
        val = int((msg.text or "").strip())
    except ValueError:
        await msg.reply("Нужно целое число.", disable_web_page_preview=True)
        return
    if val == 0:
        await msg.reply("0 смысла не имеет.", disable_web_page_preview=True)
        return
    await state.update_data(delta_value=val)
    await state.set_state(AdminStates.delta_name)
    await msg.reply("Теперь напиши название (без пробелов), например: boost10", disable_web_page_preview=True)


@router.message(AdminStates.delta_name)
async def delta_create_name(msg: Message, state: FSMContext, bot: Bot):
    if not await ensure_admin_msg(msg):
        return
    name = (msg.text or "").strip()
    if not name or " " in name or len(name) > 32:
        await msg.reply("Название: 1..32 символа, без пробелов.", disable_web_page_preview=True)
        return

    store = await load_store()
    if delta_exists(store, name):
        await msg.reply("Такое сокращение уже есть (регистр не спасёт).", disable_web_page_preview=True)
        return

    st = await state.get_data()
    val = int(st.get("delta_value", 0))

    deltas = store.get("deltas", {})
    if not isinstance(deltas, dict):
        store["deltas"] = {}
        deltas = store["deltas"]

    deltas[name] = val
    await save_store(store)

    await msg.reply(f"Создано: {esc(name)} = {val:+d}", disable_web_page_preview=True)
    await send_audit(bot, f"➕ <b>delta_add</b>: {link_from_user(msg.from_user)} -> {esc(name)}={val:+d}")
    await state.clear()


def kb_pick_delta(store: Dict[str, Any], page: int) -> InlineKeyboardMarkup:
    deltas = store.get("deltas", {})
    items: List[Tuple[str, int]] = []
    if isinstance(deltas, dict):
        for k, v in deltas.items():
            try:
                items.append((str(k), int(v)))
            except (ValueError, TypeError):
                continue

    items.sort(key=lambda x: normalize_delta_name(x[0]))
    chunk, page, total_pages = paginate(items, page, PAGE_SIZE)

    rows: List[List[InlineKeyboardButton]] = []
    for name, val in chunk:
        rows.append([InlineKeyboardButton(text=f"{name} = {val:+d}", callback_data=f"ddel:pick:{name}")])
    rows += inline_nav(page, total_pages, "ddel")
    rows.append([InlineKeyboardButton(text="↩ Назад", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(F.text == "Удалить сокращение")
async def delta_delete_start(msg: Message, state: FSMContext):
    if not await ensure_admin_msg(msg):
        return
    store = await load_store()
    deltas = store.get("deltas", {})
    if not isinstance(deltas, dict) or not deltas:
        await msg.reply("Сокращений нет.", disable_web_page_preview=True)
        return
    await state.clear()
    await state.set_state(AdminStates.delta_delete_pick)
    await msg.reply("Выбери сокращение для удаления:", reply_markup=kb_pick_delta(store, 0), disable_web_page_preview=True)


@router.callback_query(F.data.startswith("ddel:page:"))
async def delta_delete_page(cb: CallbackQuery, state: FSMContext):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != AdminStates.delta_delete_pick.state:
        await cb.answer()
        return
    store = await load_store()
    page = int(cb.data.split(":")[2])
    await cb.message.edit_reply_markup(reply_markup=kb_pick_delta(store, page))
    await cb.answer()


@router.callback_query(F.data.startswith("ddel:pick:"))
async def delta_delete_pick(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != AdminStates.delta_delete_pick.state:
        await cb.answer()
        return

    name = cb.data.split(":")[2]
    store = await load_store()
    deltas = store.get("deltas", {})
    if not isinstance(deltas, dict) or name not in deltas:
        await cb.answer("Уже нет.")
        return

    try:
        val = int(deltas.pop(name))
    except (ValueError, TypeError):
        val = 0
        deltas.pop(name, None)

    await save_store(store)

    await cb.message.edit_text(f"Удалено: {esc(name)} (было {val:+d})", disable_web_page_preview=True)
    await send_audit(bot, f"➖ <b>delta_del</b>: {link_from_user(cb.from_user)} -> {esc(name)}={val:+d}")
    await cb.answer("Удалено", show_alert=False)
    await state.clear()


# =============================
# ADMIN: MULTI GIVE/TAKE
# =============================
def list_users_for_money(store: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    users = store.get("users", {})
    if not isinstance(users, dict):
        return []
    tmp: List[Tuple[str, str, Dict[str, Any]]] = []
    for uid_str, rec in users.items():
        key = normalize_role(str(rec.get("role") or "~~~")) + "|" + normalize_name(rec.get("name", "")).lower()
        tmp.append((key, uid_str, rec))
    tmp.sort(key=lambda x: x[0])
    return [(uid_str, rec) for _, uid_str, rec in tmp]


def kb_multi_pick(store: Dict[str, Any], selected: Set[str], page: int, prefix: str) -> InlineKeyboardMarkup:
    users = list_users_for_money(store)
    chunk, page, total_pages = paginate(users, page, PAGE_SIZE)

    rows: List[List[InlineKeyboardButton]] = []
    for uid_str, rec in chunk:
        role = rec.get("role")
        label = str(role) if role else admin_label(int(uid_str), rec)
        checked = "✅ " if uid_str in selected else ""
        rows.append([InlineKeyboardButton(text=checked + label, callback_data=f"{prefix}:toggle:{uid_str}")])

    rows += inline_nav(page, total_pages, prefix)
    rows.append([
        InlineKeyboardButton(text="✅ Готово", callback_data=f"{prefix}:done"),
        InlineKeyboardButton(text="✖ Отмена", callback_data=f"{prefix}:cancel"),
    ])
    rows.append([InlineKeyboardButton(text="↩ Назад", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_pick_delta_or_manual(store: Dict[str, Any], prefix: str) -> InlineKeyboardMarkup:
    deltas = store.get("deltas", {})
    items: List[Tuple[str, int]] = []
    if isinstance(deltas, dict):
        for k, v in deltas.items():
            try:
                items.append((str(k), int(v)))
            except (ValueError, TypeError):
                continue

    items.sort(key=lambda x: normalize_delta_name(x[0]))
    rows: List[List[InlineKeyboardButton]] = []
    for name, val in items[:12]:
        rows.append([InlineKeyboardButton(text=f"{name} ({val:+d})", callback_data=f"{prefix}:delta:{name}")])
    rows.append([InlineKeyboardButton(text="⌨️ Ввести число", callback_data=f"{prefix}:manual")])
    rows.append([InlineKeyboardButton(text="✖ Отмена", callback_data=f"{prefix}:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def apply_money_change(store: Dict[str, Any], user_ids: List[str], delta: int) -> List[str]:
    lines: List[str] = []
    users = store.get("users", {})
    if not isinstance(users, dict):
        return lines

    for uid_str in user_ids:
        rec = users.get(uid_str)
        if not rec:
            continue
        before = int(rec.get("films", 0))
        after = before + delta
        if after < 0:
            after = 0
        rec["films"] = after
        rec["updated_at"] = utc_now().isoformat()

        who = public_label(int(uid_str), rec)
        lines.append(f"{who} {delta:+d} -> {after}🎞️")

    return lines


@router.message(F.text == "Выдать плёнки")
async def give_start(msg: Message, state: FSMContext):
    if not await ensure_admin_msg(msg):
        return
    store = await load_store()
    await state.clear()
    await state.set_state(AdminStates.give_pick)
    await state.update_data(selected=set(), page=0)
    await msg.reply(
        "Выбери людей (можно несколько), потом жми «Готово»:",
        reply_markup=kb_multi_pick(store, selected=set(), page=0, prefix="give"),
        disable_web_page_preview=True,
    )


@router.message(F.text == "Отнять плёнки")
async def take_start(msg: Message, state: FSMContext):
    if not await ensure_admin_msg(msg):
        return
    store = await load_store()
    await state.clear()
    await state.set_state(AdminStates.take_pick)
    await state.update_data(selected=set(), page=0)
    await msg.reply(
        "Выбери людей (можно несколько), потом жми «Готово»:",
        reply_markup=kb_multi_pick(store, selected=set(), page=0, prefix="take"),
        disable_web_page_preview=True,
    )


async def _multi_toggle(cb: CallbackQuery, state: FSMContext, prefix: str, st_pick: State):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != st_pick.state:
        await cb.answer()
        return

    store = await load_store()
    st = await state.get_data()
    selected = set(st.get("selected", set()))
    page = int(st.get("page", 0))

    uid_str = cb.data.split(":")[2]
    if uid_str in selected:
        selected.remove(uid_str)
        await cb.answer("Убрал", show_alert=False)
    else:
        selected.add(uid_str)
        await cb.answer("Добавил", show_alert=False)

    await state.update_data(selected=selected)
    await cb.message.edit_reply_markup(reply_markup=kb_multi_pick(store, selected, page, prefix))


async def _multi_page(cb: CallbackQuery, state: FSMContext, prefix: str, st_pick: State):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != st_pick.state:
        await cb.answer()
        return

    store = await load_store()
    page = int(cb.data.split(":")[2])
    st = await state.get_data()
    selected = set(st.get("selected", set()))
    await state.update_data(page=page)
    await cb.message.edit_reply_markup(reply_markup=kb_multi_pick(store, selected, page, prefix))
    await cb.answer()


async def _multi_done(cb: CallbackQuery, state: FSMContext, prefix: str, st_pick: State, st_amount: State):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != st_pick.state:
        await cb.answer()
        return

    st = await state.get_data()
    selected = list(st.get("selected", set()))
    if not selected:
        await cb.answer("Никого не выбрал.", show_alert=True)
        return

    store = await load_store()
    await state.set_state(st_amount)
    await state.update_data(selected=selected)
    await cb.message.edit_text("Выбери дельту кнопкой или введи число:", reply_markup=kb_pick_delta_or_manual(store, prefix), disable_web_page_preview=True)
    await cb.answer("Ок, дальше", show_alert=False)


async def _multi_cancel(cb: CallbackQuery, state: FSMContext):
    if not await ensure_admin_cb(cb):
        return
    await state.clear()
    await cb.message.edit_text("Ок, отменено.", disable_web_page_preview=True)
    await cb.answer("Отмена", show_alert=False)


@router.callback_query(F.data.startswith("give:toggle:"))
async def give_toggle(cb: CallbackQuery, state: FSMContext):
    await _multi_toggle(cb, state, "give", AdminStates.give_pick)


@router.callback_query(F.data.startswith("give:page:"))
async def give_page(cb: CallbackQuery, state: FSMContext):
    await _multi_page(cb, state, "give", AdminStates.give_pick)


@router.callback_query(F.data == "give:done")
async def give_done(cb: CallbackQuery, state: FSMContext):
    await _multi_done(cb, state, "give", AdminStates.give_pick, AdminStates.give_amount)


@router.callback_query(F.data.startswith("take:toggle:"))
async def take_toggle(cb: CallbackQuery, state: FSMContext):
    await _multi_toggle(cb, state, "take", AdminStates.take_pick)


@router.callback_query(F.data.startswith("take:page:"))
async def take_page(cb: CallbackQuery, state: FSMContext):
    await _multi_page(cb, state, "take", AdminStates.take_pick)


@router.callback_query(F.data == "take:done")
async def take_done(cb: CallbackQuery, state: FSMContext):
    await _multi_done(cb, state, "take", AdminStates.take_pick, AdminStates.take_amount)


@router.callback_query(F.data == "give:cancel")
async def give_cancel(cb: CallbackQuery, state: FSMContext):
    await _multi_cancel(cb, state)


@router.callback_query(F.data == "take:cancel")
async def take_cancel(cb: CallbackQuery, state: FSMContext):
    await _multi_cancel(cb, state)


@router.callback_query(F.data.startswith("give:delta:"))
async def give_delta_apply(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != AdminStates.give_amount.state:
        await cb.answer()
        return

    name = cb.data.split(":")[2]
    store = await load_store()
    deltas = store.get("deltas", {})
    if not isinstance(deltas, dict) or name not in deltas:
        await cb.answer("Дельта исчезла.", show_alert=True)
        return

    delta = int(deltas[name])
    st = await state.get_data()
    selected = list(st.get("selected", []))

    lines = apply_money_change(store, selected, delta)
    await save_store(store)

    report = "Выдано:\n" + ("\n".join(lines) if lines else "ничего")
    await cb.message.edit_text(report, disable_web_page_preview=True)
    await send_audit(bot, f"🎞️ <b>give</b> ({esc(name)}={delta:+d}) by {link_from_user(cb.from_user)}\n" + "\n".join(lines[:60]))
    await state.clear()
    await cb.answer("Готово", show_alert=False)


@router.callback_query(F.data.startswith("take:delta:"))
async def take_delta_apply(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != AdminStates.take_amount.state:
        await cb.answer()
        return

    name = cb.data.split(":")[2]
    store = await load_store()
    deltas = store.get("deltas", {})
    if not isinstance(deltas, dict) or name not in deltas:
        await cb.answer("Дельта исчезла.", show_alert=True)
        return

    delta = -int(deltas[name])
    st = await state.get_data()
    selected = list(st.get("selected", []))

    lines = apply_money_change(store, selected, delta)
    await save_store(store)

    report = "Отнято:\n" + ("\n".join(lines) if lines else "ничего")
    await cb.message.edit_text(report, disable_web_page_preview=True)
    await send_audit(bot, f"🎞️ <b>take</b> ({esc(name)}={delta:+d}) by {link_from_user(cb.from_user)}\n" + "\n".join(lines[:60]))
    await state.clear()
    await cb.answer("Готово", show_alert=False)


@router.callback_query(F.data == "give:manual")
async def give_manual(cb: CallbackQuery, state: FSMContext):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != AdminStates.give_amount.state:
        await cb.answer()
        return
    await cb.message.edit_text("Введи целое число (сколько выдать):", disable_web_page_preview=True)
    await cb.answer("Ожидаю число", show_alert=False)


@router.callback_query(F.data == "take:manual")
async def take_manual(cb: CallbackQuery, state: FSMContext):
    if not await ensure_admin_cb(cb):
        return
    if await state.get_state() != AdminStates.take_amount.state:
        await cb.answer()
        return
    await cb.message.edit_text("Введи целое число (сколько отнять):", disable_web_page_preview=True)
    await cb.answer("Ожидаю число", show_alert=False)


@router.message(AdminStates.give_amount)
async def give_amount_text(msg: Message, state: FSMContext, bot: Bot):
    if not await ensure_admin_msg(msg):
        return
    try:
        delta = int((msg.text or "").strip())
    except ValueError:
        await msg.reply("Нужно целое число.", disable_web_page_preview=True)
        return
    if delta == 0:
        await msg.reply("0 не меняет мир.", disable_web_page_preview=True)
        return

    store = await load_store()
    st = await state.get_data()
    selected = list(st.get("selected", []))
    lines = apply_money_change(store, selected, delta)
    await save_store(store)

    await msg.reply("Выдано:\n" + ("\n".join(lines) if lines else "ничего"), disable_web_page_preview=True)
    await send_audit(bot, f"🎞️ <b>give</b> ({delta:+d}) by {link_from_user(msg.from_user)}\n" + "\n".join(lines[:60]))
    await state.clear()


@router.message(AdminStates.take_amount)
async def take_amount_text(msg: Message, state: FSMContext, bot: Bot):
    if not await ensure_admin_msg(msg):
        return
    try:
        val = int((msg.text or "").strip())
    except ValueError:
        await msg.reply("Нужно целое число.", disable_web_page_preview=True)
        return
    if val == 0:
        await msg.reply("0 не меняет мир.", disable_web_page_preview=True)
        return

    delta = -abs(val)
    store = await load_store()
    st = await state.get_data()
    selected = list(st.get("selected", []))
    lines = apply_money_change(store, selected, delta)
    await save_store(store)

    await msg.reply("Отнято:\n" + ("\n".join(lines) if lines else "ничего"), disable_web_page_preview=True)
    await send_audit(bot, f"🎞️ <b>take</b> ({delta:+d}) by {link_from_user(msg.from_user)}\n" + "\n".join(lines[:60]))
    await state.clear()


# =============================
# ADMIN: BACK BTN
# =============================
@router.callback_query(F.data == "adm:back")
async def admin_back(cb: CallbackQuery, state: FSMContext):
    if not await ensure_admin_cb(cb):
        return
    await state.clear()
    await cb.message.edit_text("Админ-панель активна. Используй кнопки снизу 👇", disable_web_page_preview=True)
    await cb.answer()


@router.callback_query(F.data == "noop")
async def noop(cb: CallbackQuery):
    await cb.answer()


# =============================
# DAILY RIDDLE (ADMIN FLOW + GROUP SCAN)
# =============================
def _norm_match(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r"\s+", " ", s)
    return s


def _answer_in_text(answer: str, text: str) -> bool:
    a = _norm_match(answer)
    t = _norm_match(text)
    if not a or not t:
        return False

    if " " in a:
        return a in t

    return re.search(rf"(?<!\w){re.escape(a)}(?!\w)", t, flags=re.UNICODE) is not None


@dataclass(frozen=True)
class RiddleFinalize:
    message_id: int
    winners: List[int]
    reward: int


def _get_active_riddle(store: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = store.get("daily_riddle")
    if not isinstance(r, dict):
        return None
    if not r.get("active"):
        return None
    return r


def _set_riddle(store: Dict[str, Any], payload: Optional[Dict[str, Any]]) -> None:
    store["daily_riddle"] = payload


def _try_register_riddle_winner(store: Dict[str, Any], user) -> Tuple[bool, Optional[RiddleFinalize]]:
    r = _get_active_riddle(store)
    if not r:
        return False, None

    winners_limit = int(r.get("winners_limit", 0))
    reward = int(r.get("reward", 0))
    msg_id = int(r.get("message_id", 0))

    if winners_limit <= 0 or reward <= 0 or msg_id <= 0:
        return False, None

    winners = r.get("winners")
    if not isinstance(winners, list):
        winners = []
        r["winners"] = winners

    uid = int(user.id)
    if uid in winners:
        return False, None

    if len(winners) >= winners_limit:
        return False, None

    winners.append(uid)

    # начисляем награду сразу
    upsert_user_from_tg(store, user)
    urec = store["users"].get(user_key(uid), {})
    before = int(urec.get("films", 0))
    urec["films"] = before + reward
    urec["updated_at"] = utc_now().isoformat()
    store["users"][user_key(uid)] = urec

    if len(winners) >= winners_limit:
        finalize = RiddleFinalize(message_id=msg_id, winners=list(winners), reward=reward)
        r["active"] = False
        _set_riddle(store, None)
        return True, finalize

    return True, None


@router.message(F.text == "Загадки(дейлики)")
async def riddle_start(msg: Message, state: FSMContext):
    if not await ensure_admin_msg(msg):
        return
    store = await load_store()
    if _get_active_riddle(store):
        await msg.reply("Уже есть активная загадка. Сначала дождись окончания.", disable_web_page_preview=True)
        return
    await state.clear()
    await state.set_state(AdminStates.riddle_text)
    await msg.reply("Введи текст поста для загадки (он уйдет в группу и будет закреплён):", disable_web_page_preview=True)


@router.message(AdminStates.riddle_text)
async def riddle_text(msg: Message, state: FSMContext):
    if not await ensure_admin_msg(msg):
        return
    text = (msg.text or "").strip()
    if not text or len(text) > 3500:
        await msg.reply("Текст нужен (до ~3500 символов).", disable_web_page_preview=True)
        return
    await state.update_data(riddle_text=text)
    await state.set_state(AdminStates.riddle_answer)
    await msg.reply("Теперь введи ответ (слово/фраза). Я буду ловить это в сообщениях:", disable_web_page_preview=True)


@router.message(AdminStates.riddle_answer)
async def riddle_answer(msg: Message, state: FSMContext):
    if not await ensure_admin_msg(msg):
        return
    answer = (msg.text or "").strip()
    if not answer or len(answer) > 64:
        await msg.reply("Ответ нужен (1..64 символа).", disable_web_page_preview=True)
        return
    await state.update_data(riddle_answer=answer)
    await state.set_state(AdminStates.riddle_reward)
    await msg.reply("Сколько плёнок в награду? (целое число > 0)", disable_web_page_preview=True)


@router.message(AdminStates.riddle_reward)
async def riddle_reward(msg: Message, state: FSMContext):
    if not await ensure_admin_msg(msg):
        return
    try:
        reward = int((msg.text or "").strip())
    except ValueError:
        await msg.reply("Нужно целое число.", disable_web_page_preview=True)
        return
    if reward <= 0 or reward > 100000:
        await msg.reply("Награда должна быть > 0.", disable_web_page_preview=True)
        return
    await state.update_data(riddle_reward=reward)
    await state.set_state(AdminStates.riddle_winners)
    await msg.reply("Сколько победителей? (целое число > 0)", disable_web_page_preview=True)


@router.message(AdminStates.riddle_winners)
async def riddle_winners(msg: Message, state: FSMContext, bot: Bot):
    if not await ensure_admin_msg(msg):
        return
    try:
        winners_limit = int((msg.text or "").strip())
    except ValueError:
        await msg.reply("Нужно целое число.", disable_web_page_preview=True)
        return
    if winners_limit <= 0 or winners_limit > 200:
        await msg.reply("Победителей: 1..200.", disable_web_page_preview=True)
        return

    st = await state.get_data()
    text = str(st.get("riddle_text", "")).strip()
    answer = str(st.get("riddle_answer", "")).strip()
    reward = int(st.get("riddle_reward", 0))

    if not text or not answer or reward <= 0:
        await msg.reply("Что-то потерялось. Начни заново.", disable_web_page_preview=True)
        await state.clear()
        return

    store = await load_store()
    if _get_active_riddle(store):
        await msg.reply("Уже есть активная загадка. Сначала дождись окончания.", disable_web_page_preview=True)
        await state.clear()
        return

    sent = await bot.send_message(
        chat_id=TARGET_CHAT_ID,
        text=text,
        disable_web_page_preview=True,
    )

    try:
        await bot.pin_chat_message(
            chat_id=TARGET_CHAT_ID,
            message_id=sent.message_id,
            disable_notification=True,
        )
    except Exception as e:
        log.warning("Pin failed (no rights?): %s", e)

    payload = {
        "active": True,
        "chat_id": TARGET_CHAT_ID,
        "message_id": int(sent.message_id),
        "text": text,
        "answer": answer,
        "reward": int(reward),
        "winners_limit": int(winners_limit),
        "winners": [],
        "created_at": utc_now().isoformat(),
    }
    _set_riddle(store, payload)
    await save_store(store)

    await msg.reply("Ок. Загадка отправлена и закреплена. Жду победителей.", disable_web_page_preview=True)
    await send_audit(bot, f"🧠 <b>riddle</b>: создана ({reward}🎞️, winners={winners_limit}) by {link_from_user(msg.from_user)}")
    await state.clear()


@router.message()
async def riddle_scanner(msg: Message, bot: Bot):
    if not is_target_group(msg):
        return
    if not msg.from_user or msg.from_user.is_bot:
        return

    text = extract_text_any(msg)
    if not text:
        return

    store = await load_store()
    r = _get_active_riddle(store)
    if not r:
        return

    answer = str(r.get("answer", "")).strip()
    if not _answer_in_text(answer, text):
        return

    added, finalize = _try_register_riddle_winner(store, msg.from_user)
    if not added and not finalize:
        return

    await save_store(store)

    if finalize is None:
        return

    try:
        await bot.unpin_chat_message(chat_id=TARGET_CHAT_ID, message_id=finalize.message_id)
    except Exception as e:
        log.warning("Unpin failed: %s", e)

    fresh_store = await load_store()
    users = fresh_store.get("users", {})
    lines: List[str] = []
    if isinstance(users, dict):
        for uid in finalize.winners:
            rec = users.get(user_key(uid)) or {}
            username = (rec.get("username") or "").strip()
            uname_part = f"@{username}" if username else normalize_name(rec.get("name") or str(uid))
            role = rec.get("role") or "Без роли"
            lines.append(f"{uname_part} - {role}")

    await bot.send_message(
        chat_id=TARGET_CHAT_ID,
        text="✅ Победители:\n" + ("\n".join(lines) if lines else "никого не нашёл, но это уже магия Telegram"),
        disable_web_page_preview=True,
    )

    await send_audit(bot, f"✅ <b>riddle_end</b>: winners={len(finalize.winners)} reward={finalize.reward}")


# =============================
# ERRORS (GLOBAL)
# =============================
@router.error()
async def on_error(event: ErrorEvent, bot: Bot):
    try:
        log.exception("Unhandled error: %r", event.exception)
    except Exception:
        pass

    if AUDIT_CHAT_ID:
        err = repr(event.exception)
        upd = getattr(event.update, "event_type", None) or type(event.update).__name__
        await send_audit(bot, f"💥 <b>Error</b> ({esc(str(upd))}): <code>{esc(err)[:3800]}</code>")
    return True


# =============================
# BACKUPS
# =============================
def parse_backup_at(s: str) -> dtime:
    m = re.match(r"^\s*(\d{1,2})\s*:\s*(\d{2})\s*$", s)
    if not m:
        return dtime(4, 5)
    hh = max(0, min(23, int(m.group(1))))
    mm = max(0, min(59, int(m.group(2))))
    return dtime(hh, mm)


def get_tz() -> Optional[ZoneInfo]:
    try:
        return ZoneInfo(TZ_NAME)
    except ZoneInfoNotFoundError:
        log.error("ZoneInfo not found for %s. Install tzdata or set TZ_NAME=UTC.", TZ_NAME)
        try:
            return ZoneInfo("UTC")
        except Exception:
            return None


async def make_backup(bot: Bot, reason: str = "scheduled") -> Optional[str]:
    try:
        src = Path(DATA_PATH)
        if not src.exists():
            return None
        dst_dir = Path(BACKUP_DIR)
        dst_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        dst = dst_dir / f"data_{ts}.json"
        shutil.copy2(src, dst)
        log.info("Backup created: %s", dst)

        # 2) Ежедневный бэкап в телеграмм
        if BACKUP_CHAT_ID != 0:
            try:
                await bot.send_document(
                    chat_id=BACKUP_CHAT_ID,
                    document=FSInputFile(str(dst)),
                    caption=f"💾 Backup ({reason}): {dst.name}",
                )
            except Exception as e:
                log.warning("Sending backup to BACKUP_CHAT_ID failed: %s", e)

        if SEND_BACKUP_FILE_TO_AUDIT and AUDIT_CHAT_ID != 0:
            await send_audit(bot, f"💾 <b>Backup</b> ({esc(reason)}): {esc(dst.name)}", file_path=str(dst))

        return str(dst)
    except Exception as e:
        log.exception("Backup failed: %s", e)
        return None


async def backup_loop(bot: Bot):
    tz = get_tz()
    at = parse_backup_at(BACKUP_AT)

    while True:
        try:
            now = datetime.now(tz) if tz else datetime.now()
            target = datetime.combine(now.date(), at)
            if tz:
                target = target.replace(tzinfo=tz)

            if target <= now:
                target = target + timedelta(days=1)

            sleep_s = (target - now).total_seconds()
            if sleep_s < 1:
                sleep_s = 60

            await asyncio.sleep(sleep_s)
            await make_backup(bot, reason="scheduled")
        except Exception as e:
            log.exception("backup_loop error: %s", e)
            await asyncio.sleep(60)


@router.message(Command("backup_now"))
async def backup_now_cmd(msg: Message, bot: Bot):
    if msg.chat.type != ChatType.PRIVATE or not msg.from_user:
        return
    if not require_admin(msg.from_user.id):
        await msg.reply(_admin_denied_text(msg.from_user.id), reply_markup=ReplyKeyboardRemove(), disable_web_page_preview=True)
        return
    path = await make_backup(bot, reason="manual")
    name = Path(path).name if path else "не удалось"
    await msg.reply(f"Ок. Бэкап: {name}", disable_web_page_preview=True)


# =============================
# STARTUP / MAIN
# =============================
async def on_startup(bot: Bot):
    log.info("Bot started. TARGET_CHAT_ID=%s DATA_PATH=%s", TARGET_CHAT_ID, DATA_PATH)
    asyncio.create_task(backup_loop(bot))
    asyncio.create_task(purge_loop(bot))


async def main():
    # Глобально режем link preview, чтобы не было этих "вложений" снизу.
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    dp.startup.register(on_startup)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
