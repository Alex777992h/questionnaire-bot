import os
import json
import time
import re
import sqlite3
import logging
from datetime import datetime
from typing import Optional, Tuple, List, Dict

import aiohttp
from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from dotenv import load_dotenv

# === LOAD ENV ===
load_dotenv()

# === CONFIG ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    print("Error: BOT_TOKEN not found. Check .env")
    exit(1)

DEFAULT_CONFIG = {
    "admin_ids": [1322362053, 1875115860],
    "plugin_base_url": "http://c7.play2go.cloud:20795",
    "plugin_secret": "RrN4Jt9Vq2KpX8mZ",
    "chat_invite_url": "https://t.me/+15HwEq4ltUJmMTIy",
    "cooldown_seconds": 600,
    "pending_page_size": 5,
    "banned_user_ids": [],
    "banned_nicks": [],
    "alert_cooldown_seconds": 300,
    "reason_templates": [
        "Не подходит по возрасту",
        "Нет микрофона / не готов к войсчату",
        "Нет возможности играть на новых версиях",
        "Недостаточно информации в анкете",
    ],
    "archive_days": 90,
    "support_chat": "https://t.me/+15HwEq4ltUJmMTIy",
}

CONFIG_PATH = "config.json"
if os.path.exists(CONFIG_PATH):
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            user_cfg = json.load(f)
        DEFAULT_CONFIG.update(user_cfg)
    except Exception:
        pass

ADMIN_IDS = set(DEFAULT_CONFIG["admin_ids"])
BAN_USER_IDS = set(DEFAULT_CONFIG["banned_user_ids"])
BAN_NICKS = {n.lower() for n in DEFAULT_CONFIG["banned_nicks"]}
PLUGIN_BASE_URL = DEFAULT_CONFIG["plugin_base_url"]
PLUGIN_SECRET = DEFAULT_CONFIG["plugin_secret"]
CHAT_INVITE_URL = DEFAULT_CONFIG["chat_invite_url"]
COOLDOWN_SECONDS = int(DEFAULT_CONFIG["cooldown_seconds"])
PENDING_PAGE_SIZE = int(DEFAULT_CONFIG["pending_page_size"])
ALERT_COOLDOWN_SECONDS = int(DEFAULT_CONFIG["alert_cooldown_seconds"])
REASON_TEMPLATES = list(DEFAULT_CONFIG["reason_templates"])
ARCHIVE_DAYS = int(DEFAULT_CONFIG["archive_days"])
SUPPORT_CHAT_URL = DEFAULT_CONFIG["support_chat"]
ENABLE_BUTTON_STYLE = bool(DEFAULT_CONFIG.get("enable_button_style", False))

# === DB PATH ===
if os.path.exists("/app/data/"):
    DB_PATH = "/app/data/bot.db"
else:
    DB_PATH = "bot.db"

# === LOGGING ===
logging.basicConfig(
    filename="bot.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

router = Router()

QUESTIONS = [
    ("nick", "1. Ник", "Введите ник (3-16 символов)."),
    ("name", "2. Как к вам можно обращаться (имя/псевдоним/по нику)", "Введите, как к вам обращаться."),
    ("age", "3. Возраст", "Введите возраст."),
    ("mods", "4. Имеется ли возможность играть с модами и переходить на новые версии", "Да/Нет."),
    ("voice_listen", "5. Имеется ли возможность слушать в войсчате", "Да/Нет."),
    ("voice_speak", "6. Имеется ли возможность говорить в войсчате", "Да/Нет."),
    ("device", "7. Устройство, с которого ты играешь", "ПК/ноутбук/телефон/консоль и т.п."),
    ("plans", "8. Имеются ли планы на сервер?", "Кратко опишите планы."),
    ("host", "9. Будет ли возможность скидываться на оплату хоста? (необязательно)", "Да/Нет."),
]

MINECRAFT_NICK_RE = re.compile(r"^[A-Za-z0-9_]{3,16}$")
YES_NO_KEYS = {"mods", "voice_listen", "voice_speak", "host"}

BTN_APPLY = "📝 Подать заявку"
BTN_STATUS = "📌 Статус заявки"
BTN_ADMIN = "🛠️ Админ панель"
BTN_PENDING = "🧾 Активные заявки"
BTN_SHOW = "🔎 Показать по ID"
BTN_SEARCH = "🔍 Поиск"
BTN_STATS = "📊 Статистика"
BTN_HEALTH = "🩺 Проверка плагина"
BTN_BAN_USER = "🚫 Бан TG"
BTN_UNBAN_USER = "✅ Разбан TG"
BTN_BAN_NICK = "🚫 Бан ник"
BTN_UNBAN_NICK = "✅ Разбан ник"
BTN_ARCHIVE = "📦 Архив"
BTN_HELP = "ℹ️ Помощь"
BTN_BACK = "⬅️ Назад"
BTN_SUPPORT = "🛟 Техподдержка"
BTN_FEEDBACK = "💬 Отзыв"
BTN_SUPPORT_DONE = "✅ Отправить"
BTN_SUPPORT_CANCEL = "❌ Отменить"

LAST_PLUGIN_ALERT_TS = 0
PENDING_REJECT_REASON: Dict[int, int] = {}
PENDING_INPUT_MODE: Dict[int, str] = {}
SELECTED_NICK_BY_USER: Dict[int, str] = {}


def db_connect() -> sqlite3.Connection:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS applications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            nick TEXT NOT NULL,
            q_name TEXT,
            q_age TEXT,
            q_mods TEXT,
            q_voice_listen TEXT,
            q_voice_speak TEXT,
            q_device TEXT,
            q_plans TEXT,
            q_host TEXT,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            kind TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS support_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            selected_nick TEXT,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            text TEXT,
            file_id TEXT,
            file_type TEXT,
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS applications_archive (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            username TEXT,
            nick TEXT NOT NULL,
            q_name TEXT,
            q_age TEXT,
            q_mods TEXT,
            q_voice_listen TEXT,
            q_voice_speak TEXT,
            q_device TEXT,
            q_plans TEXT,
            q_host TEXT,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            archived_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS form_states (
            user_id INTEGER PRIMARY KEY,
            step INTEGER NOT NULL,
            data TEXT NOT NULL,
            editing_app_id INTEGER,
            updated_at INTEGER NOT NULL
        )
        """
    )
    ensure_columns(conn)
    ensure_support_columns(conn)
    return conn


def ensure_columns(conn: sqlite3.Connection):
    columns = {
        "q_name": "TEXT",
        "q_age": "TEXT",
        "q_mods": "TEXT",
        "q_voice_listen": "TEXT",
        "q_voice_speak": "TEXT",
        "q_device": "TEXT",
        "q_plans": "TEXT",
        "q_host": "TEXT",
    }
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(applications)")
    existing = {row[1] for row in cur.fetchall()}
    for col, col_type in columns.items():
        if col not in existing:
            try:
                conn.execute(f"ALTER TABLE applications ADD COLUMN {col} {col_type}")
            except sqlite3.Error:
                pass


def ensure_support_columns(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(support_sessions)")
    existing = {row[1] for row in cur.fetchall()}
    if "selected_nick" not in existing:
        try:
            conn.execute("ALTER TABLE support_sessions ADD COLUMN selected_nick TEXT")
        except sqlite3.Error:
            pass

def save_form_state(user_id: int, step: int, data: dict, editing_app_id: Optional[int]):
    with db_connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO form_states (user_id, step, data, editing_app_id, updated_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, step, json.dumps(data, ensure_ascii=False), editing_app_id, int(time.time())),
        )


def load_form_state(user_id: int) -> Optional[Tuple[int, dict, Optional[int]]]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT step, data, editing_app_id FROM form_states WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        step, data_json, editing_app_id = row
        try:
            data = json.loads(data_json)
        except Exception:
            data = {}
        return step, data, editing_app_id


def clear_form_state(user_id: int):
    with db_connect() as conn:
        conn.execute("DELETE FROM form_states WHERE user_id = ?", (user_id,))


def create_application(user_id: int, username: Optional[str], answers: dict) -> int:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO applications (user_id, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                user_id,
                username,
                answers.get("nick", ""),
                answers.get("name", ""),
                answers.get("age", ""),
                answers.get("mods", ""),
                answers.get("voice_listen", ""),
                answers.get("voice_speak", ""),
                answers.get("device", ""),
                answers.get("plans", ""),
                answers.get("host", ""),
                "pending",
                int(time.time()),
            ),
        )
        return cur.lastrowid


def update_application(app_id: int, answers: dict):
    with db_connect() as conn:
        conn.execute(
            "UPDATE applications SET nick = ?, q_name = ?, q_age = ?, q_mods = ?, q_voice_listen = ?, q_voice_speak = ?, q_device = ?, q_plans = ?, q_host = ? WHERE id = ?",
            (
                answers.get("nick", ""),
                answers.get("name", ""),
                answers.get("age", ""),
                answers.get("mods", ""),
                answers.get("voice_listen", ""),
                answers.get("voice_speak", ""),
                answers.get("device", ""),
                answers.get("plans", ""),
                answers.get("host", ""),
                app_id,
            ),
        )


def get_last_application(user_id: int):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, nick, status, created_at FROM applications "
            "WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        return cur.fetchone()


def set_status(app_id: int, status: str):
    with db_connect() as conn:
        conn.execute("UPDATE applications SET status = ? WHERE id = ?", (status, app_id))


def get_application(app_id: int):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, status, created_at "
            "FROM applications WHERE id = ?",
            (app_id,),
        )
        return cur.fetchone()


def get_pending_applications(limit: int, offset: int):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, status, created_at "
            "FROM applications WHERE status = 'pending' ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
        return cur.fetchall()


def get_pending_count() -> int:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM applications WHERE status = 'pending'")
        return cur.fetchone()[0]


def get_first_approved_nick(user_id: int) -> Optional[str]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT nick FROM applications WHERE user_id = ? AND status = 'approved' ORDER BY id ASC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_latest_approved_nick(user_id: int) -> Optional[str]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT nick FROM applications WHERE user_id = ? AND status = 'approved' ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_approved_nicks(user_id: int) -> List[str]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT nick FROM applications WHERE user_id = ? AND status = 'approved' ORDER BY id ASC",
            (user_id,),
        )
        return [row[0] for row in cur.fetchall()]


def get_stats() -> Dict[str, int]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM applications")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM applications WHERE status = 'pending'")
        pending = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM applications WHERE status = 'approved'")
        approved = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM applications WHERE status = 'rejected'")
        rejected = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM applications_archive")
        archived = cur.fetchone()[0]
    return {
        "total": total,
        "pending": pending,
        "approved": approved,
        "rejected": rejected,
        "archived": archived,
    }


def search_applications(query: str, limit: int = 10) -> List[tuple]:
    q_raw = (query or "").strip()
    if q_raw.startswith("@"):
        q_raw = q_raw[1:]
    q = f"%{q_raw}%"
    with db_connect() as conn:
        cur = conn.cursor()
        if q_raw.isdigit():
            cur.execute(
                "SELECT id, user_id, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, status, created_at "
                "FROM applications WHERE nick LIKE ? OR username LIKE ? OR q_name LIKE ? OR user_id = ? OR id = ? "
                "ORDER BY id DESC LIMIT ?",
                (q, q, q, int(q_raw), int(q_raw), limit),
            )
        else:
            cur.execute(
                "SELECT id, user_id, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, status, created_at "
                "FROM applications WHERE nick LIKE ? OR username LIKE ? OR q_name LIKE ? "
                "ORDER BY id DESC LIMIT ?",
                (q, q, q, limit),
            )
        return cur.fetchall()


def start_support_session(user_id: int, username: Optional[str], selected_nick: Optional[str]) -> int:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM support_sessions WHERE user_id = ? AND status = 'open' ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute(
            "INSERT INTO support_sessions (user_id, username, selected_nick, status, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, selected_nick, "open", int(time.time())),
        )
        return cur.lastrowid


def get_active_support_session(user_id: int) -> Optional[int]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM support_sessions WHERE user_id = ? AND status = 'open' ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_support_session_selected_nick(session_id: int) -> Optional[str]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT selected_nick FROM support_sessions WHERE id = ?", (session_id,))
        row = cur.fetchone()
        return row[0] if row else None


def close_support_session(session_id: int):
    with db_connect() as conn:
        conn.execute("UPDATE support_sessions SET status = 'closed' WHERE id = ?", (session_id,))


def add_support_message(session_id: int, kind: str, text: Optional[str], file_id: Optional[str], file_type: Optional[str]):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO support_messages (session_id, kind, text, file_id, file_type, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (session_id, kind, text, file_id, file_type, int(time.time())),
        )


def get_support_messages(session_id: int) -> List[tuple]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT kind, text, file_id, file_type FROM support_messages WHERE session_id = ? ORDER BY id ASC",
            (session_id,),
        )
        return cur.fetchall()


def archive_old_applications(days: int) -> int:
    cutoff = int(time.time()) - days * 86400
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, status, created_at "
            "FROM applications WHERE created_at < ? AND status != 'pending'",
            (cutoff,),
        )
        rows = cur.fetchall()
        if not rows:
            return 0
        for row in rows:
            (
                app_id,
                user_id,
                username,
                nick,
                q_name,
                q_age,
                q_mods,
                q_voice_listen,
                q_voice_speak,
                q_device,
                q_plans,
                q_host,
                status,
                created_at,
            ) = row
            conn.execute(
                "INSERT OR IGNORE INTO applications_archive "
                "(id, user_id, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, status, created_at, archived_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    app_id,
                    user_id,
                    username,
                    nick,
                    q_name,
                    q_age,
                    q_mods,
                    q_voice_listen,
                    q_voice_speak,
                    q_device,
                    q_plans,
                    q_host,
                    status,
                    created_at,
                    int(time.time()),
                ),
            )
            conn.execute("DELETE FROM applications WHERE id = ?", (app_id,))
        return len(rows)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def button_supports_style() -> bool:
    fields = getattr(InlineKeyboardButton, "model_fields", None) or getattr(InlineKeyboardButton, "__fields__", None)
    return bool(fields and "style" in fields)


def make_button(text: str, cb: str, style: Optional[str] = None) -> InlineKeyboardButton:
    kwargs = {"text": text, "callback_data": cb}
    if style and ENABLE_BUTTON_STYLE and button_supports_style():
        kwargs["style"] = style
    return InlineKeyboardButton(**kwargs)


def format_date(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def build_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text=BTN_APPLY), KeyboardButton(text=BTN_STATUS)],
        [KeyboardButton(text=BTN_SUPPORT), KeyboardButton(text=BTN_FEEDBACK)],
        [KeyboardButton(text=BTN_HELP)],
    ]
    if is_admin(user_id):
        keyboard.append([KeyboardButton(text=BTN_ADMIN)])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def build_admin_menu() -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text=BTN_PENDING), KeyboardButton(text=BTN_SHOW)],
        [KeyboardButton(text=BTN_SEARCH), KeyboardButton(text=BTN_STATS)],
        [KeyboardButton(text=BTN_HEALTH)],
        [KeyboardButton(text=BTN_BAN_USER), KeyboardButton(text=BTN_UNBAN_USER)],
        [KeyboardButton(text=BTN_BAN_NICK), KeyboardButton(text=BTN_UNBAN_NICK)],
        [KeyboardButton(text=BTN_ARCHIVE)],
        [KeyboardButton(text=BTN_BACK)],
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def ask_question(chat_id: int, index: int):
    if index >= len(QUESTIONS):
        return None, None
    key, title, hint = QUESTIONS[index]
    buttons = []
    if key in YES_NO_KEYS:
        buttons.append(
            [
                make_button("✅ Да", f"ans:{key}:yes", style="positive"),
                make_button("❌ Нет", f"ans:{key}:no", style="negative"),
            ]
        )
    buttons.append([make_button("⛔ Отменить", "cancel_form")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    text = f"🧩 *{title}*\n_{hint}_"
    return keyboard, text


def save_and_advance(user, key: str, value: str) -> Optional[int]:
    state = load_form_state(user.id)
    if not state:
        return None
    step, data, editing_app_id = state
    expected_key = QUESTIONS[step][0]
    if key != expected_key:
        return None
    data[key] = value
    next_step = step + 1
    save_form_state(user.id, next_step, data, editing_app_id)
    return next_step


async def call_plugin(endpoint: str, nick: str) -> bool:
    url = f"{PLUGIN_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data={"nick": nick, "secret": PLUGIN_SECRET}, timeout=5) as resp:
                text = await resp.text()
                return resp.status == 200 and text.strip().lower() == "ok"
    except Exception as e:
        logging.error("Plugin call error: %s", e)
        return False


async def health_check() -> bool:
    try:
        url = f"{PLUGIN_BASE_URL.rstrip('/')}/health"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=3) as resp:
                text = await resp.text()
                return resp.status == 200 and text.strip().lower() == "ok"
    except Exception:
        return False

async def notify_admins(bot: Bot, text: str, keyboard: Optional[InlineKeyboardMarkup] = None):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass


def maybe_alert_admins(bot: Bot, text: str):
    global LAST_PLUGIN_ALERT_TS
    now = int(time.time())
    if now - LAST_PLUGIN_ALERT_TS >= ALERT_COOLDOWN_SECONDS:
        LAST_PLUGIN_ALERT_TS = now
        return notify_admins(bot, text)
    return None


async def send_application_to_admins(bot: Bot, app_id: int, answers: dict, user):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                make_button("✅ Одобрить", f"approve:{app_id}", style="positive"),
                make_button("❌ Отклонить", f"reject:{app_id}", style="negative"),
                make_button("⛔ Без причины", f"reject_noreason:{app_id}", style="negative"),
            ],
            [make_button("📄 Подробнее", f"show:{app_id}")],
        ]
    )
    username_part = f"@{user.username}" if user.username else "без username"
    text = (
        f"✨ *Новая заявка* `#{app_id}`\n"
        f"🎮 Ник: `{answers.get('nick', '')}`\n"
        f"👤 Как обращаться: {answers.get('name', '')}\n"
        f"🎂 Возраст: {answers.get('age', '')}\n"
        f"🎙️ Войс (слушать/говорить): {answers.get('voice_listen', '—')} / {answers.get('voice_speak', '—')}\n"
        f"📎 От: {username_part} (id {user.id})"
    )
    await notify_admins(bot, text, keyboard)


async def process_decision(bot: Bot, app_id: int, action: str, admin_chat_id: int):
    app = get_application(app_id)
    if not app:
        await bot.send_message(admin_chat_id, "Заявка не найдена.")
        return

    (
        _id,
        target_user_id,
        _username,
        nick,
        _q_name,
        _q_age,
        _q_mods,
        _q_voice_listen,
        _q_voice_speak,
        _q_device,
        _q_plans,
        _q_host,
        status,
        _created,
    ) = app

    if status != "pending":
        await bot.send_message(admin_chat_id, f"Заявка уже обработана. Статус: {status}")
        return

    if action == "approve":
        ok = await call_plugin("approve", nick)
        if not ok:
            await bot.send_message(admin_chat_id, "Ошибка плагина. Попробуйте позже.")
            await maybe_alert_admins(bot, "⚠️ Плагин недоступен. Проверь порт/хост.")
            return

        set_status(app_id, "approved")
        await bot.send_message(admin_chat_id, f"Заявка #{app_id} одобрена.")
        try:
            await bot.send_message(
                target_user_id,
                f"Ваша заявка #{app_id} одобрена. Ник `{nick}` добавлен в вайтлист сервера.\n"
                f"Чат сервера: {CHAT_INVITE_URL}",
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        return

    if action == "reject":
        PENDING_REJECT_REASON[admin_chat_id] = app_id
        reason_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [make_button(r, f"reason:{app_id}:{i+1}") for i, r in enumerate(REASON_TEMPLATES[:2])],
                [make_button(r, f"reason:{app_id}:{i+1}") for i, r in enumerate(REASON_TEMPLATES[2:4], start=2)],
                [make_button("⛔ Без причины", f"reject_noreason:{app_id}", style="negative")],
            ]
        )
        await bot.send_message(
            admin_chat_id,
            f"Выберите шаблон причины или напишите свой текст для заявки #{app_id}:",
            reply_markup=reason_kb,
        )
        return


async def send_next_question(bot: Bot, chat_id: int, step: int):
    keyboard, text = ask_question(chat_id, step)
    if keyboard and text:
        await bot.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)


async def finalize_application_for_user(bot: Bot, user, chat_id: int):
    state = load_form_state(user.id)
    if not state:
        return

    _step, answers, editing_app_id = state
    clear_form_state(user.id)

    if editing_app_id:
        update_application(editing_app_id, answers)
        await bot.send_message(chat_id, f"✅ Заявка #{editing_app_id} обновлена.")
        await notify_admins(bot, f"✏️ Заявка #{editing_app_id} обновлена пользователем.")
        return

    app_id = create_application(user.id, user.username, answers)
    await bot.send_message(chat_id, f"✅ Заявка `#{app_id}` отправлена. Ожидай решения администратора.")
    await send_application_to_admins(bot, app_id, answers, user)


async def finalize_support(bot: Bot, message: Message):
    session_id = get_active_support_session(message.from_user.id)
    if not session_id:
        await message.answer("Нет активного обращения.")
        return

    msgs = get_support_messages(session_id)
    if not msgs:
        await message.answer("Вы не отправили ни одного сообщения.")
        return

    close_support_session(session_id)
    selected = get_support_session_selected_nick(session_id) or SELECTED_NICK_BY_USER.get(message.from_user.id)
    approved_nick = selected or get_latest_approved_nick(message.from_user.id) or get_first_approved_nick(message.from_user.id)
    header = (
        "🛟 *Обращение в техподдержку*\n"
        f"От: @{message.from_user.username or 'без username'} (id {message.from_user.id})\n"
        f"Одобренный ник: {approved_nick or 'нет'}\n"
        f"ID обращения: `{session_id}`"
    )

    texts = [m[1] for m in msgs if m[0] == "text" and m[1]]
    if texts:
        header += "\n\nСообщения:\n" + "\n".join([f"- {t}" for t in texts])

    await notify_admins(bot, header)

    # отправляем файлы отдельно (Telegram не позволяет объединить всё в одно сообщение)
    for admin_id in ADMIN_IDS:
        for _kind, _text, file_id, file_type in msgs:
            if not file_id:
                continue
            try:
                if file_type == "photo":
                    await bot.send_photo(admin_id, file_id)
                elif file_type == "document":
                    await bot.send_document(admin_id, file_id)
                elif file_type == "video":
                    await bot.send_video(admin_id, file_id)
                elif file_type == "audio":
                    await bot.send_audio(admin_id, file_id)
                elif file_type == "voice":
                    await bot.send_voice(admin_id, file_id)
            except Exception:
                pass

    await message.answer("✅ Обращение отправлено.", reply_markup=build_main_menu(message.from_user.id))

@router.message(F.text == "/start")
async def cmd_start(message: Message):
    await message.answer(
        "👋 *Добро пожаловать!*\n"
        "Я — бот заявок на сервер.\n"
        "📜 *Правила бота:*\n"
        "1. Заполняй анкету честно и по делу.\n"
        "2. Не флуди и не подавай дубликаты — есть ограничение по времени.\n"
        "3. Нельзя использовать оскорбления и мат в ответах.\n"
        "4. Ник — только латиница, цифры и `_` (3–16 символов).\n\n"
        "Выбери действие ниже или используй команды:\n"
        "`/apply` — подать заявку\n"
        "`/status` — статус заявки",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_main_menu(message.from_user.id),
    )


@router.message(F.text == "/help")
async def cmd_help(message: Message):
    await message.answer(
        "ℹ️ *Справка*\n"
        "`/apply` — подать заявку\n"
        "`/status` — статус заявки\n"
        "`/cancel` — отменить анкету\n"
        "`/edit` — редактировать активную заявку\n"
        "`/support` — техподдержка\n"
        "`/feedback` — оставить отзыв\n"
        "`/support_done` — отправить обращение (техподдержка)\n"
        "`/support_cancel` — отменить обращение (техподдержка)\n"
        "`/ban_user <tg_id>` — бан по Telegram ID (админ)\n"
        "`/unban_user <tg_id>` — разбан по Telegram ID (админ)\n"
        "`/ban_nick <nick>` — бан по нику (админ)\n"
        "`/unban_nick <nick>` — разбан по нику (админ)\n"
        "`/archive` — архивировать старые заявки (админ)\n",
        parse_mode=ParseMode.MARKDOWN,
    )


@router.message(F.text == "/apply")
async def cmd_apply(message: Message, bot: Bot):
    if message.from_user.id in BAN_USER_IDS:
        await message.answer("Вы в бан-листе. Заявка недоступна.")
        return

    last = get_last_application(message.from_user.id)
    if last:
        last_id, _last_nick, last_status, last_created = last
        if last_status == "pending":
            await message.answer(
                f"У тебя уже есть активная заявка #{last_id}. Дождись решения администратора."
            )
            return
        if not is_admin(message.from_user.id) and int(time.time()) - int(last_created) < COOLDOWN_SECONDS:
            wait_min = max(1, COOLDOWN_SECONDS // 60)
            await message.answer(f"Повторную заявку можно подать через {wait_min} мин.")
            return

    state = load_form_state(message.from_user.id)
    if state:
        await message.answer("У тебя уже есть незавершенная анкета. Ответь на текущий вопрос.")
        return

    save_form_state(message.from_user.id, 0, {}, None)
    await message.answer(
        "🔥 *Анкета на сервер*\n"
        "Ответь на вопросы по порядку.\n"
        "Отмена: `/cancel`",
        parse_mode=ParseMode.MARKDOWN,
    )
    await send_next_question(bot, message.chat.id, 0)


@router.message(F.text == "/edit")
async def cmd_edit(message: Message, bot: Bot):
    last = get_last_application(message.from_user.id)
    if not last:
        await message.answer("Нет активной заявки для редактирования.")
        return
    last_id, _last_nick, last_status, _created = last
    if last_status != "pending":
        await message.answer("Редактировать можно только активную заявку.")
        return

    save_form_state(message.from_user.id, 0, {}, last_id)
    await message.answer(
        "✏️ *Редактирование заявки*\n"
        "Ответь на вопросы по порядку.\n"
        "Отмена: `/cancel`",
        parse_mode=ParseMode.MARKDOWN,
    )
    await send_next_question(bot, message.chat.id, 0)


@router.message(F.text == "/status")
async def cmd_status(message: Message):
    last = get_last_application(message.from_user.id)
    if not last:
        await message.answer("У тебя нет заявок.")
        return

    app_id, nick, status, created_at = last
    await message.answer(
        f"📌 *Последняя заявка* `#{app_id}`\n"
        f"🎮 Ник: `{nick}`\n"
        f"📍 Статус: *{status}*\n"
        f"🕒 Дата: {format_date(created_at)}",
        parse_mode=ParseMode.MARKDOWN,
    )


@router.message(F.text == "/support")
async def cmd_support(message: Message):
    approved_nicks = get_approved_nicks(message.from_user.id)
    if len(approved_nicks) > 1:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [make_button(f"🎮 {n}", f"picknick:support:{n}") for n in approved_nicks[i:i+2]]
                for i in range(0, len(approved_nicks), 2)
            ]
        )
        await message.answer(
            "Выбери ник, с которым обращаешься в поддержку:",
            reply_markup=keyboard,
        )
        return
    selected_nick = approved_nicks[0] if approved_nicks else None
    SELECTED_NICK_BY_USER[message.from_user.id] = selected_nick or ""
    session_id = start_support_session(message.from_user.id, message.from_user.username, selected_nick)
    text = (
        "🛟 *Техподдержка*\n"
        "Опиши проблему и можешь прикрепить файлы/фото.\n"
        "Когда закончишь — нажми «Отправить».\n"
        f"Также можно написать в чат: {SUPPORT_CHAT_URL}\n"
        f"ID обращения: `{session_id}`"
    )
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_SUPPORT_DONE), KeyboardButton(text=BTN_SUPPORT_CANCEL)]],
        resize_keyboard=True,
    )
    await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard, disable_web_page_preview=True)


@router.message(F.text == "/feedback")
async def cmd_feedback(message: Message):
    approved_nicks = get_approved_nicks(message.from_user.id)
    if len(approved_nicks) > 1:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [make_button(f"🎮 {n}", f"picknick:feedback:{n}") for n in approved_nicks[i:i+2]]
                for i in range(0, len(approved_nicks), 2)
            ]
        )
        await message.answer(
            "Выбери ник, с которым оставляешь отзыв:",
            reply_markup=keyboard,
        )
        return
    selected_nick = approved_nicks[0] if approved_nicks else None
    SELECTED_NICK_BY_USER[message.from_user.id] = selected_nick or ""
    text = (
        "💬 *Отзыв*\n"
        "Напиши короткий отзыв: что понравилось, что улучшить."
    )
    PENDING_INPUT_MODE[message.from_user.id] = "feedback"
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


@router.message(F.text == "/support_done")
async def cmd_support_done(message: Message, bot: Bot):
    await finalize_support(bot, message)


@router.message(F.text == "/support_cancel")
async def cmd_support_cancel(message: Message):
    session_id = get_active_support_session(message.from_user.id)
    if not session_id:
        await message.answer("Нет активного обращения.")
        return
    close_support_session(session_id)
    await message.answer("Обращение отменено.", reply_markup=build_main_menu(message.from_user.id))


@router.message(F.text == "/cancel")
async def cmd_cancel(message: Message):
    state = load_form_state(message.from_user.id)
    if state:
        clear_form_state(message.from_user.id)
        await message.answer("🗑️ Анкета отменена.")
    else:
        await message.answer("Нет активной анкеты.")


@router.message(F.text == "/pending")
async def cmd_pending(message: Message, bot: Bot, page: int = 1):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return

    total = get_pending_count()
    if total == 0:
        await message.answer("🟢 Активных заявок нет.")
        return

    page = max(1, page)
    offset = (page - 1) * PENDING_PAGE_SIZE
    rows = get_pending_applications(PENDING_PAGE_SIZE, offset)

    for row in rows:
        (
            app_id,
            user_id,
            username,
            nick,
            q_name,
            q_age,
            q_mods,
            q_voice_listen,
            q_voice_speak,
            q_device,
            q_plans,
            q_host,
            _status,
            _created,
        ) = row
        username_part = f"@{username}" if username else "без username"
        who = f"{q_name or '—'} / {username_part} / id {user_id}"
        text = (
            f"*Заявка* `#{app_id}`\n"
            f"🎮 Ник: `{nick}`\n"
            f"👤 Как обращаться: {q_name or '—'}\n"
            f"🎂 Возраст: {q_age or '—'}\n"
            f"🧩 Моды/версии: {q_mods or '—'}\n"
            f"🎧 Войс (слушать): {q_voice_listen or '—'}\n"
            f"🎤 Войс (говорить): {q_voice_speak or '—'}\n"
            f"💻 Устройство: {q_device or '—'}\n"
            f"🧭 Планы: {q_plans or '—'}\n"
            f"💰 Хост: {q_host or '—'}\n"
            f"📎 От: {who}"
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    make_button("✅ Одобрить", f"approve:{app_id}", style="positive"),
                    make_button("❌ Отклонить", f"reject:{app_id}", style="negative"),
                    make_button("⛔ Без причины", f"reject_noreason:{app_id}", style="negative"),
                ]
            ]
        )
        await bot.send_message(message.chat.id, text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)

    max_page = (total + PENDING_PAGE_SIZE - 1) // PENDING_PAGE_SIZE
    nav_keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    row = []
    if page > 1:
        row.append(make_button("⬅️ Назад", f"pending_page:{page - 1}"))
    if page < max_page:
        row.append(make_button("➡️ Вперёд", f"pending_page:{page + 1}"))
    if row:
        nav_keyboard.inline_keyboard.append(row)
        await bot.send_message(message.chat.id, f"📄 Страница {page}/{max_page}", reply_markup=nav_keyboard)


@router.message(F.text == "/show")
async def cmd_show(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /show <id>")
        return

    app_id = int(parts[1])
    app = get_application(app_id)
    if not app:
        await message.answer("Заявка не найдена.")
        return

    (
        _id,
        user_id,
        username,
        nick,
        q_name,
        q_age,
        q_mods,
        q_voice_listen,
        q_voice_speak,
        q_device,
        q_plans,
        q_host,
        status,
        created_at,
    ) = app
    username_part = f"@{username}" if username else "без username"
    who = f"{q_name or '—'} / {username_part} / id {user_id}"
    text = (
        f"*Заявка* `#{app_id}`\n"
        f"🎮 Ник: `{nick}`\n"
        f"👤 Как обращаться: {q_name or '—'}\n"
        f"🎂 Возраст: {q_age or '—'}\n"
        f"🧩 Моды/версии: {q_mods or '—'}\n"
        f"🎧 Войс (слушать): {q_voice_listen or '—'}\n"
        f"🎤 Войс (говорить): {q_voice_speak or '—'}\n"
        f"💻 Устройство: {q_device or '—'}\n"
        f"🧭 Планы: {q_plans or '—'}\n"
        f"💰 Хост: {q_host or '—'}\n"
        f"📎 От: {who}\n"
        f"📍 Статус: *{status}*\n"
        f"🕒 Дата: {format_date(created_at)}"
    )
    keyboard = None
    if status == "pending":
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    make_button("✅ Одобрить", f"approve:{app_id}", style="positive"),
                    make_button("❌ Отклонить", f"reject:{app_id}", style="negative"),
                    make_button("⛔ Без причины", f"reject_noreason:{app_id}", style="negative"),
                ]
            ]
        )
    await bot.send_message(message.chat.id, text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)


@router.message(F.text == "/approve")
async def cmd_approve(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /approve <id>")
        return

    await process_decision(bot, int(parts[1]), "approve", message.chat.id)


@router.message(F.text == "/reject")
async def cmd_reject(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /reject <id>")
        return

    await process_decision(bot, int(parts[1]), "reject", message.chat.id)


@router.message(F.text == "/health")
async def cmd_health(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return

    ok = await health_check()
    await message.answer("✅ Плагин доступен." if ok else "❌ Плагин недоступен.")


@router.message(F.text == "/stats")
async def cmd_stats(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    s = get_stats()
    await message.answer(
        "📊 Статистика:\n"
        f"Всего: {s['total']}\n"
        f"Ожидают: {s['pending']}\n"
        f"Одобрено: {s['approved']}\n"
        f"Отклонено: {s['rejected']}\n"
        f"Архив: {s['archived']}",
    )


@router.message(F.text == "/archive")
async def cmd_archive(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    count = archive_old_applications(ARCHIVE_DAYS)
    await message.answer(f"Архивировано заявок: {count}")


@router.message(F.text == "/ban_user")
async def cmd_ban_user(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /ban_user <tg_id>")
        return
    user_id = int(parts[1])
    BAN_USER_IDS.add(user_id)
    await message.answer(f"Пользователь {user_id} добавлен в бан-лист.")


@router.message(F.text == "/unban_user")
async def cmd_unban_user(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /unban_user <tg_id>")
        return
    user_id = int(parts[1])
    BAN_USER_IDS.discard(user_id)
    await message.answer(f"Пользователь {user_id} удалён из бан-листа.")


@router.message(F.text == "/ban_nick")
async def cmd_ban_nick(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /ban_nick <nick>")
        return
    nick = parts[1].strip()
    if not nick:
        await message.answer("Ник пустой.")
        return
    BAN_NICKS.add(nick.lower())
    await message.answer(f"Ник {nick} добавлен в бан-лист.")


@router.message(F.text == "/unban_nick")
async def cmd_unban_nick(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /unban_nick <nick>")
        return
    nick = parts[1].strip()
    BAN_NICKS.discard(nick.lower())
    await message.answer(f"Ник {nick} удалён из бан-листа.")

@router.message(F.text == BTN_APPLY)
async def on_btn_apply(message: Message, bot: Bot):
    await cmd_apply(message, bot)


@router.message(F.text == BTN_STATUS)
async def on_btn_status(message: Message):
    await cmd_status(message)


@router.message(F.text == BTN_ADMIN)
async def on_btn_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    await message.answer("🛠️ *Админ панель*", parse_mode=ParseMode.MARKDOWN, reply_markup=build_admin_menu())


@router.message(F.text == BTN_PENDING)
async def on_btn_pending(message: Message, bot: Bot):
    await cmd_pending(message, bot, 1)


@router.message(F.text == BTN_SHOW)
async def on_btn_show(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    PENDING_INPUT_MODE[message.from_user.id] = "show"
    await message.answer("🔎 Введи ID заявки:")


@router.message(F.text == BTN_SEARCH)
async def on_btn_search(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    PENDING_INPUT_MODE[message.from_user.id] = "search"
    await message.answer("Введите поиск (ник, @username или id):")


@router.message(F.text == BTN_STATS)
async def on_btn_stats(message: Message):
    await cmd_stats(message)


@router.message(F.text == BTN_HEALTH)
async def on_btn_health(message: Message):
    await cmd_health(message)


@router.message(F.text == BTN_HELP)
async def on_btn_help(message: Message):
    await cmd_help(message)


@router.message(F.text == BTN_BACK)
async def on_btn_back(message: Message):
    await message.answer("🏠 Главное меню:", reply_markup=build_main_menu(message.from_user.id))


@router.message(F.text == BTN_SUPPORT)
async def on_btn_support(message: Message):
    await cmd_support(message)


@router.message(F.text == BTN_FEEDBACK)
async def on_btn_feedback(message: Message):
    await cmd_feedback(message)


@router.message(F.text == BTN_SUPPORT_DONE)
async def on_btn_support_done(message: Message, bot: Bot):
    await finalize_support(bot, message)


@router.message(F.text == BTN_SUPPORT_CANCEL)
async def on_btn_support_cancel(message: Message):
    session_id = get_active_support_session(message.from_user.id)
    if not session_id:
        await message.answer("Нет активного обращения.")
        return
    close_support_session(session_id)
    await message.answer("Обращение отменено.", reply_markup=build_main_menu(message.from_user.id))


@router.message(F.text == BTN_BAN_USER)
async def on_btn_ban_user(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    PENDING_INPUT_MODE[message.from_user.id] = "ban_user"
    await message.answer("Введите Telegram ID для бана:")


@router.message(F.text == BTN_UNBAN_USER)
async def on_btn_unban_user(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    PENDING_INPUT_MODE[message.from_user.id] = "unban_user"
    await message.answer("Введите Telegram ID для разбана:")


@router.message(F.text == BTN_BAN_NICK)
async def on_btn_ban_nick(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    PENDING_INPUT_MODE[message.from_user.id] = "ban_nick"
    await message.answer("Введите ник для бана:")


@router.message(F.text == BTN_UNBAN_NICK)
async def on_btn_unban_nick(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    PENDING_INPUT_MODE[message.from_user.id] = "unban_nick"
    await message.answer("Введите ник для разбана:")


@router.message(F.text == BTN_ARCHIVE)
async def on_btn_archive(message: Message):
    await cmd_archive(message)


@router.message(F.text)
async def on_text(message: Message, bot: Bot):
    if message.text and message.text.startswith("/"):
        return

    user_id = message.from_user.id

    if user_id in PENDING_REJECT_REASON:
        app_id = PENDING_REJECT_REASON.pop(user_id, None)
        if not app_id:
            return
        reason = (message.text or "").strip()
        if reason == "-" or not reason:
            reason = None

        app = get_application(app_id)
        if not app:
            await message.answer("Заявка не найдена.")
            return

        (
            _id,
            target_user_id,
            _username,
            _nick,
            _q_name,
            _q_age,
            _q_mods,
            _q_voice_listen,
            _q_voice_speak,
            _q_device,
            _q_plans,
            _q_host,
            status,
            _created,
        ) = app
        if status != "pending":
            await message.answer(f"Заявка уже обработана. Статус: {status}")
            return

        set_status(app_id, "rejected")
        await message.answer(f"Заявка #{app_id} отклонена.")
        try:
            if reason:
                await bot.send_message(target_user_id, f"Ваша заявка #{app_id} отклонена.\nПричина: {reason}")
            else:
                await bot.send_message(target_user_id, f"Ваша заявка #{app_id} отклонена.")
        except Exception:
            pass
        return

    session_id = get_active_support_session(user_id)
    if session_id:
        text = (message.text or "").strip()
        if text:
            add_support_message(session_id, "text", text, None, None)
            await message.answer("Сообщение добавлено. Можешь отправить ещё или нажать «Отправить».")
        return

    if user_id in PENDING_INPUT_MODE:
        mode = PENDING_INPUT_MODE.pop(user_id)
        text = (message.text or "").strip()
        if mode == "feedback":
            if not text:
                await message.answer("Сообщение пустое. Попробуй ещё раз.")
                return
            kind = "feedback"
            with db_connect() as conn:
                conn.execute(
                    "INSERT INTO feedback (user_id, username, kind, message, created_at) VALUES (?, ?, ?, ?, ?)",
                    (
                        user_id,
                        message.from_user.username,
                        kind,
                        text,
                        int(time.time()),
                    ),
                )
            await message.answer("✅ Спасибо! Отзыв отправлен.")
            selected = SELECTED_NICK_BY_USER.get(user_id)
            approved = selected or get_latest_approved_nick(user_id) or get_first_approved_nick(user_id)
            await notify_admins(
                bot,
                f"💬 Отзыв\n"
                f"От: @{message.from_user.username or 'без username'} (id {user_id})\n"
                f"Одобренный ник: {approved or 'нет'}\n"
                f"Сообщение: {text}",
            )
            return
        if mode == "show":
            if not text.isdigit():
                await message.answer("Нужен числовой ID заявки.")
                return
            await cmd_show(Message.model_validate({**message.model_dump(), "text": f"/show {text}"}), bot)
            return
        if mode == "search":
            rows = search_applications(text, 10)
            if not rows:
                await message.answer("Ничего не найдено.")
                return
            lines = ["🔎 Результаты поиска (до 10):"]
            for row in rows:
                app_id, user_id2, username, nick, q_name, q_age, _q_mods, _q_voice_listen, _q_voice_speak, _q_device, _q_plans, _q_host, status, _created = row
                who = q_name or (f"@{username}" if username else f"id {user_id2}")
                lines.append(f"#{app_id} — {nick} — {who} — {status} — возраст: {q_age or '—'}")
            await message.answer("\n".join(lines))
            return
        if mode == "ban_user":
            if not text.isdigit():
                await message.answer("Нужен числовой Telegram ID.")
                return
            BAN_USER_IDS.add(int(text))
            await message.answer(f"Пользователь {text} добавлен в бан-лист.")
            return
        if mode == "unban_user":
            if not text.isdigit():
                await message.answer("Нужен числовой Telegram ID.")
                return
            BAN_USER_IDS.discard(int(text))
            await message.answer(f"Пользователь {text} удалён из бан-листа.")
            return
        if mode == "ban_nick":
            if not text:
                await message.answer("Ник пустой.")
                return
            BAN_NICKS.add(text.lower())
            await message.answer(f"Ник {text} добавлен в бан-лист.")
            return
        if mode == "unban_nick":
            if not text:
                await message.answer("Ник пустой.")
                return
            BAN_NICKS.discard(text.lower())
            await message.answer(f"Ник {text} удалён из бан-листа.")
            return

    state = load_form_state(user_id)
    if not state:
        return

    step, _data, _editing_app_id = state
    key, _title, _hint = QUESTIONS[step]
    text = (message.text or "").strip()

    if key == "nick" and (len(text) < 3 or len(text) > 16):
        await message.answer("Ник должен быть 3-16 символов.")
        await send_next_question(bot, message.chat.id, step)
        return
    if key == "nick" and not MINECRAFT_NICK_RE.match(text):
        await message.answer("Ник может содержать только латинские буквы, цифры и _ (3-16 символов).")
        await send_next_question(bot, message.chat.id, step)
        return
    if key == "nick" and text.lower() in BAN_NICKS:
        await message.answer("Этот ник в бан-листе.")
        await send_next_question(bot, message.chat.id, step)
        return
    if key == "age":
        if not text.isdigit() or not (7 <= int(text) <= 100):
            await message.answer("Возраст должен быть числом (7-100).")
            await send_next_question(bot, message.chat.id, step)
            return

    if key in YES_NO_KEYS:
        lower = text.lower()
        if lower in {"да", "yes", "y"}:
            next_step = save_and_advance(message.from_user, key, "Да")
            if next_step is not None:
                if next_step < len(QUESTIONS):
                    await send_next_question(bot, message.chat.id, next_step)
                else:
                    await finalize_application_for_user(bot, message.from_user, message.chat.id)
            return
        if lower in {"нет", "no", "n"}:
            next_step = save_and_advance(message.from_user, key, "Нет")
            if next_step is not None:
                if next_step < len(QUESTIONS):
                    await send_next_question(bot, message.chat.id, next_step)
                else:
                    await finalize_application_for_user(bot, message.from_user, message.chat.id)
            return
        await message.answer("Выбери вариант кнопкой ✅ Да / ❌ Нет.")
        await send_next_question(bot, message.chat.id, step)
        return

    if not text:
        await message.answer("Ответ не может быть пустым.")
        await send_next_question(bot, message.chat.id, step)
        return

    next_step = save_and_advance(message.from_user, key, text)
    if next_step is not None:
        if next_step < len(QUESTIONS):
            await send_next_question(bot, message.chat.id, next_step)
        else:
            await finalize_application_for_user(bot, message.from_user, message.chat.id)


@router.message(F.photo | F.document | F.video | F.audio | F.voice)
async def on_support_media(message: Message):
    session_id = get_active_support_session(message.from_user.id)
    if not session_id:
        return
    if message.photo:
        file_id = message.photo[-1].file_id
        add_support_message(session_id, "media", None, file_id, "photo")
    elif message.document:
        add_support_message(session_id, "media", None, message.document.file_id, "document")
    elif message.video:
        add_support_message(session_id, "media", None, message.video.file_id, "video")
    elif message.audio:
        add_support_message(session_id, "media", None, message.audio.file_id, "audio")
    elif message.voice:
        add_support_message(session_id, "media", None, message.voice.file_id, "voice")
    await message.answer("Файл добавлен. Можешь отправить ещё или нажать «Отправить».")


@router.callback_query(F.data)
async def on_callback(call: CallbackQuery, bot: Bot):
    data = call.data or ""
    user_id = call.from_user.id

    if data == "cancel_form":
        state = load_form_state(user_id)
        if state:
            clear_form_state(user_id)
            await call.message.edit_text("🗑️ Анкета отменена.")
        else:
            await call.answer("Нет активной анкеты.")
        return

    if data.startswith("pending_page:"):
        if not is_admin(user_id):
            await call.answer("Нет прав.")
            return
        try:
            page = int(data.split(":", 1)[1])
        except Exception:
            page = 1
        await cmd_pending(call.message, bot, page)
        await call.answer()
        return

    if data.startswith("ans:"):
        parts = data.split(":")
        if len(parts) != 3:
            await call.answer("Некорректные данные.")
            return
        key = parts[1]
        val = parts[2]
        if key not in YES_NO_KEYS:
            await call.answer("Некорректный вопрос.")
            return
        value = "Да" if val == "yes" else "Нет"
        next_step = save_and_advance(call.from_user, key, value)
        if next_step is not None:
            if next_step < len(QUESTIONS):
                await send_next_question(bot, call.message.chat.id, next_step)
            else:
                await finalize_application_for_user(bot, call.from_user, call.message.chat.id)
        await call.answer()
        return

    if data.startswith("picknick:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            await call.answer("Некорректные данные.")
            return
        mode = parts[1]
        nick = parts[2]
        SELECTED_NICK_BY_USER[user_id] = nick
        if mode == "support":
            session_id = start_support_session(call.from_user.id, call.from_user.username, nick)
            text = (
                "🛟 *Техподдержка*\n"
                "Опиши проблему и можешь прикрепить файлы/фото.\n"
                "Когда закончишь — нажми «Отправить».\n"
                f"Также можно написать в чат: {SUPPORT_CHAT_URL}\n"
                f"ID обращения: `{session_id}`"
            )
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text=BTN_SUPPORT_DONE), KeyboardButton(text=BTN_SUPPORT_CANCEL)]],
                resize_keyboard=True,
            )
            await bot.send_message(call.message.chat.id, text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard, disable_web_page_preview=True)
        elif mode == "feedback":
            PENDING_INPUT_MODE[user_id] = "feedback"
            text = (
                "💬 *Отзыв*\n"
                "Напиши короткий отзыв: что понравилось, что улучшить."
            )
            await bot.send_message(call.message.chat.id, text, parse_mode=ParseMode.MARKDOWN)
        await call.answer()
        return

    if data.startswith("show:"):
        if not is_admin(user_id):
            await call.answer("Нет прав.")
            return
        try:
            app_id = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Некорректный ID.")
            return
        app = get_application(app_id)
        if not app:
            await call.answer("Заявка не найдена.")
            return
        (
            _id,
            user_id2,
            username,
            nick,
            q_name,
            q_age,
            q_mods,
            q_voice_listen,
            q_voice_speak,
            q_device,
            q_plans,
            q_host,
            status,
            created_at,
        ) = app
        username_part = f"@{username}" if username else "без username"
        who = f"{q_name or '—'} / {username_part} / id {user_id2}"
        text = (
            f"*Заявка* `#{app_id}`\n"
            f"🎮 Ник: `{nick}`\n"
            f"👤 Как обращаться: {q_name or '—'}\n"
            f"🎂 Возраст: {q_age or '—'}\n"
            f"🧩 Моды/версии: {q_mods or '—'}\n"
            f"🎧 Войс (слушать): {q_voice_listen or '—'}\n"
            f"🎤 Войс (говорить): {q_voice_speak or '—'}\n"
            f"💻 Устройство: {q_device or '—'}\n"
            f"🧭 Планы: {q_plans or '—'}\n"
            f"💰 Хост: {q_host or '—'}\n"
            f"📎 От: {who}\n"
            f"📍 Статус: *{status}*\n"
            f"🕒 Дата: {format_date(created_at)}"
        )
        keyboard = None
        if status == "pending":
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        make_button("✅ Одобрить", f"approve:{app_id}", style="positive"),
                        make_button("❌ Отклонить", f"reject:{app_id}", style="negative"),
                        make_button("⛔ Без причины", f"reject_noreason:{app_id}", style="negative"),
                    ]
                ]
            )
        await bot.send_message(call.message.chat.id, text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
        await call.answer()
        return

    if data.startswith("reject_noreason:"):
        if not is_admin(user_id):
            await call.answer("Нет прав.")
            return
        try:
            app_id = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Некорректный ID.")
            return
        app = get_application(app_id)
        if not app:
            await call.answer("Заявка не найдена.")
            return
        (
            _id,
            target_user_id,
            _username,
            _nick,
            _q_name,
            _q_age,
            _q_mods,
            _q_voice_listen,
            _q_voice_speak,
            _q_device,
            _q_plans,
            _q_host,
            status,
            _created,
        ) = app
        if status != "pending":
            await call.answer(f"Уже обработана: {status}")
            return
        set_status(app_id, "rejected")
        await call.message.edit_text(f"Заявка #{app_id} отклонена.")
        try:
            await bot.send_message(target_user_id, f"Ваша заявка #{app_id} отклонена.")
        except Exception:
            pass
        await call.answer()
        return

    if data.startswith("reason:"):
        if not is_admin(user_id):
            await call.answer("Нет прав.")
            return
        parts = data.split(":")
        if len(parts) != 3:
            await call.answer("Некорректные данные.")
            return
        try:
            app_id = int(parts[1])
            idx = int(parts[2]) - 1
        except Exception:
            await call.answer("Некорректные данные.")
            return
        if idx < 0 or idx >= len(REASON_TEMPLATES):
            await call.answer("Некорректный шаблон.")
            return
        reason = REASON_TEMPLATES[idx]
        app = get_application(app_id)
        if not app:
            await call.answer("Заявка не найдена.")
            return
        (
            _id,
            target_user_id,
            _username,
            _nick,
            _q_name,
            _q_age,
            _q_mods,
            _q_voice_listen,
            _q_voice_speak,
            _q_device,
            _q_plans,
            _q_host,
            status,
            _created,
        ) = app
        if status != "pending":
            await call.answer(f"Уже обработана: {status}")
            return
        set_status(app_id, "rejected")
        await call.message.edit_text(f"Заявка #{app_id} отклонена.")
        try:
            await bot.send_message(target_user_id, f"Ваша заявка #{app_id} отклонена.\nПричина: {reason}")
        except Exception:
            pass
        await call.answer()
        return

    if not is_admin(user_id):
        await call.answer("Нет прав.")
        return

    if ":" not in data:
        await call.message.edit_text("Некорректные данные.")
        return

    action, app_id_str = data.split(":", 1)
    if not app_id_str.isdigit():
        await call.message.edit_text("Некорректный ID заявки.")
        return

    app_id = int(app_id_str)
    await process_decision(bot, app_id, action, call.message.chat.id)


async def main():
    db_connect().close()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
