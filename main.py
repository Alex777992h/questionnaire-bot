import os
import json
import time
import re
import sqlite3
import logging
import csv
import zipfile
from datetime import datetime
from typing import Optional, Tuple, List, Dict

import aiohttp
from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.types import (
    Message,
    CallbackQuery,
    ChatMemberUpdated,
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
    "rate_limit_seconds": 2,
    "support_auto_close_hours": 12,
    "cleanup_interval_minutes": 60,
    "support_retention_days": 60,
    "form_state_retention_days": 2,
    "tickets_page_size": 5,
    "bot_username": "infinitycraftmembers_bot",
    "group_chat_ids": [],
    "reminder_interval_minutes": 120,
    "pending_reminder_hours": 24,
    "ticket_reminder_hours": 24,
    "profanity_words": ["мат", "хуй", "пизд", "еб", "сука", "бля"],
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
RATE_LIMIT_SECONDS = int(DEFAULT_CONFIG.get("rate_limit_seconds", 2))
SUPPORT_AUTO_CLOSE_SECONDS = int(DEFAULT_CONFIG.get("support_auto_close_hours", 12)) * 3600
CLEANUP_INTERVAL_SECONDS = int(DEFAULT_CONFIG.get("cleanup_interval_minutes", 60)) * 60
SUPPORT_RETENTION_DAYS = int(DEFAULT_CONFIG.get("support_retention_days", 60))
FORM_STATE_RETENTION_DAYS = int(DEFAULT_CONFIG.get("form_state_retention_days", 2))
TICKETS_PAGE_SIZE = int(DEFAULT_CONFIG.get("tickets_page_size", 5))
BOT_USERNAME = DEFAULT_CONFIG.get("bot_username", "").lstrip("@")
APPLY_DEEPLINK = f"https://t.me/{BOT_USERNAME}?start=apply" if BOT_USERNAME else ""
GROUP_CHAT_IDS = set(DEFAULT_CONFIG.get("group_chat_ids", []))
REMINDER_INTERVAL_SECONDS = int(DEFAULT_CONFIG.get("reminder_interval_minutes", 120)) * 60
PENDING_REMINDER_SECONDS = int(DEFAULT_CONFIG.get("pending_reminder_hours", 24)) * 3600
TICKET_REMINDER_SECONDS = int(DEFAULT_CONFIG.get("ticket_reminder_hours", 24)) * 3600
PROFANITY_WORDS = [w.lower() for w in DEFAULT_CONFIG.get("profanity_words", [])]

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
BTN_ACCOUNTS = "👥 Аккаунты"
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
BTN_CANCEL = "❌ Отменить"
BTN_SKIP = "⏭️ Пропустить"

TICKET_TOPICS = [
    ("server", "🌍 Проблемы на сервере"),
    ("account", "👤 Аккаунт/доступ"),
    ("rules", "📜 Вопросы по правилам"),
    ("tech", "🧩 Техпроблема"),
    ("bot", "🤖 Проблема с ботом"),
]

BRAND = "INFINITY CRAFT"
ACCENT_LINE = "━━━━━━━━━━━━━━━━━━"


def fmt_header(title: str) -> str:
    return f"✨ *{title}*\n{ACCENT_LINE}"


def fmt_section(title: str, body: str) -> str:
    return f"\n*{title}*\n{body}"


def fmt_kv(label: str, value: str) -> str:
    return f"{label}: {value}"

LAST_PLUGIN_ALERT_TS = 0
LAST_CLEANUP_TS = 0
LAST_REMINDER_TS = 0
PENDING_REJECT_REASON: Dict[int, int] = {}
PENDING_INPUT_MODE: Dict[int, str] = {}
SELECTED_NICK_BY_USER: Dict[int, str] = {}
TICKET_DRAFT_BY_USER: Dict[int, Dict[str, str]] = {}
FEEDBACK_DRAFT_BY_USER: Dict[int, Dict[str, str]] = {}
LAST_ACTION_TS: Dict[tuple, float] = {}


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
            target TEXT,
            rating INTEGER,
            message TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            nick TEXT NOT NULL UNIQUE,
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS account_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            nick TEXT NOT NULL,
            action TEXT NOT NULL,
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
            topic TEXT,
            subject TEXT,
            selected_nick TEXT,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app_id INTEGER NOT NULL,
            admin_id INTEGER NOT NULL,
            admin_username TEXT,
            action TEXT NOT NULL,
            reason TEXT,
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_prefs (
            user_id INTEGER PRIMARY KEY,
            selected_nick TEXT
        )
        """
    )
    ensure_columns(conn)
    ensure_support_columns(conn)
    return conn


def save_config_group_ids():
    save_config_updates({"group_chat_ids": sorted(GROUP_CHAT_IDS), "bot_username": BOT_USERNAME})


def save_config_updates(updates: dict):
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        else:
            cfg = {}
        cfg.update(updates)
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


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
    cur.execute("PRAGMA table_info(feedback)")
    fb_existing = {row[1] for row in cur.fetchall()}
    if "target" not in fb_existing:
        try:
            conn.execute("ALTER TABLE feedback ADD COLUMN target TEXT")
        except sqlite3.Error:
            pass
    if "rating" not in fb_existing:
        try:
            conn.execute("ALTER TABLE feedback ADD COLUMN rating INTEGER")
        except sqlite3.Error:
            pass
    cur.execute("PRAGMA table_info(user_accounts)")
    _ = cur.fetchall()


def ensure_support_columns(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(support_sessions)")
    existing = {row[1] for row in cur.fetchall()}
    if "subject" not in existing:
        try:
            conn.execute("ALTER TABLE support_sessions ADD COLUMN subject TEXT")
        except sqlite3.Error:
            pass
    if "topic" not in existing:
        try:
            conn.execute("ALTER TABLE support_sessions ADD COLUMN topic TEXT")
        except sqlite3.Error:
            pass
    if "selected_nick" not in existing:
        try:
            conn.execute("ALTER TABLE support_sessions ADD COLUMN selected_nick TEXT")
        except sqlite3.Error:
            pass
    if "updated_at" not in existing:
        try:
            conn.execute("ALTER TABLE support_sessions ADD COLUMN updated_at INTEGER")
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
    nicks = get_user_accounts(user_id)
    return nicks[0] if nicks else None


def get_latest_approved_nick(user_id: int) -> Optional[str]:
    nicks = get_user_accounts(user_id)
    return nicks[-1] if nicks else None


def get_approved_nicks(user_id: int) -> List[str]:
    return get_user_accounts(user_id)


def _bootstrap_accounts_from_applications(user_id: int):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT nick FROM applications WHERE user_id = ? AND status = 'approved' ORDER BY id ASC",
            (user_id,),
        )
        rows = [r[0] for r in cur.fetchall()]
        for nick in rows:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO user_accounts (user_id, nick, created_at) VALUES (?, ?, ?)",
                    (user_id, nick, int(time.time())),
                )
            except sqlite3.Error:
                pass


def get_user_accounts(user_id: int) -> List[str]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT nick FROM user_accounts WHERE user_id = ? ORDER BY id ASC",
            (user_id,),
        )
        rows = [row[0] for row in cur.fetchall()]
    if not rows:
        _bootstrap_accounts_from_applications(user_id)
        with db_connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT nick FROM user_accounts WHERE user_id = ? ORDER BY id ASC",
                (user_id,),
            )
            rows = [row[0] for row in cur.fetchall()]
    return rows


def count_user_accounts(user_id: int) -> int:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM user_accounts WHERE user_id = ?", (user_id,))
        return cur.fetchone()[0]


def account_exists(nick: str) -> bool:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM user_accounts WHERE lower(nick) = lower(?) LIMIT 1", (nick,))
        return cur.fetchone() is not None


def add_user_account(user_id: int, nick: str) -> bool:
    with db_connect() as conn:
        try:
            conn.execute(
                "INSERT INTO user_accounts (user_id, nick, created_at) VALUES (?, ?, ?)",
                (user_id, nick, int(time.time())),
            )
            return True
        except sqlite3.Error:
            return False


def remove_user_account(user_id: int, nick: str) -> bool:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM user_accounts WHERE user_id = ? AND lower(nick) = lower(?)", (user_id, nick))
        return cur.rowcount > 0


def log_account_action(user_id: int, nick: str, action: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO account_log (user_id, nick, action, created_at) VALUES (?, ?, ?, ?)",
            (user_id, nick, action, int(time.time())),
        )


def get_user_selected_nick(user_id: int) -> Optional[str]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT selected_nick FROM user_prefs WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def set_user_selected_nick(user_id: int, nick: Optional[str]):
    with db_connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO user_prefs (user_id, selected_nick) VALUES (?, ?)",
            (user_id, nick),
        )


def log_decision(app_id: int, admin_id: int, admin_username: Optional[str], action: str, reason: Optional[str]):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO decision_log (app_id, admin_id, admin_username, action, reason, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (app_id, admin_id, admin_username, action, reason, int(time.time())),
        )


def is_rate_limited(user_id: int, action: str, seconds: int) -> bool:
    now = time.time()
    key = (user_id, action)
    last = LAST_ACTION_TS.get(key, 0.0)
    if now - last < seconds:
        return True
    LAST_ACTION_TS[key] = now
    return False


def cleanup_old_data():
    now = int(time.time())
    support_cutoff = now - (SUPPORT_RETENTION_DAYS * 86400)
    form_cutoff = now - (FORM_STATE_RETENTION_DAYS * 86400)
    try:
        archive_old_applications(ARCHIVE_DAYS)
    except Exception:
        pass
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM support_sessions WHERE status = 'closed' AND created_at < ?",
            (support_cutoff,),
        )
        old_ids = [row[0] for row in cur.fetchall()]
        if old_ids:
            placeholders = ",".join("?" for _ in old_ids)
            conn.execute(f"DELETE FROM support_messages WHERE session_id IN ({placeholders})", old_ids)
            conn.execute(f"DELETE FROM support_sessions WHERE id IN ({placeholders})", old_ids)
        conn.execute("DELETE FROM form_states WHERE updated_at < ?", (form_cutoff,))


def maybe_cleanup():
    global LAST_CLEANUP_TS
    now = time.time()
    if now - LAST_CLEANUP_TS < CLEANUP_INTERVAL_SECONDS:
        return
    LAST_CLEANUP_TS = now
    try:
        cleanup_old_data()
    except Exception:
        pass


async def maybe_send_reminders(bot: Bot):
    global LAST_REMINDER_TS
    now = time.time()
    if now - LAST_REMINDER_TS < REMINDER_INTERVAL_SECONDS:
        return
    LAST_REMINDER_TS = now
    try:
        pending_old = get_pending_older_than(PENDING_REMINDER_SECONDS)
        stale_tickets = get_stale_tickets(TICKET_REMINDER_SECONDS)
        if pending_old > 0:
            await notify_admins(
                bot,
                f"{fmt_header('Напоминание')}\n"
                f"{fmt_kv('Заявки без ответа', str(pending_old))}\n"
                f"{fmt_kv('Порог', f'{PENDING_REMINDER_SECONDS // 3600} ч')}",
            )
        if stale_tickets > 0:
            await notify_admins(
                bot,
                f"{fmt_header('Напоминание')}\n"
                f"{fmt_kv('Тикеты ждут ответа', str(stale_tickets))}\n"
                f"{fmt_kv('Порог', f'{TICKET_REMINDER_SECONDS // 3600} ч')}",
            )
    except Exception:
        pass


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
        cur.execute("SELECT AVG(rating) FROM feedback WHERE kind = 'bot_rating'")
        avg_rating_row = cur.fetchone()
        avg_rating = float(avg_rating_row[0]) if avg_rating_row and avg_rating_row[0] is not None else 0.0
    return {
        "total": total,
        "pending": pending,
        "approved": approved,
        "rejected": rejected,
        "archived": archived,
        "avg_rating": avg_rating,
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


def create_ticket(user_id: int, username: Optional[str], selected_nick: Optional[str], topic: str, subject: str) -> int:
    with db_connect() as conn:
        cur = conn.cursor()
        now = int(time.time())
        cur.execute(
            "INSERT INTO support_sessions (user_id, username, topic, subject, selected_nick, status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, username, topic, subject, selected_nick, "open", now, now),
        )
        return cur.lastrowid


def get_open_ticket(user_id: int) -> Optional[int]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, updated_at, created_at FROM support_sessions WHERE user_id = ? AND status = 'open' ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        ticket_id, updated_at, created_at = row
        last_ts = updated_at or created_at
        if last_ts and int(time.time()) - int(last_ts) > SUPPORT_AUTO_CLOSE_SECONDS:
            conn.execute("UPDATE support_sessions SET status = 'closed' WHERE id = ?", (ticket_id,))
            return None
        return ticket_id


def get_ticket(ticket_id: int):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, username, topic, subject, selected_nick, status, created_at, updated_at "
            "FROM support_sessions WHERE id = ?",
            (ticket_id,),
        )
        return cur.fetchone()


def get_ticket_selected_nick(ticket_id: int) -> Optional[str]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT selected_nick FROM support_sessions WHERE id = ?", (ticket_id,))
        row = cur.fetchone()
        return row[0] if row else None


def set_ticket_status(ticket_id: int, status: str):
    with db_connect() as conn:
        conn.execute("UPDATE support_sessions SET status = ?, updated_at = ? WHERE id = ?", (status, int(time.time()), ticket_id))


def add_ticket_message(ticket_id: int, kind: str, text: Optional[str], file_id: Optional[str], file_type: Optional[str]):
    with db_connect() as conn:
        now = int(time.time())
        conn.execute(
            "INSERT INTO support_messages (session_id, kind, text, file_id, file_type, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (ticket_id, kind, text, file_id, file_type, now),
        )
        conn.execute("UPDATE support_sessions SET updated_at = ? WHERE id = ?", (now, ticket_id))


def get_ticket_messages(ticket_id: int, limit: Optional[int] = None) -> List[tuple]:
    with db_connect() as conn:
        cur = conn.cursor()
        if limit:
            cur.execute(
                "SELECT kind, text, file_id, file_type, created_at FROM support_messages WHERE session_id = ? ORDER BY id DESC LIMIT ?",
                (ticket_id, limit),
            )
            rows = cur.fetchall()
            return list(reversed(rows))
        cur.execute(
            "SELECT kind, text, file_id, file_type, created_at FROM support_messages WHERE session_id = ? ORDER BY id ASC",
            (ticket_id,),
        )
        return cur.fetchall()


def list_tickets(status: Optional[str], limit: int, offset: int) -> List[tuple]:
    with db_connect() as conn:
        cur = conn.cursor()
        if status:
            cur.execute(
                "SELECT id, user_id, username, topic, subject, status, created_at, updated_at "
                "FROM support_sessions WHERE status = ? ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                (status, limit, offset),
            )
        else:
            cur.execute(
                "SELECT id, user_id, username, topic, subject, status, created_at, updated_at "
                "FROM support_sessions ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            )
        return cur.fetchall()


def list_user_tickets(user_id: int, limit: int, offset: int) -> List[tuple]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, username, topic, subject, status, created_at, updated_at "
            "FROM support_sessions WHERE user_id = ? ORDER BY updated_at DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        )
        return cur.fetchall()


def search_tickets(query: str, limit: int = 10) -> List[tuple]:
    q_raw = (query or "").strip()
    if q_raw.startswith("@"):
        q_raw = q_raw[1:]
    q = f"%{q_raw}%"
    with db_connect() as conn:
        cur = conn.cursor()
        if q_raw.isdigit():
            cur.execute(
                "SELECT id, user_id, username, topic, subject, status, created_at, updated_at "
                "FROM support_sessions WHERE id = ? OR user_id = ? OR username LIKE ? OR subject LIKE ? "
                "ORDER BY updated_at DESC LIMIT ?",
                (int(q_raw), int(q_raw), q, q, limit),
            )
        else:
            cur.execute(
                "SELECT id, user_id, username, topic, subject, status, created_at, updated_at "
                "FROM support_sessions WHERE username LIKE ? OR subject LIKE ? OR topic LIKE ? "
                "ORDER BY updated_at DESC LIMIT ?",
                (q, q, q, limit),
            )
        return cur.fetchall()


def get_ticket_count(status: Optional[str] = None) -> int:
    with db_connect() as conn:
        cur = conn.cursor()
        if status:
            cur.execute("SELECT COUNT(*) FROM support_sessions WHERE status = ?", (status,))
        else:
            cur.execute("SELECT COUNT(*) FROM support_sessions")
        return cur.fetchone()[0]


def get_pending_older_than(seconds: int) -> int:
    cutoff = int(time.time()) - seconds
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM applications WHERE status = 'pending' AND created_at < ?", (cutoff,))
        return cur.fetchone()[0]


def get_stale_tickets(seconds: int) -> int:
    cutoff = int(time.time()) - seconds
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM support_sessions s "
            "JOIN support_messages m ON m.session_id = s.id "
            "WHERE s.status = 'open' AND m.id = (SELECT id FROM support_messages WHERE session_id = s.id ORDER BY id DESC LIMIT 1) "
            "AND m.created_at < ? AND m.kind LIKE 'user_%'",
            (cutoff,),
        )
        return cur.fetchone()[0]


def get_counts_by_day(table: str, days: int = 7) -> List[tuple]:
    since = int(time.time()) - days * 86400
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT strftime('%Y-%m-%d', datetime(created_at, 'unixepoch')) as day, COUNT(*) "
            f"FROM {table} WHERE created_at >= ? GROUP BY day ORDER BY day ASC",
            (since,),
        )
        return cur.fetchall()


def get_admin_decision_stats(limit: int = 5) -> List[tuple]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT admin_id, COALESCE(admin_username, ''), COUNT(*) as cnt "
            "FROM decision_log GROUP BY admin_id, admin_username "
            "ORDER BY cnt DESC LIMIT ?",
            (limit,),
        )
        return cur.fetchall()


def get_avg_decision_time_seconds() -> Optional[int]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT AVG(dl.created_at - a.created_at) "
            "FROM applications a "
            "JOIN decision_log dl ON dl.app_id = a.id "
            "WHERE dl.action IN ('approve','reject')"
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None


def get_avg_first_admin_reply_seconds() -> Optional[int]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT AVG(m.created_at - s.created_at) "
            "FROM support_sessions s "
            "JOIN support_messages m ON m.session_id = s.id "
            "WHERE m.kind LIKE 'admin_%' "
            "AND m.id = (SELECT id FROM support_messages WHERE session_id = s.id AND kind LIKE 'admin_%' ORDER BY id ASC LIMIT 1)"
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None


def format_duration(seconds: Optional[int]) -> str:
    if seconds is None:
        return "—"
    minutes = seconds // 60
    hours = minutes // 60
    if hours > 0:
        return f"{hours}ч {minutes % 60}м"
    return f"{minutes}м"


def get_user_ticket_count(user_id: int) -> int:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM support_sessions WHERE user_id = ?", (user_id,))
        return cur.fetchone()[0]


def get_last_ticket_message_kind(ticket_id: int) -> Optional[str]:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT kind FROM support_messages WHERE session_id = ? ORDER BY id DESC LIMIT 1",
            (ticket_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def has_bot_rating(user_id: int, app_id: int) -> bool:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM feedback WHERE kind = 'bot_rating' AND target = 'bot' AND message = ? AND user_id = ? LIMIT 1",
            (str(app_id), user_id),
        )
        return cur.fetchone() is not None


def build_accounts_panel(nicks: List[str], primary: Optional[str]) -> InlineKeyboardMarkup:
    rows = []
    for n in nicks:
        row = [make_button(f"❌ Удалить {n}", f"acc_del:{n}")]
        if primary and n != primary:
            row.append(make_button("⭐ Сделать основным", f"acc_primary:{n}"))
        rows.append(row)
    if len(nicks) < 2:
        rows.append([make_button("➕ Добавить ник", "acc_add")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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


def contains_profanity(text: str) -> bool:
    t = (text or "").lower()
    return any(w in t for w in PROFANITY_WORDS if w)


def build_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text=BTN_APPLY), KeyboardButton(text=BTN_STATUS)],
        [KeyboardButton(text=BTN_ACCOUNTS)],
        [KeyboardButton(text=BTN_SUPPORT), KeyboardButton(text=BTN_FEEDBACK)],
        [KeyboardButton(text=BTN_HELP)],
    ]
    if is_admin(user_id):
        keyboard.append([KeyboardButton(text=BTN_ADMIN)])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def build_admin_panel() -> InlineKeyboardMarkup:
    rows = [
        [
            make_button("🧾 Активные", "admin:pending"),
            make_button("🔎 По ID", "admin:show"),
        ],
        [
            make_button("🧭 Дашборд", "admin:dashboard"),
        ],
        [
            make_button("🔍 Поиск", "admin:search"),
            make_button("📊 Статистика", "admin:stats"),
        ],
        [
            make_button("📈 Аналитика", "admin:analytics"),
            make_button("📦 Экспорт", "admin:export"),
        ],
        [
            make_button("🗂️ Бэкап", "admin:backup"),
        ],
        [
            make_button("📮 Тикеты", "admin:tickets"),
            make_button("🔎 Тикеты", "admin:ticket_search"),
        ],
        [
            InlineKeyboardButton(text="➕ Добавить в группу", url=f"https://t.me/{BOT_USERNAME}?startgroup=true"),
        ],
        [
            make_button("🩺 Плагин", "admin:health"),
        ],
        [
            make_button("🚫 Бан TG", "admin:ban_user"),
            make_button("✅ Разбан TG", "admin:unban_user"),
        ],
        [
            make_button("🚫 Бан ник", "admin:ban_nick"),
            make_button("✅ Разбан ник", "admin:unban_nick"),
        ],
        [
            make_button("➕ Админ", "admin:add_admin"),
            make_button("➖ Админ", "admin:remove_admin"),
        ],
        [
            make_button("📦 Архив", "admin:archive"),
            make_button("ℹ️ Помощь", "admin:help"),
        ],
        [
            make_button("✖️ Закрыть", "admin:close"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
    nick_val = answers.get("nick", "")
    voice_val = f"{answers.get('voice_listen', '—')} / {answers.get('voice_speak', '—')}"
    text = (
        f"{fmt_header('Новая заявка')}\n"
        f"{fmt_kv('ID', f'`#{app_id}`')}\n"
        f"{fmt_kv('Ник', f'`{nick_val}`')}\n"
        f"{fmt_kv('Как обращаться', answers.get('name', '—'))}\n"
        f"{fmt_kv('Возраст', answers.get('age', '—'))}\n"
        f"{fmt_kv('Войс (слушать/говорить)', voice_val)}\n"
        f"{fmt_kv('От', f'{username_part} (id {user.id})')}"
    )
    await notify_admins(bot, text, keyboard)


async def process_decision(bot: Bot, app_id: int, action: str, admin_chat_id: int, admin_username: Optional[str]):
    app = get_application(app_id)
    if not app:
        await bot.send_message(admin_chat_id, "Заявка не найдена.")
        return

    (
        _id,
        target_user_id,
        _username,
        nick,
        q_name,
        q_age,
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
        if not get_user_selected_nick(target_user_id):
            set_user_selected_nick(target_user_id, nick)
        add_user_account(target_user_id, nick)
        log_decision(app_id, admin_chat_id, admin_username, "approve", None)
        await bot.send_message(admin_chat_id, f"Заявка #{app_id} одобрена.")
        try:
            await bot.send_message(
                target_user_id,
                f"{fmt_header('Заявка одобрена')}\n"
                f"{fmt_kv('ID', f'`#{app_id}`')}\n"
                f"{fmt_kv('Ник', f'`{nick}`')}\n"
                f"{fmt_kv('Чат', CHAT_INVITE_URL)}\n"
                f"{fmt_kv('IP', '`infinity-craft.ru`')}",
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
            if not has_bot_rating(target_user_id, app_id):
                rate_kb = InlineKeyboardMarkup(
                    inline_keyboard=[[
                        make_button("⭐1", f"rate:{app_id}:1"),
                        make_button("⭐2", f"rate:{app_id}:2"),
                        make_button("⭐3", f"rate:{app_id}:3"),
                        make_button("⭐4", f"rate:{app_id}:4"),
                        make_button("⭐5", f"rate:{app_id}:5"),
                        make_button("⏭️ Пропустить", f"rate:{app_id}:skip"),
                    ]]
                )
                await bot.send_message(
                    target_user_id,
                    "Оцени работу бота (1–5):",
                    reply_markup=rate_kb,
                )
        except Exception:
            pass
        if GROUP_CHAT_IDS:
            username_part = f"@{_username}" if _username else "без username"
            group_text = (
                f"{fmt_header('Новый игрок')}\n"
                f"{fmt_kv('Ник', f'`{nick}`')}\n"
                f"{fmt_kv('Как обращаться', q_name or '—')}\n"
                f"{fmt_kv('Возраст', q_age or '—')}\n"
                f"{fmt_kv('Участник', username_part)}"
            )
            for gid in GROUP_CHAT_IDS:
                try:
                    await bot.send_message(gid, group_text, parse_mode=ParseMode.MARKDOWN)
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
        await bot.send_message(chat_id, f"{fmt_header('Заявка обновлена')}\nID: `#{editing_app_id}`", parse_mode=ParseMode.MARKDOWN)
        await notify_admins(bot, f"{fmt_header('Заявка обновлена')}\nID: `#{editing_app_id}`")
        return

    app_id = create_application(user.id, user.username, answers)
    await bot.send_message(chat_id, f"{fmt_header('Заявка отправлена')}\nID: `#{app_id}`\nОжидай решения администрации.", parse_mode=ParseMode.MARKDOWN)
    await send_application_to_admins(bot, app_id, answers, user)


def ticket_status_label(status: str) -> str:
    if status == "closed":
        return "🔒 Закрыт"
    return "🟢 Открыт"


def ticket_wait_label(ticket_id: int, status: str) -> str:
    if status == "closed":
        return "—"
    kind = get_last_ticket_message_kind(ticket_id) or ""
    if kind.startswith("user_"):
        return "Ожидает админа"
    if kind.startswith("admin_"):
        return "Ожидает пользователя"
    return "—"


def build_ticket_admin_keyboard(ticket_id: int, status: str) -> InlineKeyboardMarkup:
    rows = [
        [
            make_button("✉️ Ответить", f"ticket_reply:{ticket_id}"),
        ],
    ]
    if status == "closed":
        rows.append([make_button("🔓 Открыть снова", f"ticket_reopen:{ticket_id}")])
    else:
        rows.append([make_button("✅ Закрыть", f"ticket_close:{ticket_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_ticket_user_keyboard(ticket_id: int, status: str) -> InlineKeyboardMarkup:
    rows = [
        [make_button("✉️ Добавить сообщение", f"ticket_add:{ticket_id}")],
    ]
    if status == "closed":
        rows.append([make_button("🔓 Открыть снова", f"ticket_reopen:{ticket_id}")])
    else:
        rows.append([make_button("✅ Закрыть тикет", f"ticket_close:{ticket_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_ticket_view(bot: Bot, chat_id: int, ticket_id: int, for_admin: bool):
    ticket = get_ticket(ticket_id)
    if not ticket:
        await bot.send_message(chat_id, "Тикет не найден.")
        return
    _id, user_id, username, topic, subject, selected_nick, status, created_at, updated_at = ticket
    username_part = f"@{username}" if username else "без username"
    approved = selected_nick or get_latest_approved_nick(user_id) or get_first_approved_nick(user_id)
    topic_label = next((label for key, label in TICKET_TOPICS if key == topic), topic or "—")
    wait_label = ticket_wait_label(ticket_id, status)
    lines = [
        f"🎫 *Тикет {BRAND}* `#{ticket_id}`",
        f"{ACCENT_LINE}",
        f"🏷️ Категория: {topic_label}",
        f"📝 Тема: {subject or '—'}",
        f"📌 Статус: {ticket_status_label(status)}",
        f"⏳ Ожидание: {wait_label}",
        f"🕒 Создан: {format_date(created_at)}",
    ]
    if updated_at:
        lines.append(f"🔄 Обновлён: {format_date(updated_at)}")
    if for_admin:
        lines.append(f"👤 Автор: {username_part} (id {user_id})")
        lines.append(f"🎮 Одобренный ник: {approved or 'нет'}")
    msgs = get_ticket_messages(ticket_id, limit=5)
    if msgs:
        lines.append("")
        lines.append("💬 *Последние сообщения:*")
        for kind, text, _file_id, _file_type, ts in msgs:
            who = "👤" if kind.startswith("user_") else "🛡️"
            content = text or f"[файл: {_file_type or 'media'}]"
            lines.append(f"{who} {format_date(ts)} — {content}")
    keyboard = build_ticket_admin_keyboard(ticket_id, status) if for_admin else build_ticket_user_keyboard(ticket_id, status)
    await bot.send_message(chat_id, "\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)


async def send_admin_dashboard(bot: Bot, chat_id: int):
    s = get_stats()
    pending_old = get_pending_older_than(PENDING_REMINDER_SECONDS)
    stale_tickets = get_stale_tickets(TICKET_REMINDER_SECONDS)
    avg_decision = format_duration(get_avg_decision_time_seconds())
    avg_first_reply = format_duration(get_avg_first_admin_reply_seconds())
    app_line = f"{s['total']} / {s['pending']} / {s['approved']}"
    avg_rating_line = f"{s['avg_rating']:.2f}⭐"
    text = (
        f"{fmt_header('Дашборд администрации')}\n"
        f"{fmt_kv('Заявки: всего/ожидают/одобрено', app_line)}\n"
        f"{fmt_kv('Тикеты: активные', str(get_ticket_count('open')))}\n"
        f"{fmt_kv('Старые заявки > {PENDING_REMINDER_SECONDS // 3600}ч', str(pending_old))}\n"
        f"{fmt_kv('Тикеты без ответа > {TICKET_REMINDER_SECONDS // 3600}ч', str(stale_tickets))}\n"
        f"{fmt_kv('Среднее решение заявки', avg_decision)}\n"
        f"{fmt_kv('Первый ответ в тикете', avg_first_reply)}\n"
        f"{fmt_kv('Средняя оценка бота', avg_rating_line)}"
    )
    await bot.send_message(chat_id, text, parse_mode=ParseMode.MARKDOWN)


async def send_ticket_list(bot: Bot, chat_id: int, page: int, for_admin: bool, user_id: Optional[int] = None):
    page = max(1, page)
    offset = (page - 1) * TICKETS_PAGE_SIZE
    if for_admin:
        total = get_ticket_count()
        rows = list_tickets(None, TICKETS_PAGE_SIZE, offset)
    else:
        total = get_user_ticket_count(user_id or 0)
        rows = list_user_tickets(user_id or 0, TICKETS_PAGE_SIZE, offset)
    if not rows:
        await bot.send_message(chat_id, "Тикетов пока нет.")
        return
    lines = [f"📮 *Тикеты {BRAND}* (страница {page})", ACCENT_LINE]
    buttons = []
    for tid, uid, username, topic, subject, status, created_at, updated_at in rows:
        subject_short = (subject or "Без темы")[:24]
        topic_label = next((label for key, label in TICKET_TOPICS if key == topic), topic or "—")
        status_label = ticket_status_label(status)
        lines.append(f"`#{tid}` — {topic_label} — {subject_short} — {status_label}")
        cb = f"ticket_admin_view:{tid}" if for_admin else f"ticket_user_view:{tid}"
        buttons.append([make_button(f"Открыть #{tid}", cb)])
    nav = []
    if page > 1:
        nav.append(make_button("⬅️", f"tickets_page:{'admin' if for_admin else 'user'}:{page - 1}"))
    if offset + TICKETS_PAGE_SIZE < total:
        nav.append(make_button("➡️", f"tickets_page:{'admin' if for_admin else 'user'}:{page + 1}"))
    if nav:
        buttons.append(nav)
    await bot.send_message(chat_id, "\n".join(lines), parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.message(F.text.startswith("/start"))
async def cmd_start(message: Message):
    args = (message.text or "").split(maxsplit=1)
    payload = args[1].strip() if len(args) > 1 else ""
    if payload == "apply":
        await cmd_apply(message, message.bot)
        return
    rules_text = (
        "• Пиши честно и по делу\n"
        "• Не флуди и не отправляй дубликаты\n"
        "• Без оскорблений и мата\n"
        "• Ник: латиница, цифры и `_` (3–16)"
    )
    commands_text = "`/apply` — подать заявку\n`/status` — статус заявки"
    text = (
        f"{fmt_header(BRAND)}\n"
        "👋 Привет! Я помогу подать заявку и следить за её статусом.\n"
        f"{fmt_section('Правила бота', rules_text)}\n"
        f"{fmt_section('Быстрые команды', commands_text)}"
    )
    if is_admin(message.from_user.id):
        text += "\n`/admin` — админ панель"
    if APPLY_DEEPLINK:
        text += f"\n\n🔗 Быстрая подача заявки: `{APPLY_DEEPLINK}`"
    await message.answer(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_main_menu(message.from_user.id),
    )


@router.message(F.text == "/help")
async def cmd_help(message: Message):
    base_section = (
        "`/apply` — подать заявку\n"
        "`/status` — статус заявки\n"
        "`/cancel` — отменить анкету\n"
        "`/edit` — редактировать активную заявку"
    )
    support_section = (
        "`/support` — техподдержка (тикеты)\n"
        "`/my_tickets` — мои тикеты\n"
        "`/feedback` — оставить отзыв"
    )
    admin_section = (
        "`/admin` — админ панель\n"
        "`/dashboard` — дашборд\n"
        "`/analytics` — аналитика\n"
        "`/export` — экспорт CSV\n"
        "`/backup` — бэкап (config + db)\n"
        "`/tickets` — тикеты\n"
        "`/ticket_search` — поиск тикетов\n"
        "`/ban_user <tg_id>` — бан TG\n"
        "`/unban_user <tg_id>` — разбан TG\n"
        "`/ban_nick <nick>` — бан ника\n"
        "`/unban_nick <nick>` — разбан ника\n"
        "`/archive` — архивировать старые заявки"
    )
    text = (
        f"{fmt_header('Справка ' + BRAND)}\n"
        f"{fmt_section('Основные', base_section)}\n"
        f"{fmt_section('Поддержка', support_section)}\n"
        f"{fmt_section('Админ', admin_section)}"
    )
    if APPLY_DEEPLINK:
        text += f"\n\n🔗 Быстрая заявка: `{APPLY_DEEPLINK}`"
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


@router.message(F.text == "/apply")
async def cmd_apply(message: Message, bot: Bot):
    maybe_cleanup()
    await maybe_send_reminders(bot)
    if is_rate_limited(message.from_user.id, "apply", RATE_LIMIT_SECONDS):
        await message.answer("Подожди секунду и попробуй снова.")
        return
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
        f"{fmt_header('Анкета ' + BRAND)}\n"
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
        f"{fmt_header('Редактирование заявки')}\n"
        "Ответь на вопросы по порядку.\n"
        "Отмена: `/cancel`",
        parse_mode=ParseMode.MARKDOWN,
    )
    await send_next_question(bot, message.chat.id, 0)


@router.message(F.text == "/status")
async def cmd_status(message: Message):
    maybe_cleanup()
    last = get_last_application(message.from_user.id)
    if not last:
        await message.answer("У тебя нет заявок.")
        return

    app_id, nick, status, created_at = last
    await message.answer(
        f"{fmt_header('Статус заявки')}\n"
        f"{fmt_kv('ID', f'`#{app_id}`')}\n"
        f"{fmt_kv('Ник', f'`{nick}`')}\n"
        f"{fmt_kv('Статус', f'*{status}*')}\n"
        f"{fmt_kv('Дата', format_date(created_at))}",
        parse_mode=ParseMode.MARKDOWN,
    )


@router.message(F.text == "/support")
async def cmd_support(message: Message):
    maybe_cleanup()
    await maybe_send_reminders(message.bot)
    if is_rate_limited(message.from_user.id, "support", RATE_LIMIT_SECONDS):
        await message.answer("Подожди секунду и попробуй снова.")
        return
    open_ticket = get_open_ticket(message.from_user.id)
    if open_ticket:
        await send_ticket_view(message.bot, message.chat.id, open_ticket, for_admin=False)
        return
    approved_nicks = get_approved_nicks(message.from_user.id)
    saved = get_user_selected_nick(message.from_user.id)
    if saved and saved in approved_nicks:
        selected_nick = saved
    elif len(approved_nicks) == 1:
        selected_nick = approved_nicks[0]
        set_user_selected_nick(message.from_user.id, selected_nick)
    else:
        selected_nick = None

    if len(approved_nicks) > 1 and not selected_nick:
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

    SELECTED_NICK_BY_USER[message.from_user.id] = selected_nick or ""
    TICKET_DRAFT_BY_USER[message.from_user.id] = {"selected_nick": selected_nick or ""}

    # выбор темы тикета
    allowed_topics = TICKET_TOPICS if approved_nicks else [t for t in TICKET_TOPICS if t[0] == "bot"]
    if len(allowed_topics) == 1:
        TICKET_DRAFT_BY_USER[message.from_user.id]["topic"] = allowed_topics[0][0]
        PENDING_INPUT_MODE[message.from_user.id] = "ticket_subject"
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
            resize_keyboard=True,
        )
        await message.answer(
            f"{fmt_header('Техподдержка ' + BRAND)}\n"
            "Напиши тему обращения коротко одним сообщением.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard,
        )
        return

    topic_buttons = [
        [make_button(label, f"ticket_topic:{key}")]
        for key, label in allowed_topics
    ]
    await message.answer(
        "Выбери тему обращения:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=topic_buttons),
    )
    return


@router.message(F.text == "/feedback")
async def cmd_feedback(message: Message):
    maybe_cleanup()
    await maybe_send_reminders(message.bot)
    if is_rate_limited(message.from_user.id, "feedback", RATE_LIMIT_SECONDS):
        await message.answer("Подожди секунду и попробуй снова.")
        return
    approved_nicks = get_approved_nicks(message.from_user.id)
    saved = get_user_selected_nick(message.from_user.id)
    if saved and saved in approved_nicks:
        selected_nick = saved
    elif len(approved_nicks) == 1:
        selected_nick = approved_nicks[0]
        set_user_selected_nick(message.from_user.id, selected_nick)
    else:
        selected_nick = None

    if len(approved_nicks) > 1 and not selected_nick:
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
    SELECTED_NICK_BY_USER[message.from_user.id] = selected_nick or ""
    FEEDBACK_DRAFT_BY_USER[message.from_user.id] = {"selected_nick": selected_nick or ""}
    PENDING_INPUT_MODE[message.from_user.id] = "feedback_target"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [make_button("🌍 Отзыв о сервере", "feedback_target:server")],
            [make_button("🤖 Отзыв о боте", "feedback_target:bot")],
        ]
    )
    await message.answer(
        f"💬 *Отзывы {BRAND}*\n"
        f"{ACCENT_LINE}\n"
        "Выбери, о чём отзыв:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=keyboard,
    )


@router.message(F.text == "/tickets")
async def cmd_tickets(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    await send_ticket_list(bot, message.chat.id, 1, for_admin=True)


@router.message(F.text == "/ticket_search")
async def cmd_ticket_search(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    PENDING_INPUT_MODE[message.from_user.id] = "ticket_search"
    await message.answer("🔎 Введи поиск по тикетам (тема, @username или id):")


@router.message(F.text == "/my_tickets")
async def cmd_my_tickets(message: Message, bot: Bot):
    await send_ticket_list(bot, message.chat.id, 1, for_admin=False, user_id=message.from_user.id)


@router.message(F.text == "/export")
async def cmd_export(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_zip = f"export_{ts}.zip"
    apps_csv = f"applications_{ts}.csv"
    tickets_csv = f"tickets_{ts}.csv"
    accounts_csv = f"accounts_{ts}.csv"
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM applications")
        apps = cur.fetchall()
        app_cols = [d[0] for d in cur.description]
        cur.execute("SELECT * FROM support_sessions")
        tickets = cur.fetchall()
        ticket_cols = [d[0] for d in cur.description]
        cur.execute("SELECT * FROM user_accounts")
        accounts = cur.fetchall()
        acc_cols = [d[0] for d in cur.description]
    for name, cols, rows in [
        (apps_csv, app_cols, apps),
        (tickets_csv, ticket_cols, tickets),
        (accounts_csv, acc_cols, accounts),
    ]:
        with open(name, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in [apps_csv, tickets_csv, accounts_csv]:
            zf.write(name)
    await bot.send_document(message.chat.id, document=open(out_zip, "rb"))


@router.message(F.text == "/backup")
async def cmd_backup(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_zip = f"backup_{ts}.zip"
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        if os.path.exists(DB_PATH):
            zf.write(DB_PATH, arcname="bot.db")
        if os.path.exists(CONFIG_PATH):
            zf.write(CONFIG_PATH, arcname="config.json")
    await bot.send_document(message.chat.id, document=open(out_zip, "rb"))


@router.message(F.text == "/support_cancel")
async def cmd_support_cancel(message: Message):
    if message.from_user.id in PENDING_INPUT_MODE:
        mode = PENDING_INPUT_MODE.get(message.from_user.id)
        if mode in ("ticket_subject", "ticket_body"):
            PENDING_INPUT_MODE.pop(message.from_user.id, None)
            TICKET_DRAFT_BY_USER.pop(message.from_user.id, None)
            await message.answer("Обращение отменено.", reply_markup=build_main_menu(message.from_user.id))
            return
    await message.answer("Нет активного обращения.", reply_markup=build_main_menu(message.from_user.id))


@router.message(F.text == "/cancel")
async def cmd_cancel(message: Message):
    canceled = False
    state = load_form_state(message.from_user.id)
    if state:
        clear_form_state(message.from_user.id)
        canceled = True
    if message.from_user.id in PENDING_INPUT_MODE:
        mode = PENDING_INPUT_MODE.pop(message.from_user.id, None)
        if mode in ("ticket_subject", "ticket_body"):
            TICKET_DRAFT_BY_USER.pop(message.from_user.id, None)
        if mode in ("feedback_target", "feedback_rating", "feedback_text"):
            FEEDBACK_DRAFT_BY_USER.pop(message.from_user.id, None)
        canceled = True

    if canceled:
        await message.answer("🗑️ Отменено.", reply_markup=build_main_menu(message.from_user.id))
    else:
        await message.answer("Нет активного действия.", reply_markup=build_main_menu(message.from_user.id))


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
            f"{fmt_header('Заявка')}\n"
            f"{fmt_kv('ID', f'`#{app_id}`')}\n"
            f"{fmt_kv('Ник', f'`{nick}`')}\n"
            f"{fmt_kv('Как обращаться', q_name or '—')}\n"
            f"{fmt_kv('Возраст', q_age or '—')}\n"
            f"{fmt_kv('Моды/версии', q_mods or '—')}\n"
            f"{fmt_kv('Войс (слушать)', q_voice_listen or '—')}\n"
            f"{fmt_kv('Войс (говорить)', q_voice_speak or '—')}\n"
            f"{fmt_kv('Устройство', q_device or '—')}\n"
            f"{fmt_kv('Планы', q_plans or '—')}\n"
            f"{fmt_kv('Хост', q_host or '—')}\n"
            f"{fmt_kv('От', who)}"
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

    await process_decision(bot, int(parts[1]), "approve", message.chat.id, message.from_user.username)


@router.message(F.text == "/reject")
async def cmd_reject(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /reject <id>")
        return

    await process_decision(bot, int(parts[1]), "reject", message.chat.id, message.from_user.username)


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
        f"Архив: {s['archived']}\n"
        f"Средняя оценка решений: {s['avg_rating']:.2f}⭐",
    )


@router.message(F.text == "/analytics")
async def cmd_analytics(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    apps = get_counts_by_day("applications", 7)
    tickets = get_counts_by_day("support_sessions", 7)
    avg_decision = format_duration(get_avg_decision_time_seconds())
    avg_first_reply = format_duration(get_avg_first_admin_reply_seconds())
    pending_old = get_pending_older_than(PENDING_REMINDER_SECONDS)
    stale_tickets = get_stale_tickets(TICKET_REMINDER_SECONDS)
    admin_stats = get_admin_decision_stats(5)

    lines = [fmt_header("Аналитика")]
    lines.append(fmt_section("Скорость", "\n".join([
        f"Среднее решение заявки: {avg_decision}",
        f"Первый ответ в тикете: {avg_first_reply}",
    ])))
    lines.append(fmt_section("Нагрузка", "\n".join([
        f"Заявок без ответа > {PENDING_REMINDER_SECONDS // 3600}ч: {pending_old}",
        f"Тикетов без ответа > {TICKET_REMINDER_SECONDS // 3600}ч: {stale_tickets}",
    ])))
    lines.append(fmt_section("Заявки (7 дней)", "—" if not apps else "\n".join([f"{d}: {c}" for d, c in apps])))
    lines.append(fmt_section("Тикеты (7 дней)", "—" if not tickets else "\n".join([f"{d}: {c}" for d, c in tickets])))
    if admin_stats:
        admin_lines = []
        for admin_id, admin_username, cnt in admin_stats:
            tag = f"@{admin_username}" if admin_username else f"id {admin_id}"
            admin_lines.append(f"{tag}: {cnt}")
        lines.append(fmt_section("Админ‑активность (топ 5)", "\n".join(admin_lines)))
    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


@router.message(F.text == "/dashboard")
async def cmd_dashboard(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    await send_admin_dashboard(bot, message.chat.id)


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


@router.message(F.text == BTN_ACCOUNTS)
async def on_btn_accounts(message: Message):
    nicks = get_user_accounts(message.from_user.id)
    if not nicks:
        await message.answer(
            f"👥 *Аккаунты {BRAND}*\n"
            f"{ACCENT_LINE}\n"
            "У тебя пока нет одобренных ников.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    primary = get_user_selected_nick(message.from_user.id)
    if primary not in nicks:
        primary = nicks[0]
        set_user_selected_nick(message.from_user.id, primary)
    await message.answer(
        f"👥 *Аккаунты {BRAND}*\n"
        f"{ACCENT_LINE}\n"
        "Твои ники:\n" + "\n".join([f"• `{n}`" for n in nicks]) + f"\n\n⭐ Основной: `{primary}`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_accounts_panel(nicks, primary),
    )


@router.message(F.text == BTN_ADMIN)
async def on_btn_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    await message.answer("🛠️ *Админ панель*", parse_mode=ParseMode.MARKDOWN, reply_markup=build_admin_panel())


@router.message(F.text == "/admin")
async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    await message.answer("🛠️ *Админ панель*", parse_mode=ParseMode.MARKDOWN, reply_markup=build_admin_panel())


@router.message(F.text == "/add_admin")
async def cmd_add_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /add_admin <tg_id>")
        return
    new_id = int(parts[1])
    ADMIN_IDS.add(new_id)
    save_config_updates({"admin_ids": sorted(ADMIN_IDS)})
    await message.answer(f"Админ добавлен: {new_id}")


@router.message(F.text == "/remove_admin")
async def cmd_remove_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /remove_admin <tg_id>")
        return
    rm_id = int(parts[1])
    if rm_id == message.from_user.id:
        await message.answer("Нельзя удалить самого себя.")
        return
    ADMIN_IDS.discard(rm_id)
    save_config_updates({"admin_ids": sorted(ADMIN_IDS)})
    await message.answer(f"Админ удалён: {rm_id}")


@router.my_chat_member()
async def on_bot_added(event: ChatMemberUpdated):
    chat = event.chat
    if chat.type not in ("group", "supergroup"):
        return
    new_status = event.new_chat_member.status
    if new_status not in ("member", "administrator"):
        return
    GROUP_CHAT_IDS.add(chat.id)
    save_config_group_ids()


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


@router.message(F.text == BTN_CANCEL)
async def on_btn_cancel(message: Message):
    await cmd_cancel(message)


@router.message(F.text == BTN_SUPPORT)
async def on_btn_support(message: Message):
    await cmd_support(message)


@router.message(F.text == BTN_FEEDBACK)
async def on_btn_feedback(message: Message):
    await cmd_feedback(message)


@router.message(F.text == BTN_SUPPORT_DONE)
async def on_btn_support_done(message: Message, bot: Bot):
    await message.answer("Команда больше не используется. Открой тикет через /support.")


@router.message(F.text == BTN_SUPPORT_CANCEL)
async def on_btn_support_cancel(message: Message):
    await cmd_support_cancel(message)


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
        log_decision(app_id, user_id, message.from_user.username, "reject", reason)
        await message.answer(f"Заявка #{app_id} отклонена.")
        try:
            if reason:
                await bot.send_message(target_user_id, f"{fmt_header('Заявка отклонена')}\n{fmt_kv('ID', f'`#{app_id}`')}\n{fmt_kv('Причина', reason)}", parse_mode=ParseMode.MARKDOWN)
            else:
                await bot.send_message(target_user_id, f"{fmt_header('Заявка отклонена')}\n{fmt_kv('ID', f'`#{app_id}`')}", parse_mode=ParseMode.MARKDOWN)
            if not has_bot_rating(target_user_id, app_id):
                rate_kb = InlineKeyboardMarkup(
                    inline_keyboard=[[
                        make_button("⭐1", f"rate:{app_id}:1"),
                        make_button("⭐2", f"rate:{app_id}:2"),
                        make_button("⭐3", f"rate:{app_id}:3"),
                        make_button("⭐4", f"rate:{app_id}:4"),
                        make_button("⭐5", f"rate:{app_id}:5"),
                        make_button("⏭️ Пропустить", f"rate:{app_id}:skip"),
                    ]]
                )
                await bot.send_message(
                    target_user_id,
                    "Оцени работу бота (1–5):",
                    reply_markup=rate_kb,
                )
        except Exception:
            pass
        return

    if user_id in PENDING_INPUT_MODE:
        mode = PENDING_INPUT_MODE.pop(user_id)
        text = (message.text or "").strip()
        if mode == "ticket_subject":
            if not text:
                await message.answer(f"{fmt_header('Техподдержка')}\nТема не может быть пустой. Попробуй ещё раз.", parse_mode=ParseMode.MARKDOWN)
                return
            if contains_profanity(text):
                await message.answer(f"{fmt_header('Техподдержка')}\nПожалуйста, без оскорблений.", parse_mode=ParseMode.MARKDOWN)
                return
            draft = TICKET_DRAFT_BY_USER.get(user_id, {})
            draft["subject"] = text
            TICKET_DRAFT_BY_USER[user_id] = draft
            PENDING_INPUT_MODE[user_id] = "ticket_body"
            await message.answer("Опиши проблему подробнее одним сообщением.")
            return
        if mode == "ticket_body":
            if not text:
                await message.answer(f"{fmt_header('Техподдержка')}\nСообщение пустое. Попробуй ещё раз.", parse_mode=ParseMode.MARKDOWN)
                return
            if contains_profanity(text):
                await message.answer(f"{fmt_header('Техподдержка')}\nПожалуйста, без оскорблений.", parse_mode=ParseMode.MARKDOWN)
                return
            draft = TICKET_DRAFT_BY_USER.pop(user_id, {})
            subject = draft.get("subject") or "Без темы"
            topic = draft.get("topic") or "bot"
            selected_nick = draft.get("selected_nick") or SELECTED_NICK_BY_USER.get(user_id)
            ticket_id = create_ticket(user_id, message.from_user.username, selected_nick, topic, subject)
            add_ticket_message(ticket_id, "user_text", text, None, None)
            await message.answer(f"{fmt_header('Тикет создан')}\nID: `#{ticket_id}`", parse_mode=ParseMode.MARKDOWN, reply_markup=build_main_menu(message.from_user.id))
            await send_ticket_view(bot, message.chat.id, ticket_id, for_admin=False)
            admin_kb = InlineKeyboardMarkup(
                inline_keyboard=[[make_button("Открыть тикет", f"ticket_admin_view:{ticket_id}")]]
            )
            await notify_admins(bot, f"🆕 Новый тикет `#{ticket_id}` ({BRAND})", admin_kb)
            try:
                await bot.send_message(
                    message.chat.id,
                    f"{fmt_header('Подсказка')}\n"
                    "Если можешь — добавь:\n"
                    "• версию Minecraft\n"
                    "• когда началась проблема\n"
                    "• скрин/видео (если есть)",
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass
            return
        if mode.startswith("ticket_reply:"):
            try:
                ticket_id = int(mode.split(":", 1)[1])
            except Exception:
                await message.answer("Некорректный тикет.")
                return
            ticket = get_ticket(ticket_id)
            if not ticket or ticket[6] == "closed":
                await message.answer("Тикет закрыт.")
                return
            if not text:
                await message.answer("Сообщение пустое.")
                return
            if contains_profanity(text):
                await message.answer("Пожалуйста, без оскорблений.")
                return
            add_ticket_message(ticket_id, "admin_text", text, None, None)
            await message.answer(f"{fmt_header('Ответ отправлен')}\nID: `#{ticket_id}`", parse_mode=ParseMode.MARKDOWN)
            ticket = get_ticket(ticket_id)
            if ticket:
                _id, target_user_id, _username, _topic, _subject, _selected_nick, _status, _created, _updated = ticket
                try:
                    await bot.send_message(target_user_id, f"🛠️ Ответ по тикету #{ticket_id}:\n{text}")
                except Exception:
                    pass
            return
        if mode.startswith("ticket_add:"):
            try:
                ticket_id = int(mode.split(":", 1)[1])
            except Exception:
                await message.answer("Некорректный тикет.")
                return
            ticket = get_ticket(ticket_id)
            if not ticket or ticket[5] == "closed":
                await message.answer("Тикет закрыт.")
                return
            if not text:
                await message.answer("Сообщение пустое.")
                return
            if contains_profanity(text):
                await message.answer("Пожалуйста, без оскорблений.")
                return
            add_ticket_message(ticket_id, "user_text", text, None, None)
            await message.answer(f"{fmt_header('Сообщение добавлено')}\nID: `#{ticket_id}`", parse_mode=ParseMode.MARKDOWN)
            await notify_admins(bot, f"💬 Новое сообщение в тикете #{ticket_id}")
            return
        if mode == "account_add":
            nick = text
            if not MINECRAFT_NICK_RE.match(nick):
                await message.answer("Ник должен быть 3–16 символов: латиница, цифры, `_`.")
                return
            if nick.lower() in BAN_NICKS:
                await message.answer("Этот ник в бан-листе.")
                return
            if count_user_accounts(user_id) == 0:
                await message.answer("Сначала нужен хотя бы один одобренный ник.")
                return
            if count_user_accounts(user_id) >= 2:
                await message.answer("Максимум 2 аккаунта.")
                return
            if account_exists(nick):
                await message.answer("Этот ник уже используется.")
                return
            if not await health_check():
                await message.answer("Сервер недоступен. Попробуй позже.")
                return
            ok = await call_plugin("approve", nick)
            if not ok:
                await message.answer("Плагин недоступен. Попробуй позже.")
                return
            add_user_account(user_id, nick)
            log_account_action(user_id, nick, "add")
            await message.answer(f"{fmt_header('Ник добавлен')}\nНик: `{nick}`", parse_mode=ParseMode.MARKDOWN, reply_markup=build_main_menu(message.from_user.id))
            return
        if mode in ("feedback_target", "feedback_rating"):
            await message.answer("Пожалуйста, выбери вариант кнопкой.")
            return
        if mode == "feedback_text":
            if text == BTN_SKIP:
                text = ""
            if text and contains_profanity(text):
                await message.answer("Пожалуйста, без оскорблений.")
                return
            draft = FEEDBACK_DRAFT_BY_USER.pop(user_id, {})
            kind = "feedback"
            target = draft.get("target") or "server"
            rating = int(draft.get("rating") or 0) or None
            with db_connect() as conn:
                conn.execute(
                    "INSERT INTO feedback (user_id, username, kind, target, rating, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        user_id,
                        message.from_user.username,
                        kind,
                        target,
                        rating,
                        text,
                        int(time.time()),
                    ),
                )
            await message.answer("✅ Спасибо! Отзыв отправлен.")
            selected = SELECTED_NICK_BY_USER.get(user_id)
            approved = selected or get_latest_approved_nick(user_id) or get_first_approved_nick(user_id)
            target_label = "сервер" if target == "server" else "бот"
            stars = "⭐" * rating if rating else "—"
            comment_line = text if text else "—"
            user_tag = f"@{message.from_user.username}" if message.from_user.username else "без username"
            await notify_admins(
                bot,
                f"{fmt_header('Отзыв')}\n"
                f"{fmt_kv('От', f'{user_tag} (id {user_id})')}\n"
                f"{fmt_kv('Одобренный ник', approved or 'нет')}\n"
                f"{fmt_kv('Категория', target_label)}\n"
                f"{fmt_kv('Оценка', stars)}\n"
                f"{fmt_kv('Комментарий', comment_line)}",
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
            PENDING_INPUT_MODE[user_id] = f"ban_user_confirm:{text}"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [make_button("✅ Подтвердить", f"ban_user_confirm:{text}")],
                    [make_button("❌ Отмена", "ban_user_cancel")],
                ]
            )
            await message.answer(f"Подтвердить бан пользователя {text}?", reply_markup=kb)
            return
        if mode == "unban_user":
            if not text.isdigit():
                await message.answer("Нужен числовой Telegram ID.")
                return
            PENDING_INPUT_MODE[user_id] = f"unban_user_confirm:{text}"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [make_button("✅ Подтвердить", f"unban_user_confirm:{text}")],
                    [make_button("❌ Отмена", "unban_user_cancel")],
                ]
            )
            await message.answer(f"Подтвердить разбан пользователя {text}?", reply_markup=kb)
            return
        if mode == "ban_nick":
            if not text:
                await message.answer("Ник пустой.")
                return
            PENDING_INPUT_MODE[user_id] = f"ban_nick_confirm:{text}"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [make_button("✅ Подтвердить", f"ban_nick_confirm:{text}")],
                    [make_button("❌ Отмена", "ban_nick_cancel")],
                ]
            )
            await message.answer(f"Подтвердить бан ника {text}?", reply_markup=kb)
            return
        if mode == "unban_nick":
            if not text:
                await message.answer("Ник пустой.")
                return
            PENDING_INPUT_MODE[user_id] = f"unban_nick_confirm:{text}"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [make_button("✅ Подтвердить", f"unban_nick_confirm:{text}")],
                    [make_button("❌ Отмена", "unban_nick_cancel")],
                ]
            )
            await message.answer(f"Подтвердить разбан ника {text}?", reply_markup=kb)
            return
        if mode == "ticket_search":
            rows = search_tickets(text, 10)
            if not rows:
                await message.answer("Ничего не найдено.")
                return
            lines = ["🔎 Результаты поиска тикетов (до 10):"]
            for tid, uid, username, topic, subject, status, created_at, updated_at in rows:
                topic_label = next((label for key, label in TICKET_TOPICS if key == topic), topic or "—")
                who = f"@{username}" if username else f"id {uid}"
                lines.append(f"#{tid} — {topic_label} — {subject or '—'} — {who} — {status}")
            await message.answer("\n".join(lines))
            return
        if mode == "add_admin":
            if not text.isdigit():
                await message.answer("Нужен числовой Telegram ID.")
                return
            new_id = int(text)
            ADMIN_IDS.add(new_id)
            save_config_updates({"admin_ids": sorted(ADMIN_IDS)})
            await message.answer(f"Админ добавлен: {new_id}")
            return
        if mode == "remove_admin":
            if not text.isdigit():
                await message.answer("Нужен числовой Telegram ID.")
                return
            rm_id = int(text)
            if rm_id == message.from_user.id:
                await message.answer("Нельзя удалить самого себя.")
                return
            ADMIN_IDS.discard(rm_id)
            save_config_updates({"admin_ids": sorted(ADMIN_IDS)})
            await message.answer(f"Админ удалён: {rm_id}")
            return

    state = load_form_state(user_id)
    if not state:
        return

    step, _data, _editing_app_id = state
    key, _title, _hint = QUESTIONS[step]
    text = (message.text or "").strip()
    if contains_profanity(text):
        await message.answer("Пожалуйста, без оскорблений.")
        return

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
    user_id = message.from_user.id
    mode = PENDING_INPUT_MODE.get(user_id, "")
    if mode.startswith("ticket_reply:"):
        try:
            ticket_id = int(mode.split(":", 1)[1])
        except Exception:
            return
        ticket = get_ticket(ticket_id)
        if not ticket or ticket[6] == "closed":
            await message.answer("Тикет закрыт.")
            return
        kind_prefix = "admin_"
    elif mode.startswith("ticket_add:"):
        try:
            ticket_id = int(mode.split(":", 1)[1])
        except Exception:
            return
        ticket = get_ticket(ticket_id)
        if not ticket or ticket[6] == "closed":
            await message.answer("Тикет закрыт.")
            return
        kind_prefix = "user_"
    else:
        return
    if message.photo:
        file_id = message.photo[-1].file_id
        add_ticket_message(ticket_id, f"{kind_prefix}media", None, file_id, "photo")
    elif message.document:
        add_ticket_message(ticket_id, f"{kind_prefix}media", None, message.document.file_id, "document")
    elif message.video:
        add_ticket_message(ticket_id, f"{kind_prefix}media", None, message.video.file_id, "video")
    elif message.audio:
        add_ticket_message(ticket_id, f"{kind_prefix}media", None, message.audio.file_id, "audio")
    elif message.voice:
        add_ticket_message(ticket_id, f"{kind_prefix}media", None, message.voice.file_id, "voice")
    await message.answer("Файл добавлен.")
    if kind_prefix == "user_":
        await notify_admins(message.bot, f"📎 Файл в тикете #{ticket_id}")
    else:
        ticket = get_ticket(ticket_id)
        if ticket:
            _id, target_user_id, _username, _subject, _selected_nick, _status, _created, _updated = ticket
            try:
                await message.bot.send_message(target_user_id, f"📎 Админ прикрепил файл в тикете #{ticket_id}.")
            except Exception:
                pass


@router.callback_query(F.data)
async def on_callback(call: CallbackQuery, bot: Bot):
    data = call.data or ""
    user_id = call.from_user.id
    maybe_cleanup()
    await maybe_send_reminders(bot)

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
        if is_rate_limited(user_id, "pending_page", RATE_LIMIT_SECONDS):
            await call.answer("Подожди секунду.", show_alert=False)
            return
        try:
            page = int(data.split(":", 1)[1])
        except Exception:
            page = 1
        await cmd_pending(call.message, bot, page)
        await call.answer()
        return

    if data.startswith("ans:"):
        if is_rate_limited(user_id, "answer", RATE_LIMIT_SECONDS):
            await call.answer("Подожди секунду.", show_alert=False)
            return
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
        if is_rate_limited(user_id, "picknick", RATE_LIMIT_SECONDS):
            await call.answer("Подожди секунду.", show_alert=False)
            return
        parts = data.split(":", 2)
        if len(parts) != 3:
            await call.answer("Некорректные данные.")
            return
        mode = parts[1]
        nick = parts[2]
        saved_nick = get_user_selected_nick(user_id)
        if saved_nick and saved_nick != nick:
            await call.answer("Ник уже выбран.", show_alert=True)
            return
        SELECTED_NICK_BY_USER[user_id] = nick
        set_user_selected_nick(user_id, nick)
        if mode == "support":
            TICKET_DRAFT_BY_USER[call.from_user.id] = {"selected_nick": nick}
            PENDING_INPUT_MODE[call.from_user.id] = "ticket_subject"
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
                resize_keyboard=True,
            )
            await bot.send_message(
                call.message.chat.id,
                "🛟 *Техподдержка*\nНапиши тему обращения (кратко одним сообщением).",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard,
            )
        elif mode == "feedback":
            PENDING_INPUT_MODE[user_id] = "feedback"
            text = (
                "💬 *Отзыв*\n"
                "Напиши короткий отзыв: что понравилось, что улучшить."
            )
            await bot.send_message(call.message.chat.id, text, parse_mode=ParseMode.MARKDOWN)
        await call.answer()
        return

    if data.startswith("tickets_page:"):
        parts = data.split(":")
        if len(parts) != 3:
            await call.answer("Некорректные данные.")
            return
        role = parts[1]
        try:
            page = int(parts[2])
        except Exception:
            page = 1
        if role == "admin":
            if not is_admin(user_id):
                await call.answer("Нет прав.")
                return
            await send_ticket_list(bot, call.message.chat.id, page, for_admin=True)
        else:
            await send_ticket_list(bot, call.message.chat.id, page, for_admin=False, user_id=user_id)
        await call.answer()
        return

    if data.startswith("ticket_admin_view:"):
        if not is_admin(user_id):
            await call.answer("Нет прав.")
            return
        try:
            ticket_id = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Некорректный тикет.")
            return
        await send_ticket_view(bot, call.message.chat.id, ticket_id, for_admin=True)
        await call.answer()
        return

    if data.startswith("ticket_user_view:"):
        try:
            ticket_id = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Некорректный тикет.")
            return
        ticket = get_ticket(ticket_id)
        if not ticket or ticket[1] != user_id:
            await call.answer("Нет доступа.")
            return
        await send_ticket_view(bot, call.message.chat.id, ticket_id, for_admin=False)
        await call.answer()
        return

    if data.startswith("ticket_reply:"):
        if not is_admin(user_id):
            await call.answer("Нет прав.")
            return
        try:
            ticket_id = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Некорректный тикет.")
            return
        ticket = get_ticket(ticket_id)
        if not ticket or ticket[6] == "closed":
            await call.answer("Тикет закрыт.")
            return
        PENDING_INPUT_MODE[user_id] = f"ticket_reply:{ticket_id}"
        await bot.send_message(call.message.chat.id, f"Ответ для тикета #{ticket_id}:")
        await call.answer()
        return

    if data.startswith("ticket_topic:"):
        key = data.split(":", 1)[1]
        allowed = {k for k, _ in TICKET_TOPICS}
        if key not in allowed:
            await call.answer("Некорректная тема.")
            return
        draft = TICKET_DRAFT_BY_USER.get(user_id, {})
        draft["topic"] = key
        TICKET_DRAFT_BY_USER[user_id] = draft
        PENDING_INPUT_MODE[user_id] = "ticket_subject"
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
            resize_keyboard=True,
        )
        await bot.send_message(
            call.message.chat.id,
            "Напиши тему обращения (кратко одним сообщением).",
            reply_markup=keyboard,
        )
        await call.answer()
        return

    if data.startswith("feedback_target:"):
        target = data.split(":", 1)[1]
        if target not in ("server", "bot"):
            await call.answer("Некорректная категория.")
            return
        draft = FEEDBACK_DRAFT_BY_USER.get(user_id, {})
        draft["target"] = target
        FEEDBACK_DRAFT_BY_USER[user_id] = draft
        PENDING_INPUT_MODE[user_id] = "feedback_rating"
        stars = [
            [
                make_button("⭐ 1", "feedback_rating:1"),
                make_button("⭐ 2", "feedback_rating:2"),
                make_button("⭐ 3", "feedback_rating:3"),
                make_button("⭐ 4", "feedback_rating:4"),
                make_button("⭐ 5", "feedback_rating:5"),
            ]
        ]
        await bot.send_message(
            call.message.chat.id,
            f"Оцени от 1 до 5:\n{ACCENT_LINE}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=stars),
        )
        await call.answer()
        return

    if data.startswith("feedback_rating:"):
        try:
            rating = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Некорректная оценка.")
            return
        if rating < 1 or rating > 5:
            await call.answer("Некорректная оценка.")
            return
        draft = FEEDBACK_DRAFT_BY_USER.get(user_id, {})
        draft["rating"] = str(rating)
        FEEDBACK_DRAFT_BY_USER[user_id] = draft
        PENDING_INPUT_MODE[user_id] = "feedback_text"
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=BTN_SKIP), KeyboardButton(text=BTN_CANCEL)]],
            resize_keyboard=True,
        )
        await bot.send_message(
            call.message.chat.id,
            "Напиши комментарий к оценке (или пропусти):",
            reply_markup=keyboard,
        )
        await call.answer()
        return

    if data.startswith("ticket_add:"):
        try:
            ticket_id = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Некорректный тикет.")
            return
        ticket = get_ticket(ticket_id)
        if not ticket or ticket[1] != user_id:
            await call.answer("Нет доступа.")
            return
        if ticket[5] == "closed":
            await call.answer("Тикет закрыт.")
            return
        PENDING_INPUT_MODE[user_id] = f"ticket_add:{ticket_id}"
        await bot.send_message(call.message.chat.id, f"Добавь сообщение в тикет #{ticket_id}:")
        await call.answer()
        return

    if data.startswith("ticket_close:"):
        try:
            ticket_id = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Некорректный тикет.")
            return
        ticket = get_ticket(ticket_id)
        if not ticket:
            await call.answer("Тикет не найден.")
            return
        if not is_admin(user_id) and ticket[1] != user_id:
            await call.answer("Нет прав.")
            return
        set_ticket_status(ticket_id, "closed")
        await bot.send_message(call.message.chat.id, f"{fmt_header('Тикет закрыт')}\nID: `#{ticket_id}`", parse_mode=ParseMode.MARKDOWN)
        _id, owner_id, _username, _topic, _subject, _selected_nick, _status, _created, _updated = ticket
        if is_admin(user_id) and owner_id != user_id:
            try:
                await bot.send_message(owner_id, f"🔒 Ваш тикет #{ticket_id} закрыт администратором.")
            except Exception:
                pass
        if not is_admin(user_id):
            await notify_admins(bot, f"🔒 Пользователь закрыл тикет #{ticket_id}")
        await call.answer()
        return

    if data.startswith("ticket_reopen:"):
        try:
            ticket_id = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Некорректный тикет.")
            return
        ticket = get_ticket(ticket_id)
        if not ticket:
            await call.answer("Тикет не найден.")
            return
        if not is_admin(user_id) and ticket[1] != user_id:
            await call.answer("Нет прав.")
            return
        set_ticket_status(ticket_id, "open")
        await bot.send_message(call.message.chat.id, f"{fmt_header('Тикет открыт')}\nID: `#{ticket_id}`", parse_mode=ParseMode.MARKDOWN)
        _id, owner_id, _username, _topic, _subject, _selected_nick, _status, _created, _updated = ticket
        if is_admin(user_id) and owner_id != user_id:
            try:
                await bot.send_message(owner_id, f"🔓 Ваш тикет #{ticket_id} снова открыт администратором.")
            except Exception:
                pass
        if not is_admin(user_id):
            await notify_admins(bot, f"🔓 Пользователь открыл тикет #{ticket_id}")
        await call.answer()
        return

    if data.startswith("rate:"):
        parts = data.split(":")
        if len(parts) != 3:
            await call.answer("Некорректные данные.")
            return
        try:
            app_id = int(parts[1])
        except Exception:
            await call.answer("Некорректный ID.")
            return
        value = parts[2]
        if value == "skip":
            await call.message.edit_text("Ок, оценка пропущена.")
            await call.answer()
            return
        try:
            rating = int(value)
        except Exception:
            await call.answer("Некорректная оценка.")
            return
        if rating < 1 or rating > 5:
            await call.answer("Некорректная оценка.")
            return
        if has_bot_rating(user_id, app_id):
            await call.answer("Оценка уже сохранена.")
            return
        with db_connect() as conn:
            conn.execute(
                "INSERT INTO feedback (user_id, username, kind, target, rating, message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    user_id,
                    call.from_user.username,
                    "bot_rating",
                    "bot",
                    rating,
                    str(app_id),
                    int(time.time()),
                ),
            )
        await call.message.edit_text("Спасибо за оценку!")
        await call.answer()
        return

    if data == "acc_add":
        if count_user_accounts(user_id) == 0:
            await call.answer("Сначала нужно получить хотя бы один одобренный ник.")
            return
        if count_user_accounts(user_id) >= 2:
            await call.answer("Максимум 2 аккаунта.")
            return
        PENDING_INPUT_MODE[user_id] = "account_add"
        await bot.send_message(call.message.chat.id, "Введи ник для добавления (3–16, латиница/цифры/_):")
        await call.answer()
        return

    if data.startswith("acc_del:"):
        nick = data.split(":", 1)[1]
        if not nick:
            await call.answer("Некорректный ник.")
            return
        confirm_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [make_button("✅ Да, удалить", f"acc_del_confirm:{nick}")],
                [make_button("❌ Отмена", "acc_del_cancel")],
            ]
        )
        await bot.send_message(
            call.message.chat.id,
            f"Ты точно хочешь удалить ник `{nick}` из вайтлиста?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=confirm_kb,
        )
        await call.answer()
        return

    if data == "acc_del_cancel":
        await call.answer("Отменено.")
        return

    if data.startswith("acc_del_confirm:"):
        nick = data.split(":", 1)[1]
        if not nick:
            await call.answer("Некорректный ник.")
            return
        if not await health_check():
            await call.answer("Сервер недоступен.", show_alert=True)
            return
        ok = await call_plugin("remove", nick)
        if not ok:
            await call.answer("Плагин недоступен.", show_alert=True)
            return
        if remove_user_account(user_id, nick):
            log_account_action(user_id, nick, "remove")
            primary = get_user_selected_nick(user_id)
            if primary and primary.lower() == nick.lower():
                nicks = get_user_accounts(user_id)
                if nicks:
                    set_user_selected_nick(user_id, nicks[0])
                else:
                    set_user_selected_nick(user_id, None)
            await call.answer("Ник удалён.")
        else:
            await call.answer("Не найден.")
        nicks = get_user_accounts(user_id)
        primary = get_user_selected_nick(user_id)
        await bot.send_message(
            call.message.chat.id,
            f"{fmt_header('Аккаунты')}\n"
            "Твои ники:\n" + ("\n".join([f"• `{n}`" for n in nicks]) if nicks else "—") +
            (f"\n\n⭐ Основной: `{primary}`" if primary else ""),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=build_accounts_panel(nicks, primary) if nicks else None,
        )
        return

    if data.startswith("acc_primary:"):
        nick = data.split(":", 1)[1]
        if not nick:
            await call.answer("Некорректный ник.")
            return
        nicks = get_user_accounts(user_id)
        if nick not in nicks:
            await call.answer("Ник не найден.")
            return
        set_user_selected_nick(user_id, nick)
        await call.answer("Основной ник обновлён.")
        await bot.send_message(
            call.message.chat.id,
            f"👥 *Аккаунты {BRAND}*\n"
            f"{ACCENT_LINE}\n"
            "Твои ники:\n" + "\n".join([f"• `{n}`" for n in nicks]) + f"\n\n⭐ Основной: `{nick}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=build_accounts_panel(nicks, nick),
        )
        return

    if data == "ban_user_cancel":
        await call.answer("Отменено.")
        return
    if data == "unban_user_cancel":
        await call.answer("Отменено.")
        return
    if data == "ban_nick_cancel":
        await call.answer("Отменено.")
        return
    if data == "unban_nick_cancel":
        await call.answer("Отменено.")
        return

    if data.startswith("ban_user_confirm:"):
        try:
            target_id = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Некорректный ID.")
            return
        BAN_USER_IDS.add(target_id)
        await call.message.edit_text(f"Пользователь {target_id} добавлен в бан-лист.")
        await call.answer()
        return

    if data.startswith("unban_user_confirm:"):
        try:
            target_id = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Некорректный ID.")
            return
        BAN_USER_IDS.discard(target_id)
        await call.message.edit_text(f"Пользователь {target_id} удалён из бан-листа.")
        await call.answer()
        return

    if data.startswith("ban_nick_confirm:"):
        nick = data.split(":", 1)[1]
        if not nick:
            await call.answer("Некорректный ник.")
            return
        BAN_NICKS.add(nick.lower())
        await call.message.edit_text(f"Ник {nick} добавлен в бан-лист.")
        await call.answer()
        return

    if data.startswith("unban_nick_confirm:"):
        nick = data.split(":", 1)[1]
        if not nick:
            await call.answer("Некорректный ник.")
            return
        BAN_NICKS.discard(nick.lower())
        await call.message.edit_text(f"Ник {nick} удалён из бан-листа.")
        await call.answer()
        return

    if data.startswith("admin:"):
        if not is_admin(user_id):
            await call.answer("Нет прав.")
            return
        action = data.split(":", 1)[1]
        if action == "close":
            try:
                await call.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await call.answer("Закрыто.")
            return
        if action == "pending":
            await cmd_pending(call.message, bot, 1)
        elif action == "show":
            PENDING_INPUT_MODE[user_id] = "show"
            await bot.send_message(call.message.chat.id, "🔎 Введи ID заявки:")
        elif action == "search":
            PENDING_INPUT_MODE[user_id] = "search"
            await bot.send_message(call.message.chat.id, "Введите поиск (ник, @username или id):")
        elif action == "stats":
            await cmd_stats(call.message)
        elif action == "analytics":
            await cmd_analytics(call.message)
        elif action == "export":
            await cmd_export(call.message, bot)
        elif action == "backup":
            await cmd_backup(call.message, bot)
        elif action == "dashboard":
            await send_admin_dashboard(bot, call.message.chat.id)
        elif action == "tickets":
            await send_ticket_list(bot, call.message.chat.id, 1, for_admin=True)
        elif action == "ticket_search":
            PENDING_INPUT_MODE[user_id] = "ticket_search"
            await bot.send_message(call.message.chat.id, "🔎 Введи поиск по тикетам (тема, @username или id):")
        elif action == "health":
            await cmd_health(call.message)
        elif action == "add_admin":
            PENDING_INPUT_MODE[user_id] = "add_admin"
            await bot.send_message(call.message.chat.id, "Введи Telegram ID для добавления администратора:")
        elif action == "remove_admin":
            PENDING_INPUT_MODE[user_id] = "remove_admin"
            await bot.send_message(call.message.chat.id, "Введи Telegram ID для удаления администратора:")
        elif action == "ban_user":
            PENDING_INPUT_MODE[user_id] = "ban_user"
            await bot.send_message(call.message.chat.id, "Введите Telegram ID для бана:")
        elif action == "unban_user":
            PENDING_INPUT_MODE[user_id] = "unban_user"
            await bot.send_message(call.message.chat.id, "Введите Telegram ID для разбана:")
        elif action == "ban_nick":
            PENDING_INPUT_MODE[user_id] = "ban_nick"
            await bot.send_message(call.message.chat.id, "Введите ник для бана:")
        elif action == "unban_nick":
            PENDING_INPUT_MODE[user_id] = "unban_nick"
            await bot.send_message(call.message.chat.id, "Введите ник для разбана:")
        elif action == "archive":
            await cmd_archive(call.message)
        elif action == "help":
            await cmd_help(call.message)
        else:
            await call.answer("Неизвестное действие.")
            return
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
            f"{fmt_header('Заявка')}\n"
            f"{fmt_kv('ID', f'`#{app_id}`')}\n"
            f"{fmt_kv('Ник', f'`{nick}`')}\n"
            f"{fmt_kv('Как обращаться', q_name or '—')}\n"
            f"{fmt_kv('Возраст', q_age or '—')}\n"
            f"{fmt_kv('Моды/версии', q_mods or '—')}\n"
            f"{fmt_kv('Войс (слушать)', q_voice_listen or '—')}\n"
            f"{fmt_kv('Войс (говорить)', q_voice_speak or '—')}\n"
            f"{fmt_kv('Устройство', q_device or '—')}\n"
            f"{fmt_kv('Планы', q_plans or '—')}\n"
            f"{fmt_kv('Хост', q_host or '—')}\n"
            f"{fmt_kv('От', who)}\n"
            f"{fmt_kv('Статус', f'*{status}*')}\n"
            f"{fmt_kv('Дата', format_date(created_at))}"
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
        log_decision(app_id, user_id, call.from_user.username, "reject", None)
        await call.message.edit_text(f"{fmt_header('Заявка отклонена')}\nID: `#{app_id}`", parse_mode=ParseMode.MARKDOWN)
        try:
            await bot.send_message(target_user_id, f"Ваша заявка #{app_id} отклонена.")
            if not has_bot_rating(target_user_id, app_id):
                rate_kb = InlineKeyboardMarkup(
                    inline_keyboard=[[
                        make_button("⭐1", f"rate:{app_id}:1"),
                        make_button("⭐2", f"rate:{app_id}:2"),
                        make_button("⭐3", f"rate:{app_id}:3"),
                        make_button("⭐4", f"rate:{app_id}:4"),
                        make_button("⭐5", f"rate:{app_id}:5"),
                        make_button("⏭️ Пропустить", f"rate:{app_id}:skip"),
                    ]]
                )
                await bot.send_message(
                    target_user_id,
                    "Оцени работу бота (1–5):",
                    reply_markup=rate_kb,
                )
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
        log_decision(app_id, user_id, call.from_user.username, "reject", reason)
        await call.message.edit_text(f"{fmt_header('Заявка отклонена')}\nID: `#{app_id}`", parse_mode=ParseMode.MARKDOWN)
        try:
            await bot.send_message(target_user_id, f"Ваша заявка #{app_id} отклонена.\nПричина: {reason}")
            if not has_bot_rating(target_user_id, app_id):
                rate_kb = InlineKeyboardMarkup(
                    inline_keyboard=[[
                        make_button("⭐1", f"rate:{app_id}:1"),
                        make_button("⭐2", f"rate:{app_id}:2"),
                        make_button("⭐3", f"rate:{app_id}:3"),
                        make_button("⭐4", f"rate:{app_id}:4"),
                        make_button("⭐5", f"rate:{app_id}:5"),
                        make_button("⏭️ Пропустить", f"rate:{app_id}:skip"),
                    ]]
                )
                await bot.send_message(
                    target_user_id,
                    "Оцени работу бота (1–5):",
                    reply_markup=rate_kb,
                )
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
    await process_decision(bot, app_id, action, call.message.chat.id, call.from_user.username)


async def main():
    db_connect().close()
    maybe_cleanup()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
