import sqlite3
import time
import os
import json
import logging
import socket
import re
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any
from urllib.parse import urlparse

import requests
import telebot
from telebot import types
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

bot = telebot.TeleBot(BOT_TOKEN)

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

LAST_PLUGIN_ALERT_TS = 0
PENDING_REJECT_REASON: Dict[int, int] = {}


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
            (app_id, user_id, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, status, created_at) = row
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


def call_plugin(endpoint: str, nick: str) -> bool:
    url = f"{PLUGIN_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    try:
        resp = requests.post(
            url,
            data={"nick": nick, "secret": PLUGIN_SECRET},
            timeout=5,
        )
        return resp.status_code == 200 and resp.text.strip().lower() == "ok"
    except Exception as e:
        logging.error("Plugin call error: %s", e)
        return False

def maybe_alert_admins(text: str):
    global LAST_PLUGIN_ALERT_TS
    now = int(time.time())
    if now - LAST_PLUGIN_ALERT_TS >= ALERT_COOLDOWN_SECONDS:
        LAST_PLUGIN_ALERT_TS = now
        notify_admins(text)


def format_date(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def build_main_menu(user_id: int) -> types.ReplyKeyboardMarkup:
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(BTN_APPLY, BTN_STATUS)
    keyboard.add(BTN_HELP)
    if is_admin(user_id):
        keyboard.add(BTN_ADMIN)
    return keyboard


def build_admin_menu() -> types.ReplyKeyboardMarkup:
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(BTN_PENDING, BTN_SHOW)
    keyboard.add(BTN_SEARCH, BTN_STATS)
    keyboard.add(BTN_HEALTH)
    keyboard.add(BTN_BAN_USER, BTN_UNBAN_USER)
    keyboard.add(BTN_BAN_NICK, BTN_UNBAN_NICK)
    keyboard.add(BTN_ARCHIVE)
    keyboard.add(BTN_BACK)
    return keyboard


def health_check() -> bool:
    try:
        url = f"{PLUGIN_BASE_URL.rstrip('/')}/health"
        resp = requests.get(url, timeout=3)
        return resp.status_code == 200 and resp.text.strip().lower() == "ok"
    except Exception:
        return False


def notify_admins(text: str, keyboard: Optional[types.InlineKeyboardMarkup] = None):
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(admin_id, text, reply_markup=keyboard, parse_mode="Markdown")
        except Exception:
            pass


def send_application_to_admins(app_id: int, answers: dict, user):
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("✅ Одобрить", callback_data=f"approve:{app_id}"),
        types.InlineKeyboardButton("❌ Отклонить", callback_data=f"reject:{app_id}"),
        types.InlineKeyboardButton("⛔ Без причины", callback_data=f"reject_noreason:{app_id}"),
    )
    keyboard.add(types.InlineKeyboardButton("📄 Подробнее", callback_data=f"show:{app_id}"))
    username_part = f"@{user.username}" if user.username else "без username"
    text = (
        f"✨ *Новая заявка* `#{app_id}`\n"
        f"🎮 Ник: `{answers.get('nick', '')}`\n"
        f"👤 Как обращаться: {answers.get('name', '')}\n"
        f"🎂 Возраст: {answers.get('age', '')}\n"
        f"🎙️ Войс (слушать/говорить): {answers.get('voice_listen', '—')} / {answers.get('voice_speak', '—')}\n"
        f"📎 От: {username_part} (id {user.id})"
    )
    notify_admins(text, keyboard)


def process_decision(app_id: int, action: str, admin_chat_id: int):
    app = get_application(app_id)
    if not app:
        bot.send_message(admin_chat_id, "Заявка не найдена.")
        return

    _id, target_user_id, _username, nick, _q_name, _q_age, _q_mods, _q_voice_listen, _q_voice_speak, _q_device, _q_plans, _q_host, status, _created = app
    if status != "pending":
        bot.send_message(admin_chat_id, f"Заявка уже обработана. Статус: {status}")
        return

    if action == "approve":
        ok = call_plugin("approve", nick)
        if not ok:
            bot.send_message(admin_chat_id, "Ошибка плагина. Попробуйте позже.")
            maybe_alert_admins("⚠️ Плагин недоступен. Проверь порт/хост.")
            return

        set_status(app_id, "approved")
        bot.send_message(admin_chat_id, f"Заявка #{app_id} одобрена.")
        try:
            bot.send_message(
                target_user_id,
                f"Ваша заявка #{app_id} одобрена. Ник `{nick}` добавлен в вайтлист сервера.\n"
                f"Чат сервера: {CHAT_INVITE_URL}",
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        return

    if action == "reject":
        PENDING_REJECT_REASON[admin_chat_id] = app_id
        reason_kb = types.InlineKeyboardMarkup()
        for i, r in enumerate(REASON_TEMPLATES, start=1):
            reason_kb.add(types.InlineKeyboardButton(r, callback_data=f"reason:{app_id}:{i}"))
        reason_kb.add(types.InlineKeyboardButton("Отклонить без причины", callback_data=f"reject_noreason:{app_id}"))
        bot.send_message(
            admin_chat_id,
            f"Выберите шаблон причины или напишите свой текст для заявки #{app_id}:",
            reply_markup=reason_kb,
        )
        return


@bot.message_handler(commands=["start"])
def cmd_start(message):
    bot.send_message(
        message.chat.id,
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
        parse_mode="Markdown",
        reply_markup=build_main_menu(message.from_user.id),
    )


@bot.message_handler(commands=["help"])
def cmd_help(message):
    bot.send_message(
        message.chat.id,
        "ℹ️ *Справка*\n"
        "`/apply` — подать заявку\n"
        "`/status` — статус заявки\n"
        "`/cancel` — отменить анкету\n"
        "`/edit` — редактировать активную заявку\n"
        "`/ban_user <tg_id>` — бан по Telegram ID (админ)\n"
        "`/unban_user <tg_id>` — разбан по Telegram ID (админ)\n"
        "`/ban_nick <nick>` — бан по нику (админ)\n"
        "`/unban_nick <nick>` — разбан по нику (админ)\n"
        "`/archive` — архивировать старые заявки (админ)\n",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["apply"])
def cmd_apply(message):
    if message.from_user.id in BAN_USER_IDS:
        bot.send_message(message.chat.id, "Вы в бан-листе. Заявка недоступна.")
        return

    last = get_last_application(message.from_user.id)
    if last:
        last_id, _last_nick, last_status, last_created = last
        if last_status == "pending":
            bot.send_message(
                message.chat.id,
                f"У тебя уже есть активная заявка #{last_id}. Дождись решения администратора.",
            )
            return
        if not is_admin(message.from_user.id) and int(time.time()) - int(last_created) < COOLDOWN_SECONDS:
            wait_min = max(1, COOLDOWN_SECONDS // 60)
            bot.send_message(message.chat.id, f"Повторную заявку можно подать через {wait_min} мин.")
            return

    state = load_form_state(message.from_user.id)
    if state:
        bot.send_message(
            message.chat.id,
            "У тебя уже есть незавершенная анкета. Ответь на текущий вопрос.",
        )
        return

    save_form_state(message.from_user.id, 0, {}, None)
    bot.send_message(
        message.chat.id,
        "🔥 *Анкета на сервер*\n"
        "Ответь на вопросы по порядку.\n"
        "Отмена: `/cancel`",
        parse_mode="Markdown",
    )
    ask_question(message.chat.id, 0)


@bot.message_handler(commands=["edit"])
def cmd_edit(message):
    last = get_last_application(message.from_user.id)
    if not last:
        bot.send_message(message.chat.id, "Нет активной заявки для редактирования.")
        return
    last_id, _last_nick, last_status, _created = last
    if last_status != "pending":
        bot.send_message(message.chat.id, "Редактировать можно только активную заявку.")
        return

    save_form_state(message.from_user.id, 0, {}, last_id)
    bot.send_message(
        message.chat.id,
        "✏️ *Редактирование заявки*\n"
        "Ответь на вопросы по порядку.\n"
        "Отмена: `/cancel`",
        parse_mode="Markdown",
    )
    ask_question(message.chat.id, 0)


def ask_question(chat_id: int, index: int):
    if index >= len(QUESTIONS):
        return
        return

    key, title, hint = QUESTIONS[index]
    keyboard = types.InlineKeyboardMarkup()
    if key in YES_NO_KEYS:
        keyboard.add(
            types.InlineKeyboardButton("✅ Да", callback_data=f"ans:{key}:yes"),
            types.InlineKeyboardButton("❌ Нет", callback_data=f"ans:{key}:no"),
        )
    keyboard.add(types.InlineKeyboardButton("⛔ Отменить", callback_data="cancel_form"))
    bot.clear_step_handler_by_chat_id(chat_id)
    sent = bot.send_message(
        chat_id,
        f"🧩 *{title}*\n_{hint}_",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )
    bot.register_next_step_handler(sent, handle_answer, index)


def handle_answer(message, index: int):
    state = load_form_state(message.from_user.id)
    if not state:
        return

    step, data, editing_app_id = state
    if index != step:
        step = index

    text = (message.text or "").strip()
    key, _title, _hint = QUESTIONS[step]

    if key == "nick" and (len(text) < 3 or len(text) > 16):
        bot.send_message(message.chat.id, "Ник должен быть 3-16 символов.")
        ask_question(message.chat.id, step)
        return
    if key == "nick" and not MINECRAFT_NICK_RE.match(text):
        bot.send_message(
            message.chat.id,
            "Ник может содержать только латинские буквы, цифры и _ (3-16 символов).",
        )
        ask_question(message.chat.id, step)
        return
    if key == "nick" and text.lower() in BAN_NICKS:
        bot.send_message(message.chat.id, "Этот ник в бан-листе.")
        ask_question(message.chat.id, step)
        return
    if key == "age":
        if not text.isdigit() or not (7 <= int(text) <= 100):
            bot.send_message(message.chat.id, "Возраст должен быть числом (7-100).")
            ask_question(message.chat.id, step)
            return

    if key in YES_NO_KEYS:
        lower = text.lower()
        if lower in {"да", "yes", "y"}:
            save_and_advance(message.from_user, message.chat.id, key, "Да")
            return
        if lower in {"нет", "no", "n"}:
            save_and_advance(message.from_user, message.chat.id, key, "Нет")
            return
        bot.send_message(message.chat.id, "Выбери вариант кнопкой ✅ Да / ❌ Нет.")
        ask_question(message.chat.id, step)
        return

    if not text:
        bot.send_message(message.chat.id, "Ответ не может быть пустым.")
        ask_question(message.chat.id, step)
        return

    save_and_advance(message.from_user, message.chat.id, key, text)


def finalize_application_for_user(user, chat_id: int):
    state = load_form_state(user.id)
    if not state:
        return

    _step, answers, editing_app_id = state
    clear_form_state(user.id)

    if editing_app_id:
        update_application(editing_app_id, answers)
        bot.send_message(chat_id, f"✅ Заявка #{editing_app_id} обновлена.")
        notify_admins(f"✏️ Заявка #{editing_app_id} обновлена пользователем.")
        return

    app_id = create_application(user.id, user.username, answers)
    bot.send_message(chat_id, f"✅ Заявка #{app_id} отправлена. Ожидай решения администратора.")
    send_application_to_admins(app_id, answers, user)


def save_and_advance(user, chat_id: int, key: str, value: str):
    state = load_form_state(user.id)
    if not state:
        return
    step, data, editing_app_id = state
    expected_key = QUESTIONS[step][0]
    if key != expected_key:
        return
    data[key] = value
    next_step = step + 1
    save_form_state(user.id, next_step, data, editing_app_id)
    if next_step < len(QUESTIONS):
        bot.clear_step_handler_by_chat_id(chat_id)
        ask_question(chat_id, next_step)
    else:
        finalize_application_for_user(user, chat_id)


@bot.message_handler(commands=["status"])
def cmd_status(message):
    last = get_last_application(message.from_user.id)
    if not last:
        bot.send_message(message.chat.id, "У тебя нет заявок.")
        return

    app_id, nick, status, created_at = last
    bot.send_message(
        message.chat.id,
        f"📌 *Последняя заявка* `#{app_id}`\n"
        f"🎮 Ник: `{nick}`\n"
        f"📍 Статус: *{status}*\n"
        f"🕒 Дата: {format_date(created_at)}",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["cancel"])
def cmd_cancel(message):
    state = load_form_state(message.from_user.id)
    if state:
        clear_form_state(message.from_user.id)
        bot.send_message(message.chat.id, "🗑️ Анкета отменена.")
    else:
        bot.send_message(message.chat.id, "Нет активной анкеты.")


@bot.message_handler(commands=["pending"])
def cmd_pending(message, page: int = 1):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return

    total = get_pending_count()
    if total == 0:
        bot.send_message(message.chat.id, "🟢 Активных заявок нет.")
        return

    page = max(1, page)
    offset = (page - 1) * PENDING_PAGE_SIZE
    rows = get_pending_applications(PENDING_PAGE_SIZE, offset)

    for row in rows:
        app_id, user_id, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, _status, _created = row
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
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("✅ Одобрить", callback_data=f"approve:{app_id}"),
            types.InlineKeyboardButton("❌ Отклонить", callback_data=f"reject:{app_id}"),
            types.InlineKeyboardButton("⛔ Без причины", callback_data=f"reject_noreason:{app_id}"),
        )
        bot.send_message(message.chat.id, text, parse_mode="Markdown", reply_markup=keyboard)

    max_page = (total + PENDING_PAGE_SIZE - 1) // PENDING_PAGE_SIZE
    nav_keyboard = types.InlineKeyboardMarkup()
    if page > 1:
        nav_keyboard.add(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"pending_page:{page - 1}"))
    if page < max_page:
        nav_keyboard.add(types.InlineKeyboardButton("➡️ Вперёд", callback_data=f"pending_page:{page + 1}"))
    bot.send_message(message.chat.id, f"📄 Страница {page}/{max_page}", reply_markup=nav_keyboard)


@bot.message_handler(commands=["show"])
def cmd_show(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /show <id>")
        return

    app_id = int(parts[1])
    app = get_application(app_id)
    if not app:
        bot.send_message(message.chat.id, "Заявка не найдена.")
        return

    _id, user_id, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, status, created_at = app
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
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("✅ Одобрить", callback_data=f"approve:{app_id}"),
            types.InlineKeyboardButton("❌ Отклонить", callback_data=f"reject:{app_id}"),
            types.InlineKeyboardButton("⛔ Без причины", callback_data=f"reject_noreason:{app_id}"),
        )
    bot.send_message(message.chat.id, text, parse_mode="Markdown", reply_markup=keyboard)


@bot.message_handler(commands=["approve"])
def cmd_approve(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /approve <id>")
        return

    process_decision(int(parts[1]), "approve", message.chat.id)


@bot.message_handler(commands=["reject"])
def cmd_reject(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /reject <id>")
        return

    process_decision(int(parts[1]), "reject", message.chat.id)


@bot.message_handler(commands=["health"])
def cmd_health(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return

    ok = health_check()
    bot.send_message(message.chat.id, "✅ Плагин доступен." if ok else "❌ Плагин недоступен.")
    if not ok:
        maybe_alert_admins("⚠️ Плагин недоступен. Проверь порт/хост.")


@bot.message_handler(func=lambda m: m.text == BTN_APPLY)
def on_btn_apply(message):
    cmd_apply(message)


@bot.message_handler(func=lambda m: m.text == BTN_STATUS)
def on_btn_status(message):
    cmd_status(message)


@bot.message_handler(func=lambda m: m.text == BTN_ADMIN)
def on_btn_admin(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    bot.send_message(message.chat.id, "🛠️ *Админ панель*", parse_mode="Markdown", reply_markup=build_admin_menu())


@bot.message_handler(func=lambda m: m.text == BTN_PENDING)
def on_btn_pending(message):
    cmd_pending(message, 1)


@bot.message_handler(func=lambda m: m.text == BTN_SHOW)
def on_btn_show(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    bot.send_message(message.chat.id, "🔎 Введи ID заявки:")
    bot.register_next_step_handler(message, handle_show_id)


@bot.message_handler(func=lambda m: m.text == BTN_SEARCH)
def on_btn_search(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    bot.send_message(message.chat.id, "Введите поиск (ник, @username или id):")
    bot.register_next_step_handler(message, handle_search)


@bot.message_handler(func=lambda m: m.text == BTN_STATS)
def on_btn_stats(message):
    cmd_stats(message)


@bot.message_handler(func=lambda m: m.text == BTN_HEALTH)
def on_btn_health(message):
    cmd_health(message)


@bot.message_handler(func=lambda m: m.text == BTN_HELP)
def on_btn_help(message):
    cmd_help(message)


@bot.message_handler(func=lambda m: m.text == BTN_BACK)
def on_btn_back(message):
    bot.send_message(message.chat.id, "🏠 Главное меню:", reply_markup=build_main_menu(message.from_user.id))


@bot.message_handler(func=lambda m: m.text == BTN_BAN_USER)
def on_btn_ban_user(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    bot.send_message(message.chat.id, "Введите Telegram ID для бана:")
    bot.register_next_step_handler(message, handle_ban_user)


@bot.message_handler(func=lambda m: m.text == BTN_UNBAN_USER)
def on_btn_unban_user(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    bot.send_message(message.chat.id, "Введите Telegram ID для разбана:")
    bot.register_next_step_handler(message, handle_unban_user)


@bot.message_handler(func=lambda m: m.text == BTN_BAN_NICK)
def on_btn_ban_nick(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    bot.send_message(message.chat.id, "Введите ник для бана:")
    bot.register_next_step_handler(message, handle_ban_nick)


@bot.message_handler(func=lambda m: m.text == BTN_UNBAN_NICK)
def on_btn_unban_nick(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    bot.send_message(message.chat.id, "Введите ник для разбана:")
    bot.register_next_step_handler(message, handle_unban_nick)


@bot.message_handler(func=lambda m: m.text == BTN_ARCHIVE)
def on_btn_archive(message):
    cmd_archive(message)


def handle_show_id(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    text = (message.text or "").strip()
    if not text.isdigit():
        bot.send_message(message.chat.id, "Нужен числовой ID заявки.")
        return
    app_id = int(text)
    app = get_application(app_id)
    if not app:
        bot.send_message(message.chat.id, "Заявка не найдена.")
        return

    _id, user_id, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, status, created_at = app
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
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("✅ Одобрить", callback_data=f"approve:{app_id}"),
            types.InlineKeyboardButton("❌ Отклонить", callback_data=f"reject:{app_id}"),
            types.InlineKeyboardButton("⛔ Без причины", callback_data=f"reject_noreason:{app_id}"),
        )
    bot.send_message(message.chat.id, text, parse_mode="Markdown", reply_markup=keyboard)


@bot.message_handler(func=lambda m: m.from_user.id in PENDING_REJECT_REASON)
def handle_reject_reason(message):
    admin_id = message.from_user.id
    app_id = PENDING_REJECT_REASON.pop(admin_id, None)
    if not app_id:
        return
    reason = (message.text or "").strip()
    if reason == "-" or not reason:
        reason = None

    app = get_application(app_id)
    if not app:
        bot.send_message(admin_id, "Заявка не найдена.")
        return

    _id, target_user_id, _username, _nick, _q_name, _q_age, _q_mods, _q_voice_listen, _q_voice_speak, _q_device, _q_plans, _q_host, status, _created = app
    if status != "pending":
        bot.send_message(admin_id, f"Заявка уже обработана. Статус: {status}")
        return

    set_status(app_id, "rejected")
    bot.send_message(admin_id, f"Заявка #{app_id} отклонена.")
    try:
        if reason:
            bot.send_message(target_user_id, f"Ваша заявка #{app_id} отклонена.\nПричина: {reason}")
        else:
            bot.send_message(target_user_id, f"Ваша заявка #{app_id} отклонена.")
    except Exception:
        pass


def handle_search(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    q = (message.text or "").strip()
    if not q:
        bot.send_message(message.chat.id, "Пустой запрос.")
        return
    rows = search_applications(q, 10)
    if not rows:
        bot.send_message(message.chat.id, "Ничего не найдено.")
        return
    lines = ["🔎 Результаты поиска (до 10):"]
    for row in rows:
        app_id, user_id, username, nick, q_name, q_age, _q_mods, _q_voice_listen, _q_voice_speak, _q_device, _q_plans, _q_host, status, _created = row
        who = q_name or (f"@{username}" if username else f"id {user_id}")
        lines.append(f"#{app_id} — {nick} — {who} — {status} — возраст: {q_age or '—'}")
    bot.send_message(message.chat.id, "\n".join(lines))


def handle_ban_user(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    text = (message.text or "").strip()
    if not text.isdigit():
        bot.send_message(message.chat.id, "Нужен числовой Telegram ID.")
        return
    user_id = int(text)
    BAN_USER_IDS.add(user_id)
    bot.send_message(message.chat.id, f"Пользователь {user_id} добавлен в бан-лист.")


def handle_unban_user(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    text = (message.text or "").strip()
    if not text.isdigit():
        bot.send_message(message.chat.id, "Нужен числовой Telegram ID.")
        return
    user_id = int(text)
    BAN_USER_IDS.discard(user_id)
    bot.send_message(message.chat.id, f"Пользователь {user_id} удалён из бан-листа.")


def handle_ban_nick(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    nick = (message.text or "").strip()
    if not nick:
        bot.send_message(message.chat.id, "Ник пустой.")
        return
    BAN_NICKS.add(nick.lower())
    bot.send_message(message.chat.id, f"Ник {nick} добавлен в бан-лист.")


def handle_unban_nick(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    nick = (message.text or "").strip()
    if not nick:
        bot.send_message(message.chat.id, "Ник пустой.")
        return
    BAN_NICKS.discard(nick.lower())
    bot.send_message(message.chat.id, f"Ник {nick} удалён из бан-листа.")


@bot.callback_query_handler(func=lambda call: True)
def on_callback(call):
    user_id = call.from_user.id

    if call.data == "cancel_form":
        state = load_form_state(user_id)
        if state:
            clear_form_state(user_id)
            bot.edit_message_text("🗑️ Анкета отменена.", call.message.chat.id, call.message.message_id)
        else:
            bot.answer_callback_query(call.id, "Нет активной анкеты.")
        return

    if call.data.startswith("pending_page:"):
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "Нет прав.")
            return
        try:
            page = int(call.data.split(":", 1)[1])
        except Exception:
            page = 1
        cmd_pending(call.message, page)
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("ans:"):
        parts = call.data.split(":")
        if len(parts) != 3:
            bot.answer_callback_query(call.id, "Некорректные данные.")
            return
        key = parts[1]
        val = parts[2]
        if key not in YES_NO_KEYS:
            bot.answer_callback_query(call.id, "Некорректный вопрос.")
            return
        value = "Да" if val == "yes" else "Нет"
        save_and_advance(call.from_user, call.message.chat.id, key, value)
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("show:"):
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "Нет прав.")
            return
        try:
            app_id = int(call.data.split(":", 1)[1])
        except Exception:
            bot.answer_callback_query(call.id, "Некорректный ID.")
            return
        app = get_application(app_id)
        if not app:
            bot.answer_callback_query(call.id, "Заявка не найдена.")
            return
        _id, user_id2, username, nick, q_name, q_age, q_mods, q_voice_listen, q_voice_speak, q_device, q_plans, q_host, status, created_at = app
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
            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(
                types.InlineKeyboardButton("✅ Одобрить", callback_data=f"approve:{app_id}"),
                types.InlineKeyboardButton("❌ Отклонить", callback_data=f"reject:{app_id}"),
                types.InlineKeyboardButton("⛔ Без причины", callback_data=f"reject_noreason:{app_id}"),
            )
        bot.send_message(call.message.chat.id, text, parse_mode="Markdown", reply_markup=keyboard)
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("reject_noreason:"):
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "Нет прав.")
            return
        try:
            app_id = int(call.data.split(":", 1)[1])
        except Exception:
            bot.answer_callback_query(call.id, "Некорректный ID.")
            return
        app = get_application(app_id)
        if not app:
            bot.answer_callback_query(call.id, "Заявка не найдена.")
            return
        _id, target_user_id, _username, _nick, _q_name, _q_age, _q_mods, _q_voice_listen, _q_voice_speak, _q_device, _q_plans, _q_host, status, _created = app
        if status != "pending":
            bot.answer_callback_query(call.id, f"Уже обработана: {status}")
            return
        set_status(app_id, "rejected")
        bot.edit_message_text(f"Заявка #{app_id} отклонена.", call.message.chat.id, call.message.message_id)
        try:
            bot.send_message(target_user_id, f"Ваша заявка #{app_id} отклонена.")
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("reason:"):
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "Нет прав.")
            return
        parts = call.data.split(":")
        if len(parts) != 3:
            bot.answer_callback_query(call.id, "Некорректные данные.")
            return
        try:
            app_id = int(parts[1])
            idx = int(parts[2]) - 1
        except Exception:
            bot.answer_callback_query(call.id, "Некорректные данные.")
            return
        if idx < 0 or idx >= len(REASON_TEMPLATES):
            bot.answer_callback_query(call.id, "Некорректный шаблон.")
            return
        reason = REASON_TEMPLATES[idx]
        app = get_application(app_id)
        if not app:
            bot.answer_callback_query(call.id, "Заявка не найдена.")
            return
        _id, target_user_id, _username, _nick, _q_name, _q_age, _q_mods, _q_voice_listen, _q_voice_speak, _q_device, _q_plans, _q_host, status, _created = app
        if status != "pending":
            bot.answer_callback_query(call.id, f"Уже обработана: {status}")
            return
        set_status(app_id, "rejected")
        bot.edit_message_text(f"Заявка #{app_id} отклонена.", call.message.chat.id, call.message.message_id)
        try:
            bot.send_message(target_user_id, f"Ваша заявка #{app_id} отклонена.\nПричина: {reason}")
        except Exception:
            pass
        bot.answer_callback_query(call.id)
        return

    if not is_admin(user_id):
        bot.answer_callback_query(call.id, "Нет прав.")
        return

    data = call.data or ""
    if ":" not in data:
        bot.edit_message_text("Некорректные данные.", call.message.chat.id, call.message.message_id)
        return

    action, app_id_str = data.split(":", 1)
    if not app_id_str.isdigit():
        bot.edit_message_text("Некорректный ID заявки.", call.message.chat.id, call.message.message_id)
        return

    app_id = int(app_id_str)
    process_decision(app_id, action, call.message.chat.id)


@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    s = get_stats()
    bot.send_message(
        message.chat.id,
        "📊 Статистика:\n"
        f"Всего: {s['total']}\n"
        f"Ожидают: {s['pending']}\n"
        f"Одобрено: {s['approved']}\n"
        f"Отклонено: {s['rejected']}\n"
        f"Архив: {s['archived']}",
    )


@bot.message_handler(commands=["ban_user"])
def cmd_ban_user(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /ban_user <tg_id>")
        return
    user_id = int(parts[1])
    BAN_USER_IDS.add(user_id)
    bot.send_message(message.chat.id, f"Пользователь {user_id} добавлен в бан-лист.")


@bot.message_handler(commands=["unban_user"])
def cmd_unban_user(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /unban_user <tg_id>")
        return
    user_id = int(parts[1])
    BAN_USER_IDS.discard(user_id)
    bot.send_message(message.chat.id, f"Пользователь {user_id} удалён из бан-листа.")


@bot.message_handler(commands=["ban_nick"])
def cmd_ban_nick(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Использование: /ban_nick <nick>")
        return
    nick = parts[1].strip()
    if not nick:
        bot.send_message(message.chat.id, "Ник пустой.")
        return
    BAN_NICKS.add(nick.lower())
    bot.send_message(message.chat.id, f"Ник {nick} добавлен в бан-лист.")


@bot.message_handler(commands=["unban_nick"])
def cmd_unban_nick(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Использование: /unban_nick <nick>")
        return
    nick = parts[1].strip()
    BAN_NICKS.discard(nick.lower())
    bot.send_message(message.chat.id, f"Ник {nick} удалён из бан-листа.")


@bot.message_handler(commands=["archive"])
def cmd_archive(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return
    count = archive_old_applications(ARCHIVE_DAYS)
    bot.send_message(message.chat.id, f"Архивировано заявок: {count}")


if __name__ == "__main__":
    db_connect().close()
    logging.info("Bot started. DB: %s", DB_PATH)
    bot.infinity_polling()
