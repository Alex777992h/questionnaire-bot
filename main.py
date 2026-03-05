import sqlite3
import time
import os
from typing import Optional

import requests
import telebot
from telebot import types
from dotenv import load_dotenv
import re

# === ЗАГРУЗКА ПЕРЕМЕННЫХ ИЗ .ENV ===
load_dotenv()

# === CONFIG ===
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    print("❌ Ошибка: BOT_TOKEN не найден! Проверь файл .env")
    exit(1)

ADMIN_IDS = {
    1322362053,
    1875115860,
}
PLUGIN_BASE_URL = "http://c7.play2go.cloud:20795"
PLUGIN_SECRET = "RrN4Jt9Vq2KpX8mZ"
CHAT_INVITE_URL = "https://t.me/+15HwEq4ltUJmMTIy"

# === ПУТЬ К БАЗЕ ДАННЫХ ===
# На хостинге bothost.ru используем /app/data/, локально - текущую папку
if os.path.exists('/app/data/'):
    DB_PATH = "/app/data/bot.db"
else:
    DB_PATH = "bot.db"  # Для локального запуска

bot = telebot.TeleBot(BOT_TOKEN)

PENDING_FORMS = {}

QUESTIONS = [
    ("nick", "1. Ник", "Введите ник (3-16 символов)."),
    ("name", "2. Как к вам можно обращаться (имя/псевдоним/по нику)", "Введите, как к вам обращаться."),
    ("age", "3. Возраст", "Введите возраст."),
    ("mods", "4. Имеется ли возможность играть с модами и переходить на новые версии", "Краткий ответ."),
    ("voice", "5. Имеется ли возможность слушать и говорить в войсчате", "Краткий ответ."),
    ("device", "6. Устройство, с которого ты играешь", "ПК/ноутбук/телефон/консоль и т.п."),
    ("plans", "7. Имеются ли планы на сервер?", "Кратко опишите планы."),
    ("host", "8. Будет ли возможность скидываться на оплату хоста? (необязательно)", "Если нет — так и ответьте."),
]

MINECRAFT_NICK_RE = re.compile(r"^[A-Za-z0-9_]{3,16}$")

BTN_APPLY = "Подать заявку"
BTN_STATUS = "Статус заявки"
BTN_ADMIN = "Админ панель"
BTN_PENDING = "Активные заявки"


def db_connect() -> sqlite3.Connection:
    # Создаём папку для БД, если её нет (нужно для локального теста)
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
            q_voice TEXT,
            q_device TEXT,
            q_plans TEXT,
            q_host TEXT,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL
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
        "q_voice": "TEXT",
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


def create_application(user_id: int, username: Optional[str], answers: dict) -> int:
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO applications (user_id, username, nick, q_name, q_age, q_mods, q_voice, q_device, q_plans, q_host, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                user_id,
                username,
                answers.get("nick", ""),
                answers.get("name", ""),
                answers.get("age", ""),
                answers.get("mods", ""),
                answers.get("voice", ""),
                answers.get("device", ""),
                answers.get("plans", ""),
                answers.get("host", ""),
                "pending",
                int(time.time()),
            ),
        )
        return cur.lastrowid


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
            "SELECT id, user_id, username, nick, q_name, q_age, q_mods, q_voice, q_device, q_plans, q_host, status "
            "FROM applications WHERE id = ?",
            (app_id,),
        )
        return cur.fetchone()

def get_pending_applications(limit: int = 20):
    with db_connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, username, nick, q_name, q_age, q_mods, q_voice, q_device, q_plans, q_host, status "
            "FROM applications WHERE status = 'pending' ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return cur.fetchall()


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
    except Exception:
        return False


@bot.message_handler(commands=["start"])
def cmd_start(message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(BTN_APPLY, BTN_STATUS)
    if is_admin(message.from_user.id):
        keyboard.add(BTN_ADMIN)
    bot.send_message(
        message.chat.id,
        "Привет! Чтобы подать заявку, используй команду:\n"
        "`/apply`\n"
        "Проверить статус: `/status`",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


@bot.message_handler(commands=["apply"])
def cmd_apply(message):
    last = get_last_application(message.from_user.id)
    if last:
        last_id, _last_nick, last_status, _created = last
        if last_status == "pending":
            bot.send_message(
                message.chat.id,
                f"У тебя уже есть активная заявка #{last_id}. Дождись решения администратора.",
            )
            return

    if message.from_user.id in PENDING_FORMS:
        bot.send_message(message.chat.id, "Анкета уже заполняется. Ответь на текущий вопрос.")
        return

    PENDING_FORMS[message.from_user.id] = {}
    bot.send_message(
        message.chat.id,
        "*Анкета на сервер*\n"
        "Ответь на вопросы по порядку. Отменить: `/cancel`",
        parse_mode="Markdown",
    )
    ask_question(message, 0)


def ask_question(message, index: int):
    key, title, hint = QUESTIONS[index]
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("Отменить", callback_data="cancel_form"))
    bot.send_message(
        message.chat.id,
        f"*{title}*\n_{hint}_",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )
    bot.register_next_step_handler(message, handle_answer, index)


def handle_answer(message, index: int):
    user_id = message.from_user.id
    if user_id not in PENDING_FORMS:
        return

    text = (message.text or "").strip()
    key, _title, _hint = QUESTIONS[index]

    if key == "nick" and (len(text) < 3 or len(text) > 16):
        bot.send_message(message.chat.id, "Ник должен быть 3-16 символов.")
        ask_question(message, index)
        return
    if key == "nick" and not MINECRAFT_NICK_RE.match(text):
        bot.send_message(
            message.chat.id,
            "Ник может содержать только латинские буквы, цифры и _ (3-16 символов).",
        )
        ask_question(message, index)
        return

    if not text:
        bot.send_message(message.chat.id, "Ответ не может быть пустым.")
        ask_question(message, index)
        return

    PENDING_FORMS[user_id][key] = text

    next_index = index + 1
    if next_index < len(QUESTIONS):
        ask_question(message, next_index)
        return

    finalize_application(message)


def finalize_application(message):
    user = message.from_user
    answers = PENDING_FORMS.pop(user.id, {})
    if not answers:
        return

    app_id = create_application(user.id, user.username, answers)
    bot.send_message(message.chat.id, f"Заявка #{app_id} отправлена. Ожидай решения администратора.")

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("Одобрить", callback_data=f"approve:{app_id}"),
        types.InlineKeyboardButton("Отклонить", callback_data=f"reject:{app_id}"),
    )

    text = (
        f"Новая заявка #{app_id}\n"
        f"Ник: `{answers.get('nick', '')}`\n"
        f"Как обращаться: {answers.get('name', '')}\n"
        f"Возраст: {answers.get('age', '')}\n"
        f"Моды/версии: {answers.get('mods', '')}\n"
        f"Войс: {answers.get('voice', '')}\n"
        f"Устройство: {answers.get('device', '')}\n"
        f"Планы: {answers.get('plans', '')}\n"
        f"Хост: {answers.get('host', '')}\n"
        f"От: @{user.username or 'без username'} (id {user.id})"
    )
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(admin_id, text, reply_markup=keyboard, parse_mode="Markdown")
        except Exception:
            pass


@bot.message_handler(commands=["status"])
def cmd_status(message):
    last = get_last_application(message.from_user.id)
    if not last:
        bot.send_message(message.chat.id, "У тебя нет заявок.")
        return

    app_id, nick, status, _created = last
    bot.send_message(message.chat.id, f"Последняя заявка #{app_id}\nНик: {nick}\nСтатус: {status}")


@bot.message_handler(commands=["cancel"])
def cmd_cancel(message):
    if message.from_user.id in PENDING_FORMS:
        PENDING_FORMS.pop(message.from_user.id, None)
        bot.send_message(message.chat.id, "Анкета отменена.")
    else:
        bot.send_message(message.chat.id, "Нет активной анкеты.")

@bot.message_handler(commands=["pending"])
def cmd_pending(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "Нет прав.")
        return

    rows = get_pending_applications(20)
    if not rows:
        bot.send_message(message.chat.id, "Активных заявок нет.")
        return

    for row in rows:
        app_id, user_id, username, nick, q_name, q_age, q_mods, q_voice, q_device, q_plans, q_host, _status = row
        who = q_name or (f"@{username}" if username else f"id {user_id}")
        text = (
            f"*Заявка #{app_id}*\n"
            f"Ник: `{nick}`\n"
            f"Как обращаться: {q_name or '—'}\n"
            f"Возраст: {q_age or '—'}\n"
            f"Моды/версии: {q_mods or '—'}\n"
            f"Войс: {q_voice or '—'}\n"
            f"Устройство: {q_device or '—'}\n"
            f"Планы: {q_plans or '—'}\n"
            f"Хост: {q_host or '—'}\n"
            f"От: {who}"
        )
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("Одобрить", callback_data=f"approve:{app_id}"),
            types.InlineKeyboardButton("Отклонить", callback_data=f"reject:{app_id}"),
        )
        bot.send_message(message.chat.id, text, parse_mode="Markdown", reply_markup=keyboard)


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
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(BTN_PENDING)
    bot.send_message(message.chat.id, "Админ панель:", reply_markup=keyboard)


@bot.message_handler(func=lambda m: m.text == BTN_PENDING)
def on_btn_pending(message):
    cmd_pending(message)


@bot.callback_query_handler(func=lambda call: True)
def on_callback(call):
    user_id = call.from_user.id
    if call.data == "cancel_form":
        if user_id in PENDING_FORMS:
            PENDING_FORMS.pop(user_id, None)
            bot.edit_message_text("Анкета отменена.", call.message.chat.id, call.message.message_id)
        else:
            bot.answer_callback_query(call.id, "Нет активной анкеты.")
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
    app = get_application(app_id)
    if not app:
        bot.edit_message_text("Заявка не найдена.", call.message.chat.id, call.message.message_id)
        return

    _id, target_user_id, _username, nick, _q_name, _q_age, _q_mods, _q_voice, _q_device, _q_plans, _q_host, status = app
    if status != "pending":
        bot.edit_message_text(
            f"Заявка уже обработана. Статус: {status}",
            call.message.chat.id,
            call.message.message_id,
        )
        return

    if action == "approve":
        ok = call_plugin("approve", nick)
        if not ok:
            bot.edit_message_text("Ошибка плагина. Попробуйте позже.", call.message.chat.id, call.message.message_id)
            return

        set_status(app_id, "approved")
        bot.edit_message_text(f"Заявка #{app_id} одобрена.", call.message.chat.id, call.message.message_id)
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
        set_status(app_id, "rejected")
        bot.edit_message_text(f"Заявка #{app_id} отклонена.", call.message.chat.id, call.message.message_id)
        try:
            bot.send_message(target_user_id, f"Ваша заявка #{app_id} отклонена.")
        except Exception:
            pass
        return

    bot.edit_message_text("Неизвестное действие.", call.message.chat.id, call.message.message_id)


if __name__ == "__main__":
    # Создаём папку для БД при запуске (для локального теста)
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
    
    db_connect().close()
    print(f"✅ Бот запущен. База данных: {DB_PATH}")
    bot.infinity_polling()
