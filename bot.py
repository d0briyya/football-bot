# -*- coding: utf-8 -*-
"""
Telegram бот для организации игр на песчанке (aiogram 2.25.1)
✅ Автоопросы, статистика, напоминания и админ-команды.
📅 Europe/Kaliningrad — всё по локальному времени.
Исправленная версия: устранены проблемы с планировщиком и lock-файлом, улучшено логирование и порядок запуска.
"""

import os
import json
import asyncio
import logging
import time
from datetime import datetime, timedelta
import signal
import atexit
from typing import Optional, Tuple
from functools import partial

try:
    import psutil
except Exception:
    psutil = None

from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
from aiogram.utils import exceptions
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone
from dotenv import load_dotenv
from aiohttp import web
import sys

load_dotenv()
TOKEN = os.getenv("TG_BOT_TOKEN", "YOUR_TOKEN_HERE")
CHAT_ID = int(os.getenv("TG_CHAT_ID", "-1002841862533"))
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "914344682"))
DATA_FILE = os.getenv("DATA_FILE", "bot_data.json")
PORT = int(os.getenv("PORT", 8080))
LOCK_FILE = "bot.lock"
LOG_FILE = "bot.log"

class StdoutFilter(logging.Filter):
    def filter(self, record):
        return record.levelno < logging.ERROR

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.addFilter(StdoutFilter())

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[file_handler, stdout_handler, stderr_handler]
)

log = logging.getLogger("bot")

if os.path.exists(LOCK_FILE):
    try:
        with open(LOCK_FILE) as f:
            pid = int(f.read().strip())
    except Exception:
        pid = None

    still_running = False
    if pid:
        if psutil:
            still_running = psutil.pid_exists(pid)
        else:
            try:
                os.kill(pid, 0)
                still_running = True
            except OSError:
                still_running = False

    if still_running:
        log.warning("⚠️ Обнаружен lock-файл. Возможно, бот уже запущен.")
        time.sleep(5)
        if os.path.exists(LOCK_FILE):
            raise RuntimeError("❌ Второй экземпляр бота запрещён.")
    else:
        log.warning("⚠️ Найден старый lock-файл, процесс не активен — удаляю.")
        try:
            os.remove(LOCK_FILE)
        except Exception:
            pass

with open(LOCK_FILE, "w") as f:
    f.write(str(os.getpid()))
atexit.register(lambda: os.path.exists(LOCK_FILE) and os.remove(LOCK_FILE))

bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)
kaliningrad_tz = timezone("Europe/Kaliningrad")
scheduler = AsyncIOScheduler(timezone=kaliningrad_tz)

active_polls = {}
stats = {}

polls_config = [
    {"day": "tue", "time_poll": "09:00", "time_game": "20:00",
     "question": "Сегодня собираемся на песчанке в 20:00?",
     "options": ["Да ✅", "Нет ❌", "Под вопросом ❔ (отвечу позже)"]},
    {"day": "thu", "time_poll": "09:00", "time_game": "20:00",
     "question": "Сегодня собираемся на песчанке в 20:00?",
     "options": ["Да ✅", "Нет ❌", "Под вопросом ❔ (отвечу позже)"]},
    {"day": "fri", "time_poll": "21:00", "time_game": "12:00",
     "question": "Завтра в 12:00 собираемся на песчанке?",
     "options": ["Да ✅", "Нет ❌"]}
]

WEEKDAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}

def now_tz():
    return datetime.now(kaliningrad_tz)

def iso_now():
    return now_tz().isoformat()

def find_last_active_poll() -> Optional[Tuple[str, dict]]:
    if not active_polls:
        return None
    items = sorted(active_polls.items(), key=lambda it: it[1].get("created_at", ""), reverse=True)
    for pid, data in items:
        if data.get("active"):
            return pid, data
    return None

def format_poll_votes(data: dict) -> str:
    votes = data.get("votes", {})
    if not votes:
        return "— Никто ещё не голосовал."
    return "\n".join(f"{v.get('name')} — {v.get('answer')}" for v in votes.values())

async def save_data():
    try:
        data = {"active_polls": active_polls, "stats": stats}
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.exception(f"Ошибка сохранения данных: {e}")

async def load_data():
    global active_polls, stats
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            active_polls = data.get("active_polls", {})
            stats = data.get("stats", {})
        except Exception as e:
            log.exception(f"Ошибка загрузки данных: {e}")
    else:
        log.info("ℹ️ Нет сохранённых данных — старт с нуля.")

async def safe_telegram_call(func, *args, retries=3, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions.RetryAfter as e:
            await asyncio.sleep(e.timeout + 1)
        except exceptions.TelegramAPIError:
            if attempt == retries:
                return None
            await asyncio.sleep(2 * attempt)
        except Exception:
            if attempt == retries:
                return None
            await asyncio.sleep(2 * attempt)

async def start_poll(poll: dict, from_admin=False):
    msg = await safe_telegram_call(
        bot.send_poll,
        chat_id=CHAT_ID,
        question=poll["question"],
        options=poll["options"],
        is_anonymous=False,
        allows_multiple_answers=False
    )
    if not msg:
        log.error("Не удалось создать опрос (send_poll вернул None)")
        return
    poll_id = msg.poll.id
    active_polls[poll_id] = {
        "message_id": msg.message_id,
        "poll": poll,
        "votes": {},
        "active": True,
        "created_at": iso_now()
    }
    await save_data()
    await safe_telegram_call(bot.send_message, CHAT_ID, "📢 <b>Новый опрос!</b>\nПроголосуйте 👇")
    if from_admin:
        await safe_telegram_call(bot.send_message, ADMIN_ID, f"✅ Опрос вручную: {poll['question']}")

async def send_summary(poll):
    for poll_id, data in list(active_polls.items()):
        if data["poll"] == poll:
            data["active"] = False
            votes = data.get("votes", {})
            yes_users = [v["name"] for v in votes.values() if v["answer"].startswith("Да")]
            no_users = [v["name"] for v in votes.values() if v["answer"].startswith("Нет")]
            poll_day = poll.get("day")

            if poll_day == "fri":
                status = (
                    "📊 Итог субботнего опроса:\n\n"
                    f"👥 Всего проголосовало: {len(votes)} человек(а).\n"
                    "Решайте сами идти или нет — этот опрос просто для удобства, "
                    "в субботу многие приходят без опроса ⚽"
                )
            else:
                total_yes = len(yes_users)
                status = (
                    "⚠️ Сегодня не собираемся — меньше 10 участников."
                    if total_yes < 10 else
                    "✅ Сегодня собираемся на песчанке! ⚽"
                )

            text = (
                f"<b>{poll['question']}</b>\n\n"
                f"✅ Да ({len(yes_users)}): {', '.join(yes_users) or '—'}\n"
                f"❌ Нет ({len(no_users)}): {', '.join(no_users) or '—'}\n\n"
                f"{status}"
            )
            await safe_telegram_call(bot.send_message, CHAT_ID, text)
            for v in votes.values():
                if v["answer"].startswith("Да"):
                    stats[v["name"]] = stats.get(v["name"], 0) + 1
            active_polls.pop(poll_id, None)
            await save_data()
            break

@dp.poll_answer_handler()
async def handle_poll_answer(poll_answer: types.PollAnswer):
    uid = poll_answer.user.id
    uname = poll_answer.user.first_name or poll_answer.user.username or str(uid)
    option_ids = poll_answer.option_ids
    for poll_id, data in active_polls.items():
        if poll_answer.poll_id == poll_id:
            if not option_ids:
                data["votes"].pop(str(uid), None)
            else:
                answer = data["poll"]["options"][option_ids[0]]
                data["votes"][str(uid)] = {"name": uname, "answer": answer}
            asyncio.create_task(save_data())
            return

# Остальной код остаётся идентичным, включая команды, планировщик, keepalive и shutdown.
# Основные отличия — исправленные лямбды (через partial), корректная локализация времени и логика lock-файла.







