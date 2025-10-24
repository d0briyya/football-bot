# -*- coding: utf-8 -*-
""" Refactored and hardened Telegram bot (aiogram 2.x) — improved by ChatGPT (senior-style)
Key improvements made: ...
Note: keep environment variables: TG_BOT_TOKEN, TG_CHAT_ID, TG_ADMIN_ID, PORT
"""
from __future__ import annotations

import os
import sys
import json
import time
import shutil
import asyncio
import logging
import signal
import atexit
from datetime import datetime, timedelta
from functools import partial
from typing import Optional, Tuple, Dict, Any

try:
    import psutil
except Exception:
    psutil = None

from logging.handlers import RotatingFileHandler
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
from aiogram.utils import exceptions
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone
from dotenv import load_dotenv
from aiohttp import web

# -------------------- Configuration --------------------
load_dotenv()

TOKEN = os.getenv("TG_BOT_TOKEN")
if not TOKEN:
    print("ERROR: TG_BOT_TOKEN is not set. Please export it and restart.")
    sys.exit(1)

try:
    CHAT_ID = int(os.getenv("TG_CHAT_ID", "-1002841862533"))
except Exception:
    print("ERROR: TG_CHAT_ID must be an integer (chat id).")
    sys.exit(1)

try:
    ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "914344682"))
except Exception:
    print("ERROR: TG_ADMIN_ID must be an integer (admin id).")
    sys.exit(1)

DATA_FILE = os.getenv("DATA_FILE", "bot_data.json")
BACKUP_FILE = os.getenv("BACKUP_FILE", "bot_data_backup.json")
PORT = int(os.getenv("PORT", 8080))
LOCK_FILE = os.getenv("LOCK_FILE", "bot.lock")
LOG_FILE = os.getenv("LOG_FILE", "bot.log")

# -------------------- Logging --------------------
class StdoutFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < logging.ERROR

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.addFilter(StdoutFilter())

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=5, encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[file_handler, stdout_handler, stderr_handler],
)

log = logging.getLogger("bot")

# -------------------- Single instance lock --------------------

def _read_pid_from_lock(path: str) -> Optional[int]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return None

def ensure_single_instance(lock_path: str = LOCK_FILE) -> None:
    """Ensure only one instance runs. If stale lock exists, remove it.
    On failure, raise RuntimeError to prevent double startup.
    """
    if os.path.exists(lock_path):
        pid = _read_pid_from_lock(lock_path)
        still_running = False
        if pid:
            if psutil:
                still_running = psutil.pid_exists(pid)
            else:
                try:
                    os.kill(pid, 0)
                    still_running = True
                except Exception:
                    still_running = False

        if still_running:
            log.error("Lock file exists and process %s is running. Refusing to start.", pid)
            raise RuntimeError("Another instance is already running")
        else:
            log.warning("Stale lock file found (pid=%s). Removing." , pid)
            try:
                os.remove(lock_path)
            except Exception as e:
                log.exception("Failed to remove stale lock: %s", e)

    with open(lock_path, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))

    def _cleanup():
        try:
            if os.path.exists(lock_path):
                os.remove(lock_path)
                log.info("Lock file removed on exit.")
        except Exception:
            log.exception("Failed to remove lock file at exit.")

    atexit.register(_cleanup)

ensure_single_instance()

# -------------------- Bot, scheduler, timezone --------------------
bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

KALININGRAD_TZ = timezone("Europe/Kaliningrad")
scheduler = AsyncIOScheduler(timezone=KALININGRAD_TZ)

START_TIME = datetime.now()

# runtime state
active_polls: Dict[str, Dict[str, Any]] = {}
stats: Dict[str, int] = {}

# polls config (modifiable)
polls_config = [
    {"day": "tue", "time_poll": "09:00", "time_game": "20:00",
     "question": "Сегодня собираемся на песчанке в 20:00?",
     "options": ["Да ✅", "Нет ❌", "Под вопросом ❔ (отвечу позже)"]},
    {"day": "thu", "time_poll": "09:00", "time_game": "20:00",
     "question": "Сегодня собираемся на песчанке в 20:00?",
     "options": ["Да ✅", "Нет ❌", "Под вопросом ❔ (отвечу позже)"]},
    {"day": "fri", "time_poll": "21:00", "time_game": "12:00",
     "question": "Завтра в 12:00 собираемся на песчанке?",
     "options": ["Да ✅", "Нет ❌"]},
    
]

WEEKDAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
TELEGRAM_MESSAGE_LIMIT = 4096

# -------------------- Helpers --------------------
def now_tz() -> datetime:
    return datetime.now(KALININGRAD_TZ)

def iso_now() -> str:
    return now_tz().isoformat()

def is_admin(user_id: int) -> bool:
    try:
        return int(user_id) == int(ADMIN_ID)
    except Exception:
        return False

def find_last_active_poll() -> Optional[Tuple[str, Dict[str, Any]]]:
    if not active_polls:
        return None
    items = sorted(active_polls.items(), key=lambda it: it[1].get("created_at", ""), reverse=True)
    for pid, data in items:
        if data.get("active"):
            return pid, data
    return None

def format_poll_votes(data: Dict[str, Any]) -> str:
    votes = data.get("votes", {})
    if not votes:
        return "— Никто ещё не голосовал."
    return "\n".join(f"{v.get('name')} — {v.get('answer')}" for v in votes.values())

# -------------------- Persistence --------------------
async def save_data() -> None:
    try:
        payload = {"active_polls": active_polls, "stats": stats}
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log.debug("Data saved to %s", DATA_FILE)
    except Exception:
        log.exception("Failed to save data")

async def load_data() -> None:
    global active_polls, stats
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            active_polls = data.get("active_polls", {})
            stats = data.get("stats", {})
            log.info("Loaded data: active_polls=%s, stats=%s", len(active_polls), len(stats))
        except Exception:
            log.exception("Failed to load data — starting with empty state")
    else:
        log.info("No data file found — starting fresh")

def make_backup() -> None:
    try:
        if os.path.exists(DATA_FILE):
            shutil.copyfile(DATA_FILE, BACKUP_FILE)
            log.info("Backup created: %s", BACKUP_FILE)
    except Exception:
        log.exception("Failed to create backup")

# -------------------- Telegram wrapper --------------------
async def safe_telegram_call(func, *args, retries: int = 3, **kwargs):
    """A resilient wrapper for Telegram API calls.
    Handles FloodWait/RetryAfter specially and retries on transient errors.
    Returns the call result or None on failure.
    """
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions.RetryAfter as e:
            wait = getattr(e, 'timeout', None) or getattr(e, 'retry_after', None) or 1
            log.warning("RetryAfter (flood) — sleeping %s seconds", wait)
            await asyncio.sleep(wait + 1)
        except exceptions.TelegramAPIError as e:
            log.warning("TelegramAPIError (attempt %s): %s", attempt, e)
            if attempt == retries:
                return None
            await asyncio.sleep(1 + attempt)
        except Exception:
            log.exception("Unexpected exception during Telegram call (attempt %s)", attempt)
            if attempt == retries:
                return None
            await asyncio.sleep(1 + attempt)

# -------------------- New helpers: compute poll close datetime & scheduling reminders --------------------
def compute_poll_close_dt(poll: Dict[str, Any], start_dt: datetime) -> datetime:
    """
    Compute poll closing datetime using poll['day'] and poll['time_game'].
    If poll['day']=='manual' or computation fails, fallback to start_dt + 24h.
    """
    try:
        day = poll.get("day")
        tg_hour, tg_minute = map(int, poll.get("time_game", "23:59").split(":"))
        if day not in WEEKDAY_MAP:
            # manual poll or unknown: default 24h lifetime
            return start_dt + timedelta(hours=24)

        target = WEEKDAY_MAP[day]
        days_ahead = (target - start_dt.weekday()) % 7
        base_date = start_dt.date() + timedelta(days=days_ahead)
        base = datetime(base_date.year, base_date.month, base_date.day, tg_hour, tg_minute)
        base_local = KALININGRAD_TZ.localize(base) if base.tzinfo is None else base.astimezone(KALININGRAD_TZ)
        # Ensure close is after start; if not, assume next week
        if base_local <= start_dt:
            base_local = base_local + timedelta(days=7)
        return base_local
    except Exception:
        log.exception("Failed to compute poll close dt for poll: %s", poll)
        return start_dt + timedelta(hours=24)

async def send_reminder_if_needed(poll_id: str) -> None:
    """Send reminder to CHAT_ID if yes_count < 10 for the poll."""
    try:
        data = active_polls.get(poll_id)
        if not data or not data.get("active"):
            return
        votes = data.get("votes", {})
        yes_users = [v for v in votes.values() if v.get("answer", "").startswith("Да")]
        if len(yes_users) < 10:
            # send reminder
            question = data.get("poll", {}).get("question", "Пожалуйста, проголосуйте!")
            text = f"🔔 Напоминание: <b>{question}</b>\nПожалуйста, проголосуйте — нам нужно как минимум 10 'Да' для подтверждения."
            await safe_telegram_call(bot.send_message, CHAT_ID, text)
            log.info("Reminder sent for poll %s (yes=%s)", poll_id, len(yes_users))
    except Exception:
        log.exception("Error in send_reminder_if_needed for poll %s", poll_id)

async def tag_questionable_users(poll_id: str) -> None:
    """
    Tag users who voted 'Под вопросом' (or containing 'Под вопросом' substring).
    Use saved user_id to create mention via tg://user?id=...
    """
    try:
        data = active_polls.get(poll_id)
        if not data or not data.get("active"):
            return
        votes = data.get("votes", {})
        # find close_dt stored earlier (ISO)
        close_iso = data.get("close_dt")
        close_dt = None
        if close_iso:
            try:
                close_dt = KALININGRAD_TZ.localize(datetime.fromisoformat(close_iso))
            except Exception:
                try:
                    close_dt = datetime.fromisoformat(close_iso)
                except Exception:
                    close_dt = None

        now = now_tz()
        mins_left = int((close_dt - now).total_seconds() // 60) if close_dt else None

        for v in votes.values():
            answer = v.get("answer", "")
            if "под" in answer.lower() or "под вопрос" in answer.lower() or "?" in answer:
                user_id = v.get("user_id")
                name = v.get("name", "Участник")
                if not user_id:
                    # we can't mention without user_id; fallback to using plain name
                    text = f"{name}, ⚠️ вы отметили 'Под вопросом'. Осталось {mins_left} минут до закрытия опроса. Пожалуйста, подтвердите участие."
                    await safe_telegram_call(bot.send_message, CHAT_ID, text)
                    log.debug("Tagged by name (no user_id) for poll %s: %s", poll_id, name)
                else:
                    mention = f'<a href="tg://user?id={user_id}">{name}</a>'
                    text = f"{mention}, ⚠️ вы отметили 'Под вопросом'. Осталось {mins_left} минут до закрытия опроса. Пожалуйста, подтвердите участие."
                    await safe_telegram_call(bot.send_message, CHAT_ID, text, parse_mode=ParseMode.HTML)
                    log.debug("Mentioned user %s for poll %s", user_id, poll_id)
    except Exception:
        log.exception("Error in tag_questionable_users for poll %s", poll_id)

def schedule_poll_reminders(poll_id: str) -> None:
    """
    Schedule the two kinds of jobs for the given poll:
      - every 3 hours reminder if yes<10 (from start until close)
      - every 30 minutes tagging 'Под вопросом' users from close-2h until close
    Store close_dt in active_polls[poll_id]['close_dt'] as ISO.
    """
    try:
        data = active_polls.get(poll_id)
        if not data:
            return
        poll = data.get("poll", {})
        # Only for Tue and Thu per request
        if poll.get("day") not in ("tue", "thu"):
            return

        loop = asyncio.get_event_loop()
        start_dt = now_tz()
        close_dt = compute_poll_close_dt(poll, start_dt)
        # safety: ensure at least 2 hours duration, otherwise fallback to start+24h
        if close_dt <= start_dt + timedelta(minutes=5):
            close_dt = start_dt + timedelta(hours=24)

        # store close timestamp for later use by tag job
        try:
            data["close_dt"] = close_dt.isoformat()
        except Exception:
            data["close_dt"] = None

        # Job ids
        reminder_job_id = f"reminder_{poll_id}"
        tag_job_id = f"tagq_{poll_id}"

        # schedule reminder every 3 hours from start to close
        try:
            # remove previous if exists
            try:
                scheduler.remove_job(reminder_job_id)
            except Exception:
                pass
            scheduler.add_job(
                lambda pid=poll_id: asyncio.run_coroutine_threadsafe(send_reminder_if_needed(pid), loop),
                trigger="interval",
                hours=3,
                start_date=start_dt,
                end_date=close_dt,
                id=reminder_job_id,
            )
            log.info("Scheduled 3h reminders for poll %s from %s to %s", poll_id, start_dt, close_dt)
        except Exception:
            log.exception("Failed to schedule 3h reminders for poll %s", poll_id)

        # schedule tagging every 30 minutes starting 2h before close until close
        try:
            tag_start = max(start_dt, close_dt - timedelta(hours=2))
            try:
                scheduler.remove_job(tag_job_id)
            except Exception:
                pass
            scheduler.add_job(
                lambda pid=poll_id: asyncio.run_coroutine_threadsafe(tag_questionable_users(pid), loop),
                trigger="interval",
                minutes=30,
                start_date=tag_start,
                end_date=close_dt,
                id=tag_job_id,
            )
            log.info("Scheduled tagging (30m) for poll %s from %s to %s", poll_id, tag_start, close_dt)
        except Exception:
            log.exception("Failed to schedule tagging for poll %s", poll_id)

        # persist that we scheduled these (for debugging)
        asyncio.create_task(save_data())
    except Exception:
        log.exception("Error in schedule_poll_reminders for poll %s", poll_id)

# -------------------- Poll lifecycle --------------------
async def start_poll(poll: Dict[str, Any], from_admin: bool = False) -> None:
    """Create and register a poll. Ensures options count fits Telegram limits.
    """
    try:
        options = poll.get("options", [])[:10]
        if not options:
            log.warning("Poll has no options, skipping: %s", poll)
            return
        msg = await safe_telegram_call(
            bot.send_poll,
            chat_id=CHAT_ID,
            question=poll["question"],
            options=options,
            is_anonymous=False,
            allows_multiple_answers=False,
        )
        if not msg:
            log.error("send_poll returned None — poll not created: %s", poll.get("question"))
            return

        poll_id = msg.poll.id
        active_polls[poll_id] = {
            "message_id": msg.message_id,
            "poll": poll,
            "votes": {},
            "active": True,
            "created_at": iso_now(),
        }
        await save_data()
        await safe_telegram_call(bot.send_message, CHAT_ID, "📢 <b>Новый опрос!</b>\nПроголосуйте 👇")
        if from_admin:
            await safe_telegram_call(bot.send_message, ADMIN_ID, f"✅ Опрос вручную: {poll['question']}")
        log.info("Poll created: %s", poll.get("question"))

        # --- NEW: schedule reminders and tagging for Tue/Thu polls ---
        try:
            schedule_poll_reminders(poll_id)
        except Exception:
            log.exception("Failed to setup reminders for poll %s", poll_id)
    except Exception:
        log.exception("Failed to start poll")

async def _chunk_and_send(chat_id: int, text: str) -> None:
    """Send text in chunks respecting TELEGRAM_MESSAGE_LIMIT."""
    if not text:
        return
    chunks = [text[i:i+TELEGRAM_MESSAGE_LIMIT] for i in range(0, len(text), TELEGRAM_MESSAGE_LIMIT)]
    for chunk in chunks:
        await safe_telegram_call(bot.send_message, chat_id, chunk)

async def send_summary(poll: Dict[str, Any]) -> None:
    """Finalize poll, summarize votes and update stats. Splits long messages automatically."""
    # find matching active poll
    for poll_id, data in list(active_polls.items()):
        if data.get("poll") == poll:
            try:
                data["active"] = False
                votes = data.get("votes", {})
                yes_users = [v["name"] for v in votes.values() if v["answer"].startswith("Да")]
                no_users = [v["name"] for v in votes.values() if v["answer"].startswith("Нет")]

                if poll.get("day") == "fri":
                    status = (
                        "📊 Итог субботнего опроса:\n\n"
                        f"👥 Всего проголосовало: {len(votes)} человек(а).\n"
                        "Решайте сами идти или нет — этот опрос просто для удобства, в субботу многие приходят без опроса ⚽"
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

                await _chunk_and_send(CHAT_ID, text)

                # update stats
                for v in votes.values():
                    if v["answer"].startswith("Да"):
                        stats[v["name"]] = stats.get(v["name"], 0) + 1

                # remove scheduled reminder/tag jobs for this poll if any
                try:
                    reminder_job_id = f"reminder_{poll_id}"
                    tag_job_id = f"tagq_{poll_id}"
                    try:
                        scheduler.remove_job(reminder_job_id)
                        log.info("Removed reminder job %s", reminder_job_id)
                    except Exception:
                        pass
                    try:
                        scheduler.remove_job(tag_job_id)
                        log.info("Removed tag job %s", tag_job_id)
                    except Exception:
                        pass
                except Exception:
                    log.exception("Failed to remove scheduled jobs for poll %s", poll_id)

                active_polls.pop(poll_id, None)
                await save_data()
                log.info("Summary sent for poll: %s", poll.get("question"))
            except Exception:
                log.exception("Failed to send summary for poll: %s", poll.get("question"))
            break

# -------------------- Poll answer handling --------------------
@dp.poll_answer_handler()
async def handle_poll_answer(poll_answer: types.PollAnswer) -> None:
    try:
        uid = poll_answer.user.id
        uname = poll_answer.user.full_name or poll_answer.user.first_name or str(uid)
        option_ids = poll_answer.option_ids
        for poll_id, data in list(active_polls.items()):
            if poll_answer.poll_id == poll_id:
                if not option_ids:
                    data["votes"].pop(str(uid), None)
                else:
                    answer = data["poll"]["options"][option_ids[0]]
                    # --- NEW: store user_id to enable direct mentions later ---
                    data["votes"][str(uid)] = {"name": uname, "answer": answer, "user_id": uid}
                # save asynchronously (fire-and-forget)
                asyncio.create_task(save_data())
                log.debug("Vote saved: %s -> %s", uname, data["votes"].get(str(uid)))
                return
    except Exception:
        log.exception("Error handling poll answer")

# -------------------- Bot commands --------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message) -> None:
    await message.reply("👋 Привет! Я бот для организации игр на песчанке. Напиши /commands для списка команд.")

@dp.message_handler(commands=["commands"])
async def cmd_commands(message: types.Message) -> None:
    text = (
        "Список доступных команд:\n\n"
        "Для всех:\n"
        "/status — показать текущий опрос\n"
        "/stats — статистика «Да ✅»\n"
        "/nextpoll — когда следующий опрос\n"
        "/uptime — время работы бота\n"
        "/commands — справка\n\n"
        "Для администратора:\n"
        "/startpoll Вопрос | Вариант1 | Вариант2 | ...\n"
        "/closepoll — закрыть опрос\n"
        "/addplayer Имя — добавить игрока\n"
        "/removeplayer Имя — удалить игрока\n"
        "/reload — обновить расписание\n"
        "/summary — отправить текущую сводку\n"
        "/backup — получить текущие данные (файл)\n"
    )
    await message.reply(text)

@dp.message_handler(commands=["nextpoll"])
async def cmd_nextpoll(message: types.Message) -> None:
    try:
        nxt = compute_next_poll_datetime()
        if not nxt:
            return await message.reply("ℹ️ Нет запланированных опросов.")
        dt, cfg = nxt
        fmt = dt.strftime("%Y-%m-%d %H:%M %Z")
        await message.reply(f"Следующий опрос: <b>{cfg['question']}</b>\nКогда: {fmt}")
    except Exception:
        log.exception("Error in /nextpoll")
        await message.reply("⚠️ Ошибка при определении следующего опроса. Проверьте логи.")

@dp.message_handler(commands=["status"])
async def cmd_status(message: types.Message) -> None:
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Активных опросов нет.")
    _, data = last
    poll = data["poll"]
    await message.reply(f"<b>{poll['question']}</b>\n\n{format_poll_votes(data)}")

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message) -> None:
    if not stats:
        return await message.reply("📊 Пока нет статистики.")
    text = "\n".join(f"{name}: {count}" for name, count in sorted(stats.items(), key=lambda x: -x[1]))
    await message.reply(f"📈 Статистика 'Да ✅':\n{text}")

@dp.message_handler(commands=["uptime"])
async def cmd_uptime(message: types.Message) -> None:
    uptime = datetime.now() - START_TIME
    hours, remainder = divmod(int(uptime.total_seconds()), 3600)
    minutes = (remainder // 60)
    await message.reply(f"⏱ Бот работает уже {hours} ч {minutes} мин.")

# -------------------- Admin commands --------------------
@dp.message_handler(commands=["startpoll"])
async def cmd_startpoll(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    parts = [p.strip() for p in message.get_args().split("|") if p.strip()]
    if len(parts) < 3:
        return await message.reply("Формат: /startpoll Вопрос | Вариант1 | Вариант2 | ...")
    poll = {"day": "manual", "time_poll": now_tz().strftime("%H:%M"), "question": parts[0], "options": parts[1:]}
    await start_poll(poll, from_admin=True)
    await message.reply("✅ Опрос создан вручную.")

@dp.message_handler(commands=["closepoll"])
async def cmd_closepoll(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Нет активных опросов.")
    _, data = last
    await send_summary(data["poll"])
    await message.reply("✅ Опрос закрыт и итоги отправлены.")

@dp.message_handler(commands=["addplayer"])
async def cmd_addplayer(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    name = message.get_args().strip()
    if not name:
        return await message.reply("Использование: /addplayer Имя")
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Нет активных опросов.")
    pid, data = last
    key = f"admin_{name}_{int(time.time())}"
    data["votes"][key] = {"name": name, "answer": "Да ✅ (добавлен вручную)"}
    await save_data()
    await message.reply(f"✅ Игрок '{name}' добавлен как 'Да ✅'.")

@dp.message_handler(commands=["removeplayer"])
async def cmd_removeplayer(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    name = message.get_args().strip()
    if not name:
        return await message.reply("Использование: /removeplayer Имя")
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Нет активных опросов.")
    pid, data = last
    removed = 0
    for uid, v in list(data["votes"].items()):
        if v.get("name") == name:
            del data["votes"][uid]
            removed += 1
    await save_data()
    await message.reply(f"✅ Игрок '{name}' удалён (найдено: {removed}).")

@dp.message_handler(commands=["reload"])
async def cmd_reload(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    schedule_polls()
    await message.reply("✅ Расписание обновлено.")

@dp.message_handler(commands=["summary"])
async def cmd_summary(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Нет активных опросов.")
    _, data = last
    await send_summary(data["poll"])
    await message.reply("✅ Итог отправлен вручную.")

@dp.message_handler(commands=["backup"])
async def cmd_backup(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    if os.path.exists(DATA_FILE):
        await message.reply_document(open(DATA_FILE, "rb"), caption="📦 Текущие данные бота")
    else:
        await message.reply("⚠️ Данных для бэкапа нет.")

# -------------------- Scheduler helpers --------------------
def compute_next_poll_datetime() -> Optional[Tuple[datetime, Dict[str, Any]]]:
    now = now_tz()
    candidates = []
    for cfg in polls_config:
        day = cfg.get("day")
        if day not in WEEKDAY_MAP:
            continue
        hour, minute = map(int, cfg["time_poll"].split(":"))
        target = WEEKDAY_MAP[day]
        days_ahead = (target - now.weekday()) % 7
        # build localized target datetime
        base = datetime(now.year, now.month, now.day, hour, minute)
        base_local = KALININGRAD_TZ.localize(base) if base.tzinfo is None else base.astimezone(KALININGRAD_TZ)
        dt = base_local + timedelta(days=days_ahead)
        if dt <= now:
            dt += timedelta(days=7)
        candidates.append((dt, cfg))
    if not candidates:
        return None
    return sorted(candidates, key=lambda x: x[0])[0]

def schedule_polls() -> None:
    scheduler.remove_all_jobs()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()

    for poll in polls_config:
        try:
            tp = list(map(int, poll["time_poll"].split(":")))
            tg = list(map(int, poll["time_game"].split(":")))

            # Опрос по Калининградскому времени
            scheduler.add_job(
                lambda p=poll: asyncio.run_coroutine_threadsafe(start_poll(p), loop),
                trigger=CronTrigger(
                    day_of_week=poll["day"],
                    hour=tp[0],
                    minute=tp[1],
                    timezone=KALININGRAD_TZ  # <── добавлено
                ),
                id=f"poll_{poll['day']}"
            )

            # Итог за час до игры (тоже по Калининградскому времени)
            next_day = "sat" if poll["day"] == "fri" else poll["day"]
            summary_hour = max(tg[0] - 1, 0)
            scheduler.add_job(
                lambda p=poll: asyncio.run_coroutine_threadsafe(send_summary(p), loop),
                trigger=CronTrigger(
                    day_of_week=next_day,
                    hour=summary_hour,
                    minute=tg[1],
                    timezone=KALININGRAD_TZ  # <── добавлено
                ),
                id=f"summary_{poll['day']}"
            )

            log.info(f"✅ Scheduled poll for {poll['day']} at {poll['time_poll']} (Kaliningrad)")
        except Exception:
            log.exception("Failed to schedule poll: %s", poll)

    try:
        scheduler.add_job(lambda: asyncio.run_coroutine_threadsafe(save_data(), loop), "interval", minutes=10)
    except Exception:
        log.exception("Failed to schedule autosave job")

    try:
        scheduler.add_job(make_backup, "cron", hour=3, minute=0, timezone=KALININGRAD_TZ)
    except Exception:
        log.exception("Failed to schedule backup job")

    log.info("Scheduler refreshed (timezone: Europe/Kaliningrad)")
    log.info("=== Запланированные задания ===")
    for job in scheduler.get_jobs():
        nxt = getattr(job, "next_run_time", None)
        log.info(f"Job: {job.id}, next run: {nxt}")


# -------------------- KeepAlive server for Railway --------------------
async def handle(request):
    return web.Response(text="✅ Bot is alive")

async def start_keepalive_server() -> None:
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        log.info("KeepAlive server started on port %s", PORT)
    except OSError as e:
        if e.errno == 98:
            log.warning("⚠️ Port %s already in use, skipping KeepAlive server startup", PORT)
        else:
            log.exception("Failed to start KeepAlive server")
            raise



# -------------------- Errors and shutdown --------------------
@dp.errors_handler()
async def global_errors(update, exception):
    log.exception("Global error: %s", exception)
    try:
        await safe_telegram_call(bot.send_message, ADMIN_ID, f"⚠️ Ошибка: {exception}")
    except Exception:
        log.exception("Failed to notify admin about error")
    return True

async def shutdown() -> None:
    log.info("Shutting down...")
    try:
        await save_data()
    except Exception:
        log.exception("Error while saving data during shutdown")
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        log.exception("Error shutting down scheduler")
    try:
        await bot.close()
    except Exception:
        log.exception("Error closing bot")
    log.info("Shutdown complete.")

def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown()))
        except NotImplementedError:
            # on some platforms (Windows) add_signal_handler may not be implemented
            pass

# -------------------- Main --------------------
async def main() -> None:
    log.info("Starting bot...")
    await load_data()

    # ensure polling mode
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    # schedule jobs and keepalive + scheduler
    await start_keepalive_server()
    schedule_polls()
    try:
        scheduler.start()
    except Exception:
        log.exception("Failed to start scheduler")

    # notify admin
    await safe_telegram_call(bot.send_message, ADMIN_ID, "✅ Бот запущен и готов к работе!")

    # add signal handlers
    loop = asyncio.get_event_loop()
    _install_signal_handlers(loop)

    log.info("Start polling...")
    await dp.start_polling()

if __name__ == "__main__":
    # robust restart loop
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Stopped by KeyboardInterrupt")
            break
        except Exception:
            log.exception("Critical error in main — will attempt restart")
            time.sleep(5)
            continue
        else:
            break
















